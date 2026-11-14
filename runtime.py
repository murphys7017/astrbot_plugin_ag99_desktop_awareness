"""Core-owned desktop snapshot consumers for AG99 Personal Runtime."""

from __future__ import annotations

import asyncio
import hashlib
import math
import time
from dataclasses import dataclass
from typing import Any

from astrbot.api import (
    DesktopStateSnapshot,
    SnapshotHub,
    SnapshotScope,
    logger,
)
from astrbot.core.platform.message_session import MessageSession
from astrbot.core.platform.message_type import MessageType

from .desktop_frame_difference import changed_area_percent


DESKTOP_STATE_KIND = "desktop_state"
DESKTOP_STATE_SOURCE = "ag99live.desktop"
OBSERVATION_PLUGIN_ID = "ag99live"


@dataclass(frozen=True, slots=True)
class DesktopSensor:
    plugin_id: str = OBSERVATION_PLUGIN_ID
    source_id: str = "desktop_change"


@dataclass(frozen=True, slots=True)
class DesktopReturnSensor:
    plugin_id: str = OBSERVATION_PLUGIN_ID
    source_id: str = "desktop_return"


@dataclass(slots=True)
class _FrameBaseline:
    generation: int
    client_uid: str
    revision: int
    image: str
    cancel_event: asyncio.Event | None = None


def _number(
    mapping: dict[str, Any],
    key: str,
    default: float,
    minimum: float,
    maximum: float,
) -> float:
    raw = mapping.get(key, default)
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(value):
        return default
    return min(max(value, minimum), maximum)


class DesktopAwarenessRuntime:
    """Subscribe to current desktop state and emit bounded runtime facts."""

    def __init__(self, context: Any, config: dict[str, Any]) -> None:
        self.context = context
        self.config = config if isinstance(config, dict) else {}
        get_hub = getattr(context, "get_snapshot_hub", None)
        self.snapshot_hub: SnapshotHub | None = (
            get_hub() if callable(get_hub) else None
        )
        self.handle = context.register_runtime_observation_sensor(DesktopSensor())
        self.return_handle = context.register_runtime_observation_sensor(
            DesktopReturnSensor()
        )
        self._reconcile_task: asyncio.Task | None = None
        self._subscriptions: dict[str, Any] = {}
        self._baselines: dict[str, _FrameBaseline] = {}
        self._presence_identities: dict[str, tuple[str, int]] = {}
        self._last_return_attempt: dict[tuple[str, str], float] = {}

    def start(self) -> None:
        if self.snapshot_hub is None:
            logger.warning("Desktop awareness is unavailable: SnapshotHub is missing")
            return
        if self._reconcile_task is None:
            self._reconcile_task = asyncio.create_task(
                self._reconcile_subscriptions(),
                name="ag99.desktop_awareness.subscriptions",
            )

    async def stop(self) -> None:
        tasks = [task for task in (self._reconcile_task,) if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._reconcile_task = None
        self._presence_identities.clear()
        self._last_return_attempt.clear()
        for subscription in self._subscriptions.values():
            subscription.close()
        self._subscriptions.clear()
        for baseline in self._baselines.values():
            if baseline.cancel_event is not None:
                baseline.cancel_event.set()
        self._baselines.clear()

    async def _reconcile_subscriptions(self) -> None:
        while True:
            try:
                platforms = self._platforms()
                active_ids = set()
                for platform in platforms:
                    platform_id = self._platform_id(platform)
                    if not platform_id:
                        continue
                    active_ids.add(platform_id)
                    if platform_id in self._subscriptions:
                        continue
                    scope = SnapshotScope(scope_type="platform", scope_id=platform_id)

                    async def callback(envelope, *, _platform_id=platform_id):
                        await self._on_snapshot(_platform_id, envelope)

                    self._subscriptions[platform_id] = self.snapshot_hub.subscribe(
                        kind=DESKTOP_STATE_KIND,
                        source_id=DESKTOP_STATE_SOURCE,
                        scope=scope,
                        callback=callback,
                        replay_latest=True,
                    )
                for platform_id in tuple(self._subscriptions):
                    if platform_id in active_ids:
                        continue
                    self._subscriptions.pop(platform_id).close()
                    baseline = self._baselines.pop(platform_id, None)
                    if baseline is not None and baseline.cancel_event is not None:
                        baseline.cancel_event.set()
                    self._presence_identities.pop(platform_id, None)
                    for key in tuple(self._last_return_attempt):
                        if key[0] == platform_id:
                            self._last_return_attempt.pop(key, None)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Desktop awareness subscription reconciliation failed")
            await asyncio.sleep(2)

    async def _on_snapshot(self, platform_id: str, envelope: Any) -> None:
        projection = getattr(envelope, "payload", None)
        if not isinstance(projection, DesktopStateSnapshot):
            return
        if not projection.connected:
            baseline = self._baselines.pop(platform_id, None)
            if baseline is not None and baseline.cancel_event is not None:
                baseline.cancel_event.set()
            self._presence_identities.pop(platform_id, None)
            return

        await self._on_presence_snapshot(platform_id, projection.presence)

        frame = projection.latest_frame
        if not projection.observation_supported or frame is None:
            baseline = self._baselines.pop(platform_id, None)
            if baseline is not None and baseline.cancel_event is not None:
                baseline.cancel_event.set()
            return
        attachment = self.snapshot_hub.get_attachment(frame.attachment_ref)
        image = getattr(attachment, "payload", None)
        if not isinstance(image, str) or not image:
            return

        baseline = self._baselines.get(platform_id)
        if (
            baseline is None
            or baseline.generation != frame.generation
            or baseline.client_uid != frame.client_uid
        ):
            self._baselines[platform_id] = _FrameBaseline(
                generation=frame.generation,
                client_uid=frame.client_uid,
                revision=frame.revision,
                image=image,
            )
            logger.debug(
                "DIAG desktop_awareness.baseline platform=%s revision=%s",
                platform_id,
                frame.revision,
            )
            return
        if baseline.revision == frame.revision:
            return

        previous_image = baseline.image
        try:
            changed = await asyncio.to_thread(
                changed_area_percent,
                previous_image,
                image,
            )
        except Exception as exc:
            logger.warning(
                "Desktop awareness comparison failed platform=%s error=%s",
                platform_id,
                type(exc).__name__,
            )
            return
        baseline.revision = frame.revision
        baseline.image = image
        settings = self._section("desktop_observation")
        threshold = _number(
            settings,
            "changed_area_threshold_percent",
            15.0,
            1.0,
            100.0,
        )
        if not bool(settings.get("enabled", True)) or changed < threshold:
            return
        logger.debug(
            "DIAG desktop_awareness.area platform=%s revision=%s changed_percent=%.2f threshold_percent=%.2f",
            platform_id,
            frame.revision,
            changed,
            threshold,
        )
        await self._submit_changed(
            platform_id,
            frame,
            attachment.ref.expires_at,
            changed,
            threshold,
        )

    async def _submit_changed(
        self,
        platform_id: str,
        frame: Any,
        attachment_expires_at: float | None,
        changed: float,
        threshold: float,
    ) -> None:
        baseline = self._baselines.get(platform_id)
        if baseline is None:
            return
        if baseline.cancel_event is not None:
            baseline.cancel_event.set()
        cancel_event = asyncio.Event()
        baseline.cancel_event = cancel_event
        ttl = _number(
            self._section("desktop_observation"),
            "event_ttl_seconds",
            60.0,
            5.0,
            300.0,
        )
        if attachment_expires_at is not None:
            ttl = min(ttl, max(0.0, attachment_expires_at - time.time()))
        if ttl <= 0:
            return
        try:
            await self.handle.submit(
                kind="desktop_changed",
                session=self._session(platform_id, frame.client_uid),
                payload={
                    "changed": True,
                    "changed_area_percent": round(changed, 3),
                    "threshold_percent": round(threshold, 3),
                    "frame_revision": hashlib.sha256(
                        str(frame.revision).encode("ascii")
                    ).hexdigest(),
                    "desktop_frame_attachment_id": frame.attachment_ref.attachment_id,
                    "desktop_frame_source_id": DESKTOP_STATE_SOURCE,
                    "desktop_frame_revision": frame.revision,
                    "desktop_frame_generation": frame.generation,
                },
                expires_in_seconds=ttl,
                coalesce_key="desktop",
                cancel_event=cancel_event,
            )
            logger.debug(
                "DIAG desktop_awareness.changed platform=%s revision=%s",
                platform_id,
                frame.revision,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "Desktop awareness submission failed platform=%s error=%s",
                platform_id,
                type(exc).__name__,
            )

    async def _on_presence_snapshot(
        self,
        platform_id: str,
        presence: dict[str, Any] | None,
    ) -> None:
        if not isinstance(presence, dict):
            return
        identity = (presence.get("boot_id"), presence.get("sequence"))
        if (
            not isinstance(identity[0], str)
            or not isinstance(identity[1], int)
            or self._presence_identities.get(platform_id) == identity
        ):
            return
        self._presence_identities[platform_id] = identity
        returned = presence.get("returned")
        if not isinstance(returned, dict):
            return
        platform = self._platform_by_id(platform_id)
        if platform is None or not self._is_quiet(platform):
            return
        await self._submit_returned(
            platform_id,
            str(getattr(platform, "client_uid", "") or ""),
            returned,
            presence,
        )

    async def _submit_returned(
        self,
        platform_id: str,
        client_uid: str,
        returned: dict[str, Any],
        snapshot: dict[str, Any],
    ) -> None:
        if not bool(self._section("desktop_return").get("enabled", False)):
            return
        inactive_seconds = returned.get("inactive_seconds")
        age_seconds = returned.get("age_seconds")
        if not isinstance(inactive_seconds, int | float) or not isinstance(
            age_seconds, int | float
        ):
            return
        ttl = max(0.0, 30.0 - float(age_seconds))
        if ttl <= 0:
            return
        cooldown = _number(
            self._section("desktop_return"),
            "cooldown_seconds",
            900.0,
            0.0,
            86400.0,
        )
        key = (platform_id, str(snapshot.get("boot_id") or ""))
        if time.monotonic() - self._last_return_attempt.get(key, -math.inf) < cooldown:
            return
        basis = str(returned.get("basis") or "unknown")
        summary = {
            "unlock": "系统观察到：屏幕解锁后恢复了输入操作。",
            "resume": "系统观察到：电脑从休眠恢复后出现了新的输入操作。",
            "input_resumed": "系统观察到：长时间无输入后恢复了操作；这不能确认用户身体离开过。",
        }.get(basis, "系统观察到桌面恢复了输入操作。")
        try:
            await self.return_handle.submit(
                kind="desktop_returned",
                session=self._session(platform_id, client_uid),
                payload={
                    "basis": basis,
                    "summary": summary,
                    "inactive_seconds": float(inactive_seconds),
                },
                expires_in_seconds=ttl,
                coalesce_key="desktop_return",
            )
            self._last_return_attempt[key] = time.monotonic()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "Desktop return submission failed platform=%s error=%s",
                platform_id,
                type(exc).__name__,
            )

    def _platforms(self) -> list[Any]:
        manager = getattr(self.context, "platform_manager", None)
        return list(getattr(manager, "platform_insts", ()) or ())

    def _platform_by_id(self, platform_id: str) -> Any | None:
        for platform in self._platforms():
            if self._platform_id(platform) == platform_id:
                return platform
        return None

    @staticmethod
    def _platform_id(platform: Any) -> str:
        value = getattr(platform, "platform_id", None)
        if value:
            return str(value).strip()
        meta = getattr(platform, "meta", None)
        metadata = meta() if callable(meta) else None
        return str(getattr(metadata, "id", "") or "").strip()

    @staticmethod
    def _session(platform_id: str, client_uid: str) -> MessageSession:
        return MessageSession(platform_id, MessageType.FRIEND_MESSAGE, client_uid)

    def _section(self, name: str) -> dict[str, Any]:
        value = self.config.get(name, {})
        return value if isinstance(value, dict) else {}

    @staticmethod
    def _is_quiet(platform: Any) -> bool:
        coordinator = getattr(platform, "turn_coordinator", None)
        window = getattr(coordinator, "observation_window", None)
        snapshot = getattr(window, "snapshot", None)
        if not callable(snapshot):
            return True
        token = snapshot(0)
        if token is None:
            return False
        revision = getattr(window, "revision", 0)
        checker = getattr(window, "is_current", None)
        return bool(checker(token, 0, revision)) if callable(checker) else True


__all__ = ["DesktopAwarenessRuntime"]
