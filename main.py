"""Independent AG99 desktop state consumer and Personal Runtime sensor."""

from __future__ import annotations

from typing import Any

from astrbot.api import logger
from astrbot.api.star import Context, Star, register

from .runtime import DesktopAwarenessRuntime


@register(
    "astrbot_plugin_ag99_desktop_awareness",
    "AG99",
    "Subscribe to AG99live desktop state and submit desktop facts.",
    "0.1.0",
)
class DesktopAwarenessPlugin(Star):
    """Consume Core desktop snapshots without controlling screenshot capture."""

    def __init__(self, context: Context, config: dict[str, Any] | None = None):
        super().__init__(context)
        self.config = config if isinstance(config, dict) else {}
        self.enabled = bool(self.config.get("enabled", True))
        self._runtime: DesktopAwarenessRuntime | None = None

    async def initialize(self) -> None:
        if not self.enabled:
            return
        try:
            runtime = DesktopAwarenessRuntime(self.context, self.config)
            runtime.start()
            self._runtime = runtime
        except Exception:
            logger.exception("AG99 desktop awareness could not be initialized")

    async def terminate(self) -> None:
        runtime, self._runtime = self._runtime, None
        if runtime is not None:
            await runtime.stop()

