# AG99 Desktop Awareness

This plugin consumes the current `desktop_state` snapshot published by the
AG99live Adapter through AstrBot Core `SnapshotHub`. It owns desktop change
comparison, desktop return detection, and the `desktop_changed`/
`desktop_returned` Runtime Observation facts.

AG99live remains the producer. It captures desktop frames, reads the desktop
presence protocol, and publishes state through Core. The plugin subscribes to
that state and never sends a screenshot or Presence request to AG99live. It
never stores a raw frame in an Observation.

Desktop-return submissions carry a cancellable token. A newer return snapshot,
client input resuming, a disconnected client, or a disabled sensor withdraws
the older pending fact before Personal Runtime can act on it.

The sensor keeps the legacy observation target identifier `ag99live` so existing
`platform_settings.personal_runtime_observation_targets` entries continue to
authorize the facts after the runtime moves into this repository.

## Source and deployment

The source of truth is this repository. The generated runtime copy is
`C:\Users\Administrator\Downloads\AstrBot\data\plugins\astrbot_plugin_ag99_desktop_awareness`.
Edit the source repository and commit there. The local `post-commit` hook runs
`deploy_desktop_awareness.ps1` and synchronizes the plugin files. After a fresh
clone, run `powershell -ExecutionPolicy Bypass -File .\setup_hooks.ps1` once to
activate the tracked hook.

Enable frame capture and `desktop_presence_capture` in the AG99live Adapter.
Enable desktop observation in this plugin for `desktop_changed`, and enable
`desktop_return.enabled` here for `desktop_returned`. Presence thresholds and
sampling interval belong to the Adapter producer configuration.
Personal Policy and the selected Provider still decide whether a fact becomes
visible output.
