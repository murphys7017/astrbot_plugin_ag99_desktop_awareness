# AG99 Desktop Awareness

This plugin consumes the current `desktop_state` snapshot published by the
AG99live Adapter through AstrBot Core `SnapshotHub`. It owns desktop change
comparison, desktop return detection, and the `desktop_changed`/
`desktop_returned` Runtime Observation facts.

AG99live remains the producer. It captures desktop frames, reads the desktop
presence protocol, and publishes state through Core. The plugin never sends a
screenshot request to AG99live and never stores a raw frame in an Observation.

The sensor keeps the legacy observation target identifier `ag99live` so existing
`platform_settings.personal_runtime_observation_targets` entries continue to
authorize the facts after the runtime moves into this repository.

## Source and deployment

The source of truth is this repository. The generated runtime copy is
`C:\Users\Administrator\Downloads\AstrBot\data\plugins\astrbot_plugin_ag99_desktop_awareness`.
Edit the source repository and commit there. The local `post-commit` hook runs
`deploy_desktop_awareness.ps1` and synchronizes the plugin files.

Enable frame capture in the AG99live Adapter and enable desktop observation in
this plugin. Return detection also requires `desktop_return.enabled` here.
Personal Policy and the selected Provider still decide whether a fact becomes
visible output.
