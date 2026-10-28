# Stability policy

- **Public API** = the names in `taskiq_llm_queue.__all__`. Anything else (modules starting with
  `_`, submodules not re-exported) may change without notice.
- **0.x releases:** a minor release (0.y) may break the public API. Every break is announced in
  CHANGELOG.md one minor release ahead with a deprecation warning, when possible.
- **Wire format:** the task envelope carries `v`. Workers accept their own version and the
  previous one. A newer version is released for another worker, never dropped. Bumping `v` is
  a breaking change that needs a rolling-deploy note.
- **Redis state schema:** versioned under `<key_prefix>:schema`. A schema change ships with a
  migration step and is listed in the changelog.
- **Task names** (`taskiq_llm_queue.call`, `.embed`, `.job`) are stable for all of 0.x.
- **Supported versions:** Python 3.11–3.13. Dependency ranges are declared in `pyproject.toml`,
  and CI tests the lowest and highest supported versions.
