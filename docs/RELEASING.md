# Releasing

Releases are built and uploaded by `.github/workflows/publish.yml` with PyPI **trusted
publishing** (OIDC). No PyPI token lives in the repo, in CI secrets or in `.pypirc`.

## One-time setup

1. Make the GitHub repo public (PyPI project links point at it; a private repo gives users 404s).
2. On <https://test.pypi.org> and <https://pypi.org>: *Your account → Publishing → Add a new
   pending publisher*:
   - Project name: `taskiq-llm-queue`
   - Owner/repo: `techiewonk/taskiq-llm-queue`
   - Workflow: `publish.yml`
   - Environment: `testpypi` (TestPyPI) / `pypi` (PyPI)
3. In GitHub → *Settings → Environments*, create `testpypi` and `pypi` (add required
   reviewers on `pypi` so a release needs a second click).

## Each release

1. Bump `src/taskiq_llm_queue/_version.py` (PEP 440: `0.1.0a1`, `0.1.0b1`, `0.1.0rc1`, `0.1.0`).
2. Move the `[Unreleased]` notes in `CHANGELOG.md` under the new version and date.
3. Check locally (Docker stack up):

   ```bash
   make check                       # ruff, format, mypy, unit + integration tests
   uv build --out-dir /tmp/llmq-dist && uvx twine check --strict /tmp/llmq-dist/*
   ```

4. Commit, push, then **Actions → publish → Run workflow** to upload to TestPyPI, and try it:

   ```bash
   uv venv /tmp/try && uv pip install --python /tmp/try \
     --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ \
     "taskiq-llm-queue[openai,langchain-aws]==<version>"
   ```

5. Tag and publish a GitHub release (`v<version>`); the workflow uploads to PyPI.

A version number can be uploaded only once (PyPI never allows re-use, even after deleting it),
so test on TestPyPI first.

## Manual upload (fallback only)

```bash
uv build
UV_PUBLISH_TOKEN=<pypi-token> uv publish          # token from your shell, never a committed file
```
