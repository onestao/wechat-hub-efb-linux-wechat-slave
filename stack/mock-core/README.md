# Qualification Mock Core Snapshot

`app.py` is a test-only snapshot of the WeChat Hub mock Core used by the
RC.14 EFB functional-correctness regression suite. It is included here so
the EFB repository can reproduce all 85 tests in an isolated GitHub runner
without reading an adjacent deployment worktree.

Snapshot source:

- workspace path: `stack/mock-core/app.py`
- captured SHA-256: `5BAA0E17DA47A14BA9C45C6CBD9769AE6AA157C95CCFF62163F005DD81CCF258`
- captured for candidate build: 2026-09-16

This fixture is never used by the production image entrypoint.
