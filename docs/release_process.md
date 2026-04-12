# Release Process

## Overview

Releases are published to [PyPI](https://pypi.org/project/stpstone/) automatically via the
`.github/workflows/release_pypi.yaml` GitHub Actions workflow. The workflow is **tag-driven**,
not branch-driven — the trigger is pushing a version tag, not a commit message or branch name.

## How to Release

```bash
# 1. Bump the version in pyproject.toml
make bump_version            # or edit pyproject.toml manually

# 2. Commit the version bump
git add pyproject.toml
git commit -m "chore(release): v1.2.3"

# 3. Create a version tag
git tag v1.2.3

# 4. Push both the commit and the tag
git push origin <branch>
git push origin v1.2.3       # ← this is what fires the workflow
```

> The commit message on step 2 is convention only — the workflow ignores it entirely.
> Only the tag push on step 4 triggers the pipeline.

## Supported Version Formats

| Format | Example | Meaning |
|--------|---------|---------|
| `v{major}.{minor}.{patch}` | `v1.2.3` | Stable release |
| `v{major}.{minor}.{patch}a{N}` | `v1.2.3a1` | Alpha pre-release |
| `v{major}.{minor}.{patch}b{N}` | `v1.2.3b2` | Beta pre-release |
| `v{major}.{minor}.{patch}rc{N}` | `v1.2.3rc1` | Release candidate |

## Workflow Jobs

```
push tag ──► check_tests ──► release (gate)
         └──► details ──► check_pypi ──► setup_and_build ──► pypi
                                      └──────────────────► github_release
```

| Job | What it does |
|-----|--------------|
| `check_tests` | Runs `.github/workflows/tests.yaml` to validate the tag |
| `release` | Gate: only continues if tests passed |
| `details` | Extracts version number and pre-release suffix from the tag |
| `check_pypi` | Fetches the current version on PyPI; **fails if the new tag is not newer** |
| `setup_and_build` | Installs dependencies (strips `win32`-only packages), runs `poetry build` |
| `pypi` | Publishes with `poetry publish`; falls back to `twine` on failure |
| `github_release` | Creates a GitHub Release with the built `.whl` / `.tar.gz` attached |

## Manual Trigger

You can also trigger the workflow manually from the GitHub UI without pushing a tag:

1. Go to **Actions → Release to PyPI → Run workflow**
2. Set `tests_passed` to `true`
3. Click **Run workflow**

This is useful for re-publishing an existing tag if the automated run failed for a transient reason.

## Required Secrets

| Secret | Where to set it |
|--------|----------------|
| `PYPI_TOKEN` | Repository → Settings → Secrets → Actions |

The token must have upload permissions for the `stpstone` project on PyPI.
