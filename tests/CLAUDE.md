# CLAUDE.md — tests/

Formatting and style rules for all test files under `tests/`.

## Indentation

**Tabs, not spaces.** `ruff.toml` sets `indent-style = "tab"`. Every level of indentation
is one tab character (`\t`). Never use spaces for indentation, even in test files.

This is the single most common cause of `make lint` reporting "N files reformatted": agents
generate 4-space-indented test files, which `make test_feat` does not catch because it only
runs `ruff check` (not `ruff format`).

## Import Ordering

`ruff.toml` uses `force-sort-within-sections = true`. Within each import group, `import X`
and `from X import Y` are sorted together by module name — not separated by kind. Follow this
exact pattern:

```python
# stdlib — sorted by module name
from datetime import date
from logging import Logger
from unittest.mock import MagicMock

# third-party — sorted by module name, mixing import/from freely
import pandas as pd
import pytest
from pytest_mock import MockerFixture
import requests
from requests import Response

# first-party — sorted by module name
from stpstone.ingestion.countries.br.exchange.<module> import <Class>
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement
```

Two blank lines after all imports before the first fixture or test.

## Quotes

Double quotes everywhere (`quote-style = "double"` in `ruff.toml`).
Single quotes only inside docstrings where needed for literals.

## Docstring parameter types

The same type-consistency checker that runs on source files also runs on test files.
**Never append `, optional` to a parameter's type field** — even for helper functions inside
test files. Write the type exactly as it appears in the Python annotation:

```python
# Wrong
txt_filename : str, optional

# Correct
txt_filename : str
```

This applies to every function with a docstring in `tests/`, including module-level helpers
like `_build_zip_bytes`.

## Verification

`make test_feat` runs `ruff check` only — it catches lint and isort issues but **not**
formatting. Always run `make lint` as the final step. It must report **"0 files reformatted"**
before the work is complete.
