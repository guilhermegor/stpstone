# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Where to Place New Modules

Every ingestion module must live under `stpstone/ingestion/countries/<country>/` where `<country>` is one of the existing codes (`br`, `us`, `ww`) or a new ISO code if the source belongs to a different country.

Within the country folder, use the matching domain subfolder:

| Subfolder | When to use |
|-----------|-------------|
| `bylaws` | Fund regulation documents, prospectuses, investment bylaws |
| `exchange` | Exchange-traded instruments, indexes, derivatives, trading data |
| `macroeconomics` | Economic indicators, central bank data, forecasts |
| `otc` | Over-the-counter markets, debentures, CRI/CRA, fixed income |
| `registries` | Entity registries (companies, funds, securities listings) |
| `taxation` | Tax tables, tax schedules, tax-related calculations |

If the source genuinely does not fit any of these, create a new subfolder with a lowercase, underscore-separated name and add an `__init__.py`. **Never add new `__init__.py` files without explicit approval** — the `make test_feat MODULE=<name>` recipe matches filenames and `__init__` will cause false matches or failures.

## One Class Per Module

Each source file must contain exactly **one public class**.

- Public classes: one per file, named after the file (`b3_futures_closing_adj.py` → `B3FuturesClosingAdj`).
- Private/shared base classes: allowed in their own file with a leading underscore prefix (`_b3_trading_hours_core.py`). Must not appear in the same file as a public class.

**When refactoring an existing module that contains multiple classes**, split every class into its own file and also split the corresponding test file and example file to match (one test file and one example file per class module). Delete the original multi-class files after the split.

## File Naming

- Lowercase words separated by underscores: `b3_futures_closing_adj.py`
- No duplicate filenames anywhere under `stpstone/ingestion/`
- Test file name must mirror the module exactly: `tests/unit/test_b3_futures_closing_adj.py`
- Examples file name must mirror the module exactly: `examples/b3_futures_closing_adj.py`

Every new module requires **three files** created together:

| File | Location | Purpose |
|------|----------|---------|
| `<module_name>.py` | `stpstone/ingestion/countries/<country>/<domain>/` | Implementation |
| `test_<module_name>.py` | `tests/unit/` | Unit tests |
| `<module_name>.py` | `examples/` | Runnable usage example |

## Examples File

Each examples file must live under `examples/` at the repository root and share the exact filename of its module (no `test_` prefix).

Template:

```python
"""<Human-readable title of what the data source provides>."""

from stpstone.ingestion.countries.<country>.<domain>.<module_name> import <ClassName>


cls_ = <ClassName>(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF <HUMAN READABLE LABEL>: \n{df_}")
df_.info()
```

Rules:
- Module docstring: one sentence, title-cased description of the data source, no period.
- Instantiate with all optional parameters set to `None` (demonstrates the zero-config path).
- Always call `.run()` and print both the DataFrame and `.info()`.
- No hard-coded dates, credentials, or environment-specific paths.
- No `if __name__ == "__main__"` guard — the file is meant to be run directly.

## Class Scaffolding

Every new (or refactored) ingestion module must follow this template exactly:

```python
"""Implementation of ingestion instance."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import backoff
import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import requests
from requests import Response, Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.abc.ingestion_abc import (
	ABCIngestionOperations,
	ContentParser,
	CoreIngestion,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


class IngestionConcreteClass(ABCIngestionOperations):
	"""Ingestion concrete class."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the ingestion class.

		Parameters
		----------
		date_ref : Optional[date]
			The date of reference, by default None.
		logger : Optional[Logger]
			The logger, by default None.
		cls_db : Optional[Session]
			The database session, by default None.

		Returns
		-------
		None
		"""
		super().__init__(cls_db=cls_db)
		CoreIngestion.__init__(self)
		ContentParser.__init__(self)

		self.logger = logger
		self.cls_db = cls_db
		self.cls_dir_files_management = DirFilesManagement()
		self.cls_dates_current = DatesCurrent()
		self.cls_create_log = CreateLog()
		self.cls_dates_br = DatesBRAnbima()
		self.date_ref = date_ref or self.cls_dates_br.add_working_days(
			self.cls_dates_current.curr_date(), -1
		)
		self.url = "FILL_ME"

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "<COUNTRY>_<SOURCE>_<TABLE_NAME>",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Whether to verify the SSL certificate, by default True.
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_table_name : str
			The name of the table, by default '<COUNTRY>_<SOURCE>_<TABLE_NAME>'.

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
		file = self.parse_raw_file(resp_req)
		df_ = self.transform_data(file=file)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"COL_1": str,
				"COL_2": float,
				"COL_3": int,
				"COL_4": "date",
			},
			str_fmt_dt="YYYY-MM-DD",
			url=self.url,
		)
		if self.cls_db:
			self.insert_table_db(
				cls_db=self.cls_db,
				str_table_name=str_table_name,
				df_=df_,
				bool_insert_or_ignore=bool_insert_or_ignore,
			)
		else:
			return df_

	@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=60)
	def get_response(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Return a list of response objects.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Verify the SSL certificate, by default True.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			A list of response objects.
		"""
		resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
		resp_req.raise_for_status()
		return resp_req

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> StringIO:
		"""Parse the raw file content.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response object.

		Returns
		-------
		StringIO
			The parsed content.
		"""
		return self.get_file(resp_req=resp_req)

	def transform_data(self, file: StringIO) -> pd.DataFrame:
		"""Transform a list of response objects into a DataFrame.

		Parameters
		----------
		file : StringIO
			The parsed content.

		Returns
		-------
		pd.DataFrame
			The transformed DataFrame.
		"""
		return pd.read_csv(file, sep=";")
```

## Method Ordering: Top-Down Call Order

Within every class, methods must be ordered so that the **caller appears above the callee**. This means `run` (the entry point) comes first, then `get_response`, then `parse_raw_file`, then `transform_data`.

Dunder methods (`__init__`, `__repr__`, `__eq__`, etc.) go first as the class preamble. Everything after follows call order — no exceptions. Validation helpers (`_validate_*`) follow the same rule: each goes immediately before the earliest method that calls it.

## Code Style

- Line length: 99 characters; indentation: tabs (not spaces) — enforced by `ruff.toml` (`indent-style = "tab"`)
- Double quotes everywhere; single quotes only inside docstrings
- Docstrings: NumPy style, 79-char line limit, imperative mood, period at end of first line
- **Docstring parameter types must exactly match the Python type annotation** — never append `, optional`; write `int`, not `int, optional`; write `Optional[date]`, not `Optional[date], optional`. The `, optional` NumPy convention is not supported by this project's type-consistency checker.
- Type hints mandatory on all signatures; use `Optional[X]` not `X | None` (Python 3.9 compat)
- Use primitive collection types (`list`, `dict`, `tuple`) — no `typing.List/Dict/Tuple`
- Use `TypedDict` subclass named `Return<MethodName>` for dict return types
- Use `Literal` for parameters with a fixed set of allowed values
- Validation methods that span multiple other methods must be named `_validate_<name>`
- No commented-out code; no implicit `Any`

## Tests

- File: `tests/unit/test_<exact_module_filename>.py` — no duplicate filenames allowed
- Cover: `__init__`, `run` (with and without `cls_db`), `get_response` (success, HTTP error, timeout variants), `parse_raw_file` (valid + invalid types), `transform_data` (normal + empty input), and module reload
- Mock all network calls (`requests.get`, Selenium, Playwright) — never make real HTTP requests in unit tests
- Mock `backoff.on_exception` to bypass retry delays
- For non-`Optional` parameters, assert that passing the wrong type raises `TypeError` with a message matching `"must be of type"`
- Section headers in test files: `# --------------------------` comment blocks separating `Fixtures` from `Tests`
- Use `pytest.fixture` for reusable instances; use `@pytest.mark.parametrize` for input variations

## B3 BDI API Conventions

### Date fields include a time component

B3 BDI endpoints (`https://arquivos.b3.com.br/bdi/table/…`) return date fields as ISO 8601
datetime strings — e.g. `"2026-04-17T00:00:00"` — even when the time is always midnight.

Use `str_fmt_dt="YYYY-MM-DDTHH:MM:SS"` **only when the API actually returns datetime
strings** (e.g. `"2026-04-17T00:00:00"`). If the endpoint returns plain dates (`"2026-04-17"`),
keep `str_fmt_dt="YYYY-MM-DD"`. Mixing the two will raise
`RuntimeError: Invalid date string … for format …`.

```python
df_ = self.standardize_dataframe(
    df_=df_,
    date_ref=self.date_ref,
    dict_dtypes={
        "DT_REF": "date",   # BDI returns "2026-04-17T00:00:00"
        "TCKR_SYMB": str,
    },
    str_fmt_dt="YYYY-MM-DDTHH:MM:SS",   # required for BDI — NOT "YYYY-MM-DD"
    url=None,
)
```

### `StrHandler.convert_case` and all-caps prefixes

`StrHandler.convert_case(name, "pascal", "upper_constant")` splits only on
**lowercase → uppercase** character transitions (`re.sub(r"([a-z])([A-Z])", …)`).

Column names whose PascalCase prefix is all-caps (e.g. `BRL`, `BTB`, `ADR`) produce **no
underscore separator** between the acronym and the following word:

| API column name | `convert_case` result |
|---|---|
| `BRLValue` | `BRLVALUE` (**not** `BRL_VALUE`) |
| `BTBContractQuantity` | `BTBCONTRACT_QUANTITY` (**not** `BTB_CONTRACT_QUANTITY`) |
| `BTBTrade` | `BTBTRADE` (**not** `BTB_TRADE`) |

**Always verify the actual converted name** before writing `dict_dtypes` keys or DataFrame
column references. The safest approach is to call `transform_data` against a real or mocked
API response and inspect `df_.columns`.

## Final Verification

After implementing or refactoring any ingestion module, run:

```bash
make test_feat MODULE=<module_filename_without_extension>
make lint
```

**`make test_feat` runs `ruff check` (lint + isort) but NOT `ruff format`.** Always run `make lint` afterwards — it is the only step that runs `ruff format` and will reformat files if the code was not written with tabs. `make lint` must report "0 files reformatted" before the task is complete.

Common issues caught by this recipe:

- Unused imports (`F401`)
- `.values` instead of `.to_numpy()` (`PD011`)
- Docstring type not matching type hint exactly (quote style) — **never append `, optional`** to a parameter's type field; the type string must be the literal Python annotation (`int`, `Optional[date]`, etc.) and nothing more
- `Literal` types in docstrings must use single quotes to match Python's repr

**When refactoring a module that was split into multiple files**, run `make test_feat` for **each** new module name to verify every file passes independently. Do not skip this step — ruff and type-check errors are only caught per-module, not globally.
