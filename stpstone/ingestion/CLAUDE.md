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

If the source genuinely does not fit any of these, create a new subfolder with a lowercase, underscore-separated name and add an `__init__.py`.

## File Naming

- Lowercase words separated by underscores: `b3_futures_closing_adj.py`
- No duplicate filenames anywhere under `stpstone/ingestion/`
- Test file name must mirror the module exactly: `tests/unit/test_b3_futures_closing_adj.py`

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
        date_ref : Optional[date], optional
            The date of reference, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
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
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
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
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0).
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default True.
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False.
        str_table_name : str, optional
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
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0).
        bool_verify : bool, optional
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

Within every class, methods must be ordered so that the **caller appears above the callee**. This means `run` (the entry point) comes first, then `get_response`, then `parse_raw_file`, then `transform_data`. Validation helpers (`_validate_*`) go immediately before the method that calls them, or at the bottom of the class if shared.

This is not the same as the Dependency Inversion Principle (which is about depending on abstractions). The correct term for this rule is **top-down reading order** — reading the file from top to bottom follows the logical execution flow.

## Code Style

Full rules are in `prompts/refactoring.md`. Key points for ingestion modules:

- Line length: 99 characters; indentation: 4 spaces (no tabs)
- Double quotes everywhere; single quotes only inside docstrings
- Docstrings: NumPy style, 79-char line limit, imperative mood, period at end of first line
- Type hints mandatory on all signatures; use `Optional[X]` not `X | None` (Python 3.9 compat)
- Use primitive collection types (`list`, `dict`, `tuple`) — no `typing.List/Dict/Tuple`
- Use `TypedDict` subclass named `Return<MethodName>` for dict return types
- Use `Literal` for parameters with a fixed set of allowed values
- Validation methods that span multiple other methods must be named `_validate_<name>`
- No commented-out code; no implicit `Any`

## Tests

Full rules are in `prompts/unit_test.md`. Key points:

- File: `tests/unit/test_<exact_module_filename>.py` — no duplicate filenames allowed
- Cover: `__init__`, `run` (with and without `cls_db`), `get_response` (success, HTTP error, timeout variants), `parse_raw_file` (valid + invalid types), `transform_data` (normal + empty input), and module reload
- Mock all network calls (`requests.get`, Selenium, Playwright) — never make real HTTP requests in unit tests
- Mock `backoff.on_exception` to bypass retry delays
- For non-`Optional` parameters, assert that passing the wrong type raises `TypeError` with a message matching `"must be of type"`
- Section headers in test files: `# --------------------------` comment blocks separating `Fixtures` from `Tests`
- Use `pytest.fixture` for reusable instances; use `@pytest.mark.parametrize` for input variations
