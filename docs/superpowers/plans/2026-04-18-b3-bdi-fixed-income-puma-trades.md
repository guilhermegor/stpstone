# B3 BDI Fixed Income Puma Trades Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `B3BdiFixedIncomePumaTrades` ingestion class that fetches the B3 BDI `DebenturesBusiness` table (fixed income securities traded on the Puma platform) via paginated POST requests and returns a typed DataFrame.

**Architecture:** One class per file following the established BDI paginated-POST pattern; implementation, unit tests, and example runner created together. The class extends `ABCIngestionOperations` and follows the exact same method ordering and pagination loop as `B3BdiFixedIncomeConsolidatedTrades`.

**Tech Stack:** Python 3.10+, requests, backoff, pandas, pytest, pytest-mock, ruff, codespell.

---

## File Map

| Role | Path |
|------|------|
| Implementation | `stpstone/ingestion/countries/br/exchange/b3_bdi_fixed_income_puma_trades.py` |
| Unit tests | `tests/unit/test_b3_bdi_fixed_income_puma_trades.py` |
| Example runner | `examples/b3_bdi_fixed_income_puma_trades.py` |

---

### Task 1: Write failing unit tests

**Files:**
- Create: `tests/unit/test_b3_bdi_fixed_income_puma_trades.py`

- [ ] **Step 1: Create the test file**

```python
"""Unit tests for B3BdiFixedIncomePumaTrades class."""

from datetime import date
from logging import Logger
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
import requests
from requests import Response

from stpstone.ingestion.countries.br.exchange.b3_bdi_fixed_income_puma_trades import (
	B3BdiFixedIncomePumaTrades,
)
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_date() -> date:
	"""Fixture providing a sample date.

	Returns
	-------
	date
		Sample date object.
	"""
	return date(2026, 4, 17)


@pytest.fixture
def instance(sample_date: date) -> B3BdiFixedIncomePumaTrades:
	"""Fixture providing a B3BdiFixedIncomePumaTrades instance.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	B3BdiFixedIncomePumaTrades
		Initialized instance.
	"""
	return B3BdiFixedIncomePumaTrades(date_ref=sample_date)


@pytest.fixture
def sample_table_dict() -> dict:
	"""Fixture providing a sample API table dict with one row.

	Returns
	-------
	dict
		Sample table dict mimicking the BDI DebenturesBusiness API response.
	"""
	return {
		"columns": [
			{"name": "RptDt"},
			{"name": "TckrSymb"},
			{"name": "Company"},
			{"name": "Opening"},
			{"name": "Minimum"},
			{"name": "Maximum"},
			{"name": "Average"},
			{"name": "Final"},
			{"name": "ClosRefPric"},
			{"name": "PurchaseOffer"},
			{"name": "SaleOffer"},
			{"name": "Quant"},
			{"name": "Neg"},
			{"name": "Volume"},
		],
		"values": [
			[
				"2026-04-17T00:00:00",
				"B5MB11",
				"BRAD IMA-B5MF11",
				133.46,
				132.33,
				133.46,
				132.70,
				132.53,
				132.05,
				132.52,
				133.51,
				1632,
				132,
				216559.69,
			],
		],
	}


@pytest.fixture
def empty_table_dict() -> dict:
	"""Fixture providing a table dict with empty values.

	Returns
	-------
	dict
		Sample table dict with empty values list.
	"""
	return {
		"columns": [{"name": "RptDt"}, {"name": "TckrSymb"}],
		"values": [],
	}


@pytest.fixture
def mock_response(sample_table_dict: dict) -> Response:
	"""Mock Response with one-row table payload.

	Parameters
	----------
	sample_table_dict : dict
		Sample table dict fixture.

	Returns
	-------
	Response
		Mocked Response object.
	"""
	response = MagicMock(spec=Response)
	response.status_code = 200
	response.json.return_value = {"table": sample_table_dict}
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def mock_empty_response(empty_table_dict: dict) -> Response:
	"""Mock Response with empty-values table payload.

	Parameters
	----------
	empty_table_dict : dict
		Empty table dict fixture.

	Returns
	-------
	Response
		Mocked Response object.
	"""
	response = MagicMock(spec=Response)
	response.status_code = 200
	response.json.return_value = {"table": empty_table_dict}
	response.raise_for_status = MagicMock()
	return response


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(sample_date: date) -> None:
	"""Test initialization with all parameters provided.

	Verifies
	--------
	- date_ref, int_page_size, url_tpl are set correctly.
	- Standard helpers are initialized.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	inst = B3BdiFixedIncomePumaTrades(date_ref=sample_date, int_page_size=500)
	assert inst.date_ref == sample_date
	assert inst.int_page_size == 500
	assert "2026-04-17" in inst.url_tpl
	assert "{page}" in inst.url_tpl
	assert "500" in inst.url_tpl
	assert "DebenturesBusiness" in inst.url_tpl
	assert inst.logger is None
	assert isinstance(inst.cls_dir_files_management, DirFilesManagement)
	assert isinstance(inst.cls_dates_br, DatesBRAnbima)
	assert isinstance(inst.cls_create_log, CreateLog)


def test_init_default_page_size(sample_date: date) -> None:
	"""Test that default int_page_size is 1_000.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	inst = B3BdiFixedIncomePumaTrades(date_ref=sample_date)
	assert inst.int_page_size == 1_000


def test_init_without_date_ref(mocker: MockerFixture) -> None:
	"""Test initialization without date_ref defaults to previous working day.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch.object(DatesBRAnbima, "add_working_days", return_value=date(2026, 4, 16))
	inst = B3BdiFixedIncomePumaTrades()
	assert inst.date_ref == date(2026, 4, 16)


def test_init_logger_propagated() -> None:
	"""Test that logger is stored on the instance.

	Returns
	-------
	None
	"""
	mock_logger = MagicMock(spec=Logger)
	inst = B3BdiFixedIncomePumaTrades(date_ref=date(2026, 4, 17), logger=mock_logger)
	assert inst.logger is mock_logger


def test_get_response_success(
	instance: B3BdiFixedIncomePumaTrades, mocker: MockerFixture
) -> None:
	"""Test get_response posts to the correct URL and returns the response.

	Parameters
	----------
	instance : B3BdiFixedIncomePumaTrades
		Initialized instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.raise_for_status = MagicMock()
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mock_post = mocker.patch("requests.post", return_value=mock_resp)

	result = instance.get_response(int_page=3)

	expected_url = instance.url_tpl.format(page=3)
	mock_post.assert_called_once_with(expected_url, json={}, timeout=(12.0, 21.0), verify=True)
	assert result is mock_resp
	mock_resp.raise_for_status.assert_called_once()


def test_get_response_http_error(
	instance: B3BdiFixedIncomePumaTrades, mocker: MockerFixture
) -> None:
	"""Test get_response raises HTTPError on bad status.

	Parameters
	----------
	instance : B3BdiFixedIncomePumaTrades
		Initialized instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch("requests.post", side_effect=requests.exceptions.HTTPError("500 Server Error"))
	with pytest.raises(requests.exceptions.HTTPError):
		instance.get_response()


def test_parse_raw_file_returns_table(
	instance: B3BdiFixedIncomePumaTrades,
	sample_table_dict: dict,
) -> None:
	"""Test parse_raw_file extracts the table dict from the JSON response.

	Parameters
	----------
	instance : B3BdiFixedIncomePumaTrades
		Initialized instance.
	sample_table_dict : dict
		Expected table dict.

	Returns
	-------
	None
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.json.return_value = {"table": sample_table_dict}
	result = instance.parse_raw_file(mock_resp)
	assert result == sample_table_dict


def test_transform_data_normal(
	instance: B3BdiFixedIncomePumaTrades, sample_table_dict: dict
) -> None:
	"""Test transform_data builds a DataFrame with UPPER_SNAKE columns.

	Parameters
	----------
	instance : B3BdiFixedIncomePumaTrades
		Initialized instance.
	sample_table_dict : dict
		Sample table dict with one row.

	Returns
	-------
	None
	"""
	df_ = instance.transform_data(sample_table_dict)
	assert isinstance(df_, pd.DataFrame)
	assert len(df_) == 1
	assert list(df_.columns) == [
		"RPT_DT",
		"TCKR_SYMB",
		"COMPANY",
		"OPENING",
		"MINIMUM",
		"MAXIMUM",
		"AVERAGE",
		"FINAL",
		"CLOS_REF_PRIC",
		"PURCHASE_OFFER",
		"SALE_OFFER",
		"QUANT",
		"NEG",
		"VOLUME",
	]
	assert df_["TCKR_SYMB"].iloc[0] == "B5MB11"
	assert df_["OPENING"].iloc[0] == pytest.approx(133.46)
	assert df_["VOLUME"].iloc[0] == pytest.approx(216559.69)


def test_transform_data_empty_values(
	instance: B3BdiFixedIncomePumaTrades, empty_table_dict: dict
) -> None:
	"""Test transform_data returns empty DataFrame when values list is empty.

	Parameters
	----------
	instance : B3BdiFixedIncomePumaTrades
		Initialized instance.
	empty_table_dict : dict
		Table dict with empty values.

	Returns
	-------
	None
	"""
	df_ = instance.transform_data(empty_table_dict)
	assert isinstance(df_, pd.DataFrame)
	assert df_.empty


def test_run_without_db_paginates(
	instance: B3BdiFixedIncomePumaTrades,
	mock_response: Response,
	mock_empty_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run fetches pages until an empty response, then returns a DataFrame.

	Pages: page 1 returns one row, page 2 returns empty -> loop stops after page 1.

	Parameters
	----------
	instance : B3BdiFixedIncomePumaTrades
		Initialized instance.
	mock_response : Response
		Mocked Response with one data row.
	mock_empty_response : Response
		Mocked Response with empty values.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch.object(
		instance,
		"get_response",
		side_effect=[mock_response, mock_empty_response],
	)
	mocker.patch.object(
		instance,
		"standardize_dataframe",
		side_effect=lambda df_, **kw: df_,
	)

	result = instance.run()

	assert isinstance(result, pd.DataFrame)
	assert len(result) == 1
	assert "TCKR_SYMB" in result.columns
	assert "URL" in result.columns


def test_run_with_db(
	instance: B3BdiFixedIncomePumaTrades,
	mock_response: Response,
	mock_empty_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run inserts into DB and returns None when cls_db is set.

	Parameters
	----------
	instance : B3BdiFixedIncomePumaTrades
		Initialized instance.
	mock_response : Response
		Mocked Response with one data row.
	mock_empty_response : Response
		Mocked Response with empty values.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	instance.cls_db = MagicMock()
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch.object(
		instance,
		"get_response",
		side_effect=[mock_response, mock_empty_response],
	)
	mocker.patch.object(instance, "standardize_dataframe", side_effect=lambda df_, **kw: df_)
	mock_insert = mocker.patch.object(instance, "insert_table_db")

	result = instance.run()

	assert result is None
	mock_insert.assert_called_once()


def test_run_no_data_returns_none(
	instance: B3BdiFixedIncomePumaTrades,
	mock_empty_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run returns None when the first page is already empty.

	Parameters
	----------
	instance : B3BdiFixedIncomePumaTrades
		Initialized instance.
	mock_empty_response : Response
		Mocked Response with empty values.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch.object(instance, "get_response", return_value=mock_empty_response)

	result = instance.run()

	assert result is None


@pytest.mark.parametrize(
	"timeout",
	[10, 10.5, (10.0, 20.0), (10, 20)],
)
def test_get_response_timeout_variants(
	instance: B3BdiFixedIncomePumaTrades,
	mocker: MockerFixture,
	timeout: int | float | tuple,
) -> None:
	"""Test get_response accepts various timeout types.

	Parameters
	----------
	instance : B3BdiFixedIncomePumaTrades
		Initialized instance.
	mocker : MockerFixture
		Pytest-mock fixture.
	timeout : int | float | tuple
		Various timeout values.

	Returns
	-------
	None
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.raise_for_status = MagicMock()
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mock_post = mocker.patch("requests.post", return_value=mock_resp)

	instance.get_response(timeout=timeout)

	_, kwargs = mock_post.call_args
	assert kwargs["timeout"] == timeout
```

- [ ] **Step 2: Run tests to confirm they all fail (ImportError expected)**

```bash
poetry run pytest tests/unit/test_b3_bdi_fixed_income_puma_trades.py -v 2>&1 | head -30
```

Expected: `ImportError` or `ModuleNotFoundError` — the module does not exist yet.

- [ ] **Step 3: Commit the failing tests**

```bash
git add tests/unit/test_b3_bdi_fixed_income_puma_trades.py
git commit -m "test(bdi-b3): add failing unit tests for B3BdiFixedIncomePumaTrades

Tests cover __init__, run (with/without db, empty data), get_response
(success, HTTPError, timeout variants), parse_raw_file, transform_data."
```

---

### Task 2: Implement the ingestion class

**Files:**
- Create: `stpstone/ingestion/countries/br/exchange/b3_bdi_fixed_income_puma_trades.py`

- [ ] **Step 1: Create the implementation file**

```python
"""B3 BDI Fixed Income Puma Trades ingestion."""

from datetime import date
from logging import Logger
import time
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
from stpstone.utils.parsers.str import StrHandler


class B3BdiFixedIncomePumaTrades(ABCIngestionOperations):
	"""B3 BDI Fixed Income Puma Trades ingestion class."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
		int_page_size: int = 1_000,
		int_page_min: int = 1,
		int_page_max: Optional[int] = None,
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
		int_page_size : int, optional
			Number of records per page, by default 1_000.
		int_page_min : int, optional
			First page to fetch (1-based), by default 1.
		int_page_max : Optional[int], optional
			Last page to fetch inclusive; None means fetch until the API
			returns an empty result, by default None.

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
		self.int_page_size = int_page_size
		self.int_page_min = int_page_min
		self.int_page_max = int_page_max
		str_date = self.date_ref.strftime("%Y-%m-%d")
		self.url_tpl = (
			f"https://arquivos.b3.com.br/bdi/table/DebenturesBusiness/"
			f"{str_date}/{str_date}/{{page}}/{self.int_page_size}"
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_bdi_fixed_income_puma_trades",
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
			The name of the table, by default "br_b3_bdi_fixed_income_puma_trades".

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame, or None when writing to database.
		"""
		int_page = self.int_page_min
		list_dfs: list[pd.DataFrame] = []
		while True:
			resp_req = self.get_response(
				int_page=int_page, timeout=timeout, bool_verify=bool_verify
			)
			data = self.parse_raw_file(resp_req)
			df_page = self.transform_data(data)
			if df_page.empty:
				break
			self._log_page_progress(int_page, len(df_page))
			df_page["URL"] = self.url_tpl.format(page=int_page)
			list_dfs.append(df_page)
			if self.int_page_max is not None and int_page >= self.int_page_max:
				break
			int_page += 1
			time.sleep(2)
		if not list_dfs:
			return None
		df_ = pd.concat(list_dfs, ignore_index=True)
		dict_dtypes = {
			"RPT_DT": "date",
			"TCKR_SYMB": str,
			"COMPANY": str,
			"OPENING": float,
			"MINIMUM": float,
			"MAXIMUM": float,
			"AVERAGE": float,
			"FINAL": float,
			"CLOS_REF_PRIC": float,
			"PURCHASE_OFFER": float,
			"SALE_OFFER": float,
			"QUANT": int,
			"NEG": int,
			"VOLUME": float,
			"URL": str,
		}
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes=dict_dtypes,
			str_fmt_dt="YYYY-MM-DDTHH:MM:SS",
			url=None,
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

	def _log_page_progress(self, int_page: int, int_rows: int) -> None:
		"""Emit a progress message for the current page.

		Parameters
		----------
		int_page : int
			The page number just fetched.
		int_rows : int
			Number of rows returned on that page.

		Returns
		-------
		None
		"""
		self.cls_create_log.log_message(
			logger=self.logger,
			message=(f"B3BdiFixedIncomePumaTrades: page {int_page} fetched ({int_rows} rows)"),
			log_level="info",
		)

	@backoff.on_exception(
		backoff.expo,
		(
			requests.exceptions.HTTPError,
			requests.exceptions.Timeout,
			requests.exceptions.ConnectionError,
		),
		max_tries=5,
		factor=2,
		max_time=120,
	)
	def get_response(
		self,
		int_page: int = 1,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Return a response object for the given page.

		Parameters
		----------
		int_page : int, optional
			The page number to fetch, by default 1.
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
			The timeout, by default (12.0, 21.0).
		bool_verify : bool, optional
			Verify the SSL certificate, by default True.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			A response object.
		"""
		url = self.url_tpl.format(page=int_page)
		resp_req = requests.post(url, json={}, timeout=timeout, verify=bool_verify)
		resp_req.raise_for_status()
		return resp_req

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> dict:
		"""Parse the JSON response into the table metadata dict.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response object.

		Returns
		-------
		dict
			The table object containing columns and values from the API response.
		"""
		return resp_req.json()["table"]

	def transform_data(self, data: dict) -> pd.DataFrame:
		"""Build a DataFrame from the API table dict.

		Column names are converted from PascalCase to UPPER_SNAKE_CASE.

		Parameters
		----------
		data : dict
			The table dict with keys 'columns' and 'values'.

		Returns
		-------
		pd.DataFrame
			DataFrame with UPPER_SNAKE_CASE column names, or empty DataFrame
			when values is an empty list.
		"""
		values = data.get("values", [])
		if not values:
			return pd.DataFrame()
		str_handler = StrHandler()
		col_names = [
			str_handler.convert_case(col["name"], "pascal", "upper_constant")
			for col in data.get("columns", [])
		]
		df_ = pd.DataFrame(values)
		rename_map = {i: name for i, name in enumerate(col_names)}
		return df_.rename(columns=rename_map)
```

- [ ] **Step 2: Run the tests — all should pass**

```bash
poetry run pytest tests/unit/test_b3_bdi_fixed_income_puma_trades.py -v
```

Expected: all tests PASS.

- [ ] **Step 3: Commit implementation**

```bash
git add stpstone/ingestion/countries/br/exchange/b3_bdi_fixed_income_puma_trades.py
git commit -m "feat(bdi-b3): Add B3 BDI fixed income Puma trades ingestion

Ingests DebenturesBusiness table from the B3 BDI API via paginated POST.
Covers open/min/max/avg/last prices, buy/sell offers, quantity, trades,
and volume for debentures traded on the Puma electronic platform."
```

---

### Task 3: Add example runner

**Files:**
- Create: `examples/b3_bdi_fixed_income_puma_trades.py`

- [ ] **Step 1: Create the example file**

```python
"""B3 BDI fixed income securities trades on the Puma platform."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_fixed_income_puma_trades import (
	B3BdiFixedIncomePumaTrades,
)


cls_ = B3BdiFixedIncomePumaTrades(
	date_ref=None,
	logger=None,
	cls_db=None,
	int_page_min=1,
	int_page_max=5,
)

df_ = cls_.run()
print(f"DF B3 BDI FIXED INCOME PUMA TRADES: \n{df_}")
df_.info()
```

- [ ] **Step 2: Commit the example**

```bash
git add examples/b3_bdi_fixed_income_puma_trades.py
git commit -m "docs(bdi-b3): add example runner for B3BdiFixedIncomePumaTrades"
```

---

### Task 4: Full verification

- [ ] **Step 1: Run the full test-feat recipe**

```bash
make test_feat MODULE=b3_bdi_fixed_income_puma_trades
```

Expected: codespell, ruff check, ruff format, type hints, docstring checks, and pytest all PASS with no errors.

- [ ] **Step 2: Fix any ruff/codespell/type issues surfaced and re-run until clean**

Common issues to watch for:
- `PD011`: replace `.values` with `.to_numpy()` if used
- `F401`: remove any unused imports
- Docstring type annotations must match signature exactly (e.g., `Optional[int]` not `int | None`)
- `Literal` types in docstrings use single quotes

- [ ] **Step 3: Commit fixes if any**

```bash
git add stpstone/ingestion/countries/br/exchange/b3_bdi_fixed_income_puma_trades.py \
        tests/unit/test_b3_bdi_fixed_income_puma_trades.py
git commit -m "fix(bdi-b3): address ruff/type/docstring issues in Puma trades module"
```
