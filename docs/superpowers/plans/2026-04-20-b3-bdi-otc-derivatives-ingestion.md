# B3 BDI OTC Derivatives Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add three ingestion classes (`B3BdiDerivativesInventoryCcp`, `B3BdiDerivativesInventoryWccp`, `B3BdiDerivativesOptionsFlex`) that consume the B3 BDI OTC derivatives JSON API and return tidy DataFrames.

**Architecture:** Each class extends `ABCIngestionOperations` with `POST json={}` pagination, PascalCase→UPPER_SNAKE column renaming via `StrHandler`, and a trailing-null-column drop in `transform_data`. CCP/WCCP default to single-page; FlexibleOptions paginates until the API returns an empty values list.

**Tech Stack:** Python 3.12+, requests, backoff, pandas, pytest, pytest-mock, stpstone internal ABCIngestionOperations / StrHandler / DatesBRAnbima.

---

## File Map

| Role | Path |
|------|------|
| CCP implementation | `stpstone/ingestion/countries/br/otc/b3_bdi_derivatives_inventory_ccp.py` |
| CCP tests | `tests/unit/test_b3_bdi_derivatives_inventory_ccp.py` |
| CCP example | `examples/b3_bdi_derivatives_inventory_ccp.py` |
| WCCP implementation | `stpstone/ingestion/countries/br/otc/b3_bdi_derivatives_inventory_wccp.py` |
| WCCP tests | `tests/unit/test_b3_bdi_derivatives_inventory_wccp.py` |
| WCCP example | `examples/b3_bdi_derivatives_inventory_wccp.py` |
| Flex implementation | `stpstone/ingestion/countries/br/otc/b3_bdi_derivatives_options_flex.py` |
| Flex tests | `tests/unit/test_b3_bdi_derivatives_options_flex.py` |
| Flex example | `examples/b3_bdi_derivatives_options_flex.py` |

---

## Task 1: CCP — Write Failing Tests

**Files:**
- Create: `tests/unit/test_b3_bdi_derivatives_inventory_ccp.py`

> **Style:** Use **tab** characters for indentation (not spaces). Double quotes throughout.

- [ ] **Step 1.1: Create the test file**

```python
"""Unit tests for B3BdiDerivativesInventoryCcp class."""

from datetime import date
from logging import Logger
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
import requests
from requests import Response

from stpstone.ingestion.countries.br.otc.b3_bdi_derivatives_inventory_ccp import (
	B3BdiDerivativesInventoryCcp,
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
def instance(sample_date: date) -> B3BdiDerivativesInventoryCcp:
	"""Fixture providing a B3BdiDerivativesInventoryCcp instance.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	B3BdiDerivativesInventoryCcp
		Initialized instance.
	"""
	return B3BdiDerivativesInventoryCcp(date_ref=sample_date)


@pytest.fixture
def sample_table_dict() -> dict:
	"""Fixture providing a sample API table dict with one row.

	Returns
	-------
	dict
		Sample table dict mimicking the BDI OTCInventoryCCP API response.
	"""
	return {
		"columns": [
			{"name": "TckrSymb"},
			{"name": "TradQty"},
			{"name": "FinancialVol"},
		],
		"values": [
			["Opção", 506105, 91603180637.66, None],
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
		"columns": [
			{"name": "TckrSymb"},
			{"name": "TradQty"},
			{"name": "FinancialVol"},
		],
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

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	inst = B3BdiDerivativesInventoryCcp(date_ref=sample_date, int_page_size=500)
	assert inst.date_ref == sample_date
	assert inst.int_page_size == 500
	assert "2026-04-17" in inst.url_tpl
	assert "{page}" in inst.url_tpl
	assert "500" in inst.url_tpl
	assert "OTCInventoryCCP" in inst.url_tpl
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
	inst = B3BdiDerivativesInventoryCcp(date_ref=sample_date)
	assert inst.int_page_size == 1_000


def test_init_default_page_max(sample_date: date) -> None:
	"""Test that default int_page_max is 1.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	inst = B3BdiDerivativesInventoryCcp(date_ref=sample_date)
	assert inst.int_page_max == 1


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
	inst = B3BdiDerivativesInventoryCcp()
	assert inst.date_ref == date(2026, 4, 16)


def test_init_logger_propagated() -> None:
	"""Test that logger is stored on the instance.

	Returns
	-------
	None
	"""
	mock_logger = MagicMock(spec=Logger)
	inst = B3BdiDerivativesInventoryCcp(date_ref=date(2026, 4, 17), logger=mock_logger)
	assert inst.logger is mock_logger


def test_get_response_success(
	instance: B3BdiDerivativesInventoryCcp,
	mocker: MockerFixture,
) -> None:
	"""Test get_response posts to the correct URL and returns the response.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryCcp
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

	result = instance.get_response(int_page=1)

	expected_url = instance.url_tpl.format(page=1)
	mock_post.assert_called_once_with(expected_url, json={}, timeout=(12.0, 21.0), verify=True)
	assert result is mock_resp
	mock_resp.raise_for_status.assert_called_once()


def test_get_response_http_error(
	instance: B3BdiDerivativesInventoryCcp,
	mocker: MockerFixture,
) -> None:
	"""Test get_response raises HTTPError on bad status.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryCcp
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


def test_get_response_timeout_error(
	instance: B3BdiDerivativesInventoryCcp,
	mocker: MockerFixture,
) -> None:
	"""Test get_response raises Timeout when the server does not respond.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryCcp
		Initialized instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch("requests.post", side_effect=requests.exceptions.Timeout("timed out"))
	with pytest.raises(requests.exceptions.Timeout):
		instance.get_response()


def test_get_response_connection_error(
	instance: B3BdiDerivativesInventoryCcp,
	mocker: MockerFixture,
) -> None:
	"""Test get_response raises ConnectionError when the host is unreachable.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryCcp
		Initialized instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch(
		"requests.post",
		side_effect=requests.exceptions.ConnectionError("connection refused"),
	)
	with pytest.raises(requests.exceptions.ConnectionError):
		instance.get_response()


def test_parse_raw_file_returns_table(
	instance: B3BdiDerivativesInventoryCcp,
	sample_table_dict: dict,
) -> None:
	"""Test parse_raw_file extracts the table dict from the JSON response.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryCcp
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


def test_parse_raw_file_missing_table_key(instance: B3BdiDerivativesInventoryCcp) -> None:
	"""Test parse_raw_file raises KeyError when 'table' key is absent.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryCcp
		Initialized instance.

	Returns
	-------
	None
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.json.return_value = {"unexpected": {}}
	with pytest.raises(KeyError):
		instance.parse_raw_file(mock_resp)


def test_transform_data_normal(
	instance: B3BdiDerivativesInventoryCcp,
	sample_table_dict: dict,
) -> None:
	"""Test transform_data builds a DataFrame with UPPER_SNAKE columns, drops trailing null.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryCcp
		Initialized instance.
	sample_table_dict : dict
		Sample table dict with one row (4 values, 3 columns).

	Returns
	-------
	None
	"""
	df_ = instance.transform_data(sample_table_dict)
	assert isinstance(df_, pd.DataFrame)
	assert len(df_) == 1
	assert list(df_.columns) == ["TCKR_SYMB", "TRAD_QTY", "FINANCIAL_VOL"]
	assert df_["TCKR_SYMB"].iloc[0] == "Opção"
	assert df_["TRAD_QTY"].iloc[0] == 506105
	assert df_["FINANCIAL_VOL"].iloc[0] == 91603180637.66


def test_transform_data_multiple_rows(instance: B3BdiDerivativesInventoryCcp) -> None:
	"""Test transform_data handles multiple rows correctly.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryCcp
		Initialized instance.

	Returns
	-------
	None
	"""
	table = {
		"columns": [
			{"name": "TckrSymb"},
			{"name": "TradQty"},
			{"name": "FinancialVol"},
		],
		"values": [
			["Opção", 506105, 91603180637.66, None],
			["Swap", 16730, 57428601893.84, None],
			["Termo", 185, 1363249405.39, None],
		],
	}
	df_ = instance.transform_data(table)
	assert len(df_) == 3
	assert set(df_["TCKR_SYMB"].tolist()) == {"Opção", "Swap", "Termo"}


def test_transform_data_empty_values(
	instance: B3BdiDerivativesInventoryCcp,
	empty_table_dict: dict,
) -> None:
	"""Test transform_data returns empty DataFrame when values list is empty.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryCcp
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


def test_run_without_db(
	instance: B3BdiDerivativesInventoryCcp,
	mock_response: Response,
	mock_empty_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run fetches pages until page_max reached, then returns a DataFrame.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryCcp
		Initialized instance.
	mock_response : Response
		Mocked Response with one data row.
	mock_empty_response : Response
		Mocked Response with empty values (unused since page_max=1 stops first).
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch("time.sleep")
	mocker.patch.object(instance, "get_response", return_value=mock_response)
	mocker.patch.object(instance, "standardize_dataframe", side_effect=lambda df_, **kw: df_)

	result = instance.run()

	assert isinstance(result, pd.DataFrame)
	assert len(result) == 1
	assert "TCKR_SYMB" in result.columns
	assert "URL" in result.columns


def test_run_with_db(
	instance: B3BdiDerivativesInventoryCcp,
	mock_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run inserts into DB and returns None when cls_db is set.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryCcp
		Initialized instance.
	mock_response : Response
		Mocked Response with one data row.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	instance.cls_db = MagicMock()
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch("time.sleep")
	mocker.patch.object(instance, "get_response", return_value=mock_response)
	mocker.patch.object(instance, "standardize_dataframe", side_effect=lambda df_, **kw: df_)
	mock_insert = mocker.patch.object(instance, "insert_table_db")

	result = instance.run()

	assert result is None
	mock_insert.assert_called_once()


def test_run_no_data_returns_none(
	instance: B3BdiDerivativesInventoryCcp,
	mock_empty_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run returns None when the first page is already empty.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryCcp
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
	instance: B3BdiDerivativesInventoryCcp,
	mocker: MockerFixture,
	timeout: int | float | tuple,
) -> None:
	"""Test get_response accepts various timeout types.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryCcp
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


def test_module_reload(sample_date: date) -> None:
	"""Test that the module can be reloaded and the class re-instantiated.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.ingestion.countries.br.otc.b3_bdi_derivatives_inventory_ccp as mod

	importlib.reload(mod)
	inst = mod.B3BdiDerivativesInventoryCcp(date_ref=sample_date)
	assert inst.date_ref == sample_date
	assert "OTCInventoryCCP" in inst.url_tpl
```

- [ ] **Step 1.2: Format the test file**

```bash
poetry run ruff format tests/unit/test_b3_bdi_derivatives_inventory_ccp.py
```

- [ ] **Step 1.3: Run tests — expect ImportError (module not yet created)**

```bash
poetry run pytest tests/unit/test_b3_bdi_derivatives_inventory_ccp.py -v 2>&1 | head -20
```

Expected: `ImportError` or `ModuleNotFoundError` — the implementation file does not exist yet.

---

## Task 2: CCP — Write Implementation + Example

**Files:**
- Create: `stpstone/ingestion/countries/br/otc/b3_bdi_derivatives_inventory_ccp.py`
- Create: `examples/b3_bdi_derivatives_inventory_ccp.py`

- [ ] **Step 2.1: Create the implementation file**

> Use **tab** indentation throughout.

```python
"""B3 BDI OTC derivatives inventory with CCP ingestion."""

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


class B3BdiDerivativesInventoryCcp(ABCIngestionOperations):
	"""B3 BDI OTC derivatives inventory with CCP ingestion class."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
		int_page_size: int = 1_000,
		int_page_min: int = 1,
		int_page_max: int = 1,
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
		int_page_max : int, optional
			Last page to fetch inclusive; defaults to 1 because this endpoint
			returns all inventory rows on a single page.

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
			f"https://arquivos.b3.com.br/bdi/table/OTCInventoryCCP/"
			f"{str_date}/{str_date}/{{page}}/{self.int_page_size}"
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_bdi_derivatives_inventory_ccp",
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
			The name of the table, by default
			"br_b3_bdi_derivatives_inventory_ccp".

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
			"TCKR_SYMB": str,
			"TRAD_QTY": int,
			"FINANCIAL_VOL": float,
			"URL": str,
		}
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes=dict_dtypes,
			str_fmt_dt="YYYY-MM-DD",
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
			message=(
				f"B3BdiDerivativesInventoryCcp: page {int_page} fetched ({int_rows} rows)"
			),
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
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
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
		"""Build a DataFrame from the API table dict, dropping the trailing null column.

		Column names are converted from PascalCase to UPPER_SNAKE_CASE. Each values
		row has 4 elements but only 3 column definitions — the 4th element is always
		null and is dropped by selecting only the named columns.

		Parameters
		----------
		data : dict
			The table dict with keys 'columns' and 'values'.

		Returns
		-------
		pd.DataFrame
			DataFrame with UPPER_SNAKE_CASE column names (3 columns), or empty
			DataFrame when values is an empty list.
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
		df_ = df_.rename(columns=rename_map)
		return df_[col_names]
```

- [ ] **Step 2.2: Format the implementation file**

```bash
poetry run ruff format stpstone/ingestion/countries/br/otc/b3_bdi_derivatives_inventory_ccp.py
```

- [ ] **Step 2.3: Create the example file**

```python
"""B3 BDI OTC derivatives inventory with CCP ingestion."""

from stpstone.ingestion.countries.br.otc.b3_bdi_derivatives_inventory_ccp import (
	B3BdiDerivativesInventoryCcp,
)


cls_ = B3BdiDerivativesInventoryCcp(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Derivatives Inventory CCP: \n{df_}")
df_.info()
```

- [ ] **Step 2.4: Format the example file**

```bash
poetry run ruff format examples/b3_bdi_derivatives_inventory_ccp.py
```

- [ ] **Step 2.5: Run tests — expect PASS**

```bash
make test_feat MODULE=b3_bdi_derivatives_inventory_ccp
```

Expected: all tests green, ruff lint passes, no import errors.

- [ ] **Step 2.6: Commit**

```bash
git add stpstone/ingestion/countries/br/otc/b3_bdi_derivatives_inventory_ccp.py \
        tests/unit/test_b3_bdi_derivatives_inventory_ccp.py \
        examples/b3_bdi_derivatives_inventory_ccp.py
git commit -m "feat(bdi-b3): Add B3 BDI OTC derivatives inventory with CCP ingestion"
```

---

## Task 3: WCCP — Write Failing Tests

**Files:**
- Create: `tests/unit/test_b3_bdi_derivatives_inventory_wccp.py`

> **Style:** Tab indentation, double quotes.

- [ ] **Step 3.1: Create the test file**

```python
"""Unit tests for B3BdiDerivativesInventoryWccp class."""

from datetime import date
from logging import Logger
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
import requests
from requests import Response

from stpstone.ingestion.countries.br.otc.b3_bdi_derivatives_inventory_wccp import (
	B3BdiDerivativesInventoryWccp,
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
def instance(sample_date: date) -> B3BdiDerivativesInventoryWccp:
	"""Fixture providing a B3BdiDerivativesInventoryWccp instance.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	B3BdiDerivativesInventoryWccp
		Initialized instance.
	"""
	return B3BdiDerivativesInventoryWccp(date_ref=sample_date)


@pytest.fixture
def sample_table_dict() -> dict:
	"""Fixture providing a sample API table dict with one row.

	Returns
	-------
	dict
		Sample table dict mimicking the BDI OTCInventoryWCCP API response.
	"""
	return {
		"columns": [
			{"name": "TckrSymb"},
			{"name": "TradQty"},
			{"name": "FinancialVol"},
		],
		"values": [
			["BOX", 208, 3640842373.43, None],
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
		"columns": [
			{"name": "TckrSymb"},
			{"name": "TradQty"},
			{"name": "FinancialVol"},
		],
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

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	inst = B3BdiDerivativesInventoryWccp(date_ref=sample_date, int_page_size=500)
	assert inst.date_ref == sample_date
	assert inst.int_page_size == 500
	assert "2026-04-17" in inst.url_tpl
	assert "{page}" in inst.url_tpl
	assert "500" in inst.url_tpl
	assert "OTCInventoryWCCP" in inst.url_tpl
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
	inst = B3BdiDerivativesInventoryWccp(date_ref=sample_date)
	assert inst.int_page_size == 1_000


def test_init_default_page_max(sample_date: date) -> None:
	"""Test that default int_page_max is 1.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	inst = B3BdiDerivativesInventoryWccp(date_ref=sample_date)
	assert inst.int_page_max == 1


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
	inst = B3BdiDerivativesInventoryWccp()
	assert inst.date_ref == date(2026, 4, 16)


def test_init_logger_propagated() -> None:
	"""Test that logger is stored on the instance.

	Returns
	-------
	None
	"""
	mock_logger = MagicMock(spec=Logger)
	inst = B3BdiDerivativesInventoryWccp(date_ref=date(2026, 4, 17), logger=mock_logger)
	assert inst.logger is mock_logger


def test_get_response_success(
	instance: B3BdiDerivativesInventoryWccp,
	mocker: MockerFixture,
) -> None:
	"""Test get_response posts to the correct URL and returns the response.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryWccp
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

	result = instance.get_response(int_page=1)

	expected_url = instance.url_tpl.format(page=1)
	mock_post.assert_called_once_with(expected_url, json={}, timeout=(12.0, 21.0), verify=True)
	assert result is mock_resp
	mock_resp.raise_for_status.assert_called_once()


def test_get_response_http_error(
	instance: B3BdiDerivativesInventoryWccp,
	mocker: MockerFixture,
) -> None:
	"""Test get_response raises HTTPError on bad status.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryWccp
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


def test_get_response_timeout_error(
	instance: B3BdiDerivativesInventoryWccp,
	mocker: MockerFixture,
) -> None:
	"""Test get_response raises Timeout when the server does not respond.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryWccp
		Initialized instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch("requests.post", side_effect=requests.exceptions.Timeout("timed out"))
	with pytest.raises(requests.exceptions.Timeout):
		instance.get_response()


def test_get_response_connection_error(
	instance: B3BdiDerivativesInventoryWccp,
	mocker: MockerFixture,
) -> None:
	"""Test get_response raises ConnectionError when the host is unreachable.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryWccp
		Initialized instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch(
		"requests.post",
		side_effect=requests.exceptions.ConnectionError("connection refused"),
	)
	with pytest.raises(requests.exceptions.ConnectionError):
		instance.get_response()


def test_parse_raw_file_returns_table(
	instance: B3BdiDerivativesInventoryWccp,
	sample_table_dict: dict,
) -> None:
	"""Test parse_raw_file extracts the table dict from the JSON response.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryWccp
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


def test_parse_raw_file_missing_table_key(instance: B3BdiDerivativesInventoryWccp) -> None:
	"""Test parse_raw_file raises KeyError when 'table' key is absent.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryWccp
		Initialized instance.

	Returns
	-------
	None
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.json.return_value = {"unexpected": {}}
	with pytest.raises(KeyError):
		instance.parse_raw_file(mock_resp)


def test_transform_data_normal(
	instance: B3BdiDerivativesInventoryWccp,
	sample_table_dict: dict,
) -> None:
	"""Test transform_data builds a DataFrame with UPPER_SNAKE columns, drops trailing null.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryWccp
		Initialized instance.
	sample_table_dict : dict
		Sample table dict with one row (4 values, 3 columns).

	Returns
	-------
	None
	"""
	df_ = instance.transform_data(sample_table_dict)
	assert isinstance(df_, pd.DataFrame)
	assert len(df_) == 1
	assert list(df_.columns) == ["TCKR_SYMB", "TRAD_QTY", "FINANCIAL_VOL"]
	assert df_["TCKR_SYMB"].iloc[0] == "BOX"
	assert df_["TRAD_QTY"].iloc[0] == 208
	assert df_["FINANCIAL_VOL"].iloc[0] == 3640842373.43


def test_transform_data_multiple_rows(instance: B3BdiDerivativesInventoryWccp) -> None:
	"""Test transform_data handles multiple rows correctly.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryWccp
		Initialized instance.

	Returns
	-------
	None
	"""
	table = {
		"columns": [
			{"name": "TckrSymb"},
			{"name": "TradQty"},
			{"name": "FinancialVol"},
		],
		"values": [
			["BOX", 208, 3640842373.43, None],
			["Opção Flexível", 291754, 622966620328.47, None],
			["Swap", 176301, 2585319245125.82, None],
			["Termo", 123737, 1146455340557.35, None],
		],
	}
	df_ = instance.transform_data(table)
	assert len(df_) == 4
	assert set(df_["TCKR_SYMB"].tolist()) == {"BOX", "Opção Flexível", "Swap", "Termo"}


def test_transform_data_empty_values(
	instance: B3BdiDerivativesInventoryWccp,
	empty_table_dict: dict,
) -> None:
	"""Test transform_data returns empty DataFrame when values list is empty.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryWccp
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


def test_run_without_db(
	instance: B3BdiDerivativesInventoryWccp,
	mock_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run fetches one page and returns a DataFrame.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryWccp
		Initialized instance.
	mock_response : Response
		Mocked Response with one data row.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch("time.sleep")
	mocker.patch.object(instance, "get_response", return_value=mock_response)
	mocker.patch.object(instance, "standardize_dataframe", side_effect=lambda df_, **kw: df_)

	result = instance.run()

	assert isinstance(result, pd.DataFrame)
	assert len(result) == 1
	assert "TCKR_SYMB" in result.columns
	assert "URL" in result.columns


def test_run_with_db(
	instance: B3BdiDerivativesInventoryWccp,
	mock_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run inserts into DB and returns None when cls_db is set.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryWccp
		Initialized instance.
	mock_response : Response
		Mocked Response with one data row.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	instance.cls_db = MagicMock()
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch("time.sleep")
	mocker.patch.object(instance, "get_response", return_value=mock_response)
	mocker.patch.object(instance, "standardize_dataframe", side_effect=lambda df_, **kw: df_)
	mock_insert = mocker.patch.object(instance, "insert_table_db")

	result = instance.run()

	assert result is None
	mock_insert.assert_called_once()


def test_run_no_data_returns_none(
	instance: B3BdiDerivativesInventoryWccp,
	mock_empty_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run returns None when the first page is already empty.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryWccp
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
	instance: B3BdiDerivativesInventoryWccp,
	mocker: MockerFixture,
	timeout: int | float | tuple,
) -> None:
	"""Test get_response accepts various timeout types.

	Parameters
	----------
	instance : B3BdiDerivativesInventoryWccp
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


def test_module_reload(sample_date: date) -> None:
	"""Test that the module can be reloaded and the class re-instantiated.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.ingestion.countries.br.otc.b3_bdi_derivatives_inventory_wccp as mod

	importlib.reload(mod)
	inst = mod.B3BdiDerivativesInventoryWccp(date_ref=sample_date)
	assert inst.date_ref == sample_date
	assert "OTCInventoryWCCP" in inst.url_tpl
```

- [ ] **Step 3.2: Format the test file**

```bash
poetry run ruff format tests/unit/test_b3_bdi_derivatives_inventory_wccp.py
```

- [ ] **Step 3.3: Run tests — expect ImportError**

```bash
poetry run pytest tests/unit/test_b3_bdi_derivatives_inventory_wccp.py -v 2>&1 | head -20
```

Expected: `ImportError` or `ModuleNotFoundError`.

---

## Task 4: WCCP — Write Implementation + Example

**Files:**
- Create: `stpstone/ingestion/countries/br/otc/b3_bdi_derivatives_inventory_wccp.py`
- Create: `examples/b3_bdi_derivatives_inventory_wccp.py`

- [ ] **Step 4.1: Create the implementation file**

> Use **tab** indentation throughout.

```python
"""B3 BDI OTC derivatives inventory without CCP ingestion."""

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


class B3BdiDerivativesInventoryWccp(ABCIngestionOperations):
	"""B3 BDI OTC derivatives inventory without CCP ingestion class."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
		int_page_size: int = 1_000,
		int_page_min: int = 1,
		int_page_max: int = 1,
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
		int_page_max : int, optional
			Last page to fetch inclusive; defaults to 1 because this endpoint
			returns all inventory rows on a single page.

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
			f"https://arquivos.b3.com.br/bdi/table/OTCInventoryWCCP/"
			f"{str_date}/{str_date}/{{page}}/{self.int_page_size}"
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_bdi_derivatives_inventory_wccp",
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
			The name of the table, by default
			"br_b3_bdi_derivatives_inventory_wccp".

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
			"TCKR_SYMB": str,
			"TRAD_QTY": int,
			"FINANCIAL_VOL": float,
			"URL": str,
		}
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes=dict_dtypes,
			str_fmt_dt="YYYY-MM-DD",
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
			message=(
				f"B3BdiDerivativesInventoryWccp: page {int_page} fetched ({int_rows} rows)"
			),
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
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
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
		"""Build a DataFrame from the API table dict, dropping the trailing null column.

		Column names are converted from PascalCase to UPPER_SNAKE_CASE. Each values
		row has 4 elements but only 3 column definitions — the 4th element is always
		null and is dropped by selecting only the named columns.

		Parameters
		----------
		data : dict
			The table dict with keys 'columns' and 'values'.

		Returns
		-------
		pd.DataFrame
			DataFrame with UPPER_SNAKE_CASE column names (3 columns), or empty
			DataFrame when values is an empty list.
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
		df_ = df_.rename(columns=rename_map)
		return df_[col_names]
```

- [ ] **Step 4.2: Format the implementation file**

```bash
poetry run ruff format stpstone/ingestion/countries/br/otc/b3_bdi_derivatives_inventory_wccp.py
```

- [ ] **Step 4.3: Create the example file**

```python
"""B3 BDI OTC derivatives inventory without CCP ingestion."""

from stpstone.ingestion.countries.br.otc.b3_bdi_derivatives_inventory_wccp import (
	B3BdiDerivativesInventoryWccp,
)


cls_ = B3BdiDerivativesInventoryWccp(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Derivatives Inventory WCCP: \n{df_}")
df_.info()
```

- [ ] **Step 4.4: Format the example file**

```bash
poetry run ruff format examples/b3_bdi_derivatives_inventory_wccp.py
```

- [ ] **Step 4.5: Run tests — expect PASS**

```bash
make test_feat MODULE=b3_bdi_derivatives_inventory_wccp
```

Expected: all tests green, ruff lint passes.

- [ ] **Step 4.6: Commit**

```bash
git add stpstone/ingestion/countries/br/otc/b3_bdi_derivatives_inventory_wccp.py \
        tests/unit/test_b3_bdi_derivatives_inventory_wccp.py \
        examples/b3_bdi_derivatives_inventory_wccp.py
git commit -m "feat(bdi-b3): Add B3 BDI OTC derivatives inventory without CCP ingestion"
```

---

## Task 5: FlexibleOptions — Write Failing Tests

**Files:**
- Create: `tests/unit/test_b3_bdi_derivatives_options_flex.py`

> **Style:** Tab indentation, double quotes.

- [ ] **Step 5.1: Create the test file**

```python
"""Unit tests for B3BdiDerivativesOptionsFlex class."""

from datetime import date
from logging import Logger
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
import requests
from requests import Response

from stpstone.ingestion.countries.br.otc.b3_bdi_derivatives_options_flex import (
	B3BdiDerivativesOptionsFlex,
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
def instance(sample_date: date) -> B3BdiDerivativesOptionsFlex:
	"""Fixture providing a B3BdiDerivativesOptionsFlex instance.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	B3BdiDerivativesOptionsFlex
		Initialized instance.
	"""
	return B3BdiDerivativesOptionsFlex(date_ref=sample_date)


@pytest.fixture
def sample_table_dict() -> dict:
	"""Fixture providing a sample API table dict with one row.

	Returns
	-------
	dict
		Sample table dict mimicking the BDI FlexibleOptions API response.
	"""
	return {
		"columns": [
			{"name": "TckrSymb"},
			{"name": "OptionType"},
			{"name": "OptionNeg"},
			{"name": "DueDate"},
			{"name": "NmbrBusinesses"},
			{"name": "NationalVol"},
			{"name": "AvrgPremium"},
			{"name": "AvrgPrice"},
		],
		"values": [
			[
				"A1MD4",
				"Compra",
				"Opção Flexível de Compra de Ações e Units",
				"2026-05-05T00:00:00",
				2,
				35643,
				5.19,
				177.61,
				None,
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
		"columns": [
			{"name": "TckrSymb"},
			{"name": "OptionType"},
			{"name": "OptionNeg"},
			{"name": "DueDate"},
			{"name": "NmbrBusinesses"},
			{"name": "NationalVol"},
			{"name": "AvrgPremium"},
			{"name": "AvrgPrice"},
		],
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

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	inst = B3BdiDerivativesOptionsFlex(date_ref=sample_date, int_page_size=500)
	assert inst.date_ref == sample_date
	assert inst.int_page_size == 500
	assert "2026-04-17" in inst.url_tpl
	assert "{page}" in inst.url_tpl
	assert "500" in inst.url_tpl
	assert "FlexibleOptions" in inst.url_tpl
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
	inst = B3BdiDerivativesOptionsFlex(date_ref=sample_date)
	assert inst.int_page_size == 1_000


def test_init_default_page_max_is_none(sample_date: date) -> None:
	"""Test that default int_page_max is None (paginate until empty).

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	inst = B3BdiDerivativesOptionsFlex(date_ref=sample_date)
	assert inst.int_page_max is None


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
	inst = B3BdiDerivativesOptionsFlex()
	assert inst.date_ref == date(2026, 4, 16)


def test_init_logger_propagated() -> None:
	"""Test that logger is stored on the instance.

	Returns
	-------
	None
	"""
	mock_logger = MagicMock(spec=Logger)
	inst = B3BdiDerivativesOptionsFlex(date_ref=date(2026, 4, 17), logger=mock_logger)
	assert inst.logger is mock_logger


def test_get_response_success(
	instance: B3BdiDerivativesOptionsFlex,
	mocker: MockerFixture,
) -> None:
	"""Test get_response posts to the correct URL and returns the response.

	Parameters
	----------
	instance : B3BdiDerivativesOptionsFlex
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

	result = instance.get_response(int_page=1)

	expected_url = instance.url_tpl.format(page=1)
	mock_post.assert_called_once_with(expected_url, json={}, timeout=(12.0, 21.0), verify=True)
	assert result is mock_resp
	mock_resp.raise_for_status.assert_called_once()


def test_get_response_http_error(
	instance: B3BdiDerivativesOptionsFlex,
	mocker: MockerFixture,
) -> None:
	"""Test get_response raises HTTPError on bad status.

	Parameters
	----------
	instance : B3BdiDerivativesOptionsFlex
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


def test_get_response_timeout_error(
	instance: B3BdiDerivativesOptionsFlex,
	mocker: MockerFixture,
) -> None:
	"""Test get_response raises Timeout when the server does not respond.

	Parameters
	----------
	instance : B3BdiDerivativesOptionsFlex
		Initialized instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch("requests.post", side_effect=requests.exceptions.Timeout("timed out"))
	with pytest.raises(requests.exceptions.Timeout):
		instance.get_response()


def test_get_response_connection_error(
	instance: B3BdiDerivativesOptionsFlex,
	mocker: MockerFixture,
) -> None:
	"""Test get_response raises ConnectionError when the host is unreachable.

	Parameters
	----------
	instance : B3BdiDerivativesOptionsFlex
		Initialized instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch(
		"requests.post",
		side_effect=requests.exceptions.ConnectionError("connection refused"),
	)
	with pytest.raises(requests.exceptions.ConnectionError):
		instance.get_response()


def test_parse_raw_file_returns_table(
	instance: B3BdiDerivativesOptionsFlex,
	sample_table_dict: dict,
) -> None:
	"""Test parse_raw_file extracts the table dict from the JSON response.

	Parameters
	----------
	instance : B3BdiDerivativesOptionsFlex
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


def test_parse_raw_file_missing_table_key(instance: B3BdiDerivativesOptionsFlex) -> None:
	"""Test parse_raw_file raises KeyError when 'table' key is absent.

	Parameters
	----------
	instance : B3BdiDerivativesOptionsFlex
		Initialized instance.

	Returns
	-------
	None
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.json.return_value = {"unexpected": {}}
	with pytest.raises(KeyError):
		instance.parse_raw_file(mock_resp)


def test_transform_data_normal(
	instance: B3BdiDerivativesOptionsFlex,
	sample_table_dict: dict,
) -> None:
	"""Test transform_data builds a DataFrame with 8 UPPER_SNAKE columns, drops trailing null.

	Parameters
	----------
	instance : B3BdiDerivativesOptionsFlex
		Initialized instance.
	sample_table_dict : dict
		Sample table dict with one row (9 values, 8 columns).

	Returns
	-------
	None
	"""
	df_ = instance.transform_data(sample_table_dict)
	assert isinstance(df_, pd.DataFrame)
	assert len(df_) == 1
	assert list(df_.columns) == [
		"TCKR_SYMB",
		"OPTION_TYPE",
		"OPTION_NEG",
		"DUE_DATE",
		"NMBR_BUSINESSES",
		"NATIONAL_VOL",
		"AVRG_PREMIUM",
		"AVRG_PRICE",
	]
	assert df_["TCKR_SYMB"].iloc[0] == "A1MD4"
	assert df_["OPTION_TYPE"].iloc[0] == "Compra"
	assert df_["DUE_DATE"].iloc[0] == "2026-05-05T00:00:00"
	assert df_["NMBR_BUSINESSES"].iloc[0] == 2
	assert df_["AVRG_PRICE"].iloc[0] == 177.61


def test_transform_data_multiple_rows(instance: B3BdiDerivativesOptionsFlex) -> None:
	"""Test transform_data handles multiple rows correctly.

	Parameters
	----------
	instance : B3BdiDerivativesOptionsFlex
		Initialized instance.

	Returns
	-------
	None
	"""
	table = {
		"columns": [
			{"name": "TckrSymb"},
			{"name": "OptionType"},
			{"name": "OptionNeg"},
			{"name": "DueDate"},
			{"name": "NmbrBusinesses"},
			{"name": "NationalVol"},
			{"name": "AvrgPremium"},
			{"name": "AvrgPrice"},
		],
		"values": [
			["A1MD4", "Compra", "Opção Flexível", "2026-05-05T00:00:00", 2, 35643, 5.19, 177.61, None],
			["B2XY5", "Venda", "Opção Flexível de Venda", "2026-06-03T00:00:00", 5, 80000, 3.50, 120.00, None],
		],
	}
	df_ = instance.transform_data(table)
	assert len(df_) == 2
	assert set(df_["TCKR_SYMB"].tolist()) == {"A1MD4", "B2XY5"}


def test_transform_data_empty_values(
	instance: B3BdiDerivativesOptionsFlex,
	empty_table_dict: dict,
) -> None:
	"""Test transform_data returns empty DataFrame when values list is empty.

	Parameters
	----------
	instance : B3BdiDerivativesOptionsFlex
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
	instance: B3BdiDerivativesOptionsFlex,
	mock_response: Response,
	mock_empty_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run fetches pages until empty response, then returns a DataFrame.

	Parameters
	----------
	instance : B3BdiDerivativesOptionsFlex
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
	mocker.patch("time.sleep")
	mocker.patch.object(
		instance,
		"get_response",
		side_effect=[mock_response, mock_empty_response],
	)
	mocker.patch.object(instance, "standardize_dataframe", side_effect=lambda df_, **kw: df_)

	result = instance.run()

	assert isinstance(result, pd.DataFrame)
	assert len(result) == 1
	assert "TCKR_SYMB" in result.columns
	assert "URL" in result.columns


def test_run_with_db(
	instance: B3BdiDerivativesOptionsFlex,
	mock_response: Response,
	mock_empty_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run inserts into DB and returns None when cls_db is set.

	Parameters
	----------
	instance : B3BdiDerivativesOptionsFlex
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
	mocker.patch("time.sleep")
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
	instance: B3BdiDerivativesOptionsFlex,
	mock_empty_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run returns None when the first page is already empty.

	Parameters
	----------
	instance : B3BdiDerivativesOptionsFlex
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
	instance: B3BdiDerivativesOptionsFlex,
	mocker: MockerFixture,
	timeout: int | float | tuple,
) -> None:
	"""Test get_response accepts various timeout types.

	Parameters
	----------
	instance : B3BdiDerivativesOptionsFlex
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


def test_module_reload(sample_date: date) -> None:
	"""Test that the module can be reloaded and the class re-instantiated.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.ingestion.countries.br.otc.b3_bdi_derivatives_options_flex as mod

	importlib.reload(mod)
	inst = mod.B3BdiDerivativesOptionsFlex(date_ref=sample_date)
	assert inst.date_ref == sample_date
	assert "FlexibleOptions" in inst.url_tpl
```

- [ ] **Step 5.2: Format the test file**

```bash
poetry run ruff format tests/unit/test_b3_bdi_derivatives_options_flex.py
```

- [ ] **Step 5.3: Run tests — expect ImportError**

```bash
poetry run pytest tests/unit/test_b3_bdi_derivatives_options_flex.py -v 2>&1 | head -20
```

Expected: `ImportError` or `ModuleNotFoundError`.

---

## Task 6: FlexibleOptions — Write Implementation + Example

**Files:**
- Create: `stpstone/ingestion/countries/br/otc/b3_bdi_derivatives_options_flex.py`
- Create: `examples/b3_bdi_derivatives_options_flex.py`

- [ ] **Step 6.1: Create the implementation file**

> Use **tab** indentation throughout.

```python
"""B3 BDI OTC flexible options ingestion."""

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


class B3BdiDerivativesOptionsFlex(ABCIngestionOperations):
	"""B3 BDI OTC flexible options ingestion class."""

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
			f"https://arquivos.b3.com.br/bdi/table/FlexibleOptions/"
			f"{str_date}/{str_date}/{{page}}/{self.int_page_size}"
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_bdi_derivatives_options_flex",
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
			The name of the table, by default "br_b3_bdi_derivatives_options_flex".

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
			"TCKR_SYMB": str,
			"OPTION_TYPE": str,
			"OPTION_NEG": str,
			"DUE_DATE": str,
			"NMBR_BUSINESSES": int,
			"NATIONAL_VOL": float,
			"AVRG_PREMIUM": float,
			"AVRG_PRICE": float,
			"URL": str,
		}
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes=dict_dtypes,
			str_fmt_dt="YYYY-MM-DD",
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
			message=(
				f"B3BdiDerivativesOptionsFlex: page {int_page} fetched ({int_rows} rows)"
			),
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
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
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
		"""Build a DataFrame from the API table dict, dropping the trailing null column.

		Column names are converted from PascalCase to UPPER_SNAKE_CASE. Each values
		row has 9 elements but only 8 column definitions — the 9th element is always
		null and is dropped by selecting only the named columns.

		Parameters
		----------
		data : dict
			The table dict with keys 'columns' and 'values'.

		Returns
		-------
		pd.DataFrame
			DataFrame with UPPER_SNAKE_CASE column names (8 columns), or empty
			DataFrame when values is an empty list.
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
		df_ = df_.rename(columns=rename_map)
		return df_[col_names]
```

- [ ] **Step 6.2: Format the implementation file**

```bash
poetry run ruff format stpstone/ingestion/countries/br/otc/b3_bdi_derivatives_options_flex.py
```

- [ ] **Step 6.3: Create the example file**

```python
"""B3 BDI OTC flexible options ingestion."""

from stpstone.ingestion.countries.br.otc.b3_bdi_derivatives_options_flex import (
	B3BdiDerivativesOptionsFlex,
)


cls_ = B3BdiDerivativesOptionsFlex(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Derivatives Options Flex: \n{df_}")
df_.info()
```

- [ ] **Step 6.4: Format the example file**

```bash
poetry run ruff format examples/b3_bdi_derivatives_options_flex.py
```

- [ ] **Step 6.5: Run tests — expect PASS**

```bash
make test_feat MODULE=b3_bdi_derivatives_options_flex
```

Expected: all tests green, ruff lint passes.

- [ ] **Step 6.6: Commit**

```bash
git add stpstone/ingestion/countries/br/otc/b3_bdi_derivatives_options_flex.py \
        tests/unit/test_b3_bdi_derivatives_options_flex.py \
        examples/b3_bdi_derivatives_options_flex.py
git commit -m "feat(bdi-b3): Add B3 BDI OTC flexible options ingestion"
```

---

## Task 7: Final Lint Pass

**Files:** All nine files created above.

- [ ] **Step 7.1: Run global lint**

```bash
make lint
```

Expected output includes: `0 files reformatted` — if any files are reformatted, re-run `make test_feat` for the affected module to confirm tests still pass.

- [ ] **Step 7.2: Commit spec and plan docs**

```bash
git add docs/superpowers/specs/2026-04-20-b3-bdi-otc-derivatives-ingestion-design.md \
        docs/superpowers/plans/2026-04-20-b3-bdi-otc-derivatives-ingestion.md
git commit -m "docs: Add spec and plan for B3 BDI OTC derivatives ingestion"
```
