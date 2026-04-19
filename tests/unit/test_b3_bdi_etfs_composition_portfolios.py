"""Unit tests for B3BdiEtfsCompositionPortfolios class."""

from datetime import date
from logging import Logger
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
import requests
from requests import Response

from stpstone.ingestion.countries.br.exchange.b3_bdi_etfs_composition_portfolios import (
	B3BdiEtfsCompositionPortfolios,
)
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_date() -> date:
	"""Fixture providing a sample reference date.

	Returns
	-------
	date
		Sample date object.
	"""
	return date(2026, 4, 17)


@pytest.fixture
def instance(sample_date: date) -> B3BdiEtfsCompositionPortfolios:
	"""Fixture providing a B3BdiEtfsCompositionPortfolios instance.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	B3BdiEtfsCompositionPortfolios
		Initialized instance.
	"""
	return B3BdiEtfsCompositionPortfolios(date_ref=sample_date)


@pytest.fixture
def sample_children() -> list[dict]:
	"""Fixture providing two index children (IBOVESPA and IBRX) with 2 rows each.

	Returns
	-------
	list[dict]
		List of child dicts mimicking the BDI PreviaQuadrimestral API response.
	"""
	columns = [
		{"name": "TckrSymb"},
		{"name": "Stock"},
		{"name": "CodeType"},
		{"name": "QtyTheoretical"},
		{"name": "StockParticipation"},
	]
	return [
		{
			"name": "IBOV",
			"friendlyNameEn": "IBOVESPA",
			"columns": columns,
			"values": [
				["PETR4", "PETROBRAS PN N2", "ON", 3_500_000_000, 9.123, None],
				["VALE3", "VALE ON NM", "ON", 2_800_000_000, 7.654, None],
			],
		},
		{
			"name": "IBRX",
			"friendlyNameEn": "IBrX 100",
			"columns": columns,
			"values": [
				["ITUB4", "ITAUUNIBANCO PN", "PN", 1_200_000_000, 5.432, None],
				["BBDC4", "BRADESCO PN", "PN", 900_000_000, 4.321, None],
			],
		},
	]


@pytest.fixture
def empty_children() -> list[dict]:
	"""Fixture providing an empty children list.

	Returns
	-------
	list[dict]
		Empty list.
	"""
	return []


@pytest.fixture
def mock_response(sample_children: list[dict]) -> Response:
	"""Mock Response with two-index payload.

	Parameters
	----------
	sample_children : list[dict]
		Sample children fixture.

	Returns
	-------
	Response
		Mocked Response object.
	"""
	response = MagicMock(spec=Response)
	response.status_code = 200
	response.json.return_value = {"table": {"values": [], "children": sample_children}}
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def mock_empty_response(empty_children: list[dict]) -> Response:
	"""Mock Response with empty children payload.

	Parameters
	----------
	empty_children : list[dict]
		Empty children fixture.

	Returns
	-------
	Response
		Mocked Response object.
	"""
	response = MagicMock(spec=Response)
	response.status_code = 200
	response.json.return_value = {"table": {"values": [], "children": empty_children}}
	response.raise_for_status = MagicMock()
	return response


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(sample_date: date) -> None:
	"""Test initialization with all parameters provided.

	Verifies
	--------
	- date_ref, int_page_size, and url are set correctly.
	- Standard helpers are initialized.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	inst = B3BdiEtfsCompositionPortfolios(date_ref=sample_date, int_page_size=50)
	assert inst.date_ref == sample_date
	assert inst.int_page_size == 50
	assert "PreviaQuadrimestral" in inst.url
	assert "2026-04-17" in inst.url
	assert "50" in inst.url
	assert inst.logger is None
	assert isinstance(inst.cls_dir_files_management, DirFilesManagement)
	assert isinstance(inst.cls_dates_br, DatesBRAnbima)
	assert isinstance(inst.cls_create_log, CreateLog)


def test_init_default_page_size(sample_date: date) -> None:
	"""Test that default int_page_size is 100.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	inst = B3BdiEtfsCompositionPortfolios(date_ref=sample_date)
	assert inst.int_page_size == 100


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
	inst = B3BdiEtfsCompositionPortfolios()
	assert inst.date_ref == date(2026, 4, 16)


def test_init_logger_propagated() -> None:
	"""Test that logger is stored on the instance.

	Returns
	-------
	None
	"""
	mock_logger = MagicMock(spec=Logger)
	inst = B3BdiEtfsCompositionPortfolios(date_ref=date(2026, 4, 17), logger=mock_logger)
	assert inst.logger is mock_logger


def test_init_url_contains_date_and_endpoint(sample_date: date) -> None:
	"""Test that url contains the reference date and endpoint name.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	inst = B3BdiEtfsCompositionPortfolios(date_ref=sample_date)
	assert "PreviaQuadrimestral" in inst.url
	assert "2026-04-17" in inst.url


def test_get_response_success(
	instance: B3BdiEtfsCompositionPortfolios,
	mocker: MockerFixture,
) -> None:
	"""Test get_response posts to self.url and returns the response.

	Parameters
	----------
	instance : B3BdiEtfsCompositionPortfolios
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

	result = instance.get_response()

	mock_post.assert_called_once_with(
		instance.url, json={}, timeout=(12.0, 21.0), verify=True
	)
	assert result is mock_resp
	mock_resp.raise_for_status.assert_called_once()


def test_get_response_http_error(
	instance: B3BdiEtfsCompositionPortfolios,
	mocker: MockerFixture,
) -> None:
	"""Test get_response raises HTTPError on bad status.

	Parameters
	----------
	instance : B3BdiEtfsCompositionPortfolios
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
		side_effect=requests.exceptions.HTTPError("500 Server Error"),
	)
	with pytest.raises(requests.exceptions.HTTPError):
		instance.get_response()


def test_get_response_timeout_error(
	instance: B3BdiEtfsCompositionPortfolios,
	mocker: MockerFixture,
) -> None:
	"""Test get_response raises Timeout when the server does not respond.

	Parameters
	----------
	instance : B3BdiEtfsCompositionPortfolios
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
		side_effect=requests.exceptions.Timeout("timed out"),
	)
	with pytest.raises(requests.exceptions.Timeout):
		instance.get_response()


def test_get_response_connection_error(
	instance: B3BdiEtfsCompositionPortfolios,
	mocker: MockerFixture,
) -> None:
	"""Test get_response raises ConnectionError when the host is unreachable.

	Parameters
	----------
	instance : B3BdiEtfsCompositionPortfolios
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


@pytest.mark.parametrize(
	"timeout",
	[10, 10.5, (10.0, 20.0), (10, 20)],
)
def test_get_response_timeout_variants(
	instance: B3BdiEtfsCompositionPortfolios,
	mocker: MockerFixture,
	timeout: int | float | tuple,
) -> None:
	"""Test get_response accepts various timeout types.

	Parameters
	----------
	instance : B3BdiEtfsCompositionPortfolios
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


def test_parse_raw_file_returns_children(
	instance: B3BdiEtfsCompositionPortfolios,
	sample_children: list[dict],
) -> None:
	"""Test parse_raw_file extracts the children list from the JSON response.

	Parameters
	----------
	instance : B3BdiEtfsCompositionPortfolios
		Initialized instance.
	sample_children : list[dict]
		Expected children list.

	Returns
	-------
	None
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.json.return_value = {
		"table": {"values": [], "children": sample_children}
	}
	result = instance.parse_raw_file(mock_resp)
	assert result == sample_children


def test_parse_raw_file_missing_table_key(
	instance: B3BdiEtfsCompositionPortfolios,
) -> None:
	"""Test parse_raw_file raises KeyError when 'table' key is absent.

	Parameters
	----------
	instance : B3BdiEtfsCompositionPortfolios
		Initialized instance.

	Returns
	-------
	None
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.json.return_value = {"unexpected": {}}
	with pytest.raises(KeyError):
		instance.parse_raw_file(mock_resp)


def test_transform_data_two_children(
	instance: B3BdiEtfsCompositionPortfolios,
	sample_children: list[dict],
) -> None:
	"""Test transform_data concatenates rows from two index children.

	Parameters
	----------
	instance : B3BdiEtfsCompositionPortfolios
		Initialized instance.
	sample_children : list[dict]
		Two children, each with 2 rows.

	Returns
	-------
	None
	"""
	df_ = instance.transform_data(sample_children)
	assert isinstance(df_, pd.DataFrame)
	assert len(df_) == 4
	assert list(df_.columns) == [
		"TCKR_SYMB",
		"STOCK",
		"CODE_TYPE",
		"QTY_THEORETICAL",
		"STOCK_PARTICIPATION",
		"INDEX_NM",
	]


def test_transform_data_empty_list(
	instance: B3BdiEtfsCompositionPortfolios,
	empty_children: list[dict],
) -> None:
	"""Test transform_data returns an empty DataFrame for an empty children list.

	Parameters
	----------
	instance : B3BdiEtfsCompositionPortfolios
		Initialized instance.
	empty_children : list[dict]
		Empty list.

	Returns
	-------
	None
	"""
	df_ = instance.transform_data(empty_children)
	assert isinstance(df_, pd.DataFrame)
	assert df_.empty


def test_transform_data_skips_child_with_empty_values(
	instance: B3BdiEtfsCompositionPortfolios,
) -> None:
	"""Test transform_data skips children whose values list is empty.

	Parameters
	----------
	instance : B3BdiEtfsCompositionPortfolios
		Initialized instance.

	Returns
	-------
	None
	"""
	columns = [
		{"name": "TckrSymb"},
		{"name": "Stock"},
		{"name": "CodeType"},
		{"name": "QtyTheoretical"},
		{"name": "StockParticipation"},
	]
	children = [
		{
			"name": "EMPTY_IDX",
			"friendlyNameEn": "Empty Index",
			"columns": columns,
			"values": [],
		},
		{
			"name": "IBOV",
			"friendlyNameEn": "IBOVESPA",
			"columns": columns,
			"values": [["PETR4", "PETROBRAS PN", "ON", 3_500_000_000, 9.123, None]],
		},
	]
	df_ = instance.transform_data(children)
	assert len(df_) == 1
	assert df_["INDEX_NM"].iloc[0] == "IBOVESPA"


def test_transform_data_index_nm_populated(
	instance: B3BdiEtfsCompositionPortfolios,
	sample_children: list[dict],
) -> None:
	"""Test transform_data populates INDEX_NM from friendlyNameEn.

	Parameters
	----------
	instance : B3BdiEtfsCompositionPortfolios
		Initialized instance.
	sample_children : list[dict]
		Two children with friendlyNameEn set.

	Returns
	-------
	None
	"""
	df_ = instance.transform_data(sample_children)
	index_names = df_["INDEX_NM"].unique().tolist()
	assert "IBOVESPA" in index_names
	assert "IBrX 100" in index_names


def test_run_without_db_returns_dataframe(
	instance: B3BdiEtfsCompositionPortfolios,
	mock_response: Response,
	sample_children: list[dict],
	mocker: MockerFixture,
) -> None:
	"""Test run returns a DataFrame when cls_db is not set.

	Parameters
	----------
	instance : B3BdiEtfsCompositionPortfolios
		Initialized instance.
	mock_response : Response
		Mocked Response with two indices.
	sample_children : list[dict]
		Sample children for count verification.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch.object(instance, "get_response", return_value=mock_response)
	mocker.patch.object(
		instance,
		"standardize_dataframe",
		side_effect=lambda df_, **kw: df_,
	)

	result = instance.run()

	assert isinstance(result, pd.DataFrame)
	assert len(result) == 4
	assert "TCKR_SYMB" in result.columns
	assert "INDEX_NM" in result.columns
	assert "URL" in result.columns


def test_run_with_db_inserts_and_returns_none(
	instance: B3BdiEtfsCompositionPortfolios,
	mock_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run inserts into DB and returns None when cls_db is set.

	Parameters
	----------
	instance : B3BdiEtfsCompositionPortfolios
		Initialized instance.
	mock_response : Response
		Mocked Response with two indices.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	instance.cls_db = MagicMock()
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch.object(instance, "get_response", return_value=mock_response)
	mocker.patch.object(instance, "standardize_dataframe", side_effect=lambda df_, **kw: df_)
	mock_insert = mocker.patch.object(instance, "insert_table_db")

	result = instance.run()

	assert result is None
	mock_insert.assert_called_once()


def test_run_empty_children_returns_none(
	instance: B3BdiEtfsCompositionPortfolios,
	mock_empty_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run returns None when no index children carry data.

	Parameters
	----------
	instance : B3BdiEtfsCompositionPortfolios
		Initialized instance.
	mock_empty_response : Response
		Mocked Response with empty children.
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


def test_transform_data_truncates_extra_values(
	instance: B3BdiEtfsCompositionPortfolios,
) -> None:
	"""Test transform_data drops trailing values that have no matching column.

	The B3 API returns a 6th null value per row that has no corresponding
	column entry. The DataFrame must contain only the 5 defined columns plus
	INDEX_NM, with no integer-named leftover columns.

	Parameters
	----------
	instance : B3BdiEtfsCompositionPortfolios
		Initialized instance.

	Returns
	-------
	None
	"""
	columns = [
		{"name": "TckrSymb"},
		{"name": "Stock"},
		{"name": "CodeType"},
		{"name": "QtyTheoretical"},
		{"name": "StockParticipation"},
	]
	children = [
		{
			"name": "IBOV",
			"friendlyNameEn": "IBOVESPA",
			"columns": columns,
			"values": [
				["ABEV3", "AMBEV S/A", "ON  EDJ", 4_273_841_357, 2.4958, None],
			],
		}
	]
	df_ = instance.transform_data(children)
	assert list(df_.columns) == [
		"TCKR_SYMB",
		"STOCK",
		"CODE_TYPE",
		"QTY_THEORETICAL",
		"STOCK_PARTICIPATION",
		"INDEX_NM",
	]
	assert len(df_) == 1
	assert df_["TCKR_SYMB"].iloc[0] == "ABEV3"


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

	import stpstone.ingestion.countries.br.exchange.b3_bdi_etfs_composition_portfolios as mod

	importlib.reload(mod)
	inst = mod.B3BdiEtfsCompositionPortfolios(date_ref=sample_date)
	assert inst.date_ref == sample_date
	assert "PreviaQuadrimestral" in inst.url
