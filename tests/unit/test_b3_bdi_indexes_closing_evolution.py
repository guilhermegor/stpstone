"""Unit tests for B3BdiIndexesClosingEvolution class."""

from __future__ import annotations

from datetime import date
from logging import Logger
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
import requests
from requests import Response

from stpstone.ingestion.countries.br.exchange.b3_bdi_indexes_closing_evolution import (
	B3BdiIndexesClosingEvolution,
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
def instance(sample_date: date) -> B3BdiIndexesClosingEvolution:
	"""Fixture providing a B3BdiIndexesClosingEvolution instance for IBOVESPA.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	B3BdiIndexesClosingEvolution
		Initialized instance.
	"""
	return B3BdiIndexesClosingEvolution(str_index_name="IBOVESPA", date_ref=sample_date)


@pytest.fixture
def sample_table_dict() -> dict:
	"""Fixture providing a ClosingEvolution child table dict with one data row.

	Returns
	-------
	dict
		Sample child table dict mimicking the BDI INDEXES API response.
	"""
	return {
		"name": "IbovespaClosingEvolution",
		"columns": [
			{"name": "TckrSymb"},
			{"name": "InDay"},
			{"name": "Yesterday"},
			{"name": "InTheWeek"},
			{"name": "InWeek"},
			{"name": "InTheMonth"},
			{"name": "InMonth"},
			{"name": "InTheYear"},
			{"name": "InYear"},
		],
		"values": [
			["IBOVESPA", -0.0055, 197500.0, -0.012, 198000.0, 0.032, 191000.0, -0.08, 215000.0],
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
		"name": "IbovespaClosingEvolution",
		"columns": [
			{"name": "TckrSymb"},
			{"name": "InDay"},
			{"name": "Yesterday"},
			{"name": "InTheWeek"},
			{"name": "InWeek"},
			{"name": "InTheMonth"},
			{"name": "InMonth"},
			{"name": "InTheYear"},
			{"name": "InYear"},
		],
		"values": [],
	}


@pytest.fixture
def mock_response(sample_table_dict: dict) -> Response:
	"""Mock Response with full nested INDEXES structure for IBOVESPA.

	Parameters
	----------
	sample_table_dict : dict
		Sample child table dict fixture.

	Returns
	-------
	Response
		Mocked Response object.
	"""
	response = MagicMock(spec=Response)
	response.status_code = 200
	response.json.return_value = {
		"table": {
			"children": [
				{"name": "IBOVESPA", "children": [sample_table_dict]},
			]
		}
	}
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def mock_empty_response(empty_table_dict: dict) -> Response:
	"""Mock Response with empty-values nested INDEXES structure.

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
	response.json.return_value = {
		"table": {
			"children": [
				{"name": "IBOVESPA", "children": [empty_table_dict]},
			]
		}
	}
	response.raise_for_status = MagicMock()
	return response


# --------------------------
# Tests
# --------------------------
def test_init_stores_index_name(sample_date: date) -> None:
	"""Test that str_index_name is stored on the instance.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	inst = B3BdiIndexesClosingEvolution(str_index_name="IBRX100", date_ref=sample_date)
	assert inst.str_index_name == "IBRX100"


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
	inst = B3BdiIndexesClosingEvolution(
		str_index_name="IBOVESPA", date_ref=sample_date, int_page_size=500
	)
	assert inst.date_ref == sample_date
	assert inst.int_page_size == 500
	assert "2026-04-17" in inst.url_tpl
	assert "INDEXES" in inst.url_tpl
	assert inst.logger is None
	assert isinstance(inst.cls_dir_files_management, DirFilesManagement)
	assert isinstance(inst.cls_dates_br, DatesBRAnbima)
	assert isinstance(inst.cls_create_log, CreateLog)


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
	inst = B3BdiIndexesClosingEvolution(str_index_name="IBOVESPA")
	assert inst.date_ref == date(2026, 4, 16)


def test_init_logger_propagated(sample_date: date) -> None:
	"""Test that logger is stored on the instance.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	mock_logger = MagicMock(spec=Logger)
	inst = B3BdiIndexesClosingEvolution(
		str_index_name="IBOVESPA", date_ref=sample_date, logger=mock_logger
	)
	assert inst.logger is mock_logger


def test_get_response_success(
	instance: B3BdiIndexesClosingEvolution,
	mocker: MockerFixture,
) -> None:
	"""Test get_response posts to the correct URL.

	Parameters
	----------
	instance : B3BdiIndexesClosingEvolution
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

	result = instance.get_response(int_page=2)

	expected_url = instance.url_tpl.format(page=2)
	mock_post.assert_called_once_with(expected_url, json={}, timeout=(12.0, 21.0), verify=True)
	assert result is mock_resp


def test_get_response_http_error(
	instance: B3BdiIndexesClosingEvolution,
	mocker: MockerFixture,
) -> None:
	"""Test get_response raises HTTPError on bad status.

	Parameters
	----------
	instance : B3BdiIndexesClosingEvolution
		Initialized instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch("requests.post", side_effect=requests.exceptions.HTTPError("500"))
	with pytest.raises(requests.exceptions.HTTPError):
		instance.get_response()


def test_get_response_timeout_error(
	instance: B3BdiIndexesClosingEvolution,
	mocker: MockerFixture,
) -> None:
	"""Test get_response raises Timeout when the request times out.

	Parameters
	----------
	instance : B3BdiIndexesClosingEvolution
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


def test_parse_raw_file_returns_matching_group(
	instance: B3BdiIndexesClosingEvolution,
	sample_table_dict: dict,
) -> None:
	"""Test parse_raw_file extracts the ClosingEvolution child for str_index_name.

	Parameters
	----------
	instance : B3BdiIndexesClosingEvolution
		Initialized instance.
	sample_table_dict : dict
		Expected child table dict.

	Returns
	-------
	None
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.json.return_value = {
		"table": {
			"children": [
				{"name": "IBOVESPA", "children": [sample_table_dict]},
				{
					"name": "IBRX50",
					"children": [{"name": "IBRX50ClosingEvolution", "columns": [], "values": []}],
				},
			]
		}
	}
	result = instance.parse_raw_file(mock_resp)
	assert result["name"] == "IbovespaClosingEvolution"


def test_parse_raw_file_inconsistent_naming(sample_date: date) -> None:
	"""Test parse_raw_file matches suffix even when group and child names differ (e.g. AGFS).

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	inst = B3BdiIndexesClosingEvolution(str_index_name="AGFS", date_ref=sample_date)
	agro_child = {
		"name": "IagroClosingEvolution",
		"columns": [{"name": "TckrSymb"}],
		"values": [["AGFS"]],
	}
	mock_resp = MagicMock(spec=Response)
	mock_resp.json.return_value = {
		"table": {"children": [{"name": "AGFS", "children": [agro_child]}]}
	}
	result = inst.parse_raw_file(mock_resp)
	assert result["name"] == "IagroClosingEvolution"


def test_parse_raw_file_unknown_group(instance: B3BdiIndexesClosingEvolution) -> None:
	"""Test parse_raw_file returns empty dict when str_index_name is not found.

	Parameters
	----------
	instance : B3BdiIndexesClosingEvolution
		Initialized instance.

	Returns
	-------
	None
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.json.return_value = {"table": {"children": []}}
	result = instance.parse_raw_file(mock_resp)
	assert result == {"columns": [], "values": []}


def test_transform_data_normal(
	instance: B3BdiIndexesClosingEvolution,
	sample_table_dict: dict,
) -> None:
	"""Test transform_data builds a DataFrame with UPPER_SNAKE columns, dropping trailing None.

	Parameters
	----------
	instance : B3BdiIndexesClosingEvolution
		Initialized instance.
	sample_table_dict : dict
		Sample table dict with one row (includes trailing None).

	Returns
	-------
	None
	"""
	df_ = instance.transform_data(sample_table_dict)
	assert isinstance(df_, pd.DataFrame)
	assert len(df_) == 1
	assert list(df_.columns) == [
		"TCKR_SYMB",
		"IN_DAY",
		"YESTERDAY",
		"IN_THE_WEEK",
		"IN_WEEK",
		"IN_THE_MONTH",
		"IN_MONTH",
		"IN_THE_YEAR",
		"IN_YEAR",
	]
	assert df_["TCKR_SYMB"].iloc[0] == "IBOVESPA"
	assert df_["IN_DAY"].iloc[0] == -0.0055


def test_transform_data_empty_values(
	instance: B3BdiIndexesClosingEvolution,
	empty_table_dict: dict,
) -> None:
	"""Test transform_data returns empty DataFrame when values list is empty.

	Parameters
	----------
	instance : B3BdiIndexesClosingEvolution
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


def test_run_derives_table_name_from_index(
	instance: B3BdiIndexesClosingEvolution,
	mock_response: Response,
	mock_empty_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run derives str_table_name from str_index_name when not provided.

	Parameters
	----------
	instance : B3BdiIndexesClosingEvolution
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
	mocker.patch.object(instance, "get_response", side_effect=[mock_response, mock_empty_response])
	mocker.patch.object(instance, "standardize_dataframe", side_effect=lambda df_, **kw: df_)
	mock_insert = mocker.patch.object(instance, "insert_table_db")

	instance.run()

	_, kwargs = mock_insert.call_args
	assert kwargs["str_table_name"] == "br_b3_bdi_indexes_ibovespa_closing_evolution"


def test_run_without_db_returns_dataframe(
	instance: B3BdiIndexesClosingEvolution,
	mock_response: Response,
	mock_empty_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run returns a DataFrame when cls_db is not set.

	Parameters
	----------
	instance : B3BdiIndexesClosingEvolution
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
	mocker.patch.object(instance, "get_response", side_effect=[mock_response, mock_empty_response])
	mocker.patch.object(instance, "standardize_dataframe", side_effect=lambda df_, **kw: df_)

	result = instance.run()

	assert isinstance(result, pd.DataFrame)
	assert len(result) == 1
	assert "TCKR_SYMB" in result.columns


def test_run_with_db(
	instance: B3BdiIndexesClosingEvolution,
	mock_response: Response,
	mock_empty_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run inserts into DB and returns None when cls_db is set.

	Parameters
	----------
	instance : B3BdiIndexesClosingEvolution
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
	mocker.patch.object(instance, "get_response", side_effect=[mock_response, mock_empty_response])
	mocker.patch.object(instance, "standardize_dataframe", side_effect=lambda df_, **kw: df_)
	mock_insert = mocker.patch.object(instance, "insert_table_db")

	result = instance.run()

	assert result is None
	mock_insert.assert_called_once()


def test_run_no_data_returns_none(
	instance: B3BdiIndexesClosingEvolution,
	mock_empty_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run returns None when the first page is already empty.

	Parameters
	----------
	instance : B3BdiIndexesClosingEvolution
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


@pytest.mark.parametrize("timeout", [10, 10.5, (10.0, 20.0), (10, 20)])
def test_get_response_timeout_variants(
	instance: B3BdiIndexesClosingEvolution,
	mocker: MockerFixture,
	timeout: int | float | tuple,
) -> None:
	"""Test get_response accepts various timeout types.

	Parameters
	----------
	instance : B3BdiIndexesClosingEvolution
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
