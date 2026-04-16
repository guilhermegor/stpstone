"""Unit tests for RatingsCorpSPGlobalOnePage class."""

from datetime import date
from typing import Union
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.ww.registries.ratings_corp_spglobal_one_page import (
	RatingsCorpSPGlobalOnePage,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_date() -> date:
	"""Provide a fixed date for testing.

	Returns
	-------
	date
		Fixed reference date.
	"""
	return date(2025, 1, 1)


@pytest.fixture
def mock_token_response() -> Response:
	"""Mock HTTP Response for token fetching.

	Returns
	-------
	Response
		Mocked Response returning an apiKey.
	"""
	response = MagicMock(spec=Response)
	response.status_code = 200
	response.raise_for_status = MagicMock()
	response.json.return_value = {"apiKey": "test-api-key"}
	return response


@pytest.fixture
def mock_requests_get(mocker: MockerFixture, mock_token_response: Response) -> object:
	"""Patch requests.get to return the token response.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.
	mock_token_response : Response
		Token response mock.

	Returns
	-------
	object
		Mocked requests.get.
	"""
	return mocker.patch("requests.get", return_value=mock_token_response)


@pytest.fixture
def mock_backoff(mocker: MockerFixture) -> object:
	"""Patch backoff.on_exception to bypass retry delays.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	object
		Mocked backoff decorator.
	"""
	return mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)


@pytest.fixture
def instance(
	sample_date: date,
	mock_requests_get: object,
	mock_backoff: object,
) -> RatingsCorpSPGlobalOnePage:
	"""Provide a RatingsCorpSPGlobalOnePage instance for testing.

	Parameters
	----------
	sample_date : date
		Fixed reference date.
	mock_requests_get : object
		Mocked requests.get (auto-applied for token fetch).
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	RatingsCorpSPGlobalOnePage
		Initialized instance.
	"""
	return RatingsCorpSPGlobalOnePage(
		bearer="Bearer test-token",
		token="test-api-key",  # noqa: S106
		date_ref=sample_date,
	)


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(
	sample_date: date,
	mock_requests_get: object,
	mock_backoff: object,
) -> None:
	"""Test initialization sets expected attributes.

	Parameters
	----------
	sample_date : date
		Fixed reference date.
	mock_requests_get : object
		Mocked requests.get.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	obj = RatingsCorpSPGlobalOnePage(
		bearer="Bearer test-token",
		token="test-api-key",  # noqa: S106
		date_ref=sample_date,
	)
	assert obj.date_ref == sample_date
	assert obj.bearer == "Bearer test-token"
	assert obj.token == "test-api-key"  # noqa: S105
	assert obj.pg_number == 1
	assert isinstance(obj.cls_dates_current, DatesCurrent)
	assert isinstance(obj.cls_dates_br, DatesBRAnbima)
	assert isinstance(obj.cls_create_log, CreateLog)
	assert isinstance(obj.cls_dir_files_management, DirFilesManagement)
	assert "spglobal.com" in obj.url


def test_init_with_default_date(
	mock_requests_get: object,
	mock_backoff: object,
) -> None:
	"""Test initialization without date_ref uses the previous working day.

	Parameters
	----------
	mock_requests_get : object
		Mocked requests.get.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	with patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 1)):
		obj = RatingsCorpSPGlobalOnePage(bearer="Bearer test", token="key")  # noqa: S106
		assert obj.date_ref == date(2025, 1, 1)


def test_get_response_posts_correct_payload(
	instance: RatingsCorpSPGlobalOnePage,
	mock_backoff: object,
) -> None:
	"""Test get_response calls requests.post with the correct payload.

	Parameters
	----------
	instance : RatingsCorpSPGlobalOnePage
		Ingestion instance.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	mock_post_resp = MagicMock(spec=Response)
	mock_post_resp.raise_for_status = MagicMock()
	with patch("requests.post", return_value=mock_post_resp) as mock_post:
		result = instance.get_response()
	assert result is mock_post_resp
	call_kwargs = mock_post.call_args[1]
	assert call_kwargs["json"]["pageNumber"] == "1"
	assert call_kwargs["json"]["pageLength"] == "100"


def test_get_response_http_error(
	instance: RatingsCorpSPGlobalOnePage,
	mock_backoff: object,
) -> None:
	"""Test get_response propagates HTTPError.

	Parameters
	----------
	instance : RatingsCorpSPGlobalOnePage
		Ingestion instance.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	import requests as req_lib

	with (
		patch("requests.post", side_effect=req_lib.exceptions.HTTPError("500")),
		pytest.raises(req_lib.exceptions.HTTPError),
	):
		instance.get_response()


@pytest.mark.parametrize("timeout", [10, 10.5, (5.0, 10.0), (5, 10)])
def test_get_response_timeout_variations(
	instance: RatingsCorpSPGlobalOnePage,
	mock_backoff: object,
	timeout: Union[int, float, tuple],
) -> None:
	"""Test get_response accepts various timeout formats.

	Parameters
	----------
	instance : RatingsCorpSPGlobalOnePage
		Ingestion instance.
	mock_backoff : object
		Mocked backoff decorator.
	timeout : Union[int, float, tuple]
		Timeout value to test.

	Returns
	-------
	None
	"""
	mock_post_resp = MagicMock(spec=Response)
	mock_post_resp.raise_for_status = MagicMock()
	with patch("requests.post", return_value=mock_post_resp):
		result = instance.get_response(timeout=timeout)
	assert result is mock_post_resp


def test_parse_raw_file_passthrough(
	instance: RatingsCorpSPGlobalOnePage,
) -> None:
	"""Test parse_raw_file returns the response object unchanged.

	Parameters
	----------
	instance : RatingsCorpSPGlobalOnePage
		Ingestion instance.

	Returns
	-------
	None
	"""
	mock_resp = MagicMock(spec=Response)
	result = instance.parse_raw_file(mock_resp)
	assert result is mock_resp


def test_transform_data_returns_dataframe(
	instance: RatingsCorpSPGlobalOnePage,
) -> None:
	"""Test transform_data builds a DataFrame from JSON.

	Parameters
	----------
	instance : RatingsCorpSPGlobalOnePage
		Ingestion instance.

	Returns
	-------
	None
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.json.return_value = {
		"RatingAction": [
			{
				"ratingActionDate": "2025-01-01",
				"actionTypeCode": "DN",
				"entityId": 123,
				"sourceProvidedName": "Test Corp",
				"actionLevelIndicator": "L",
				"actionName": "Downgrade",
				"sectorCode": "FIN",
				"ratingFrom": "BBB",
				"ratingTo": "BB",
				"ratingType": "ICR",
				"maturityDate": None,
				"id": 456,
			}
		]
	}
	df_ = instance.transform_data(resp_req=mock_resp)
	assert isinstance(df_, pd.DataFrame)
	assert not df_.empty


def test_transform_data_empty_response(
	instance: RatingsCorpSPGlobalOnePage,
) -> None:
	"""Test transform_data with empty RatingAction list returns empty DataFrame.

	Parameters
	----------
	instance : RatingsCorpSPGlobalOnePage
		Ingestion instance.

	Returns
	-------
	None
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.json.return_value = {"RatingAction": []}
	df_ = instance.transform_data(resp_req=mock_resp)
	assert isinstance(df_, pd.DataFrame)
	assert df_.empty


def test_run_without_db(
	instance: RatingsCorpSPGlobalOnePage,
	mock_backoff: object,
) -> None:
	"""Test run returns a DataFrame when no cls_db is provided.

	Parameters
	----------
	instance : RatingsCorpSPGlobalOnePage
		Ingestion instance.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	mock_post_resp = MagicMock(spec=Response)
	mock_post_resp.raise_for_status = MagicMock()
	sample_df = pd.DataFrame({"RATING_ACTION_DATE": ["2025-01-01"]})
	with (
		patch("requests.post", return_value=mock_post_resp),
		patch.object(instance, "transform_data", return_value=sample_df),
		patch.object(instance, "standardize_dataframe", return_value=sample_df),
	):
		result = instance.run()
	assert isinstance(result, pd.DataFrame)


def test_run_with_db(
	instance: RatingsCorpSPGlobalOnePage,
	mock_backoff: object,
) -> None:
	"""Test run calls insert_table_db and returns None when cls_db is set.

	Parameters
	----------
	instance : RatingsCorpSPGlobalOnePage
		Ingestion instance.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	instance.cls_db = MagicMock()
	mock_post_resp = MagicMock(spec=Response)
	mock_post_resp.raise_for_status = MagicMock()
	sample_df = pd.DataFrame({"RATING_ACTION_DATE": ["2025-01-01"]})
	with (
		patch("requests.post", return_value=mock_post_resp),
		patch.object(instance, "transform_data", return_value=sample_df),
		patch.object(instance, "standardize_dataframe", return_value=sample_df),
		patch.object(instance, "insert_table_db") as mock_insert,
	):
		result = instance.run()
	assert result is None
	mock_insert.assert_called_once()


def test_reload_module() -> None:
	"""Test that the module can be reloaded without errors.

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.ingestion.countries.ww.registries.ratings_corp_spglobal_one_page as mod

	with patch("requests.get") as mock_get:
		mock_get.return_value.json.return_value = {"apiKey": "key"}
		mock_get.return_value.raise_for_status = MagicMock()
		importlib.reload(mod)
		obj = mod.RatingsCorpSPGlobalOnePage(
			bearer="Bearer test",
			token="key",
			date_ref=date(2025, 1, 1),  # noqa: S106
		)
	assert obj.date_ref == date(2025, 1, 1)
