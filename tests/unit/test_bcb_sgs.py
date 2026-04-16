"""Unit tests for BCBSGS class.

Tests the BCB SGS data ingestion functionality with various scenarios including:
- Initialization with valid and invalid inputs
- Data retrieval and transformation
- Metadata fetching with Playwright and fallback
- Error handling and edge cases
"""

from datetime import date
import importlib
import sys
from typing import Any, Union
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.macroeconomics.bcb_sgs import BCBSGS
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def default_bcbsgs() -> BCBSGS:
	"""Fixture providing a default BCBSGS instance.

	Returns
	-------
	BCBSGS
		Instance initialized with default parameters
	"""
	return BCBSGS()


@pytest.fixture
def sample_series_codes() -> list[int]:
	"""Fixture providing sample series codes.

	Returns
	-------
	list[int]
		List of sample series codes
	"""
	return [11, 4390]


@pytest.fixture
def sample_dates() -> tuple[date, date]:
	"""Fixture providing sample date range.

	Returns
	-------
	tuple[date, date]
		Tuple of start and end dates
	"""
	return (date(2023, 1, 1), date(2023, 12, 31))


@pytest.fixture
def mock_response() -> Response:
	"""Fixture providing a mock Response object.

	Returns
	-------
	Response
		Mocked response with sample JSON data
	"""
	response = MagicMock(spec=Response)
	response.status_code = 200
	response.json.return_value = [
		{"data": "01/01/2023", "valor": "13.75"},
		{"data": "02/01/2023", "valor": "13.75"},
	]
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def mock_playwright_page(mocker: MockerFixture) -> MagicMock:
	"""Fixture providing a mock Playwright page.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	MagicMock
		Mocked Playwright page
	"""
	page = MagicMock()
	page.locator.return_value.all.return_value = [
		MagicMock(
			locator=lambda x: [
				MagicMock(text_content=lambda: "Full name"),
				MagicMock(text_content=lambda: "Taxa de juros - Selic"),
				MagicMock(text_content=lambda: ""),
				MagicMock(text_content=lambda: "Selic"),
			]
		),
		MagicMock(
			locator=lambda x: [
				MagicMock(text_content=lambda: "Periodicity"),
				MagicMock(text_content=lambda: ""),
				MagicMock(text_content=lambda: ""),
				MagicMock(text_content=lambda: "Diária"),
			]
		),
		MagicMock(
			locator=lambda x: [
				MagicMock(text_content=lambda: "Unit"),
				MagicMock(text_content=lambda: "% a.d."),
				MagicMock(text_content=lambda: ""),
				MagicMock(text_content=lambda: ""),
			]
		),
		MagicMock(
			locator=lambda x: [
				MagicMock(text_content=lambda: "Start date"),
				MagicMock(text_content=lambda: "4/6/1986"),
				MagicMock(text_content=lambda: ""),
				MagicMock(text_content=lambda: ""),
			]
		),
	]
	return page


@pytest.fixture
def mock_playwright_scraper(mocker: MockerFixture, mock_playwright_page: MagicMock) -> MagicMock:
	"""Fixture providing a mock PlaywrightScraper.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks
	mock_playwright_page : MagicMock
		Mocked Playwright page from fixture

	Returns
	-------
	MagicMock
		Mocked PlaywrightScraper instance
	"""
	scraper = MagicMock(spec=PlaywrightScraper)
	scraper.launch.return_value.__enter__.return_value = scraper
	scraper.navigate.return_value = True
	scraper.page = mock_playwright_page
	scraper.page.locator.return_value.content_frame.return_value = mock_playwright_page
	mocker.patch(
		"stpstone.ingestion.countries.br.macroeconomics.bcb_sgs.PlaywrightScraper",
		return_value=scraper,
	)
	return scraper


# --------------------------
# Tests
# --------------------------
def test_init_default_values() -> None:
	"""Test initialization with default values.

	Verifies
	--------
	- Default series codes are set correctly
	- Default date range is set (30 days ago to yesterday)
	- Logger and database session are None by default
	- URLs are correctly formatted

	Returns
	-------
	None
	"""
	bcbsgs = BCBSGS()
	assert bcbsgs.list_series_codes == [11, 4390, 433, 3695, 190]
	assert isinstance(bcbsgs.date_start, date)
	assert isinstance(bcbsgs.date_end, date)
	assert bcbsgs.logger is None
	assert bcbsgs.cls_db is None
	assert bcbsgs.url_data.startswith("https://api.bcb.gov.br")
	assert bcbsgs.url_metadata.startswith("https://www3.bcb.gov.br")


@pytest.mark.parametrize("invalid_codes", [[""], [1.5], ["11"], [[11]], [[]]])
def test_init_invalid_series_codes(invalid_codes: list[Any]) -> None:
	"""Test initialization with invalid series codes.

	Parameters
	----------
	invalid_codes : list[Any]
		Invalid series code inputs to test

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be one of types|must be of type"):
		BCBSGS(list_series_codes=invalid_codes)


@pytest.mark.parametrize("invalid_codes_with_none", [[None]])
def test_init_invalid_series_codes_with_none(invalid_codes_with_none: list[Any]) -> None:
	"""Test initialization with None in series codes list.

	Parameters
	----------
	invalid_codes_with_none : list[Any]
		Invalid series code inputs containing None

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be one of types|must be of type"):
		BCBSGS(list_series_codes=invalid_codes_with_none)


def test_init_with_none_series_codes() -> None:
	"""Test initialization with None as series codes (should use defaults).

	Returns
	-------
	None
	"""
	bcbsgs = BCBSGS(list_series_codes=None)
	assert bcbsgs.list_series_codes == [11, 4390, 433, 3695, 190]


@pytest.mark.parametrize("invalid_date", ["2023-01-01", 123, []])
def test_init_invalid_dates(invalid_date: Any) -> None:  # noqa ANN401: typing.Any is not allowed
	"""Test initialization with invalid date inputs.

	Parameters
	----------
	invalid_date : Any
		Invalid date inputs to test

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be one of types|must be of type"):
		BCBSGS(date_start=invalid_date)
	with pytest.raises(TypeError, match="must be one of types|must be of type"):
		BCBSGS(date_end=invalid_date)


def test_init_with_none_dates() -> None:
	"""Test initialization with None dates (should use defaults).

	Returns
	-------
	None
	"""
	bcbsgs = BCBSGS(date_start=None, date_end=None)
	assert bcbsgs.date_start is not None
	assert bcbsgs.date_end is not None
	assert isinstance(bcbsgs.date_start, date)
	assert isinstance(bcbsgs.date_end, date)


def test_run_happy_path(
	default_bcbsgs: BCBSGS, mock_response: Response, mocker: MockerFixture
) -> None:
	"""Test run method with valid inputs.

	Verifies
	--------
	- Successful data retrieval and transformation
	- Correct DataFrame structure and columns
	- Metadata integration

	Parameters
	----------
	default_bcbsgs : BCBSGS
		Default BCBSGS instance from fixture
	mock_response : Response
		Mocked response object from fixture
	mocker : MockerFixture
		Pytest-mock fixture for mocking requests

	Returns
	-------
	None
	"""
	mocker.patch("requests.get", return_value=mock_response)
	mocker.patch.object(
		default_bcbsgs,
		"get_series_metadata",
		return_value=("Taxa de juros - Selic", "Selic", "Diária", "% a.d.", "4/6/1986"),
	)

	df_ = default_bcbsgs.run()

	assert isinstance(df_, pd.DataFrame)
	assert set(df_.columns) >= {"DATA", "VALOR", "CODIGO_SERIE", "NOME_COMPLETO"}
	assert len(df_) == 2 * len(default_bcbsgs.list_series_codes)  # Two records per series
	assert df_["CODIGO_SERIE"].iloc[0] == 11
	assert df_["NOME_COMPLETO"].iloc[0] == "Taxa de juros - Selic"


def test_run_empty_response(default_bcbsgs: BCBSGS, mocker: MockerFixture) -> None:
	"""Test run method with empty API response.

	Verifies
	--------
	- Handles empty JSON response correctly
	- Raises RuntimeError when DataFrame is empty due to pipeline validation

	Parameters
	----------
	default_bcbsgs : BCBSGS
		Default BCBSGS instance from fixture
	mocker : MockerFixture
		Pytest-mock fixture for mocking requests

	Returns
	-------
	None
	"""
	mock_response = MagicMock(spec=Response)
	mock_response.status_code = 200
	mock_response.json.return_value = []
	mock_response.raise_for_status = MagicMock()
	mocker.patch("requests.get", return_value=mock_response)
	mocker.patch.object(
		default_bcbsgs,
		"get_series_metadata",
		return_value=("Taxa de juros - Selic", "Selic", "Diária", "% a.d.", "4/6/1986"),
	)

	with pytest.raises(RuntimeError, match="Error in check_if_empty: DataFrame is empty"):
		default_bcbsgs.run()


def test_run_empty_response_with_mocked_pipeline(
	default_bcbsgs: BCBSGS, mocker: MockerFixture
) -> None:
	"""Test run method with empty API response using mocked pipeline.

	Verifies
	--------
	- Returns empty DataFrame when pipeline is mocked to allow empty data

	Parameters
	----------
	default_bcbsgs : BCBSGS
		Default BCBSGS instance from fixture
	mocker : MockerFixture
		Pytest-mock fixture for mocking requests

	Returns
	-------
	None
	"""
	mock_response = MagicMock(spec=Response)
	mock_response.status_code = 200
	mock_response.json.return_value = []
	mock_response.raise_for_status = MagicMock()
	mocker.patch("requests.get", return_value=mock_response)
	mocker.patch.object(
		default_bcbsgs,
		"get_series_metadata",
		return_value=("Taxa de juros - Selic", "Selic", "Diária", "% a.d.", "4/6/1986"),
	)

	# Mock standardize_dataframe to return empty DataFrame without validation
	empty_df = pd.DataFrame()
	mocker.patch.object(default_bcbsgs, "standardize_dataframe", return_value=empty_df)

	df_ = default_bcbsgs.run()

	assert isinstance(df_, pd.DataFrame)
	assert df_.empty


def test_get_series_metadata_from_cache(default_bcbsgs: BCBSGS) -> None:
	"""Test getting series metadata from cache.

	Verifies
	--------
	- Returns cached metadata when available
	- Cache is used instead of fetching new data

	Parameters
	----------
	default_bcbsgs : BCBSGS
		Default BCBSGS instance from fixture

	Returns
	-------
	None
	"""
	default_bcbsgs._series_names_cache[11] = (
		"Taxa de juros - Selic",
		"Selic",
		"Diária",
		"% a.d.",
		"4/6/1986",
	)

	result = default_bcbsgs.get_series_metadata(11)

	assert result == ("Taxa de juros - Selic", "Selic", "Diária", "% a.d.", "4/6/1986")


def test_get_series_metadata_fallback(default_bcbsgs: BCBSGS, mocker: MockerFixture) -> None:
	"""Test getting series metadata using fallback mapping.

	Verifies
	--------
	- Falls back to mapping when Playwright fails
	- Returns correct metadata from mapping

	Parameters
	----------
	default_bcbsgs : BCBSGS
		Default BCBSGS instance from fixture
	mocker : MockerFixture
		Pytest-mock fixture for mocking Playwright

	Returns
	-------
	None
	"""
	mocker.patch.object(default_bcbsgs, "_fetch_series_name_with_playwright", return_value=None)

	result = default_bcbsgs.get_series_metadata(11)

	assert result == ("Taxa de juros - Selic", "Selic", "Diária", "% a.d.", "4/6/1986")


def test_fetch_series_name_with_playwright_failure(
	default_bcbsgs: BCBSGS, mocker: MockerFixture
) -> None:
	"""Test metadata fetching failure with Playwright.

	Verifies
	--------
	- Returns None when Playwright navigation fails
	- Logs appropriate error message

	Parameters
	----------
	default_bcbsgs : BCBSGS
		Default BCBSGS instance from fixture
	mocker : MockerFixture
		Pytest-mock fixture for mocking Playwright

	Returns
	-------
	None
	"""
	mock_ctx = MagicMock()
	mock_ctx.__enter__ = MagicMock(return_value=None)
	mock_ctx.__exit__ = MagicMock(return_value=False)
	mocker.patch.object(default_bcbsgs.cls_scraper, "launch", return_value=mock_ctx)
	mocker.patch.object(default_bcbsgs.cls_scraper, "navigate", return_value=False)
	mocker.patch.object(default_bcbsgs.cls_create_log, "log_message")

	result = default_bcbsgs._fetch_series_name_with_playwright(11)

	assert result is None
	default_bcbsgs.cls_create_log.log_message.assert_called_once()


def test_parse_raw_file(default_bcbsgs: BCBSGS, mock_response: Response) -> None:
	"""Test parsing raw file content.

	Verifies
	--------
	- Correctly parses JSON response into list of dictionaries
	- Maintains data integrity

	Parameters
	----------
	default_bcbsgs : BCBSGS
		Default BCBSGS instance from fixture
	mock_response : Response
		Mocked response object from fixture

	Returns
	-------
	None
	"""
	result = default_bcbsgs.parse_raw_file(mock_response)

	assert isinstance(result, list)
	assert len(result) == 2
	assert result[0]["data"] == "01/01/2023"
	assert result[0]["valor"] == "13.75"


def test_transform_data(default_bcbsgs: BCBSGS) -> None:
	"""Test data transformation to DataFrame.

	Verifies
	--------
	- Correctly transforms JSON data to DataFrame
	- Maintains expected column names and data types

	Parameters
	----------
	default_bcbsgs : BCBSGS
		Default BCBSGS instance from fixture

	Returns
	-------
	None
	"""
	json_data = [{"data": "01/01/2023", "valor": "13.75"}]
	df_ = default_bcbsgs.transform_data(json_data)

	assert isinstance(df_, pd.DataFrame)
	assert set(df_.columns) == {"data", "valor"}
	assert len(df_) == 1
	assert df_["data"].iloc[0] == "01/01/2023"
	assert df_["valor"].iloc[0] == "13.75"


def test_run_with_db_session(
	default_bcbsgs: BCBSGS, mock_response: Response, mocker: MockerFixture
) -> None:
	"""Test run method with database session.

	Verifies
	--------
	- Calls insert_table_db when database session is provided
	- Does not return DataFrame when database session is present

	Parameters
	----------
	default_bcbsgs : BCBSGS
		Default BCBSGS instance from fixture
	mock_response : Response
		Mocked response object from fixture
	mocker : MockerFixture
		Pytest-mock fixture for mocking requests and database

	Returns
	-------
	None
	"""
	mock_db = MagicMock()
	bcbsgs = BCBSGS(cls_db=mock_db)
	mocker.patch("requests.get", return_value=mock_response)
	mocker.patch.object(
		bcbsgs,
		"get_series_metadata",
		return_value=("Taxa de juros - Selic", "Selic", "Diária", "% a.d.", "4/6/1986"),
	)
	mocker.patch.object(bcbsgs, "insert_table_db")

	result = bcbsgs.run()

	assert result is None
	bcbsgs.insert_table_db.assert_called_once()


def test_reload_module() -> None:
	"""Test module reloading behavior.

	Verifies
	--------
	- Module can be reloaded without errors
	- Class attributes are properly initialized after reload

	Returns
	-------
	None
	"""
	importlib.reload(sys.modules["stpstone.ingestion.countries.br.macroeconomics.bcb_sgs"])
	bcbsgs = BCBSGS()
	assert bcbsgs.list_series_codes == [11, 4390, 433, 3695, 190]
	assert isinstance(bcbsgs.cls_scraper, PlaywrightScraper)


@pytest.mark.parametrize("timeout", [10, 10.0, (10.0, 20.0), (10, 20)])
def test_get_response_valid_timeout(
	default_bcbsgs: BCBSGS,
	mock_response: Response,
	mocker: MockerFixture,
	timeout: Union[int, float, tuple],
) -> None:
	"""Test get_response with valid timeout values.

	Verifies
	--------
	- Accepts various timeout formats
	- Successfully returns response object

	Parameters
	----------
	default_bcbsgs : BCBSGS
		Default BCBSGS instance from fixture
	mock_response : Response
		Mocked response object from fixture
	mocker : MockerFixture
		Pytest-mock fixture for mocking requests
	timeout : Union[int, float, tuple]
		Valid timeout values to test

	Returns
	-------
	None
	"""
	mocker.patch("requests.get", return_value=mock_response)

	result = default_bcbsgs.get_response("https://test.com", timeout=timeout)

	assert result == mock_response


@pytest.mark.parametrize("invalid_timeout", ["10", [10], {}])
def test_get_response_invalid_timeout(
	default_bcbsgs: BCBSGS,
	mocker: MockerFixture,
	invalid_timeout: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test get_response with invalid timeout values.

	Verifies
	--------
	- Raises TypeError for invalid timeout types

	Parameters
	----------
	default_bcbsgs : BCBSGS
		Default BCBSGS instance from fixture
	mocker : MockerFixture
		Pytest-mock fixture for mocking requests
	invalid_timeout : Any
		Invalid timeout values to test

	Returns
	-------
	None
	"""
	mocker.patch("requests.get")
	with pytest.raises(TypeError, match="must be one of types|must be of type"):
		default_bcbsgs.get_response("https://test.com", timeout=invalid_timeout)


def test_get_response_none_timeout(default_bcbsgs: BCBSGS, mocker: MockerFixture) -> None:
	"""Test get_response with None timeout (should be allowed).

	Verifies
	--------
	- None timeout is accepted (Union includes NoneType)
	- Successfully returns response object

	Parameters
	----------
	default_bcbsgs : BCBSGS
		Default BCBSGS instance from fixture
	mocker : MockerFixture
		Pytest-mock fixture for mocking requests

	Returns
	-------
	None
	"""
	mock_response = MagicMock(spec=Response)
	mock_response.status_code = 200
	mocker.patch("requests.get", return_value=mock_response)

	result = default_bcbsgs.get_response("https://test.com", timeout=None)

	assert result == mock_response
