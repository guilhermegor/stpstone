"""Unit tests for GlobalRatesCanadianCpi class."""

from datetime import date
from typing import Union
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.ww.macroeconomics.global_rates_canadian_cpi import (
    GlobalRatesCanadianCpi,
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
def mock_response() -> Response:
    """Mock HTTP Response.

    Returns
    -------
    Response
        Mocked Response object.
    """
    response = MagicMock(spec=Response)
    response.status_code = 200
    response.raise_for_status = MagicMock()
    return response


@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> object:
    """Patch requests.get to prevent real HTTP calls.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture.

    Returns
    -------
    object
        Mocked requests.get.
    """
    return mocker.patch("requests.get")


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
def instance(sample_date: date) -> GlobalRatesCanadianCpi:
    """Provide a GlobalRatesCanadianCpi instance for testing.

    Parameters
    ----------
    sample_date : date
        Fixed reference date.

    Returns
    -------
    GlobalRatesCanadianCpi
        Initialized instance.
    """
    return GlobalRatesCanadianCpi(date_ref=sample_date)


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_date(sample_date: date) -> None:
    """Test initialization sets expected attributes.

    Parameters
    ----------
    sample_date : date
        Fixed reference date.

    Returns
    -------
    None
    """
    obj = GlobalRatesCanadianCpi(date_ref=sample_date)
    assert obj.date_ref == sample_date
    assert isinstance(obj.cls_dates_current, DatesCurrent)
    assert isinstance(obj.cls_dates_br, DatesBRAnbima)
    assert isinstance(obj.cls_create_log, CreateLog)
    assert isinstance(obj.cls_dir_files_management, DirFilesManagement)
    assert "canada" in obj.url


def test_init_with_default_date() -> None:
    """Test initialization without date_ref uses the previous working day.

    Returns
    -------
    None
    """
    with patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 1)):
        obj = GlobalRatesCanadianCpi()
        assert obj.date_ref == date(2025, 1, 1)


def test_get_response_success(
    instance: GlobalRatesCanadianCpi,
    mock_requests_get: object,
    mock_response: Response,
    mock_backoff: object,
) -> None:
    """Test get_response returns mocked Response on success.

    Parameters
    ----------
    instance : GlobalRatesCanadianCpi
        Ingestion instance.
    mock_requests_get : object
        Mocked requests.get.
    mock_response : Response
        Mocked Response.
    mock_backoff : object
        Mocked backoff decorator.

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    result = instance.get_response()
    assert result is mock_response


@pytest.mark.parametrize("timeout", [10, 10.5, (5.0, 10.0), (5, 10)])
def test_get_response_timeout_variations(
    instance: GlobalRatesCanadianCpi,
    mock_requests_get: object,
    mock_response: Response,
    mock_backoff: object,
    timeout: Union[int, float, tuple],
) -> None:
    """Test get_response accepts various timeout formats.

    Parameters
    ----------
    instance : GlobalRatesCanadianCpi
        Ingestion instance.
    mock_requests_get : object
        Mocked requests.get.
    mock_response : Response
        Mocked Response.
    mock_backoff : object
        Mocked backoff decorator.
    timeout : Union[int, float, tuple]
        Timeout value to test.

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    result = instance.get_response(timeout=timeout)
    assert result is mock_response


def test_parse_raw_file_passthrough(
    instance: GlobalRatesCanadianCpi,
    mock_response: Response,
) -> None:
    """Test parse_raw_file returns the response object unchanged.

    Parameters
    ----------
    instance : GlobalRatesCanadianCpi
        Ingestion instance.
    mock_response : Response
        Mocked Response.

    Returns
    -------
    None
    """
    result = instance.parse_raw_file(mock_response)
    assert result is mock_response


def test_run_without_db(
    instance: GlobalRatesCanadianCpi,
    mock_requests_get: object,
    mock_response: Response,
    mock_backoff: object,
) -> None:
    """Test run returns a DataFrame when no cls_db is provided.

    Parameters
    ----------
    instance : GlobalRatesCanadianCpi
        Ingestion instance.
    mock_requests_get : object
        Mocked requests.get.
    mock_response : Response
        Mocked Response.
    mock_backoff : object
        Mocked backoff decorator.

    Returns
    -------
    None
    """
    with patch.object(
        instance, "get_response", return_value=mock_response
    ), patch.object(
        instance, "transform_data", return_value=pd.DataFrame({"DATE": ["2025-01"]})
    ), patch.object(
        instance, "standardize_dataframe", return_value=pd.DataFrame({"DATE": ["2025-01"]})
    ):
        result = instance.run()
    assert isinstance(result, pd.DataFrame)


def test_run_with_db(
    instance: GlobalRatesCanadianCpi,
    mock_requests_get: object,
    mock_response: Response,
    mock_backoff: object,
) -> None:
    """Test run calls insert_table_db and returns None when cls_db is set.

    Parameters
    ----------
    instance : GlobalRatesCanadianCpi
        Ingestion instance.
    mock_requests_get : object
        Mocked requests.get.
    mock_response : Response
        Mocked Response.
    mock_backoff : object
        Mocked backoff decorator.

    Returns
    -------
    None
    """
    instance.cls_db = MagicMock()
    with patch.object(
        instance, "get_response", return_value=mock_response
    ), patch.object(
        instance, "transform_data", return_value=pd.DataFrame({"DATE": ["2025-01"]})
    ), patch.object(
        instance, "standardize_dataframe", return_value=pd.DataFrame({"DATE": ["2025-01"]})
    ), patch.object(instance, "insert_table_db") as mock_insert:
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

    import stpstone.ingestion.countries.ww.macroeconomics.global_rates_canadian_cpi as mod

    importlib.reload(mod)
    obj = mod.GlobalRatesCanadianCpi(date_ref=date(2025, 1, 1))
    assert obj.date_ref == date(2025, 1, 1)
