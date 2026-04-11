"""Unit tests for SlickChartsIndexesComponents ingestion class."""

from datetime import date
from typing import Union
from unittest.mock import MagicMock, patch

from lxml.html import HtmlElement
import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.us.registries.slickcharts_indexes_components import (
    _ALL_SOURCES,
    _SOURCE_CONFIG,
    SlickChartsIndexesComponents,
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
    """Provide a fixed sample date for deterministic tests.

    Returns
    -------
    date
        Fixed reference date.
    """
    return date(2025, 1, 2)


@pytest.fixture
def slick_instance(sample_date: date) -> SlickChartsIndexesComponents:
    """Provide an initialized SlickChartsIndexesComponents instance.

    Parameters
    ----------
    sample_date : date
        Reference date used during initialization.

    Returns
    -------
    SlickChartsIndexesComponents
        Initialized instance.
    """
    return SlickChartsIndexesComponents(date_ref=sample_date)


_MINIMAL_HTML = b"""
<html><body>
<table class="table table-hover table-borderless table-sm">
<tbody>
<tr><td>1</td><td><a>Apple Inc.</a></td><td><a>AAPL</a></td><td>7.10%</td></tr>
<tr><td>2</td><td><a>Microsoft Corp.</a></td><td><a>MSFT</a></td><td>6.50%</td></tr>
</tbody>
</table>
</body></html>
"""


@pytest.fixture
def mock_html_response() -> Response:
    """Provide a mocked Response with minimal HTML content.

    Returns
    -------
    Response
        Mocked response object.
    """
    response = MagicMock(spec=Response)
    response.status_code = 200
    response.raise_for_status = MagicMock()
    response.content = _MINIMAL_HTML
    response.text = _MINIMAL_HTML.decode("utf-8")
    return response


@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> object:
    """Mock requests.get to prevent real HTTP calls.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture.

    Returns
    -------
    object
        Mocked requests.get.
    """
    return mocker.patch(
        "stpstone.ingestion.countries.us.registries.slickcharts_indexes_components.requests.get"
    )


@pytest.fixture
def mock_backoff(mocker: MockerFixture) -> object:
    """Mock backoff.on_exception to bypass retry delays.

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


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(sample_date: date) -> None:
    """Test initialization with explicit inputs sets all attributes correctly.

    Parameters
    ----------
    sample_date : date
        Reference date.

    Returns
    -------
    None
    """
    instance = SlickChartsIndexesComponents(date_ref=sample_date)
    assert instance.date_ref == sample_date
    assert isinstance(instance.cls_dates_current, DatesCurrent)
    assert isinstance(instance.cls_dates_br, DatesBRAnbima)
    assert isinstance(instance.cls_create_log, CreateLog)
    assert isinstance(instance.cls_dir_files_management, DirFilesManagement)
    assert instance.list_slugs == _ALL_SOURCES


def test_init_default_date_falls_back_to_previous_working_day() -> None:
    """Test that omitting date_ref uses the previous working day.

    Returns
    -------
    None
    """
    with patch.object(
        DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 2)
    ) as mock_add:
        instance = SlickChartsIndexesComponents()
        assert instance.date_ref == date(2025, 1, 2)
        mock_add.assert_called_once()


def test_init_custom_slugs(sample_date: date) -> None:
    """Test initialization with a custom slug list.

    Parameters
    ----------
    sample_date : date
        Reference date.

    Returns
    -------
    None
    """
    instance = SlickChartsIndexesComponents(date_ref=sample_date, list_slugs=["sp500"])
    assert instance.list_slugs == ["sp500"]


def test_source_config_covers_all_sources() -> None:
    """Test that all slugs in _ALL_SOURCES have entries in _SOURCE_CONFIG.

    Returns
    -------
    None
    """
    for slug in _ALL_SOURCES:
        assert slug in _SOURCE_CONFIG
        assert "url" in _SOURCE_CONFIG[slug]
        assert "xpath_list_tr" in _SOURCE_CONFIG[slug]
        assert "table_name" in _SOURCE_CONFIG[slug]


def test_get_response_success(
    slick_instance: SlickChartsIndexesComponents,
    mock_requests_get: object,
    mock_html_response: Response,
    mock_backoff: object,
) -> None:
    """Test get_response returns a Response on success.

    Parameters
    ----------
    slick_instance : SlickChartsIndexesComponents
        Initialized instance.
    mock_requests_get : object
        Mocked requests.get.
    mock_html_response : Response
        Mocked successful response.
    mock_backoff : object
        Mocked backoff decorator.

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_html_response
    result = slick_instance.get_response()
    assert result is mock_html_response
    mock_html_response.raise_for_status.assert_called_once()


def test_get_response_raises_on_http_error(
    slick_instance: SlickChartsIndexesComponents,
    mock_requests_get: object,
    mock_backoff: object,
) -> None:
    """Test that get_response propagates HTTPError.

    Parameters
    ----------
    slick_instance : SlickChartsIndexesComponents
        Initialized instance.
    mock_requests_get : object
        Mocked requests.get.
    mock_backoff : object
        Mocked backoff decorator.

    Returns
    -------
    None
    """
    import requests as req_lib

    bad_response = MagicMock(spec=Response)
    bad_response.raise_for_status.side_effect = req_lib.exceptions.HTTPError("503")
    mock_requests_get.return_value = bad_response
    with pytest.raises(req_lib.exceptions.HTTPError):
        slick_instance.get_response()


def test_parse_raw_file_returns_html_element(
    slick_instance: SlickChartsIndexesComponents,
    mock_html_response: Response,
) -> None:
    """Test that parse_raw_file returns an lxml HtmlElement.

    Parameters
    ----------
    slick_instance : SlickChartsIndexesComponents
        Initialized instance.
    mock_html_response : Response
        Mocked response.

    Returns
    -------
    None
    """
    root = slick_instance.parse_raw_file(mock_html_response)
    assert isinstance(root, HtmlElement)


def test_transform_data_returns_empty_when_no_rows(
    slick_instance: SlickChartsIndexesComponents,
    mock_html_response: Response,
) -> None:
    """Test transform_data returns an empty DataFrame when no matching rows.

    Parameters
    ----------
    slick_instance : SlickChartsIndexesComponents
        Initialized instance.
    mock_html_response : Response
        Mocked response.

    Returns
    -------
    None
    """
    empty_html = b"<html><body><table class='table'><tbody></tbody></table></body></html>"
    mock_html_response.content = empty_html
    mock_html_response.text = empty_html.decode("utf-8")
    root = slick_instance.parse_raw_file(mock_html_response)
    df_ = slick_instance.transform_data(root=root)
    assert isinstance(df_, pd.DataFrame)
    assert df_.empty


def test_run_without_db_returns_dataframe(
    slick_instance: SlickChartsIndexesComponents,
    mock_requests_get: object,
    mock_html_response: Response,
    mock_backoff: object,
) -> None:
    """Test run returns a concatenated DataFrame when no cls_db is set.

    Parameters
    ----------
    slick_instance : SlickChartsIndexesComponents
        Initialized instance.
    mock_requests_get : object
        Mocked requests.get.
    mock_html_response : Response
        Mocked successful response.
    mock_backoff : object
        Mocked backoff decorator.

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_html_response
    sample_df = pd.DataFrame({"NUM_COMPANY": [1], "TICKER": ["AAPL"]})
    with patch.object(slick_instance, "get_response", return_value=mock_html_response), \
            patch.object(slick_instance, "parse_raw_file", return_value=MagicMock()), \
            patch.object(slick_instance, "transform_data", return_value=sample_df), \
            patch.object(
                slick_instance, "standardize_dataframe", return_value=sample_df
            ):
        result = slick_instance.run()
    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(slick_instance.list_slugs)


def test_run_with_db_calls_insert_and_returns_none(
    slick_instance: SlickChartsIndexesComponents,
    mock_requests_get: object,
    mock_html_response: Response,
    mock_backoff: object,
) -> None:
    """Test run calls insert_table_db and returns None when cls_db is set.

    Parameters
    ----------
    slick_instance : SlickChartsIndexesComponents
        Initialized instance.
    mock_requests_get : object
        Mocked requests.get.
    mock_html_response : Response
        Mocked successful response.
    mock_backoff : object
        Mocked backoff decorator.

    Returns
    -------
    None
    """
    mock_db = MagicMock()
    slick_instance.cls_db = mock_db
    sample_df = pd.DataFrame({"NUM_COMPANY": [1], "TICKER": ["AAPL"]})
    with patch.object(slick_instance, "get_response", return_value=mock_html_response), \
            patch.object(slick_instance, "parse_raw_file", return_value=MagicMock()), \
            patch.object(slick_instance, "transform_data", return_value=sample_df), \
            patch.object(
                slick_instance, "standardize_dataframe", return_value=sample_df
            ), \
            patch.object(slick_instance, "insert_table_db") as mock_insert:
        result = slick_instance.run()
    assert result is None
    assert mock_insert.call_count == len(slick_instance.list_slugs)


@pytest.mark.parametrize("timeout", [10, 10.5, (5.0, 10.0), (5, 10)])
def test_get_response_timeout_variations(
    slick_instance: SlickChartsIndexesComponents,
    mock_requests_get: object,
    mock_html_response: Response,
    mock_backoff: object,
    timeout: Union[int, float, tuple],
) -> None:
    """Test get_response accepts all valid timeout types.

    Parameters
    ----------
    slick_instance : SlickChartsIndexesComponents
        Initialized instance.
    mock_requests_get : object
        Mocked requests.get.
    mock_html_response : Response
        Mocked successful response.
    mock_backoff : object
        Mocked backoff decorator.
    timeout : Union[int, float, tuple]
        Timeout value to test.

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_html_response
    result = slick_instance.get_response(timeout=timeout)
    assert result is mock_html_response


def test_reload_module() -> None:
    """Test module reloading preserves class contract.

    Returns
    -------
    None
    """
    import importlib

    import stpstone.ingestion.countries.us.registries.slickcharts_indexes_components as mod

    original = SlickChartsIndexesComponents(date_ref=date(2025, 1, 2))
    importlib.reload(mod)
    fresh = mod.SlickChartsIndexesComponents(date_ref=date(2025, 1, 2))
    assert fresh.list_slugs == original.list_slugs
