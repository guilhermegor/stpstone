"""Unit tests for B3TradingHoursCryptoFutures ingestion class."""

from datetime import date
from unittest.mock import MagicMock

from lxml.html import HtmlElement
import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response, Session

from stpstone.ingestion.countries.br.exchange._b3_trading_hours_core import B3TradingHoursCore
from stpstone.ingestion.countries.br.exchange.b3_trading_hours_crypto_futures import (
	B3TradingHoursCryptoFutures,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response() -> Response:
	"""Mock Response object with sample content.

	Returns
	-------
	Response
		Mocked requests Response object.
	"""
	response = MagicMock(spec=Response)
	response.content = b"<html><table><tr><td>Test</td></tr></table></html>"
	response.status_code = 200
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def mock_html_element() -> HtmlElement:
	"""Mock HtmlElement for testing.

	Returns
	-------
	HtmlElement
		Mocked lxml HtmlElement.
	"""
	html = MagicMock(spec=HtmlElement)
	html.xpath.return_value = [MagicMock(text="Test Data")]
	return html


@pytest.fixture
def mock_dataframe() -> pd.DataFrame:
	"""Mock DataFrame for testing.

	Returns
	-------
	pd.DataFrame
		Sample DataFrame with test data.
	"""
	return pd.DataFrame({"CONTRACT": ["BTC"], "TICKER": ["BTC1"]})


@pytest.fixture(autouse=True)
def mock_network(mocker: MockerFixture, mock_response: Response) -> None:
	"""Mock network operations for all tests.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks.
	mock_response : Response
		Mocked Response object.

	Returns
	-------
	None
	"""
	mocker.patch("requests.get", return_value=mock_response)
	mocker.patch("backoff.on_exception", side_effect=lambda *args, **kwargs: lambda func: func)
	mocker.patch("time.sleep")

	import backoff

	mocker.patch.object(
		backoff, "on_exception", side_effect=lambda *args, **kwargs: lambda func: func
	)


# --------------------------
# Tests
# --------------------------
def test_init_correct_url() -> None:
	"""Test that the correct B3 cryptoassets URL is set on initialization.

	Returns
	-------
	None
	"""
	instance = B3TradingHoursCryptoFutures(date_ref=date(2025, 9, 12))
	assert instance.url == (
		"https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/"
		"for-members-and-traders/trading-hours/derivatives/cryptoassets/"
	)


def test_init_inherits_from_core() -> None:
	"""Test that B3TradingHoursCryptoFutures inherits from B3TradingHoursCore.

	Returns
	-------
	None
	"""
	instance = B3TradingHoursCryptoFutures(date_ref=date(2025, 9, 12))
	assert isinstance(instance, B3TradingHoursCore)


def test_run_returns_dataframe_without_db(
	mock_response: Response,
	mock_html_element: HtmlElement,
	mock_dataframe: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run delegates to super and returns DataFrame when no db is set.

	Parameters
	----------
	mock_response : Response
		Mocked Response object.
	mock_html_element : HtmlElement
		Mocked HtmlElement.
	mock_dataframe : pd.DataFrame
		Mocked DataFrame.
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks.

	Returns
	-------
	None
	"""
	instance = B3TradingHoursCryptoFutures(date_ref=date(2025, 9, 12), cls_db=None)
	mocker.patch("requests.get", return_value=mock_response)
	mocker.patch.object(B3TradingHoursCore, "parse_raw_file", return_value=mock_html_element)
	mocker.patch.object(B3TradingHoursCore, "transform_data", return_value=mock_dataframe)
	mocker.patch.object(B3TradingHoursCore, "standardize_dataframe", return_value=mock_dataframe)

	result = instance.run(str_table_name="br_b3_trading_hours_crypto_futures")
	assert isinstance(result, pd.DataFrame)


def test_run_with_db_returns_none(
	mock_response: Response,
	mock_html_element: HtmlElement,
	mock_dataframe: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run returns None when db is set.

	Parameters
	----------
	mock_response : Response
		Mocked Response object.
	mock_html_element : HtmlElement
		Mocked HtmlElement.
	mock_dataframe : pd.DataFrame
		Mocked DataFrame.
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks.

	Returns
	-------
	None
	"""
	mock_session = MagicMock(spec=Session)
	instance = B3TradingHoursCryptoFutures(date_ref=date(2025, 9, 12), cls_db=mock_session)
	mocker.patch("requests.get", return_value=mock_response)
	mocker.patch.object(B3TradingHoursCore, "parse_raw_file", return_value=mock_html_element)
	mocker.patch.object(B3TradingHoursCore, "transform_data", return_value=mock_dataframe)
	mocker.patch.object(B3TradingHoursCore, "standardize_dataframe", return_value=mock_dataframe)
	mocker.patch.object(B3TradingHoursCore, "insert_table_db")

	result = instance.run(str_table_name="br_b3_trading_hours_crypto_futures")
	assert result is None


def test_transform_data_calls_super_with_correct_args(
	mock_html_element: HtmlElement, mocker: MockerFixture
) -> None:
	"""Test transform_data calls super with correct list_th and xpath_td.

	Parameters
	----------
	mock_html_element : HtmlElement
		Mocked HtmlElement.
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks.

	Returns
	-------
	None
	"""
	instance = B3TradingHoursCryptoFutures()
	mock_transform = mocker.patch.object(
		B3TradingHoursCore, "transform_data", return_value=pd.DataFrame()
	)

	instance.transform_data(mock_html_element)
	mock_transform.assert_called_once_with(
		html_root=mock_html_element,
		list_th=[
			"CONTRACT",
			"TICKER",
			"REGULAR_HOURS_OPENING",
			"REGULAR_HOURS_CLOSING",
			"REGULAR_HOURS_ORDER_CANCELLATION_OPENING",
			"REGULAR_HOURS_ORDER_CANCELLATION_CLOSING",
			"ELETRONIC_CALL_OPENING",
			"ELETRONIC_CALL_ORDER_CANCELLATION_OPENING",
			"ELETRONIC_CALL_ORDER_CANCELLATION_CLOSING",
			"EXTENDED_HOURS_T_0_OPENING",
			"EXTENDED_HOURS_T_0_CLOSING",
		],
		xpath_td='//*[@id="conteudo-principal"]/div[4]/div/table/tbody/tr/td',
		na_values="-",
	)
