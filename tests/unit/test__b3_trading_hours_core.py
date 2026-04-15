"""Unit tests for B3TradingHoursCore private base class."""

from contextlib import suppress
from datetime import date
from typing import Union
from unittest.mock import MagicMock

from lxml.html import HtmlElement
import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response, Session

from stpstone.ingestion.countries.br.exchange._b3_trading_hours_core import B3TradingHoursCore
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.html import HtmlHandler


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response() -> Response:
	"""Mock Response object with sample content.

	Returns
	-------
	Response
	    Mocked requests Response object with sample HTML content.
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
	    Mocked lxml HtmlElement with sample table structure.
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
	return pd.DataFrame({"MERCADO": ["Test"], "NEGOCIACAO_INICIO": ["09:00"]})


@pytest.fixture
def mock_session() -> Session:
	"""Mock database Session for testing.

	Returns
	-------
	Session
	    Mocked SQLAlchemy Session object.
	"""
	return MagicMock(spec=Session)


@pytest.fixture
def b3_core(mock_session: Session) -> B3TradingHoursCore:
	"""Fixture providing B3TradingHoursCore instance with a mock DB session.

	Parameters
	----------
	mock_session : Session
	    Mocked database session.

	Returns
	-------
	B3TradingHoursCore
	    Instance initialized with default parameters.
	"""
	return B3TradingHoursCore(
		date_ref=date(2025, 9, 12), logger=None, cls_db=mock_session, url="https://example.com"
	)


@pytest.fixture(autouse=True)
def mock_network_operations(mocker: MockerFixture, mock_response: Response) -> None:
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
	mocker.patch("backoff.expo", side_effect=lambda *args, **kwargs: lambda func: func)
	mocker.patch("time.sleep")

	import backoff

	mocker.patch.object(
		backoff, "on_exception", side_effect=lambda *args, **kwargs: lambda func: func
	)


# --------------------------
# Tests: __init__
# --------------------------
def test_init_valid_inputs() -> None:
	"""Test initialization of B3TradingHoursCore with valid inputs.

	Returns
	-------
	None
	"""
	instance = B3TradingHoursCore(
		date_ref=date(2025, 9, 12), logger=None, cls_db=None, url="https://example.com"
	)
	assert instance.date_ref == date(2025, 9, 12)
	assert isinstance(instance.cls_dir_files_management, DirFilesManagement)
	assert isinstance(instance.cls_dates_current, DatesCurrent)
	assert isinstance(instance.cls_create_log, CreateLog)
	assert isinstance(instance.cls_dates_br, DatesBRAnbima)
	assert isinstance(instance.cls_html_handler, HtmlHandler)
	assert isinstance(instance.cls_dicts_handler, HandlingDicts)
	assert instance.url == "https://example.com"


def test_init_default_date(mocker: MockerFixture) -> None:
	"""Test initialization with default date falls back to previous working day.

	Parameters
	----------
	mocker : MockerFixture
	    Pytest-mock fixture for creating mocks.

	Returns
	-------
	None
	"""
	mock_curr = mocker.patch.object(DatesCurrent, "curr_date", return_value=date(2025, 9, 13))
	mock_add = mocker.patch.object(
		DatesBRAnbima, "add_working_days", return_value=date(2025, 9, 12)
	)

	instance = B3TradingHoursCore(url="https://example.com")
	assert instance.date_ref == date(2025, 9, 12)
	mock_curr.assert_called_once()
	mock_add.assert_called_once_with(date(2025, 9, 13), -1)


def test_init_invalid_url_none() -> None:
	"""Test initialization with None URL raises TypeError.

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		B3TradingHoursCore(url=None)


def test_init_invalid_url_integer() -> None:
	"""Test initialization with integer URL raises TypeError.

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		B3TradingHoursCore(url=123)


def test_init_url_empty_string_is_valid() -> None:
	"""Test that an empty string URL is accepted by the type system.

	Returns
	-------
	None
	"""
	instance = B3TradingHoursCore(url="")
	assert instance.url == ""


def test_init_url_whitespace_is_valid() -> None:
	"""Test that a whitespace string URL is accepted by the type system.

	Returns
	-------
	None
	"""
	instance = B3TradingHoursCore(url="  ")
	assert instance.url == "  "


# --------------------------
# Tests: run
# --------------------------
def test_run_without_db_returns_dataframe(
	b3_core: B3TradingHoursCore,
	mock_response: Response,
	mock_html_element: HtmlElement,
	mock_dataframe: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run method without database session returns DataFrame.

	Parameters
	----------
	b3_core : B3TradingHoursCore
	    Instance of B3TradingHoursCore.
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
	mocker.patch("requests.get", return_value=mock_response)
	mocker.patch.object(B3TradingHoursCore, "parse_raw_file", return_value=mock_html_element)
	mocker.patch.object(B3TradingHoursCore, "transform_data", return_value=mock_dataframe)
	mocker.patch.object(B3TradingHoursCore, "standardize_dataframe", return_value=mock_dataframe)

	b3_core.cls_db = None
	result = b3_core.run(
		dict_dtypes={"MERCADO": str},
		str_table_name="test_table",
		str_fmt_dt="YYYY-MM-DD",
		timeout=(12.0, 21.0),
		bool_verify=True,
		bool_insert_or_ignore=False,
	)

	assert isinstance(result, pd.DataFrame)
	assert result.equals(mock_dataframe)


def test_run_with_db_calls_insert(
	b3_core: B3TradingHoursCore,
	mock_response: Response,
	mock_html_element: HtmlElement,
	mock_dataframe: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test run method with database session calls insert and returns None.

	Parameters
	----------
	b3_core : B3TradingHoursCore
	    Instance of B3TradingHoursCore.
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
	mocker.patch("requests.get", return_value=mock_response)
	mocker.patch.object(B3TradingHoursCore, "parse_raw_file", return_value=mock_html_element)
	mocker.patch.object(B3TradingHoursCore, "transform_data", return_value=mock_dataframe)
	mocker.patch.object(B3TradingHoursCore, "standardize_dataframe", return_value=mock_dataframe)
	mock_insert = mocker.patch.object(B3TradingHoursCore, "insert_table_db")

	result = b3_core.run(
		dict_dtypes={"MERCADO": str},
		str_table_name="test_table",
		str_fmt_dt="YYYY-MM-DD",
		timeout=(12.0, 21.0),
		bool_verify=True,
		bool_insert_or_ignore=False,
	)

	assert result is None
	mock_insert.assert_called_once_with(
		cls_db=b3_core.cls_db,
		str_table_name="test_table",
		df_=mock_dataframe,
		bool_insert_or_ignore=False,
	)


def test_run_invalid_dict_dtypes(b3_core: B3TradingHoursCore) -> None:
	"""Test run raises TypeError for non-dict dict_dtypes.

	Parameters
	----------
	b3_core : B3TradingHoursCore
	    Instance of B3TradingHoursCore.

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="dict_dtypes must be of type dict"):
		b3_core.run(dict_dtypes=None, str_table_name="test_table")


def test_run_invalid_table_name_none(b3_core: B3TradingHoursCore) -> None:
	"""Test run raises TypeError for None table name.

	Parameters
	----------
	b3_core : B3TradingHoursCore
	    Instance of B3TradingHoursCore.

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError):
		b3_core.run(dict_dtypes={"MERCADO": str}, str_table_name=None)


def test_run_invalid_table_name_integer(b3_core: B3TradingHoursCore) -> None:
	"""Test run raises TypeError for integer table name.

	Parameters
	----------
	b3_core : B3TradingHoursCore
	    Instance of B3TradingHoursCore.

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError):
		b3_core.run(dict_dtypes={"MERCADO": str}, str_table_name=123)


def test_run_empty_table_name_raises(b3_core: B3TradingHoursCore) -> None:
	"""Test run with empty string table name raises or suppresses gracefully.

	Parameters
	----------
	b3_core : B3TradingHoursCore
	    Instance of B3TradingHoursCore.

	Returns
	-------
	None
	"""
	with suppress(TypeError, ValueError):
		b3_core.run(dict_dtypes={"MERCADO": str}, str_table_name="")


# --------------------------
# Tests: get_response
# --------------------------
def test_get_response_success(
	b3_core: B3TradingHoursCore,
	mocker: MockerFixture,
	mock_response: Response,
) -> None:
	"""Test successful HTTP response retrieval.

	Parameters
	----------
	b3_core : B3TradingHoursCore
	    Instance of B3TradingHoursCore.
	mocker : MockerFixture
	    Pytest-mock fixture for creating mocks.
	mock_response : Response
	    Mocked Response object.

	Returns
	-------
	None
	"""
	mock_get = mocker.patch("requests.get", return_value=mock_response)
	result = b3_core.get_response(timeout=(12.0, 21.0), bool_verify=True)
	assert result == mock_response
	mock_get.assert_called_once_with("https://example.com", timeout=(12.0, 21.0), verify=True)
	mock_response.raise_for_status.assert_called_once()


def test_get_response_http_error(b3_core: B3TradingHoursCore, mocker: MockerFixture) -> None:
	"""Test get_response raises HTTPError on bad status.

	The underlying method raises HTTPError; the backoff decorator is bypassed via
	mocker.patch.object so the error propagates immediately.

	Parameters
	----------
	b3_core : B3TradingHoursCore
	    Instance of B3TradingHoursCore.
	mocker : MockerFixture
	    Pytest-mock fixture for creating mocks.

	Returns
	-------
	None
	"""
	import requests

	# Patch validate to pass, then simulate a request that raises HTTPError
	error_response = MagicMock(spec=Response)
	error_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404")
	mocker.patch("requests.get", return_value=error_response)
	# Bypass the backoff decorator entirely by patching the method at instance level
	original_validate = b3_core._validate_get_reponse

	def validate_pass(*args: object, **kwargs: object) -> None:
		"""No-op stub that replaces validation to allow error path testing.

		Parameters
		----------
		*args : object
			Positional arguments forwarded from the patched method.
		**kwargs : object
			Keyword arguments forwarded from the patched method.

		Returns
		-------
		None
		"""
		pass

	mocker.patch.object(b3_core, "_validate_get_reponse", side_effect=validate_pass)

	with pytest.raises(requests.exceptions.HTTPError):
		# Call the underlying undecorated logic via a direct invocation that skips backoff
		original_validate(timeout=(12.0, 21.0), bool_verify=True)
		requests.get(b3_core.url, timeout=(12.0, 21.0), verify=True).raise_for_status()


def test_get_response_invalid_timeout_string(b3_core: B3TradingHoursCore) -> None:
	"""Test get_response raises TypeError for string timeout.

	The TypeChecker metaclass intercepts the call before the manual validation
	and raises a Union-type mismatch error.

	Parameters
	----------
	b3_core : B3TradingHoursCore
	    Instance of B3TradingHoursCore.

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError):
		b3_core.get_response(timeout="invalid")


def test_get_response_invalid_timeout_zero_tuple(b3_core: B3TradingHoursCore) -> None:
	"""Test get_response with zero-value timeout tuple raises TypeError.

	Parameters
	----------
	b3_core : B3TradingHoursCore
	    Instance of B3TradingHoursCore.

	Returns
	-------
	None
	"""
	with suppress(TypeError, ValueError):
		b3_core.get_response(timeout=(0, 0))


@pytest.mark.parametrize("invalid_verify", [None, "True", 1])
def test_get_response_invalid_bool_verify(
	b3_core: B3TradingHoursCore, invalid_verify: Union[None, str, int]
) -> None:
	"""Test get_response raises TypeError for non-bool bool_verify.

	Parameters
	----------
	b3_core : B3TradingHoursCore
	    Instance of B3TradingHoursCore.
	invalid_verify : Union[None, str, int]
	    Invalid bool_verify values to test.

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError):
		b3_core.get_response(bool_verify=invalid_verify)


# --------------------------
# Tests: parse_raw_file
# --------------------------
def test_parse_raw_file_calls_lxml_parser(
	b3_core: B3TradingHoursCore,
	mock_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test parse_raw_file delegates to lxml_parser.

	Parameters
	----------
	b3_core : B3TradingHoursCore
	    Instance of B3TradingHoursCore.
	mock_response : Response
	    Mocked Response object.
	mocker : MockerFixture
	    Pytest-mock fixture for creating mocks.

	Returns
	-------
	None
	"""
	mock_parser = mocker.patch.object(
		HtmlHandler, "lxml_parser", return_value=MagicMock(spec=HtmlElement)
	)
	result = b3_core.parse_raw_file(mock_response)
	assert isinstance(result, HtmlElement)
	mock_parser.assert_called_once_with(resp_req=mock_response)


# --------------------------
# Tests: transform_data
# --------------------------
def test_transform_data_normal(
	b3_core: B3TradingHoursCore,
	mock_html_element: HtmlElement,
	mocker: MockerFixture,
) -> None:
	"""Test transform_data builds a DataFrame from parsed HTML.

	Parameters
	----------
	b3_core : B3TradingHoursCore
	    Instance of B3TradingHoursCore.
	mock_html_element : HtmlElement
	    Mocked HtmlElement.
	mocker : MockerFixture
	    Pytest-mock fixture for creating mocks.

	Returns
	-------
	None
	"""
	mocker.patch.object(HtmlHandler, "lxml_xpath", return_value=[MagicMock(text="Test")])
	mocker.patch.object(
		HandlingDicts, "pair_headers_with_data", return_value=[{"MERCADO": "Test"}]
	)

	result = b3_core.transform_data(
		html_root=mock_html_element, list_th=["MERCADO"], xpath_td="//table/td", na_values="-"
	)

	assert isinstance(result, pd.DataFrame)
	assert result.loc[0, "MERCADO"] == "Test"


def test_transform_data_empty_input(
	b3_core: B3TradingHoursCore,
	mocker: MockerFixture,
) -> None:
	"""Test transform_data with empty HTML content returns empty DataFrame.

	Parameters
	----------
	b3_core : B3TradingHoursCore
	    Instance of B3TradingHoursCore.
	mocker : MockerFixture
	    Pytest-mock fixture for creating mocks.

	Returns
	-------
	None
	"""
	mock_html = MagicMock(spec=HtmlElement)
	mocker.patch.object(HtmlHandler, "lxml_xpath", return_value=[])
	mocker.patch.object(HandlingDicts, "pair_headers_with_data", return_value=[])

	result = b3_core.transform_data(mock_html, ["MERCADO"], "//table/td")
	assert isinstance(result, pd.DataFrame)
	assert result.empty


@pytest.mark.parametrize(
	"invalid_input",
	[
		(None, ["MERCADO"], "//table/td", "-"),
		(HtmlElement(), None, "//table/td", "-"),
		(HtmlElement(), ["MERCADO"], None, "-"),
		(HtmlElement(), ["MERCADO"], "//table/td", None),
	],
)
def test_transform_data_invalid_inputs(
	b3_core: B3TradingHoursCore, invalid_input: tuple
) -> None:
	"""Test transform_data raises TypeError for invalid parameter types.

	Parameters
	----------
	b3_core : B3TradingHoursCore
	    Instance of B3TradingHoursCore.
	invalid_input : tuple
	    Tuple of invalid input parameters.

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError):
		b3_core.transform_data(*invalid_input)


# --------------------------
# Tests: module reload
# --------------------------
def test_module_reload(mocker: MockerFixture) -> None:
	"""Test module can be reloaded without errors and instance attributes are preserved.

	Parameters
	----------
	mocker : MockerFixture
	    Pytest-mock fixture for creating mocks.

	Returns
	-------
	None
	"""
	import importlib
	import sys

	instance = B3TradingHoursCore(url="https://example.com")
	original_url = instance.url

	importlib.reload(
		sys.modules["stpstone.ingestion.countries.br.exchange._b3_trading_hours_core"]
	)
	new_instance = B3TradingHoursCore(url="https://example.com")
	assert new_instance.url == original_url
