"""Unit tests for HandlingLXML class.

Tests the HTML data extraction functionality with various input scenarios including:
- Valid URL fetching and parsing
- Invalid URL handling
- HTTP method variations
- Error conditions and edge cases
"""

from unittest.mock import Mock, patch

from lxml.etree import _Element
import pytest
from requests import Response

from stpstone.utils.parsers.lxml import HandlingLXML


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def handling_lxml() -> HandlingLXML:
    """Fixture providing HandlingLXML instance.

    Returns
    -------
    HandlingLXML
        Instance of HandlingLXML class
    """
    return HandlingLXML()


@pytest.fixture
def valid_url() -> str:
    """Fixture providing a valid URL.

    Returns
    -------
    str
        Valid URL string
    """
    return "https://example.com"


@pytest.fixture
def mock_response() -> Response:
    """Fixture providing a mock response with valid HTML content.

    Returns
    -------
    Response
        Mocked requests.Response object with HTML content
    """
    response = Mock(spec=Response)
    response.content = b"<html><body>Test</body></html>"
    return response


# --------------------------
# Tests for _validate_url
# --------------------------
def test_validate_url_empty(handling_lxml: HandlingLXML) -> None:
    """Test _validate_url raises ValueError for empty URL.

    Verifies
    --------
    That providing an empty URL string raises ValueError with appropriate message.

    Parameters
    ----------
    handling_lxml : HandlingLXML
        Instance of HandlingLXML class

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="URL cannot be empty"):
        handling_lxml._validate_url("")


def test_validate_url_non_string(handling_lxml: HandlingLXML) -> None:
    """Test _validate_url raises ValueError for non-string URL.

    Verifies
    --------
    That providing a non-string URL raises ValueError with appropriate message.

    Parameters
    ----------
    handling_lxml : HandlingLXML
        Instance of HandlingLXML class

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        handling_lxml._validate_url(123)


def test_validate_url_invalid_protocol(handling_lxml: HandlingLXML) -> None:
    """Test _validate_url raises ValueError for invalid protocol.

    Verifies
    --------
    That providing a URL without http:// or https:// raises ValueError.

    Parameters
    ----------
    handling_lxml : HandlingLXML
        Instance of HandlingLXML class

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="URL must start with http:// or https://"):
        handling_lxml._validate_url("ftp://example.com")


def test_validate_url_valid(handling_lxml: HandlingLXML, valid_url: str) -> None:
    """Test _validate_url accepts valid URL.

    Verifies
    --------
    That providing a valid URL does not raise any errors.

    Parameters
    ----------
    handling_lxml : HandlingLXML
        Instance of HandlingLXML class
    valid_url : str
        Valid URL from fixture

    Returns
    -------
    None
    """
    handling_lxml._validate_url(valid_url)
    assert True  # no exception raised


# --------------------------
# Tests for fetch
# --------------------------
@patch("stpstone.utils.parsers.lxml.request")
def test_fetch_valid_get(
    mock_request: Mock, handling_lxml: HandlingLXML, valid_url: str, mock_response: Response
) -> None:
    """Test fetch with valid GET request.

    Verifies
    --------
    That a valid GET request returns a parsed HTML document tree.

    Parameters
    ----------
    mock_request : Mock
        Mocked request function
    handling_lxml : HandlingLXML
        Instance of HandlingLXML class
    valid_url : str
        Valid URL from fixture
    mock_response : Response
        Mocked response with valid HTML content

    Returns
    -------
    None
    """
    mock_request.return_value = mock_response
    result = handling_lxml.fetch(valid_url, method="get")
    assert isinstance(result, _Element)
    mock_request.assert_called_once_with("get", valid_url, timeout=(200, 200))


@patch("stpstone.utils.parsers.lxml.request")
def test_fetch_valid_post(
    mock_request: Mock, handling_lxml: HandlingLXML, valid_url: str, mock_response: Response
) -> None:
    """Test fetch with valid POST request.

    Verifies
    --------
    That a valid POST request returns a parsed HTML document tree.

    Parameters
    ----------
    mock_request : Mock
        Mocked request function
    handling_lxml : HandlingLXML
        Instance of HandlingLXML class
    valid_url : str
        Valid URL from fixture
    mock_response : Response
        Mocked response with valid HTML content

    Returns
    -------
    None
    """
    mock_request.return_value = mock_response
    result = handling_lxml.fetch(valid_url, method="post")
    assert isinstance(result, _Element)
    mock_request.assert_called_once_with("post", valid_url, timeout=(200, 200))


@patch("stpstone.utils.parsers.lxml.request")
def test_fetch_empty_response(
    mock_request: Mock, handling_lxml: HandlingLXML, valid_url: str
) -> None:
    """Test fetch raises ValueError for empty response.

    Verifies
    --------
    That an empty response from the server raises ValueError.

    Parameters
    ----------
    mock_request : Mock
        Mocked request function
    handling_lxml : HandlingLXML
        Instance of HandlingLXML class
    valid_url : str
        Valid URL from fixture

    Returns
    -------
    None
    """
    mock_response = Mock(spec=Response)
    mock_response.content = b""
    mock_request.return_value = mock_response
    with pytest.raises(ValueError, match="Received empty response from URL"):
        handling_lxml.fetch(valid_url)


@patch("stpstone.utils.parsers.lxml.request")
def test_fetch_request_failure(
    mock_request: Mock, handling_lxml: HandlingLXML, valid_url: str
) -> None:
    """Test fetch handles request failure.

    Verifies
    --------
    That a request exception is caught and raises ValueError with appropriate message.

    Parameters
    ----------
    mock_request : Mock
        Mocked request function
    handling_lxml : HandlingLXML
        Instance of HandlingLXML class
    valid_url : str
        Valid URL from fixture

    Returns
    -------
    None
    """
    mock_request.side_effect = Exception("Connection error")
    with pytest.raises(ValueError, match="Failed to fetch or parse URL: Connection error"):
        handling_lxml.fetch(valid_url)


def test_fetch_invalid_method(handling_lxml: HandlingLXML, valid_url: str) -> None:
    """Test fetch with invalid HTTP method.

    Verifies
    --------
    That an invalid HTTP method raises ValueError through URL validation.

    Parameters
    ----------
    handling_lxml : HandlingLXML
        Instance of HandlingLXML class
    valid_url : str
        Valid URL from fixture

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be one of"):
        handling_lxml.fetch(valid_url, method="invalid")  # type: ignore


@patch("stpstone.utils.parsers.lxml.request")
def test_fetch_unicode_url(
    mock_request: Mock, handling_lxml: HandlingLXML, mock_response: Response
) -> None:
    """Test fetch with URL containing unicode characters.

    Verifies
    --------
    That a URL with unicode characters is handled correctly.

    Parameters
    ----------
    mock_request : Mock
        Mocked request function
    handling_lxml : HandlingLXML
        Instance of HandlingLXML class
    mock_response : Response
        Mocked response with valid HTML content

    Returns
    -------
    None
    """
    unicode_url = "https://exämple.com"
    mock_request.return_value = mock_response
    result = handling_lxml.fetch(unicode_url)
    assert isinstance(result, _Element)
    mock_request.assert_called_once_with("get", unicode_url, timeout=(200, 200))


@patch("stpstone.utils.parsers.lxml.request")
def test_fetch_malformed_html(
    mock_request: Mock, handling_lxml: HandlingLXML, valid_url: str
) -> None:
    """Test fetch with malformed HTML content.

    Verifies
    --------
    That malformed HTML content is handled without raising parsing errors.

    Parameters
    ----------
    mock_request : Mock
        Mocked request function
    handling_lxml : HandlingLXML
        Instance of HandlingLXML class
    valid_url : str
        Valid URL from fixture

    Returns
    -------
    None
    """
    mock_response = Mock(spec=Response)
    mock_response.content = b"<html><body>Malformed HTML"
    mock_request.return_value = mock_response
    result = handling_lxml.fetch(valid_url)
    assert isinstance(result, _Element)


def test_fetch_none_url(handling_lxml: HandlingLXML) -> None:
    """Test fetch raises ValueError for None URL.

    Verifies
    --------
    That providing None as URL raises ValueError with appropriate message.

    Parameters
    ----------
    handling_lxml : HandlingLXML
        Instance of HandlingLXML class

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        handling_lxml.fetch(None)  # type: ignore