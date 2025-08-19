"""Unit tests for UserAgents class in stpstone.utils.connections.netops.scraping.user_agents.

This module tests the user agent string retrieval and management functionality,
covering normal operations, edge cases, error conditions, and type validation.
"""

import pytest
from pytest_mock import MockerFixture
from requests.exceptions import (
    ChunkedEncodingError,
    ConnectTimeout,
    HTTPError,
    ReadTimeout,
    RequestException,
)

from stpstone.utils.connections.netops.scraping.user_agents import UserAgents


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture(autouse=True)
def mock_backoff_sleep(mocker: MockerFixture) -> None:
    """Mock time.sleep in backoff to prevent delays during retries.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mocker.patch("backoff._sync.time.sleep")


@pytest.fixture(scope="function")
def user_agents() -> UserAgents:
    """Fixture providing UserAgents instance.

    Returns
    -------
    UserAgents
        Instance of UserAgents class
    """
    return UserAgents()


@pytest.fixture(scope="function")
def sample_user_agents() -> list[str]:
    """Fixture providing sample user agent strings.

    Returns
    -------
    list[str]
        List of sample user agent strings
    """
    return [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",  # noqa: E501
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",  # noqa: E501
    ]


@pytest.fixture(scope="function")
def mock_requests_get(mocker: MockerFixture) -> object:
    """Mock requests.get to return a mocked response.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    object
        Mock object for requests.get
    """
    return mocker.patch("requests.get")


@pytest.fixture(scope="function")
def mock_html_fromstring(mocker: MockerFixture) -> object:
    """Mock html.fromstring to return a mocked tree.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    object
        Mock object for html.fromstring
    """
    return mocker.patch("lxml.html.fromstring")


@pytest.fixture(scope="function")
def mock_random_choice(mocker: MockerFixture, sample_user_agents: list[str]) -> object:
    """Mock random.choice to return a specific user agent.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    sample_user_agents : list[str]
        List of sample user agent strings

    Returns
    -------
    object
        Mock object for random.choice
    """
    return mocker.patch("random.choice", return_value=sample_user_agents[0])


@pytest.fixture(scope="function")
def mock_random_randint(mocker: MockerFixture) -> object:
    """Mock random.randint to return a fixed index.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    object
        Mock object for random.randint
    """
    return mocker.patch("random.randint", return_value=0)


# --------------------------
# Tests for _validate_url
# --------------------------
def test_validate_url_valid(user_agents: UserAgents) -> None:
    """Test URL validation with valid URLs.

    Verifies
    --------
    That valid URLs (http:// and https://) do not raise exceptions.

    Parameters
    ----------
    user_agents : UserAgents
        Instance of UserAgents class

    Returns
    -------
    None
    """
    user_agents._validate_url("https://example.com")
    user_agents._validate_url("http://example.com")


def test_validate_url_empty(user_agents: UserAgents) -> None:
    """Test URL validation with empty URL.

    Verifies
    --------
    That an empty URL raises ValueError with appropriate message.

    Parameters
    ----------
    user_agents : UserAgents
        Instance of UserAgents class

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="URL cannot be empty"):
        user_agents._validate_url("")


def test_validate_url_invalid_scheme(user_agents: UserAgents) -> None:
    """Test URL validation with invalid scheme.

    Verifies
    --------
    That URLs without http:// or https:// raise ValueError with appropriate message.

    Parameters
    ----------
    user_agents : UserAgents
        Instance of UserAgents class

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="URL must start with http:// or https://"):
        user_agents._validate_url("ftp://example.com")


def test_validate_url_none(user_agents: UserAgents) -> None:
    """Test URL validation with None input.

    Verifies
    --------
    That None URL raises TypeError due to TypeChecker metaclass.

    Parameters
    ----------
    user_agents : UserAgents
        Instance of UserAgents class

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="url must be of type str, got NoneType"):
        user_agents._validate_url(None)


def test_validate_url_non_string(user_agents: UserAgents) -> None:
    """Test URL validation with non-string input.

    Verifies
    --------
    That non-string inputs raise TypeError due to TypeChecker metaclass.

    Parameters
    ----------
    user_agents : UserAgents
        Instance of UserAgents class

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="url must be of type str"):
        user_agents._validate_url(123)


# --------------------------
# Tests for fetch_user_agents
# --------------------------
def test_fetch_user_agents_success(
    user_agents: UserAgents,
    mock_requests_get: object,
    mock_html_fromstring: object,
    sample_user_agents: list[str],
) -> None:
    """Test successful fetching of user agents.

    Verifies
    --------
    That fetch_user_agents returns a list of user agent strings when the request
    and parsing succeed.

    Parameters
    ----------
    user_agents : UserAgents
        Instance of UserAgents class
    mock_requests_get : object
        Mock for requests.get
    mock_html_fromstring : object
        Mock for html.fromstring
    sample_user_agents : list[str]
        List of sample user agent strings

    Returns
    -------
    None
    """
    mock_response = mock_requests_get.return_value
    mock_response.content = b"content"
    mock_response.raise_for_status.return_value = None

    mock_tree = mock_html_fromstring.return_value
    mock_tree.xpath.side_effect = [
        [sample_user_agents[0]],
        [sample_user_agents[1]],
        [],
    ]

    result = user_agents.fetch_user_agents()
    assert result == sample_user_agents
    mock_requests_get.assert_called_once()
    mock_html_fromstring.assert_called_once_with(b"content")
    assert mock_tree.xpath.call_count == 3


def test_fetch_user_agents_empty_response(
    user_agents: UserAgents,
    mock_requests_get: object,
    mock_html_fromstring: object,
) -> None:
    """Test fetch_user_agents with empty response content.

    Verifies
    --------
    That an empty response raises ValueError with appropriate message.

    Parameters
    ----------
    user_agents : UserAgents
        Instance of UserAgents class
    mock_requests_get : object
        Mock for requests.get
    mock_html_fromstring : object
        Mock for html.fromstring

    Returns
    -------
    None
    """
    mock_response = mock_requests_get.return_value
    mock_response.content = b""
    mock_response.raise_for_status.return_value = None

    mock_html_fromstring.side_effect = Exception("Parsing error")

    with pytest.raises(ValueError, match="Failed to parse HTML content"):
        user_agents.fetch_user_agents()


@pytest.mark.parametrize(
    "exception, message",
    [
        (RequestException, "Request failed"),
        (HTTPError, "HTTP error"),
        (ConnectTimeout, "Connection timeout"),
        (ReadTimeout, "Read timeout"),
        (ChunkedEncodingError, "Chunked encoding error"),
    ],
    ids=["request_exception", "http_error", "connect_timeout", "read_timeout", "chunked_encoding"],
)
def test_fetch_user_agents_exceptions(
    user_agents: UserAgents,
    mock_requests_get: object,
    exception: type,
    message: str,
) -> None:
    """Test fetch_user_agents with various request exceptions.

    Verifies
    --------
    That specified exceptions are raised immediately.

    Parameters
    ----------
    user_agents : UserAgents
        Instance of UserAgents class
    mock_requests_get : object
        Mock for requests.get
    exception : type
        The exception type to raise
    message : str
        The expected exception message

    Returns
    -------
    None
    """
    mock_response = mock_requests_get.return_value
    if exception == HTTPError:
        mock_response.raise_for_status.side_effect = exception(message)
    else:
        mock_requests_get.side_effect = exception(message)

    with pytest.raises(exception, match=message):
        user_agents.fetch_user_agents()


# --------------------------
# Tests for get_random_user_agent
# --------------------------
def test_get_random_user_agent_success(
    user_agents: UserAgents,
    mock_requests_get: object,
    mock_html_fromstring: object,
    mock_random_randint: object,
    sample_user_agents: list[str],
) -> None:
    """Test get_random_user_agent with successful fetch.

    Verifies
    --------
    That a random user agent is returned from fetched list when fetch succeeds.

    Parameters
    ----------
    user_agents : UserAgents
        Instance of UserAgents class
    mock_requests_get : object
        Mock for requests.get
    mock_html_fromstring : object
        Mock for html.fromstring
    mock_random_randint : object
        Mock for random.randint
    sample_user_agents : list[str]
        List of sample user agent strings

    Returns
    -------
    None
    """
    mock_response = mock_requests_get.return_value
    mock_response.content = b"content"
    mock_response.raise_for_status.return_value = None

    mock_tree = mock_html_fromstring.return_value
    mock_tree.xpath.side_effect = [[sample_user_agents[0]], []]

    result = user_agents.get_random_user_agent()
    assert result == sample_user_agents[0]
    mock_random_randint.assert_called_once_with(0, 0)


def test_get_random_user_agent_fallback(
    user_agents: UserAgents,
    mock_requests_get: object,
    mock_random_choice: object,
) -> None:
    """Test get_random_user_agent fallback when fetch fails.

    Verifies
    --------
    That a fallback user agent is returned when fetch_user_agents raises an exception.

    Parameters
    ----------
    user_agents : UserAgents
        Instance of UserAgents class
    mock_requests_get : object
        Mock for requests.get
    mock_random_choice : object
        Mock for random.choice

    Returns
    -------
    None
    """
    mock_requests_get.side_effect = RequestException("Request failed")

    result = user_agents.get_random_user_agent()
    assert result == mock_random_choice.return_value
    mock_random_choice.assert_called_once()


def test_get_random_user_agent_empty_fetch(
    user_agents: UserAgents,
    mock_requests_get: object,
    mock_html_fromstring: object,
    mock_random_choice: object,
) -> None:
    """Test get_random_user_agent with empty fetch result.

    Verifies
    --------
    That a fallback user agent is returned when fetch_user_agents returns an empty list.

    Parameters
    ----------
    user_agents : UserAgents
        Instance of UserAgents class
    mock_requests_get : object
        Mock for requests.get
    mock_html_fromstring : object
        Mock for html.fromstring
    mock_random_choice : object
        Mock for random.choice

    Returns
    -------
    None
    """
    mock_response = mock_requests_get.return_value
    mock_response.content = b"content"
    mock_response.raise_for_status.return_value = None

    mock_tree = mock_html_fromstring.return_value
    mock_tree.xpath.return_value = []

    result = user_agents.get_random_user_agent()
    assert result == mock_random_choice.return_value
    mock_random_choice.assert_called_once()


def test_get_random_user_agent_type(
    user_agents: UserAgents,
    mock_requests_get: object,
    mock_html_fromstring: object,
    mock_random_randint: object,
    sample_user_agents: list[str],
) -> None:
    """Test get_random_user_agent return type.

    Verifies
    --------
    That get_random_user_agent returns a string.

    Parameters
    ----------
    user_agents : UserAgents
        Instance of UserAgents class
    mock_requests_get : object
        Mock for requests.get
    mock_html_fromstring : object
        Mock for html.fromstring
    mock_random_randint : object
        Mock for random.randint
    sample_user_agents : list[str]
        List of sample user agent strings

    Returns
    -------
    None
    """
    mock_response = mock_requests_get.return_value
    mock_response.content = b"content"
    mock_response.raise_for_status.return_value = None

    mock_tree = mock_html_fromstring.return_value
    mock_tree.xpath.side_effect = [[sample_user_agents[0]], []]

    result = user_agents.get_random_user_agent()
    assert isinstance(result, str)


def test_get_random_user_agent_non_empty(
    user_agents: UserAgents,
    mock_requests_get: object,
    mock_html_fromstring: object,
    mock_random_randint: object,
    sample_user_agents: list[str],
) -> None:
    """Test get_random_user_agent returns non-empty string.

    Verifies
    --------
    That get_random_user_agent returns a non-empty string when fetch succeeds.

    Parameters
    ----------
    user_agents : UserAgents
        Instance of UserAgents class
    mock_requests_get : object
        Mock for requests.get
    mock_html_fromstring : object
        Mock for html.fromstring
    mock_random_randint : object
        Mock for random.randint
    sample_user_agents : list[str]
        List of sample user agent strings

    Returns
    -------
    None
    """
    mock_response = mock_requests_get.return_value
    mock_response.content = b"content"
    mock_response.raise_for_status.return_value = None

    mock_tree = mock_html_fromstring.return_value
    mock_tree.xpath.side_effect = [[sample_user_agents[0]], []]

    result = user_agents.get_random_user_agent()
    assert len(result) > 0