"""Unit tests for ScrapeChecker class.

This module contains unit tests for the ScrapeChecker class, verifying
web scraping permission checking functionality, including validation
of URLs and user agents, normal operation, edge cases, and error conditions.
"""

from typing import Any

import pytest
from pytest_mock import MockerFixture

from stpstone.utils.connections.netops.scraping.scrape_checker import ScrapeChecker


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def scrape_checker() -> ScrapeChecker:
	"""Fixture providing ScrapeChecker instance.

	Returns
	-------
	ScrapeChecker
		Instance of ScrapeChecker class
	"""
	return ScrapeChecker()


@pytest.fixture
def mock_robot_parser(mocker: MockerFixture) -> Any:  # noqa: ANN401 - typing.Any is not allowed
	"""Fixture mocking RobotFileParser.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	Any
		Mocked RobotFileParser object
	"""
	return mocker.patch("urllib.robotparser.RobotFileParser")


# --------------------------
# Tests for _validate_url
# --------------------------
def test_validate_url_empty(scrape_checker: ScrapeChecker) -> None:
	"""Test _validate_url raises ValueError for empty URL.

	Verifies
	--------
	That an empty URL raises ValueError with appropriate message.

	Parameters
	----------
	scrape_checker : ScrapeChecker
		ScrapeChecker instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="URL cannot be empty"):
		scrape_checker._validate_url("")


def test_validate_url_non_string(scrape_checker: ScrapeChecker) -> None:
	"""Test _validate_url raises ValueError for non-string URL.

	Verifies
	--------
	That a non-string URL raises ValueError with appropriate message.

	Parameters
	----------
	scrape_checker : ScrapeChecker
		ScrapeChecker instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		scrape_checker._validate_url(123)


def test_validate_url_invalid_protocol(scrape_checker: ScrapeChecker) -> None:
	"""Test _validate_url raises ValueError for invalid protocol.

	Verifies
	--------
	That a URL without http:// or https:// raises ValueError with appropriate message.

	Parameters
	----------
	scrape_checker : ScrapeChecker
		ScrapeChecker instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="URL must start with http:// or https://"):
		scrape_checker._validate_url("ftp://example.com")


def test_validate_url_valid(scrape_checker: ScrapeChecker) -> None:
	"""Test _validate_url accepts valid URLs.

	Verifies
	--------
	That valid URLs (http:// or https://) pass without raising exceptions.

	Parameters
	----------
	scrape_checker : ScrapeChecker
		ScrapeChecker instance from fixture

	Returns
	-------
	None
	"""
	scrape_checker._validate_url("https://example.com")
	scrape_checker._validate_url("http://example.com")
	assert True  # No exception raised


# --------------------------
# Tests for _validate_user_agent
# --------------------------
def test_validate_user_agent_empty(scrape_checker: ScrapeChecker) -> None:
	"""Test _validate_user_agent raises ValueError for empty user agent.

	Verifies
	--------
	That an empty user agent raises ValueError with appropriate message.

	Parameters
	----------
	scrape_checker : ScrapeChecker
		ScrapeChecker instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="User agent cannot be empty"):
		scrape_checker._validate_user_agent("")


def test_validate_user_agent_non_string(scrape_checker: ScrapeChecker) -> None:
	"""Test _validate_user_agent raises ValueError for non-string user agent.

	Verifies
	--------
	That a non-string user agent raises ValueError with appropriate message.

	Parameters
	----------
	scrape_checker : ScrapeChecker
		ScrapeChecker instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		scrape_checker._validate_user_agent(123)


def test_validate_user_agent_valid(scrape_checker: ScrapeChecker) -> None:
	"""Test _validate_user_agent accepts valid user agents.

	Verifies
	--------
	That valid user agent strings pass without raising exceptions.

	Parameters
	----------
	scrape_checker : ScrapeChecker
		ScrapeChecker instance from fixture

	Returns
	-------
	None
	"""
	scrape_checker._validate_user_agent("*")
	scrape_checker._validate_user_agent("Mozilla/5.0")
	assert True  # No exception raised


# --------------------------
# Tests for is_allowed
# --------------------------
def test_is_allowed_success(
	scrape_checker: ScrapeChecker,
	mock_robot_parser: Any,  # noqa: ANN401 - typing.Any is not allowed
) -> None:
	"""Test is_allowed with valid inputs and successful robots.txt fetch.

	Verifies
	--------
	That is_allowed returns correct boolean and calls RobotFileParser correctly.

	Parameters
	----------
	scrape_checker : ScrapeChecker
		ScrapeChecker instance from fixture
	mock_robot_parser : Any
		Mocked RobotFileParser object

	Returns
	-------
	None
	"""
	mock_instance = mock_robot_parser.return_value
	mock_instance.can_fetch.return_value = True

	result = scrape_checker.is_allowed("https://example.com", "*")
	assert result
	mock_robot_parser.assert_called_once()
	mock_instance.set_url.assert_called_once_with("https://example.com/robots.txt")
	mock_instance.read.assert_called_once()
	mock_instance.can_fetch.assert_called_once_with("*", "https://example.com")


def test_is_allowed_failure(
	scrape_checker: ScrapeChecker,
	mock_robot_parser: Any,  # noqa: ANN401 - typing.Any is not allowed
) -> None:
	"""Test is_allowed with valid inputs and disallowed scraping.

	Verifies
	--------
	That is_allowed returns False when robots.txt disallows scraping.

	Parameters
	----------
	scrape_checker : ScrapeChecker
		ScrapeChecker instance from fixture
	mock_robot_parser : Any
		Mocked RobotFileParser object

	Returns
	-------
	None
	"""
	mock_instance = mock_robot_parser.return_value
	mock_instance.can_fetch.return_value = False

	result = scrape_checker.is_allowed("https://example.com", "*")
	assert not result
	mock_instance.can_fetch.assert_called_once_with("*", "https://example.com")


def test_is_allowed_invalid_url(scrape_checker: ScrapeChecker) -> None:
	"""Test is_allowed raises ValueError for invalid URL.

	Verifies
	--------
	That is_allowed raises ValueError for invalid URLs.

	Parameters
	----------
	scrape_checker : ScrapeChecker
		ScrapeChecker instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="URL must start with http:// or https://"):
		scrape_checker.is_allowed("ftp://example.com")


def test_is_allowed_empty_user_agent(scrape_checker: ScrapeChecker) -> None:
	"""Test is_allowed raises ValueError for empty user agent.

	Verifies
	--------
	That is_allowed raises ValueError for empty user agent.

	Parameters
	----------
	scrape_checker : ScrapeChecker
		ScrapeChecker instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="User agent cannot be empty"):
		scrape_checker.is_allowed("https://example.com", "")


def test_is_allowed_robots_txt_fetch_error(
	scrape_checker: ScrapeChecker,
	mock_robot_parser: Any,  # noqa: ANN401 - typing.Any is not allowed
) -> None:
	"""Test is_allowed handles robots.txt fetch errors.

	Verifies
	--------
	That is_allowed raises RuntimeError when robots.txt fetch fails.

	Parameters
	----------
	scrape_checker : ScrapeChecker
		ScrapeChecker instance from fixture
	mock_robot_parser : Any
		Mocked RobotFileParser object

	Returns
	-------
	None
	"""
	mock_instance = mock_robot_parser.return_value
	mock_instance.read.side_effect = Exception("Network error")

	with pytest.raises(RuntimeError, match="Failed to fetch or parse robots.txt"):
		scrape_checker.is_allowed("https://example.com", "*")


def test_is_allowed_unicode_url(
	scrape_checker: ScrapeChecker,
	mock_robot_parser: Any,  # noqa: ANN401 - typing.Any is not allowed
) -> None:
	"""Test is_allowed with URL containing unicode characters.

	Verifies
	--------
	That is_allowed handles URLs with unicode characters correctly.

	Parameters
	----------
	scrape_checker : ScrapeChecker
		ScrapeChecker instance from fixture
	mock_robot_parser : Any
		Mocked RobotFileParser object

	Returns
	-------
	None
	"""
	mock_instance = mock_robot_parser.return_value
	mock_instance.can_fetch.return_value = True

	result = scrape_checker.is_allowed("https://例子.com", "Mozilla/5.0")
	assert result
	mock_instance.set_url.assert_called_once_with("https://例子.com/robots.txt")


def test_is_allowed_special_chars_user_agent(
	scrape_checker: ScrapeChecker,
	mock_robot_parser: Any,  # noqa: ANN401 - typing.Any is not allowed
) -> None:
	"""Test is_allowed with user agent containing special characters.

	Verifies
	--------
	That is_allowed handles user agents with special characters correctly.

	Parameters
	----------
	scrape_checker : ScrapeChecker
		ScrapeChecker instance from fixture
	mock_robot_parser : Any
		Mocked RobotFileParser object

	Returns
	-------
	None
	"""
	mock_instance = mock_robot_parser.return_value
	mock_instance.can_fetch.return_value = True

	result = scrape_checker.is_allowed("https://example.com", "Bot@#$%")
	assert result
	mock_instance.can_fetch.assert_called_once_with("Bot@#$%", "https://example.com")
