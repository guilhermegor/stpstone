"""Unit tests for PlaywrightScraper class.

Tests cover initialization, timeout validation, browser launch and closure,
navigation, element selection, attribute retrieval, page URL access, cookie handling,
HTML export, error conditions, and edge cases following specified code and test standards.
"""

import os
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, PropertyMock, patch

from playwright.sync_api import Error as PlaywrightError
import pytest

from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def scraper_default() -> PlaywrightScraper:
	"""Fixture for a default PlaywrightScraper instance.

	Returns
	-------
	PlaywrightScraper
		Default PlaywrightScraper instance
	"""
	return PlaywrightScraper()


@pytest.fixture
def scraper_with_logger() -> PlaywrightScraper:
	"""Fixture for PlaywrightScraper instance with a logger mock.

	Returns
	-------
	PlaywrightScraper
		PlaywrightScraper instance with a logger mock
	"""
	logger = MagicMock()
	return PlaywrightScraper(logger=logger)


@pytest.fixture
def mock_page_with_element() -> MagicMock:
	"""Fixture returning a mock page with basic element behavior.

	Returns
	-------
	MagicMock
		Mock page with basic element behavior
	"""
	page = MagicMock()
	element = MagicMock()
	element.text_content.return_value = "Sample Text"
	element.inner_html.return_value = "<div>Sample Text</div>"
	element.bounding_box.return_value = {"x": 0, "y": 0, "width": 100, "height": 20}
	page.locator.return_value.first = element
	return page


@pytest.fixture
def mock_page_no_element() -> MagicMock:
	"""Fixture returning a mock page that simulates no element found.

	Returns
	-------
	MagicMock
		Mock page that simulates no element found
	"""
	page = MagicMock()
	page.locator.return_value.first = None
	return page


# --------------------------
# Tests
# --------------------------
def test_validate_timeout_accepts_valid_and_none(scraper_default: PlaywrightScraper) -> None:
	"""Test _validate_timeout with valid timeouts and None.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance

	Returns
	-------
	None
	"""
	scraper_default._validate_timeout(10)
	scraper_default._validate_timeout(None)


def test_validate_timeout_rejects_negative(scraper_default: PlaywrightScraper) -> None:
	"""Test _validate_timeout raises ValueError on negative input.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="timeout must be a positive integer or None"):
		scraper_default._validate_timeout(-1)


def test_init_sets_defaults(scraper_default: PlaywrightScraper) -> None:
	"""Test that initialization sets defaults correctly.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance

	Returns
	-------
	None
	"""
	assert scraper_default.bool_headless is True
	assert "Mozilla" in scraper_default.user_agent
	assert isinstance(scraper_default.viewport, dict)
	assert scraper_default.int_default_timeout == 10
	assert scraper_default.bool_accept_cookies is True
	assert scraper_default.bool_incognito is False
	assert scraper_default.logger is None
	assert scraper_default.playwright is None
	assert scraper_default.browser is None
	assert scraper_default.context is None
	assert scraper_default.page is None


def test_close_closes_resources(scraper_default: PlaywrightScraper) -> None:
	"""Test close method calls close/stop on context, browser, playwright if set.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance

	Returns
	-------
	None
	"""
	mock_context = MagicMock()
	mock_browser = MagicMock()
	mock_playwright = MagicMock()
	scraper_default.context = mock_context
	scraper_default.browser = mock_browser
	scraper_default.playwright = mock_playwright
	scraper_default.page = MagicMock()

	scraper_default.close()

	mock_context.close.assert_called_once()
	mock_browser.close.assert_called_once()
	mock_playwright.stop.assert_called_once()
	assert scraper_default.context is None
	assert scraper_default.browser is None
	assert scraper_default.playwright is None
	assert scraper_default.page is None


def test_close_handles_exceptions_and_logs(scraper_with_logger: PlaywrightScraper) -> None:
	"""Test close method logs errors during resource cleanup.

	Parameters
	----------
	scraper_with_logger : PlaywrightScraper
		PlaywrightScraper instance with logger

	Returns
	-------
	None
	"""
	magic_obj = MagicMock()
	magic_obj.close.side_effect = Exception("close error")
	scraper_with_logger.context = magic_obj
	scraper_with_logger.browser = magic_obj
	scraper_with_logger.playwright = magic_obj
	with patch("stpstone.utils.loggs.create_logs.CreateLog.log_message") as mock_log:
		scraper_with_logger.close()
		assert mock_log.call_count >= 1
		assert "close error" in mock_log.call_args_list[0][0][1]


def test_navigate_success_and_accept_cookies(scraper_default: PlaywrightScraper) -> None:
	"""Test navigate returns True on success and calls cookie handler if accept cookies enabled.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance

	Returns
	-------
	None
	"""
	mock_page = MagicMock()
	scraper_default.page = mock_page
	scraper_default.bool_accept_cookies = True

	with patch.object(scraper_default, "_handle_cookie_popup") as mock_cookie:
		result = scraper_default.navigate("http://example.com", timeout=5000)
		assert result is True
		mock_page.goto.assert_called_once_with("http://example.com", timeout=5000)
		mock_cookie.assert_called_once()


def test_navigate_failure_logs_and_returns_false(scraper_with_logger: PlaywrightScraper) -> None:
	"""Test navigate handles exceptions, logs error, and returns False.

	Parameters
	----------
	scraper_with_logger : PlaywrightScraper
		PlaywrightScraper instance with logger

	Returns
	-------
	None
	"""
	mock_page = MagicMock()
	mock_page.goto.side_effect = PlaywrightError("fail")
	scraper_with_logger.page = mock_page

	result = scraper_with_logger.navigate("http://fail.com")
	assert result is False
	assert scraper_with_logger.logger.method_calls


def test_get_current_url_returns_url(scraper_default: PlaywrightScraper) -> None:
	"""Test get_current_url returns URL when page is initialized.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance

	Returns
	-------
	None
	"""
	mock_page = MagicMock()
	type(mock_page).url = PropertyMock(return_value="http://test.url")
	scraper_default.page = mock_page

	url = scraper_default.get_current_url()
	assert url == "http://test.url"


def test_get_current_url_raises_if_no_page(scraper_default: PlaywrightScraper) -> None:
	"""Test get_current_url raises RuntimeError if no page initialized.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance

	Returns
	-------
	None
	"""
	scraper_default.page = None
	with pytest.raises(RuntimeError, match="Page not initialized"):
		scraper_default.get_current_url()


def test_get_current_url_logs_error_and_returns_none(
	scraper_with_logger: PlaywrightScraper,
) -> None:
	"""Test get_current_url logs errors and returns None on exception.

	Parameters
	----------
	scraper_with_logger : PlaywrightScraper
		PlaywrightScraper instance with logger

	Returns
	-------
	None
	"""
	mock_page = MagicMock()
	type(mock_page).url = PropertyMock(side_effect=Exception("bad"))
	scraper_with_logger.page = mock_page

	result = scraper_with_logger.get_current_url()
	assert result is None
	assert scraper_with_logger.logger.method_calls


def test_handle_cookie_popup_logs_info_and_handles_errors(
	scraper_with_logger: PlaywrightScraper,
) -> None:
	"""Test _handle_cookie_popup logs info when accepting cookies and logs error on failure.

	Parameters
	----------
	scraper_with_logger : PlaywrightScraper
		PlaywrightScraper instance with logger

	Returns
	-------
	None
	"""
	mock_page = MagicMock()
	scraper_with_logger.page = mock_page
	mock_page.click.return_value = True

	# Success case
	scraper_with_logger._handle_cookie_popup(timeout=10)
	assert scraper_with_logger.logger.method_calls

	# Error case
	mock_page.click.side_effect = Exception("click fail")
	scraper_with_logger._handle_cookie_popup(timeout=10)
	assert scraper_with_logger.logger.method_calls


@pytest.mark.parametrize(
	"visible,expected_method",
	[
		(True, "is_visible"),
		(False, "is_hidden"),
		(None, "wait_for_selector"),
	],
)
def test_selector_exists_behavior(
	scraper_default: PlaywrightScraper, visible: Optional[bool], expected_method: str
) -> None:
	"""Test selector_exists returns True/False based on locator behavior and visibility parameter.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance
	visible : Optional[bool]
		Visibility requirement (True=visible, False=hidden, None=either)
	expected_method : str
		Expected locator method to be called (is_visible, is_hidden, or wait_for_selector)

	Returns
	-------
	None
	"""
	mock_element = MagicMock()
	if expected_method == "wait_for_selector":
		scraper_default.page = MagicMock()
		scraper_default.page.wait_for_selector.return_value = mock_element
		scraper_default.page.locator.return_value.first = mock_element
		result = scraper_default.selector_exists("test_selector", visible=visible)
		assert result is True
		scraper_default.page.wait_for_selector.assert_called_once()
	else:
		mock_locator = MagicMock()
		setattr(mock_locator.first, expected_method, MagicMock(return_value=True))
		scraper_default.page = MagicMock()
		scraper_default.page.locator.return_value = mock_locator
		result = scraper_default.selector_exists("test_selector", visible=visible)
		assert result is True


def test_selector_exists_raises_if_no_page(scraper_default: PlaywrightScraper) -> None:
	"""Test selector_exists raises RuntimeError if page not initialized.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance

	Returns
	-------
	None
	"""
	scraper_default.page = None
	with pytest.raises(RuntimeError, match="Page not initialized"):
		scraper_default.selector_exists("selector")


def test_selector_exists_logs_and_returns_false_on_error(
	scraper_with_logger: PlaywrightScraper,
) -> None:
	"""Test selector_exists logs warning and returns False on exceptions.

	Parameters
	----------
	scraper_with_logger : PlaywrightScraper
		PlaywrightScraper instance with logger

	Returns
	-------
	None
	"""
	mock_locator = MagicMock()
	mock_locator.first.is_visible.side_effect = Exception("fail")
	scraper_with_logger.page = MagicMock()
	scraper_with_logger.page.locator.return_value = mock_locator
	result = scraper_with_logger.selector_exists("selector", visible=True)
	assert result is False
	assert scraper_with_logger.logger.method_calls


def test_get_element_returns_expected_data(
	scraper_default: PlaywrightScraper, mock_page_with_element: MagicMock
) -> None:
	"""Test get_element returns text, html, and bounding box as expected.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance
	mock_page_with_element : MagicMock
		Mock page with basic element behavior

	Returns
	-------
	None
	"""
	scraper_default.page = mock_page_with_element
	result = scraper_default.get_element("selector")
	assert result is not None
	assert isinstance(result, dict)
	assert "text" in result and "html" in result and "bounding_box" in result


def test_get_element_returns_none_when_text_empty(scraper_default: PlaywrightScraper) -> None:
	"""Test get_element returns None when element text is empty or whitespace.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance

	Returns
	-------
	None
	"""
	mock_element = MagicMock()
	mock_element.text_content.return_value = "   "
	scraper_default.page = MagicMock()
	scraper_default.page.locator.return_value.first = mock_element

	result = scraper_default.get_element("selector")
	assert result is None


def test_get_element_returns_none_if_no_element(
	scraper_default: PlaywrightScraper, mock_page_no_element: MagicMock
) -> None:
	"""Test get_element returns None if element is not found.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance
	mock_page_no_element : MagicMock
		Mock page with no element found

	Returns
	-------
	None
	"""
	scraper_default.page = mock_page_no_element
	result = scraper_default.get_element("selector")
	assert result is None


def test_get_element_raises_if_no_page(scraper_default: PlaywrightScraper) -> None:
	"""Test get_element raises RuntimeError if page is not initialized.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance

	Returns
	-------
	None
	"""
	scraper_default.page = None
	with pytest.raises(RuntimeError, match="Page not initialized"):
		scraper_default.get_element("selector")


def test_get_element_logs_and_returns_none_on_error(
	scraper_with_logger: PlaywrightScraper, mock_page_with_element: MagicMock
) -> None:
	"""Test get_element logs error and returns None if there is an exception.

	Parameters
	----------
	scraper_with_logger : PlaywrightScraper
		PlaywrightScraper instance with logger
	mock_page_with_element : MagicMock
		Mock page with basic element behavior

	Returns
	-------
	None
	"""
	scraper_with_logger.page = mock_page_with_element
	mock_page_with_element.page = MagicMock()
	mock_page_with_element.page.wait_for_selector.side_effect = Exception("fail")

	with patch.object(
		scraper_with_logger.page, "wait_for_selector", side_effect=Exception("fail")
	):
		result = scraper_with_logger.get_element("selector")
		assert result is None
		assert scraper_with_logger.logger.method_calls


def test_get_element_attrb_returns_attribute(scraper_default: PlaywrightScraper) -> None:
	"""Test get_element_attrb returns attribute value if element found and attribute exists.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance

	Returns
	-------
	None
	"""
	mock_element = MagicMock()
	mock_element.get_attribute.return_value = "http://example.com"

	scraper_default.page = MagicMock()
	scraper_default.page.locator.return_value.first = mock_element

	result = scraper_default.get_element_attrb("selector", attribute="href")
	assert result == "http://example.com"


def test_get_element_attrb_returns_none_if_element_or_attribute_missing(
	scraper_default: PlaywrightScraper,
) -> None:
	"""Test get_element_attrb returns None if element not found or attribute missing.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance

	Returns
	-------
	None
	"""
	scraper_default.page = MagicMock()
	scraper_default.page.locator.return_value.first = None
	assert scraper_default.get_element_attrb("selector") is None

	mock_element = MagicMock()
	mock_element.get_attribute.return_value = None
	scraper_default.page.locator.return_value.first = mock_element
	assert scraper_default.get_element_attrb("selector") is None


def test_get_element_attrb_raises_if_no_page(scraper_default: PlaywrightScraper) -> None:
	"""Test get_element_attrb raises RuntimeError if page is not initialized.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance

	Returns
	-------
	None
	"""
	scraper_default.page = None
	with pytest.raises(RuntimeError, match="Page not initialized"):
		scraper_default.get_element_attrb("selector")


def test_get_element_attrb_logs_and_returns_none_on_error(
	scraper_with_logger: PlaywrightScraper,
) -> None:
	"""Test get_element_attrb logs error and returns None on exception.

	Parameters
	----------
	scraper_with_logger : PlaywrightScraper
		PlaywrightScraper instance with logger

	Returns
	-------
	None
	"""
	mock_element = MagicMock()
	mock_element.get_attribute.side_effect = Exception("fail")
	scraper_with_logger.page = MagicMock()
	scraper_with_logger.page.locator.return_value.first = mock_element

	result = scraper_with_logger.get_element_attrb("selector")
	assert result is None
	assert scraper_with_logger.logger.method_calls


def test_get_elements_returns_filtered_elements(scraper_default: PlaywrightScraper) -> None:
	"""Test get_elements returns list of elements with non-empty text.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance

	Returns
	-------
	None
	"""
	mock_element1 = MagicMock()
	mock_element1.text_content.return_value = "Text1"
	mock_element1.inner_html.return_value = "<div>Text1</div>"
	mock_element1.bounding_box.return_value = {"x": 1, "y": 1, "width": 5, "height": 5}

	mock_element2 = MagicMock()
	mock_element2.text_content.return_value = " "
	mock_element2.inner_html.return_value = "<div>Empty</div>"
	mock_element2.bounding_box.return_value = {"x": 2, "y": 2, "width": 6, "height": 6}

	scraper_default.page = MagicMock()
	scraper_default.page.wait_for_selector.return_value = True
	scraper_default.page.locator.return_value.all.return_value = [mock_element1, mock_element2]

	results = scraper_default.get_elements("selector")
	assert len(results) == 1
	assert results[0]["text"] == "Text1"


def test_get_elements_returns_empty_list_if_no_page(scraper_default: PlaywrightScraper) -> None:
	"""Test get_elements raises RuntimeError if page not initialized.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance

	Returns
	-------
	None
	"""
	scraper_default.page = None
	with pytest.raises(RuntimeError, match="Page not initialized"):
		scraper_default.get_elements("selector")


def test_get_elements_logs_and_returns_empty_on_error(
	scraper_with_logger: PlaywrightScraper,
) -> None:
	"""Test get_elements logs error and returns empty list if exception occurs.

	Parameters
	----------
	scraper_with_logger : PlaywrightScraper
		PlaywrightScraper instance with logger

	Returns
	-------
	None
	"""
	scraper_with_logger.page = MagicMock()
	scraper_with_logger.page.wait_for_selector.side_effect = Exception("fail")

	results = scraper_with_logger.get_elements("selector")
	assert results == []
	assert scraper_with_logger.logger.method_calls


def test_get_list_data_returns_text_list(scraper_default: PlaywrightScraper) -> None:
	"""Test get_list_data returns list of texts from elements.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance

	Returns
	-------
	None
	"""
	return_elements = [{"text": "text1"}, {"text": "text2"}]
	with patch.object(
		scraper_default, "get_elements", return_value=return_elements
	) as mock_get_elements:
		results = scraper_default.get_list_data("selector")
		assert results == ["text1", "text2"]
		mock_get_elements.assert_called_once_with("xpath=selector", None)


def test_export_html_creates_file_and_returns_path(
	scraper_default: PlaywrightScraper, tmp_path: Path
) -> None:
	"""Test export_html creates html file with correct name and path.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance
	tmp_path : Path
		Temporary directory path

	Returns
	-------
	None
	"""
	content = "<html></html>"
	folder = tmp_path.as_posix()
	returned_path = scraper_default.export_html(
		content, folder_path=folder, bool_include_timestamp=False, filename="filetest"
	)
	assert Path(returned_path).exists()
	assert returned_path.endswith(".html")
	with open(returned_path, encoding="utf-8") as f:
		assert f.read() == content


def test_export_html_includes_timestamp(
	scraper_default: PlaywrightScraper, tmp_path: Path
) -> None:
	"""Test export_html includes timestamp in filename when requested.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance
	tmp_path : Path
		Temporary directory path

	Returns
	-------
	None
	"""
	content = "<html></html>"
	folder = tmp_path.as_posix()
	returned_path = scraper_default.export_html(
		content, folder_path=folder, bool_include_timestamp=True, filename="filetest"
	)
	assert Path(returned_path).exists()
	assert returned_path.startswith(os.path.join(folder, "filetest_"))
	assert returned_path.endswith(".html")


def test_export_html_generates_filename_from_url(
	scraper_default: PlaywrightScraper, tmp_path: Path
) -> None:
	"""Test export_html generates filename from current url if filename not given.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance
	tmp_path : Path
		Temporary directory path

	Returns
	-------
	None
	"""
	content = "<html></html>"
	folder = tmp_path.as_posix()
	mock_url = "http://example.com/page?query=1"
	with patch.object(scraper_default, "get_current_url", return_value=mock_url):
		returned_path = scraper_default.export_html(
			content, folder_path=folder, bool_include_timestamp=False, filename=None
		)
		assert Path(returned_path).exists()
		assert "example.com_page_query_1" in returned_path


def test_export_html_raises_and_logs_on_failure(
	scraper_with_logger: PlaywrightScraper, tmp_path: Path
) -> None:
	"""Test export_html raises RuntimeError and logs message if file writing fails.

	Parameters
	----------
	scraper_with_logger : PlaywrightScraper
		PlaywrightScraper instance with logger
	tmp_path : Path
		Temporary directory path

	Returns
	-------
	None
	"""
	content = "<html></html>"
	folder = tmp_path.as_posix()

	with patch("builtins.open", side_effect=OSError("fail open")):
		with pytest.raises(RuntimeError, match="Failed to save HTML file: fail open"):
			scraper_with_logger.export_html(content, folder_path=folder, filename="testfile")
		assert scraper_with_logger.logger.method_calls


def test_export_html_fallback_filename(scraper_default: PlaywrightScraper, tmp_path: Path) -> None:
	"""Test export_html uses 'scraped' as filename fallback if empty or invalid URL.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance
	tmp_path : Path
		Temporary directory path

	Returns
	-------
	None
	"""
	content = "<html></html>"
	folder = tmp_path.as_posix()

	# Simulate an empty URL after cleaning for filename fallback
	with patch.object(scraper_default, "get_current_url", return_value=""):
		returned_path = scraper_default.export_html(
			content, folder_path=folder, bool_include_timestamp=False, filename=None
		)
		assert Path(returned_path).exists()
		assert "scraped" in returned_path


def test_export_html_appends_html_extension(
	scraper_default: PlaywrightScraper, tmp_path: Path
) -> None:
	"""Test export_html adds .html if not present in given filename.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance
	tmp_path : Path
		Temporary directory path

	Returns
	-------
	None
	"""
	content = "<html></html>"
	folder = tmp_path.as_posix()

	file_name = "testfile.txt"
	returned_path = scraper_default.export_html(
		content, folder_path=folder, bool_include_timestamp=False, filename=file_name
	)
	assert returned_path.endswith(".html")
	assert Path(returned_path).exists()


def test_runtime_error_on_methods_when_page_not_initialized(
	scraper_default: PlaywrightScraper,
) -> None:
	"""Test that methods depending on page raise RuntimeError when page is None.

	Parameters
	----------
	scraper_default : PlaywrightScraper
		Default PlaywrightScraper instance

	Returns
	-------
	None
	"""
	scraper_default.page = None
	with pytest.raises(RuntimeError):
		scraper_default.selector_exists("selector")

	with pytest.raises(RuntimeError):
		scraper_default.get_element("selector")

	with pytest.raises(RuntimeError):
		scraper_default.get_element_attrb("selector")

	with pytest.raises(RuntimeError):
		scraper_default.get_elements("selector")
