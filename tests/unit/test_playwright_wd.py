"""Unit tests for PlaywrightScraper class.

Tests the web scraping functionality including browser initialization,
navigation, element selection, and content extraction with various input scenarios.
"""

from collections.abc import Generator
from contextlib import suppress
import os
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from playwright.sync_api import Browser, BrowserContext, Page, Playwright
import pytest

from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_playwright() -> Generator[MagicMock, None, None]:
    """Fixture providing mocked Playwright instance.

    Yields
    ------
    MagicMock
        Mocked Playwright instance
    """
    with patch("playwright.sync_api.sync_playwright") as mock:
        mock_playwright_instance = MagicMock(spec=Playwright)
        mock_browser = MagicMock(spec=Browser)
        mock_context = MagicMock(spec=BrowserContext)
        mock_page = MagicMock(spec=Page)
        
        mock.return_value.__enter__.return_value = mock_playwright_instance
        mock_playwright_instance.chromium.launch.return_value = mock_browser
        mock_playwright_instance.chromium.launch_persistent_context.return_value = mock_context
        mock_browser.new_context.return_value = mock_context
        mock_context.new_page.return_value = mock_page
        mock_context.pages = [mock_page]
        
        # Prevent premature stopping
        mock_playwright_instance.stop.side_effect = None
        mock_browser.close.side_effect = None
        mock_context.close.side_effect = None
        
        yield mock_playwright_instance


@pytest.fixture
def scraper() -> Generator[Any, None, None]:
    """Fixture providing PlaywrightScraper instance with default settings.

    Yields
    ------
    PlaywrightScraper
        Instance with headless=False for testing
    """
    scraper = PlaywrightScraper(bool_headless=True)
    yield scraper
    scraper.close()


@pytest.fixture
def mock_browser(mock_playwright: MagicMock) -> MagicMock:
    """Fixture providing mocked Browser instance.

    Parameters
    ----------
    mock_playwright : MagicMock
        Mocked Playwright instance

    Returns
    -------
    MagicMock
        Mocked Browser instance
    """
    mock_browser = MagicMock(spec=Browser)
    mock_playwright.chromium.launch.return_value = mock_browser
    return mock_browser


@pytest.fixture
def mock_context(mock_browser: MagicMock) -> MagicMock:
    """Fixture providing mocked BrowserContext instance.

    Parameters
    ----------
    mock_browser : MagicMock
        Mocked Browser instance

    Returns
    -------
    MagicMock
        Mocked BrowserContext instance
    """
    mock_context = MagicMock(spec=BrowserContext)
    mock_browser.new_context.return_value = mock_context
    return mock_context


@pytest.fixture
def mock_page(mock_context: MagicMock) -> MagicMock:
    """Fixture providing mocked Page instance.

    Parameters
    ----------
    mock_context : MagicMock
        Mocked BrowserContext instance

    Returns
    -------
    MagicMock
        Mocked Page instance
    """
    mock_page = MagicMock(spec=Page)
    mock_context.new_page.return_value = mock_page
    mock_context.pages = [mock_page]
    mock_page.url = "about:blank"
    
    mock_locator = MagicMock()
    mock_element = MagicMock()
    mock_page.locator.return_value = mock_locator
    mock_locator.first = mock_element
    mock_locator.count.return_value = 1
    mock_locator.all.return_value = []
    mock_element.is_visible.return_value = True
    mock_element.is_hidden.return_value = False
    mock_element.text_content.return_value = "test content"
    mock_element.inner_html.return_value = "<div>test</div>"
    mock_element.bounding_box.return_value = {"x": 10, "y": 20}
    mock_element.get_attribute.return_value = None
    mock_page.wait_for_selector.return_value = mock_locator  # Return locator to avoid timeout
    
    return mock_page

# --------------------------
# Tests
# --------------------------
class TestInitialization:
    """Tests for PlaywrightScraper initialization."""

    def test_default_initialization(self) -> None:
        """Test initialization with default parameters.

        Verifies
        --------
        - Instance is created with default values
        - Attributes are correctly set

        Returns
        -------
        None
        """
        scraper = PlaywrightScraper()
        assert scraper.bool_headless is True
        assert scraper.int_default_timeout == 30000
        assert scraper.viewport == {"width": 1920, "height": 1080}
        assert "Mozilla/5.0" in scraper.user_agent
        scraper.close()

    def test_custom_initialization(self) -> None:
        """Test initialization with custom parameters.

        Verifies
        --------
        - Instance accepts and stores custom parameters
        - Non-provided parameters use defaults

        Returns
        -------
        None
        """
        scraper = PlaywrightScraper(
            bool_headless=False,
            user_agent="test-agent",
            proxy="http://test:1234",
            viewport={"width": 800, "height": 600},
            int_default_timeout=10000,
            bool_accept_cookies=False,
            bool_incognito=True
        )
        assert scraper.bool_headless is False
        assert scraper.user_agent == "test-agent"
        assert scraper.proxy == "http://test:1234"
        assert scraper.viewport == {"width": 800, "height": 600}
        assert scraper.int_default_timeout == 10000
        assert scraper.bool_accept_cookies is False
        assert scraper.bool_incognito is True
        scraper.close()


class TestBrowserLaunch:
    """Tests for browser launch and context management."""

    @patch('playwright.sync_api.sync_playwright')
    def test_normal_launch(
        self,
        mock_sync_playwright: MagicMock,
    ) -> None:
        """Test successful browser launch.

        Verifies
        --------
        - Playwright is initialized correctly
        - Browser is launched with expected parameters
        - Context and page are created

        Parameters
        ----------
        mock_sync_playwright : MagicMock
            Mocked Playwright instance

        Returns
        -------
        None
        """
        mock_playwright = MagicMock()
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_page = MagicMock()
        
        mock_sync_playwright.return_value.__enter__.return_value = mock_playwright
        mock_playwright.chromium.launch.return_value = mock_browser
        mock_browser.new_context.return_value = mock_context
        mock_context.new_page.return_value = mock_page
        
        scraper = PlaywrightScraper(bool_headless=True)
        with scraper.launch():
            assert scraper.playwright is not None
            assert scraper.browser is not None
            assert scraper.context is not None
            assert scraper.page is not None

    def test_incognito_launch(
        self,
        mock_playwright: MagicMock,
        mock_context: MagicMock
    ) -> None:
        """Test incognito mode browser launch.

        Verifies
        --------
        - Persistent context is used when incognito=True
        - Correct parameters are passed

        Parameters
        ----------
        mock_playwright : MagicMock
            Mocked Playwright instance
        mock_context : MagicMock
            Mocked BrowserContext instance

        Returns
        -------
        None
        """
        scraper = PlaywrightScraper(bool_incognito=True, bool_headless=False)
        
        with scraper.launch():
            pass

        mock_playwright.chromium.launch_persistent_context.assert_called_once_with(
            user_data_dir=None,
            headless=False,
            proxy=None,
            viewport={"width": 1920, "height": 1080},
            user_agent=scraper.user_agent
        )
        assert not mock_context.new_page.called

    def test_launch_with_proxy(
        self,
        mock_playwright: MagicMock
    ) -> None:
        """Test browser launch with proxy configuration.

        Verifies
        --------
        - Proxy settings are correctly passed to browser launch

        Parameters
        ----------
        mock_playwright : MagicMock
            Mocked Playwright instance

        Returns
        -------
        None
        """
        scraper = PlaywrightScraper(proxy="http://proxy:8080", bool_headless=False)
        
        with scraper.launch():
            pass

        mock_playwright.chromium.launch.assert_called_once_with(
            headless=False,
            proxy={"server": "http://proxy:8080"}
        )

    def test_launch_failure(self, mock_playwright: MagicMock) -> None:
        """Test browser launch failure handling.

        Verifies
        --------
        - Exceptions during launch are properly caught and logged
        - Resources are cleaned up on failure

        Parameters
        ----------
        mock_playwright : MagicMock
            Mocked Playwright instance

        Returns
        -------
        None
        """
        scraper = PlaywrightScraper(bool_headless=False)
        mock_playwright.chromium.launch.side_effect = Exception("Launch failed")

        with suppress(RuntimeError):
            scraper.launch()

        assert scraper.browser is None
        assert scraper.context is None
        assert scraper.page is None


class TestNavigation:
    """Tests for page navigation functionality."""

    def test_successful_navigation(
        self,
        scraper: Any, # noqa ANN401: typing.Any is disallowed
        mock_page: MagicMock
    ) -> None:
        """Test successful page navigation.

        Verifies
        --------
        - Page.goto is called with correct URL and timeout
        - Returns True on success

        Parameters
        ----------
        scraper : Any
            PlaywrightScraper instance
        mock_page : MagicMock
            Mocked Page instance

        Returns
        -------
        None
        """
        with scraper.launch():
            result = scraper.navigate("http://example.com")
            assert result is True
            mock_page.goto.assert_called_once_with(
                "http://example.com",
                timeout=30000
            )

    def test_navigation_with_custom_timeout(
        self,
        scraper: Any, # noqa ANN401: typing.Any is disallowed
        mock_page: MagicMock
    ) -> None:
        """Test navigation with custom timeout.

        Verifies
        --------
        - Custom timeout is properly passed to page.goto

        Parameters
        ----------
        scraper : Any
            PlaywrightScraper instance
        mock_page : MagicMock
            Mocked Page instance

        Returns
        -------
        None
        """
        with scraper.launch():
            scraper.navigate("http://example.com", timeout=10000)
            mock_page.goto.assert_called_once_with(
                "http://example.com",
                timeout=10000
            )

    def test_navigation_failure(
        self,
        scraper: Any, # noqa ANN401: typing.Any is disallowed
        mock_page: MagicMock
    ) -> None:
        """Test navigation failure handling.

        Verifies
        --------
        - Returns False on navigation failure
        - Exception is logged

        Parameters
        ----------
        scraper : Any
            PlaywrightScraper instance
        mock_page : MagicMock
            Mocked Page instance

        Returns
        -------
        None
        """
        mock_page.goto.side_effect = Exception("Navigation failed")
        
        with scraper.launch():
            result = scraper.navigate("http://example.com")
            assert result is False

    def test_cookie_acceptance(
        self,
        scraper: Any, # noqa ANN401: typing.Any is disallowed
        mock_page: MagicMock
    ) -> None:
        """Test cookie acceptance handling.

        Verifies
        --------
        - Click is attempted on cookie acceptance button
        - No exception raised if button not found

        Parameters
        ----------
        scraper : Any
            PlaywrightScraper instance
        mock_page : MagicMock
            Mocked Page instance

        Returns
        -------
        None
        """
        scraper.bool_accept_cookies = True
        
        with scraper.launch():
            scraper.navigate("http://example.com")
            mock_page.click.assert_called_once_with(
                "text=Accept All",
                timeout=3000
            )


class TestElementSelection:
    """Tests for element selection and inspection methods."""

    def test_selector_exists_visible(
        self,
        scraper: Any, # noqa ANN401: typing.Any is disallowed
        mock_page: MagicMock
    ) -> None:
        """Test selector_exists with visible=True.

        Verifies
        --------
        - Correct wait_for_selector call for visible element
        - Returns True when element exists

        Parameters
        ----------
        scraper : Any
            PlaywrightScraper instance
        mock_page : MagicMock
            Mocked Page instance

        Returns
        -------
        None
        """
        mock_locator = MagicMock()
        mock_element = MagicMock()
        mock_page.locator.return_value = mock_locator
        mock_locator.first = mock_element
        mock_element.is_visible.return_value = True
        # Ensure wait_for_selector returns the same locator
        mock_page.wait_for_selector.return_value = mock_locator

        with scraper.launch():
            result = scraper.selector_exists("//div", visible=True)
            assert result is True
            mock_page.wait_for_selector.assert_not_called()  # Not called when visible=True
            mock_element.is_visible.assert_called_once()

    def test_selector_exists_hidden(
        self,
        scraper: Any, # noqa ANN401: typing.Any is disallowed
        mock_page: MagicMock
    ) -> None:
        """Test selector_exists with visible=False.

        Verifies
        --------
        - Correct is_hidden check is performed
        - Returns True when element is hidden

        Parameters
        ----------
        scraper : Any
            PlaywrightScraper instance
        mock_page : MagicMock
            Mocked Page instance

        Returns
        -------
        None
        """
        mock_locator = MagicMock()
        mock_element = MagicMock()
        mock_page.locator.return_value = mock_locator
        mock_locator.first = mock_element
        mock_element.is_hidden.return_value = True
        mock_page.wait_for_selector.return_value = mock_locator

        with scraper.launch():
            result = scraper.selector_exists("//div", visible=False)
            assert result is True
            mock_element.is_hidden.assert_called_once()

    def test_selector_exists_with_timeout(
        self,
        scraper: Any, # noqa ANN401: typing.Any is disallowed
        mock_page: MagicMock
    ) -> None:
        """Test selector_exists with timeout parameter.

        Verifies
        --------
        - wait_for_selector is called with correct timeout
        - Returns True when element appears within timeout

        Parameters
        ----------
        scraper : Any
            PlaywrightScraper instance
        mock_page : MagicMock
            Mocked Page instance

        Returns
        -------
        None
        """
        mock_locator = MagicMock()
        mock_element = MagicMock()
        mock_page.locator.return_value = mock_locator
        mock_locator.first = mock_element
        mock_element.is_visible.return_value = True
        mock_page.wait_for_selector.return_value = mock_locator

        with scraper.launch():
            result = scraper.selector_exists("//div", timeout=5000, visible=True)
            assert result is True
            mock_page.wait_for_selector.assert_called_once_with(
                "//div",
                state="visible",
                timeout=5000
            )

    def test_get_element_success(
        self,
        scraper: Any, # noqa ANN401: typing.Any is disallowed
        mock_page: MagicMock
    ) -> None:
        """Test successful get_element call.

        Verifies
        --------
        - Returns dictionary with element properties
        - Includes text, html and bounding box

        Parameters
        ----------
        scraper : Any
            PlaywrightScraper instance
        mock_page : MagicMock
            Mocked Page instance

        Returns
        -------
        None
        """
        mock_element = MagicMock()
        mock_element.text_content.return_value = "test content"
        mock_element.inner_html.return_value = "<div>test</div>"
        mock_element.bounding_box.return_value = {"x": 10, "y": 20}
        mock_locator = MagicMock()
        mock_locator.first = mock_element
        mock_page.locator.return_value = mock_locator
        mock_page.wait_for_selector.return_value = mock_locator

        with scraper.launch():
            result = scraper.get_element("//div")
            assert result == {
                "text": "test content",
                "html": "<div>test</div>",
                "bounding_box": {"x": 10, "y": 20}
            }

    def test_get_element_empty_text(
        self,
        scraper: Any, # noqa ANN401: typing.Any is disallowed
        mock_page: MagicMock
    ) -> None:
        """Test get_element with empty text content.

        Verifies
        --------
        - Returns None when element has no text content

        Parameters
        ----------
        scraper : Any
            PlaywrightScraper instance
        mock_page : MagicMock
            Mocked Page instance

        Returns
        -------
        None
        """
        mock_element = MagicMock()
        mock_element.text_content.return_value = "   "
        mock_page.locator.return_value.first = mock_element
        
        with scraper.launch():
            result = scraper.get_element("//div")
            assert result is None

    def test_get_elements_multiple(
        self,
        scraper: Any, # noqa ANN401: typing.Any is disallowed
        mock_page: MagicMock
    ) -> None:
        """Test get_elements with multiple matches.

        Verifies
        --------
        - Returns list of element dictionaries
        - Skips elements with empty text

        Parameters
        ----------
        scraper : Any
            PlaywrightScraper instance
        mock_page : MagicMock
            Mocked Page instance

        Returns
        -------
        None
        """
        mock_element1 = MagicMock()
        mock_element1.text_content.return_value = "content 1"
        mock_element1.inner_html.return_value = "<div>1</div>"
        mock_element1.bounding_box.return_value = {"x": 1, "y": 1}
        
        mock_element2 = MagicMock()
        mock_element2.text_content.return_value = "   "
        
        mock_element3 = MagicMock()
        mock_element3.text_content.return_value = "content 3"
        mock_element3.inner_html.return_value = "<div>3</div>"
        mock_element3.bounding_box.return_value = {"x": 3, "y": 3}
        
        mock_locator = MagicMock()
        mock_locator.all.return_value = [mock_element1, mock_element2, mock_element3]
        mock_page.locator.return_value = mock_locator
        mock_page.wait_for_selector.return_value = mock_locator

        with scraper.launch():
            results = scraper.get_elements("//div")
            assert len(results) == 2
            assert results[0]["text"] == "content 1"
            assert results[1]["text"] == "content 3"
    
    def test_get_element_attribute(
        self,
        scraper: Any, # noqa ANN401: typing.Any is disallowed
        mock_page: MagicMock
    ) -> None:
        """Test get_element_attrb method.

        Verifies
        --------
        - Returns attribute value when present
        - Returns None on error

        Parameters
        ----------
        scraper : Any
            PlaywrightScraper instance
        mock_page : MagicMock
            Mocked Page instance

        Returns
        -------
        None
        """
        mock_element = MagicMock()
        mock_element.get_attribute.return_value = "http://example.com"
        mock_locator = MagicMock()
        mock_locator.first = mock_element
        mock_page.locator.return_value = mock_locator
        mock_page.wait_for_selector.return_value = mock_locator

        with scraper.launch():
            result = scraper.get_element_attrb("//a", "href")
            assert result == "http://example.com"
            mock_element.get_attribute.assert_called_once_with("href")


class TestHTMLExport:
    """Tests for HTML content export functionality."""

    def test_export_html_success(
        self,
        scraper: Any, # noqa ANN401: typing.Any is disallowed
        tmp_path: Path
    ) -> None:
        """Test successful HTML export.

        Verifies
        --------
        - File is created with correct content
        - Returns correct file path
        - Directory is created if not exists

        Parameters
        ----------
        scraper : Any
            PlaywrightScraper instance
        tmp_path : Path
            Temporary directory path

        Returns
        -------
        None
        """
        export_dir = tmp_path / "exports"
        test_content = "<html><body>Test</body></html>"
        
        with scraper.launch():
            file_path = scraper.export_html(
                content=test_content,
                folder_path=str(export_dir),
                filename="test_page",
                bool_include_timestamp=False 
            )
            
            assert file_path.startswith(str(export_dir / "test_page"))
            assert file_path.endswith(".html")
            assert os.path.exists(file_path)
            with open(file_path, encoding="utf-8") as f:
                assert f.read() == test_content

    def test_export_html_auto_filename(
        self,
        scraper: Any, # noqa ANN401: typing.Any is disallowed
        mock_page: MagicMock,
        tmp_path: Path
    ) -> None:
        """Test HTML export with automatic filename generation.

        Verifies
        --------
        - Filename is generated from URL when not provided
        - Special characters are replaced

        Parameters
        ----------
        scraper : Any
            PlaywrightScraper instance
        mock_page : MagicMock
            Mocked Page instance
        tmp_path : Path
            Temporary directory path

        Returns
        -------
        None
        """
        export_dir = tmp_path / "exports"
        mock_page.url = "https://example.com/path?param=value"
        scraper.get_current_url = MagicMock(return_value="https://example.com/path?param=value")

        with scraper.launch():
            file_path = scraper.export_html(
                content="<html></html>",
                folder_path=str(export_dir)
            )
            
            assert "example.com_path_param_value" in file_path
            assert file_path.endswith(".html")
            assert os.path.exists(file_path)

    def test_export_html_with_timestamp(
        self,
        scraper: Any, # noqa ANN401: typing.Any is disallowed
        tmp_path: Path
    ) -> None:
        """Test HTML export with timestamp in filename.

        Verifies
        --------
        - Timestamp is appended to filename when requested
        - Format is correct (YYYYMMDD_HHMMSS)

        Parameters
        ----------
        scraper : Any
            PlaywrightScraper instance
        tmp_path : Path
            Temporary directory path

        Returns
        -------
        None
        """
        export_dir = tmp_path / "exports"
        
        with scraper.launch():
            file_path = scraper.export_html(
                content="<html></html>",
                folder_path=str(export_dir),
                filename="test",
                bool_include_timestamp=True
            )
            
            assert "_202" in file_path  # Will match any timestamp starting with current year
            assert file_path.endswith(".html")

    def test_export_html_failure(
        self,
        scraper: Any, # noqa ANN401: typing.Any is disallowed
        tmp_path: Path
    ) -> None:
        """Test HTML export failure handling.

        Verifies
        --------
        - Exception is raised on file write failure
        - Error is logged

        Parameters
        ----------
        scraper : Any
            PlaywrightScraper instance
        tmp_path : Path
            Temporary directory path

        Returns
        -------
        None
        """
        export_dir = tmp_path / "readonly"
        export_dir.mkdir(mode=0o444)  # Read-only directory
        
        with scraper.launch(), pytest.raises(RuntimeError, match="Failed to save HTML file"):
            scraper.export_html(
                content="<html></html>",
                folder_path=str(export_dir)
            )


class TestValidation:
    """Tests for validation methods."""

    def test_validate_timeout_positive(
        self, 
        scraper: Any # noqa ANN401: typing.Any is disallowed
    ) -> None:
        """Test valid timeout values.

        Verifies
        --------
        - No exception raised for positive timeout or None

        Parameters
        ----------
        scraper : Any
            PlaywrightScraper instance

        Returns
        -------
        None
        """
        scraper._validate_timeout(1000)
        scraper._validate_timeout(None)

    def test_validate_timeout_negative(
        self, 
        scraper: Any # noqa ANN401: typing.Any is disallowed
    ) -> None:
        """Test invalid timeout values.

        Verifies
        --------
        - ValueError raised for negative timeout

        Parameters
        ----------
        scraper : Any
            PlaywrightScraper instance

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="timeout must be a positive integer or None"):
            scraper._validate_timeout(-1000)