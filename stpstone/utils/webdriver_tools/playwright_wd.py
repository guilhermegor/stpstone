"""Playwright-based web scraping utilities.

This module provides a class for web scraping using Playwright with features for browser 
automation,element selection, and content extraction. Includes robust error handling and logging 
capabilities.
"""

from contextlib import contextmanager, suppress
from datetime import datetime
from logging import Logger
import os
from pathlib import Path
import re
from typing import Any, Literal, Optional, TypedDict, Union

from playwright.sync_api import Page, sync_playwright
import requests

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.loggs.create_logs import CreateLog


class ReturnGetElement(TypedDict):
    """Return type for get_element method.

    Parameters
    ----------
    text : str
        Text content of the element
    html : str
        HTML content of the element
    bounding_box : dict
        Bounding box coordinates of the element
    """

    text: str
    html: str
    bounding_box: dict


class PlaywrightScraper(metaclass=TypeChecker):
    """Playwright-based web scraper with configurable browser settings."""

    def _validate_timeout(self, timeout: Optional[int]) -> None:
        """Validate timeout parameter.

        Parameters
        ----------
        timeout : Optional[int]
            Timeout value to validate

        Raises
        ------
        ValueError
            If timeout is negative
        """
        if timeout and timeout < 0:
            raise ValueError("timeout must be a positive integer or None")

    def __init__(
        self,
        bool_headless: bool = True,
        user_agent: Optional[str] = None,
        proxy: Optional[str] = None,
        viewport: Optional[dict[str, int]] = None,
        int_default_timeout: int = 10,
        bool_accept_cookies: bool = True,
        bool_incognito: bool = False,
        bool_minimized_window: bool = False,
        logger: Optional[Logger] = None
    ) -> None:
        """Initialize Playwright scraper instance.

        Parameters
        ----------
        bool_headless : bool
            Run browser in headless mode (default: True)
        user_agent : Optional[str]
            Custom user agent string
        proxy : Optional[str]
            Proxy server address
        viewport : Optional[dict[str, int]]
            Browser viewport settings (default: {"width": 1920, "height": 1080})
        int_default_timeout : int
            Default timeout in milliseconds (default: 10)
        bool_accept_cookies : bool
            Attempt to accept cookies if popup appears (default: True)
        bool_incognito : bool
            Run browser in incognito mode (default: False)
        bool_minimized_window : bool
            Run browser in minimized window mode (default: False)
        logger : Optional[Logger]
            Custom logger instance
        """
        self.bool_headless = bool_headless
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        self.proxy = proxy
        self.viewport = viewport or {"width": 1920, "height": 1080}
        self.int_default_timeout = int_default_timeout
        self.bool_accept_cookies = bool_accept_cookies
        self.bool_incognito = bool_incognito
        self.bool_minimized_window = bool_minimized_window
        self.logger = logger
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    @contextmanager
    def launch(self) -> "PlaywrightScraper":
        """Context manager for browser session.

        Yields
        ------
        PlaywrightScraper
            The scraper instance with active browser session

        Raises
        ------
        RuntimeError
            If browser launch fails
        """
        launch_args: list[str] = []

        try:
            self.playwright = sync_playwright().start()
            if not self.bool_headless and self.bool_minimized_window:
                launch_args = [
                    "--start-minimized",
                    "--window-position=0,0",
                    "--window-size=400,300",
                    "--disable-extensions",
                    "--disable-plugins",
                    "--disable-background-timer-throttling",
                    "--disable-renderer-backgrounding",
                    "--disable-background-networking",
                    "--no-first-run",
                ]
                CreateLog().log_message(
                    self.logger,
                    "Browser configured with minimized window settings",
                    "info"
                )

            if self.bool_incognito:
                self.context = self.playwright.chromium.launch_persistent_context(
                    user_data_dir=None,
                    headless=self.bool_headless,
                    proxy={"server": self.proxy} if self.proxy else None,
                    args=launch_args,
                    viewport=self.viewport,
                    user_agent=self.user_agent
                )
                self.page = self.context.pages[0]
            else:
                browser_args = {
                    "headless": self.bool_headless,
                    "proxy": {"server": self.proxy} if self.proxy else None,
                    "args": launch_args if launch_args else None
                }
                browser_args = {k: v for k, v in browser_args.items() if v is not None}
                
                self.browser = self.playwright.chromium.launch(**browser_args)
                self.context = self.browser.new_context(
                    viewport=self.viewport,
                    user_agent=self.user_agent
                )
                self.page = self.context.new_page()
            
            if not self.bool_headless and self.bool_minimized_window:
                self._minimize_window_os_level()

            self.page.set_default_timeout(self.int_default_timeout)
            yield self
        except Exception as err:
            CreateLog().log_message(
                self.logger,
                f"Error launching browser: {err}",
                "error"
            )
            raise RuntimeError(f"Browser launch failed: {err}") from err
        finally:
            self.close()

    def _minimize_window_os_level(self) -> None:
        """Minimize browser window using OS-specific commands.
        
        Notes
        -----
        [1] Extensions to install for linux distributions:
        ```bash
        # ubuntu/Debian
        sudo apt-get install wmctrl xdotool -y

        # fedora/RHEL  
        sudo dnf install wmctrl xdotool -y

        # arch Linux
        sudo pacman -S wmctrl xdotool -y
        ```
        """
        import subprocess
        import sys
        import time
        
        time.sleep(2)
        
        try:
            if sys.platform == "win32":
                subprocess.run( # noqa S607: starting a process with a partial executable path
                    [ # noqa S607: starting a process with a partial executable path
                        "powershell", "-WindowStyle", "Hidden", "-Command",
                        "Add-Type -TypeDefinition 'using System; using System.Runtime.InteropServices; " # noqa E501: line too long
                        "public class Win32 { [DllImport(\"user32.dll\")] public static extern bool ShowWindow(IntPtr hWnd, int nCmdShow); }';" # noqa E501: line too long
                        "(Get-Process | Where-Object {$_.MainWindowTitle -match 'Chrome|Chromium'}) | " # noqa E501: line too long
                        "ForEach-Object { [Win32]::ShowWindow($_.MainWindowHandle, 2) }"
                    ],
                    shell=True, capture_output=True, timeout=10
                )
                
            elif sys.platform == "darwin":
                subprocess.run(
                    [ # noqa S607: starting a process with a partial executable path
                        "osascript", "-e",
                        'tell application "System Events" to tell (first process whose name contains "Chrome") to set properties of windows to {miniaturized:true}' # noqa E501: line too long
                    ],
                    capture_output=True, timeout=10
                )
                
            elif sys.platform.startswith("linux"):
                
                success = False
                try:
                    subprocess.run( 
                        ["wmctrl", "-r", "Chrome", "-b", "add,minimized"], # noqa S607: starting a process with a partial executable path
                        check=True, capture_output=True, timeout=5
                    )
                    success = True
                except (subprocess.CalledProcessError, FileNotFoundError):
                    try:
                        subprocess.run(
                            ["wmctrl", "-r", "Chromium", "-b", "add,minimized"], # noqa S607: starting a process with a partial executable path
                            check=True, capture_output=True, timeout=5
                        )
                        success = True
                    except (subprocess.CalledProcessError, FileNotFoundError):
                        pass
                
                if not success:
                    try:
                        _ = subprocess.run(
                            ["xdotool", "search", "--name", "Chrome", "windowminimize"], # noqa S607: starting a process with a partial executable path
                            check=True, capture_output=True, timeout=5
                        )
                        success = True
                    except (subprocess.CalledProcessError, FileNotFoundError):
                        try:
                            subprocess.run(
                                ["xdotool", "search", "--name", "Chromium", "windowminimize"], # noqa S607: starting a process with a partial executable path
                                check=True, capture_output=True, timeout=5
                            )
                            success = True
                        except (subprocess.CalledProcessError, FileNotFoundError):
                            pass
                
                if not success:
                    CreateLog().log_message(
                        self.logger, 
                        "Linux minimization failed: install wmctrl or xdotool", 
                        "warning"
                    )
            
            CreateLog().log_message(
                self.logger, 
                "Browser window minimized via OS command", 
                "info"
            )
            
        except Exception as e:
            CreateLog().log_message(self.logger, f"OS minimization failed: {e}", "warning")

    def close(self) -> None:
        """Clean up browser resources.
        
        Returns
        -------
        None
        """
        try:
            if hasattr(self, "context") and self.context:
                self.context.close()
                self.context = None
            if hasattr(self, "browser") and self.browser:
                self.browser.close()
                self.browser = None
            if hasattr(self, "playwright") and self.playwright:
                self.playwright.stop()
                self.playwright = None
            self.page = None
        except Exception as err:
            CreateLog().log_message(
                self.logger,
                f"Error closing browser resources: {err}",
                "error"
            )

    def navigate(self, url: str, timeout: Optional[int] = None) -> bool:
        """Navigate to specified URL.

        Parameters
        ----------
        url : str
            URL to navigate to
        timeout : Optional[int]
            Custom timeout in milliseconds

        Returns
        -------
        bool
            True if navigation succeeded, False otherwise
        """
        try:
            self.page.goto(url, timeout=timeout or self.int_default_timeout)
            if self.bool_accept_cookies:
                self._handle_cookie_popup()
            return True
        except Exception as err:
            CreateLog().log_message(
                self.logger,
                f"Error navigating to {url}: {err}",
                "error"
            )
            return False

    def get_current_url(self) -> Optional[str]:
        """Get current page URL.

        Returns
        -------
        Optional[str]
            Current URL if page exists, None otherwise

        Raises
        ------
        RuntimeError
            If page is not initialized
        """
        if not self.page:
            raise RuntimeError("Page not initialized")
        
        try:
            return self.page.url
        except Exception as err:
            CreateLog().log_message(
                self.logger,
                f"Error getting current URL: {err}",
                "error"
            )
            return None

    def _handle_cookie_popup(self, timeout: Optional[int] = 30_000) -> None:
        """Attempt to accept cookies if popup appears.

        Parameters
        ----------
        timeout : Optional[int]
            Timeout for cookie acceptance attempt (default: 3000ms)
        """
        try:
            self.page.click("text=Accept All", timeout=timeout)
            CreateLog().log_message(self.logger, "Accepted cookies", "info")
        except Exception as err:
            CreateLog().log_message(self.logger, f"Error accepting cookies: {err}", "error")

    def selector_exists(
        self,
        selector: str,
        timeout: Optional[int] = None,
        visible: Optional[bool] = None
    ) -> bool:
        """Check if selector exists on page.

        Parameters
        ----------
        selector : str
            Selector to check
        timeout : Optional[int]
            Maximum wait time in milliseconds
        visible : Optional[bool]
            Visibility requirement (True=visible, False=hidden, None=either)

        Returns
        -------
        bool
            True if selector exists with given visibility, False otherwise

        Raises
        ------
        RuntimeError
            If page is not initialized
        """
        if not self.page:
            raise RuntimeError("Page not initialized")
        
        try:
            self._validate_timeout(timeout)
            timeout = timeout or self.int_default_timeout
            
            locator = self.page.locator(selector)
            
            if visible is True:
                return locator.first.is_visible()
            if visible is False:
                return locator.first.is_hidden()
            
            element = self.page.wait_for_selector(selector, state="visible", timeout=timeout)
            return element is not None
        except Exception as err:
            CreateLog().log_message(
                self.logger,
                f"Selector check failed for {selector}: {err}",
                "warning"
            )
            return False

    def get_element(
        self,
        selector: str,
        timeout: Optional[int] = None,
        visible: bool = True
    ) -> Optional[ReturnGetElement]:
        """Get single element matching selector.

        Parameters
        ----------
        selector : str
            Selector to find element
        timeout : Optional[int]
            Maximum wait time in milliseconds
        visible : bool
            Wait for element visibility (default: True)

        Returns
        -------
        Optional[ReturnGetElement]
            Element data if found, None otherwise

        Raises
        ------
        RuntimeError
            If page is not initialized
        """
        if self.page is None:
            raise RuntimeError("Page not initialized")
        
        try:
            self._validate_timeout(timeout)
            timeout = timeout or self.int_default_timeout
            
            self.page.wait_for_selector(
                selector,
                state="visible" if visible else "attached",
                timeout=timeout
            )
            locator = self.page.locator(selector)
            element = locator.first
            if not element:
                return None
                
            text = element.text_content(timeout=timeout)
            if not text or text.strip() == "":
                return None
                
            return {
                "text": text,
                "html": element.inner_html(timeout=timeout),
                "bounding_box": element.bounding_box()
            }
        except Exception as err:
            CreateLog().log_message(
                self.logger,
                f"Error getting element: {err}",
                "error"
            )
            return None
    
    def get_element_attrb(
        self,
        selector: str,
        attribute: str = "href",
        timeout: Optional[int] = None
    ) -> Optional[str]:
        """Get attribute value from element.

        Parameters
        ----------
        selector : str
            Selector to find element
        attribute : str
            Attribute to get value from
        timeout : Optional[int]
            Maximum wait time in milliseconds

        Returns
        -------
        Optional[str]
            Attribute value if found, None otherwise

        Raises
        ------
        RuntimeError
            If page is not initialized
        """
        if not self.page:
            raise RuntimeError("Page not initialized")
        
        try:
            self._validate_timeout(timeout)
            timeout = timeout or self.int_default_timeout
            
            locator = self.page.locator(selector)
            element = locator.first
            if not element:
                return None
                
            return element.get_attribute(attribute, timeout=timeout)
        except Exception as err:
            CreateLog().log_message(
                self.logger,
                f"Error getting attribute: {err}",
                "error"
            )
            return None
    
    def get_elements(
        self,
        selector: str,
        timeout: Optional[int] = None
    ) -> list[ReturnGetElement]:
        """Get all elements matching selector.

        Parameters
        ----------
        selector : str
            Selector to find elements
        timeout : Optional[int]
            Maximum wait time in milliseconds

        Returns
        -------
        list[ReturnGetElement]
            List of element data

        Raises
        ------
        RuntimeError
            If page is not initialized
        """
        if not self.page:
            raise RuntimeError("Page not initialized")

        try:
            self._validate_timeout(timeout)
            timeout = timeout or self.int_default_timeout
            
            # Ensure elements exist
            if not self.page.wait_for_selector(selector, state="visible", timeout=timeout):
                return []
                
            elements = self.page.locator(selector).all()
            results = []
            
            for element in elements:
                text = element.text_content(timeout=timeout)
                if text and text.strip():
                    results.append({
                        "text": text,
                        "html": element.inner_html(timeout=timeout),
                        "bounding_box": element.bounding_box()
                    })
                    
            return results
        except Exception as err:
            CreateLog().log_message(
                self.logger,
                f"Error getting elements: {err}",
                "error"
            )
            return []

    def get_list_data(
        self,
        table_selector: str,
        selector_type: Literal['xpath', 'css'] = "xpath",
        timeout: Optional[int] = None
    ) -> list[str]:
        """Get text content from table cells.

        Parameters
        ----------
        table_selector : str
            Selector for table or cells
        selector_type : Literal['xpath', 'css']
            Type of selector (default: "xpath")
        timeout : Optional[int]
            Maximum wait time in milliseconds

        Returns
        -------
        list[str]
            List of text content from cells
        """
        if selector_type == "xpath" and not table_selector.startswith("xpath="):
            table_selector = f"xpath={table_selector}"
        elements = self.get_elements(table_selector, timeout)
        return [el["text"] for el in elements]

    def export_html(
        self,
        content: str,
        folder_path: str = "scraped_data",
        filename: Optional[str] = None,
        bool_include_timestamp: bool = True
    ) -> str:
        """Export HTML content to file.

        Parameters
        ----------
        content : str
            HTML content to save
        folder_path : str
            Output folder path (default: "scraped_data")
        filename : Optional[str]
            Custom filename without extension
        bool_include_timestamp : bool
            Include timestamp in filename (default: True)

        Returns
        -------
        str
            Path to saved file

        Raises
        ------
        RuntimeError
            If file saving fails
        """
        try:
            Path(folder_path).mkdir(parents=True, exist_ok=True)
            if not filename:
                url = self.get_current_url() or "scraped"
                filename = (
                    url.split("//")[-1]
                    .replace("/", "_")
                    .replace("?", "_")
                    .replace("=", "_")
                )
                if not filename:
                    filename = "scraped"

            if bool_include_timestamp:
                timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{filename}_{timestamp_str}"

            if not filename.endswith(".html"):
                filename += ".html"

            file_path = os.path.join(folder_path, filename)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
                
            CreateLog().log_message(
                self.logger,
                f"HTML content saved to {file_path}",
                "info"
            )
            return file_path
        except Exception as err:
            CreateLog().log_message(
                self.logger,
                f"Error saving HTML file: {err}",
                "error"
            )
            raise RuntimeError(f"Failed to save HTML file: {err}") from err
        
    def trigger_strategies(
        self, 
        json_steps: list[dict[str, str]],
        target_content_selectors: Optional[list[str]] = None,
        timeout: Optional[int] = 30_000,
    ) -> None:
        """Trigger strategies.

        Strategies are records of selectors and actions to be performed on the page.
        This function iterates through the strategies and performs the actions.

        Parameters
        ----------
        json_steps : list[dict[str, str]]
            List of strategy dictionaries
        target_content_selectors : Optional[list[str]]
            List of selectors to check for target content
        timeout : Optional[int]
            Maximum wait time in milliseconds

        Returns
        -------
        None

        Notes
        -----
        [1] Go to Recorder section, in Google Chrome DevTools, and create a new recorder in order 
        to get the json_steps.
        [2] The target content selectors are used to check if the page has reached the desired 
        state.
        """
        try:
            self._handle_cookie_popup()
        except Exception as e:
            if self.logger:
                CreateLog().log_message(
                    self.logger, 
                    f"Cookie handling failed: {e}", 
                    "warning"
                )
        
        for i, strategy in enumerate(json_steps, 1):
            try:
                CreateLog().log_message(
                    self.logger, 
                    f"Trying strategy {i}: {strategy['description']}", 
                    "info"
                )
                
                if strategy['type'] == 'xpath':
                    selector = f"xpath={strategy['selector']}"
                elif strategy['type'] == 'aria':
                    selector = strategy['selector']
                else:
                    selector = strategy['selector']
                
                self.page.wait_for_selector(
                    selector, 
                    timeout=timeout,
                    state='visible'
                )

                element = self.page.locator(selector)
                element.wait_for(state='attached', timeout=timeout)
                
                self.page.click(selector, timeout=timeout)
                
                CreateLog().log_message(
                    self.logger, 
                    f"Successfully clicked button with: {strategy['description']}", 
                    "info"
                )
                
                self.page.wait_for_timeout(2000)
                
                # wait for network activity to settle
                try:
                    self.page.wait_for_load_state("networkidle", timeout=15000)
                except Exception:
                    self.page.wait_for_timeout(3000)
                
                bool_content_found: bool = False
                
                for table_selector in target_content_selectors:
                    with suppress(Exception):
                        if table_selector.startswith('//'):
                            check_selector = f"xpath={table_selector}"
                        else:
                            check_selector = table_selector
                            
                        el_contents = self.page.locator(check_selector).all()
                        if len(el_contents) > 0:
                            for table in el_contents[:3]:
                                text_content = table.text_content()
                                if text_content and len(text_content) > 10:
                                    bool_content_found = True
                                    break
                            if bool_content_found:
                                break
                
                if bool_content_found:
                    CreateLog().log_message(
                        self.logger, 
                        "Content successfully loaded - el_contents detected", 
                        "info"
                    )
                    return
                else:
                    CreateLog().log_message(
                        self.logger, 
                        "Click successful but content verification failed", 
                        "warning"
                    )
                        
            except Exception as e:
                CreateLog().log_message(
                    self.logger, 
                    f"Strategy {i} failed ({strategy['description']}): {e}", 
                    "error"
                )
                continue
            
        CreateLog().log_message(
            self.logger, 
            "All json_steps failed", 
            "error"
        )
        
        self.page.wait_for_timeout(5000)

    def get_json(
        self, 
        url: str, 
        timeout: Optional[int] = None, 
        cookies: Optional[Union[dict[str, str], list[dict[str, str]]]] = None,
    ) -> Union[dict, list]:
        """Fetch JSON data from a URL using Playwright browser automation.
    
        This method uses a real browser instance to bypass anti-bot protections
        like Cloudflare, handle JavaScript-rendered content, and manage complex
        authentication flows that would otherwise block simple HTTP requests.
        
        Parameters
        ----------
        url : str
            The URL to fetch JSON data from. Must be a valid HTTP/HTTPS URL
            that returns JSON content.
        timeout : Optional[int]
            Request timeout in milliseconds. If None, uses the instance's
            default timeout (self.int_default_timeout). Default is None.
        cookies : Optional[Union[dict[str, str], list[dict[str, str]]]]
            Cookies to add to the browser context. Can be either:
            - dict: Simple key-value pairs {"name": "value"}
            - list[dict]: Full cookie objects with domain, path, etc.
            Example: [{"name": "session_id", "value": "abc123", "domain": ".example.com"}]
            Default is None.
        
        Returns
        -------
        Union[dict, list]
            The parsed JSON response. Returns a dictionary for JSON objects
            or a list for JSON arrays.

        Examples
        --------
        Basic usage:
        >>> scraper = PlaywrightScraper()
        >>> data = scraper.get_json("https://api.example.com/data.json")
        >>> print(data["results"])
        
        With cookies:
        >>> cookies = {"session": "abc123", "user_pref": "en"}
        >>> data = scraper.get_json("https://protected-api.com/data.json", cookies=cookies)
        
        With timeout:
        >>> data = scraper.get_json("https://slow-api.com/data.json", timeout=30000)
        
        Notes
        -----
        - Uses Chromium browser engine with the configured user agent and viewport
        - Automatically handles JavaScript execution and dynamic content loading
        - More resource-intensive than simple HTTP requests but necessary for
        protected endpoints
        - Browser context is created fresh for each request to avoid state issues
        - Use this method when requests.get() fails with 403/bot detection errors
        """
        with sync_playwright() as playwright:
            self.browser = playwright.chromium.launch(headless=self.bool_headless)
            context = self.browser.new_context(
                user_agent=self.user_agent,
                viewport=self.viewport,
            )

            if cookies:
                if isinstance(cookies, dict):
                    cookie_list = [
                        {"name": name, "value": value, "domain": url.split("//")[1].split("/")[0]}
                        for name, value in cookies.items()
                    ]
                    context.add_cookies(cookie_list)
                else:
                    context.add_cookies(cookies)
            
            page = context.new_page()
            resp_req = page.goto(url, timeout=timeout or self.int_default_timeout)

            if resp_req.status != 200:
                raise requests.HTTPError(f"Request failed with status code {resp_req.status}")
            
            json_data = resp_req.json()
            return json_data
        
    def extract_data_from_xpath_mapping(
        self,
        page: Page,
        xpath_mapping: dict[str, str],
        row_idx: Optional[int] = None,
        text_part_idx: Optional[int] = None,
        timeout: int = 5_000,
        logger: Optional[Any] = None # noqa ANN401: typing.Any is not allowed
    ) -> dict[str, Any]: # noqa ANN401: typing.Any is not allowed
        """Extract data from page using XPath/CSS selector mapping.
        
        This method generalizes the data extraction pattern used in Anbima scrapers,
        allowing flexible configuration through parameters instead of hardcoded logic.
        
        Parameters
        ----------
        page : Page
            The Playwright page object to extract data from.
        xpath_mapping : dict[str, str]
            Dictionary mapping field names to their XPath/CSS selectors.
            Selectors can contain:
            - {} placeholder for row_idx formatting (e.g., '//table/tr[{}]/td[1]')
            - /@attribute suffix to extract attribute values (e.g., '//a/@href')
            - /text()[n] suffix to extract specific text node
        row_idx : Optional[int]
            Row index to format into selectors with {} placeholder.
            If None, {} placeholders are not replaced. Default is None.
        text_part_idx : Optional[int]
            Index of text part to extract when splitting by newline.
            Useful for elements with multiple text nodes.
            If None, uses full text. If specified, splits by '\n' and
            extracts the part at this index. Default is None.
        timeout : int
            Timeout in milliseconds for element visibility checks.
            Default is 5_000 (5 seconds).
        logger : Optional[Any]
            Logger instance for logging warnings. Default is None.
        
        Returns
        -------
        dict[str, Any]
            Dictionary with extracted data where keys are field names from
            xpath_mapping and values are extracted content (text or None).
        
        Examples
        --------
        Basic extraction without row index:
        >>> mapping = {
        ...     'title': '//h1[@class="title"]',
        ...     'link': '//a[@class="main-link"]/@href',
        ...     'description': '//p[@class="desc"]'
        ... }
        >>> data = scraper.extract_data_from_xpath_mapping(page, mapping)
        >>> print(data['title'])
        
        Extraction with row index (for table rows):
        >>> mapping = {
        ...     'name': '//table/tr[{}]/td[1]',
        ...     'value': '//table/tr[{}]/td[2]',
        ...     'link': '//table/tr[{}]/td[3]/a/@href'
        ... }
        >>> for i in range(1, 6):
        ...     data = scraper.extract_data_from_xpath_mapping(page, mapping, row_idx=i)
        ...     print(f"Row {i}: {data['name']} = {data['value']}")
        
        Extraction with text part index:
        >>> mapping = {
        ...     'timestamp': '//div[@class="update-info"]/p'
        ... }
        >>> # Extract second line of text (e.g., "Last updated: 2024-01-15")
        >>> data = scraper.extract_data_from_xpath_mapping(
        ...     page, mapping, text_part_idx=1
        ... )
        
        Combined usage:
        >>> mapping = {
        ...     'product_name': '//table/tbody/tr[{}]/td[1]',
        ...     'product_price': '//table/tbody/tr[{}]/td[2]',
        ...     'product_link': '//table/tbody/tr[{}]/td[1]/a/@href'
        ... }
        >>> products = []
        >>> for idx in range(1, 11):
        ...     product = scraper.extract_data_from_xpath_mapping(
        ...         page, mapping, row_idx=idx
        ...     )
        ...     products.append(product)
        
        Notes
        -----
        [1] The method handles three special XPath suffixes:
            - /@attribute: Extracts attribute value (e.g., href, src, class)
            - /text()[n]: Extracts specific text node (though text_part_idx is preferred)
            - No suffix: Extracts full inner text content
        
        [2] When text_part_idx is specified, the text content is split by '\n'
            and the part at the given index is returned. This is useful for
            elements that contain multiple lines or mixed content.
        
        [3] Error handling is built-in: if an element is not found or not visible,
            the field value will be None instead of raising an exception.
        
        [4] The {} placeholder in selectors is only replaced when row_idx is not None,
            allowing the same mapping to be reused for different rows.
        """
        data = {}
        
        for field_name, xpath in xpath_mapping.items():
            try:
                if row_idx is not None and '{}' in xpath:
                    xpath = xpath.format(row_idx)
                
                if '/@' in xpath:
                    parts = xpath.rsplit('/@', 1)
                    clean_xpath = parts[0]
                    attribute_name = parts[1]
                    
                    element = page.locator(f"xpath={clean_xpath}").first
                    
                    if element.count() > 0 and element.is_visible(timeout=timeout):
                        attr_value = element.get_attribute(attribute_name)
                        data[field_name] = attr_value if attr_value else None
                    else:
                        data[field_name] = None
                
                elif '/text()[' in xpath:
                    match = re.search(r'/text\(\)\[(\d+)\]', xpath)
                    if match:
                        text_node_idx = int(match.group(1))
                        clean_xpath = xpath.replace(f'/text()[{text_node_idx}]', '')
                    else:
                        clean_xpath = xpath.replace('/text()[2]', '')
                        text_node_idx = text_part_idx if text_part_idx is not None else 1
                    
                    element = page.locator(f"xpath={clean_xpath}").first
                    
                    if element.count() > 0 and element.is_visible(timeout=timeout):
                        text = element.inner_text().strip()
                        parts = text.split('\n')
                        
                        idx = text_node_idx if 'text_node_idx' in locals() else (
                            text_part_idx if text_part_idx is not None else 1
                        )
                        
                        if len(parts) > idx:
                            data[field_name] = parts[idx].strip() if parts[idx].strip() else None
                        else:
                            data[field_name] = text if text else None
                    else:
                        data[field_name] = None
                        
                else:
                    if xpath.startswith('//'):
                        selector = f"xpath={xpath}"
                    else:
                        selector = xpath
                    
                    element = page.locator(selector).first
                    
                    if element.count() > 0 and element.is_visible(timeout=timeout):
                        text = element.inner_text().strip()
                        
                        if text_part_idx is not None:
                            parts = text.split('\n')
                            if len(parts) > text_part_idx:
                                data[field_name] = (
                                    parts[text_part_idx].strip() 
                                    if parts[text_part_idx].strip() 
                                    else None
                                )
                            else:
                                data[field_name] = text if text else None
                        else:
                            data[field_name] = text if text else None
                    else:
                        data[field_name] = None
                        
            except Exception as e:
                if logger:
                    CreateLog().log_message(
                        logger,
                        f'Error extracting {field_name}: {e}',
                        "warning"
                    )
                data[field_name] = None
        
        return data
    
    def extract_multiple_rows(
        self,
        page: Page,
        xpath_mapping: dict[str, str],
        start_idx: int = 1,
        end_idx: Optional[int] = None,
        max_rows: Optional[int] = None,
        timeout: int = 5_000,
        logger: Optional[Any] = None,
        additional_data: Optional[dict[str, Any]] = None
    ) -> list[dict[str, Any]]:
        """Extract data from multiple rows using the same XPath mapping.
        
        This method automates the extraction of data from table rows or repeated
        elements, applying the same selector pattern across multiple indices.
        
        Parameters
        ----------
        page : Page
            The Playwright page object to extract data from.
        xpath_mapping : dict[str, str]
            Dictionary mapping field names to XPath/CSS selectors with {}
            placeholder for row index.
        start_idx : int
            Starting row index (inclusive). Default is 1.
        end_idx : Optional[int]
            Ending row index (inclusive). If None, extracts until no more
            elements are found. Default is None.
        max_rows : Optional[int]
            Maximum number of rows to extract. Useful for limiting results.
            If None, no limit is applied. Default is None.
        timeout : int
            Timeout in milliseconds for element visibility checks.
            Default is 5_000 (5 seconds).
        logger : Optional[Any]
            Logger instance for logging. Default is None.
        additional_data : Optional[dict[str, Any]]
            Additional data to include in each row's dictionary.
            Useful for adding context like fund_code, page_number, etc.
            Default is None.
        
        Returns
        -------
        list[dict[str, Any]]
            List of dictionaries, each containing extracted data for one row.
        
        Examples
        --------
        Extract all rows from a table:
        >>> mapping = {
        ...     'name': '//table/tr[{}]/td[1]',
        ...     'value': '//table/tr[{}]/td[2]',
        ...     'status': '//table/tr[{}]/td[3]'
        ... }
        >>> rows = scraper.extract_multiple_rows(page, mapping)
        
        Extract specific range with additional context:
        >>> rows = scraper.extract_multiple_rows(
        ...     page,
        ...     mapping,
        ...     start_idx=1,
        ...     end_idx=10,
        ...     additional_data={'fund_code': 'ABC123', 'page': 1}
        ... )
        
        Extract with maximum limit:
        >>> rows = scraper.extract_multiple_rows(
        ...     page,
        ...     mapping,
        ...     max_rows=50,
        ...     additional_data={'scrape_date': '2024-01-15'}
        ... )
        
        Notes
        -----
        [1] If end_idx is None, the method continues until it encounters
            an error (typically when no more elements are found), then stops.
        
        [2] Each dictionary in the returned list includes all fields from
            xpath_mapping plus any additional_data provided.
        
        [3] Failed row extractions are logged but don't stop the process;
            the method continues to the next row.
        """
        results = []
        current_idx = start_idx
        rows_extracted = 0
        
        while True:
            if end_idx is not None and current_idx > end_idx:
                break
            if max_rows is not None and rows_extracted >= max_rows:
                break
            
            try:
                row_data = self.extract_data_from_xpath_mapping(
                    page=page,
                    xpath_mapping=xpath_mapping,
                    row_idx=current_idx,
                    timeout=timeout,
                    logger=logger
                )
                
                if any(value is not None for value in row_data.values()):
                    if additional_data:
                        row_data.update(additional_data)
                    
                    results.append(row_data)
                    rows_extracted += 1
                else:
                    if end_idx is None:
                        if logger:
                            CreateLog().log_message(
                                logger,
                                f"No data found at row {current_idx}, stopping extraction",
                                "info"
                            )
                        break
                
                current_idx += 1
                
            except Exception as e:
                if logger:
                    CreateLog().log_message(
                        logger,
                        f"Error extracting row {current_idx}: {e}",
                        "warning"
                    )
                
                if end_idx is None:
                    break
                
                current_idx += 1
        
        return results
        
    def __del__(self) -> None:
        """Cleanup browser resources when the instance is destroyed.
        
        Safely closes the browser instance to prevent resource leaks.
        Uses suppress to handle cases where the browser might already
        be closed or never initialized.
        
        Returns
        -------
        None
            This method doesn't return anything.
        
        Notes
        -----
        [1] Called automatically when the object is garbage collected
        [2] Uses contextlib.suppress to handle cleanup errors gracefully
        [3] Prevents memory leaks from unclosed browser processes
        """
        with suppress(Exception):
            if hasattr(self, 'browser') and self.browser:
                self.browser.close()