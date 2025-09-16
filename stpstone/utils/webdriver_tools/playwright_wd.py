"""Playwright-based web scraping utilities.

This module provides a class for web scraping using Playwright with features for browser 
automation,element selection, and content extraction. Includes robust error handling and logging 
capabilities.
"""

from contextlib import contextmanager
from datetime import datetime
from logging import Logger
import os
from pathlib import Path
from typing import Literal, Optional, TypedDict

from playwright.sync_api import sync_playwright

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
        try:
            self.playwright = sync_playwright().start()
            browser_args = {
                "headless": self.bool_headless,
                "proxy": {"server": self.proxy} if self.proxy else None
            }

            if self.bool_incognito:
                self.context = self.playwright.chromium.launch_persistent_context(
                    user_data_dir=None,
                    **browser_args,
                    viewport=self.viewport,
                    user_agent=self.user_agent
                )
                self.page = self.context.pages[0]
            else:
                self.browser = self.playwright.chromium.launch(**browser_args)
                self.context = self.browser.new_context(
                    viewport=self.viewport,
                    user_agent=self.user_agent
                )
                self.page = self.context.new_page()

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
        selector_type: Literal["xpath", "css"] = "xpath",
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
        list_ser_strategies: list[dict[str, str]],
        timeout: Optional[int] = 30_000,
    ) -> None:
        """Trigger strategies.

        Parameters
        ----------
        list_ser_strategies : list[dict[str, str]]
            List of strategy dictionaries
        timeout : Optional[int]
            Maximum wait time in milliseconds

        Returns
        -------
        None
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
        
        for i, strategy in enumerate(list_ser_strategies, 1):
            try:
                if self.logger:
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
                
                # ensure element is clickable
                element = self.page.locator(selector)
                element.wait_for(state='attached', timeout=timeout)
                
                # click the element
                self.page.click(selector, timeout=timeout)
                
                if self.logger:
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
                    # if networkidle fails, just wait a bit more
                    self.page.wait_for_timeout(3000)
                
                # Verify that content has loaded by checking for tables
                tables_found = False
                table_selectors = [
                    "table",
                    "[id='profile'] table",
                    ".table",
                    "//table[contains(., 'DATA') or contains(., 'MES')]"
                ]
                
                for table_selector in table_selectors:
                    try:
                        if table_selector.startswith('//'):
                            check_selector = f"xpath={table_selector}"
                        else:
                            check_selector = table_selector
                            
                        tables = self.page.locator(check_selector).all()
                        if len(tables) > 0:
                            # Check if any table has meaningful content
                            for table in tables[:3]:  # Check first 3 tables
                                text_content = table.text_content()
                                if text_content and len(text_content) > 100:  # Has substantial content
                                    tables_found = True
                                    break
                            if tables_found:
                                break
                    except Exception:
                        continue
                
                if tables_found:
                    if self.logger:
                        CreateLog().log_message(
                            self.logger, 
                            "Content successfully loaded - tables detected", 
                            "info"
                        )
                    return
                else:
                    if self.logger:
                        CreateLog().log_message(
                            self.logger, 
                            "Click successful but content verification failed", 
                            "warning"
                        )
                    # Continue to next strategy
                        
            except Exception as e:
                if self.logger:
                    CreateLog().log_message(
                        self.logger, 
                        f"Strategy {i} failed ({strategy['description']}): {e}", 
                        "error"
                    )
                continue
        
        # If all strategies failed
        if self.logger:
            CreateLog().log_message(
                self.logger, 
                "All list_ser_strategies failed", 
                "error"
            )
        
        # Final attempt: wait a bit more in case content loads automatically
        self.page.wait_for_timeout(5000)