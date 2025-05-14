from playwright.sync_api import sync_playwright
from typing import Optional, List, Dict, Union
import logging
from contextlib import contextmanager


class PlaywrightScraper:
    def __init__(
        self,
        headless: bool = True,
        user_agent: Optional[str] = None,
        proxy: Optional[str] = None,
        viewport: Dict[str, int] = None,
        default_timeout: int = 30000,
        accept_cookies: bool = True
    ):
        """
        Initialize a generic Playwright scraper
        
        Args:
            headless (bool): Run in headless mode
            user_agent (str): Custom user agent string
            proxy (str): Proxy server address
            viewport (dict): Browser viewport settings {'width': 1920, 'height': 1080}
            default_timeout (int): Default timeout in milliseconds
            accept_cookies (bool): Attempt to accept cookies if popup appears
        """
        self.headless = headless
        self.user_agent = user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        self.proxy = proxy
        self.viewport = viewport or {'width': 1920, 'height': 1080}
        self.default_timeout = default_timeout
        self.accept_cookies = accept_cookies
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    @contextmanager
    def launch(self):
        """Context manager for browser session"""
        try:
            self.playwright = sync_playwright().start()
            browser_args = {
                'headless': self.headless,
                'proxy': {'server': self.proxy} if self.proxy else None
            }
            
            self.browser = self.playwright.chromium.launch(**browser_args)
            self.context = self.browser.new_context(
                viewport=self.viewport,
                user_agent=self.user_agent
            )
            self.page = self.context.new_page()
            self.page.set_default_timeout(self.default_timeout)
            yield self
        except Exception as e:
            logging.error(f"Error launching browser: {e}")
            raise
        finally:
            self.close()

    def close(self):
        """Clean up resources"""
        if hasattr(self, 'context') and self.context:
            self.context.close()
        if hasattr(self, 'browser') and self.browser:
            self.browser.close()
        if hasattr(self, 'playwright') and self.playwright:
            self.playwright.stop()

    def navigate(self, url: str, timeout: Optional[int] = None):
        """Navigate to a URL"""
        try:
            self.page.goto(url, timeout=timeout or self.default_timeout)
            if self.accept_cookies:
                self._handle_cookie_popup()
            return True
        except Exception as e:
            logging.error(f"Error navigating to {url}: {e}")
            return False

    def _handle_cookie_popup(self, timeout: int = 3000):
        """Attempt to accept cookies if popup appears"""
        try:
            self.page.click("text=Accept All", timeout=timeout)
            logging.info("Accepted cookies")
        except:
            pass

    def get_elements(
        self,
        selector: str,
        selector_type: str = "xpath",
        timeout: Optional[int] = None,
        visible: bool = True
    ) -> List[Dict[str, Union[str, Dict]]]:
        """
        Get elements matching selector
        
        Args:
            selector (str): The selector to find elements
            selector_type (str): 'xpath' or 'css'
            timeout (int): Maximum time to wait in milliseconds
            visible (bool): Wait for elements to be visible
            
        Returns:
            List of elements with text, html and bounding box info
        """
        try:
            if selector_type.lower() == "xpath":
                self.page.wait_for_selector(
                    selector,
                    state="visible" if visible else "attached",
                    timeout=timeout or self.default_timeout
                )
                elements = self.page.locator(selector).all()
            elif selector_type.lower() == "css":
                self.page.wait_for_selector(
                    selector,
                    state="visible" if visible else "attached",
                    timeout=timeout or self.default_timeout
                )
                elements = self.page.locator(selector).all()
            else:
                raise ValueError(f"Unsupported selector type: {selector_type}")

            return [{
                'text': el.text_content().strip(),
                'html': el.inner_html(),
                'bounding_box': el.bounding_box()
            } for el in elements if el.text_content().strip()]
            
        except Exception as e:
            logging.error(f"Error getting elements: {e}")
            return []

    def get_table_data(
        self,
        table_selector: str,
        selector_type: str = "xpath",
        timeout: Optional[int] = None
    ) -> List[str]:
        """
        Get text content from table cells
        
        Args:
            table_selector (str): Selector for table or table cells
            selector_type (str): 'xpath' or 'css'
            timeout (int): Maximum time to wait in milliseconds
            
        Returns:
            List of text content from table cells
        """
        elements = self.get_elements(table_selector, selector_type, timeout)
        return [el['text'] for el in elements]