import os
from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright
from typing import Optional, List, Dict, Union
from contextlib import contextmanager
from logging import Logger
from stpstone.utils.loggs.create_logs import CreateLog


class PlaywrightScraper:
    def __init__(
        self,
        bl_headless: bool = True,
        user_agent: Optional[str] = None,
        proxy: Optional[str] = None,
        viewport: Dict[str, int] = None,
        int_default_timeout: int = 30000,
        bl_accept_cookies: bool = True, 
        bl_incognito: bool = False,
        logger: Optional[Logger] = None
    ):
        """
        Initialize a generic Playwright scraper
        
        Args:
            bl_headless (bool): Run in bl_headless mode
            user_agent (str): Custom user agent string
            proxy (str): Proxy server address
            viewport (dict): Browser viewport settings {"width": 1920, "height": 1080}
            int_default_timeout (int): Default timeout in milliseconds
            bl_accept_cookies (bool): Attempt to accept cookies if popup appears
            bl_incognito (bool): Run in incognito mode
        """
        self.bl_headless = bl_headless
        self.user_agent = user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        self.proxy = proxy
        self.viewport = viewport or {"width": 1920, "height": 1080}
        self.int_default_timeout = int_default_timeout
        self.bl_accept_cookies = bl_accept_cookies
        self.bl_incognito = bl_incognito
        self.logger = logger
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
                "headless": self.bl_headless,
                "proxy": {"server": self.proxy} if self.proxy else None
            }
            if self.bl_incognito:
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
        except Exception as e:
            CreateLog().log_message(self.logger, f"Error launching browser: {e}", "error")
            raise
        finally:
            self.close()

    def close(self):
        """Clean up resources"""
        if hasattr(self, "context") and self.context:
            self.context.close()
        if hasattr(self, "browser") and self.browser:
            self.browser.close()
        if hasattr(self, "playwright") and self.playwright:
            self.playwright.stop()

    def navigate(self, url: str, timeout: Optional[int] = None):
        """Navigate to a URL"""
        try:
            self.page.goto(url, timeout=timeout or self.int_default_timeout)
            if self.bl_accept_cookies:
                self._handle_cookie_popup()
            return True
        except Exception as e:
            CreateLog().log_message(self.logger, f"Error navigating to {url}: {e}", "error")
            return False

    def get_current_url(self) -> Optional[str]:
        """
        Get the current page URL
        
        Returns:
            str: Current URL if page exists, None otherwise
        """
        if hasattr(self, 'page') and self.page:
            return self.page.url
        return None

    def _handle_cookie_popup(self, timeout: int = 3000):
        """Attempt to accept cookies if popup appears"""
        try:
            self.page.click("text=Accept All", timeout=timeout)
            CreateLog().log_message(self.logger, "Accepted cookies", "info")
        except:
            pass

    def selector_exists(
        self,
        selector: str,
        selector_type: str = "xpath",
        timeout: Optional[int] = None,
        visible: Optional[bool] = None
    ) -> bool:
        """
        Check if a selector exists on the page
        
        Args:
            selector (str): The selector to check
            selector_type (str): "xpath" or "css"
            timeout (int): Maximum time to wait in milliseconds (None for no wait)
            visible (bool): Check for visibility (True=visible, False=hidden, None=either)
            
        Returns:
            bool: True if selector exists (with given visibility), False otherwise
        """
        try:
            if selector_type.lower() not in ("xpath", "css"):
                raise ValueError(f"Unsupported selector type: {selector_type}")
            if timeout is not None:
                # wait for selector with specified visibility
                state = "visible" if visible is True else (
                        "hidden" if visible is False else "attached")
                self.page.wait_for_selector(
                    selector,
                    state=state,
                    timeout=timeout
                )
                return True
            else:
                # immediate check without waiting
                if visible is True:
                    return self.page.locator(selector).first.is_visible()
                elif visible is False:
                    return self.page.locator(selector).first.is_hidden()
                else:
                    return self.page.locator(selector).count() > 0
                    
        except Exception as e:
            CreateLog().log_message(
                self.logger, 
                f"Selector check failed for {selector}: {str(e)}", 
                "warning"
            )
            return False

    def get_element(
        self,
        selector: str,
        selector_type: str = "xpath",
        timeout: Optional[int] = None,
        visible: bool = True
    ) -> Optional[Dict[str, Union[str, Dict]]]:
        """
        Get a single element matching selector
        
        Args:
            selector (str): The selector to find the element
            selector_type (str): "xpath" or "css"
            timeout (int): Maximum time to wait in milliseconds
            visible (bool): Wait for element to be visible
            
        Returns:
            Dictionary with element"s text, html and bounding box info, or None if not found
        """
        try:
            if selector_type.lower() == "xpath":
                if timeout is not None:
                    self.page.wait_for_selector(
                        selector,
                        state="visible" if visible else "attached",
                        timeout=timeout or self.int_default_timeout
                    )
                element = self.page.locator(selector).first
            elif selector_type.lower() == "css":
                if timeout is not None:
                    self.page.wait_for_selector(
                        selector,
                        state="visible" if visible else "attached",
                        timeout=timeout or self.int_default_timeout
                    )
                element = self.page.locator(selector).first
            else:
                raise ValueError(f"Unsupported selector type: {selector_type}")
            if not element.text_content().strip():
                return None
            return {
                "text": element.text_content().strip(),
                "html": element.inner_html(),
                "bounding_box": element.bounding_box()
            }
        except Exception as e:
            CreateLog().log_message(self.logger, f"Error getting element: {e}", "error")
            return None
    
    def get_element_attrb(
        self,
        selector: str,
        str_attribute: str = "href",
        selector_type: str = "xpath"
    ) -> Optional[str]:
        try:
            if selector_type == "xpath":
                element = self.page.locator(selector).first
            elif selector_type == "css":
                element = self.page.locator(selector).first
            else:
                raise ValueError(f"Unsupported selector type: {selector_type}")
            return element.get_attribute(str_attribute)
        except Exception as e:
            CreateLog().log_message(self.logger, f"Error getting href: {e}", "error")
            return None

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
            selector_type (str): "xpath" or "css"
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
                    timeout=timeout or self.int_default_timeout
                )
                elements = self.page.locator(selector).all()
            elif selector_type.lower() == "css":
                self.page.wait_for_selector(
                    selector,
                    state="visible" if visible else "attached",
                    timeout=timeout or self.int_default_timeout
                )
                elements = self.page.locator(selector).all()
            else:
                raise ValueError(f"Unsupported selector type: {selector_type}")
            return [{
                "text": el.text_content().strip(),
                "html": el.inner_html(),
                "bounding_box": el.bounding_box()
            } for el in elements if el.text_content().strip()]
        except Exception as e:
            CreateLog().log_message(self.logger, f"Error getting elements: {e}", "error")
            return []

    def get_list_data(
        self,
        table_selector: str,
        selector_type: str = "xpath",
        timeout: Optional[int] = None
    ) -> List[str]:
        """
        Get text content from table cells
        
        Args:
            table_selector (str): Selector for table or table cells
            selector_type (str): "xpath" or "css"
            timeout (int): Maximum time to wait in milliseconds
            
        Returns:
            List of text content from table cells
        """
        elements = self.get_elements(table_selector, selector_type, timeout)
        return [el["text"] for el in elements]
    
    def export_html(
        self,
        content: str,
        folder_path: str = "scraped_data",
        filename: Optional[str] = None,
        bl_include_timestamp: bool = True
    ) -> str:
        """
        Export HTML content to a file in the specified folder.
        
        Args:
            content (str): HTML content to save
            folder_path (str): Path to the output folder (default: "scraped_data")
            filename (str): Optional custom filename (without extension)
            bl_include_timestamp (bool): Whether to append bl_include_timestamp to filename
            
        Returns:
            str: Path to the saved file
        """
        try:
            Path(folder_path).mkdir(parents=True, exist_ok=True)
            if not filename:
                url = self.page.url if hasattr(self, "page") and self.page else "scraped"
                filename = url.split("//")[-1].replace("/", "_").replace("?", "_").replace("=", "_")
            if bl_include_timestamp:
                timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{filename}_{timestamp_str}"
            if not filename.endswith(".html"):
                filename += ".html"
            file_path = os.path.join(folder_path, filename)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
            CreateLog().log_message(self.logger, f"HTML content saved to {file_path}", "info")
            return file_path
        except Exception as e:
            CreateLog().log_message(self.logger, f"Error saving HTML file: {e}", "error")
            raise Exception(f"Error saving HTML file: {e}")