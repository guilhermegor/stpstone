"""Selenium WebDriver wrapper for browser automation.

This module provides a class for managing Selenium WebDriver instances with extended
functionality for network traffic monitoring, element interaction, and logging.
"""

from contextlib import suppress
import json
import logging
from typing import Literal, Optional, Union

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.loggs.create_logs import CreateLog


class SeleniumWD(metaclass=TypeChecker):
    """Wrapper class for Selenium WebDriver with extended functionality."""

    def _validate_url(self, url: str) -> None:
        """Validate URL format and content.

        Parameters
        ----------
        url : str
            URL to validate

        Raises
        ------
        ValueError
            If URL is empty
            If URL is not a string
            If URL does not start with http:// or https://
        """
        if not url:
            raise ValueError("URL cannot be empty")
        if not isinstance(url, str):
            raise ValueError("URL must be a string")
        if not (url.startswith("http://") or url.startswith("https://")):
            raise ValueError("URL must start with http:// or https://")

    def _validate_wait_time(self, seconds: int) -> None:
        """Validate wait time is positive.

        Parameters
        ----------
        seconds : int
            Time in seconds to validate

        Raises
        ------
        ValueError
            If seconds is negative
        """
        if seconds < 0:
            raise ValueError("Wait time cannot be negative")

    def __init__(
        self,
        url: str,
        path_webdriver: Optional[str] = None,
        int_port: Optional[int] = None,
        str_user_agent: Optional[str] = None,
        int_wait_load_seconds: int = 10,
        int_delay_seconds: int = 10,
        str_proxy: Optional[str] = None,
        bool_headless: bool = False,
        bool_incognito: bool = False,
        list_args: Optional[list[str]] = None, 
        logger: Optional[logging.Logger] = None
    ) -> None:
        """Initialize selenium web driver.

        Parameters
        ----------
        url : str
            URL to open
        path_webdriver : Optional[str]
            Path to webdriver executable (default: None)
        int_port : Optional[int]
            Port number for webdriver (default: None)
        str_user_agent : Optional[str]
            Custom user agent string (default: Chrome 91)
        int_wait_load_seconds : int
            Time to wait for page to load (default: 10)
        int_delay_seconds : int
            Time to wait between actions (default: 10)
        str_proxy : Optional[str]
            Proxy server address (default: None)
        bool_headless : bool
            Run in headless mode (default: False)
        bool_incognito : bool
            Run in incognito mode (default: False)
        list_args : Optional[list[str]]
            Additional Chrome arguments (default: None)
        logger : Optional[logging.Logger]
            Logger object (default: None)

        References
        ----------
        .. [1] https://gist.github.com/pzb/b4b6f57144aea7827ae4
        .. [2] https://chromedriver.chromium.org/capabilities
        .. [3] https://gist.github.com/dodying/34ea4760a699b47825a766051f47d43b
        """
        self._validate_url(url)
        self._validate_wait_time(int_wait_load_seconds)
        self._validate_wait_time(int_delay_seconds)

        self.url = url
        self.path_webdriver = path_webdriver
        self.int_port = int_port
        self.str_user_agent = str_user_agent if str_user_agent is not None else (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36"
        )
        self.int_wait_load_seconds = int_wait_load_seconds
        self.int_delay_seconds = int_delay_seconds
        self.bool_headless = bool_headless
        self.bool_incognito = bool_incognito
        self.logger = logger
        self.list_default_args = list_args if list_args is not None else [
            "--no-sandbox",
            "--disable-gpu",
            "--disable-setuid-sandbox",
            "--disable-web-security",
            "--disable-dev-shm-usage",
            "--memory-pressure-off",
            "--ignore-certificate-errors",
            "--disable-features=site-per-process",
            "--disable-extensions",
            "--disable-popup-blocking",
            "--disable-notifications",
            "--window-size=1920,1080",
            "--window-position=0,0",
            "--enable-unsafe-swiftshader",
            f"--user-agent={self.str_user_agent}"
        ]

        if self.bool_headless:
            self.list_default_args.append("--headless=new")
        if self.bool_incognito:
            self.list_default_args.append("--incognito")
        if str_proxy is not None:
            self.list_default_args.append(f"--proxy-server={str_proxy}")

        self.web_driver = self.get_web_driver()

    def get_web_driver(self) -> WebDriver:
        """Initialize and configure Chrome WebDriver instance.

        Returns
        -------
        WebDriver
            Configured Chrome WebDriver instance

        Raises
        ------
        ValueError
            If WebDriver initialization fails
        """
        try:
            capabilities = DesiredCapabilities.CHROME
            capabilities["goog:loggingPrefs"] = {"performance": "ALL"}
            
            browser_options = webdriver.ChromeOptions()
            for arg in self.list_default_args:
                browser_options.add_argument(arg)

            if self.path_webdriver:
                service = Service(executable_path=self.path_webdriver)
            else:
                service = Service(ChromeDriverManager().install())

            driver = webdriver.Chrome(
                service=service,
                options=browser_options,
                desired_capabilities=capabilities
            )
            driver.get(self.url)
            driver.implicitly_wait(self.int_wait_load_seconds)
            return driver
        except Exception as err:
            raise ValueError(f"Failed to initialize WebDriver: {str(err)}") from err

    def process_log(
        self, 
        log: dict[str, Union[str, dict]]
    ) -> Optional[dict[str, Union[str, dict]]]:
        """Process browser log entry to extract network response details.

        Parameters
        ----------
        log : dict[str, Union[str, dict]]
            Raw browser log entry

        Returns
        -------
        Optional[dict[str, Union[str, dict]]]
            Processed network response parameters if applicable, else None
        """
        try:
            log_data = json.loads(log["message"])
            message = log_data.get("message", log_data)
            if "Network.response" in message.get("method", "") and "params" in message:
                body = self.web_driver.execute_cdp_cmd(
                    "Network.getResponseBody",
                    {"requestId": message["params"]["requestId"]}
                )
                CreateLog().log_message(
                    self.logger, 
                    f"Browser log: {json.dumps(body, indent=4, sort_keys=True)}", 
                    "info"
                )
                return message["params"]
            return None
        except (json.JSONDecodeError, KeyError):
            return None

    def get_browser_log_entries(self) -> list[dict[str, Union[str, dict]]]:
        """Retrieve and process all browser log entries.

        Returns
        -------
        list[dict[str, Union[str, dict]]]
            list of processed browser log entries
        """
        loglevels = {
            "NOTSET": 0, "DEBUG": 10, "INFO": 20,
            "WARNING": 30, "ERROR": 40, "SEVERE": 40, "CRITICAL": 50
        }
        browserlog = logging.getLogger("chrome")
        slurped_logs = self.web_driver.get_log("browser")

        for entry in slurped_logs:
            # Ensure entry has required fields
            entry.setdefault("source", "unknown")
            entry.setdefault("level", "INFO")
            entry.setdefault("timestamp", 0)
            entry.setdefault("message", "")
            
            rec = browserlog.makeRecord(
                f"{browserlog.name}.{entry['source']}",
                loglevels.get(entry["level"]),
                ".",
                0,
                entry["message"],
                None,
                None
            )
            rec.created = entry["timestamp"] / 1000
            with suppress(Exception):
                browserlog.handle(rec)

        return slurped_logs

    def process_browser_log_entry(self, entry: dict[str, Union[str, dict]]) -> dict[str, dict]:
        """Parse individual browser log entry.

        Parameters
        ----------
        entry : dict[str, Union[str, dict]]
            Raw browser log entry

        Returns
        -------
        dict[str, dict]
            Parsed log message content
        """
        try:
            log_data = json.loads(entry["message"])
            if isinstance(log_data, dict) and "message" in log_data:
                return log_data["message"]
            return log_data
        except (json.JSONDecodeError, KeyError):
            return {}

    def get_network_traffic(self) -> list[dict[str, Union[str, dict]]]:
        """Retrieve and filter network traffic logs.

        Returns
        -------
        list[dict[str, Union[str, dict]]]
            list of network response events
        """
        browser_log = self.web_driver.get_log("performance")
        list_events = [self.process_browser_log_entry(entry) for entry in browser_log]
        return [event for event in list_events if "Network.response" in event["method"]]

    def find_element(
        self,
        selector: Union[WebElement, WebDriver],
        str_element_interest: str,
        selector_type: Literal['XPATH', 'ID', 'NAME', 'TAG_NAME', 'CLASS_NAME'] = "XPATH"
    ) -> WebElement:
        """Find single web element using specified selector.

        Parameters
        ----------
        selector : Union[WebElement, WebDriver]
            Parent element or driver to search from
        str_element_interest : str
            Selector string to locate element
        selector_type : Literal['XPATH', 'ID', 'NAME', 'TAG_NAME', 'CLASS_NAME']
            Type of selector to use (default: "XPATH")

        Returns
        -------
        WebElement
            Located web element

        Raises
        ------
        ValueError
            If element cannot be found
        """
        try:
            return selector.find_element(getattr(By, selector_type), str_element_interest)
        except Exception as err:
            raise ValueError(f"Element not found: {str(err)}") from err

    def find_elements(
        self,
        selector: Union[WebElement, WebDriver],
        str_element_interest: str,
        selector_type: Literal['XPATH', 'ID', 'NAME', 'TAG_NAME', 'CLASS_NAME'] = "XPATH"
    ) -> list[WebElement]:
        """Find multiple web elements using specified selector.

        Parameters
        ----------
        selector : Union[WebElement, WebDriver]
            Parent element or driver to search from
        str_element_interest : str
            Selector string to locate elements
        selector_type : Literal['XPATH', 'ID', 'NAME', 'TAG_NAME', 'CLASS_NAME']
            Type of selector to use (default: "XPATH")

        Returns
        -------
        list[WebElement]
            list of located web elements
        """
        return selector.find_elements(getattr(By, selector_type), str_element_interest)

    def fill_input(self, web_element: WebElement, str_input: str) -> None:
        """Fill input field with specified text.

        Parameters
        ----------
        web_element : WebElement
            Target input element
        str_input : str
            Text to input
        """
        web_element.send_keys(str_input)

    def el_is_enabled(self, str_xpath: str) -> bool:
        """Check if element is present and enabled.

        Parameters
        ----------
        str_xpath : str
            XPath selector for target element

        Returns
        -------
        bool
            True if element is present and enabled, False otherwise
        """
        return ec.presence_of_element_located((By.XPATH, str_xpath))

    def wait_until_el_loaded(self, str_xpath: str) -> WebElement:
        """Wait until specified element is loaded.

        Parameters
        ----------
        str_xpath : str
            XPath selector for target element

        Returns
        -------
        WebElement
            Located web element

        Raises
        ------
        TimeoutException
            If element cannot be found
        """
        try:
            return WebDriverWait(self.web_driver, self.int_delay_seconds).until(
                ec.presence_of_element_located((By.XPATH, str_xpath)))
        except TimeoutException as e:
            raise TimeoutException(f"Timed out waiting for element {str_xpath}") from e

    def wait(self, seconds: int) -> None:
        """Pause execution for specified duration.

        Parameters
        ----------
        seconds : int
            Duration to wait in seconds
        """
        self._validate_wait_time(seconds)
        self.web_driver.implicitly_wait(seconds)