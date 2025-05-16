import logging
import json
from typing import Optional, Union, List, Dict
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.remote.webelement import WebElement


class SeleniumWD:

    def __init__(
        self,
        url: str,
        path_webdriver: Optional[str] = None,
        int_port: Optional[int] = None,
        str_user_agent: Optional[str] = None,
        int_wait_load_seconds: int = 10,
        int_delay_seconds: int = 10,
        str_proxy: Optional[str] = None,
        bl_headless: bool = False,
        bl_incognito: bool = False,
        list_args: Optional[List[str]] = None
    ) -> None:
        """
        Initialization of selenium web driver

        Args:
            url (str): url to open
            path_webdriver (str, optional): path to webdriver. Defaults to None.
            int_port (int, optional): port to open. Defaults to None.
            str_user_agent (str, optional): user agent. Defaults to "Mozilla/5.0 (Windowns NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36".
            int_wait_load_seconds (int, optional): time to wait for page to load. Defaults to 10.
            int_delay_seconds (int, optional): time to wait between actions. Defaults to 10.
            str_proxy (str, optional): proxy to use. Defaults to None.
            bl_opn_min (bool, optional): open in minimal mode. Defaults to False.
            bl_headless (bool, optional): open in headless mode. Defaults to False.
            bl_incognito (bool, optional): open in incognito mode. Defaults to False.
            list_args (Optional[List[str]], optional): webdriver arguments. Defaults to None.

        Returns:
            None

        Notes:
            User agents: https://gist.github.com/pzb/b4b6f57144aea7827ae4
            Webdriver arguments: https://chromedriver.chromium.org/capabilities
            Chromium command line switches: https://gist.github.com/dodying/34ea4760a699b47825a766051f47d43b
        """
        self.url = url
        self.path_webdriver = path_webdriver
        self.int_port = int_port
        self.str_user_agent = str_user_agent if str_user_agent is not None \
            else "Mozilla/5.0 (Windowns NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36"
        self.int_wait_load_seconds = int_wait_load_seconds
        self.int_delay_seconds = int_delay_seconds
        self.bl_headless = bl_headless
        self.bl_incognito = bl_incognito
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
            f"--user-agent={str_user_agent}"
        ]
        # set headless mode for operations without graphical user interface (GUI) - if true
        if self.bl_headless == True:
            self.list_default_args.append("--headless=new")
        if self.bl_incognito == True:
            self.list_default_args.append("--incognito")
        if str_proxy is not None:
            self.list_default_args.append(f"--proxy-server={str_proxy}")
        self.web_driver = self.get_web_driver

    @property
    def get_web_driver(self) -> WebDriver:
        d = DesiredCapabilities.CHROME
        d["goog:loggingPrefs"] = {"performance": "ALL"}
        browser_options = webdriver.ChromeOptions()
        for arg in self.list_default_args:
            browser_options.add_argument(arg)
        if (self.path_webdriver is None) and (self.int_port is not None):
            service = Service(executable_path=ChromeDriverManager().install())
        else:
            service = Service(executable_path=self.path_webdriver)
        web_driver = webdriver.Chrome(service=service, options=browser_options)
        web_driver.get(self.url)
        web_driver.implicitly_wait(self.int_wait_load_seconds)
        return web_driver

    def process_log(self, log:Dict[str, Union[str, dict]]) -> Optional[Dict[str, Union[str, dict]]]:
        log = json.loads(log["message"])["message"]
        if ("Network.response" in log["method"] and "params" in log.keys()):
            body = self.web_driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": log[
                "params"]["requestId"]})
            print(json.dumps(body, indent=4, sort_keys=True))
            return log["params"]

    @property
    def get_browser_log_entries(self) -> List[Dict[str, Union[str, dict]]]:
        loglevels = {"NOTSET": 0, "DEBUG": 10, "INFO": 20,
                     "WARNING": 30, "ERROR": 40, "SEVERE": 40, "CRITICAL": 50}
        browserlog = logging.getLogger("chrome")
        slurped_logs = self.web_driver.get_log("web_driver")
        for entry in slurped_logs:
            # convert broswer log to python log format
            rec = browserlog.makeRecord("%s.%s" % (browserlog.name, entry["source"]), loglevels.get(
                entry["level"]), ".", 0, entry["message"], None, None)
            # log using original timestamp.. us -> ms
            rec.created = entry["timestamp"] / 1000
            try:
                # add web_driver log to python log
                browserlog.handle(rec)
            except:
                print(entry)
        return slurped_logs

    def process_browser_log_entry(self, entry: Dict[str, Union[str, dict]]) \
        -> Dict[str, Union[str, dict]]:
        return json.loads(entry["message"])["message"]

    @property
    def get_network_traffic(self) -> List[Dict[str, Union[str, dict]]]:
        browser_log = self.web_driver.get_log("performance")
        list_events = [self.process_browser_log_entry(entry) for entry in browser_log]
        list_events = [event for event in list_events if "Network.response" in event["method"]]
        return list_events

    def find_element(self, selector:Union[WebElement, WebDriver], str_element_interest:str,
                     selector_type:str="XPATH") -> WebElement:
        return selector.find_element(getattr(By, selector_type), str_element_interest)

    def find_elements(self, selector:Union[WebElement, WebDriver], str_element_interest:str,
                     selector_type:str="XPATH") -> WebElement:
        return selector.find_elements(getattr(By, selector_type), str_element_interest)

    def fill_input(self, web_element:WebElement, str_input:str) -> None:
        web_element.send_keys(str_input)

    def el_is_enabled(self, str_xpath:str) -> bool:
        return ec.presence_of_element_located((By.XPATH, str_xpath))

    def wait_until_el_loaded(self, str_xpath:str) -> WebDriverWait:
        return WebDriverWait(self.web_driver, self.int_delay_seconds).until(self.el_is_enabled(str_xpath))

    def wait(self, seconds:int) -> None:
        self.web_driver.implicitly_wait(seconds)
