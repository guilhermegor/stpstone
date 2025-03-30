import logging
import json
from requests import HTTPError, request, Response
from bs4 import BeautifulSoup
from lxml import html, etree
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
from stpstone.utils.connections.netops.user_agents import UserAgents


class HtmlHandler:

    def bs_parser(self, req_resp: Response, parser:str="html.parser") -> Union[BeautifulSoup, str]:
        try:
            return BeautifulSoup(req_resp.content, parser)
        except HTTPError as e:
            return "HTTP Error: {}".format(e)

    def lxml_parser(self, req_resp:Response) -> html.HtmlElement:
        page = req_resp.content
        return html.fromstring(page)

    def lxml_xpath(self, html_content, str_xpath):
        return html_content.xpath(str_xpath)

    def html_tree(self, html_root:html.HtmlElement, file_path:str=None) -> None:
        html_string = etree.tostring(html_root, pretty_print=True, encoding="unicode")
        if file_path:
            with open(file_path, "w", encoding="utf-8") as file:
                file.write(html_string)
        else:
            print(html_string)

    def to_txt(self, html_):
        soup = BeautifulSoup(html_, features="lxml")
        return soup(html_)

    def parse_html_to_string(self, html_, parsing_lib="html.parser",
                             str_body_html="",
                             join_td_character="|", td_size_ajust_character=" "):
        # setting variables
        list_ = list()
        list_tr_html = list()
        dict_ = dict()
        dict_fill_blanks_td = dict()
        # creating a parseable object
        obj_soup = BeautifulSoup(html_, parsing_lib)
        html_parsed_raw = obj_soup.get_text()
        # creating a raw parsed html body
        list_body_html = html_parsed_raw.split("\n")
        # looping through tables and periods in the raw parsed html body
        for str_ in list_body_html:
            #   append to tr, provided the value is different from empty, what is an indicative of
            #       line scape
            if str_ != "":
                list_.append(str_)
            else:
                if len(list_) > 0:
                    list_tr_html.append(list_)
                list_ = list()
            #   add tr to the list, without reseting the intermediary list, provided it is the
            #       last instance of list body html
            if (str_ == list_body_html[-1]) and (len(list_) > 0):
                list_tr_html.append(list_)
        # looping through each tr to find the maximum td length
        for i in range(len(list_tr_html)):
            #   if tr length is greater than 1 its a sign of a row from a table, otherwise its is
            #   considered a period from a phrase
            if len(list_tr_html[i]) > 1:
                dict_[i] = {j: len(list_tr_html[i][j])
                            for j in range(len(list_tr_html[i]))}
        # build dictionary with blank spaces, aiming to reach columns of same size
        for _, dict_j in dict_.items():
            for j, _ in dict_j.items():
                dict_fill_blanks_td[j] = max([dict_[i][j]
                                              for i in list(dict_.keys()) if i in dict_ and j in
                                              dict_[i]])
        # joining td"s with a separator
        for i in range(len(list_tr_html)):
            #   filling blanks to construct columns of the same size
            str_body_html += join_td_character.join([list_tr_html[i][j]
                                                     + td_size_ajust_character *
                                                     (dict_fill_blanks_td[j] -
                                                      len(list_tr_html[i][j]))
                                                     for j in range(len(list_tr_html[i]))])
            #   adding line scapes
            try:
                if len(list_tr_html[i]) == len(list_tr_html[i + 1]):
                    str_body_html += "\n"
                else:
                    str_body_html += 2 * "\n"
            except IndexError:
                continue
        # returning html body parsed
        return str_body_html


class SeleniumWD:

    def __init__(
        self,
        url: str,
        path_webdriver: Optional[str] = None,
        int_port: Optional[int] = None,
        str_user_agent: Optional[str] = None,
        int_wait_load: int = 10,
        int_delay: int = 10,
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
            int_wait_load (int, optional): time to wait for page to load. Defaults to 10.
            int_delay (int, optional): time to wait between actions. Defaults to 10.
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
            else UserAgents().get_random_user_agent
        self.int_wait_load = int_wait_load
        self.int_delay = int_delay
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
            f"--window-size=1920,1080",
            f"--window-position=0,0",
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
        web_driver.implicitly_wait(self.int_wait_load)
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
        return WebDriverWait(self.web_driver, self.int_delay).until(self.el_is_enabled(str_xpath))


class HtmlBuilder:

    def tag(self, name, *content, cls=None, **attrs):
        """
        REFERENCES: - FLUENT PYTHON BY LUCIANO RAMALHO (O’REILLY). COPYRIGHT 2015 LUCIANO RAMALHO, 978-1-491-94600-8.
        DOCSTRINGS: HTML TAG CONSTRUCTOR
        INPUTS: *ARGUMENTS, AND **ATTRIBUTES, BESIDE A CLS WORKAROUND SINCE CLASS IS A SPECIAL
            WORD FOR PYTHON
        OUTPUTS: STRING
        """
        # defining tag & method
        if cls is not None:
            attrs["class"] = cls
        if attrs:
            attr_str = ''.join(' {}="{}"'.format(attr, value) for attr, value
                               in sorted(attrs.items()))
        else:
            attr_str = ''
        # defining element
        if content:
            return '\n'.join('<{}{}>{}</{}>'.format(name, attr_str, c,
                                                    name) for c in content)
        else:
            return '<{}{} />'.format(name, attr_str)
