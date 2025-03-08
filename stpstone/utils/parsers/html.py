### HANDLING HTML TEXT ###

import logging
import json
from requests import HTTPError, request, Response
from bs4 import BeautifulSoup
from lxml import html, etree
from typing import Optional, Union, List, Dict
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.remote.webelement import WebElement


class HtmlHndler:

    def bs_parser(self, url:str, bl_verify:bool=True,
                       method:str='GET', parser:str='html.parser') -> Union[BeautifulSoup, str]:
        """
        DOCSTRING: HTML PARSER THROUGH BEAUTIFULSOUP
        INPUTS: HTML TEXT
        OUTPUTS: SOUP
        """
        try:
            html_status_invest = request(method, url, verify=bl_verify).content
            return BeautifulSoup(html_status_invest, parser)
        except HTTPError as e:
            return 'HTTP Error: {}'.format(e)

    def lxml_parser(self, req_resp:Response) -> html.HtmlElement:
        """
        DOCSTRING: HTML PARSER FOR LXML PURPOSES
        INPUTS: URL, METHOD (GET AS DEFAULT) AND BOOLEAN VERIFY (TRUE AS DEFAULT)
        OUTPUTS: DOCUMENT WITH HTML CONTENT
        """
        page = req_resp.content
        return html.fromstring(page)

    def lxml_xpath(self, html_content, str_xpath):
        """
        DOCSTRING: XPATH TO HANDLE LXML PARSER
        INPUTS: HTML CONTENT AND STRING XPATH
        OUTPUTS: XPATH CONTENT
        """
        return html_content.xpath(str_xpath)
    
    def html_tree(self, html_root:html.HtmlElement, file_path:str=None) -> None:
        html_string = etree.tostring(html_root, pretty_print=True, encoding='unicode')
        if file_path:
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(html_string)
        else:
            print(html_string)

    def to_txt(self, html_):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        soup = BeautifulSoup(html_, features='lxml')
        return soup(html_)

    def parse_html_to_string(self, html_, parsing_lib='html.parser',
                             str_body_html='',
                             join_td_character='|', td_size_ajust_character=' '):
        """
        DOCSTRING: PARSE HTML BODY
        INPUTS: HTML
        OUTPUTS: STRING
        """
        # setting variables
        list_ = list()
        list_tr_html = list()
        dict_ = dict()
        dict_fill_blanks_td = dict()
        # creating a parseable object
        obj_soup = BeautifulSoup(html_, parsing_lib)
        html_parsed_raw = obj_soup.get_text()
        # creating a raw parsed html body
        list_body_html = html_parsed_raw.split('\n')
        # looping through tables and periods in the raw parsed html body
        for str_ in list_body_html:
            #   append to tr, provided the value is different from empty, what is an indicative of
            #       line scape
            if str_ != '':
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
        # joining td's with a separator
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
                    str_body_html += '\n'
                else:
                    str_body_html += 2 * '\n'
            except IndexError:
                continue
        # returning html body parsed
        return str_body_html


class SeleniumWD:

    def __init__(self, url:str, path_webdriver:str, 
                 int_port:int, str_user_agent:str='Mozilla/5.0 (Windowns NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36', 
                 int_wait_load:int=10, 
                 int_delay:int=10,
                 str_proxy:str=None,
                 bl_opn_min:bool=False, 
                 bl_headless:bool=False,
                 bl_incognito:bool=False,
                 dict_args:Optional[List[str]]=None) -> None:
        """
        REFERENCES: 
            1. LIST OF CHROMIUM COMMAND LINE SWITCHES: https://gist.github.com/dodying/34ea4760a699b47825a766051f47d43b
            2. LIST OF USER AGENTS: https://gist.github.com/pzb/b4b6f57144aea7827ae4
        DOCSTRING: SELENIUM INTEGRATION
        INPUTS: 
            - URL:STR
            - PATH_WEBDRIVER:STR
            - PORT:INT
            - USER_AGENT:STR
            - WAIT_LOAD:INT (SECONDS)
            - DELAY:INT (SECONDS)
            - BL OPEN MINIMIZED:BOOLEAN (DEFAULT TRUE)
            - DICT ARGUMENTS:DICT (DEFAULT NONE)
        OUTPUTS: NONE
        """
        self.url = url
        self.path_webdriver = path_webdriver
        self.int_port = int_port
        self.str_user_agent = str_user_agent
        self.int_wait_load = int_wait_load
        self.int_delay = int_delay
        self.bl_opn_min = bl_opn_min
        self.bl_headless = bl_headless
        self.bl_incognito = bl_incognito
        self.dict_default_args = dict_args if dict_args is not None else [
            '--no-sandbox',
            '--disable-gpu',
            '--disable-setuid-sandbox',
            '--disable-web-security',
            '--disable-dev-shm-usage',
            '--memory-pressure-off',
            '--ignore-certificate-errors',
            '--disable-features=site-per-process',
            f'--user-agent={str_user_agent}'
        ]
        # set headless mode for operations without graphical user interface (GUI) - if true
        if self.bl_headless == True:
            self.dict_default_args.append('--headless')
        if self.bl_incognito == True:
            self.dict_default_args.append('--incognito')
        if str_proxy is not None:
            self.dict_default_args.append(f'--proxy-server={str_proxy}')
        self.web_driver = self.get_web_driver
    
    @property
    def get_web_driver(self) -> WebDriver:
        """
        DOCSTRING: BROWSER INITIATION WITH THE RESPECTIVE WEB DRIVER
        INPUTS: -
        OUTPUTS: WEB DRIVER
        """
        # setting preferences
        d = DesiredCapabilities.CHROME
        d['goog:loggingPrefs'] = {'performance': 'ALL'}
        # instantiate the web_driver command with passed args
        browser_options = webdriver.ChromeOptions()
        for arg in self.dict_default_args:
            browser_options.add_argument(arg)
        # instantiate the web_driver command
        service = Service(executable_path=self.path_webdriver)
        web_driver = webdriver.Chrome(service=service, options=browser_options)
        # open minimized
        if (self.bl_opn_min == True) \
            and (self.bl_headless == False):
            web_driver.minimize_window()
        # open the provided url
        web_driver.get(self.url)
        web_driver.implicitly_wait(self.int_wait_load)
        # return web_driver for webbrowser tests
        return web_driver

    def process_log(self, log:Dict[str, Union[str, dict]]) -> Optional[Dict[str, Union[str, dict]]]:
        """
        DOCSTRING: COLLECT NETWORK ACTIVITY
        INPUTS: DRIVER, LOG
        OUTPUTS: OBJECT
        """
        log = json.loads(log['message'])['message']
        if ('Network.response' in log['method'] and 'params' in log.keys()):
            body = self.web_driver.execute_cdp_cmd('Network.getResponseBody', {'requestId': log[
                'params']['requestId']})
            print(json.dumps(body, indent=4, sort_keys=True))
            return log['params']

    @property
    def get_browser_log_entries(self) -> List[Dict[str, Union[str, dict]]]:
        """
        REFERENCES: https://stackoverflow.com/questions/20907180/getting-console-log-output-from-chrome-with-selenium-python-api-bindings
        DOCSTRING: GET LOGGING SELENIUM RESPONSE FROM WEBDRIVER
        INPUTS: DRIVER
        OUPUTS: BROWSER LOG ENTRIES (LIST)
        """
        loglevels = {'NOTSET': 0, 'DEBUG': 10, 'INFO': 20,
                     'WARNING': 30, 'ERROR': 40, 'SEVERE': 40, 'CRITICAL': 50}
        # initialise a logger
        browserlog = logging.getLogger("chrome")
        # get web_driver logs
        slurped_logs = self.web_driver.get_log('web_driver')
        for entry in slurped_logs:
            # convert broswer log to python log format
            rec = browserlog.makeRecord("%s.%s" % (browserlog.name, entry['source']), loglevels.get(
                entry['level']), '.', 0, entry['message'], None, None)
            # log using original timestamp.. us -> ms
            rec.created = entry['timestamp'] / 1000
            try:
                # add web_driver log to python log
                browserlog.handle(rec)
            except:
                print(entry)
        # and return logs incase you want them
        return slurped_logs

    def process_browser_log_entry(self, entry: Dict[str, Union[str, dict]]) \
        -> Dict[str, Union[str, dict]]:
        """
        REFERENCES: https://stackoverflow.com/questions/52633697/selenium-python-how-to-capture-network-traffics-response
        INPUTS: ENTRY
        OUTPUTS: STRING
        """
        # process json from entry
        response = json.loads(entry['message'])['message']
        # return messge
        return response

    @property
    def get_network_traffic(self) -> List[Dict[str, Union[str, dict]]]:
        """
        REFERENCES: https://stackoverflow.com/questions/52633697/selenium-python-how-to-capture-network-traffics-response
        INPUTS:
        OUTPUTS:
        """
        # get web_driver log
        browser_log = self.web_driver.get_log('performance')
        # getting events
        list_events = [self.process_browser_log_entry(
            entry) for entry in browser_log]
        list_events = [
            event for event in list_events if 'Network.response' in event['method']]
        # returning data
        return list_events

    def find_element(self, selector:Union[WebElement, WebDriver], str_element_interest:str, 
                     selector_type:str='XPATH') -> WebElement:
        """
        DOCSTRING: FINDING ELEMENT IN HTML BY SELECTOR TYPE
        INPUTS: WEB DRIVER (FROM SELENIUM, EITHER CHROME, SAFARI, FIREFOX, INTERNET EXPLORER WEB
            BROWSERS), STRING WITH THE ELEMENT OF INTEREST (IDENTIFIER TO SELECTOR),
            SELECTOR TYPE(CLAS_NAME, CSS_SELECTOR, ID, LINK_TEXT, NAME,
            PARTIAL_LINK_TEXT, TAG_NAME AND XPATH, THE FORMER AS DEFAULT)
        OUTPUTS: WEB DRIVER ELEMENT OF INTEREST
        """
        try:
            return selector.find_element(getattr(By, selector_type), str_element_interest)
        except AttributeError:
            raise Exception('Attribute, difined in selector type, not available in find element, ' +
                            'please consider revisiting these argument')

    def find_elements(self, str_xpath:str) -> List[WebElement]:
        """
        DOCSTRING: FINDING ELEMENTS IN HTML BY XPATH
        INPUTS: WEB DRIVER (FROM SELENIUM, EITHER CHROME, SAFARI, FIREFOX, INTERNET EXPLORER WEB
            BROWSERS) AND XPATH
        OUTPUTS: WEB DRIVER ELEMENT OF INTEREST
        """
        try:
            return self.web_driver.find_elements_by_xpath(str_xpath)
        except AttributeError:
            raise Exception('Attribute, difined in selector type, not available in find element, ' +
                            'please consider revisiting these argument')

    def fill_input(self, web_element:WebElement, str_input:str) -> str:
        """
        DOCSTRING: FILLING INPUT BOXES IN HTML
        INPUTS: WEB ELEMENT AND STRING TO INPUT
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        """
        try:
            web_element.send_keys(str_input)
            return 'OK'
        except:
            raise Exception(
                'Web element error, please consider revisiting this parameter')

    def el_is_enabled(self, str_xpath:str) -> bool:
        """
        REFERENCES: https://github.com/clemfromspace/scrapy-selenium
        DOCSTRING: CHECK WHETHER WEB ELEMENT IS ENABLED OR NOT
        INPUTS: STR XPATH
        OUTPUTS: BOOLEAN
        """
        return ec.element_to_be_clickable((By.XPATH, str_xpath))

    def wait_until_el_loaded(self, str_xpath:str) -> WebDriverWait:
        """
        REFERENCES: https://stackoverflow.com/questions/26566799/wait-until-page-is-loaded-with-selenium-webdriver-for-python
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
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
            attrs['class'] = cls
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
