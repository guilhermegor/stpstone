import pandas as pd
from typing import Dict, Union
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from stpstone.utils.parsers.html import SeleniumWD
from stpstone.utils.connections.netops.sessions.abc import ABCSession


class ProxyNova(ABCSession):

    def __init__(self, str_country_code: str) -> None:
        self.fstr_url = "https://www.proxynova.com/proxy-server-list/country-{}/"
        self.xpath_tr = '//*[@id="tbl_proxy_list"]/tbody/tr'
        self.url = self.fstr_url.format(str_country_code.lower())
        self.selenium_wd = SeleniumWD(
            url=self.url,
            bl_headless=True
        )
        self.driver = self.selenium_wd.get_web_driver

    @property
    def sessions(self) -> pd.DataFrame:
        list_ser = list()
        el_trs = self.selenium_wd.find_elements(self.driver, self.xpath_tr)
        for el_tr in el_trs:
            ip = self.selenium_wd.find_element(el_tr, './td[1]').text
            port = self.selenium_wd.find_element(el_tr, './td[2]').text
            last_checked = self.selenium_wd.find_element(el_tr, './td[3]').text
            proxy_speed = self.selenium_wd.find_element(el_tr, './td[4]').text
            uptime = self.selenium_wd.find_element(el_tr, './td[5]').text
            country = self.selenium_wd.find_element(el_tr, './td[6]').text
            anonymous = self.selenium_wd.find_element(el_tr, './td[7]').text
            list_ser.append({
                "ip": ip,
                "port": port,
                "last_checked": last_checked,
                "proxy_speed": proxy_speed,
                "uptime": uptime,
                "country": country,
                "anonymous": anonymous
            })
        return pd.DataFrame(list_ser)

    def configure_session(self, dict_proxy:Union[Dict[str, str], None]=None,
                          int_retries:int=10, int_backoff_factor:int=1) -> Session:
        """
        DOCSTRING: CONFIGURES AN HTTP SESSION WITH RETRY MECHANISM AND EXPONENTIAL BACKOFF
        INPUTS: NONE
        OUTPUTS: CONFIGURED HTTP SESSION OBJECT
        OBS:
            1. RETRY_STRATEGY OVERVIEW:
                - TOTAL: TOTAL NUMBER OF RETRIES
                - BACKOFF_FACTOR: EXPONENTIAL BACKOFF FACTOR
                    . CALCULATED AS THE DEALAY BEFORE THE NEXT RETRY
                    . DELAY = BACKOFF_FACTOR * (2 ** (RETRY_NUMBER - 1))
                    . AFTER THE 1ST RETRY: DELAY = 1 * 2**0 = 1 SECOND
                    . AFTER THE 2ND RETRY: DELAY = 1 * 2**1 = 4 SECONDS
                    . IN THE AFORE EXAMPLE THE BACKOFF FACTOR IS 1
                - STATUS_FORCELIST: LIST OF STATUS CODES TO RETRY
            2. SESSION OBJECT OVERVIEW:
                - MOUNT: MOUNTS THE RETRY STRATEGY TO THE SESSION, WITH THE GIVEN ADAPTER
                - SESSION OBJECTS HAVE METHODS AS .GET() AND .POST()
        """
        retry_strategy = Retry(
            total=int_retries,
            backoff_factor=int_backoff_factor,
            status_forcelist=self.list_status_forcelist
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = Session()
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        if dict_proxy is not None:
            session.proxies.update(dict_proxy)
        return session
