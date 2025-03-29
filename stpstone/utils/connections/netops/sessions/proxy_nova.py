import pandas as pd
from requests import request
from stpstone.utils.parsers.html import HtmlHandler, SeleniumWD
from stpstone.utils.parsers.folders import DirFilesManagement


class ProxyNova:

    def __init__(self, str_country_code: str) -> None:
        self.fstr_url = "https://www.proxynova.com/proxy-server-list/country-{}/"
        self.xpath_tr = '//*[@id="tbl_proxy_list"]/tbody/tr'
        self.url = self.fstr_url.format(str_country_code)
        self.selenium_wd = SeleniumWD(
            url=self.url,
            bl_headless=True
        )
        self.driver = self.selenium_wd.get_web_driver

    @property
    def proxies(self) -> pd.DataFrame:
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
