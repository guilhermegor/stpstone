import pandas as pd
from stpstone.utils.parsers.html import SeleniumWD
from stpstone.utils.connections.netops.sessions.abc import ABCSession
from stpstone.utils.geography.ww import WWTimezones, WWGeography


class ProxyNova(ABCSession):

    def __init__(self, str_country_code: str) -> None:
        self.str_continent_code = str_country_code
        self.fstr_url = "https://www.proxynova.com/proxy-server-list/country-{}/"
        self.xpath_tr = '//*[@id="tbl_proxy_list"]/tbody/tr'
        self.url = self.fstr_url.format(str_country_code.lower())
        self.selenium_wd = SeleniumWD(
            url=self.url,
            bl_headless=True
        )
        self.driver = self.selenium_wd.get_web_driver

    @property
    def _available_proxies(self) -> pd.DataFrame:
        list_ser = list()
        el_trs = self.selenium_wd.find_elements(self.driver, self.xpath_tr)
        for el_tr in el_trs:
            ip = self.selenium_wd.find_element(el_tr, './td[1]').text
            port = self.selenium_wd.find_element(el_tr, './td[2]').text
            last_checked = self.selenium_wd.find_element(el_tr, './td[3]').text
            proxy_speed = self.selenium_wd.find_element(el_tr, './td[4]').text
            uptime_tested_times = str(self.selenium_wd.find_element(el_tr, './td[5]').text)
            uptime = float(uptime_tested_times.split("(")[0].replace("%", "")) / 100.0
            int_times_alive = int(uptime_tested_times.split("(")[1].replace(")", ""))
            country_city = self.selenium_wd.find_element(el_tr, './td[6]').text
            country = country_city.split(" - ")[0].strip()
            city = country_city.split("- ")[1].strip()
            anonimity = self.selenium_wd.find_element(el_tr, './td[7]').text
            list_ser.append({
                "protocol": "http",
                "bl_alive": True,
                "status": "success",
                "alive_since": self.mins_ago_to_timestamp(last_checked),
                "anonymity": anonimity,
                "average_timeout": 1.0 / self.proxy_speed_to_float(proxy_speed),
                "first_seen": self.mins_ago_to_timestamp(last_checked),
                "ip_data": "",
                "ip_name": "",
                "timezone": ", ".join(WWTimezones().get_timezones_by_country(self.str_continent_code)),
                "continent": WWGeography().get_continent_by_country_code(self.str_continent_code),
                "continent_code": WWGeography().get_continent_code_by_country_code(self.str_continent_code),
                "country": country,
                "country_code": self.str_continent_code,
                "city": city,
                "district": "",
                "region_name": "",
                "zip": "",
                "bl_hosting": True,
                "isp": "",
                "latitude": 0.0,
                "longitude": 0.0,
                "organization": "",
                "proxy": True,
                "ip": ip,
                "port": port,
                "bl_ssl": False,
                "timeout": 1.0 / self.proxy_speed_to_float(proxy_speed),
                "times_alive": int_times_alive,
                "times_dead": "",
                "ratio_times_alive_dead": "",
                "uptime": uptime
            })
        return pd.DataFrame(list_ser)
