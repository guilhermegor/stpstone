from typing import Union, Dict, List, Optional
from logging import Logger
from stpstone.utils.webdriver_tools.selenium_wd import SeleniumWD
from stpstone.utils.connections.netops.proxies.abc import ABCSession
from stpstone.utils.geography.ww import WWTimezones, WWGeography


class ProxyNova(ABCSession):

    def __init__(
        self,
        bl_new_proxy: bool = True,
        dict_proxies: Union[Dict[str, str], None] = None,
        int_retries: int = 10,
        int_backoff_factor: int = 1,
        bl_alive: bool = True,
        list_anonymity_value: List[str] = ["anonymous", "elite"],
        list_protocol: str = 'http',
        str_continent_code: Union[str, None] = None,
        str_country_code: Union[str, None] = None,
        bl_ssl: Union[bool, None] = None,
        float_min_ratio_times_alive_dead: Optional[float] = 0.02,
        float_max_timeout: Optional[float] = 600,
        bl_use_timer: bool = False,
        list_status_forcelist: List[int] = [429, 500, 502, 503, 504],
        logger: Optional[Logger] = None
    ) -> None:
        super().__init__(
            bl_new_proxy=bl_new_proxy,
            dict_proxies=dict_proxies,
            int_retries=int_retries,
            int_backoff_factor=int_backoff_factor,
            bl_alive=bl_alive,
            list_anonymity_value=list_anonymity_value,
            list_protocol=list_protocol,
            str_continent_code=str_continent_code,
            str_country_code=str_country_code,
            bl_ssl=bl_ssl,
            float_min_ratio_times_alive_dead=float_min_ratio_times_alive_dead,
            float_max_timeout=float_max_timeout,
            bl_use_timer=bl_use_timer,
            list_status_forcelist=list_status_forcelist,
            logger=logger
        )
        self.fstr_url = "https://www.proxynova.com/proxy-server-list/country-{}/"
        self.xpath_tr = '//*[@id="tbl_proxy_list"]/tbody/tr'
        self.url = self.fstr_url.format(str_country_code.lower())
        self.selenium_wd = SeleniumWD(
            url=self.url,
            bl_headless=True
        )
        self.driver = self.selenium_wd.get_web_driver

    @property
    def _available_proxies(self) -> List[Dict[str, Union[str, float]]]:
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
            anonymity = self.selenium_wd.find_element(el_tr, './td[7]').text
            list_ser.append({
                "protocol": "http",
                "bl_alive": True,
                "status": "success",
                "alive_since": self.time_ago_to_ts_unix(last_checked),
                "anonymity": anonymity.lower(),
                "average_timeout": 1.0 / self.proxy_speed_to_float(proxy_speed),
                "first_seen": self.time_ago_to_ts_unix(last_checked),
                "ip_data": "",
                "ip_name": "",
                "timezone": ", ".join(WWTimezones().get_timezones_by_country_code(
                    self.str_country_code)),
                "continent": WWGeography().get_continent_by_country_code(self.str_country_code),
                "continent_code": WWGeography().get_continent_code_by_country_code(
                    self.str_country_code).upper(),
                "country": country,
                "country_code": self.str_country_code.upper(),
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
                "bl_ssl": True,
                "timeout": 1.0 / self.proxy_speed_to_float(proxy_speed),
                "times_alive": int_times_alive,
                "times_dead": "",
                "ratio_times_alive_dead": 1.0,
                "uptime": uptime
            })
        return list_ser
