import re
from typing import Union, Dict, List, Optional
from logging import Logger
from requests import request
from stpstone.utils.webdriver_tools.selenium_wd import SeleniumWD
from stpstone.utils.connections.netops.proxies.abc import ABCSession
from stpstone.utils.geography.ww import WWTimezones, WWGeography
from stpstone.utils.parsers.numbers import NumHandler
from stpstone.utils.parsers.str import StrHandler


class SpysOneCountry(ABCSession):

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
        logger: Optional[Logger] = None,
        int_wait_load_seconds: int = 10,
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
        self.int_wait_load_seconds = int_wait_load_seconds
        self.fstr_url = "https://spys.one/free-proxy-list/{}/"
        self.xpath_tr = '//tr[contains(@class, "spy1x")]'
        self.xpath_dd_anonimity = '//select[@name="xf1"]'
        self.xpath_dd_show = '//select[@name="xpp"]'
        self.xpath_ssl = '//select[@name="xf2"]'
        self.xpath_type = '//select[@name="xf5"]'
        self.ww_timezones = WWTimezones()
        self.ww_geography = WWGeography()
        self.num_handler = NumHandler()
        self.str_handler = StrHandler()

    def parse_location(self, location_str: str) -> Optional[Dict[str, str]]:
        """
        Parses location strings in the format:
        "BR São João da Ponte (Minas Gerais)" or "BR Birigui (São Paulo)"

        Args:
            location_str (str): The location string to parse.

        Returns:
            Optional[Dict[str, str]]: A dictionary containing country, city, and state.
        """
        if len(location_str) == 2: return location_str, "N/A", "N/A"
        elif self.str_handler.match_string_like(location_str, "*(*") == False:
            re_pattern = r'(?i)^([a-z]{2})\s*([^*]+?)$'
            re_match = re.match(re_pattern, location_str.strip())
            if re_match is not None:
                return re_match.group(1), re_match.group(2), "N/A"
            return None
        re_pattern = r'(?i)^([a-z]{2})\s*([^*]+?)\(([^*]+?)\)'
        re_match = re.match(re_pattern, location_str.strip())
        if re_match is not None:
            return re_match.group(1), re_match.group(2), re_match.group(3)
        return None

    @property
    def _available_proxies(self) -> List[Dict[str, Union[str, float]]]:
        list_ser = list()
        cls_selenium_wd = SeleniumWD(
            url=self.fstr_url.format(self.str_country_code.upper()),
            bl_headless=True,
            bl_incognito=True,
            int_wait_load_seconds=self.int_wait_load_seconds
        )
        try:
            web_driver = cls_selenium_wd.get_web_driver
            cls_selenium_wd.wait_until_el_loaded(self.xpath_tr)
            for el_, el_opt in [
                (self.xpath_dd_anonimity, './option[@value="1"]'),
                (self.xpath_dd_show, './/option[last()]'),
                (self.xpath_ssl, './/option[@value="1"]'),
                (self.xpath_type, './/option[@value="1"]'),
            ]:
                el_ = cls_selenium_wd.find_element(web_driver, self.xpath_dd_anonimity)
                el_opt = cls_selenium_wd.find_element(el_, './option[@value="1"]')
                el_opt.click()
                cls_selenium_wd.wait(10)
            el_trs = cls_selenium_wd.find_elements(web_driver, self.xpath_tr)
            for el_tr in el_trs:
                try:
                    tup_ip_port = cls_selenium_wd.find_element(el_tr, './td[1]').text.split(":")
                    str_ip = tup_ip_port[0]
                    str_port = tup_ip_port[1]
                except IndexError:
                    continue
                if self.num_handler.is_numeric(str_port) == False: continue
                str_protocol = cls_selenium_wd.find_element(el_tr, './td[2]').text.lower()
                str_protocol = str_protocol.split(" ")[0]
                str_anonimity = cls_selenium_wd.find_element(el_tr, './td[3]').text.lower()
                str_anonimity = "elite" if str_anonimity == "hia" else (
                    "anonymous" if str_anonimity == "anm" else "transparent"
                )
                str_country_city_region = cls_selenium_wd.find_element(el_tr, './td[4]').text
                str_country_code, str_city, str_region = self.parse_location(str_country_city_region)
                str_org_name = cls_selenium_wd.find_element(el_tr, './td[5]').text
                str_latency = cls_selenium_wd.find_element(el_tr, './td[6]').text
                float_latency = float(str_latency.replace(".", ""))
                str_uptime = cls_selenium_wd.find_element(el_tr, './td[8]').text
                str_check_date = cls_selenium_wd.find_element(el_tr, './td[9]').text
                ts_check_date = self.composed_time_ago_to_ts_unix(str_check_date)
                list_ser.append({
                    "protocol": str_protocol,
                    "bl_alive": True,
                    "status": "success",
                    "alive_since": ts_check_date,
                    "anonymity": str_anonimity,
                    "average_timeout": 1.0 / float_latency,
                    "first_seen": ts_check_date,
                    "ip_data": "",
                    "ip_name": "",
                    "timezone": ", ".join(self.ww_timezones.get_timezones_by_country_code(
                        self.str_country_code)),
                    "continent": self.ww_geography.get_continent_by_country_code(
                        self.str_country_code),
                    "continent_code": self.ww_geography.get_continent_code_by_country_code(
                        self.str_country_code),
                    "country": self.ww_geography.get_country_details(self.str_country_code)["name"],
                    "country_code": self.str_country_code,
                    "city": str_city,
                    "district": str_region,
                    "region_name": str_region,
                    "zip": "",
                    "bl_hosting": False,
                    "isp": "",
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "organization": str_org_name,
                    "proxy": f"{str_ip}:{str_port}",
                    "ip": str_ip,
                    "port": str_port,
                    "bl_ssl": True,
                    "timeout": 1.0 / float_latency,
                    "times_alive": 0,
                    "times_dead": 0,
                    "ratio_times_alive_dead": 1.0,
                    "uptime": str_uptime
                })
        finally:
            web_driver.quit()
        return list_ser
