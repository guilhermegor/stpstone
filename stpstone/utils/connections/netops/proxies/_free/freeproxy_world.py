from typing import Union, Dict, List, Optional
from logging import Logger
from selenium.common.exceptions import TimeoutException
from stpstone.utils.webdriver_tools.selenium_wd import SeleniumWD
from stpstone.utils.connections.netops.proxies.abc import ABCSession
from stpstone.utils.geography.ww import WWTimezones, WWGeography


class FreeProxyWorld(ABCSession):

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
        self.fstr_url = "https://www.freeproxy.world/?type=&anonymity=&country={}&speed=&port=&page={}"
        self.xpath_tr = '//table[@class="layui-table"]//tbody/tr'

    @property
    def _available_proxies(self) -> List[Dict[str, Union[str, float]]]:
        list_ser = list()
        int_pg = 1
        while True:
            cls_selenium_wd = SeleniumWD(
                url=self.fstr_url.format(self.str_country_code.upper(), int_pg),
                bl_headless=True,
                bl_incognito=True,
                int_wait_load_seconds=self.int_wait_load_seconds
            )
            try:
                web_driver = cls_selenium_wd.get_web_driver
                try:
                    cls_selenium_wd.wait_until_el_loaded(self.xpath_tr)
                except TimeoutException:
                    self.create_log.log_message(
                        self.logger,
                        f"TimeoutException - URL: {self.fstr_url.format(self.str_country_code.upper(), int_pg)}",
                        "warning"
                    )
                    break
                el_trs = cls_selenium_wd.find_elements(web_driver, self.xpath_tr)
                list_range = [i for i in range(2, len(el_trs) + 2, 2)]
                for i_tr in list_range:
                    ip = cls_selenium_wd.find_element(web_driver, self.xpath_tr \
                                                    + f'[{i_tr}]/td[1]').text
                    port = cls_selenium_wd.find_element(web_driver, self.xpath_tr \
                                                    + f'[{i_tr}]/td[2]/a').text
                    country = cls_selenium_wd.find_element(web_driver, self.xpath_tr \
                                                    + f'[{i_tr}]/td[3]//span[@class="table-country"]'
                                                    ).text
                    city = cls_selenium_wd.find_element(web_driver, self.xpath_tr \
                                                    + f'[{i_tr}]/td[4]').text
                    speed = cls_selenium_wd.find_element(web_driver, self.xpath_tr \
                                                    + f'[{i_tr}]//div[@class="n-bar-wrapper"]/p/a'
                                                    ).text
                    type_ = cls_selenium_wd.find_element(web_driver, self.xpath_tr \
                                                    + f'[{i_tr}]/td[6]/a').text
                    anonymity = cls_selenium_wd.find_element(web_driver, self.xpath_tr \
                                                    + f'[{i_tr}]/td[7]/a').text.lower()
                    anonymity = "elite" if anonymity == "high" else "transparent"
                    last_checked = cls_selenium_wd.find_element(web_driver, self.xpath_tr \
                                                    + f'[{i_tr}]/td[8]').text
                    list_ser.append({
                        "protocol": type_.lower(),
                        "bl_alive": True,
                        "status": "success",
                        "alive_since": self.composed_time_ago_to_ts_unix(last_checked),
                        "anonymity": anonymity.lower(),
                        "average_timeout": 1.0 / self.proxy_speed_to_float(speed),
                        "first_seen": self.composed_time_ago_to_ts_unix(last_checked),
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
                        "timeout": 1.0 / self.proxy_speed_to_float(speed),
                        "times_alive": 1,
                        "times_dead": "",
                        "ratio_times_alive_dead": 1.0,
                        "uptime": 1.0
                    })
                int_pg += 1
                cls_selenium_wd.wait(60)
            finally:
                web_driver.quit()
        return list_ser
