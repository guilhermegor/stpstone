"""Proxy management class for freeproxy.world integration.

This module provides a class for fetching and managing proxy information from
freeproxy.world using Selenium for web scraping and proxy validation.
"""

from logging import Logger
from typing import Literal, Optional, TypedDict

from selenium.common.exceptions import TimeoutException

from stpstone.utils.connections.netops.proxies.abc import ABCSession
from stpstone.utils.geography.geo_ww import WWGeography, WWTimezones
from stpstone.utils.webdriver_tools.selenium_wd import SeleniumWD


class ReturnAvailableProxies(TypedDict):
    """Typed dictionary for proxy information.

    Attributes
    ----------
    protocol : str
        Proxy protocol (http/https)
    bool_alive : bool
        Whether proxy is currently alive
    status : str
        Proxy status
    alive_since : float
        Unix timestamp when proxy was last alive
    anonymity : str
        Proxy anonymity level
    average_timeout : float
        Average response timeout
    first_seen : float
        Unix timestamp when proxy was first seen
    ip_data : str
        IP metadata
    ip_name : str
        IP hostname
    timezone : str
        Proxy timezone
    continent : str
        Proxy continent name
    continent_code : str
        Proxy continent code
    country : str
        Proxy country name
    country_code : str
        Proxy country code
    city : str
        Proxy city
    district : str
        Proxy district
    region_name : str
        Proxy region name
    zip : str
        Proxy postal code
    bool_hosting : bool
        Whether proxy is hosting
    isp : str
        Internet Service Provider
    latitude : float
        Proxy latitude
    longitude : float
        Proxy longitude
    organization : str
        Proxy organization
    proxy : bool
        Whether is a proxy
    ip : str
        Proxy IP address
    port : str
        Proxy port
    bool_ssl : bool
        Whether SSL is supported
    timeout : float
        Current timeout
    times_alive : int
        Number of times proxy was alive
    times_dead : int
        Number of times proxy was dead
    ratio_times_alive_dead : float
        Alive/dead ratio
    uptime : float
        Proxy uptime percentage
    """

    protocol: str
    bool_alive: bool
    status: str
    alive_since: float
    anonymity: Literal["transparent", "anonymous", "elite"]
    average_timeout: float
    first_seen: float
    ip_data: str
    ip_name: str
    timezone: str
    continent: str
    continent_code: str
    country: str
    country_code: str
    city: str
    district: str
    region_name: str
    zip: str
    bool_hosting: bool
    isp: str
    latitude: float
    longitude: float
    organization: str
    proxy: bool
    ip: str
    port: str
    bool_ssl: bool
    timeout: float
    times_alive: int
    times_dead: int
    ratio_times_alive_dead: float
    uptime: float


class FreeProxyWorld(ABCSession):
    """Proxy management class for freeproxy.world."""

    def __init__(
        self,
        bool_new_proxy: bool = True,
        dict_proxies: Optional[dict[str, str]] = None,
        int_retries: int = 10,
        int_backoff_factor: int = 1,
        bool_alive: bool = True,
        list_anonymity_value: Optional[list[str]] = None,
        list_protocol: str = "http",
        str_continent_code: Optional[str] = None,
        str_country_code: Optional[str] = None,
        bool_ssl: Optional[bool] = None,
        float_min_ratio_times_alive_dead: Optional[float] = 0.02,
        float_max_timeout: Optional[float] = 600,
        bool_use_timer: bool = False,
        list_status_forcelist: list[int] = None,
        logger: Optional[Logger] = None,
        int_wait_load_seconds: int = 10,
    ) -> None:
        """Initialize FreeProxyWorld instance.

        Parameters
        ----------
        bool_new_proxy : bool
            Whether to fetch new proxies
        dict_proxies : Optional[dict[str, str]]
            Existing proxy dictionary
        int_retries : int
            Number of retry attempts
        int_backoff_factor : int
            Backoff factor for retries
        bool_alive : bool
            Whether to check proxy liveness
        list_anonymity_value : list[str]
            Allowed anonymity levels
        list_protocol : str
            Preferred protocol
        str_continent_code : Optional[str]
            Continent code filter
        str_country_code : Optional[str]
            Country code filter
        bool_ssl : Optional[bool]
            SSL requirement
        float_min_ratio_times_alive_dead : Optional[float]
            Minimum alive/dead ratio
        float_max_timeout : Optional[float]
            Maximum timeout threshold
        bool_use_timer : bool
            Whether to use timing
        list_status_forcelist : list[int]
            HTTP status codes to retry
        logger : Optional[Logger]
            Logger instance
        int_wait_load_seconds : int
            Seconds to wait for page load
        """
        super().__init__(
            bool_new_proxy=bool_new_proxy,
            dict_proxies=dict_proxies,
            int_retries=int_retries,
            int_backoff_factor=int_backoff_factor,
            bool_alive=bool_alive,
            list_anonymity_value=list_anonymity_value,
            list_protocol=list_protocol,
            str_continent_code=str_continent_code,
            str_country_code=str_country_code,
            bool_ssl=bool_ssl,
            float_min_ratio_times_alive_dead=float_min_ratio_times_alive_dead,
            float_max_timeout=float_max_timeout,
            bool_use_timer=bool_use_timer,
            list_status_forcelist=list_status_forcelist,
            logger=logger
        )
        self.int_wait_load_seconds = int_wait_load_seconds
        self.fstr_url = "https://www.freeproxy.world/?type=&anonymity=&country={}&speed=&port=&page={}"
        self.xpath_tr = '//table[@class="layui-table"]//tbody/tr'

    def _validate_country_code(self, country_code: Optional[str]) -> None:
        """Validate country code format.

        Parameters
        ----------
        country_code : Optional[str]
            Country code to validate

        Raises
        ------
        ValueError
            If country code is invalid
        """
        if country_code is not None and len(country_code) != 2:
            raise ValueError("Country code must be 2 characters")

    def _available_proxies(self) -> list[ReturnAvailableProxies]:
        """Fetch and parse available proxies from freeproxy.world.

        Returns
        -------
        list[ReturnAvailableProxies]
            List of proxy dictionaries with metadata

        Raises
        ------
        ValueError
            If proxy data fetching or parsing fails
        """
        self._validate_country_code(self.str_country_code)
        list_ser = []
        int_pg = 1

        while True:
            cls_selenium_wd = SeleniumWD(
                url=self.fstr_url.format(self.str_country_code.upper(), int_pg),
                bool_headless=True,
                bool_incognito=True,
                int_wait_load_seconds=self.int_wait_load_seconds
            )

            try:
                web_driver = cls_selenium_wd.get_web_driver
                try:
                    cls_selenium_wd.wait_until_el_loaded(self.xpath_tr)
                except TimeoutException as err:
                    self.create_log.log_message(
                        self.logger,
                        "TimeoutException - URL: "
                        + f"{self.fstr_url.format(self.str_country_code.upper(), int_pg)} "
                        + f"/ Error message: {err.msg}",
                        "warning"
                    )
                    break

                el_trs = cls_selenium_wd.find_elements(web_driver, self.xpath_tr)
                list_range = list(range(2, len(el_trs) + 2, 2))

                for i_tr in list_range:
                    ip = cls_selenium_wd.find_element(
                        web_driver,
                        f"{self.xpath_tr}[{i_tr}]/td[1]"
                    ).text
                    port = cls_selenium_wd.find_element(
                        web_driver,
                        f"{self.xpath_tr}[{i_tr}]/td[2]/a"
                    ).text
                    country = cls_selenium_wd.find_element(
                        web_driver,
                        f"{self.xpath_tr}[{i_tr}]/td[3]//span[@class='table-country']"
                    ).text
                    city = cls_selenium_wd.find_element(
                        web_driver,
                        f"{self.xpath_tr}[{i_tr}]/td[4]"
                    ).text
                    speed = cls_selenium_wd.find_element(
                        web_driver,
                        f"{self.xpath_tr}[{i_tr}]//div[@class='n-bar-wrapper']/p/a"
                    ).text
                    type_ = cls_selenium_wd.find_element(
                        web_driver,
                        f"{self.xpath_tr}[{i_tr}]/td[6]/a"
                    ).text
                    anonymity = cls_selenium_wd.find_element(
                        web_driver,
                        f"{self.xpath_tr}[{i_tr}]/td[7]/a"
                    ).text.lower()
                    anonymity = "elite" if anonymity == "high" else "transparent"
                    last_checked = cls_selenium_wd.find_element(
                        web_driver,
                        f"{self.xpath_tr}[{i_tr}]/td[8]"
                    ).text

                    proxy_data: ReturnAvailableProxies = {
                        "protocol": type_.lower(),
                        "bool_alive": True,
                        "status": "success",
                        "alive_since": self.composed_time_ago_to_ts_unix(last_checked),
                        "anonymity": anonymity.lower(),
                        "average_timeout": 1.0 / self.proxy_speed_to_float(speed),
                        "first_seen": self.composed_time_ago_to_ts_unix(last_checked),
                        "ip_data": "",
                        "ip_name": "",
                        "timezone": ", ".join(WWTimezones().get_timezones_by_country_code(
                            self.str_country_code)),
                        "continent": WWGeography().get_continent_by_country_code(
                            self.str_country_code),
                        "continent_code": WWGeography().get_continent_code_by_country_code(
                            self.str_country_code).upper(),
                        "country": country,
                        "country_code": self.str_country_code.upper(),
                        "city": city,
                        "district": "",
                        "region_name": "",
                        "zip": "",
                        "bool_hosting": True,
                        "isp": "",
                        "latitude": 0.0,
                        "longitude": 0.0,
                        "organization": "",
                        "proxy": True,
                        "ip": ip,
                        "port": port,
                        "bool_ssl": True,
                        "timeout": 1.0 / self.proxy_speed_to_float(speed),
                        "times_alive": 1,
                        "times_dead": 0,
                        "ratio_times_alive_dead": 1.0,
                        "uptime": 1.0
                    }
                    list_ser.append(proxy_data)

                int_pg += 1
                cls_selenium_wd.wait(60)

            except Exception as err:
                raise ValueError(f"Failed to fetch proxy data: {str(err)}") from err
            finally:
                web_driver.quit()

        return list_ser