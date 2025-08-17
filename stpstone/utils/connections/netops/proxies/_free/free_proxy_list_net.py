"""Proxy management class for free-proxy-list.net integration.

This module provides a class for fetching and managing proxy information from
free-proxy-list.net, including proxy validation and metadata enrichment.
"""

from logging import Logger
from typing import Optional, TypedDict

from requests import request

from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.abc import ABCSession
from stpstone.utils.geography.geo_ww import WWGeography, WWTimezones
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.str import StrHandler


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
    anonymity: str
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


class FreeProxyNet(ABCSession):
    """Proxy management class for free-proxy-list.net."""

    def __init__(
        self,
        bool_new_proxy: bool = True,
        dict_proxies: Optional[dict[str, str]] = None,
        int_retries: int = 10,
        int_backoff_factor: int = 1,
        bool_alive: bool = True,
        list_anonymity_value: list[str] = ["anonymous", "elite"],
        list_protocol: str = "http",
        str_continent_code: Optional[str] = None,
        str_country_code: Optional[str] = None,
        bool_ssl: Optional[bool] = None,
        float_min_ratio_times_alive_dead: Optional[float] = 0.02,
        float_max_timeout: Optional[float] = 600,
        bool_use_timer: bool = False,
        list_status_forcelist: list[int] = [429, 500, 502, 503, 504],
        logger: Optional[Logger] = None
    ) -> None:
        """Initialize FreeProxyNet instance.

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
        self.dates_br = DatesBR()
        self.ww_timezones = WWTimezones()
        self.ww_geography = WWGeography()
        self.html_parser = HtmlHandler()
        self.str_parser = StrHandler()

    def _validate_proxy_anonymity(self, anonymity: str) -> None:
        """Validate proxy anonymity level.

        Parameters
        ----------
        anonymity : str
            Anonymity level to validate

        Raises
        ------
        ValueError
            If anonymity level is invalid
        """
        valid_levels = {"anonymous", "elite", "transparent"}
        if anonymity.lower() not in valid_levels:
            raise ValueError(f"Invalid anonymity level: {anonymity}")

    def _available_proxies(self) -> list[ReturnAvailableProxies]:
        """Fetch and parse available proxies from free-proxy-list.net.

        Returns
        -------
        list[ReturnAvailableProxies]
            List of proxy dictionaries with metadata

        Raises
        ------
        ValueError
            If proxy data fetching or parsing fails
        """
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
                      "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9,pt;q=0.8,es;q=0.7",
            "cache-control": "max-age=0",
            "if-modified-since": "Sun, 06 Apr 2025 10:02:02 GMT",
            "priority": "u=0, i",
            "sec-ch-ua": '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "Cookie": "_ga=GA1.1.2121373567.1743934016; _ga_F5HK5559Z2=GS1.1.1743937290.2.1.1743937312.0.0.0"
        }

        try:
            resp_req = request(
                "GET",
                "https://free-proxy-list.net/",
                headers=headers,
                timeout=10
            )
            resp_req.raise_for_status()
            html_root = self.html_parser.lxml_parser(resp_req)
            el_trs = self.html_parser.lxml_xpath(
                html_root,
                '//*[@id="list"]/div/div[2]/div/table/tbody/tr'
            )

            list_ser = []
            for el_tr in el_trs:
                str_ip = self.html_parser.lxml_xpath(el_tr, "./td[1]")[0].text
                str_port = self.html_parser.lxml_xpath(el_tr, "./td[2]")[0].text
                str_country_code = self.html_parser.lxml_xpath(el_tr, "./td[3]")[0].text
                str_country_name = self.html_parser.lxml_xpath(el_tr, './/td[@class="hm"][1]')\
                    [0].text
                str_anonymity = self.html_parser.lxml_xpath(el_tr, "./td[5]")[0].text

                if self.str_parser.match_string_like(str_anonymity, "elite*"):
                    str_anonymity = "elite"
                elif self.str_parser.match_string_like(str_anonymity, "anonymous*"):
                    str_anonymity = "anonymous"
                else:
                    str_anonymity = "transparent"

                self._validate_proxy_anonymity(str_anonymity)

                str_protocol_code = self.html_parser.lxml_xpath(el_tr, './/td[@class="hx"]')\
                    [0].text
                str_protocol = "https" if self.str_parser.match_string_like(str_protocol_code, 
                                                                            "yes*") else "http"
                str_last_checked = self.html_parser.lxml_xpath(el_tr, './/td[@class="hm"][3]')\
                    [0].text

                obj_timezone = (
                    self.ww_timezones.get_timezones_by_country_code(str_country_code)
                    if str_country_code is not None
                    else None
                )
                str_timezone = (
                    ", ".join(obj_timezone)
                    if isinstance(obj_timezone, (list, tuple, set))
                    else str(obj_timezone) if obj_timezone is not None
                    else "Unknown"
                )

                str_continent = (
                    self.ww_geography.get_continent_by_country_code(str_country_code)
                    if str_country_code is not None
                    else "Unknown"
                )

                str_continent_code = (
                    self.ww_geography.get_continent_code_by_country_code(str_country_code)
                    if str_country_code is not None
                    else "Unknown"
                )

                proxy_data: ReturnAvailableProxies = {
                    "protocol": str_protocol,
                    "bool_alive": True,
                    "status": "success",
                    "alive_since": self.time_ago_to_ts_unix(str_last_checked),
                    "anonymity": str_anonymity.lower(),
                    "average_timeout": 1.0,
                    "first_seen": self.time_ago_to_ts_unix(str_last_checked),
                    "ip_data": "",
                    "ip_name": "",
                    "timezone": str_timezone,
                    "continent": str_continent,
                    "continent_code": str_continent_code,
                    "country": str_country_name,
                    "country_code": str_country_code if str_country_code is not None \
                        else "Unknown",
                    "city": "",
                    "district": "",
                    "region_name": "",
                    "zip": "",
                    "bool_hosting": False,
                    "isp": "",
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "organization": "",
                    "proxy": True,
                    "ip": str_ip,
                    "port": str_port,
                    "bool_ssl": True,
                    "timeout": 1.0,
                    "times_alive": 1,
                    "times_dead": 0,
                    "ratio_times_alive_dead": 1.0,
                    "uptime": 1.0
                }
                list_ser.append(proxy_data)

            return list_ser

        except Exception as err:
            raise ValueError(f"Failed to fetch or parse proxy data: {str(err)}") from err