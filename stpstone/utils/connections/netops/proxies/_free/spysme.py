"""Proxy management class for interacting with Spys.me proxy service.

This module provides a class for fetching and managing proxy information from
Spys.me's proxy list service, supporting country-specific proxy filtering.
"""

from logging import Logger
import re
from typing import Optional

from requests import request

from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.connections.netops.proxies.proxies_abc import (
    ABCSession,
    ReturnAvailableProxies,
)
from stpstone.utils.geography.geo_ww import WWGeography, WWTimezones


class SpysMeCountries(ABCSession):
    """Class for fetching proxies from Spys.me service."""

    PROXY_REGEX = r"^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5})\s+([A-Z]{2})" \
                  r"(?:-([ANH])(?:[!+-]?)(?:\s*-\s*([S+]?))?)?.*$"

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
        list_status_forcelist: Optional[list[int]] = None,
        logger: Optional[Logger] = None
    ) -> None:
        """Initialize SpysMeCountries instance.

        Parameters
        ----------
        bool_new_proxy : bool
            Flag to use new proxy (default: True)
        dict_proxies : Optional[dict[str, str]]
            Existing proxies dictionary (default: None)
        int_retries : int
            Number of retries for requests (default: 10)
        int_backoff_factor : int
            Backoff factor for retries (default: 1)
        bool_alive : bool
            Flag to check proxy liveness (default: True)
        list_anonymity_value : Optional[list[str]]
            Allowed anonymity levels (default: ["anonymous", "elite"])
        list_protocol : str
            Proxy protocol (default: "http")
        str_continent_code : Optional[str]
            Continent code filter (default: None)
        str_country_code : Optional[str]
            Country code filter (default: None)
        bool_ssl : Optional[bool]
            SSL requirement flag (default: None)
        float_min_ratio_times_alive_dead : Optional[float]
            Minimum alive/dead ratio (default: 0.02)
        float_max_timeout : Optional[float]
            Maximum timeout threshold (default: 600)
        bool_use_timer : bool
            Flag to use timer (default: False)
        list_status_forcelist : Optional[list[int]]
            HTTP status codes to force retry (default: [429, 500, 502, 503, 504])
        logger : Optional[Logger]
            Logger instance (default: None)
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
        self.dates_br = DatesBRAnbima()
        self.ww_timezones = WWTimezones()
        self.ww_geography = WWGeography()

    def _get_request_headers(self) -> dict[str, str]:
        """Generate headers for Spys.me request.

        Returns
        -------
        dict[str, str]
            Dictionary of HTTP headers
        """
        return {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9,pt;q=0.8,es;q=0.7',
            'authorization': 'Token 1yzdyxhh7yg3dv5v17d5uxpsixz5xzfb9w7l64i1',
            'origin': 'https://dashboard.webshare.io',
            'priority': 'u=1, i',
            'referer': 'https://dashboard.webshare.io/',
            'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"),
            'Cookie': '_gcl_au=1.1.915571832.1743243656; _tid=6ec40fde-5dd2-4d9e-a5d5-aee93aae48fb; _ga=GA1.1.20341267.1743243657; _did=20341267.1743243657; _ir=no-referrer; _gcl_gs=2.1.k1$i1743495693$u101187463; _gcl_aw=GCL.1743495703.Cj0KCQjwna6_BhCbARIsALId2Z2IXgfZkTk_BdDu2ioMsUOLTrX8_M-gxCu9R0-38WABC4hnnQpQR4kaAiz0EALw_wcB; _sid=1743495703; ssotoken=1yzdyxhh7yg3dv5v17d5uxpsixz5xzfb9w7l64i1; newDesignLoginToken=1yzdyxhh7yg3dv5v17d5uxpsixz5xzfb9w7l64i1; intercom-session-zsppwl0f=NkM0aFo4Q0hzSWdtRllHQnJiUjlrbCtWSkxBTWJMaFlWRlVzWW1nUG5MZ0FZMnBhQ0ROWXZ1Nzk0UFkrZnZsSkNZZjVtWEd2dFdnbi9XclZlK3djR2d1UWxlaUlXZmdyUzZqb3dLeEJ5N1E9LS0wV3k5MXNSdXRJWXhsdC9EQkF4a0ZRPT0=--1187e227a07523f74ece39ca541dbf9d2c16d830; intercom-device-id-zsppwl0f=6eda0713-af9b-486f-9a17-321880576b16; _ga_Z1CFG0XGWL=GS1.1.1743495703.4.1.1743495796.28.1.1316803691; ph_phc_SgStpNtFchAMqb1IKSAIPiDKGdGrkEYEap1wqhRjcD8_posthog=%7B%22distinct_id%22%3A%226ec40fde-5dd2-4d9e-a5d5-aee93aae48fb%22%2C%22%24sesid%22%3A%5B1743495808069%2C%220195f071-28ba-7bbf-9736-107b86a56fc6%22%2C1743495702714%5D%2C%22%24epp%22%3Atrue%2C%22%24initial_person_info%22%3A%7B%22r%22%3A%22https%3A%2F%2Fwww.google.com%2F%22%2C%22u%22%3A%22https%3A%2F%2Fwww.webshare.io%2Facademy-article%2Fselenium-proxy%22%7D%7D; _tid=6ec40fde-5dd2-4d9e-a5d5-aee93aae48fb' # noqa E501: line too long
        }

    def _parse_proxy_line(self, proxy_match: re.Match) -> ReturnAvailableProxies:
        """Parse a single proxy line from Spys.me response.

        Parameters
        ----------
        proxy_match : re.Match
            Regex match object containing proxy data

        Returns
        -------
        ReturnAvailableProxies
            Dictionary with parsed proxy information
        """
        ip_port = proxy_match.group(1)
        country_code = proxy_match.group(2)
        anonymity_code = proxy_match.group(3) if proxy_match.group(3) else "N"
        ip, port = ip_port.split(":")

        anonymity = {
            "H": "elite",
            "A": "anonymous",
            "N": "transparent"
        }.get(anonymity_code, "transparent")

        timezones = self.ww_timezones.get_timezones_by_country_code(country_code)
        timezone_str = ", ".join(timezones) if isinstance(timezones, (list, tuple)) \
            else str(timezones)
        
        continent = self.ww_geography.get_continent_by_country_code(country_code) \
            or "Unknown"
        continent_code = self.ww_geography.get_continent_code_by_country_code(country_code) \
            or "Unknown"
        
        country_details = self.ww_geography.get_country_details(country_code) or {}
        country_name = country_details.get("name", "")

        return {
            "protocol": "https",
            "bool_alive": True,
            "status": "success",
            "alive_since": self.dates_br.datetime_to_unix_timestamp(self.dates_br.curr_time()),
            "anonymity": anonymity.lower(),
            "average_timeout": 1.0,
            "first_seen": self.dates_br.datetime_to_unix_timestamp(self.dates_br.curr_time()),
            "ip_data": "",
            "ip_name": "",
            "timezone": timezone_str,
            "continent": continent,
            "continent_code": continent_code,
            "country": country_name,
            "country_code": country_code,
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
            "ip": ip,
            "port": port,
            "bool_ssl": True,
            "timeout": 1.0,
            "times_alive": 1,
            "times_dead": 0,
            "ratio_times_alive_dead": 1.0,
            "uptime": 1.0
        }

    def _available_proxies(self) -> list[ReturnAvailableProxies]:
        """Fetch and parse available proxies from Spys.me.

        Returns
        -------
        list[ReturnAvailableProxies]
            List of proxy dictionaries with detailed information

        Raises
        ------
        ValueError
            If request fails or response parsing fails
        """
        try:
            headers = self._get_request_headers()
            resp = request("GET", "https://spys.me/proxy.txt", headers=headers, timeout=10)
            resp.raise_for_status()
            
            proxies = []
            for match in re.finditer(self.PROXY_REGEX, resp.text, re.MULTILINE):
                proxies.append(self._parse_proxy_line(match))
                
            return proxies
        except Exception as err:
            raise ValueError(f"Failed to fetch or parse proxies: {str(err)}") from err