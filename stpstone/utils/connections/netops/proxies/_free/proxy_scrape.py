"""Proxy management classes for interacting with ProxyScrape API.

This module provides classes for fetching and managing proxy information from
ProxyScrape's free proxy API, supporting both global and country-specific proxy lists.
"""

from logging import Logger
from typing import Optional

from requests import request

from stpstone.utils.connections.netops.proxies.proxies_abc import (
    ABCSession,
    ReturnAvailableProxies,
)


class ProxyScrapeAll(ABCSession):
    """Class for fetching all available proxies from ProxyScrape."""

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

    def _available_proxies(self) -> list[ReturnAvailableProxies]:
        """Fetch and format all available proxies from ProxyScrape API.

        Returns
        -------
        list[ReturnAvailableProxies]
            List of proxy dictionaries with detailed information

        Raises
        ------
        ValueError
            If API request fails or returns invalid data
        """
        try:
            resp_req = request(
                "GET",
                "https://api.proxyscrape.com/v4/free-proxy-list/get"
                "?request=display_proxies&proxy_format=protocolipport&format=json",
            )
            resp_req.raise_for_status()
            json_proxies = resp_req.json()

            proxies = []
            for dict_ in json_proxies["proxies"]:
                proxy_data: ReturnAvailableProxies = {
                    "protocol": str(dict_.get("protocol", "")).lower(),
                    "bool_alive": bool(dict_.get("alive", False)),
                    "status": str(dict_.get("ip_data", {}).get("status", "")),
                    "alive_since": float(dict_.get("alive_since", 0)),
                    "anonymity": str(dict_.get("anonymity", "")).lower(),
                    "average_timeout": float(dict_.get("average_timeout", 0)),
                    "first_seen": float(dict_.get("first_seen", 0)),
                    "ip_data": str(dict_.get("ip_data", {}).get("as", "")),
                    "ip_name": str(dict_.get("ip_data", {}).get("asname", "")),
                    "timezone": str(dict_.get("ip_data", {}).get("timezone", "")),
                    "continent": str(dict_.get("ip_data", {}).get("continent", "")),
                    "continent_code": str(dict_.get("ip_data", {}).get("continentCode", "")),
                    "country": str(dict_.get("ip_data", {}).get("country", "")),
                    "country_code": str(dict_.get("ip_data", {}).get("countryCode", "")),
                    "city": str(dict_.get("ip_data", {}).get("city", "")),
                    "district": str(dict_.get("ip_data", {}).get("district", "")),
                    "region_name": str(dict_.get("ip_data", {}).get("regionName", "")),
                    "zip": str(dict_.get("ip_data", {}).get("zip", "")),
                    "bool_hosting": bool(dict_.get("ip_data", {}).get("hosting", False)),
                    "isp": str(dict_.get("ip_data", {}).get("isp", "")),
                    "latitude": float(dict_.get("ip_data", {}).get("lat", 0)),
                    "longitude": float(dict_.get("ip_data", {}).get("lon", 0)),
                    "organization": str(dict_.get("ip_data", {}).get("org", "")),
                    "proxy": str(dict_.get("proxy", "")),
                    "ip": str(dict_.get("ip", "")),
                    "port": int(dict_.get("port", 0)),
                    "bool_ssl": bool(dict_.get("ssl", False)),
                    "timeout": float(dict_.get("timeout", 0)),
                    "times_alive": float(dict_.get("times_alive", 0)),
                    "times_dead": float(dict_.get("times_dead", 0)),
                    "uptime": float(dict_.get("uptime", 0))
                }

                times_alive = float(dict_.get("times_alive", 0))
                times_dead = float(dict_.get("times_dead", 0))
                proxy_data["ratio_times_alive_dead"] = (
                    times_alive / times_dead if times_dead != 0 else 0
                )

                proxies.append(proxy_data)

            return proxies
        except Exception as err:
            raise ValueError(f"Failed to fetch or process proxies: {str(err)}") from err


class ProxyScrapeCountry(ABCSession):
    """Class for fetching country-specific proxies from ProxyScrape."""

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

    def _validate_country_code(self) -> None:
        """Validate country code before making API request.

        Raises
        ------
        ValueError
            If country code is not set or invalid
        """
        if not self.str_country_code:
            raise ValueError("Country code must be set")
        if not isinstance(self.str_country_code, str):
            raise ValueError("Country code must be a string")
        if len(self.str_country_code) != 2:
            raise ValueError("Country code must be 2 characters long")

    def _available_proxies(self) -> list[ReturnAvailableProxies]:
        """Fetch and format country-specific proxies from ProxyScrape API.

        Returns
        -------
        list[ReturnAvailableProxies]
            List of proxy dictionaries with detailed information

        Raises
        ------
        ValueError
            If API request fails or returns invalid data
        """
        self._validate_country_code()
        try:
            resp_req = request(
                "GET",
                f"https://api.proxyscrape.com/v4/free-proxy-list/get"
                f"?request=get_proxies&country={self.str_country_code.lower()}"
                f"&skip=0&proxy_format=protocolipport&format=json&limit=1000",
            )
            resp_req.raise_for_status()
            json_proxies = resp_req.json()

            proxies = []
            for dict_ in json_proxies["proxies"]:
                proxy_data: ReturnAvailableProxies = {
                    "protocol": str(dict_.get("protocol", "")).lower(),
                    "bool_alive": bool(dict_.get("alive", False)),
                    "status": str(dict_.get("ip_data", {}).get("status", "")),
                    "alive_since": float(dict_.get("alive_since", 0)),
                    "anonymity": str(dict_.get("anonymity", "")).lower(),
                    "average_timeout": float(dict_.get("average_timeout", 0)),
                    "first_seen": float(dict_.get("first_seen", 0)),
                    "ip_data": str(dict_.get("ip_data", {}).get("as", "")),
                    "ip_name": str(dict_.get("ip_data", {}).get("asname", "")),
                    "timezone": str(dict_.get("ip_data", {}).get("timezone", "")),
                    "continent": str(dict_.get("ip_data", {}).get("continent", "")),
                    "continent_code": str(dict_.get("ip_data", {}).get("continentCode", "")),
                    "country": str(dict_.get("ip_data", {}).get("country", "")),
                    "country_code": str(dict_.get("ip_data", {}).get("countryCode", "")),
                    "city": str(dict_.get("ip_data", {}).get("city", "")),
                    "district": str(dict_.get("ip_data", {}).get("district", "")),
                    "region_name": str(dict_.get("ip_data", {}).get("regionName", "")),
                    "zip": str(dict_.get("ip_data", {}).get("zip", "")),
                    "bool_hosting": bool(dict_.get("ip_data", {}).get("hosting", False)),
                    "isp": str(dict_.get("ip_data", {}).get("isp", "")),
                    "latitude": float(dict_.get("ip_data", {}).get("lat", 0)),
                    "longitude": float(dict_.get("ip_data", {}).get("lon", 0)),
                    "organization": str(dict_.get("ip_data", {}).get("org", "")),
                    "proxy": str(dict_.get("proxy", "")),
                    "ip": str(dict_.get("ip", "")),
                    "port": int(dict_.get("port", 0)),
                    "bool_ssl": bool(dict_.get("ssl", False)),
                    "timeout": float(dict_.get("timeout", 0)),
                    "times_alive": float(dict_.get("times_alive", 0)),
                    "times_dead": float(dict_.get("times_dead", 0)),
                    "uptime": float(dict_.get("uptime", 0))
                }

                times_alive = float(dict_.get("times_alive", 0))
                times_dead = float(dict_.get("times_dead", 0))
                proxy_data["ratio_times_alive_dead"] = (
                    times_alive / times_dead if times_dead != 0 else 0
                )

                proxies.append(proxy_data)

            return proxies
        except Exception as err:
            raise ValueError(f"Failed to fetch or process proxies: {str(err)}") from err