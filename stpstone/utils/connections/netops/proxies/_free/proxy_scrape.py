from requests import request
from typing import Dict, Union, List, Optional
from logging import Logger
from stpstone.utils.connections.netops.proxies.abc import ABCSession


class ProxyScrapeAll(ABCSession):

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

    @property
    def _available_proxies(self) -> List[Dict[str, Union[str, float]]]:
        resp_req = request(
            "GET",
            "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=json",
        )
        resp_req.raise_for_status()
        json_proxies = resp_req.json()
        return [
            {
                "protocol": str(dict_["protocol"]).lower(),
                "bl_alive": bool(dict_["alive"]),
                "status": str(dict_["ip_data"]["status"]) if "ip_data" in dict_ else "",
                "alive_since": float(dict_["alive_since"]),
                "anonymity": str(dict_["anonymity"]).lower(),
                "average_timeout": float(dict_["average_timeout"]),
                "first_seen": float(dict_["first_seen"]),
                "ip_data": str(dict_["ip_data"]["as"]) if "ip_data" in dict_ else "",
                "ip_name": str(dict_["ip_data"]["asname"]) if "ip_data" in dict_ else "",
                "timezone": str(dict_["ip_data"]["timezone"]) if "ip_data" in dict_ else "",
                "continent": str(dict_["ip_data"]["continent"]) if "ip_data" in dict_ else "",
                "continent_code": str(dict_["ip_data"]["continentCode"]) if "ip_data" in dict_ else "",
                "country": str(dict_["ip_data"]["country"]) if "ip_data" in dict_ else "",
                "country_code": str(dict_["ip_data"]["countryCode"]) if "ip_data" in dict_ else "",
                "city": str(dict_["ip_data"]["city"]) if "ip_data" in dict_ else "",
                "district": str(dict_["ip_data"]["district"]) if "ip_data" in dict_ else "",
                "region_name": str(dict_["ip_data"]["regionName"]) if "ip_data" in dict_ else "",
                "zip": str(dict_["ip_data"]["zip"]) if "ip_data" in dict_ else "",
                "bl_hosting": bool(dict_["ip_data"]["hosting"]) if "ip_data" in dict_ else "",
                "isp": str(dict_["ip_data"]["isp"]) if "ip_data" in dict_ else "",
                "latitude": float(dict_["ip_data"]["lat"]) if "ip_data" in dict_ else "",
                "longitude": float(dict_["ip_data"]["lon"]) if "ip_data" in dict_ else "",
                "organization": str(dict_["ip_data"]["org"]) if "ip_data" in dict_ else "",
                "proxy": str(dict_["proxy"]),
                "ip": str(dict_["ip"]),
                "port": int(dict_["port"]),
                "bl_ssl": bool(dict_["ssl"]),
                "timeout": float(dict_["timeout"]),
                "times_alive": float(dict_["times_alive"]),
                "times_dead": float(dict_["times_dead"]),
                "ratio_times_alive_dead": float(dict_["times_alive"] / dict_["times_dead"])
                    if "times_alive" in dict_ and "times_dead" in dict_ and dict_["times_dead"] != 0
                    else 0,
                "uptime": float(dict_["uptime"])
            } for dict_ in json_proxies["proxies"]
        ]


class ProxyScrapeCountry(ABCSession):

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

    @property
    def _available_proxies(self) -> List[Dict[str, Union[str, float]]]:
        resp_req = request(
            "GET",
            f"https://api.proxyscrape.com/v4/free-proxy-list/get?request=get_proxies&country={self.str_country_code.lower()}&skip=0&proxy_format=protocolipport&format=json&limit=1000",
        )
        resp_req.raise_for_status()
        json_proxies = resp_req.json()
        return [
            {
                "protocol": str(dict_["protocol"]).lower(),
                "bl_alive": bool(dict_["alive"]),
                "status": str(dict_["ip_data"]["status"]) if "ip_data" in dict_ else "",
                "alive_since": float(dict_["alive_since"]),
                "anonymity": str(dict_["anonymity"]).lower(),
                "average_timeout": float(dict_["average_timeout"]),
                "first_seen": float(dict_["first_seen"]),
                "ip_data": str(dict_["ip_data"]["as"]) if "ip_data" in dict_ else "",
                "ip_name": str(dict_["ip_data"]["asname"]) if "ip_data" in dict_ else "",
                "timezone": str(dict_["ip_data"]["timezone"]) if "ip_data" in dict_ else "",
                "continent": str(dict_["ip_data"]["continent"]) if "ip_data" in dict_ else "",
                "continent_code": str(dict_["ip_data"]["continentCode"]) if "ip_data" in dict_ else "",
                "country": str(dict_["ip_data"]["country"]) if "ip_data" in dict_ else "",
                "country_code": str(dict_["ip_data"]["countryCode"]) if "ip_data" in dict_ else "",
                "city": str(dict_["ip_data"]["city"]) if "ip_data" in dict_ else "",
                "district": str(dict_["ip_data"]["district"]) if "ip_data" in dict_ else "",
                "region_name": str(dict_["ip_data"]["regionName"]) if "ip_data" in dict_ else "",
                "zip": str(dict_["ip_data"]["zip"]) if "ip_data" in dict_ else "",
                "bl_hosting": bool(dict_["ip_data"]["hosting"]) if "ip_data" in dict_ else "",
                "isp": str(dict_["ip_data"]["isp"]) if "ip_data" in dict_ else "",
                "latitude": float(dict_["ip_data"]["lat"]) if "ip_data" in dict_ else "",
                "longitude": float(dict_["ip_data"]["lon"]) if "ip_data" in dict_ else "",
                "organization": str(dict_["ip_data"]["org"]) if "ip_data" in dict_ else "",
                "proxy": str(dict_["proxy"]),
                "ip": str(dict_["ip"]),
                "port": int(dict_["port"]),
                "bl_ssl": bool(dict_["ssl"]),
                "timeout": float(dict_["timeout"]),
                "times_alive": float(dict_["times_alive"]),
                "times_dead": float(dict_["times_dead"]),
                "ratio_times_alive_dead": float(dict_["times_alive"] / dict_["times_dead"])
                    if "times_alive" in dict_ and "times_dead" in dict_ and dict_["times_dead"] != 0
                    else 0,
                "uptime": float(dict_["uptime"])
            } for dict_ in json_proxies["proxies"]
        ]
