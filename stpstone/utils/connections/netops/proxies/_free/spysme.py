import re
from requests import request
from typing import Dict, Union, List, Optional
from logging import Logger
from stpstone.utils.connections.netops.proxies.abc import ABCSession
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.geography.ww import WWTimezones, WWGeography


class SpysMeCountries(ABCSession):

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
        self.dates_br = DatesBR()
        self.ww_timezones = WWTimezones()
        self.ww_geography = WWGeography()

    @property
    def _available_proxies(self) -> List[Dict[str, Union[str, float]]]:
        regex_pattern =  r"^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5})\s+([A-Z]{2})(?:-([ANH])(?:[!+-]?)(?:\s*-\s*([S+]?))?)?.*$"
        list_ser = []
        headers = {
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
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
            'Cookie': '_gcl_au=1.1.915571832.1743243656; _tid=6ec40fde-5dd2-4d9e-a5d5-aee93aae48fb; _ga=GA1.1.20341267.1743243657; _did=20341267.1743243657; _ir=no-referrer; _gcl_gs=2.1.k1$i1743495693$u101187463; _gcl_aw=GCL.1743495703.Cj0KCQjwna6_BhCbARIsALId2Z2IXgfZkTk_BdDu2ioMsUOLTrX8_M-gxCu9R0-38WABC4hnnQpQR4kaAiz0EALw_wcB; _sid=1743495703; ssotoken=1yzdyxhh7yg3dv5v17d5uxpsixz5xzfb9w7l64i1; newDesignLoginToken=1yzdyxhh7yg3dv5v17d5uxpsixz5xzfb9w7l64i1; intercom-session-zsppwl0f=NkM0aFo4Q0hzSWdtRllHQnJiUjlrbCtWSkxBTWJMaFlWRlVzWW1nUG5MZ0FZMnBhQ0ROWXZ1Nzk0UFkrZnZsSkNZZjVtWEd2dFdnbi9XclZlK3djR2d1UWxlaUlXZmdyUzZqb3dLeEJ5N1E9LS0wV3k5MXNSdXRJWXhsdC9EQkF4a0ZRPT0=--1187e227a07523f74ece39ca541dbf9d2c16d830; intercom-device-id-zsppwl0f=6eda0713-af9b-486f-9a17-321880576b16; _ga_Z1CFG0XGWL=GS1.1.1743495703.4.1.1743495796.28.1.1316803691; ph_phc_SgStpNtFchAMqb1IKSAIPiDKGdGrkEYEap1wqhRjcD8_posthog=%7B%22distinct_id%22%3A%226ec40fde-5dd2-4d9e-a5d5-aee93aae48fb%22%2C%22%24sesid%22%3A%5B1743495808069%2C%220195f071-28ba-7bbf-9736-107b86a56fc6%22%2C1743495702714%5D%2C%22%24epp%22%3Atrue%2C%22%24initial_person_info%22%3A%7B%22r%22%3A%22https%3A%2F%2Fwww.google.com%2F%22%2C%22u%22%3A%22https%3A%2F%2Fwww.webshare.io%2Facademy-article%2Fselenium-proxy%22%7D%7D; _tid=6ec40fde-5dd2-4d9e-a5d5-aee93aae48fb'
        }
        resp_req = request("GET", "https://spys.me/proxy.txt", headers=headers, timeout=10)
        resp_req.raise_for_status()
        str_proxies = resp_req.text
        list_proxies = re.finditer(regex_pattern, str_proxies, re.MULTILINE)
        for proxy in list_proxies:
            str_ip_port_porxy = proxy.group(1)
            str_country_code = proxy.group(2)
            str_anonymity_code = proxy.group(3) if proxy.group(3) else "N"
            str_ip = str_ip_port_porxy.split(":")[0]
            str_port = str_ip_port_porxy.split(":")[1]
            str_anonymity = "elite" if str_anonymity_code == "H" else (
                "anonymous" if str_anonymity_code == "A" else "transparent"
            )
            obj_timezone = self.ww_timezones.get_timezones_by_country_code(str_country_code)
            str_timezone = ", ".join(obj_timezone) if isinstance(obj_timezone, (list, tuple, set)) \
                else str(obj_timezone)
            str_continent = self.ww_geography.get_continent_by_country_code(str_country_code)
            str_continent = str_continent if str_continent is not None else "Unknown"
            str_continent_code = self.ww_geography.get_continent_code_by_country_code(
                    str_country_code)
            str_continent_code = str_continent_code if str_continent_code is not None else "Unknown"
            list_ser.append({
                "protocol": "https",
                "bl_alive": True,
                "status": "success",
                "alive_since": self.dates_br.datetime_to_unix_timestamp(self.dates_br.curr_time),
                "anonymity": str_anonymity.lower(),
                "average_timeout": 1.0,
                "first_seen": self.dates_br.datetime_to_unix_timestamp(self.dates_br.curr_time),
                "ip_data": "",
                "ip_name": "",
                "timezone": str_timezone,
                "continent": str_continent,
                "continent_code": str_continent_code,
                "country": self.ww_geography.get_country_details(str_country_code)["name"] \
                    if self.ww_geography.get_country_details(str_country_code) is not None else "",
                "country_code": str_country_code,
                "city": "",
                "district": "",
                "region_name": "",
                "zip": "",
                "bl_hosting": False,
                "isp": "",
                "latitude": 0.0,
                "longitude": 0.0,
                "organization": "",
                "proxy": True,
                "ip": str_ip,
                "port": str_port,
                "bl_ssl": True,
                "timeout": 1.0,
                "times_alive": 1,
                "times_dead": 0,
                "ratio_times_alive_dead": 1.0,
                "uptime": 1.0
            })
        return list_ser
