from typing import Union, Dict, List, Optional
from logging import Logger
from requests import request
from stpstone.utils.connections.netops.proxies.abc import ABCSession
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.geography.ww import WWTimezones, WWGeography


class ProxyWebShare(ABCSession):

    def __init__(
        self,
        str_plan_id: str = "free",
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
        self.str_plan_id = str_plan_id
        self.fstr_url = "https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size=10&plan_id={}"
        self.dates_br = DatesBR()
        self.ww_timezones = WWTimezones()
        self.ww_geography = WWGeography()

    @property
    def _available_proxies(self) -> List[Dict[str, Union[str, float]]]:
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
        payload= {}
        resp_req = request("GET", self.fstr_url.format(self.str_plan_id), headers=headers,
                           data=payload)
        resp_req.raise_for_status()
        json_ = resp_req.json()
        return [
            {
                "protocol": "http",
                "bl_alive": bool(dict_["valid"]),
                "status": "success",
                "alive_since": self.dates_br.iso_to_unix_timestamp(dict_["last_verification"]),
                "anonymity": "elite" if bool(dict_["high_country_confidence"]) == True \
                    else "anonymous",
                "average_timeout": 1.0,
                "first_seen": self.dates_br.iso_to_unix_timestamp(dict_["created_at"]),
                "ip_data": "",
                "ip_name": dict_["asn_name"],
                "timezone": ", ".join(self.ww_timezones.get_timezones_by_country_code(
                    dict_["country_code"])),
                "continent": self.ww_geography.get_continent_by_country_code(dict_["country_code"]),
                "continent_code": self.ww_geography.get_continent_code_by_country_code(
                    dict_["country_code"]),
                "country": self.ww_geography.get_country_details(dict_["country_code"])["name"],
                "country_code": dict_["country_code"],
                "city": dict_["city_name"],
                "district": "",
                "region_name": "",
                "zip": "",
                "bl_hosting": False,
                "isp": "",
                "latitude": 0.0,
                "longitude": 0.0,
                "organization": dict_["asn_name"],
                "proxy": True,
                "ip": dict_["proxy_address"],
                "port": dict_["port"],
                "bl_ssl": True,
                "timeout": 1.0,
                "times_alive": 1,
                "times_dead": 0,
                "ratio_times_alive_dead": 1.0,
                "uptime": 1.0
            } for dict_ in json_["results"]
        ]
