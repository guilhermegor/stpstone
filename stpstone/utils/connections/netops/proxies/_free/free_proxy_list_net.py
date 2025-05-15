from requests import request
from typing import Dict, Union, List, Optional
from logging import Logger
from stpstone.utils.connections.netops.proxies.abc import ABCSession
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.geography.ww import WWTimezones, WWGeography
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.str import StrHandler


class FreeProxyNet(ABCSession):

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
        self.html_parser = HtmlHandler()
        self.str_parser = StrHandler()

    @property
    def _available_proxies(self) -> List[Dict[str, Union[str, float]]]:
        list_ser = []
        payload = {}
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9,pt;q=0.8,es;q=0.7',
            'cache-control': 'max-age=0',
            'if-modified-since': 'Sun, 06 Apr 2025 10:02:02 GMT',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            'Cookie': '_ga=GA1.1.2121373567.1743934016; _ga_F5HK5559Z2=GS1.1.1743937290.2.1.1743937312.0.0.0'
        }
        resp_req = request("GET", "https://free-proxy-list.net/", headers=headers, data=payload,
                           timeout=10)
        resp_req.raise_for_status()
        html_root = self.html_parser.lxml_parser(resp_req)
        el_trs = self.html_parser.lxml_xpath(
            html_root, '//*[@id="list"]/div/div[2]/div/table/tbody/tr')
        for el_tr in el_trs:
            str_ip = self.html_parser.lxml_xpath(el_tr, './td[1]')[0].text
            str_port = self.html_parser.lxml_xpath(el_tr, './td[2]')[0].text
            str_country_code = self.html_parser.lxml_xpath(el_tr, './td[3]')[0].text
            str_country_name = self.html_parser.lxml_xpath(el_tr, './/td[@class="hm"][1]')[0].text
            str_anonymity = self.html_parser.lxml_xpath(el_tr, './td[5]')[0].text
            str_anonymity = "elite" if self.str_parser.match_string_like(str_anonymity, "elite*") \
                else (
                    "anonymous" if self.str_parser.match_string_like(str_anonymity, "anonymous*") \
                    else "transparent"
                )
            str_protocol_code = self.html_parser.lxml_xpath(el_tr, './/td[@class="hx"]')[0].text
            str_protocol = "https" if self.str_parser.match_string_like(str_protocol_code, "yes*") \
                else "http"
            str_last_checked = self.html_parser.lxml_xpath(el_tr, './/td[@class="hm"][3]')[0].text
            obj_timezone = self.ww_timezones.get_timezones_by_country_code(str_country_code) if \
                str_country_code is not None else None
            str_timezone = ", ".join(obj_timezone) if isinstance(obj_timezone, (list, tuple, set)) \
                else (str(obj_timezone) if obj_timezone is not None else "Unknown")
            str_continent = self.ww_geography.get_continent_by_country_code(str_country_code) if \
                str_country_code is not None else "Unknown"
            str_continent_code = self.ww_geography.get_continent_code_by_country_code(
                str_country_code) if str_country_code is not None else "Unknown"
            list_ser.append({
                "protocol": str_protocol,
                "bl_alive": True,
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
                "country_code": str_country_code if str_country_code is not None else "Unknown",
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
