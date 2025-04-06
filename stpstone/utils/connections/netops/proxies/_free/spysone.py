from typing import Union, Dict, List, Optional
from logging import Logger
from requests import request
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.connections.netops.proxies.abc import ABCSession
from stpstone.utils.geography.ww import WWTimezones, WWGeography


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
        int_wait_load: int = 10,
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
        self.int_wait_load = int_wait_load
        self.fstr_url = "https://spys.one/free-proxy-list/{}/"
        self.xpath_tr = '//table[@class="layui-table"]//tbody/tr'
        self.html_handler = HtmlHandler()
        self.ww_timezones = WWTimezones()
        self.ww_geography = WWGeography()

    @property
    def _available_proxies(self) -> List[Dict[str, Union[str, float]]]:
        list_ser = list()
        payload = {}
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9,pt;q=0.8,es;q=0.7',
            'cache-control': 'max-age=0',
            'priority': 'u=0, i',
            'referer': 'https://www.google.com/',
            'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'cross-site',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            'Cookie': '_ga=GA1.1.1520361583.1743243658; FCNEC=%5B%5B%22AKsRol_WCzC8J05ZFnUHaYGVqCj8rnHRaRzRi2hyRRWaueYPs4Z9sJbRszvV0m596kyACUNdGnpLPJ1IHF8wx0SMiqQ3h6b30_Vrm2w1je79NlTQOYJ3XilyLM9HFlvCyw2zwqlXj-BuaW8Q5oWHa_8x4wnGltNz8Q%3D%3D%22%5D%5D; cf_clearance=hmcMuJoI0p4SOFKMPZwrkWlCIsJL6lKuNyiCveYsSRk-1743933785-1.2.1.1-naHHui9pCj8OVWovIj_BSX712IJMOwWQOPK8jyEIUJd6UxTB.UgUfdAymMbVXeL95aMnJOl4Uz95KRMgsTpQMGcjdSu_2loVfn738K2SuTsLcCEhnsS_Lvx4BWW7tCSCPd3tULJIBzCefxZGnREPjXAfWOGo8EF8.8ADKuqslGFOwWhZX4UcOgITvxLBhmsExsVw6dJfo937tIFTW1s6cd_IaRDuzibd_bj3TYLKaJKbMQ.7sRqP58RZFl4fnUNbXzU1KZkmo0mXAob23N3.T6RZCU3RZ5x2watUuudzSgjWkSgO7mIrf.X94dVxW7L2AboMMRVwpi9.2vnKEzOm9BbOoYcZWk7G2x.epGEuXR0; _ga_XWX5S73YKH=GS1.1.1743933755.6.1.1743933846.0.0.0'
        }
        req_resp = request("GET", self.fstr_url.format(self.str_country_code), headers=headers, data=payload)
        req_resp.raise_for_status()
        html_root = self.html_handler.lxml_parser(req_resp)
        el_trs = self.html_handler.lxml_xpath(
            html_root, """//tr[contains(@class, "spy1x") and contains(@onmouseover, "this.style.background='#002424'")]"""
        )
        for el_tr in el_trs:
            str_ip = self.html_handler.lxml_xpath(el_tr, "./td[1]/font/text()[1]")
            str_port = self.html_handler.lxml_xpath(el_tr, "./td[1]/font/text()[2]")
            print(f"IP: {str_ip}, Port: {str_port}")
            raise Exception("Debugging")
            # list_ser.append({
            #     "protocol": type_.lower(),
            #     "bl_alive": True,
            #     "status": "success",
            #     "alive_since": self.composed_time_ago_to_ts_unix(last_checked),
            #     "anonymity": anonymity.lower(),
            #     "average_timeout": 1.0 / self.proxy_speed_to_float(speed),
            #     "first_seen": self.composed_time_ago_to_ts_unix(last_checked),
            #     "ip_data": "",
            #     "ip_name": "",
            #     "timezone": ", ".join(WWTimezones().get_timezones_by_country_code(
            #         self.str_country_code)),
            #     "continent": WWGeography().get_continent_by_country_code(self.str_country_code),
            #     "continent_code": WWGeography().get_continent_code_by_country_code(
            #         self.str_country_code).upper(),
            #     "country": country,
            #     "country_code": self.str_country_code.upper(),
            #     "city": city,
            #     "district": "",
            #     "region_name": "",
            #     "zip": "",
            #     "bl_hosting": True,
            #     "isp": "",
            #     "latitude": 0.0,
            #     "longitude": 0.0,
            #     "organization": "",
            #     "proxy": True,
            #     "ip": ip,
            #     "port": port,
            #     "bl_ssl": True,
            #     "timeout": 1.0 / self.proxy_speed_to_float(speed),
            #     "times_alive": 1,
            #     "times_dead": "",
            #     "ratio_times_alive_dead": "",
            #     "uptime": 1.0
            # })
        return list_ser
