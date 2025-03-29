from requests.adapters import HTTPAdapter
from requests import Session, request
from requests.exceptions import ProxyError, ConnectTimeout, SSLError, ConnectionError
from urllib3.util import Retry
from typing import Dict, Union, Any, List, Tuple, Optional
from random import shuffle
from stpstone._config.global_slots import YAML_SESSION
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.connections.netops.sessions.abc import ABCSession
from stpstone.utils.loggs.create_logs import conditional_timeit


class ProxyUtils(ABCSession):

    @property
    def proxy_scrape_free(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        req_resp = request(
            YAML_SESSION['proxy_scrape']['method'],
            YAML_SESSION['proxy_scrape']['url'],
        )
        req_resp.raise_for_status()
        json_proxies = req_resp.json()
        return [
            {
                'protocol': str(dict_['protocol']).lower(),
                'bl_alive': bool(dict_['alive']),
                'status': str(dict_['ip_data']['status']) if 'ip_data' in dict_ else '',
                'alive_since': float(dict_['alive_since']),
                'anonymity': str(dict_['anonymity']).lower(),
                'average_timeout': float(dict_['average_timeout']),
                'first_seen': float(dict_['first_seen']),
                'ip_data': str(dict_['ip_data']['as']) if 'ip_data' in dict_ else '',
                'ip_name': str(dict_['ip_data']['asname']) if 'ip_data' in dict_ else '',
                'timezone': str(dict_['ip_data']['timezone']) if 'ip_data' in dict_ else '',
                'continent': str(dict_['ip_data']['continent']) if 'ip_data' in dict_ else '',
                'continent_code': str(dict_['ip_data']['continentCode']) if 'ip_data' in dict_ else '',
                'country': str(dict_['ip_data']['country']) if 'ip_data' in dict_ else '',
                'country_code': str(dict_['ip_data']['countryCode']) if 'ip_data' in dict_ else '',
                'city': str(dict_['ip_data']['city']) if 'ip_data' in dict_ else '',
                'district': str(dict_['ip_data']['district']) if 'ip_data' in dict_ else '',
                'region_name': str(dict_['ip_data']['regionName']) if 'ip_data' in dict_ else '',
                'zip': str(dict_['ip_data']['zip']) if 'ip_data' in dict_ else '',
                'bl_hosting': bool(dict_['ip_data']['hosting']) if 'ip_data' in dict_ else '',
                'isp': str(dict_['ip_data']['isp']) if 'ip_data' in dict_ else '',
                'latitude': float(dict_['ip_data']['lat']) if 'ip_data' in dict_ else '',
                'longitude': float(dict_['ip_data']['lon']) if 'ip_data' in dict_ else '',
                'organization': str(dict_['ip_data']['org']) if 'ip_data' in dict_ else '',
                'proxy': str(dict_['proxy']),
                'ip': str(dict_['ip']),
                'port': int(dict_['port']),
                'bl_ssl': bool(dict_['ssl']),
                'timeout': float(dict_['timeout']),
                'times_alive': float(dict_['times_alive']),
                'times_dead': float(dict_['times_dead']),
                'ratio_times_alive_dead': float(dict_['times_alive'] / dict_['times_dead'])
                    if 'times_alive' in dict_ and 'times_dead' in dict_ and dict_['times_dead'] != 0
                    else 0,
                'uptime': float(dict_['uptime'])
            } for dict_ in json_proxies['proxies']
        ]

    @property
    def _available_proxies(self):
        return self.proxy_scrape_free


class ProxyScrape(ProxyUtils):

    def __init__(self, bl_new_proxy:bool=True, dict_proxies:Union[Dict[str, str], None]=None,
                 int_retries:int=10, int_backoff_factor:int=1, bl_alive:bool=True,
                 list_anonimity_value:List[str]=['anonymous', 'elite', 'transparent'],
                 str_protocol:str='http', str_continent_code:Union[str, None]=None,
                 str_country_code:Union[str, None]=None, bl_ssl:Union[bool, None]=None,
                 float_min_ratio_times_alive_dead:Optional[float]=0.02,
                 float_max_timeout:Optional[float]=600, bl_use_timer:bool=False,
                 list_status_forcelist:list=[429, 500, 502, 503, 504]) -> None:
        """
        DOCSTRING: SESSION CONFIGURATION
        INPUTS:
            - URL:STR
            - PROXIES:DICT (NONE AS DEFAULT)
                . FORMAT: {'http': 'http://127.0.0.1:8080', 'https': 'http://127.0.0.1:8080'}
            - RETRIES:INT (10 AS DEFAULT)
            - BACKOFF_FACTOR:INT (1 AS DEFAULT)
            - STATUS_FORCELIST:LIST (429, 500, 502, 503, 504 AS DEFAULT)
        OUTPUTS: SESSION
        """
        self.bl_new_proxy = bl_new_proxy
        self.dict_proxies = dict_proxies
        self.int_retries = int_retries
        self.int_backoff_factor = int_backoff_factor
        self.bl_alive = bl_alive
        self.list_anonimity_value = list_anonimity_value
        self.str_protocol = str_protocol
        self.str_continent_code = str_continent_code
        self.str_country_code = str_country_code
        self.bl_ssl = bl_ssl
        self.float_min_ratio_times_alive_dead = float_min_ratio_times_alive_dead
        self.float_max_timeout = float_max_timeout
        self.bl_use_timer = bl_use_timer
        self.list_status_forcelist = list_status_forcelist

    @property
    def session(self):
        proxy = self.get_proxy if self.bl_new_proxy == True else None
        dict_proxy = self.dict_proxies if self.dict_proxies is not None else (
            self._dict_proxy(proxy['ip'], proxy['port'])
            if proxy is not None else None
        )
        return self._configure_session(dict_proxy, self.int_retries, self.int_backoff_factor)

    def _dict_proxy(self, str_ip:str, int_port:int) -> Dict[str, str]:
        return {
            'http': 'http://{}:{}'.format(str_ip, str(int_port)),
            'https': 'http://{}:{}'.format(str_ip, str(int_port))
        }

    def _configure_session(self, dict_proxy:Union[Dict[str, str], None]=None,
                          int_retries:int=10, int_backoff_factor:int=1) -> Session:
        retry_strategy = Retry(
            total=int_retries,
            backoff_factor=int_backoff_factor,
            status_forcelist=self.list_status_forcelist
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = Session()
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        if dict_proxy is not None:
            session.proxies.update(dict_proxy)
        return session

    def ip_infos(self, session:Session, bl_return_availability:bool=False,
                 tup_timeout:Tuple[int, int]=(5,5)) -> Union[List[Dict[str, Any]], None]:
        dict_payload = {}
        dict_headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9,pt;q=0.8,es;q=0.7',
            'cache-control': 'max-age=0',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
        }
        req_resp = session.get(YAML_SESSION['ipinfos']['url'], headers=dict_headers,
                                    data=dict_payload, timeout=tup_timeout)
        req_resp.raise_for_status()
        if bl_return_availability == True:
            return True
        else:
            return req_resp.json()

    def _test_proxy(self, str_ip:str, int_port:int, bl_return_availability:bool=True) -> bool:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        try:
            session = self._configure_session(
                dict_proxy={
                    'http': 'http://{}:{}'.format(str_ip, str(int_port)),
                    'https': 'http://{}:{}'.format(str_ip, str(int_port))
                },
                int_retries=0,
                int_backoff_factor=0
            )
            return self.ip_infos(session, bl_return_availability=bl_return_availability)
        except (ProxyError, ConnectTimeout, SSLError, ConnectionError):
            return False

    @property
    def _filter_proxies(self) -> List[Dict[str, Union[str, int]]]:
        list_ser = self._available_proxies
        # filtering proxies
        for k_filt, v_filt, str_strategy in [
            ('bl_alive', self.bl_alive, 'equal'),
            ('anonymity', self.list_anonimity_value, 'isin'),
            ('protocol', self.str_protocol, 'equal'),
            ('bl_ssl', self.bl_ssl, 'equal'),
            ('ratio_times_alive_dead', self.float_min_ratio_times_alive_dead, 'greater_than_or_equal_to'),
            ('timeout', self.float_max_timeout, 'less_than_or_equal_to'),
            ('continent_code', self.str_continent_code, 'equal'),
            ('country_code', self.str_country_code, 'equal')
        ]:
            if v_filt is not None:
                try:
                    list_ser = HandlingDicts().filter_list_ser(
                        list_ser,
                        k_filt,
                        v_filt,
                        str_filter_type=str_strategy
                    )
                except KeyError as e:
                    print(list_ser)
                    raise Exception(e)
        return list_ser

    @property
    def get_proxy(self) -> Dict[str, str]:
        @conditional_timeit(bl_use_timer=self.bl_use_timer)
        def retrieve_proxy():
            list_ser = self._filter_proxies
            shuffle(list_ser)
            for dict_proxy in list_ser:
                str_ip = dict_proxy['ip']
                int_port = dict_proxy['port']
                if all([x is not None for x in [str_ip, int_port]]) == True:
                    if self._test_proxy(str_ip, int_port, bl_return_availability=True) == True:
                        return {'ip': str_ip, 'port': int_port}
            return None
        return retrieve_proxy()

    @property
    def get_proxies(self) -> List[Dict[str, str]]:
        list_ = list()
        @conditional_timeit(bl_use_timer=self.bl_use_timer)
        def retrieve_proxy():
            list_ser = self._filter_proxies
            shuffle(list_ser)
            for dict_proxy in list_ser:
                str_ip = dict_proxy['ip']
                int_port = dict_proxy['port']
                if all([x is not None for x in [str_ip, int_port]]) == True:
                    if self._test_proxy(str_ip, int_port, bl_return_availability=True) == True:
                        list_.append({'ip': str_ip, 'port': int_port})
            if len(list_) > 0:
                return list_
            else:
                return None
        return retrieve_proxy()
