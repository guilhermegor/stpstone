import time
from random import shuffle
from requests import Session
from requests.adapters import HTTPAdapter
from requests.exceptions import ProxyError, ConnectTimeout, SSLError, ConnectionError
from typing import List, Dict, Union, Tuple, Any, Optional
from abc import ABC, abstractmethod
from urllib3.util import Retry
from datetime import datetime, timedelta
from stpstone._config.global_slots import YAML_SESSION
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.loggs.create_logs import conditional_timeit


class ABCSession(ABC):

    def __init__(
        self,
        bl_new_proxy: bool = True,
        dict_proxies: Union[Dict[str, str], None] = None,
        int_retries: int = 10,
        int_backoff_factor: int = 1,
        bl_alive: bool = True,
        list_anonimity_value: List[str] = ['anonymous', 'elite'],
        str_protocol: str = 'http',
        str_continent_code: Union[str, None] = None,
        str_country_code: Union[str, None] = None,
        bl_ssl: Union[bool, None] = None,
        float_min_ratio_times_alive_dead: Optional[float] = 0.02,
        float_max_timeout:Optional[float] = 600,
        bl_use_timer: bool = False,
        list_status_forcelist: list = [429, 500, 502, 503, 504]
    ):
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
        self.set_required_keys_avl_proxies = {
            "protocol", "bl_alive", "status", "alive_since", "anonymity",
            "average_timeout", "first_seen", "ip_data", "ip_name", "timezone",
            "continent", "continent_code", "country", "country_code", "city",
            "district", "region_name", "zip", "bl_hosting", "isp", "latitude",
            "longitude", "organization", "proxy", "ip", "port", "bl_ssl",
            "timeout", "times_alive", "times_dead", "ratio_times_alive_dead",
            "uptime"
        }
        """
        Notes:
            Proxy levels:
                - Transparent: target server knows your IP address and it knows that you are connecting via a proxy server.
                - Anonymous: target server does not know your IP address, but it knows that you're using a proxy.
                - Elite or High anonymity: target server does not know your IP address, or that the request is relayed through a proxy server.
        """

    def _validate_proxy_structure(
        self,
        list_proxies: List[Dict[str, Union[str, int, float, bool]]]
    ) -> None:
        for proxy in list_proxies:
            missing_keys = self.set_required_keys_avl_proxies - proxy.keys()
            if missing_keys:
                raise ValueError(f"Missing required keys in proxy data: {missing_keys}")

    def time_ago_to_timestamp(self, time_ago_str: str) -> float:
        time_ = int(time_ago_str.split()[0])
        time_measure = time_ago_str.split()[1]
        if time_measure in ['min', 'mins', 'minute', 'minutes']:
            past_time = datetime.now() - timedelta(minutes=time_)
        elif time_measure in ['hour', 'hours']:
            past_time = datetime.now() - timedelta(hours=time_)
        elif time_measure in ['day', 'days']:
            past_time = datetime.now() - timedelta(days=time_)
        else:
            raise ValueError(f"Unknown time measure: {time_measure}")
        timestamp = time.mktime(past_time.timetuple()) + past_time.microsecond / 1e6
        return timestamp

    def proxy_speed_to_float(self, str_speed: str) -> float:
        int_speed = str_speed.split()[0]
        str_time_measure = str_speed.split()[-1]
        if str_time_measure == "ms":
            return float(int_speed) / 1000.0
        elif str_time_measure == "µs":
            return float(int_speed) / 1000000.0
        elif str_time_measure == 's':
            return float(int_speed)
        else:
            raise ValueError(f"Unknown time measure: {str_time_measure}")

    @property
    @abstractmethod
    def _available_proxies(self):
        pass

    def _test_proxy(self, str_ip:str, int_port:int, bl_return_availability:bool=True) -> bool:
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

    @property
    def _filter_proxies(self) -> List[Dict[str, Union[str, int]]]:
        list_ser = self._available_proxies
        self._validate_proxy_structure(list_ser)
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

    @property
    def session(self):
        proxy = self.get_proxy if self.bl_new_proxy == True else None
        dict_proxy = self.dict_proxies if self.dict_proxies is not None else (
            self._dict_proxy(proxy['ip'], proxy['port'])
            if proxy is not None else None
        )
        return self._configure_session(dict_proxy, self.int_retries, self.int_backoff_factor)
