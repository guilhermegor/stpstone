from requests import request
from typing import Dict, Union, List, Optional
from stpstone._config.global_slots import YAML_SESSION
from stpstone.utils.connections.netops.sessions.abc import ABCSession


class ProxyScrape(ABCSession):

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
    def _available_proxies(self):
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
