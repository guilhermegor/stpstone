from typing import Union, Dict, List, Optional
from logging import Logger
from stpstone.utils.connections.netops.sessions.proxy_nova import ProxyNova
from stpstone.utils.connections.netops.sessions.proxy_scrape import ProxyScrapeAll, ProxyScrapeCountry
from stpstone.utils.connections.netops.sessions.proxy_webshare import ProxyWebShare
from stpstone.utils.parsers.lists import HandlingLists


class YieldProxy:

    def __init__(
        self,
        bl_new_proxy: bool = True,
        dict_proxies: Union[Dict[str, str], None] = None,
        int_retries: int = 10,
        int_backoff_factor: int = 1,
        bl_alive: bool = True,
        list_anonimity_value: List[str] = ["anonymous", "elite"],
        list_protocol: str = 'http',
        str_continent_code: Union[str, None] = None,
        str_country_code: Union[str, None] = None,
        bl_ssl: Union[bool, None] = None,
        float_min_ratio_times_alive_dead: Optional[float] = 0.02,
        float_max_timeout:Optional[float] = 600,
        bl_use_timer: bool = False,
        list_status_forcelist: list = [429, 500, 502, 503, 504],
        logger: Optional[Logger] = None,
        str_plan_id_webshare: str = "free",
    ) -> None:
        self.bl_new_proxy = bl_new_proxy
        self.dict_proxies = dict_proxies
        self.int_retries = int_retries
        self.int_backoff_factor = int_backoff_factor
        self.bl_alive = bl_alive
        self.list_anonimity_value = list_anonimity_value
        self.list_protocol = list_protocol
        self.str_continent_code = str_continent_code
        self.str_country_code = str_country_code
        self.bl_ssl = bl_ssl
        self.float_min_ratio_times_alive_dead = float_min_ratio_times_alive_dead
        self.float_max_timeout = float_max_timeout
        self.bl_use_timer = bl_use_timer
        self.list_status_forcelist = list_status_forcelist
        self.logger = logger
        self.str_plan_id_webshare = str_plan_id_webshare

        self.cls_proxy_nova = ProxyNova().__init__(
            bl_new_proxy=self.bl_new_proxy,
            dict_proxies=self.dict_proxies,
            int_retries=self.int_retries,
            int_backoff_factor=self.int_backoff_factor,
            bl_alive=self.bl_alive,
            list_anonimity_value=self.list_anonimity_value,
            list_protocol=self.list_protocol,
            str_continent_code=self.str_continent_code,
            str_country_code=self.str_country_code,
            bl_ssl=self.bl_ssl,
            float_min_ratio_times_alive_dead=self.float_min_ratio_times_alive_dead,
            float_max_timeout=self.float_max_timeout,
            bl_use_timer=self.bl_use_timer,
            list_status_forcelist=self.list_status_forcelist,
            logger=self.logger
        )

        self.cls_proxy_scrape_all = ProxyScrapeAll().__init__(
            bl_new_proxy=self.bl_new_proxy,
            dict_proxies=self.dict_proxies,
            int_retries=self.int_retries,
            int_backoff_factor=self.int_backoff_factor,
            bl_alive=self.bl_alive,
            list_anonimity_value=self.list_anonimity_value,
            list_protocol=self.list_protocol,
            str_continent_code=self.str_continent_code,
            str_country_code=self.str_country_code,
            bl_ssl=self.bl_ssl,
            float_min_ratio_times_alive_dead=self.float_min_ratio_times_alive_dead,
            float_max_timeout=self.float_max_timeout,
            bl_use_timer=self.bl_use_timer,
            list_status_forcelist=self.list_status_forcelist,
            logger=self.logger
        )

        self.cls_proxy_scrape_country = ProxyScrapeCountry().__init__(
            bl_new_proxy=self.bl_new_proxy,
            dict_proxies=self.dict_proxies,
            int_retries=self.int_retries,
            int_backoff_factor=self.int_backoff_factor,
            bl_alive=self.bl_alive,
            list_anonimity_value=self.list_anonimity_value,
            list_protocol=self.list_protocol,
            str_continent_code=self.str_continent_code,
            str_country_code=self.str_country_code,
            bl_ssl=self.bl_ssl,
            float_min_ratio_times_alive_dead=self.float_min_ratio_times_alive_dead,
            float_max_timeout=self.float_max_timeout,
            bl_use_timer=self.bl_use_timer,
            list_status_forcelist=self.list_status_forcelist,
            logger=self.logger
        )

        self.cls_proxy_webshare = ProxyWebShare().__init__(
            str_plan_id_webshare=self.str_plan_id_webshare,
            bl_new_proxy=self.bl_new_proxy,
            dict_proxies=self.dict_proxies,
            int_retries=self.int_retries,
            int_backoff_factor=self.int_backoff_factor,
            bl_alive=self.bl_alive,
            list_anonimity_value=self.list_anonimity_value,
            list_protocol=self.list_protocol,
            str_continent_code=self.str_continent_code,
            str_country_code=self.str_country_code,
            bl_ssl=self.bl_ssl,
            float_min_ratio_times_alive_dead=self.float_min_ratio_times_alive_dead,
            float_max_timeout=self.float_max_timeout,
            bl_use_timer=self.bl_use_timer,
            list_status_forcelist=self.list_status_forcelist,
            logger=self.logger
        )

        self.cache_sessions = self._cache

    @property
    def _cache(self) -> List[Dict[str, str]]:
        if self.bl_new_proxy == False: return None
        list_ser = list()
        for list_ in [
            self.cls_proxy_nova.configured_sessions,
            self.cls_proxy_scrape_all.configured_sessions,
            self.cls_proxy_scrape_country.configured_sessions,
            self.cls_proxy_webshare.configured_sessions
        ]:
            if list_ is not None:
                list_ser.extend(list_)
        return list_ser

    @property
    def __next__(self) -> Dict[str, str]:
        if self.bl_new_proxy == False: return None
        list_proxies = self.cache_sessions
        if len(list_proxies) == 0:
            self.cache_sessions = self._cache
        return list_proxies.pop(0)
