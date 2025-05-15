import time
from typing import Union, Dict, List, Optional
from logging import Logger
from stpstone.utils.connections.netops.proxies._free.proxy_nova import ProxyNova
from stpstone.utils.connections.netops.proxies._free.proxy_scrape import ProxyScrapeAll, ProxyScrapeCountry
from stpstone.utils.connections.netops.proxies._free.proxy_webshare import ProxyWebShare
from stpstone.utils.connections.netops.proxies._free.freeproxy_world import FreeProxyWorld
from stpstone.utils.connections.netops.proxies._free.free_proxy_list_net import FreeProxyNet
from stpstone.utils.connections.netops.proxies._free.spysme import SpysMeCountries
from stpstone.utils.connections.netops.proxies._free.spysone import SpysOneCountry
from stpstone.utils.loggs.create_logs import CreateLog


class YieldFreeProxy:

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
        str_plan_id_webshare: str = "free",
        max_iter_find_healthy_proxy: int = 10,
        timeout_session: Optional[float] = 1000.0,
        int_wait_load_seconds: Optional[int] = 10,
    ) -> None:
        self.bl_new_proxy = bl_new_proxy
        self.dict_proxies = dict_proxies
        self.int_retries = int_retries
        self.int_backoff_factor = int_backoff_factor
        self.bl_alive = bl_alive
        self.list_anonymity_value = list_anonymity_value
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
        self.max_iter_find_healthy_proxy = max_iter_find_healthy_proxy
        self.timeout_session = timeout_session
        self.int_wait_load_seconds = int_wait_load_seconds
        self.create_logs = CreateLog()

        self.cls_spys_one_country = SpysOneCountry(
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

        self.cls_free_proxy_net = FreeProxyNet(
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

        self.cls_spysme_countries = SpysMeCountries(
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

        self.cls_proxy_nova = ProxyNova(
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

        self.cls_proxy_scrape_all = ProxyScrapeAll(
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

        self.cls_proxy_scrape_country = ProxyScrapeCountry(
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

        self.cls_proxy_webshare = ProxyWebShare(
            str_plan_id=str_plan_id_webshare,
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

        self.cls_freeproxy_world = FreeProxyWorld(
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
            logger=logger,
            int_wait_load_seconds=int_wait_load_seconds
        )

        self._retry_count = 0
        self.cached_sessions = self._cache
        self._last_cache_time = time.time()

    @property
    def _cache(self) -> List[Dict[str, str]]:
        list_ser = list()
        for list_ in [
            self.cls_spys_one_country.configured_sessions,
            self.cls_free_proxy_net.configured_sessions,
            self.cls_spysme_countries.configured_sessions,
            self.cls_freeproxy_world.configured_sessions,
            self.cls_proxy_nova.configured_sessions,
            self.cls_proxy_scrape_all.configured_sessions,
            self.cls_proxy_scrape_country.configured_sessions,
            self.cls_proxy_webshare.configured_sessions,
        ]:
            if list_ is not None:
                list_ser.extend(list_)
        self.create_logs.log_message(
            self.logger,
            f"Number of proxies healthy: {len(list_ser)}",
            log_level="info"
        )
        if (len(list_ser) == 0) and (self._retry_count < self.max_iter_find_healthy_proxy):
            self.create_logs.log_message(
                self.logger,
                f"*** No proxies available - retrying {self._retry_count+1}/{self.max_iter_find_healthy_proxy}",
                log_level="warning"
            )
            self._retry_count += 1
        if (len(list_ser) == 0) and (self._retry_count >= self.max_iter_find_healthy_proxy):
            self.create_logs.log_message(
                self.logger,
                f"*** No proxies available after {self.max_iter_find_healthy_proxy} attempts",
                log_level="error"
            )
            raise ValueError("No proxies available")
        if len(list_ser) > 0:
            self._last_cache_time = time.time()
            self._retry_count = 0
        return list_ser

    def __next__(self) -> Dict[str, str]:
        if self.bl_new_proxy == False: return None
        list_proxies = list()
        while (len(list_proxies) == 0) \
            and (time.time() - self._last_cache_time < self.timeout_session):
            list_proxies = self.cached_sessions
            if len(list_proxies) == 0: self.cached_sessions = self._cache
        if (len(list_proxies) == 0) \
            and (time.time() - self._last_cache_time > self.timeout_session):
            self.create_logs.log_message(
                self.logger,
                f"*** Timeout reached: {self.timeout_session} seconds / Retries: {self._retry_count}",
                log_level="critical"
            )
            raise ValueError("No proxies available")
        if len(list_proxies) == 0:
            self.create_logs.log_message(
                self.logger,
                f"*** No proxies available after {self.max_iter_find_healthy_proxy} attempts",
                log_level="critical"
            )
            raise ValueError("No proxies available")
        return list_proxies.pop(0)
