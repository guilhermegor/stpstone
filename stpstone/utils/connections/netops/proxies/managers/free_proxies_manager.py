"""Free proxy management utilities for fetching and caching proxy sessions.

This module provides a class for managing free proxy sessions from various sources,
with caching and retry mechanisms for reliable proxy acquisition and validation.
"""

from logging import Logger
import time
from typing import Optional, TypedDict

from requests import Session

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.connections.netops.proxies._free.free_proxy_list_net import FreeProxyNet
from stpstone.utils.connections.netops.proxies._free.freeproxy_world import FreeProxyWorld
from stpstone.utils.connections.netops.proxies._free.proxy_nova import ProxyNova
from stpstone.utils.connections.netops.proxies._free.proxy_scrape import (
    ProxyScrapeAll,
    ProxyScrapeCountry,
)
from stpstone.utils.connections.netops.proxies._free.proxy_webshare import ProxyWebShare
from stpstone.utils.connections.netops.proxies._free.spysme import SpysMeCountries
from stpstone.utils.connections.netops.proxies._free.spysone import SpysOneCountry
from stpstone.utils.loggs.create_logs import CreateLog


class ReturnNext(TypedDict):
    """Dictionary containing proxy session details.

    Parameters
    ----------
    session : Session
        Configured requests session
    """

    session: Session


class YieldFreeProxy(metaclass=TypeChecker):
    """Class for managing free proxy sessions with caching and retry logic.

    This class provides methods for fetching and validating free proxy sessions,
    with caching and retry mechanisms for reliable proxy acquisition and validation.
    """

    def __init__(
        self,
        bool_new_proxy: bool = True,
        dict_proxies: Optional[dict[str, str]] = None,
        int_retries: int = 10,
        int_backoff_factor: int = 1,
        bool_alive: bool = True,
        list_anonymity_value: Optional[list[str]] = None,
        list_protocol: str = "http",
        str_continent_code: Optional[str] = None,
        str_country_code: Optional[str] = None,
        bool_ssl: Optional[bool] = None,
        float_min_ratio_times_alive_dead: Optional[float] = 0.02,
        float_max_timeout: Optional[float] = 600,
        bool_use_timer: bool = False,
        list_status_forcelist: Optional[list[int]] = None,
        logger: Optional[Logger] = None,
        str_plan_id_webshare: str = "free",
        max_iter_find_healthy_proxy: int = 10,
        timeout_session: Optional[float] = 1000.0,
        int_wait_load_seconds: Optional[int] = 10,
    ) -> None:
        """Initialize YieldFreeProxy with configuration parameters.

        Parameters
        ----------
        bool_new_proxy : bool
            Whether to fetch a new proxy (default: True)
        dict_proxies : Optional[dict[str, str]]
            Dictionary of proxy settings (default: None)
        int_retries : int
            Number of retry attempts (default: 10)
        int_backoff_factor : int
            Backoff factor for retries (default: 1)
        bool_alive : bool
            Whether to filter for alive proxies (default: True)
        list_anonymity_value : Optional[list[str]]
            List of allowed anonymity levels (default: ['anonymous', 'elite'])
        list_protocol : str
            Proxy protocol (default: 'http')
        str_continent_code : Optional[str]
            Continent code filter (default: None)
        str_country_code : Optional[str]
            Country code filter (default: None)
        bool_ssl : Optional[bool]
            SSL support filter (default: None)
        float_min_ratio_times_alive_dead : Optional[float]
            Minimum alive/dead ratio (default: 0.02)
        float_max_timeout : Optional[float]
            Maximum timeout in seconds (default: 600)
        bool_use_timer : bool
            Whether to use timing decorator (default: False)
        list_status_forcelist : Optional[list[int]]
            HTTP status codes for retry (default: [429, 500, 502, 503, 504])
        logger : Optional[Logger]
            Logger instance (default: None)
        str_plan_id_webshare : str
            Webshare plan ID (default: 'free')
        max_iter_find_healthy_proxy : int
            Maximum iterations to find a healthy proxy (default: 10)
        timeout_session : Optional[float]
            Session timeout in seconds (default: 1000.0)
        int_wait_load_seconds : Optional[int]
            Wait time between proxy loads in seconds (default: 10)
        """
        self._validate_init_params(
            dict_proxies,
            int_retries,
            int_backoff_factor,
            list_anonymity_value,
            float_min_ratio_times_alive_dead,
            float_max_timeout,
            max_iter_find_healthy_proxy,
            timeout_session,
            int_wait_load_seconds,
            str_plan_id_webshare,
            list_status_forcelist,
            logger,
        )
        self.bool_new_proxy = bool_new_proxy
        self.dict_proxies = dict_proxies
        self.int_retries = int_retries
        self.int_backoff_factor = int_backoff_factor
        self.bool_alive = bool_alive
        self.list_anonymity_value = list_anonymity_value or ["anonymous", "elite"]
        self.list_protocol = list_protocol
        self.str_continent_code = str_continent_code
        self.str_country_code = str_country_code
        self.bool_ssl = bool_ssl
        self.float_min_ratio_times_alive_dead = float_min_ratio_times_alive_dead
        self.float_max_timeout = float_max_timeout
        self.bool_use_timer = bool_use_timer
        self.list_status_forcelist = list_status_forcelist or [429, 500, 502, 503, 504]
        self.logger = logger
        self.str_plan_id_webshare = str_plan_id_webshare
        self.max_iter_find_healthy_proxy = max_iter_find_healthy_proxy
        self.timeout_session = timeout_session
        self.int_wait_load_seconds = int_wait_load_seconds
        self.create_logs = CreateLog()
        self._retry_count = 0
        self._last_cache_time = time.time()
        self.cached_sessions: list[Session] = []
        self.cls_spys_one_country = SpysOneCountry(
            bool_new_proxy=bool_new_proxy,
            dict_proxies=dict_proxies,
            int_retries=int_retries,
            int_backoff_factor=int_backoff_factor,
            bool_alive=bool_alive,
            list_anonymity_value=list_anonymity_value,
            list_protocol=list_protocol,
            str_continent_code=str_continent_code,
            str_country_code=str_country_code,
            bool_ssl=bool_ssl,
            float_min_ratio_times_alive_dead=float_min_ratio_times_alive_dead,
            float_max_timeout=float_max_timeout,
            bool_use_timer=bool_use_timer,
            list_status_forcelist=list_status_forcelist,
            logger=logger,
        )
        self.cls_free_proxy_net = FreeProxyNet(
            bool_new_proxy=bool_new_proxy,
            dict_proxies=dict_proxies,
            int_retries=int_retries,
            int_backoff_factor=int_backoff_factor,
            bool_alive=bool_alive,
            list_anonymity_value=list_anonymity_value,
            list_protocol=list_protocol,
            str_continent_code=str_continent_code,
            str_country_code=str_country_code,
            bool_ssl=bool_ssl,
            float_min_ratio_times_alive_dead=float_min_ratio_times_alive_dead,
            float_max_timeout=float_max_timeout,
            bool_use_timer=bool_use_timer,
            list_status_forcelist=list_status_forcelist,
            logger=logger,
        )
        self.cls_spysme_countries = SpysMeCountries(
            bool_new_proxy=bool_new_proxy,
            dict_proxies=dict_proxies,
            int_retries=int_retries,
            int_backoff_factor=int_backoff_factor,
            bool_alive=bool_alive,
            list_anonymity_value=list_anonymity_value,
            list_protocol=list_protocol,
            str_continent_code=str_continent_code,
            str_country_code=str_country_code,
            bool_ssl=bool_ssl,
            float_min_ratio_times_alive_dead=float_min_ratio_times_alive_dead,
            float_max_timeout=float_max_timeout,
            bool_use_timer=bool_use_timer,
            list_status_forcelist=list_status_forcelist,
            logger=logger,
        )
        self.cls_freeproxy_world = FreeProxyWorld(
            bool_new_proxy=bool_new_proxy,
            dict_proxies=dict_proxies,
            int_retries=int_retries,
            int_backoff_factor=int_backoff_factor,
            bool_alive=bool_alive,
            list_anonymity_value=list_anonymity_value,
            list_protocol=list_protocol,
            str_continent_code=str_continent_code,
            str_country_code=str_country_code,
            bool_ssl=bool_ssl,
            float_min_ratio_times_alive_dead=float_min_ratio_times_alive_dead,
            float_max_timeout=float_max_timeout,
            bool_use_timer=bool_use_timer,
            list_status_forcelist=list_status_forcelist,
            logger=logger,
            int_wait_load_seconds=int_wait_load_seconds,
        )
        self.cls_proxy_nova = ProxyNova(
            bool_new_proxy=bool_new_proxy,
            dict_proxies=dict_proxies,
            int_retries=int_retries,
            int_backoff_factor=int_backoff_factor,
            bool_alive=bool_alive,
            list_anonymity_value=list_anonymity_value,
            list_protocol=list_protocol,
            str_continent_code=str_continent_code,
            str_country_code=str_country_code,
            bool_ssl=bool_ssl,
            float_min_ratio_times_alive_dead=float_min_ratio_times_alive_dead,
            float_max_timeout=float_max_timeout,
            bool_use_timer=bool_use_timer,
            list_status_forcelist=list_status_forcelist,
            logger=logger,
        )
        self.cls_proxy_scrape_all = ProxyScrapeAll(
            bool_new_proxy=bool_new_proxy,
            dict_proxies=dict_proxies,
            int_retries=int_retries,
            int_backoff_factor=int_backoff_factor,
            bool_alive=bool_alive,
            list_anonymity_value=list_anonymity_value,
            list_protocol=list_protocol,
            str_continent_code=str_continent_code,
            str_country_code=str_country_code,
            bool_ssl=bool_ssl,
            float_min_ratio_times_alive_dead=float_min_ratio_times_alive_dead,
            float_max_timeout=float_max_timeout,
            bool_use_timer=bool_use_timer,
            list_status_forcelist=list_status_forcelist,
            logger=logger,
        )
        self.cls_proxy_scrape_country = ProxyScrapeCountry(
            bool_new_proxy=bool_new_proxy,
            dict_proxies=dict_proxies,
            int_retries=int_retries,
            int_backoff_factor=int_backoff_factor,
            bool_alive=bool_alive,
            list_anonymity_value=list_anonymity_value,
            list_protocol=list_protocol,
            str_continent_code=str_continent_code,
            str_country_code=str_country_code,
            bool_ssl=bool_ssl,
            float_min_ratio_times_alive_dead=float_min_ratio_times_alive_dead,
            float_max_timeout=float_max_timeout,
            bool_use_timer=bool_use_timer,
            list_status_forcelist=list_status_forcelist,
            logger=logger,
        )
        self.cls_proxy_webshare = ProxyWebShare(
            str_plan_id=str_plan_id_webshare,
            bool_new_proxy=bool_new_proxy,
            dict_proxies=dict_proxies,
            int_retries=int_retries,
            int_backoff_factor=int_backoff_factor,
            bool_alive=bool_alive,
            list_anonymity_value=list_anonymity_value,
            list_protocol=list_protocol,
            str_continent_code=str_continent_code,
            str_country_code=str_country_code,
            bool_ssl=bool_ssl,
            float_min_ratio_times_alive_dead=float_min_ratio_times_alive_dead,
            float_max_timeout=float_max_timeout,
            bool_use_timer=bool_use_timer,
            list_status_forcelist=list_status_forcelist,
            logger=logger,
        )

    def _validate_init_params(
        self,
        int_retries: int,
        int_backoff_factor: int,
        list_anonymity_value: list[str],
        float_min_ratio_times_alive_dead: Optional[float],
        float_max_timeout: Optional[float],
        max_iter_find_healthy_proxy: int,
        timeout_session: Optional[float],
        int_wait_load_seconds: Optional[int],
        str_plan_id_webshare: str,
    ) -> None:
        """Validate initialization parameters.

        Parameters
        ----------
        int_retries : int
            Number of retry attempts
        int_backoff_factor : int
            Backoff factor for retries
        list_anonymity_value : list[str]
            List of anonymity values
        float_min_ratio_times_alive_dead : Optional[float]
            Minimum alive/dead ratio
        float_max_timeout : Optional[float]
            Maximum timeout in seconds
        max_iter_find_healthy_proxy : int
            Maximum number of iterations to find a healthy proxy
        timeout_session : Optional[float]
            Session timeout in seconds
        int_wait_load_seconds : Optional[int]
            Number of seconds to wait before loading proxies
        str_plan_id_webshare : str
            Plan ID for WebShare

        Raises
        ------
        ValueError
            If int_retries is negative
            If int_backoff_factor is negative
            If list_anonymity_value is empty
            If float_min_ratio_times_alive_dead is negative
            If float_max_timeout is not positive
            If max_iter_find_healthy_proxy is not positive
            If timeout_session is not positive
            If int_wait_load_seconds is negative
            If str_plan_id_webshare is empty
        """
        if int_retries < 0:
            raise ValueError("int_retries must be non-negative")
        if int_backoff_factor < 0:
            raise ValueError("int_backoff_factor must be non-negative")
        if not list_anonymity_value:
            raise ValueError("list_anonymity_value cannot be empty")
        if float_min_ratio_times_alive_dead is not None and float_min_ratio_times_alive_dead < 0:
            raise ValueError("float_min_ratio_times_alive_dead must be non-negative")
        if float_max_timeout is not None and float_max_timeout <= 0:
            raise ValueError("float_max_timeout must be positive")
        if max_iter_find_healthy_proxy <= 0:
            raise ValueError("max_iter_find_healthy_proxy must be positive")
        if timeout_session is not None and timeout_session <= 0:
            raise ValueError("timeout_session must be positive")
        if int_wait_load_seconds is not None and int_wait_load_seconds < 0:
            raise ValueError("int_wait_load_seconds must be non-negative")
        if not str_plan_id_webshare:
            raise ValueError("str_plan_id_webshare cannot be empty")

    def _cache(self) -> list[Session]:
        """Fetch and cache proxy sessions from multiple sources.

        Returns
        -------
        list[Session]
            List of configured proxy sessions

        Raises
        ------
        ValueError
            If no proxies are available after max retries
        """
        list_sessions = []
        for session_list in [
            self.cls_spys_one_country.configured_sessions(),
            self.cls_free_proxy_net.configured_sessions(),
            self.cls_spysme_countries.configured_sessions(),
            self.cls_freeproxy_world.configured_sessions(),
            self.cls_proxy_nova.configured_sessions(),
            self.cls_proxy_scrape_all.configured_sessions(),
            self.cls_proxy_scrape_country.configured_sessions(),
            self.cls_proxy_webshare.configured_sessions(),
        ]:
            if session_list is not None:
                list_sessions.extend(session_list)
        self.create_logs.log_message(
            self.logger,
            f"Number of proxies healthy: {len(list_sessions)}",
            log_level="info",
        )
        if len(list_sessions) == 0 and self._retry_count < self.max_iter_find_healthy_proxy:
            self.create_logs.log_message(
                self.logger,
                f"No proxies available - retrying {self._retry_count + 1}"
                + f"/{self.max_iter_find_healthy_proxy}",
                log_level="warning",
            )
            self._retry_count += 1
            return self._cache()
        if len(list_sessions) == 0 and self._retry_count >= self.max_iter_find_healthy_proxy:
            self.create_logs.log_message(
                self.logger,
                f"No proxies available after {self.max_iter_find_healthy_proxy} attempts",
                log_level="error",
            )
            raise ValueError("No proxies available")
        self._last_cache_time = time.time()
        self._retry_count = 0
        return list_sessions

    def __next__(self) -> Session:
        """Retrieve the next available proxy session.

        Returns
        -------
        Session
            Configured requests session

        Raises
        ------
        ValueError
            If no proxies are available or timeout is reached
        """
        if not self.bool_new_proxy:
            raise ValueError("New proxy fetching is disabled")
        list_proxies = self.cached_sessions
        while len(list_proxies) == 0 \
            and time.time() - self._last_cache_time < self.timeout_session:
            self.cached_sessions = self._cache()
            list_proxies = self.cached_sessions
        if len(list_proxies) == 0 \
            and time.time() - self._last_cache_time > self.timeout_session:
            self.create_logs.log_message(
                self.logger,
                f"Timeout reached: {self.timeout_session} seconds / Retries: {self._retry_count}",
                log_level="critical",
            )
            raise ValueError("No proxies available")
        if len(list_proxies) == 0:
            self.create_logs.log_message(
                self.logger,
                f"No proxies available after {self.max_iter_find_healthy_proxy} attempts",
                log_level="critical",
            )
            raise ValueError("No proxies available")
        return list_proxies.pop(0)