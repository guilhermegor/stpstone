"""Abstract base class for proxy management.

This module defines an abstract base class (ABC) for proxy management,
providing a common interface for fetching and validating proxies.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from logging import Logger
from random import shuffle
import re
import time
from typing import Literal, Optional, TypedDict, Union

from requests import Session
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, ConnectTimeout, ProxyError, SSLError
from urllib3.util import Retry

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker, type_checker
from stpstone.utils.loggs.create_logs import CreateLog, conditional_timeit
from stpstone.utils.parsers.dicts import HandlingDicts


class ReturnAvailableProxies(TypedDict):
    """Typed dictionary for proxy information.

    Parameters
    ----------
    protocol : str
        Proxy protocol (http/https)
    bool_alive : bool
        Whether proxy is currently alive
    status : str
        Proxy status
    alive_since : float
        Unix timestamp when proxy was last alive
    anonymity : str
        Proxy anonymity level
    average_timeout : float
        Average response timeout
    first_seen : float
        Unix timestamp when proxy was first seen
    ip_data : str
        IP metadata
    ip_name : str
        IP hostname
    timezone : str
        Proxy timezone
    continent : str
        Proxy continent name
    continent_code : str
        Proxy continent code
    country : str
        Proxy country name
    country_code : str
        Proxy country code
    city : str
        Proxy city
    district : str
        Proxy district
    region_name : str
        Proxy region name
    zip : str
        Proxy postal code
    bool_hosting : bool
        Whether proxy is hosting
    isp : str
        Internet Service Provider
    latitude : float
        Proxy latitude
    longitude : float
        Proxy longitude
    organization : str
        Proxy organization
    proxy : bool
        Whether is a proxy
    ip : str
        Proxy IP address
    port : str
        Proxy port
    bool_ssl : bool
        Whether SSL is supported
    timeout : float
        Current timeout
    times_alive : int
        Number of times proxy was alive
    times_dead : int
        Number of times proxy was dead
    ratio_times_alive_dead : float
        Alive/dead ratio
    uptime : float
        Proxy uptime percentage
    """

    protocol: str
    bool_alive: bool
    status: str
    alive_since: float
    anonymity: Literal["anonymous", "elite", "transparent"]
    average_timeout: float
    first_seen: float
    ip_data: str
    ip_name: str
    timezone: str
    continent: str
    continent_code: str
    country: str
    country_code: str
    city: str
    district: str
    region_name: str
    zip: str
    bool_hosting: bool
    isp: str
    latitude: float
    longitude: float
    organization: str
    proxy: bool
    ip: str
    port: str
    bool_ssl: bool
    timeout: float
    times_alive: int
    times_dead: int
    ratio_times_alive_dead: float
    uptime: float


class ABCMetaClass(TypeChecker, ABC):
    """Metaclass combining TypeChecker and ABC functionality."""


class ABCSession(ABC, metaclass=ABCMetaClass):
    """Abstract base class for proxy session management.

    Parameters
    ----------
    bool_new_proxy : bool
        Whether to fetch a new proxy (default: True)
    dict_proxies : dict[str, str] | None
        Dictionary of proxy settings (default: None)
    int_retries : int
        Number of retry attempts (default: 10)
    int_backoff_factor : int
        Backoff factor for retries (default: 1)
    bool_alive : bool
        Whether to filter for alive proxies (default: True)
    list_anonymity_value : list[str] | None
        List of allowed anonymity levels (default: None)
    list_protocol : list[str]
        List of allowed protocols (default: ["http", "https"])
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
        HTTP status codes for retry (default: None)
    logger : Optional[Logger]
        Logger instance (default: None)

    Notes
    -----
    Proxy levels:
        - Transparent: target server knows your IP address and it knows that you are connecting via a
          proxy server.
        - Anonymous: target server does not know your IP address, but it knows that you're using a
          proxy.
        - Elite or High anonymity: target server does not know your IP address, or that the request is
          relayed through a proxy server.
    """

    def __init__(
        self,
        bool_new_proxy: bool = True,
        dict_proxies: Optional[dict[str, str]] = None,
        int_retries: int = 10,
        int_backoff_factor: int = 1,
        bool_alive: bool = True,
        list_anonymity_value: Optional[list[str]] = None,
        list_protocol: list[str] = ["http", "https"],
        str_continent_code: Optional[str] = None,
        str_country_code: Optional[str] = None,
        bool_ssl: Optional[bool] = None,
        float_min_ratio_times_alive_dead: Optional[float] = 0.02,
        float_max_timeout: Optional[float] = 600,
        bool_use_timer: bool = False,
        list_status_forcelist: Optional[list[int]] = None,
        logger: Optional[Logger] = None,
    ) -> None:
        self._validate_init_params(
            bool_new_proxy,
            dict_proxies,
            int_retries,
            int_backoff_factor,
            bool_alive,
            list_anonymity_value,
            list_protocol,
            str_continent_code,
            str_country_code,
            bool_ssl,
            float_min_ratio_times_alive_dead,
            float_max_timeout,
            bool_use_timer,
            list_status_forcelist,
            logger,
        )
        self.bool_new_proxy = bool_new_proxy
        self.dict_proxies = dict_proxies
        self.int_retries = int_retries
        self.int_backoff_factor = int_backoff_factor
        self.bool_alive = bool_alive
        self.list_anonymity_value = list_anonymity_value or ["anonymous", "elite"]
        self.list_protocol = list_protocol if isinstance(list_protocol, list) else [list_protocol]
        self.str_continent_code = str_continent_code
        self.str_country_code = str_country_code
        self.bool_ssl = bool_ssl
        self.float_min_ratio_times_alive_dead = float_min_ratio_times_alive_dead
        self.float_max_timeout = float_max_timeout
        self.bool_use_timer = bool_use_timer
        self.list_status_forcelist = list_status_forcelist or [429, 500, 502, 503, 504]
        self.logger = logger
        self.cls_create_log = CreateLog()

    def _validate_init_params(
        self,
        int_retries: int,
        int_backoff_factor: int,
        float_min_ratio_times_alive_dead: Optional[float],
        float_max_timeout: Optional[float],
    ) -> None:
        """Validate initialization parameters.

        Parameters
        ----------
        int_retries : int
            Number of retry attempts
        int_backoff_factor : int
            Backoff factor for retries
        float_min_ratio_times_alive_dead : Optional[float]
            Minimum alive/dead ratio
        float_max_timeout : Optional[float]
            Maximum timeout in seconds

        Raises
        ------
        ValueError
            If int_retries is negative
            If int_backoff_factor is negative
            If float_min_ratio_times_alive_dead is negative
            If float_max_timeout is not positive
        """
        if int_retries < 0:
            raise ValueError("int_retries must be a non-negative integer")
        if int_backoff_factor < 0:
            raise ValueError("int_backoff_factor must be a non-negative integer")
        if float_min_ratio_times_alive_dead is not None and float_min_ratio_times_alive_dead < 0:
            raise ValueError("float_min_ratio_times_alive_dead must be non-negative")
        if float_max_timeout is not None and float_max_timeout <= 0:
            raise ValueError("float_max_timeout must be positive")

    def _validate_proxy_structure(
        self, 
        list_proxies: list[dict[str, Union[str, int, float, bool]]]
    ) -> None:
        """Validate proxy data structure.

        Parameters
        ----------
        list_proxies : list[dict[str, str | int | float | bool]]
            List of proxy dictionaries to validate

        Raises
        ------
        ValueError
            If any proxy dictionary is missing required keys
        """
        if not list_proxies:
            raise ValueError("list_proxies cannot be empty")
        required_keys = {
            "protocol", "bool_alive", "status", "alive_since", "anonymity",
            "average_timeout", "first_seen", "ip_data", "ip_name", "timezone",
            "continent", "continent_code", "country", "country_code", "city",
            "district", "region_name", "zip", "bool_hosting", "isp", "latitude",
            "longitude", "organization", "proxy", "ip", "port", "bool_ssl",
            "timeout", "times_alive", "times_dead", "ratio_times_alive_dead",
            "uptime"
        }
        for proxy in list_proxies:
            missing_keys = required_keys - set(proxy.keys())
            if missing_keys:
                raise ValueError(f"Missing required keys in proxy data: {missing_keys}")

    def time_ago_to_ts_unix(self, time_ago_str: str) -> float:
        """Convert a time-ago string into a Unix timestamp.

        Parameters
        ----------
        time_ago_str : str
            A string representing a time duration in the past (e.g., '5 minutes ago')

        Returns
        -------
        float
            Unix timestamp corresponding to the time in the past

        Raises
        ------
        ValueError
            If time_ago_str is empty
            If the time measure is unknown or format is invalid
        """
        if not time_ago_str:
            raise ValueError("time_ago_str cannot be empty")
        try:
            time_value = int(time_ago_str.split()[0])
            time_measure = time_ago_str.split()[1]
        except (IndexError, ValueError) as err:
            raise ValueError("Invalid time_ago_str format") from err
        if time_measure in ["min", "mins", "minute", "minutes"]:
            past_time = datetime.now() - timedelta(minutes=time_value)
        elif time_measure in ["hour", "hours"]:
            past_time = datetime.now() - timedelta(hours=time_value)
        elif time_measure in ["day", "days"]:
            past_time = datetime.now() - timedelta(days=time_value)
        elif time_measure in ["secs", "seconds", "sec"]:
            past_time = datetime.now() - timedelta(seconds=time_value)
        else:
            raise ValueError(f"Unknown time measure: {time_measure}")
        return time.mktime(past_time.timetuple()) + past_time.microsecond / 1e6

    def composed_time_ago_to_ts_unix(self, time_elapsed_str: str) -> float:
        """Convert a complex time-ago string into a Unix timestamp.

        Parameters
        ----------
        time_elapsed_str : str
            A string representing time duration (e.g., '1 day 2 hours 3 minutes')

        Returns
        -------
        float
            Unix timestamp corresponding to the time in the past

        Raises
        ------
        ValueError
            If the time string format is invalid
        """
        if not time_elapsed_str:
            raise ValueError("time_elapsed_str cannot be empty")
        time_elapsed_str = time_elapsed_str.strip().lower()
        days, hours, minutes = 0, 0, 0
        day_match = re.search(r"(\d+)\s*(d\.|days|day)", time_elapsed_str)
        hour_match = re.search(r"(\d+)\s*(h\.|hours|hour)", time_elapsed_str)
        minute_match = re.search(r"(\d+)\s*(min|mins|minute|minutes)", time_elapsed_str)
        if day_match:
            days = int(day_match.group(1))
        if hour_match:
            hours = int(hour_match.group(1))
        if minute_match:
            minutes = int(minute_match.group(1))
        past_time = datetime.now() - timedelta(days=days, hours=hours, minutes=minutes)
        return time.mktime(past_time.timetuple()) + past_time.microsecond / 1e6

    def proxy_speed_to_float(self, str_speed: str) -> float:
        """Convert a proxy speed string to seconds.

        Parameters
        ----------
        str_speed : str
            Speed string with unit (e.g., '100 ms')

        Returns
        -------
        float
            Speed in seconds

        Raises
        ------
        ValueError
            If the time measure is unknown or format is invalid
        """
        if not str_speed:
            raise ValueError("str_speed cannot be empty")
        try:
            speed_value = float(str_speed.split()[0])
            time_measure = str_speed.split()[-1]
        except (IndexError, ValueError) as err:
            raise ValueError(f"Invalid str_speed format. Error: {err}") from err
        if time_measure == "ms":
            return speed_value / 1000.0
        elif time_measure == "µs":
            return speed_value / 1000000.0
        elif time_measure == "s":
            return speed_value
        else:
            raise ValueError(f"Unknown time measure: {time_measure}")

    @abstractmethod
    def _available_proxies(self) -> list[ReturnAvailableProxies]:
        """Fetch available proxies.

        Returns
        -------
        list[ReturnAvailableProxies]
            List of proxy dictionaries
        """
        pass

    def _test_proxy(
        self, 
        str_ip: str, 
        int_port: int, 
        bool_return_availability: bool = True
    ) -> Union[bool, Union[bool, dict[str, Union[str, float, bool]]]]:
        """Test if a proxy is available.

        Parameters
        ----------
        str_ip : str
            Proxy IP address
        int_port : int
            Proxy port
        bool_return_availability : bool
            Whether to return availability status (default: True)

        Returns
        -------
        Union[bool, Union[bool, dict[str, Union[str, float, bool]]]]
            True if proxy is available, False otherwise

        Raises
        ------
        ValueError
            If IP or port is invalid
        """
        if not str_ip:
            raise ValueError("str_ip cannot be empty")
        if int_port <= 0:
            raise ValueError("int_port must be positive")
        try:
            session = self._configure_session(
                dict_proxy={
                    "http": f"http://{str_ip}:{int_port}",
                    "https": f"http://{str_ip}:{int_port}"
                },
                int_retries=0,
                int_backoff_factor=0
            )
            return self.ip_infos(session, bool_return_availability=bool_return_availability)
        except (ProxyError, ConnectTimeout, SSLError, ConnectionError):
            return False

    def get_proxy(self) -> Optional[dict[str, str]]:
        """Retrieve a single working proxy.

        Returns
        -------
        Optional[dict[str, str]]
            Dictionary with IP and port of a working proxy, or None if none found

        Notes
        -----
        Proxies are shuffled before testing to ensure randomness.
        """
        @type_checker
        @conditional_timeit(bool_use_timer=self.bool_use_timer)
        def retrieve_proxy() -> Optional[dict[str, str]]:
            """Retrieve a single working proxy.
            
            Returns
            -------
            Optional[dict[str, str]]
                Dictionary with IP and port of a working proxy, or None if none found
            """
            list_proxies = self._filtered_proxies()
            shuffle(list_proxies)
            for dict_proxy in list_proxies:
                str_ip = dict_proxy["ip"]
                int_port = dict_proxy["port"]
                if str_ip and int_port:
                    if self._test_proxy(str_ip, int(int_port), bool_return_availability=True):
                        return {"ip": str_ip, "port": str(int_port)}
            return None
        return retrieve_proxy()

    def get_proxies(self) -> Optional[list[dict[str, str]]]:
        """Retrieve a list of working proxies.

        Returns
        -------
        Optional[list[dict[str, str]]]
            List of dictionaries with IP and port of working proxies, or None if none found
        """
        @type_checker
        @conditional_timeit(bool_use_timer=self.bool_use_timer)
        def retrieve_proxies() -> Optional[list[dict[str, str]]]:
            list_result = []
            list_proxies = self._filtered_proxies()
            for dict_proxy in list_proxies:
                str_ip = dict_proxy["ip"]
                int_port = dict_proxy["port"]
                if str_ip and int_port:
                    bool_test_proxy = self._test_proxy(str_ip, int(int_port), 
                                                       bool_return_availability=True)
                    self.cls_create_log.log_message(
                        self.logger,
                        f"Testing proxy {str_ip}:{int_port} - Healthy: {bool_test_proxy}",
                        log_level="info"
                    )
                    if bool_test_proxy:
                        list_result.append({"ip": str_ip, "port": str(int_port)})
            self.cls_create_log.log_message(
                self.logger,
                f"Number of working proxies: {len(list_result)}",
                log_level="info"
            )
            return list_result if list_result else None
        return retrieve_proxies()

    def ip_infos(
        self, 
        session: Session, 
        bool_return_availability: bool = False, 
        timeout: Union[tuple[int, int], tuple[float, float], float, int] = (5, 5)
    ) -> Union[bool, dict[str, Union[str, float, bool]]]:
        """Fetch IP information using the provided session.

        Parameters
        ----------
        session : Session
            Requests session object
        bool_return_availability : bool
            Whether to return only availability status (default: False)
        timeout : Union[tuple[int, int], tuple[float, float], float, int]
            Connection and read timeout values (default: (5, 5))

        Returns
        -------
        Union[bool, dict[str, Union[str, float, bool]]]
            Availability status or IP information dictionary

        Raises
        ------
        ValueError
            If timeout values are invalid
        """
        if len(timeout) != 2:
            raise TypeError("timeout must be a tuple of two integers")
        if not all(x > 0 for x in timeout):
            raise ValueError("timeout values must be positive integers")
        dict_payload: dict[str, str] = {}
        dict_headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9,pt;q=0.8,es;q=0.7",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
        }
        try:
            resp_req = session.get("https://lumtest.com/myip.json", headers=dict_headers,
                                   data=dict_payload, timeout=timeout)
            resp_req.raise_for_status()
            return True if bool_return_availability else resp_req.json()
        except (ConnectionError, ConnectTimeout, ProxyError, SSLError) as err:
            if bool_return_availability:
                return False
            raise ValueError(f"Failed to fetch IP information. Error: {err}") from err

    def _filtered_proxies(self) -> list[dict[str, Union[str, int, float, bool]]]:
        """Filter available proxies based on specified criteria.

        Returns
        -------
        list[dict[str, Union[str, int, float, bool]]]
            Filtered list of proxy dictionaries
        """
        list_proxies = self._available_proxies()
        self.cls_create_log.log_message(
            self.logger,
            f"Number of available proxies: {len(list_proxies)}",
            log_level="info"
        )
        self._validate_proxy_structure(list_proxies)
        for key, value, strategy in [
            ("bool_alive", self.bool_alive, "equal"),
            ("anonymity", self.list_anonymity_value, "isin"),
            ("protocol", self.list_protocol, "isin"),
            ("bool_ssl", self.bool_ssl, "equal"),
            ("ratio_times_alive_dead", self.float_min_ratio_times_alive_dead, 
             "greater_than_or_equal_to"),
            ("timeout", self.float_max_timeout, "less_than_or_equal_to"),
            ("continent_code", self.str_continent_code, "equal"),
            ("country_code", self.str_country_code, "equal")
        ]:
            if value is not None:
                self.cls_create_log.log_message(
                    self.logger,
                    f"Filtering proxies with {key}={value} / Length: {len(list_proxies)}",
                    log_level="info"
                )
                list_proxies = HandlingDicts().filter_list_ser(
                    list_proxies,
                    key,
                    value,
                    str_filter_type=strategy
                )
                self.cls_create_log.log_message(
                    self.logger,
                    f"Filtered proxies with {key}={value} / Length: {len(list_proxies)}",
                    log_level="info"
                )
        return list_proxies

    def _dict_proxy(
        self, 
        str_ip: str, 
        int_port: int
    ) -> dict[str, str]:
        """Create a proxy dictionary from IP and port.

        Parameters
        ----------
        str_ip : str
            Proxy IP address
        int_port : int
            Proxy port

        Returns
        -------
        dict[str, str]
            Proxy dictionary with http and https entries

        Raises
        ------
        ValueError
            If IP or port is invalid
        """
        if not str_ip:
            raise ValueError("str_ip cannot be empty")
        if int_port <= 0:
            raise ValueError("int_port must be positive")
        return {
            "http": f"http://{str_ip}:{int_port}",
            "https": f"http://{str_ip}:{int_port}"
        }

    def _configure_session(
        self, 
        dict_proxy: Optional[dict[str, str]] = None, 
        int_retries: int = 10, 
        int_backoff_factor: int = 1
    ) -> Session:
        """Configure a requests session with retry strategy.

        Parameters
        ----------
        dict_proxy : dict[str, str] | None
            Proxy dictionary (default: None)
        int_retries : int
            Number of retry attempts (default: 10)
        int_backoff_factor : int
            Backoff factor for retries (default: 1)

        Returns
        -------
        Session
            Configured requests session

        Raises
        ------
        ValueError
            If retry or backoff values are invalid
        """
        if int_retries < 0:
            raise ValueError("int_retries must be a non-negative integer")
        if int_backoff_factor < 0:
            raise ValueError("int_backoff_factor must be a non-negative integer")
        retry_strategy = Retry(
            total=int_retries,
            backoff_factor=int_backoff_factor,
            status_forcelist=self.list_status_forcelist
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        if dict_proxy is not None:
            session.proxies.update(dict_proxy)
        return session

    def session(self) -> Session:
        """Create a configured session with a proxy.

        Returns
        -------
        Session
            Configured requests session
        """
        proxy = self.get_proxy() if self.bool_new_proxy else None
        dict_proxy = self.dict_proxies if self.dict_proxies is not None else (
            self._dict_proxy(proxy["ip"], proxy["port"])
            if proxy is not None else None
        )
        return self._configure_session(dict_proxy, self.int_retries, self.int_backoff_factor)

    def configured_sessions(self) -> Optional[list[Session]]:
        """Create a list of configured sessions with proxies.

        Returns
        -------
        Optional[list[Session]]
            List of configured sessions, or None if no proxies are available
        """
        if not self.bool_new_proxy:
            return None
        list_proxies = self.get_proxies()
        if not list_proxies:
            return None
        list_sessions = []
        for dict_proxy in list_proxies:
            str_ip = dict_proxy["ip"]
            int_port = dict_proxy["port"]
            proxy_dict = self._dict_proxy(str_ip, int(int_port))
            session = self._configure_session(
                proxy_dict, self.int_retries, self.int_backoff_factor)
            list_sessions.append(session)
        return list_sessions