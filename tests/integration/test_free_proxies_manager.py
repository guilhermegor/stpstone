"""Proxy load testing utilities for evaluating proxy server performance.

This module provides a class for testing proxy server performance by sending HTTP requests
through multiple proxies, tracking success rates, and logging results with comprehensive
validation and error handling.
"""

from logging import Logger
import time
from typing import Optional

from requests import Session
from requests.exceptions import RequestException

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy
from stpstone.utils.loggs.create_logs import CreateLog


class ProxyLoadTester(metaclass=TypeChecker):
	"""Class for load testing proxy servers.

	Parameters
	----------
	bool_new_proxy : bool
		Whether to fetch a new proxy (default: True)
	dict_proxies : Optional[dict[str, str]]
		Dictionary of proxy settings (default: None)
	int_retries_new_proxies_not_mapped : int
		Number of retry attempts for new proxies (default: 10)
	int_backoff_factor : int
		Backoff factor for retries (default: 1)
	bool_alive : bool
		Whether to filter for alive proxies (default: True)
	list_anonymity_value : list[str]
		List of allowed anonymity levels (default: ["anonymous", "elite"])
	list_protocol : str
		Proxy protocol (default: "http")
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
	list_status_forcelist : list[int]
		HTTP status codes for retry (default: [429, 500, 502, 503, 504])
	logger : Optional[Logger]
		Logger instance (default: None)
	str_plan_id_webshare : str
		Webshare plan ID (default: "free")
	max_iter_find_healthy_proxy : int
		Maximum iterations to find a healthy proxy (default: 30)
	timeout_session : Optional[float]
		Session timeout in milliseconds (default: 1000.0)
	int_wait_load_seconds : Optional[int]
		Wait time between proxy loads in seconds (default: 10)

	Attributes
	----------
	set_used_proxies : set[str]
		Set of used proxy HTTP addresses
	time_ : float
		Start time of the test
	cls_yield_proxy : YieldFreeProxy
		Proxy yield instance
	create_log : CreateLog
		Logging utility instance
	"""

	def __init__(
		self,
		bool_new_proxy: bool = True,
		dict_proxies: Optional[dict[str, str]] = None,
		int_retries_new_proxies_not_mapped: int = 10,
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
		max_iter_find_healthy_proxy: int = 30,
		timeout_session: Optional[float] = 1000.0,
		int_wait_load_seconds: Optional[int] = 10,
	) -> None:
		"""Initialize ProxyLoadTester with configuration parameters.

		Parameters
		----------
		bool_new_proxy : bool
			Whether to fetch a new proxy (default: True)
		dict_proxies : Optional[dict[str, str]]
			Dictionary of proxy settings (default: None)
		int_retries_new_proxies_not_mapped : int
			Number of retry attempts for new proxies (default: 10)
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
			Maximum iterations to find a healthy proxy (default: 30)
		timeout_session : Optional[float]
			Session timeout in milliseconds (default: 1000.0)
		int_wait_load_seconds : Optional[int]
			Wait time between proxy loads in seconds (default: 10)
		"""
		self._validate_init_params(
			dict_proxies,
			int_retries_new_proxies_not_mapped,
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
		self.int_retries_new_proxies_not_mapped = int_retries_new_proxies_not_mapped
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
		self.create_log = CreateLog()
		self.time_ = time.time()
		self.set_used_proxies: set[str] = set()
		self.cls_yield_proxy = YieldFreeProxy(
			bool_new_proxy=bool_new_proxy,
			dict_proxies=dict_proxies,
			int_retries=int_retries_new_proxies_not_mapped,
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
			str_plan_id_webshare=str_plan_id_webshare,
			max_iter_find_healthy_proxy=max_iter_find_healthy_proxy,
			timeout_session=timeout_session,
			int_wait_load_seconds=int_wait_load_seconds,
		)

	def _validate_init_params(
		self,
		int_retries_new_proxies_not_mapped: int,
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
		int_retries_new_proxies_not_mapped : int
			Number of retry attempts for new proxies (default: 10)
		int_backoff_factor : int
			Backoff factor for retries (default: 1)
		list_anonymity_value : list[str]
			List of allowed anonymity levels (default: ['anonymous', 'elite'])
		float_min_ratio_times_alive_dead : Optional[float]
			Minimum alive/dead ratio (default: 0.02)
		float_max_timeout : Optional[float]
			Maximum timeout in seconds (default: 600)
		max_iter_find_healthy_proxy : int
			Maximum iterations to find a healthy proxy (default: 30)
		timeout_session : Optional[float]
			Session timeout in milliseconds (default: 1000.0)
		int_wait_load_seconds : Optional[int]
			Wait time between proxy loads in seconds (default: 10)
		str_plan_id_webshare : str
			Webshare plan ID (default: 'free')

		Raises
		------
		ValueError
			If int_retries_new_proxies_not_mapped is negative
			If int_backoff_factor is negative
			If list_anonymity_value is empty
			If float_min_ratio_times_alive_dead is negative
			If float_max_timeout is not positive
			If max_iter_find_healthy_proxy is not positive
			If timeout_session is not positive
			If int_wait_load_seconds is negative
		"""
		if int_retries_new_proxies_not_mapped < 0:
			raise ValueError("int_retries_new_proxies_not_mapped must be non-negative")
		if int_backoff_factor < 0:
			raise ValueError("int_backoff_factor must be non-negative")
		if not list_anonymity_value:
			raise ValueError("list_anonymity_value cannot be empty")
		if float_min_ratio_times_alive_dead < 0:
			raise ValueError("float_min_ratio_times_alive_dead must be non-negative")
		if float_max_timeout <= 0:
			raise ValueError("float_max_timeout must be positive")
		if max_iter_find_healthy_proxy <= 0:
			raise ValueError("max_iter_find_healthy_proxy must be positive")
		if timeout_session <= 0:
			raise ValueError("timeout_session must be positive")
		if int_wait_load_seconds < 0:
			raise ValueError("int_wait_load_seconds must be non-negative")
		if not str_plan_id_webshare:
			raise ValueError("str_plan_id_webshare cannot be empty")

	def _validate_test_proxy_session_params(self, test_num: int) -> None:
		"""Validate parameters for testing a proxy session.

		Parameters
		----------
		test_num : int
			Test number identifier

		Raises
		------
		ValueError
			If test_num is not positive
		"""
		if test_num <= 0:
			raise ValueError("test_num must be positive")

	def test_proxy_session(self, session: Session, test_num: int) -> bool:
		"""Test a proxy session by sending an HTTP request.

		Parameters
		----------
		session : Session
			Requests session object
		test_num : int
			Test number identifier

		Returns
		-------
		bool
			True if the test is successful
		"""
		self._validate_test_proxy_session_params(session, test_num)
		try:
			self.create_log.log_message(self.logger, f"--- Testing Proxy #{test_num} ---", "info")
			self.create_log.log_message(self.logger, f"Proxy: {session.proxies}", "info")
			resp_req = session.get("https://jsonplaceholder.typicode.com/todos/1", timeout=10)
			resp_req.raise_for_status()
			self.create_log.log_message(self.logger, "Proxy Test Successful!", "info")
			self.create_log.log_message(
				self.logger, f"Response Status: {resp_req.status_code}", "info"
			)
			self.create_log.log_message(self.logger, f"Response Data: {resp_req.json()}", "info")
			return True
		except RequestException as err:
			self.create_log.log_message(self.logger, f"Proxy Test Failed: {str(err)}", "info")
			return False

	def _validate_run_tests_params(self, n_trials: int) -> None:
		"""Validate parameters for running proxy tests.

		Parameters
		----------
		n_trials : int
			Number of test trials

		Raises
		------
		ValueError
			If n_trials is not positive
		"""
		if n_trials <= 0:
			raise ValueError("n_trials must be positive")

	def run_tests(self, n_trials: int = 20) -> None:
		"""Run multiple proxy tests and log results.

		Parameters
		----------
		n_trials : int
			Number of test trials (default: 20)

		Raises
		------
		ValueError
			If proxy test or iteration fails
			If n_trials is not positive
		"""
		self._validate_run_tests_params(n_trials)
		successful_tests = 0
		for i in range(1, n_trials + 1):
			self.create_log.log_message(self.logger, f"--- Testing Proxy #{i} ---", "info")
			int_try = 0
			try:
				session = next(self.cls_yield_proxy)
				while (
					session.proxies["http"] in self.set_used_proxies
					and int_try < self.int_retries_new_proxies_not_mapped
				):
					self.create_log.log_message(
						self.logger,
						f"Proxy {session.proxies} is already used - attempt #{int_try + 1} "
						f"/ {self.int_retries_new_proxies_not_mapped}",
						"warning",
					)
					session = next(self.cls_yield_proxy)
					int_try += 1
				if int_try >= self.int_retries_new_proxies_not_mapped:
					self.create_log.log_message(
						self.logger, f"--- Max retries reached for Proxy #{i} ---", "critical"
					)
					raise ValueError(f"Max retries reached for Proxy #{i}")
				self.set_used_proxies.add(session.proxies["http"])
				if self.test_proxy_session(session, i)["success"]:
					successful_tests += 1
			except StopIteration as err:
				self.create_log.log_message(self.logger, "No more proxies available", "critical")
				raise ValueError("No more proxies available") from err
			except RequestException as err:
				self.create_log.log_message(
					self.logger, f"Error getting proxy #{i}: {str(err)}", "critical"
				)
				raise ValueError(f"Error getting proxy #{i}: {str(err)}") from err
		self.create_log.log_message(self.logger, "--- Test Summary ---", "info")
		self.create_log.log_message(self.logger, f"Total tests attempted: {n_trials}", "info")
		self.create_log.log_message(self.logger, f"Successful tests: {successful_tests}", "info")
		self.create_log.log_message(
			self.logger, f"Success rate: {successful_tests / n_trials * 100:.1f}%", "info"
		)
		self.create_log.log_message(
			self.logger, f"Number of unique proxies used: {len(self.set_used_proxies)}", "info"
		)
		self.create_log.log_message(
			self.logger,
			f"Elapsed time for {n_trials} trials: {time.time() - self.time_:.2f} seconds",
			"info",
		)


if __name__ == "__main__":
	cls_load_test_proxies = ProxyLoadTester(
		int_retries_new_proxies_not_mapped=20,
		max_iter_find_healthy_proxy=30,
		str_country_code="BR",
		bool_new_proxy=True,
		str_continent_code=None,
		bool_alive=True,
		list_anonymity_value=["elite", "anonymous"],
		list_protocol=["http", "https"],
		bool_ssl=None,
		float_min_ratio_times_alive_dead=None,
		float_max_timeout=10000,
		bool_use_timer=False,
		list_status_forcelist=[429, 500, 502, 503, 504],
		logger=None,
		str_plan_id_webshare="free",
		timeout_session=1000.0,
		int_wait_load_seconds=100,
	)

	cls_load_test_proxies.run_tests(n_trials=20)
