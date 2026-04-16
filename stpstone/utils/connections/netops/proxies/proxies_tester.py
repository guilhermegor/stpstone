"""Proxy testing utilities for validating proxy server connectivity.

This module provides a class for testing proxy server connectivity using HTTP requests.
It includes methods for configuring sessions and testing proxy responsiveness with
robust input validation and error handling.
"""

import time
from typing import Any, Optional, TypedDict

from requests import Session
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util import Retry

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class ReturnTestSpecificProxy(TypedDict):
	"""Dictionary containing proxy test results.

	Parameters
	----------
	success : bool
		Indicates if the proxy test was successful
	response_time : Optional[float]
		Response time in seconds if successful
	error : Optional[str]
		Error message if the test failed
	response : Optional[dict[str, Any]]
		Response data if successful
	"""

	success: bool
	response_time: Optional[float]
	error: Optional[str]
	response: Optional[dict[str, Any]]


class ProxyTester(metaclass=TypeChecker):
	"""Class for testing proxy server connectivity.

	Parameters
	----------
	ip : str
		Proxy IP address
	port : int
		Proxy port number

	Attributes
	----------
	ip : str
		Proxy IP address
	port : int
		Proxy port number
	list_status_forcelist : list[int]
		HTTP status codes for retry
	"""

	def __init__(self, ip: str, port: int) -> None:
		"""Initialize ProxyTester with IP and port.

		Parameters
		----------
		ip : str
			Proxy IP address
		port : int
			Proxy port number
		"""
		self._validate_init_params(ip, port)
		self.ip = ip
		self.port = port
		self.list_status_forcelist: list[int] = [429, 500, 502, 503, 504]

	def _validate_init_params(self, ip: str, port: int) -> None:
		"""Validate initialization parameters.

		Parameters
		----------
		ip : str
			Proxy IP address
		port : int
			Proxy port number

		Raises
		------
		ValueError
			If IP is empty or invalid
			If port is not positive
		"""
		if not ip:
			raise ValueError("IP cannot be empty")
		if not isinstance(ip, str):
			raise ValueError("IP must be a string")
		if not isinstance(port, int):
			raise ValueError("Port must be an integer")
		if port <= 0:
			raise ValueError("Port must be positive")

	def _validate_configure_session_params(
		self,
		int_retries: int,
		int_backoff_factor: int,
	) -> None:
		"""Validate parameters for session configuration.

		Parameters
		----------
		dict_proxy : Optional[dict[str, str]]
			Proxy dictionary
		int_retries : int
			Number of retry attempts
		int_backoff_factor : int
			Backoff factor for retries

		Raises
		------
		ValueError
			If int_retries is negative
			If int_backoff_factor is negative
		"""
		if int_retries < 0:
			raise ValueError("int_retries must be a non-negative integer")
		if int_backoff_factor < 0:
			raise ValueError("int_backoff_factor must be a non-negative integer")

	def _validate_test_specific_proxy_params(self, test_url: str) -> None:
		"""Validate parameters for testing a specific proxy.

		Parameters
		----------
		test_url : str
			URL to use for testing

		Raises
		------
		ValueError
			If test_url is empty
			If test_url does not start with http:// or https://
		"""
		if not test_url:
			raise ValueError("test_url cannot be empty")
		if not (test_url.startswith("http://") or test_url.startswith("https://")):
			raise ValueError("test_url must start with http:// or https://")

	def _configure_session(
		self,
		dict_proxy: Optional[dict[str, str]] = None,
		int_retries: int = 10,
		int_backoff_factor: int = 1,
	) -> Session:
		"""Configure a requests session with retry strategy.

		Parameters
		----------
		dict_proxy : Optional[dict[str, str]]
			Proxy dictionary (default: None)
		int_retries : int
			Number of retry attempts (default: 10)
		int_backoff_factor : int
			Backoff factor for retries (default: 1)

		Returns
		-------
		Session
			Configured requests session
		"""
		self._validate_configure_session_params(dict_proxy, int_retries, int_backoff_factor)
		retry_strategy = Retry(
			total=int_retries,
			backoff_factor=int_backoff_factor,
			status_forcelist=self.list_status_forcelist,
		)
		adapter = HTTPAdapter(max_retries=retry_strategy)
		session = Session()
		session.mount("http://", adapter)
		session.mount("https://", adapter)
		if dict_proxy is not None:
			session.proxies.update(dict_proxy)
		return session

	def test_specific_proxy(
		self, test_url: str = "https://lumtest.com/myip.json"
	) -> ReturnTestSpecificProxy:
		"""Test a specific proxy IP and port combination.

		Parameters
		----------
		test_url : str
			URL to use for testing (default: 'https://lumtest.com/myip.json')

		Returns
		-------
		ReturnTestSpecificProxy
			Dictionary with test results containing:
			- success: bool indicating if proxy worked
			- response_time: Optional[float] with response time in seconds
			- error: Optional[str] with error message if failed
			- response: Optional[dict[str, Any]] with response data if successful

		Raises
		------
		ValueError
			If RequestException is raised during session configuration / response retrieval
		"""
		self._validate_test_specific_proxy_params(test_url)
		result: ReturnTestSpecificProxy = {
			"success": False,
			"response_time": None,
			"error": None,
			"response": None,
		}
		start_time = time.time()
		try:
			session = self._configure_session(
				dict_proxy={
					"http": f"http://{self.ip}:{self.port}",
					"https": f"http://{self.ip}:{self.port}",
				},
				int_retries=0,
				int_backoff_factor=0,
			)
			response = session.get(
				test_url,
				headers={
					"accept": "application/json",
					"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
					"(KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
				},
				timeout=(5, 10),
			)
			response.raise_for_status()
			result.update(
				{
					"success": True,
					"response_time": time.time() - start_time,
					"response": response.json(),
				}
			)
		except RequestException as err:
			result["error"] = str(err)
			raise ValueError(f"Failed to test proxy: {str(err)}") from err
		return result
