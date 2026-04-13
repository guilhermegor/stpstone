"""B3 LINE API connection and authentication adapter.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
.. [2] https://line.bvmfnet.com.br/#/endpoints
"""

import time
from time import sleep
from typing import Any, Literal, Optional, Union

from requests import Response, request

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.json import JsonFiles
from stpstone.utils.providers.br.line_b3._ports import IConnectionApi


class ConnectionApi(metaclass=TypeChecker):
	"""Adapter that fulfils the ``IConnectionApi`` port for B3 LINE API authentication.

	Owns auth-header retrieval, token refresh, and the generic ``app_request`` method
	used by all downstream adapters.

	Notes
	-----
	Please contact CAU manager account to request a service user.

	Parameters
	----------
	client_id : str
		Client identifier for API authentication.
	client_secret : str
		Client secret for API authentication.
	broker_code : str
		Broker identification code.
	category_code : str
		Category classification code.
	hostname_api_line_b3 : str, optional
		API base URL (default: ``"https://api.line.bvmfnet.com.br"``).

	Raises
	------
	ValueError
		If any credential is empty or the URL does not start with ``https://``.
	NotImplementedError
		If this class does not satisfy the ``IConnectionApi`` port.
	"""

	def __init__(
		self,
		client_id: str,
		client_secret: str,
		broker_code: str,
		category_code: str,
		hostname_api_line_b3: str = "https://api.line.bvmfnet.com.br",
	) -> None:
		"""Initialize API connection with authentication credentials.

		Parameters
		----------
		client_id : str
			Client identifier for API authentication.
		client_secret : str
			Client secret for API authentication.
		broker_code : str
			Broker identification code.
		category_code : str
			Category classification code.
		hostname_api_line_b3 : str, optional
			API base URL (default: ``"https://api.line.bvmfnet.com.br"``).

		Raises
		------
		NotImplementedError
			If this class does not satisfy the ``IConnectionApi`` port.
		"""
		self._validate_credentials(client_id, client_secret, broker_code, category_code)
		self._validate_url(hostname_api_line_b3)

		self.client_id = client_id
		self.client_secret = client_secret
		self.broker_code = broker_code
		self.category_code = category_code
		self.hostname_api_line_b3 = hostname_api_line_b3
		self.token = self.access_token()

		if not isinstance(self, IConnectionApi):
			raise NotImplementedError(
				f"{type(self).__name__} does not satisfy IConnectionApi — "
				"implement auth_header(), access_token(), and app_request()."
			)

	def _validate_credentials(
		self,
		client_id: str,
		client_secret: str,
		broker_code: str,
		category_code: str,
	) -> None:
		"""Validate authentication credentials.

		Parameters
		----------
		client_id : str
			Client identifier.
		client_secret : str
			Client secret.
		broker_code : str
			Broker code.
		category_code : str
			Category code.

		Raises
		------
		ValueError
			If any credential is empty.
		"""
		if not all([client_id, client_secret, broker_code, category_code]):
			raise ValueError("All credentials must be non-empty")

	def _validate_url(self, url: str) -> None:
		"""Validate URL format.

		Parameters
		----------
		url : str
			URL to validate.

		Raises
		------
		ValueError
			If URL is empty or doesn't start with https://.
		"""
		if not url:
			raise ValueError("URL cannot be empty")
		if not url.startswith("https://"):
			raise ValueError("URL must start with https://")

	def auth_header(
		self,
		int_max_retries: int = 2,
		timeout: Union[tuple[float, float], float, int] = 10,
	) -> str:
		"""Retrieve authorization header for API authentication.

		Parameters
		----------
		int_max_retries : int, optional
			Maximum number of retries (default: 2).
		timeout : Union[tuple[float, float], float, int], optional
			Request timeout (default: 10).

		Returns
		-------
		str
			Authorization header value.

		Raises
		------
		ValueError
			If maximum retries exceeded or request fails.
		"""
		dict_headers: dict[str, str] = {
			"Accept": "application/json",
			"Content-Type": "application/x-www-form-urlencoded",
		}
		i: int = 0

		while i <= int_max_retries:
			try:
				resp_req = request(
					method="GET",
					url=self.hostname_api_line_b3 + "/api/v1.0/token/authorization",
					headers=dict_headers,
					verify=False,
					timeout=timeout,
				)
				if resp_req.status_code == 200:
					return resp_req.json()["header"]
				i += 1
			except Exception:
				i += 1
				continue
			sleep(1)

		raise ValueError("Maximum retry attempts exceeded")

	def access_token(
		self,
		int_max_retries: int = 2,
		timeout: Union[tuple[float, float], float, int] = 10,
	) -> str:
		"""Retrieve and manage API access token.

		Parameters
		----------
		int_max_retries : int, optional
			Maximum number of retries (default: 2).
		timeout : Union[tuple[float, float], float, int], optional
			Request timeout (default: 10).

		Returns
		-------
		str
			Valid access token.

		Raises
		------
		ValueError
			If token retrieval fails or maximum retries exceeded.
		"""
		str_auth_header = self.auth_header(int_max_retries=int_max_retries, timeout=timeout)
		dict_headers = {"Authorization": f"Basic {str_auth_header}"}
		int_expiration_time = 0
		i_retrieves = 0
		token = ""
		refresh_token = ""
		int_refresh_min_time: int = 4000
		max_retrieves: int = 100
		int_status_code_ok: int = 200
		i_aux: int = 0
		int_status_code_iteration: int = 400

		while int_expiration_time < int_refresh_min_time and i_retrieves < max_retrieves:
			if i_retrieves == 0:
				dict_params = {
					"grant_type": "password",
					"username": self.client_id,
					"password": self.client_secret,
					"brokerCode": self.broker_code,
					"categoryCode": self.category_code,
				}
			else:
				dict_params = {"grant_type": "refresh_token", "refresh_token": refresh_token}

			while int_status_code_iteration != int_status_code_ok and i_aux <= max_retrieves:
				try:
					resp_req = request(
						method="POST",
						url=self.hostname_api_line_b3 + "/api/oauth/token",
						headers=dict_headers,
						params=dict_params,
						verify=False,
						timeout=timeout,
					)
					int_status_code_iteration = resp_req.status_code
				except Exception:
					i_aux += 1
					continue
				i_aux += 1

			if i_aux > max_retrieves:
				raise ValueError("Maximum retry attempts exceeded")

			resp_req.raise_for_status()
			dict_token = resp_req.json()

			refresh_token = dict_token["refresh_token"]
			token = dict_token["access_token"]
			int_expiration_time = dict_token["expires_in"]
			i_retrieves += 1

		return token

	def app_request(
		self,
		method: Literal["GET", "POST", "DELETE"],
		app: str,
		dict_params: Optional[dict[str, Any]] = None,
		dict_payload: Optional[list[dict[str, Any]]] = None,
		bool_parse_dict_params_data: bool = False,
		bool_retry_if_error: bool = False,
		float_secs_sleep: Optional[float] = None,
		timeout: Union[tuple[float, float], float, int] = 10,
	) -> Union[list[dict[str, Any]], int]:
		"""Execute API request with retry logic.

		Parameters
		----------
		method : Literal['GET', 'POST', 'DELETE']
			HTTP method.
		app : str
			API endpoint.
		dict_params : Optional[dict[str, Any]], optional
			Request parameters (default: None).
		dict_payload : Optional[list[dict[str, Any]]], optional
			Request payload (default: None).
		bool_parse_dict_params_data : bool, optional
			Parse parameters as JSON (default: False).
		bool_retry_if_error : bool, optional
			Enable retry on error (default: False).
		float_secs_sleep : Optional[float], optional
			Sleep time between retries (default: None).
		timeout : Union[tuple[float, float], float, int], optional
			Request timeout (default: 10).

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Response data or status code.

		Raises
		------
		ValueError
			If request fails after retries.
		"""
		i: int = 0
		float_secs_sleep_increase_error: float = 1.0
		float_secs_sleep_iteration: float = float_secs_sleep if float_secs_sleep else 1.0
		dict_header: dict[str, str] = {
			"Authorization": f"Bearer {self.token}",
			"Content-Type": "application/json",
		}
		int_max_retries: int = 100
		int_status_code_ok: int = 200
		list_int_http_error_token: list[int] = [401]
		resp_req: Response = None
		bool_retry_request: bool = bool_retry_if_error

		if bool_parse_dict_params_data:
			if dict_params:
				dict_params = JsonFiles().dict_to_json(dict_params)
			if dict_payload:
				dict_payload = JsonFiles().dict_to_json(dict_payload)

		if bool_retry_if_error:
			while bool_retry_request and i <= int_max_retries:
				try:
					resp_req = request(
						method=method,
						url=self.hostname_api_line_b3 + app,
						headers=dict_header,
						params=dict_params,
						data=dict_payload,
						timeout=timeout,
					)

					if resp_req.status_code == int_status_code_ok:
						bool_retry_request = False
					elif resp_req.status_code in list_int_http_error_token:
						dict_header = {
							"Authorization": f"Bearer {self.access_token()}",
							"Content-Type": "application/json",
						}
					else:
						if float_secs_sleep_iteration:
							float_secs_sleep_iteration += float_secs_sleep_increase_error
				except Exception:
					bool_retry_request = True

				if float_secs_sleep_iteration:
					time.sleep(float_secs_sleep_iteration)
				i += 1
		else:
			resp_req = request(
				method=method,
				url=self.hostname_api_line_b3 + app,
				headers=dict_header,
				params=dict_params,
				data=dict_payload,
				timeout=timeout,
			)

		if resp_req is None:
			raise ValueError("Request failed after retries")

		resp_req.raise_for_status()

		try:
			return resp_req.json()
		except Exception:
			return resp_req.status_code
