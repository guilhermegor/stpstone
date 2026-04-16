"""ANBIMA Data API base client: authentication and generic HTTP dispatch.

References
----------
.. [1] https://developers.anbima.com.br/pt/
"""

from typing import Any, Literal, Optional

from requests import exceptions as req_exceptions, request

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.json import JsonFiles
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.providers.br.anbimadata_api._dto import ReturnAccessToken
from stpstone.utils.providers.br.anbimadata_api._ports import IAnbimaApiClient


class AnbimaDataGen(metaclass=TypeChecker):
	"""Base ANBIMA API client — handles OAuth token acquisition and generic requests."""

	_URL_TOKEN = "https://api.anbima.com.br/oauth/access-token"  # noqa: S105
	_URL_HOST_DEV = "https://api-sandbox.anbima.com.br/"
	_URL_HOST_PRD = "https://api.anbima.com.br/"

	def __init__(
		self,
		str_client_id: str,
		str_client_secret: str,
		str_env: Literal["dev", "prd"] = "dev",
		int_chunk: int = 1000,
	) -> None:
		"""Initialize ANBIMA API client.

		Parameters
		----------
		str_client_id : str
			Client ID for API authentication.
		str_client_secret : str
			Client secret for API authentication.
		str_env : Literal['dev', 'prd']
			Target environment, defaults to ``"dev"``.
		int_chunk : int
			Page size for paginated requests, defaults to 1000.

		Raises
		------
		ValueError
			If ``str_client_id`` or ``str_client_secret`` is empty, or if the
			authentication request fails.
		NotImplementedError
			If this class does not satisfy the ``IAnbimaApiClient`` port.
		"""
		if not str_client_id or not str_client_id.strip():
			raise ValueError("str_client_id cannot be empty")
		if not str_client_secret or not str_client_secret.strip():
			raise ValueError("str_client_secret cannot be empty")

		self.str_client_id = str_client_id
		self.str_client_secret = str_client_secret
		self.int_chunk = int_chunk
		self.str_host = self._URL_HOST_DEV if str_env == "dev" else self._URL_HOST_PRD
		self._token_cache: Optional[ReturnAccessToken] = None
		self._cls_str = StrHandler()
		self._cls_json = JsonFiles()
		self.str_token = self.access_token()["access_token"]
		if not isinstance(self, IAnbimaApiClient):
			raise NotImplementedError(
				f"{type(self).__name__} does not satisfy the IAnbimaApiClient port — "
				"implement access_token() and generic_request()"
			)

	def access_token(self) -> ReturnAccessToken:
		"""Return a cached or freshly acquired OAuth access token.

		Returns
		-------
		ReturnAccessToken
			Dictionary containing the ``access_token`` string.

		Raises
		------
		ValueError
			If the authentication request fails.
		"""
		if self._token_cache is not None:
			return self._token_cache

		base64_credentials = self._cls_str.base64_encode(
			self.str_client_id, self.str_client_secret
		)
		dict_headers = {
			"Content-Type": "application/json",
			"Authorization": base64_credentials,
		}
		dict_payload = {"grant_type": "client_credentials"}

		try:
			resp = request(
				method="POST",
				url=self._URL_TOKEN,
				headers=dict_headers,
				data=self._cls_json.dict_to_json(dict_payload),
				timeout=(200, 200),
			)
			resp.raise_for_status()
		except req_exceptions.HTTPError as err:
			raise ValueError("Failed to retrieve access token") from err

		self._token_cache = resp.json()
		return self._token_cache

	def generic_request(
		self,
		str_app: str,
		str_method: Literal["GET", "POST"],
		dict_payload: Optional[dict[str, Any]] = None,
	) -> list[dict[str, Any]]:
		"""Dispatch an authenticated request to ANBIMA's API.

		Parameters
		----------
		str_app : str
			Relative endpoint path appended to the host URL.
		str_method : Literal['GET', 'POST']
			HTTP verb.
		dict_payload : Optional[dict[str, Any]]
			Query params for GET or request body for POST.

		Returns
		-------
		list[dict[str, Any]]
			Parsed JSON response.

		Raises
		------
		ValueError
			If the HTTP request fails.
		"""
		str_url = self.str_host + str_app
		dict_headers = {
			"accept": "application/json",
			"client_id": self.str_client_id,
			"access_token": self.str_token,
		}
		params = dict_payload if str_method == "GET" else None
		data = (
			self._cls_json.dict_to_json(dict_payload)
			if str_method == "POST" and dict_payload
			else None
		)

		try:
			resp = request(
				method=str_method,
				url=str_url,
				headers=dict_headers,
				params=params,
				data=data,
				timeout=(200, 200),
			)
			resp.raise_for_status()
		except req_exceptions.HTTPError as err:
			raise ValueError(f"API request failed: {err}") from err

		return resp.json()
