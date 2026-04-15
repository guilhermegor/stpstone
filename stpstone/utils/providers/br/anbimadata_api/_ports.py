"""Protocols (structural interfaces) for ANBIMA Data API client.

References
----------
.. [1] https://developers.anbima.com.br/pt/
"""

from typing import Any, Literal, Optional, Protocol, runtime_checkable

from stpstone.utils.providers.br.anbimadata_api._dto import ReturnAccessToken


@runtime_checkable
class IAnbimaApiClient(Protocol):
	"""Structural protocol for any ANBIMA API client.

	Defines the minimum interface required to authenticate and dispatch
	requests against ANBIMA's REST API. Any class that implements
	``access_token`` and ``generic_request`` with compatible signatures
	satisfies this protocol without explicit inheritance.
	"""

	def access_token(self) -> ReturnAccessToken:
		"""Return a valid access token dict.

		Returns
		-------
		ReturnAccessToken
			Dictionary with at least the ``access_token`` key.
		"""
		...

	def generic_request(
		self,
		str_app: str,
		str_method: Literal['GET', 'POST'],
		dict_payload: Optional[dict[str, Any]] = None,
	) -> list[dict[str, Any]]:
		"""Dispatch an authenticated HTTP request to ANBIMA's API.

		Parameters
		----------
		str_app : str
			Relative endpoint path (appended to the host URL).
		str_method : Literal['GET', 'POST']
			HTTP verb.
		dict_payload : Optional[dict[str, Any]]
			Query params (GET) or request body (POST).

		Returns
		-------
		list[dict[str, Any]]
			Parsed JSON response.
		"""
		...
