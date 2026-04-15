"""Protocols for Reuters API client.

References
----------
.. [1] https://www.reuters.com/markets/currencies
"""

from typing import Literal, Optional, Protocol, Union, runtime_checkable

from stpstone.utils.providers.ww.reuters._dto import ReturnToken


@runtime_checkable
class IReuters(Protocol):
	"""Structural protocol for Reuters API clients."""

	def fetch_data(
		self,
		app: str,
		payload: Optional[dict],
		method: Literal['GET', 'POST'],
		endpoint: str,
		timeout: Union[tuple, float, int],
	) -> str:
		"""Fetch raw data from a Reuters API endpoint.

		Parameters
		----------
		app : str
			Endpoint path appended to the base URL.
		payload : Optional[dict]
			Request parameters.
		method : Literal['GET', 'POST']
			HTTP method.
		endpoint : str
			Base API URL.
		timeout : Union[tuple, float, int]
			Request timeout.

		Returns
		-------
		str
			Raw response text.
		"""
		...

	def token(self, api_key: str, deviceid: str) -> ReturnToken:
		"""Get authentication token from Reuters API.

		Parameters
		----------
		api_key : str
			API key for authentication.
		deviceid : str
			Device identifier.

		Returns
		-------
		ReturnToken
			Dictionary containing token information.
		"""
		...

	def quotes(self, currency: str) -> dict:
		"""Fetch currency quotes from Reuters API.

		Parameters
		----------
		currency : str
			Currency code to fetch.

		Returns
		-------
		dict
			Dictionary containing quote information.
		"""
		...
