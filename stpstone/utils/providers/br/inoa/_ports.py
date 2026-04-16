"""Protocols (structural interfaces) for INOA Alpha Tools API client.

References
----------
.. [1] https://alphatools.inoa.com.br/
"""

from typing import Any, Literal, Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class IAlphaToolsClient(Protocol):
	"""Structural protocol for any INOA Alpha Tools API client.

	Defines the minimum interface required to dispatch requests,
	retrieve fund information, and fetch portfolio quotes from the
	Alpha Tools REST API. Any class that implements ``generic_req``,
	``funds``, and ``quotes`` with compatible signatures satisfies
	this protocol without explicit inheritance.
	"""

	def generic_req(
		self,
		str_method: Literal["GET", "POST"],
		str_app: str,
		dict_params: dict[str, Any],
	) -> list[dict[str, Any]]:
		"""Make a generic API request with retry logic.

		Parameters
		----------
		str_method : Literal['GET', 'POST']
			HTTP method to use.
		str_app : str
			API endpoint path.
		dict_params : dict[str, Any]
			Parameters to send with the request.

		Returns
		-------
		list[dict[str, Any]]
			Parsed JSON response from the API.
		"""
		...

	def funds(self) -> pd.DataFrame:
		"""Retrieve funds information from the API.

		Returns
		-------
		pd.DataFrame
			DataFrame containing fund IDs, names, legal IDs and origin.
		"""
		...

	def quotes(self, list_ids: list[int]) -> pd.DataFrame:
		"""Retrieve quotes for given fund IDs within the configured date range.

		Parameters
		----------
		list_ids : list[int]
			List of fund IDs to query.

		Returns
		-------
		pd.DataFrame
			DataFrame containing quotes data with standardised column names.
		"""
		...
