"""TypedDicts for INOA Alpha Tools API client.

References
----------
.. [1] https://alphatools.inoa.com.br/
"""

from typing import Any, TypedDict


class ReturnGenericReq(TypedDict):
	"""Typed dictionary for generic request response.

	Parameters
	----------
	items : list[dict[str, Any]]
		List of items returned from the API.
	"""

	items: list[dict[str, Any]]
