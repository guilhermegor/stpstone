"""TypedDicts for ANBIMA Data API client.

References
----------
.. [1] https://developers.anbima.com.br/pt/
"""

from typing import TypedDict


class ReturnAccessToken(TypedDict):
	"""Typed dictionary for access token response.

	Parameters
	----------
	access_token : str
		Authentication token string.
	"""

	access_token: str
