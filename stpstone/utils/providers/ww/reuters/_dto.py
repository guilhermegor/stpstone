"""TypedDicts for Reuters API client.

References
----------
.. [1] https://www.reuters.com/markets/currencies
"""

from typing import TypedDict


class ReturnToken(TypedDict):
	"""Typed dictionary for token response.

	Attributes
	----------
	access_token : str
		Access token for API authentication.
	token_type : str
		Type of the token.
	expires_in : int
		Expiration time in seconds.
	"""

	access_token: str
	token_type: str
	expires_in: int
