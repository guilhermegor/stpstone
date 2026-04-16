"""TypedDicts for GPT client.

References
----------
.. [1] https://platform.openai.com/docs/guides/gpt
"""

from typing import TypedDict


class ReturnRunPrompt(TypedDict):
	"""Typed dictionary for run_prompt message envelope.

	Attributes
	----------
	role : str
		Role of the message sender (``"user"``, ``"system"``, or ``"assistant"``).
	content : list[dict]
		List of message content dictionaries.
	"""

	role: str
	content: list[dict]
