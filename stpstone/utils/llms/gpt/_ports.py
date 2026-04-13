"""Protocols for GPT client.

References
----------
.. [1] https://platform.openai.com/docs/guides/gpt
"""

from typing import Protocol, runtime_checkable

from openai.types.chat import ChatCompletion


@runtime_checkable
class IGPT(Protocol):
	"""Structural protocol for any GPT-compatible client."""

	def run_prompt(self, list_tuple: list[tuple]) -> ChatCompletion: ...
