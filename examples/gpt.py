"""Minimal usage example for GPT client."""

from unittest.mock import MagicMock, patch

from stpstone.utils.llms.gpt.gpt import GPT


def main() -> None:
	"""Run a single-turn prompt against a GPT model."""
	with patch("stpstone.utils.llms.gpt.gpt.OpenAI") as mock_openai:
		mock_client = MagicMock()
		mock_openai.return_value = mock_client
		mock_client.chat.completions.create.return_value = MagicMock()

		gpt = GPT(api_key="sk-placeholder", str_model="gpt-4o", int_max_tokens=50)
		response = gpt.run_prompt([("text", "What is 2 + 2?")])
		print(response)


if __name__ == "__main__":
	main()
