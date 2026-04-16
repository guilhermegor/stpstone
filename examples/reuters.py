"""Minimal usage example for Reuters API client."""

from unittest.mock import MagicMock, patch

from stpstone.utils.providers.ww.reuters.reuters import Reuters


def main() -> None:
	"""Fetch a currency quote from Reuters."""
	with patch("stpstone.utils.providers.ww.reuters.reuters.request") as mock_req:
		mock_resp = MagicMock()
		mock_resp.text = '{"bid": 5.10, "ask": 5.12}'
		mock_req.return_value = mock_resp

		client = Reuters()
		print(client.quotes("BRL="))


if __name__ == "__main__":
	main()
