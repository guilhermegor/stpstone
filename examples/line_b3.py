"""Minimal usage example for the B3 LINE API client.

Demonstrates how to compose the injected adapters:
    conn = ConnectionApi(...)
    ops  = Operations(conn)
    res  = Resources(ops)
    accs = AccountsData(conn)
"""

from unittest.mock import MagicMock, patch

from stpstone.utils.providers.br.line_b3.accounts_data import AccountsData
from stpstone.utils.providers.br.line_b3.connection_api import ConnectionApi
from stpstone.utils.providers.br.line_b3.operations import Operations
from stpstone.utils.providers.br.line_b3.resources import Resources


def main() -> None:
	"""Build the adapter graph with mocked HTTP and run a sample call."""
	mock_resp = MagicMock()
	mock_resp.status_code = 200
	mock_resp.json.return_value = {"header": "mock_header"}
	mock_resp.raise_for_status = MagicMock()

	with (
		patch(
			"stpstone.utils.providers.br.line_b3.connection_api.request",
			return_value=mock_resp,
		),
	):
		conn = ConnectionApi(
			client_id="placeholder_id",
			client_secret="placeholder_secret",
			broker_code="1234",
			category_code="5678",
		)
		ops = Operations(conn=conn)
		res = Resources(ops=ops)
		accs = AccountsData(conn=conn)

		print(f"ConnectionApi token: {conn.token!r}")
		print(f"Operations: {ops}")
		print(f"Resources: {res}")
		print(f"AccountsData: {accs}")


if __name__ == "__main__":
	main()
