"""Unit tests for Operations — B3 LINE API market operations adapter.

Covers the ``app_request`` proxy, individual endpoint helpers, and the
``authorized_markets_instruments`` aggregation that iterates groups and
instruments. All I/O is mocked.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
"""

import pytest
from pytest_mock import MockerFixture

from stpstone.utils.providers.br.line_b3.operations import Operations


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_conn(mocker: MockerFixture) -> object:
	"""``MagicMock`` satisfying ``IConnectionApi``.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	Any
		Mocked connection.
	"""
	conn = mocker.MagicMock()
	conn.broker_code = "0001"
	conn.category_code = "1"
	conn.app_request.return_value = [{"result": "ok"}]
	return conn


@pytest.fixture
def operations(mock_conn: object) -> Operations:
	"""``Operations`` backed by a mocked connection.

	Parameters
	----------
	mock_conn : object
		Mocked ``IConnectionApi``.

	Returns
	-------
	Operations
		Ready-to-use adapter.
	"""
	return Operations(conn=mock_conn)


# --------------------------
# Tests
# --------------------------
class TestOperations:
	"""Test suite for ``Operations``."""

	def test_init_stores_connection(self, mock_conn: object) -> None:
		"""Constructor stores the injected connection.

		Parameters
		----------
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		adapter = Operations(conn=mock_conn)
		assert adapter._conn is mock_conn

	def test_app_request_proxies_to_conn(
		self,
		operations: Operations,
		mock_conn: object,
	) -> None:
		"""``app_request`` delegates every argument to the underlying connection.

		Parameters
		----------
		operations : Operations
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		operations.app_request(
			method="GET",
			app="/api/v1.0/test",
			dict_params={"k": "v"},
			bool_retry_if_error=True,
		)
		mock_conn.app_request.assert_called_once_with(
			method="GET",
			app="/api/v1.0/test",
			dict_params={"k": "v"},
			dict_payload=None,
			bool_parse_dict_params_data=False,
			bool_retry_if_error=True,
			float_secs_sleep=None,
			timeout=10,
		)

	def test_exchange_limits_calls_correct_endpoint(
		self,
		operations: Operations,
		mock_conn: object,
	) -> None:
		"""``exchange_limits`` calls the SPXI exchange-limits endpoint for the broker.

		Parameters
		----------
		operations : Operations
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		operations.exchange_limits()
		mock_conn.app_request.assert_called_once_with(
			method="GET",
			app="/api/v1.0/exchangeLimits/spxi/0001",
			bool_retry_if_error=True,
		)

	def test_groups_authorized_markets_calls_correct_endpoint(
		self,
		operations: Operations,
		mock_conn: object,
	) -> None:
		"""``groups_authorized_markets`` calls the authorized-markets endpoint.

		Parameters
		----------
		operations : Operations
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		operations.groups_authorized_markets()
		mock_conn.app_request.assert_called_once_with(
			method="GET",
			app="/api/v1.0/exchangeLimits/autorizedMarkets",
			bool_retry_if_error=True,
		)

	def test_intruments_per_group_sends_group_id_in_payload(
		self,
		operations: Operations,
		mock_conn: object,
	) -> None:
		"""``intruments_per_group`` POSTs with the group ID in the payload.

		Parameters
		----------
		operations : Operations
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		operations.intruments_per_group(group_id="G1")
		mock_conn.app_request.assert_called_once_with(
			method="POST",
			app="/api/v1.0/exchangeLimits/findInstruments",
			dict_payload={"authorizedMarketGroupId": "G1", "isLimitSetted": "true"},
			bool_parse_dict_params_data=True,
		)

	def test_authorized_markets_instruments_aggregates_groups_and_assets(
		self,
		operations: Operations,
		mock_conn: object,
	) -> None:
		"""``authorized_markets_instruments`` aggregates group data with asset details.

		The method calls ``groups_authorized_markets`` (first ``app_request`` call) and
		then ``intruments_per_group`` for each group (subsequent calls). The returned
		dict must be keyed by group ID with nested assets list.

		Parameters
		----------
		operations : Operations
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		markets_response = [{"id": "G1", "name": "Renda Variável"}]
		instruments_response = [
			{
				"instrumentSymbol": "PETR4",
				"instrumentAsset": "PETR",
				"limitSpci": 1000,
				"limitSpvi": 2000,
			}
		]
		mock_conn.app_request.side_effect = [markets_response, instruments_response]
		result = operations.authorized_markets_instruments()
		assert "G1" in result
		assert result["G1"]["name"] == "Renda Variável"
		assert len(result["G1"]["assets_associated"]) == 1
		asset = result["G1"]["assets_associated"][0]
		assert asset["instrument_symbol"] == "PETR4"
		assert asset["limit_spci"] == 1000
		assert asset["limit_spvi"] == 2000

	def test_authorized_markets_instruments_includes_option_limits_when_present(
		self,
		operations: Operations,
		mock_conn: object,
	) -> None:
		"""Option limit fields are included in the asset dict only when present in response.

		Parameters
		----------
		operations : Operations
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		mock_conn.app_request.side_effect = [
			[{"id": "G2", "name": "Opções"}],
			[
				{
					"instrumentSymbol": "PETR4F25",
					"instrumentAsset": "PETR",
					"limitSpci": 500,
					"limitSpvi": 500,
					"limitSpciOption": 250,
					"limitSpviOption": 250,
				}
			],
		]
		result = operations.authorized_markets_instruments()
		asset = result["G2"]["assets_associated"][0]
		assert asset["limit_spci_option"] == 250
		assert asset["limit_spvi_option"] == 250
