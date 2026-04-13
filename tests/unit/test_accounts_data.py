"""Unit tests for AccountsData — B3 LINE API account data adapter.

Verifies that each method delegates to ``IConnectionApi.app_request`` with the
correct endpoint, HTTP method, and parameter shapes. All network I/O is replaced
by a ``MagicMock`` so no real API calls are made.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
"""

import pytest
from pytest_mock import MockerFixture

from stpstone.utils.providers.br.line_b3.accounts_data import AccountsData


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_conn(mocker: MockerFixture) -> object:
	"""Provide a ``MagicMock`` that satisfies ``IConnectionApi``.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	Any
		Mocked connection object.
	"""
	conn = mocker.MagicMock()
	conn.broker_code = "0001"
	conn.category_code = "1"
	conn.app_request.return_value = [{"result": "ok"}]
	return conn


@pytest.fixture
def accounts_data(mock_conn: object) -> AccountsData:
	"""Provide an ``AccountsData`` instance backed by a mocked connection.

	Parameters
	----------
	mock_conn : object
		Mocked ``IConnectionApi``.

	Returns
	-------
	AccountsData
		Ready-to-use adapter instance.
	"""
	return AccountsData(conn=mock_conn)


# --------------------------
# Tests
# --------------------------
class TestAccountsData:
	"""Test suite for ``AccountsData``."""

	def test_init_creates_instance(self, mock_conn: object) -> None:
		"""Constructor stores the injected connection adapter.

		Parameters
		----------
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		adapter = AccountsData(conn=mock_conn)
		assert adapter._conn is mock_conn

	def test_client_infos_calls_get_with_correct_params(
		self,
		accounts_data: AccountsData,
		mock_conn: object,
	) -> None:
		"""``client_infos`` issues a GET to ``/api/v1.0/account`` with all required params.

		Parameters
		----------
		accounts_data : AccountsData
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		accounts_data.client_infos(account_code="ACC001")
		mock_conn.app_request.assert_called_once_with(
			method="GET",
			app="/api/v1.0/account",
			dict_params={
				"participantCode": "0001",
				"pnpCode": "1",
				"accountCode": "ACC001",
			},
			bool_retry_if_error=True,
		)

	def test_spxi_get_calls_get_with_account_id_in_path(
		self,
		accounts_data: AccountsData,
		mock_conn: object,
	) -> None:
		"""``spxi_get`` issues a GET to the correct SPXI endpoint for the given account.

		Parameters
		----------
		accounts_data : AccountsData
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		accounts_data.spxi_get(account_id="ACC999")
		mock_conn.app_request.assert_called_once_with(
			method="GET",
			app="/api/v1.0/account/ACC999/lmt/spxi",
			dict_params={"accId": "ACC999"},
		)

	def test_spxi_instrument_post_calls_post_with_payload(
		self,
		accounts_data: AccountsData,
		mock_conn: object,
	) -> None:
		"""``spxi_instrument_post`` issues a POST with the provided payload.

		Parameters
		----------
		accounts_data : AccountsData
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		payload = [{"symbol": "PETR4", "limitSpci": 100}]
		accounts_data.spxi_instrument_post(account_id="ACC1", dict_payload=payload)
		mock_conn.app_request.assert_called_once_with(
			method="POST",
			app="/api/v1.0/account/ACC1/lmt/spxi",
			dict_payload=payload,
			bool_parse_dict_params_data=True,
		)

	def test_spxi_tmox_global_metrics_remove_calls_delete(
		self,
		accounts_data: AccountsData,
		mock_conn: object,
	) -> None:
		"""``spxi_tmox_global_metrics_remove`` issues DELETE to the limits endpoint.

		Parameters
		----------
		accounts_data : AccountsData
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		accounts_data.spxi_tmox_global_metrics_remove(account_id="ACC2")
		mock_conn.app_request.assert_called_once_with(
			method="DELETE",
			app="/api/v1.0/account/ACC2/lmt",
			dict_params={"accId": "ACC2"},
		)

	def test_specific_global_metric_remotion_calls_delete_v2(
		self,
		accounts_data: AccountsData,
		mock_conn: object,
	) -> None:
		"""``specific_global_metric_remotion`` uses the v2.0 endpoint with metric param.

		Parameters
		----------
		accounts_data : AccountsData
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		accounts_data.specific_global_metric_remotion(account_id="ACC3", metric="TMOC")
		mock_conn.app_request.assert_called_once_with(
			method="DELETE",
			app="/api/v2.0/account/ACC3/lmt",
			dict_params={"accId": "ACC3", "metric": "TMOC"},
		)
