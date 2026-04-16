"""Unit tests for Professional — B3 LINE API professional data adapter.

Verifies that ``professional_code_get`` and ``professional_historic_position``
delegate to ``IConnectionApi.app_request`` with the correct method, endpoint,
and payload (including date formatting). All I/O is mocked.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
"""

from datetime import date

import pytest
from pytest_mock import MockerFixture

from stpstone.utils.providers.br.line_b3.professional import Professional


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
def professional(mock_conn: object) -> Professional:
	"""``Professional`` backed by a mocked connection.

	Parameters
	----------
	mock_conn : object
		Mocked ``IConnectionApi``.

	Returns
	-------
	Professional
		Ready-to-use adapter.
	"""
	return Professional(conn=mock_conn)


# --------------------------
# Tests
# --------------------------
class TestProfessional:
	"""Test suite for ``Professional``."""

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
		adapter = Professional(conn=mock_conn)
		assert adapter._conn is mock_conn

	def test_professional_code_get_calls_get_with_participant_params(
		self,
		professional: Professional,
		mock_conn: object,
	) -> None:
		"""``professional_code_get`` sends a GET with broker and category codes.

		Parameters
		----------
		professional : Professional
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		professional.professional_code_get()
		mock_conn.app_request.assert_called_once_with(
			method="GET",
			app="/api/v1.0/operationsProfessionalParticipant/code",
			dict_params={"participantCode": "0001", "pnpCode": "1"},
		)

	def test_professional_historic_position_formats_dates_correctly(
		self,
		professional: Professional,
		mock_conn: object,
	) -> None:
		"""``professional_historic_position`` formats both dates as ``YYYY-MM-DD``.

		Parameters
		----------
		professional : Professional
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		professional.professional_historic_position(
			professional_code="TRADER01",
			date_start=date(2024, 1, 15),
			date_end=date(2024, 1, 31),
		)
		call_kwargs = mock_conn.app_request.call_args.kwargs
		payload = call_kwargs["dict_payload"]
		assert payload["registryDateStart"] == "2024-01-15"
		assert payload["registryDateEnd"] == "2024-01-31"

	def test_professional_historic_position_includes_trader_and_broker_codes(
		self,
		professional: Professional,
		mock_conn: object,
	) -> None:
		"""Payload includes ``traderCode``, ``ownerBrokerCode``, and ``ownerCategoryType``.

		Parameters
		----------
		professional : Professional
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		professional.professional_historic_position(
			professional_code="TRADER42",
			date_start=date(2024, 3, 1),
			date_end=date(2024, 3, 31),
		)
		payload = mock_conn.app_request.call_args.kwargs["dict_payload"]
		assert payload["traderCode"] == "TRADER42"
		assert payload["ownerBrokerCode"] == 1
		assert payload["ownerCategoryType"] == 1

	def test_professional_historic_position_uses_full_url_endpoint(
		self,
		professional: Professional,
		mock_conn: object,
	) -> None:
		"""``professional_historic_position`` uses the full URL (not a relative path).

		Parameters
		----------
		professional : Professional
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		professional.professional_historic_position(
			professional_code="T1",
			date_start=date(2024, 1, 1),
			date_end=date(2024, 1, 2),
		)
		call_kwargs = mock_conn.app_request.call_args.kwargs
		assert call_kwargs["app"] == (
			"https://api.line.trd.cert.bvmfnet.com.br/api/v2.0/position/hstry"
		)
		assert call_kwargs["method"] == "POST"
