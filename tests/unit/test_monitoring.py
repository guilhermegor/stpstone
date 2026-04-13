"""Unit tests for Monitoring — B3 LINE API monitoring adapter.

Verifies that ``alerts()`` delegates to ``IConnectionApi.app_request`` with the
correct endpoint and retry flag. All I/O is mocked.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
"""


import pytest
from pytest_mock import MockerFixture

from stpstone.utils.providers.br.line_b3.monitoring import Monitoring


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
	conn.app_request.return_value = [{"alertId": "A1", "message": "test alert"}]
	return conn


@pytest.fixture
def monitoring(mock_conn: object) -> Monitoring:
	"""Provide a ``Monitoring`` instance backed by a mocked connection.

	Parameters
	----------
	mock_conn : object
		Mocked ``IConnectionApi``.

	Returns
	-------
	Monitoring
		Ready-to-use adapter.
	"""
	return Monitoring(conn=mock_conn)


# --------------------------
# Tests
# --------------------------
class TestMonitoring:
	"""Test suite for ``Monitoring``."""

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
		adapter = Monitoring(conn=mock_conn)
		assert adapter._conn is mock_conn

	def test_alerts_calls_get_with_correct_endpoint(
		self,
		monitoring: Monitoring,
		mock_conn: object,
	) -> None:
		"""``alerts`` issues a GET to the last-alerts endpoint with retry enabled.

		Parameters
		----------
		monitoring : Monitoring
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		monitoring.alerts()
		mock_conn.app_request.assert_called_once_with(
			method="GET",
			app="/api/v1.0/alert/lastalerts?filterRead=true",
			bool_retry_if_error=True,
		)

	def test_alerts_returns_connection_response(
		self,
		monitoring: Monitoring,
		mock_conn: object,
	) -> None:
		"""``alerts`` propagates the value returned by ``app_request``.

		Parameters
		----------
		monitoring : Monitoring
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		expected = [{"alertId": "A1", "message": "test alert"}]
		mock_conn.app_request.return_value = expected
		result = monitoring.alerts()
		assert result == expected
