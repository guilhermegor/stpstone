"""Unit tests for SystemEventManagement — B3 LINE API system event adapter.

Verifies that ``report`` builds the correct payload (including date formatting
in ``DD/MM/YYYY`` and time fields) and delegates to ``IConnectionApi.app_request``
with the right endpoint. All I/O is mocked.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
"""

from datetime import date

import pytest
from pytest_mock import MockerFixture

from stpstone.utils.providers.br.line_b3.system_event_management import (
	SystemEventManagement,
)


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
	conn.app_request.return_value = [{"eventId": "E1"}]
	return conn


@pytest.fixture
def system_event_management(mock_conn: object) -> SystemEventManagement:
	"""``SystemEventManagement`` backed by a mocked connection.

	Parameters
	----------
	mock_conn : object
		Mocked ``IConnectionApi``.

	Returns
	-------
	SystemEventManagement
		Ready-to-use adapter.
	"""
	return SystemEventManagement(conn=mock_conn)


# --------------------------
# Tests
# --------------------------
class TestSystemEventManagement:
	"""Test suite for ``SystemEventManagement``."""

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
		adapter = SystemEventManagement(conn=mock_conn)
		assert adapter._conn is mock_conn

	def test_report_calls_post_to_system_event_endpoint(
		self,
		system_event_management: SystemEventManagement,
		mock_conn: object,
	) -> None:
		"""``report`` issues a POST to ``/api/v1.0/systemEvent``.

		Parameters
		----------
		system_event_management : SystemEventManagement
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		system_event_management.report(
			date_start=date(2024, 1, 1),
			date_end=date(2024, 1, 31),
		)
		call_kwargs = mock_conn.app_request.call_args.kwargs
		assert call_kwargs["method"] == "POST"
		assert call_kwargs["app"] == "/api/v1.0/systemEvent"

	def test_report_formats_dates_as_dd_mm_yyyy(
		self,
		system_event_management: SystemEventManagement,
		mock_conn: object,
	) -> None:
		"""Dates in the payload are formatted as ``DD/MM/YYYY``.

		Parameters
		----------
		system_event_management : SystemEventManagement
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		system_event_management.report(
			date_start=date(2024, 3, 5),
			date_end=date(2024, 3, 28),
		)
		payload = mock_conn.app_request.call_args.kwargs["dict_payload"]
		assert payload["startDate"] == "05/03/2024"
		assert payload["endDate"] == "28/03/2024"

	def test_report_includes_participant_and_category_codes(
		self,
		system_event_management: SystemEventManagement,
		mock_conn: object,
	) -> None:
		"""Payload includes ``participantCode`` and ``categoryType`` from the connection.

		Parameters
		----------
		system_event_management : SystemEventManagement
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		system_event_management.report(
			date_start=date(2024, 1, 1),
			date_end=date(2024, 1, 31),
		)
		payload = mock_conn.app_request.call_args.kwargs["dict_payload"]
		assert payload["participantCode"] == 1
		assert payload["categoryType"] == 1

	def test_report_uses_default_times_when_not_specified(
		self,
		system_event_management: SystemEventManagement,
		mock_conn: object,
	) -> None:
		"""Default start and end times are ``00:00`` and ``23:59`` respectively.

		Parameters
		----------
		system_event_management : SystemEventManagement
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		system_event_management.report(
			date_start=date(2024, 1, 1),
			date_end=date(2024, 1, 31),
		)
		payload = mock_conn.app_request.call_args.kwargs["dict_payload"]
		assert payload["startTime"] == "00:00"
		assert payload["endTime"] == "23:59"

	def test_report_custom_times_override_defaults(
		self,
		system_event_management: SystemEventManagement,
		mock_conn: object,
	) -> None:
		"""Custom time strings replace the default ``00:00``/``23:59`` values.

		Parameters
		----------
		system_event_management : SystemEventManagement
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		system_event_management.report(
			date_start=date(2024, 1, 1),
			date_end=date(2024, 1, 1),
			str_start_time="09:00",
			str_sup_time="18:00",
		)
		payload = mock_conn.app_request.call_args.kwargs["dict_payload"]
		assert payload["startTime"] == "09:00"
		assert payload["endTime"] == "18:00"
