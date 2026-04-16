"""Unit tests for ProfilesData — B3 LINE API risk-profile data adapter.

Verifies that all seven public methods delegate to ``IConnectionApi.app_request``
with the correct endpoints and parameter shapes. All I/O is mocked.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
"""

import pytest
from pytest_mock import MockerFixture

from stpstone.utils.providers.br.line_b3.profiles_data import ProfilesData


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
def profiles_data(mock_conn: object) -> ProfilesData:
	"""``ProfilesData`` backed by a mocked connection.

	Parameters
	----------
	mock_conn : object
		Mocked ``IConnectionApi``.

	Returns
	-------
	ProfilesData
		Ready-to-use adapter.
	"""
	return ProfilesData(conn=mock_conn)


# --------------------------
# Tests
# --------------------------
class TestProfilesData:
	"""Test suite for ``ProfilesData``."""

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
		adapter = ProfilesData(conn=mock_conn)
		assert adapter._conn is mock_conn

	def test_risk_profile_calls_get_endpoint(
		self,
		profiles_data: ProfilesData,
		mock_conn: object,
	) -> None:
		"""``risk_profile`` issues a GET to ``/api/v1.0/riskProfile``.

		Parameters
		----------
		profiles_data : ProfilesData
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		profiles_data.risk_profile()
		mock_conn.app_request.assert_called_once_with(method="GET", app="/api/v1.0/riskProfile")

	def test_entities_associated_profile_passes_id_and_participant_params(
		self,
		profiles_data: ProfilesData,
		mock_conn: object,
	) -> None:
		"""``entities_associated_profile`` sends broker and category codes alongside profile id.

		Parameters
		----------
		profiles_data : ProfilesData
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		profiles_data.entities_associated_profile(id_profile="P5")
		mock_conn.app_request.assert_called_once_with(
			method="GET",
			app="/api/v1.0/riskProfile/enty",
			dict_params={"id": "P5", "participantCode": "0001", "pnpCode": "1"},
			bool_retry_if_error=True,
		)

	def test_profile_global_limits_get_uses_profile_id_in_path(
		self,
		profiles_data: ProfilesData,
		mock_conn: object,
	) -> None:
		"""``profile_global_limits_get`` encodes profile ID in the URL path.

		Parameters
		----------
		profiles_data : ProfilesData
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		profiles_data.profile_global_limits_get(prof_id="P7")
		mock_conn.app_request.assert_called_once_with(
			method="GET", app="/api/v1.0/riskProfile/P7/lmt"
		)

	def test_profile_market_limits_get_uses_mkta_suffix(
		self,
		profiles_data: ProfilesData,
		mock_conn: object,
	) -> None:
		"""``profile_market_limits_get`` appends ``/lmt/mkta`` to the profile path.

		Parameters
		----------
		profiles_data : ProfilesData
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		profiles_data.profile_market_limits_get(prof_id="P8")
		mock_conn.app_request.assert_called_once_with(
			method="GET",
			app="/api/v1.0/riskProfile/P8/lmt/mkta",
			bool_retry_if_error=True,
		)

	def test_profile_spxi_limits_get_uses_spxi_suffix(
		self,
		profiles_data: ProfilesData,
		mock_conn: object,
	) -> None:
		"""``profile_spxi_limits_get`` appends ``/lmt/spxi`` to the profile path.

		Parameters
		----------
		profiles_data : ProfilesData
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		profiles_data.profile_spxi_limits_get(prof_id="P9")
		mock_conn.app_request.assert_called_once_with(
			method="GET", app="/api/v1.0/riskProfile/P9/lmt/spxi"
		)

	def test_profile_tmox_limits_get_uses_tmox_suffix(
		self,
		profiles_data: ProfilesData,
		mock_conn: object,
	) -> None:
		"""``profile_tmox_limits_get`` appends ``/lmt/tmox`` to the profile path.

		Parameters
		----------
		profiles_data : ProfilesData
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		profiles_data.profile_tmox_limits_get(prof_id="P10")
		mock_conn.app_request.assert_called_once_with(
			method="GET", app="/api/v1.0/riskProfile/P10/lmt/tmox"
		)

	def test_spxi_instrument_post_sends_payload_to_tmox_endpoint(
		self,
		profiles_data: ProfilesData,
		mock_conn: object,
	) -> None:
		"""``spxi_instrument_post`` POSTs to the TMOX limits endpoint with the payload.

		Parameters
		----------
		profiles_data : ProfilesData
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		payload = [{"symbol": "PETR4", "limitTmoc": 500}]
		profiles_data.spxi_instrument_post(prof_id="P11", dict_payload=payload)
		mock_conn.app_request.assert_called_once_with(
			method="POST",
			app="/api/v1.0/riskProfile/P11/lmt/tmox",
			dict_payload=payload,
			bool_parse_dict_params_data=True,
			bool_retry_if_error=True,
		)
