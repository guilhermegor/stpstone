"""Unit tests for ConnectionApi — B3 LINE API connection/authentication adapter.

Covers credential validation, URL validation, auth-header retrieval, access-token
acquisition, and the generic ``app_request`` dispatcher. All HTTP calls are mocked
so no real network traffic is generated.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
"""

import pytest
from pytest_mock import MockerFixture

from stpstone.utils.providers.br.line_b3.connection_api import ConnectionApi


# --------------------------
# Helpers
# --------------------------
def _make_ok_auth_resp(mocker: MockerFixture) -> object:
	"""Build a mock HTTP response for a successful auth-header GET.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture used to create the mock object.

	Returns
	-------
	object
		Mock with ``status_code=200`` and ``json()`` returning ``{"header": ...}``.
	"""
	mock = mocker.MagicMock()
	mock.status_code = 200
	mock.json.return_value = {"header": "dGVzdC1oZWFkZXI="}
	return mock


def _make_ok_token_resp(mocker: MockerFixture) -> object:
	"""Build a mock HTTP response for a successful token POST.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture used to create the mock object.

	Returns
	-------
	object
		Mock with ``status_code=200`` and ``json()`` returning token fields.
	"""
	mock = mocker.MagicMock()
	mock.status_code = 200
	mock.raise_for_status.return_value = None
	mock.json.return_value = {
		"access_token": "test-bearer-token",
		"refresh_token": "test-refresh-token",
		"expires_in": 9999,
	}
	return mock


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_request(mocker: MockerFixture) -> object:
	"""Patch ``requests.request`` inside connection_api module.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	object
		Patched callable.
	"""
	return mocker.patch("stpstone.utils.providers.br.line_b3.connection_api.request")


@pytest.fixture
def connection_api(mocker: MockerFixture, mock_request: object) -> ConnectionApi:
	"""Provide an initialised ``ConnectionApi`` with all HTTP mocked.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.
	mock_request : object
		Patched ``requests.request``.

	Returns
	-------
	ConnectionApi
		Ready-to-use instance.
	"""
	mock_request.side_effect = [  # type: ignore[attr-defined]
		_make_ok_auth_resp(mocker),
		_make_ok_token_resp(mocker),
	]
	return ConnectionApi(
		client_id="test-client",
		client_secret="test-secret",  # noqa: S106
		broker_code="0001",
		category_code="1",
	)


# --------------------------
# Validation tests (no network needed)
# --------------------------
class TestConnectionApiValidation:
	"""Tests for credential and URL guard-clauses."""

	def test_validate_credentials_empty_client_id_raises_value_error(self) -> None:
		"""Empty client_id triggers ValueError before any network call.

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="All credentials must be non-empty"):
			ConnectionApi("", "secret", "0001", "1")

	def test_validate_credentials_empty_secret_raises_value_error(self) -> None:
		"""Empty client_secret triggers ValueError.

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="All credentials must be non-empty"):
			ConnectionApi("client", "", "0001", "1")

	def test_validate_url_http_scheme_raises_value_error(self) -> None:
		"""Non-HTTPS URL triggers ValueError.

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="URL must start with https://"):
			ConnectionApi("client", "secret", "0001", "1", "http://bad.com")  # noqa: S106

	def test_validate_url_empty_raises_value_error(self) -> None:
		"""Empty URL triggers ValueError.

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="URL cannot be empty"):
			ConnectionApi("client", "secret", "0001", "1", "")  # noqa: S106


# --------------------------
# Initialisation tests
# --------------------------
class TestConnectionApiInit:
	"""Tests for successful initialisation."""

	def test_init_valid_credentials_stores_attributes(
		self,
		connection_api: ConnectionApi,
	) -> None:
		"""Valid credentials are stored on the instance after __init__.

		Parameters
		----------
		connection_api : ConnectionApi
			Mocked instance.

		Returns
		-------
		None
		"""
		assert connection_api.client_id == "test-client"
		assert connection_api.broker_code == "0001"
		assert connection_api.category_code == "1"
		assert connection_api.token == "test-bearer-token"  # noqa: S105


# --------------------------
# auth_header tests
# --------------------------
class TestAuthHeader:
	"""Tests for ``auth_header``."""

	def test_auth_header_success_returns_header_string(
		self,
		connection_api: ConnectionApi,
		mock_request: object,
		mocker: MockerFixture,
	) -> None:
		"""Successful GET returns the ``header`` field from the JSON response.

		Parameters
		----------
		connection_api : ConnectionApi
			Mocked instance.
		mock_request : object
			Patched ``requests.request``.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		mock_request.side_effect = None  # type: ignore[attr-defined]
		mock_request.return_value = _make_ok_auth_resp(mocker)  # type: ignore[attr-defined]
		result = connection_api.auth_header()
		assert result == "dGVzdC1oZWFkZXI="

	def test_auth_header_all_retries_exhausted_raises_value_error(
		self,
		connection_api: ConnectionApi,
		mock_request: object,
		mocker: MockerFixture,
	) -> None:
		"""All responses non-200 exhaust retries and raise ValueError.

		Parameters
		----------
		connection_api : ConnectionApi
			Mocked instance.
		mock_request : object
			Patched ``requests.request``.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		failing = mocker.MagicMock()
		failing.status_code = 500
		mock_request.side_effect = None  # type: ignore[attr-defined]
		mock_request.return_value = failing  # type: ignore[attr-defined]
		with pytest.raises(ValueError, match="Maximum retry attempts exceeded"):
			connection_api.auth_header(int_max_retries=0)


# --------------------------
# app_request tests
# --------------------------
class TestAppRequest:
	"""Tests for ``app_request``."""

	def test_app_request_get_without_retry_returns_json(
		self,
		connection_api: ConnectionApi,
		mock_request: object,
		mocker: MockerFixture,
	) -> None:
		"""GET without retry returns the parsed JSON body.

		Parameters
		----------
		connection_api : ConnectionApi
			Mocked instance.
		mock_request : object
			Patched ``requests.request``.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		resp = mocker.MagicMock()
		resp.status_code = 200
		resp.raise_for_status.return_value = None
		resp.json.return_value = [{"id": "42"}]
		mock_request.side_effect = None  # type: ignore[attr-defined]
		mock_request.return_value = resp  # type: ignore[attr-defined]

		result = connection_api.app_request(method="GET", app="/api/v1.0/test")
		assert result == [{"id": "42"}]

	def test_app_request_non_json_response_returns_status_code(
		self,
		connection_api: ConnectionApi,
		mock_request: object,
		mocker: MockerFixture,
	) -> None:
		"""Response with no JSON body returns the HTTP status code instead.

		Parameters
		----------
		connection_api : ConnectionApi
			Mocked instance.
		mock_request : object
			Patched ``requests.request``.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		resp = mocker.MagicMock()
		resp.status_code = 204
		resp.raise_for_status.return_value = None
		resp.json.side_effect = Exception("No JSON")
		mock_request.side_effect = None  # type: ignore[attr-defined]
		mock_request.return_value = resp  # type: ignore[attr-defined]

		result = connection_api.app_request(method="DELETE", app="/api/v1.0/item/1")
		assert result == 204

	def test_app_request_with_retry_stops_on_200(
		self,
		connection_api: ConnectionApi,
		mock_request: object,
		mocker: MockerFixture,
	) -> None:
		"""With ``bool_retry_if_error=True``, loop stops immediately on 200.

		Parameters
		----------
		connection_api : ConnectionApi
			Mocked instance.
		mock_request : object
			Patched ``requests.request``.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		resp = mocker.MagicMock()
		resp.status_code = 200
		resp.raise_for_status.return_value = None
		resp.json.return_value = [{"ok": True}]
		mock_request.side_effect = None  # type: ignore[attr-defined]
		mock_request.return_value = resp  # type: ignore[attr-defined]
		mock_request.reset_mock()  # type: ignore[attr-defined]

		result = connection_api.app_request(
			method="GET",
			app="/api/v1.0/resource",
			bool_retry_if_error=True,
			float_secs_sleep=0.0,
		)
		assert result == [{"ok": True}]
		mock_request.assert_called_once()  # type: ignore[attr-defined]
