"""Unit tests for AnbimaDataGen — ANBIMA Data API base client.

Covers authentication, token caching, generic HTTP dispatch, guard-clause
validation, and type enforcement without requiring real API credentials.

References
----------
.. [1] https://developers.anbima.com.br/pt/
"""

from typing import Union

import pytest
from pytest_mock import MockerFixture
from requests import exceptions

from stpstone.utils.parsers.json import JsonFiles
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.providers.br.anbimadata_api.anbimadata_api_gen import AnbimaDataGen


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response_ok(mocker: MockerFixture) -> object:
	"""Fixture providing a mocked successful HTTP response.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks.

	Returns
	-------
	object
		Mocked response with status 200 and ``{"access_token": "test_token"}`` JSON.
	"""
	mock_resp = mocker.Mock()
	mock_resp.status_code = 200
	mock_resp.raise_for_status.return_value = None
	mock_resp.json.return_value = {"access_token": "test_token"}
	return mock_resp


@pytest.fixture
def mock_requests(mocker: MockerFixture) -> object:
	"""Fixture patching ``requests.request`` inside ``anbimadata_api_gen``.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks.

	Returns
	-------
	object
		Patched ``request`` callable.
	"""
	return mocker.patch("stpstone.utils.providers.br.anbimadata_api.anbimadata_api_gen.request")


@pytest.fixture
def anbima_gen(mock_requests: object, mock_response_ok: object) -> AnbimaDataGen:
	"""Fixture providing an ``AnbimaDataGen`` instance with mocked HTTP.

	Parameters
	----------
	mock_requests : object
		Patched ``requests.request`` callable.
	mock_response_ok : object
		Mocked successful HTTP response.

	Returns
	-------
	AnbimaDataGen
		Initialised ``AnbimaDataGen`` instance.
	"""
	mock_requests.return_value = mock_response_ok
	return AnbimaDataGen(
		str_client_id="test_client",
		str_client_secret="test_secret",  # noqa S106
		str_env="dev",
		int_chunk=1000,
	)


# --------------------------
# Tests
# --------------------------
class TestAnbimaDataGen:
	"""Test suite for AnbimaDataGen."""

	def test_init_valid_inputs(self, mocker: MockerFixture) -> None:
		"""Initialisation with valid credentials sets attributes correctly.

		Parameters
		----------
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		mocker.patch(
			"stpstone.utils.providers.br.anbimadata_api.anbimadata_api_gen"
			".AnbimaDataGen.access_token",
			return_value={"access_token": "test_token"},
		)
		gen = AnbimaDataGen(
			str_client_id="client",
			str_client_secret="secret",  # noqa S106
			str_env="dev",
			int_chunk=500,
		)
		assert gen.str_client_id == "client"
		assert gen.str_client_secret == "secret"  # noqa S105
		assert gen.str_host == "https://api-sandbox.anbima.com.br/"
		assert gen.int_chunk == 500
		assert gen.str_token == "test_token"  # noqa S105

	@pytest.mark.parametrize(
		"client_id,client_secret,env,chunk,expected_error,match",
		[
			(123, "secret", "dev", 1000, TypeError, "must be of type"),
			("client", 123, "dev", 1000, TypeError, "must be of type"),
			("client", "secret", "invalid", 1000, TypeError, "must be one of"),
			("client", "secret", "dev", -1, ValueError, "Failed to retrieve access token"),
			("", "secret", "dev", 1000, ValueError, "cannot be empty"),
			("client", "", "dev", 1000, ValueError, "cannot be empty"),
		],
	)
	def test_init_invalid_inputs(
		self,
		client_id: Union[str, int],
		client_secret: Union[str, int],
		env: str,
		chunk: int,
		expected_error: type,
		match: str,
		mocker: MockerFixture,
	) -> None:
		"""Initialisation with invalid arguments raises the expected error.

		Parameters
		----------
		client_id : Union[str, int]
			Client ID value under test.
		client_secret : Union[str, int]
			Client secret value under test.
		env : str
			Environment string value under test.
		chunk : int
			Chunk size value under test.
		expected_error : type
			Expected exception type.
		match : str
			Substring expected in the exception message.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		mocker.patch(
			"stpstone.utils.providers.br.anbimadata_api.anbimadata_api_gen"
			".AnbimaDataGen.access_token",
			side_effect=ValueError("Failed to retrieve access token"),
		)
		with pytest.raises(expected_error, match=match):
			AnbimaDataGen(
				str_client_id=client_id,
				str_client_secret=client_secret,
				str_env=env,
				int_chunk=chunk,
			)

	def test_access_token_success(
		self,
		anbima_gen: AnbimaDataGen,
		mock_requests: object,
		mock_response_ok: object,
	) -> None:
		"""``access_token`` returns cached token without a second HTTP call.

		Parameters
		----------
		anbima_gen : AnbimaDataGen
			Initialised instance (token already cached during fixture setup).
		mock_requests : object
			Patched ``requests.request`` callable.
		mock_response_ok : object
			Mocked successful HTTP response.

		Returns
		-------
		None
		"""
		mock_requests.return_value = mock_response_ok
		result = anbima_gen.access_token()
		assert result == {"access_token": "test_token"}
		# The HTTP call happened once during fixture __init__; the second
		# call is served from cache, so the mock was called exactly once.
		mock_requests.assert_called_once_with(
			method="POST",
			url="https://api.anbima.com.br/oauth/access-token",
			headers={
				"Content-Type": "application/json",
				"Authorization": StrHandler().base64_encode("test_client", "test_secret"),
			},
			data=JsonFiles().dict_to_json({"grant_type": "client_credentials"}),
			timeout=(200, 200),
		)

	def test_access_token_returns_cache_on_http_failure(
		self,
		anbima_gen: AnbimaDataGen,
		mock_requests: object,
		mocker: MockerFixture,
	) -> None:
		"""``access_token`` returns cached value even when a fresh HTTP call would fail.

		Parameters
		----------
		anbima_gen : AnbimaDataGen
			Initialised instance (token already cached during fixture setup).
		mock_requests : object
			Patched ``requests.request`` callable.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		failing_resp = mocker.Mock()
		failing_resp.raise_for_status.side_effect = exceptions.HTTPError("HTTP error")
		failing_resp.status_code = 500
		mock_requests.return_value = failing_resp
		assert anbima_gen.access_token()["access_token"] == "test_token"  # noqa S105

	def test_generic_request_get_success(
		self,
		anbima_gen: AnbimaDataGen,
		mock_requests: object,
		mock_response_ok: object,
		mocker: MockerFixture,
	) -> None:
		"""GET request is dispatched with correct headers and returns parsed JSON.

		Parameters
		----------
		anbima_gen : AnbimaDataGen
			Initialised instance.
		mock_requests : object
			Patched ``requests.request`` callable.
		mock_response_ok : object
			Mocked successful HTTP response.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		mocker.patch.object(
			anbima_gen, "access_token", return_value={"access_token": "test_token"}
		)
		mock_response_ok.json.return_value = [{"key": "value"}]
		mock_requests.return_value = mock_response_ok
		mock_requests.reset_mock()
		result = anbima_gen.generic_request(str_app="test_endpoint", str_method="GET")
		assert result == [{"key": "value"}]
		mock_requests.assert_called_once_with(
			method="GET",
			url="https://api-sandbox.anbima.com.br/test_endpoint",
			headers={
				"accept": "application/json",
				"client_id": "test_client",
				"access_token": "test_token",
			},
			params=None,
			data=None,
			timeout=(200, 200),
		)

	def test_generic_request_invalid_method_raises(
		self,
		anbima_gen: AnbimaDataGen,
	) -> None:
		"""An unsupported HTTP verb raises ``TypeError``.

		Parameters
		----------
		anbima_gen : AnbimaDataGen
			Initialised instance.

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="str_method must be one of"):
			anbima_gen.generic_request(str_app="test_endpoint", str_method="PUT")

	def test_generic_request_http_failure_raises(
		self,
		anbima_gen: AnbimaDataGen,
		mock_requests: object,
		mocker: MockerFixture,
	) -> None:
		"""An HTTP error is converted to ``ValueError`` with the original message.

		Parameters
		----------
		anbima_gen : AnbimaDataGen
			Initialised instance.
		mock_requests : object
			Patched ``requests.request`` callable.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		failing_resp = mocker.Mock()
		failing_resp.raise_for_status.side_effect = exceptions.HTTPError("Error")
		mock_requests.return_value = failing_resp
		with pytest.raises(ValueError, match="API request failed: Error"):
			anbima_gen.generic_request(str_app="test_endpoint", str_method="GET")
