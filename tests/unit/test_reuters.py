"""Unit tests for Reuters API client.

Tests the functionality for fetching tokens and currency quotes from Reuters API,
including validation, error handling, and successful response scenarios.
"""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import RequestException

from stpstone.utils.providers.ww.reuters.reuters import Reuters


class TestReuters:
	"""Test cases for Reuters API client."""

	@pytest.fixture
	def reuters_client(self) -> Any:  # noqa ANN401: typing.Any is not allowed
		"""Fixture providing Reuters client instance.

		Returns
		-------
		Any
			Instance of Reuters class
		"""
		return Reuters()

	@pytest.fixture
	def mock_response(self) -> MagicMock:
		"""Fixture providing mocked response object.

		Returns
		-------
		MagicMock
			Mocked response with text attribute
		"""
		mock = MagicMock()
		mock.text = "mock_response_text"
		return mock

	# --------------------------
	# Validation Tests
	# --------------------------
	def test_validate_api_key_empty(
		self,
		reuters_client: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test validation raises error for empty API key.

		Verifies
		--------
		That empty API key raises ValueError with correct message.

		Parameters
		----------
		reuters_client : Any
			Instance of Reuters class

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="API key cannot be empty"):
			reuters_client._validate_api_key("")

	def test_validate_api_key_non_string(
		self,
		reuters_client: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test validation raises error for non-string API key.

		Verifies
		--------
		That non-string API key raises ValueError with correct message.

		Parameters
		----------
		reuters_client : Any
			Instance of Reuters class

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be of type"):
			reuters_client._validate_api_key(123)

	def test_validate_device_id_empty(
		self,
		reuters_client: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test validation raises error for empty device ID.

		Verifies
		--------
		That empty device ID raises ValueError with correct message.

		Parameters
		----------
		reuters_client : Any
			Instance of Reuters class

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Device ID cannot be empty"):
			reuters_client._validate_device_id("")

	def test_validate_device_id_non_string(
		self,
		reuters_client: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test validation raises error for non-string device ID.

		Verifies
		--------
		That non-string device ID raises ValueError with correct message.

		Parameters
		----------
		reuters_client : Any
			Instance of Reuters class

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be of type"):
			reuters_client._validate_device_id(123)

	def test_quotes_empty_currency(
		self,
		reuters_client: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test quotes raises error for empty currency.

		Verifies
		--------
		That empty currency raises ValueError with correct message.

		Parameters
		----------
		reuters_client : Any
			Instance of Reuters class

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Currency cannot be empty"):
			reuters_client.quotes("")

	def test_quotes_non_string_currency(
		self,
		reuters_client: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test quotes raises error for non-string currency.

		Verifies
		--------
		That non-string currency raises ValueError with correct message.

		Parameters
		----------
		reuters_client : Any
			Instance of Reuters class

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be of type"):
			reuters_client.quotes(123)

	@patch("stpstone.utils.providers.ww.reuters.reuters.request")
	def test_fetch_data_empty_response(
		self,
		mock_request: MagicMock,
		reuters_client: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test fetch data with empty response.

		Verifies
		--------
		That empty response raises ValueError with correct message.

		Parameters
		----------
		mock_request : MagicMock
			Mocked requests.request function
		reuters_client : Any
			Instance of Reuters class

		Returns
		-------
		None
		"""
		mock_response = MagicMock()
		mock_response.text = ""
		mock_request.return_value = mock_response

		with pytest.raises(ValueError, match="Failed to fetch data"):
			reuters_client.fetch_data("test_app")

	@patch("stpstone.utils.providers.ww.reuters.reuters.request")
	def test_fetch_data_request_failure(
		self,
		mock_request: MagicMock,
		reuters_client: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test fetch data when request fails.

		Verifies
		--------
		That request failure raises ValueError with correct message.

		Parameters
		----------
		mock_request : MagicMock
			Mocked requests.request function
		reuters_client : Any
			Instance of Reuters class

		Returns
		-------
		None
		"""
		mock_request.side_effect = RequestException("Connection error")

		with pytest.raises(ValueError, match="Failed to fetch data"):
			reuters_client.fetch_data("test_app")
