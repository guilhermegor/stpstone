"""Unit tests for B3TradableSecurityList ingestion class."""

from collections.abc import Callable
from io import StringIO
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.exchange.b3_tradable_security_list import (
	B3TradableSecurityList,
)


# --------------------------
# Module Utilities
# --------------------------
def create_mock_response(content: bytes = b"test content") -> MagicMock:
	"""Create a mock response object.

	Parameters
	----------
	content : bytes
		Response content, by default b"test content"

	Returns
	-------
	MagicMock
		Mock response object
	"""
	response = MagicMock(spec=Response)
	response.content = content
	response.status_code = 200
	response.raise_for_status = MagicMock()
	return response


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture(autouse=True)
def mock_fast_operations(mocker: MockerFixture) -> dict[str, MagicMock]:
	"""Auto-mock expensive operations for all tests.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	dict[str, MagicMock]
		Dictionary of mock objects
	"""

	def bypass_backoff(
		*args: Any,  # noqa ANN401: typing.Any is not allowed
		**kwargs: Any,  # noqa ANN401: typing.Any is not allowed
	) -> Callable:
		"""Bypass backoff decorator.

		Parameters
		----------
		*args : Any
			Variable-length argument list
		**kwargs : Any
			Arbitrary keyword arguments

		Returns
		-------
		Callable
			Function unchanged
		"""

		def decorator(func: Callable) -> Callable:
			"""Return the function unchanged.

			Parameters
			----------
			func : Callable
				Function to be decorated

			Returns
			-------
			Callable
				Function unchanged
			"""
			return func

		return decorator

	mocks = {
		"requests_get": mocker.patch("requests.get"),
		"time_sleep": mocker.patch("time.sleep"),
		"backoff_on_exception": mocker.patch("backoff.on_exception", side_effect=bypass_backoff),
		"subprocess_run": mocker.patch("subprocess.run"),
		"shutil_rmtree": mocker.patch("shutil.rmtree"),
		"tempfile_mkdtemp": mocker.patch("tempfile.mkdtemp"),
	}

	mocks["requests_get"].return_value = create_mock_response()
	mocks["subprocess_run"].return_value = MagicMock(returncode=0, stdout="", stderr="")
	mocks["tempfile_mkdtemp"].return_value = "/tmp/test_dir"  # noqa S108: probable insecure usage of temporary file or directory

	return mocks


# --------------------------
# Tests for B3TradableSecurityList
# --------------------------
class TestB3TradableSecurityList:
	"""Test cases for B3TradableSecurityList class."""

	def test_init_url_pattern(self) -> None:
		"""Test initialization with correct URL pattern.

		Verifies
		--------
		- Correct URL pattern is used
		- File extension matches expected type

		Returns
		-------
		None
		"""
		instance = B3TradableSecurityList()

		assert "pesquisapregao/download?filelist=SecurityList" in instance.url
		assert instance.url.endswith(".zip")

	def test_run_parameter_validation(self) -> None:
		"""Test run method parameter type validation.

		Verifies
		--------
		- TypeError raised for invalid timeout type
		- TypeError raised for invalid bool_verify type

		Returns
		-------
		None
		"""
		instance = B3TradableSecurityList()

		with pytest.raises(TypeError):
			instance.run(timeout="invalid")

		with pytest.raises(TypeError):
			instance.run(bool_verify="not_bool")

	def test_has_transform_data_method(self) -> None:
		"""Test that transform_data method is implemented.

		Verifies
		--------
		- Method exists
		- Method is callable

		Returns
		-------
		None
		"""
		instance = B3TradableSecurityList()

		assert hasattr(instance, "transform_data")
		assert callable(instance.transform_data)

	def test_run_pipeline_executes(
		self,
		mock_fast_operations: dict[str, MagicMock],
	) -> None:
		"""Test that run method executes the full pipeline.

		Verifies
		--------
		- Pipeline stages are called in order
		- DataFrame is returned when no db session

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3TradableSecurityList()

		with (
			patch.object(instance, "get_response") as mock_get_response,
			patch.object(instance, "parse_raw_file") as mock_parse,
			patch.object(instance, "transform_data") as mock_transform,
			patch.object(instance, "standardize_dataframe") as mock_standardize,
		):
			mock_get_response.return_value = create_mock_response()
			mock_parse.return_value = (StringIO("test,data"), "test.csv")
			mock_transform.return_value = pd.DataFrame({"col": [1], "FILE_NAME": ["test.csv"]})
			mock_standardize.return_value = pd.DataFrame({"COL": [1]})

			result = instance.run()

			mock_get_response.assert_called_once()
			mock_parse.assert_called_once()
			mock_transform.assert_called_once()
			mock_standardize.assert_called_once()
			assert isinstance(result, pd.DataFrame)
