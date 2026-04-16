"""Unit tests for BrazilGeo class in geographical information utilities module.

Tests cover normal operation, edge cases, error handling, input validation,
and fallback behaviors in retrieving Brazilian states and zip code data using APIs.
"""

from typing import Any, NoReturn
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import HTTPError, Timeout

from stpstone.utils.geography.geo_br import BrazilGeo, ReturnStates, ReturnZipCode


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def geo_br() -> BrazilGeo:
	"""Fixture providing an instance of BrazilGeo class.

	Returns
	-------
	BrazilGeo
		An initialized BrazilGeo instance
	"""
	return BrazilGeo()


@pytest.fixture
def sample_states_response() -> list[ReturnStates]:
	"""Fixture providing a sample API response for states.

	Returns
	-------
	list[ReturnStates]
		A list containing one sample state dictionary
	"""
	return [
		{
			"id": 33,
			"sigla": "RJ",
			"nome": "Rio de Janeiro",
			"regiao": {"id": 3, "sigla": "SE", "nome": "Sudeste"},
		}
	]


@pytest.fixture
def sample_zip_code_response() -> ReturnZipCode:
	"""Fixture providing a sample zip code API response.

	Returns
	-------
	ReturnZipCode
		A dictionary representing a successful zip code location info
	"""
	return {
		"cep": "20040030",
		"address_type": "Rua",
		"address_name": "Saúde",
		"address": "Rua Saúde",
		"state": "RJ",
		"district": "Centro",
		"lat": "-22.9083",
		"long": "-43.1964",
		"city": "Rio de Janeiro",
		"city_ibge": "3304557",
		"ddd": "21",
	}


# --------------------------
# Tests for _validate_zip_codes
# --------------------------
def test_validate_zip_codes_valid_list(geo_br: BrazilGeo) -> None:
	"""Test _validate_zip_codes accepts valid non-empty strings.

	Parameters
	----------
	geo_br : BrazilGeo
		Instance of the class being tested

	Returns
	-------
	None
	"""
	geo_br._validate_zip_codes(["12345678", "87654321"])


@pytest.mark.parametrize(
	"invalid_list,expected_error,match_msg",
	[
		([], ValueError, "Zip code list cannot be empty"),
		(["   "], ValueError, "Zip code cannot be empty"),
		(["1234", " "], ValueError, "Zip code cannot be empty"),
	],
)
def test_validate_zip_codes_invalid(
	geo_br: BrazilGeo, invalid_list: list[str], expected_error: type[Exception], match_msg: str
) -> None:
	"""Test _validate_zip_codes raises ValueError for empty list or empty strings.

	Parameters
	----------
	geo_br : BrazilGeo
		Instance of the class being tested
	invalid_list : list[str]
		List of zip codes to validate
	expected_error : type[Exception]
		Expected error type to be raised
	match_msg : str
		Expected error message to match

	Returns
	-------
	None
	"""
	with pytest.raises(expected_error, match=match_msg):
		geo_br._validate_zip_codes(invalid_list)


# --------------------------
# Tests for states method
# --------------------------
@patch("stpstone.utils.geography.geo_br.requests.get")
def test_states_success(
	mock_get: MagicMock, geo_br: BrazilGeo, sample_states_response: list[ReturnStates]
) -> None:
	"""Test that states() successfully returns states data when API responds correctly.

	Parameters
	----------
	mock_get : MagicMock
		Mocked requests.get function
	geo_br : BrazilGeo
		Instance of the class being tested
	sample_states_response : list[ReturnStates]
		Sample API response data

	Returns
	-------
	None
	"""
	mock_resp = MagicMock()
	mock_resp.json.return_value = sample_states_response
	mock_resp.raise_for_status.return_value = None
	mock_get.return_value = mock_resp

	result = geo_br.states()
	assert isinstance(result, list)
	assert result == sample_states_response
	mock_get.assert_called_once_with(
		"https://servicodados.ibge.gov.br/api/v1/localidades/estados", timeout=10
	)


@patch("stpstone.utils.geography.geo_br.requests.get")
def test_states_http_error_raises(mock_get: MagicMock, geo_br: BrazilGeo) -> None:
	"""Test states() raises HTTPError when API returns HTTP error.

	Parameters
	----------
	mock_get : MagicMock
		Mocked requests.get function
	geo_br : BrazilGeo
		Instance of the class being tested

	Returns
	-------
	None
	"""
	mock_resp = MagicMock()
	mock_resp.raise_for_status.side_effect = HTTPError("HTTP Error")
	mock_get.return_value = mock_resp

	with pytest.raises(HTTPError, match="Server error:"):
		geo_br.states()


@patch("stpstone.utils.geography.geo_br.requests.get")
def test_states_timeout_error_raises(mock_get: MagicMock, geo_br: BrazilGeo) -> None:
	"""Test states() raises Timeout when request times out.

	Parameters
	----------
	mock_get : MagicMock
		Mocked requests.get function
	geo_br : BrazilGeo
		Instance of the class being tested

	Returns
	-------
	None
	"""
	mock_get.side_effect = Timeout("Timeout Error")

	with pytest.raises(Timeout, match="Request timed out"):
		geo_br.states()


# --------------------------
# Tests for zip_code method
# --------------------------
@patch("stpstone.utils.geography.geo_br.requests.get")
@patch("stpstone.utils.geography.geo_br.sleep", return_value=None)
def test_zip_code_success(
	mock_sleep: MagicMock,
	mock_get: MagicMock,
	geo_br: BrazilGeo,
	sample_zip_code_response: ReturnZipCode,
) -> None:
	"""Test zip_code returns correct location info dictionary on successful API calls.

	Parameters
	----------
	mock_sleep : MagicMock
		Mocked sleep function
	mock_get : MagicMock
		Mocked requests.get function
	geo_br : BrazilGeo
		Instance of the class being tested
	sample_zip_code_response : ReturnZipCode
		Sample API response data

	Returns
	-------
	None
	"""
	mock_resp = MagicMock()
	mock_resp.json.return_value = sample_zip_code_response
	mock_resp.raise_for_status.return_value = None
	mock_get.return_value = mock_resp

	zips = ["20040030", "01001000"]
	result = geo_br.zip_code(zips)
	assert isinstance(result, dict)
	assert all(zip_code in result for zip_code in zips)
	assert all(isinstance(info, dict) for info in result.values())
	assert result["20040030"] == sample_zip_code_response
	assert mock_get.call_count == len(zips)
	assert mock_sleep.call_count == len(zips)


def test_zip_code_invalid_list_empty_raises(geo_br: BrazilGeo) -> None:
	"""Test zip_code raises ValueError when provided empty list.

	Parameters
	----------
	geo_br : BrazilGeo
		Instance of the class being tested

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Zip code list cannot be empty"):
		geo_br.zip_code([])


@pytest.mark.parametrize("invalid_zip", ["", " ", "   "])
def test_zip_code_invalid_zip_empty_raises(invalid_zip: str, geo_br: BrazilGeo) -> None:
	"""Test zip_code raises ValueError for list containing empty or whitespace zip code.

	Parameters
	----------
	invalid_zip : str
		Invalid zip code
	geo_br : BrazilGeo
		Instance of the class being tested

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Zip code cannot be empty"):
		geo_br.zip_code([invalid_zip])


@patch("stpstone.utils.geography.geo_br.requests.get")
@patch("stpstone.utils.geography.geo_br.sleep", return_value=None)
def test_zip_code_http_error_handles(
	mock_sleep: MagicMock,
	mock_get: MagicMock,
	geo_br: BrazilGeo,
) -> None:
	"""Test zip_code handles HTTPError by returning error dict for the zip code.

	Parameters
	----------
	mock_sleep : MagicMock
		Mocked sleep function
	mock_get : MagicMock
		Mocked requests.get function
	geo_br : BrazilGeo
		Instance of the class being tested

	Returns
	-------
	None

	Raises
	------
	HTTPError
		HTTP error
	"""

	def raise_http_error(
		*args: Any,  # noqa ANN401: typing.Any is not allowed
		**kwargs: Any,  # noqa ANN401: typing.Any is not allowed
	) -> NoReturn:
		"""Raise an HTTPError.

		Parameters
		----------
		args : Any
			Positional arguments
		kwargs : Any
			Keyword arguments

		Returns
		-------
		NoReturn
			This function always raises an exception

		Raises
		------
		HTTPError
			HTTP error
		"""
		raise HTTPError("HTTP error")

	mock_get.side_effect = raise_http_error

	zips = ["99999999"]
	result = geo_br.zip_code(zips)

	expected_error_dict = {
		"cep": "ERROR",
		"address_type": "ERROR",
		"address_name": "ERROR",
		"address": "ERROR",
		"state": "ERROR",
		"district": "ERROR",
		"lat": "ERROR",
		"long": "ERROR",
		"city": "ERROR",
		"city_ibge": "ERROR",
		"ddd": "ERROR",
	}
	assert result == {zips[0]: expected_error_dict}
	assert mock_get.call_count == 1
	assert mock_sleep.call_count == 0  # sleep not called after error


@patch("stpstone.utils.geography.geo_br.requests.get")
@patch("stpstone.utils.geography.geo_br.sleep", return_value=None)
def test_zip_code_timeout_error_handles(
	mock_sleep: MagicMock,
	mock_get: MagicMock,
	geo_br: BrazilGeo,
) -> None:
	"""Test zip_code handles Timeout by returning error dict for the zip code.

	Parameters
	----------
	mock_sleep : MagicMock
		Mocked sleep function
	mock_get : MagicMock
		Mocked requests.get function
	geo_br : BrazilGeo
		Instance of the class being tested

	Returns
	-------
	None

	Raises
	------
	Timeout
		Timeout error
	"""

	def raise_timeout(
		*args: Any,  # noqa ANN401: typing.Any is not allowed
		**kwargs: Any,  # noqa ANN401: typing.Any is not allowed
	) -> NoReturn:
		"""Raise a Timeout error.

		Parameters
		----------
		args : Any
			Positional arguments
		kwargs : Any
			Keyword arguments

		Returns
		-------
		NoReturn
			This function always raises an exception

		Raises
		------
		Timeout
			Timeout error
		"""
		raise Timeout("Timeout error")

	mock_get.side_effect = raise_timeout

	zips = ["88888888"]
	result = geo_br.zip_code(zips)

	expected_error_dict = {
		"cep": "ERROR",
		"address_type": "ERROR",
		"address_name": "ERROR",
		"address": "ERROR",
		"state": "ERROR",
		"district": "ERROR",
		"lat": "ERROR",
		"long": "ERROR",
		"city": "ERROR",
		"city_ibge": "ERROR",
		"ddd": "ERROR",
	}
	assert result == {zips[0]: expected_error_dict}
	assert mock_get.call_count == 1
	assert mock_sleep.call_count == 0  # sleep not called after error


@patch("stpstone.utils.geography.geo_br.requests.get")
def test_zip_code_mixed_success_and_error(
	mock_get: MagicMock,
	geo_br: BrazilGeo,
	sample_zip_code_response: ReturnZipCode,
) -> None:
	"""Test zip_code returns successful and error results for multiple zip codes.

	Parameters
	----------
	mock_get : MagicMock
		Mocked requests.get function
	geo_br : BrazilGeo
		Instance of the class being tested
	sample_zip_code_response : ReturnZipCode
		Sample zip code response

	Returns
	-------
	None

	Raises
	------
	HTTPError
		HTTP error
	"""
	mock_resp_ok = MagicMock()
	mock_resp_ok.json.return_value = sample_zip_code_response
	mock_resp_ok.raise_for_status.return_value = None

	def side_effect(url: str, timeout: float) -> MagicMock:
		"""Mock the requests.get function.

		Parameters
		----------
		url : str
			URL
		timeout : float
			Timeout

		Returns
		-------
		MagicMock
			Mocked response

		Raises
		------
		HTTPError
			HTTP error
		"""
		if url.endswith("00000000"):
			raise HTTPError("HTTP error")
		return mock_resp_ok

	mock_get.side_effect = side_effect

	zips = ["20040030", "00000000"]
	result = geo_br.zip_code(zips)

	assert isinstance(result, dict)
	assert "20040030" in result and "00000000" in result
	assert result["20040030"] == sample_zip_code_response
	assert result["00000000"]["cep"] == "ERROR"


# --------------------------
# Type Checks (Signatures)
# --------------------------
def test_method_signatures(geo_br: BrazilGeo) -> None:
	"""Test BrazilGeo method type annotations are present.

	Parameters
	----------
	geo_br : BrazilGeo
		Instance of the class being tested

	Returns
	-------
	None
	"""
	assert callable(geo_br._validate_zip_codes)
	assert callable(geo_br.states)
	assert callable(geo_br.zip_code)


def test_return_types(geo_br: BrazilGeo, sample_zip_code_response: ReturnZipCode) -> None:
	"""Test return types of methods with mocked data.

	Parameters
	----------
	geo_br : BrazilGeo
		Instance of the class being tested
	sample_zip_code_response : ReturnZipCode
		Sample zip code response

	Returns
	-------
	None
	"""
	with (
		patch("stpstone.utils.geography.geo_br.requests.get") as mock_get,
		patch("stpstone.utils.geography.geo_br.sleep", return_value=None),
	):
		mock_resp = MagicMock()
		mock_resp.raise_for_status.return_value = None
		mock_resp.json.return_value = sample_zip_code_response
		mock_get.return_value = mock_resp

		# Test states method
		with patch("stpstone.utils.geography.geo_br.requests.get") as mock_get_states:
			mock_resp_states = MagicMock()
			mock_resp_states.raise_for_status.return_value = None
			mock_resp_states.json.return_value = [
				{
					"id": 33,
					"sigla": "RJ",
					"nome": "Rio de Janeiro",
					"regiao": {"id": 3, "sigla": "SE", "nome": "Sudeste"},
				}
			]
			mock_get_states.return_value = mock_resp_states

			result_states = geo_br.states()
			assert isinstance(result_states, list)
			assert all(isinstance(item, dict) for item in result_states)

		# Test zip_code method
		result_zip = geo_br.zip_code(["20040030"])
		assert isinstance(result_zip, dict)
		for val in result_zip.values():
			assert isinstance(val, dict)
