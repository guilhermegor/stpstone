"""Unit tests for DocumentsNumbersBR class.

Tests the Brazilian document number validation and processing functionality,
including initialization, validation, masking, unmasking, and public info retrieval.
"""

import re
from typing import Any
from unittest.mock import Mock

from pydantic import ValidationError
import pytest
from pytest_mock import MockerFixture
from requests.exceptions import HTTPError

from stpstone.transformations.validation.docs_br import (
	Atividade,
	DocumentsNumbersBR,
	ResultCNPJInfo,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def valid_cnpj_list() -> list[str]:
	"""Provide a list of valid CNPJ numbers for testing.

	Returns
	-------
	list[str]
		List of valid CNPJ numbers
	"""
	return ["60746948000112", "12345678000195"]


@pytest.fixture
def invalid_cnpj_list() -> list[str]:
	"""Provide a list of invalid CNPJ numbers for testing.

	Returns
	-------
	list[str]
		List of invalid CNPJ numbers
	"""
	return ["00000000000000", "12345678901234"]


@pytest.fixture
def mixed_doc_list() -> list[str]:
	"""Provide a mixed list of valid and invalid document numbers.

	Returns
	-------
	list[str]
		Mixed list of valid and invalid document numbers
	"""
	return ["60746948000112", "12345678901234", ""]


@pytest.fixture
def documents_numbers_br(valid_cnpj_list: list[str]) -> DocumentsNumbersBR:
	"""Provide a DocumentsNumbersBR instance with valid CNPJ numbers.

	Parameters
	----------
	valid_cnpj_list : list[str]
		List of valid CNPJ numbers from fixture

	Returns
	-------
	DocumentsNumbersBR
		Initialized instance with valid CNPJ numbers
	"""
	return DocumentsNumbersBR(valid_cnpj_list)


@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> Mock:
	"""Mock requests.request for CNPJ info retrieval.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	Mock
		Mock object for requests.request
	"""
	mock = mocker.patch("stpstone.transformations.validation.docs_br.request")
	mock_response = Mock()
	mock_response.json.return_value = {
		"abertura": "01/01/2000",
		"situacao": "ATIVA",
		"tipo": "MATRIZ",
		"nome": "Test Company",
		"fantasia": "Test Fantasy",
		"porte": "MICRO EMPRESA",
		"natureza_juridica": "1234-5",
		"atividade_principal": [{"code": "12.34.56", "text": "Test Activity"}],
		"atividades_secundarias": [],
		"qsa": [{"nome": "Test Person", "qual": "Administrator"}],
		"logradouro": "Test Street",
		"numero": "123",
		"municipio": "Test City",
		"bairro": "Test Neighborhood",
		"uf": "SP",
		"cep": "12345678",
		"telefone": "(11) 1234-5678",
		"data_situacao": "01/01/2000",
		"cnpj": "60746948000112",
		"ultima_atualizacao": "2023-01-01",
		"status": "OK",
		"complemento": "",
		"email": "test@example.com",
		"efr": "",
		"motivo_situacao": "",
		"situacao_especial": "",
		"data_situacao_especial": "",
		"capital_social": "1000.00",
		"simples": {
			"optante": True,
			"data_opcao": "01/01/2000",
			"data_exclusao": None,
			"ultima_atualizacao": "2023-01-01",
		},
		"simei": {
			"optante": False,
			"data_opcao": None,
			"data_exclusao": None,
			"ultima_atualizacao": "2023-01-01",
		},
		"extra": {},
		"billing": {"free": True, "database": False},
	}
	mock_response.raise_for_status.return_value = None
	mock.return_value = mock_response
	return mock


# --------------------------
# Tests for Initialization
# --------------------------
def test_init_valid_list(valid_cnpj_list: list[str]) -> None:
	"""Test initialization with valid list of document numbers.

	Verifies
	--------
	- Instance is created successfully
	- list_docs attribute is set correctly

	Parameters
	----------
	valid_cnpj_list : list[str]
		List of valid CNPJ numbers from fixture

	Returns
	-------
	None
	"""
	doc = DocumentsNumbersBR(valid_cnpj_list)
	assert doc.list_docs == valid_cnpj_list


def test_init_empty_list() -> None:
	"""Test initialization with empty list raises ValueError.

	Verifies
	--------
	- Raises ValueError with appropriate message for empty list

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="list_docs cannot be empty"):
		DocumentsNumbersBR([])


@pytest.mark.parametrize("invalid_input", [None, "not a list", 123])
def test_init_invalid_type(
	invalid_input: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test initialization with invalid input types raises TypeError.

	Parameters
	----------
	invalid_input : Any
		Invalid input for list_docs parameter

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		DocumentsNumbersBR(invalid_input)


# --------------------------
# Tests for _validate_list_docs
# --------------------------
def test_validate_list_docs_non_list() -> None:
	"""Test _validate_list_docs with non-list input raises TypeError.

	Verifies
	--------
	- TypeError is raised when list_docs is not a list

	Returns
	-------
	None
	"""
	doc = DocumentsNumbersBR.__new__(DocumentsNumbersBR)
	doc.list_docs = "not a list"
	with pytest.raises(TypeError, match="list_docs must be of type list"):
		doc._validate_list_docs()


def test_validate_list_docs_non_string_elements() -> None:
	"""Test _validate_list_docs with non-string elements raises TypeError.

	Verifies
	--------
	- TypeError is raised when list_docs contains non-string elements

	Returns
	-------
	None
	"""
	doc = DocumentsNumbersBR.__new__(DocumentsNumbersBR)
	doc.list_docs = ["valid", 123]
	with pytest.raises(TypeError, match="All elements in list_docs must be of type str"):
		doc._validate_list_docs()


# --------------------------
# Tests for _get_doc_validator
# --------------------------
def test_get_doc_validator_valid_doc(documents_numbers_br: DocumentsNumbersBR) -> None:
	"""Test _get_doc_validator with valid document type.

	Verifies
	--------
	- Returns correct validator instance for valid document type

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance from fixture

	Returns
	-------
	None
	"""
	from validate_docbr import CNPJ

	validator = documents_numbers_br._get_doc_validator("CNPJ")
	assert isinstance(validator, CNPJ)


@pytest.mark.parametrize("invalid_doc", ["INVALID", "", "XYZ"])
def test_get_doc_validator_invalid_doc(
	documents_numbers_br: DocumentsNumbersBR, invalid_doc: str
) -> None:
	"""Test _get_doc_validator with invalid document type raises ValueError.

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance from fixture
	invalid_doc : str
		Invalid document type

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match=f"Invalid document type: {invalid_doc}"):
		documents_numbers_br._get_doc_validator(invalid_doc)


def test_get_doc_validator_invalid_type(documents_numbers_br: DocumentsNumbersBR) -> None:
	"""Test _get_doc_validator with non-string doc raises TypeError.

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		documents_numbers_br._get_doc_validator(123)


# --------------------------
# Tests for _is_already_masked
# --------------------------
def test_is_already_masked_valid_cnpj(documents_numbers_br: DocumentsNumbersBR) -> None:
	"""Test _is_already_masked with valid masked CNPJ.

	Verifies
	--------
	- Correctly identifies a properly masked CNPJ number

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance from fixture

	Returns
	-------
	None
	"""
	assert documents_numbers_br._is_already_masked("12.345.678/9012-34", "CNPJ") is True


@pytest.mark.parametrize(
	"doc_number,doc_type,expected",
	[
		("12345678901234", "CNPJ", False),
		("123.456.789-01", "CPF", True),
		("123 4567 8901 2345", "CNS", True),
		("123.45678.90-1", "PIS", True),
		("1234 5678 9012", "TITULO_ELEITORAL", True),
		("12345678901", "RENAVAM", True),
		("12345678901", "CNH", True),
	],
)
def test_is_already_masked_valid_formats(
	documents_numbers_br: DocumentsNumbersBR, doc_number: str, doc_type: str, expected: bool
) -> None:
	"""Test _is_already_masked with various document formats.

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance from fixture
	doc_number : str
		Document number to test
	doc_type : str
		Document type to test
	expected : bool
		Expected result of the masking check

	Returns
	-------
	None
	"""
	result = documents_numbers_br._is_already_masked(doc_number, doc_type)
	assert result == expected


def test_is_already_masked_invalid_doc_type(documents_numbers_br: DocumentsNumbersBR) -> None:
	"""Test _is_already_masked with invalid document type raises ValueError.

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Invalid document type: INVALID"):
		documents_numbers_br._is_already_masked("12.345.678/9012-34", "INVALID")


def test_is_already_masked_invalid_input_type(documents_numbers_br: DocumentsNumbersBR) -> None:
	"""Test _is_already_masked with non-string doc_number raises TypeError.

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		documents_numbers_br._is_already_masked(123, "CNPJ")


# --------------------------
# Tests for validate_doc
# --------------------------
def test_validate_doc_valid_cnpjs(
	documents_numbers_br: DocumentsNumbersBR, valid_cnpj_list: list[str]
) -> None:
	"""Test validate_doc with valid CNPJ numbers.

	Verifies
	--------
	- Correctly validates all CNPJ numbers
	- Returns correct validation results

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance from fixture
	valid_cnpj_list : list[str]
		List of valid CNPJ numbers from fixture

	Returns
	-------
	None
	"""
	results = documents_numbers_br.validate_doc("CNPJ")
	assert all(is_valid for _, is_valid in results)
	assert [doc for doc, _ in results] == valid_cnpj_list


def test_validate_doc_mixed_cnpjs(
	documents_numbers_br: DocumentsNumbersBR, mixed_doc_list: list[str]
) -> None:
	"""Test validate_doc with mixed valid and invalid CNPJ numbers.

	Verifies
	--------
	- Correctly identifies valid and invalid CNPJ numbers

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance with mixed documents
	mixed_doc_list : list[str]
		List of mixed valid and invalid documents from fixture

	Returns
	-------
	None
	"""
	doc = DocumentsNumbersBR(mixed_doc_list)
	results = doc.validate_doc("CNPJ")
	expected = [(num, num == "60746948000112") for num in mixed_doc_list]
	assert results == expected


def test_validate_doc_invalid_doc_type(documents_numbers_br: DocumentsNumbersBR) -> None:
	"""Test validate_doc with invalid document type raises ValueError.

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Invalid document type: INVALID"):
		documents_numbers_br.validate_doc("INVALID")


# --------------------------
# Tests for mask_numbers
# --------------------------
def test_mask_numbers_unmasked_cnpjs(
	documents_numbers_br: DocumentsNumbersBR, valid_cnpj_list: list[str]
) -> None:
	"""Test mask_numbers with unmasked valid CNPJ numbers.

	Verifies
	--------
	- Correctly masks valid CNPJ numbers
	- Returns properly formatted CNPJ numbers

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance from fixture
	valid_cnpj_list : list[str]
		List of valid CNPJ numbers from fixture

	Returns
	-------
	None
	"""
	results = documents_numbers_br.mask_numbers("CNPJ")
	assert all(re.match(r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$", r) for r in results)
	assert len(results) == len(valid_cnpj_list)


def test_mask_numbers_already_masked(
	documents_numbers_br: DocumentsNumbersBR, valid_cnpj_list: list[str]
) -> None:
	"""Test mask_numbers with already masked CNPJ numbers.

	Verifies
	--------
	- Preserves already masked numbers
	- Returns original masked numbers unchanged

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance from fixture
	valid_cnpj_list : list[str]
		List of valid CNPJ numbers from fixture

	Returns
	-------
	None
	"""
	masked = documents_numbers_br.mask_numbers("CNPJ")
	doc = DocumentsNumbersBR(masked)
	results = doc.mask_numbers("CNPJ")
	assert results == masked


def test_mask_numbers_invalid_doc_type(documents_numbers_br: DocumentsNumbersBR) -> None:
	"""Test mask_numbers with invalid document type raises ValueError.

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Invalid document type: INVALID"):
		documents_numbers_br.mask_numbers("INVALID")


# --------------------------
# Tests for unmask_docs
# --------------------------
def test_unmask_docs_mixed_formats(documents_numbers_br: DocumentsNumbersBR) -> None:
	"""Test unmask_docs with mixed format document numbers.

	Verifies
	--------
	- Correctly removes all formatting from document numbers
	- Handles various input formats correctly

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance with mixed format documents

	Returns
	-------
	None
	"""
	doc = DocumentsNumbersBR(["12.345.678/9012-34", "12345678901234", "123.456.789-01"])
	results = doc.unmask_docs()
	assert results == ["12345678901234", "12345678901234", "12345678901"]


def test_unmask_docs_empty_strings(documents_numbers_br: DocumentsNumbersBR) -> None:
	"""Test unmask_docs with empty strings in document list.

	Verifies
	--------
	- Correctly handles empty strings
	- Returns empty string for empty input

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance from fixture

	Returns
	-------
	None
	"""
	doc = DocumentsNumbersBR(["", "12.345.678/9012-34"])
	results = doc.unmask_docs()
	assert results == ["", "12345678901234"]


# --------------------------
# Tests for _validate_max_requests
# --------------------------
def test_validate_max_requests_valid(documents_numbers_br: DocumentsNumbersBR) -> None:
	"""Test _validate_max_requests with valid positive integer.

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance from fixture

	Returns
	-------
	None
	"""
	documents_numbers_br._validate_max_requests(3)
	assert True  # No exception raised


@pytest.mark.parametrize("invalid_value", [0, -1, -10])
def test_validate_max_requests_non_positive(
	documents_numbers_br: DocumentsNumbersBR, invalid_value: int
) -> None:
	"""Test _validate_max_requests with non-positive values raises ValueError.

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance from fixture
	invalid_value : int
		Non-positive integer value

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="max_requests_per_minute must be positive"):
		documents_numbers_br._validate_max_requests(invalid_value)


def test_validate_max_requests_invalid_type(documents_numbers_br: DocumentsNumbersBR) -> None:
	"""Test _validate_max_requests with non-integer type raises TypeError.

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		documents_numbers_br._validate_max_requests("3")


# --------------------------
# Tests for get_public_info_cnpj
# --------------------------
def test_get_public_info_cnpj_success(
	documents_numbers_br: DocumentsNumbersBR, mock_requests_get: Mock
) -> None:
	"""Test get_public_info_cnpj with valid CNPJ numbers and successful response.

	Verifies
	--------
	- Successfully retrieves and validates CNPJ information
	- Returns list of ResultCNPJInfo objects
	- Makes correct number of API calls

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance from fixture
	mock_requests_get : Mock
		Mocked requests.request from fixture

	Returns
	-------
	None
	"""
	results = documents_numbers_br.get_public_info_cnpj(max_requests_per_minute=2)
	assert len(results) == 2  # Only valid CNPJs are processed
	assert all(isinstance(r, ResultCNPJInfo) for r in results)
	assert mock_requests_get.call_count == 2


def test_get_public_info_cnpj_rate_limiting(
	documents_numbers_br: DocumentsNumbersBR, mock_requests_get: Mock, mocker: MockerFixture
) -> None:
	"""Test get_public_info_cnpj rate limiting behavior.

	Verifies
	--------
	- Correctly applies rate limiting by sleeping after max requests
	- Makes correct number of API calls

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance from fixture
	mock_requests_get : Mock
		Mocked requests.request from fixture
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	mock_sleep = mocker.patch("time.sleep")  # retorna o mock
	doc = DocumentsNumbersBR(["60746948000112"] * 4)
	results = doc.get_public_info_cnpj(max_requests_per_minute=2)
	assert len(results) == 4
	assert mock_requests_get.call_count == 4
	assert mock_sleep.call_count == 1


def test_get_public_info_cnpj_invalid_response(
	documents_numbers_br: DocumentsNumbersBR, mock_requests_get: Mock
) -> None:
	"""Test get_public_info_cnpj with invalid API response raises ValueError.

	Parameters
	----------
	documents_numbers_br : DocumentsNumbersBR
		DocumentsNumbersBR instance from fixture
	mock_requests_get : Mock
		Mocked requests.request from fixture

	Returns
	-------
	None
	"""
	mock_requests_get.side_effect = HTTPError("Request failed")
	with pytest.raises(ValueError, match="Failed to fetch or validate info"):
		documents_numbers_br.get_public_info_cnpj()


# --------------------------
# Tests for ResultCNPJInfo Pydantic Model
# --------------------------
def test_result_cnpj_info_valid_data() -> None:
	"""Test ResultCNPJInfo with valid data.

	Verifies
	--------
	- Successfully creates ResultCNPJInfo instance with valid data
	- All fields are correctly assigned

	Returns
	-------
	None
	"""
	data = {
		"abertura": "01/01/2000",
		"situacao": "ATIVA",
		"tipo": "MATRIZ",
		"nome": "Test Company",
		"fantasia": "Test Fantasy",
		"porte": "MICRO EMPRESA",
		"natureza_juridica": "1234-5",
		"atividade_principal": [{"code": "12.34.56", "text": "Test Activity"}],
		"atividades_secundarias": [],
		"qsa": [{"nome": "Test Person", "qual": "Administrator"}],
		"logradouro": "Test Street",
		"numero": "123",
		"municipio": "Test City",
		"bairro": "Test Neighborhood",
		"uf": "SP",
		"cep": "12345678",
		"telefone": "(11) 1234-5678",
		"data_situacao": "01/01/2000",
		"cnpj": "60746948000112",
		"ultima_atualizacao": "2023-01-01",
		"status": "OK",
		"complemento": "",
		"email": "test@example.com",
		"efr": "",
		"motivo_situacao": "",
		"situacao_especial": "",
		"data_situacao_especial": "",
		"capital_social": "1000.00",
		"simples": {
			"optante": True,
			"data_opcao": "01/01/2000",
			"data_exclusao": None,
			"ultima_atualizacao": "2023-01-01",
		},
		"simei": {
			"optante": False,
			"data_opcao": None,
			"data_exclusao": None,
			"ultima_atualizacao": "2023-01-01",
		},
		"extra": {},
		"billing": {"free": True, "database": False},
	}
	result = ResultCNPJInfo(**data)
	assert result.abertura == "01/01/2000"
	assert result.nome == "Test Company"
	assert len(result.atividade_principal) == 1
	assert isinstance(result.atividade_principal[0], Atividade)


def test_result_cnpj_info_invalid_date() -> None:
	"""Test ResultCNPJInfo with invalid abertura date raises ValidationError.

	Verifies
	--------
	- Raises ValidationError for invalid date format
	- Validates date components correctly

	Returns
	-------
	None
	"""
	invalid_data = {
		"abertura": "32/01/2000",  # Invalid day
		"situacao": "ATIVA",
		"tipo": "MATRIZ",
		"nome": "Test Company",
		"fantasia": "Test Fantasy",
		"porte": "MICRO EMPRESA",
		"natureza_juridica": "1234-5",
		"atividade_principal": [{"code": "12.34.56", "text": "Test Activity"}],
		"atividades_secundarias": [],
		"qsa": [{"nome": "Test Person", "qual": "Administrator"}],
		"logradouro": "Test Street",
		"numero": "123",
		"municipio": "Test City",
		"bairro": "Test Neighborhood",
		"uf": "SP",
		"cep": "12345678",
		"telefone": "(11) 1234-5678",
		"data_situacao": "01/01/2000",
		"cnpj": "60746948000112",
		"ultima_atualizacao": "2023-01-01",
		"status": "OK",
		"complemento": "",
		"email": "test@example.com",
		"efr": "",
		"motivo_situacao": "",
		"situacao_especial": "",
		"data_situacao_especial": "",
		"capital_social": "1000.00",
		"simples": {
			"optante": True,
			"data_opcao": "01/01/2000",
			"data_exclusao": None,
			"ultima_atualizacao": "2023-01-01",
		},
		"simei": {
			"optante": False,
			"data_opcao": None,
			"data_exclusao": None,
			"ultima_atualizacao": "2023-01-01",
		},
		"extra": {},
		"billing": {"free": True, "database": False},
	}
	with pytest.raises(ValidationError, match="Invalid date format for abertura"):
		ResultCNPJInfo(**invalid_data)


# --------------------------
# Tests for Module Reload
# --------------------------
def test_module_reload(mocker: MockerFixture) -> None:
	"""Test module reload behavior.

	Verifies
	--------
	- Module can be reloaded without errors
	- Instance creation works post-reload

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.transformations.validation.docs_br

	importlib.reload(stpstone.transformations.validation.docs_br)
	doc = DocumentsNumbersBR(["60746948000112"])
	assert isinstance(doc, DocumentsNumbersBR)
	assert doc.list_docs == ["60746948000112"]


# --------------------------
# Tests for Edge Cases
# --------------------------
def test_validate_doc_empty_string_in_list() -> None:
	"""Test validate_doc with empty string in document list.

	Verifies
	--------
	- Handles empty strings correctly in validation

	Returns
	-------
	None
	"""
	doc = DocumentsNumbersBR(["60746948000112", ""])
	results = doc.validate_doc("CNPJ")
	assert results == [("60746948000112", True), ("", False)]


def test_mask_numbers_empty_string_in_list() -> None:
	"""Test mask_numbers with empty string in document list.

	Verifies
	--------
	- Handles empty strings correctly in masking

	Returns
	-------
	None
	"""
	doc = DocumentsNumbersBR(["60746948000112", ""])
	results = doc.mask_numbers("CNPJ")
	assert len(results) == 2
	assert re.match(r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$", results[0])
	assert results[1] == ""


def test_get_public_info_cnpj_no_valid_cnpjs(mocker: MockerFixture) -> None:
	"""Test get_public_info_cnpj with no valid CNPJ numbers.

	Verifies
	--------
	- Returns empty list when no valid CNPJs are provided
	- Does not make any API calls

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	mock = mocker.patch("requests.request")
	doc = DocumentsNumbersBR(["00000000000000"])
	results = doc.get_public_info_cnpj()
	assert results == []
	assert mock.call_count == 0
