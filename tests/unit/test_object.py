"""Unit tests for HandlingObjects class.

This module contains tests for the HandlingObjects class, verifying its functionality
in parsing and converting objects to their inherent Python types with ast.literal_eval.
Tests cover normal operations, edge cases, error conditions, and type validation.
"""

import pytest
from pytest_mock import MockerFixture

from stpstone.utils.parsers.object import HandlingObjects
from stpstone.utils.parsers.str import StrHandler


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def handling_objects() -> HandlingObjects:
    """Fixture providing a HandlingObjects instance.

    Returns
    -------
    HandlingObjects
        Instance of HandlingObjects class for testing
    """
    return HandlingObjects()

@pytest.fixture
def valid_list_string() -> str:
    """Fixture providing a valid string representation of a list.

    Returns
    -------
    str
        String containing "[1, 2, 3]"
    """
    return "[1, 2, 3]"

@pytest.fixture
def valid_dict_string() -> str:
    r"""Fixture providing a valid string representation of a dictionary.

    Returns
    -------
    str
        String containing "{\"key\": \"value\"}"
    """
    return "{\"key\": \"value\"}"


# --------------------------
# Tests for _validate_bounds
# --------------------------
def test_validate_bounds_both_none(handling_objects: HandlingObjects) -> None:
    """Test _validate_bounds with both boundaries None.

    Verifies
    --------
    - No exception is raised when both boundaries are None

    Parameters
    ----------
    handling_objects : HandlingObjects
        Instance of HandlingObjects class

    Returns
    -------
    None
    """
    handling_objects._validate_bounds(None, None)
    assert True

def test_validate_bounds_both_provided(handling_objects: HandlingObjects) -> None:
    """Test _validate_bounds with both boundaries provided.

    Verifies
    --------
    - No exception is raised when both boundaries are valid strings

    Parameters
    ----------
    handling_objects : HandlingObjects
        Instance of HandlingObjects class

    Returns
    -------
    None
    """
    handling_objects._validate_bounds("[", "]")
    assert True

def test_validate_bounds_only_left_provided(handling_objects: HandlingObjects) -> None:
    """Test _validate_bounds when only left boundary is provided.

    Verifies
    --------
    - Raises ValueError when str_right_bound is None but str_left_bound is provided

    Parameters
    ----------
    handling_objects : HandlingObjects
        Instance of HandlingObjects class

    Returns
    -------
    None
    """
    with pytest.raises(
        ValueError, match="Both str_left_bound and str_right_bound must be provided or both None"
    ):
        handling_objects._validate_bounds("[", None)

def test_validate_bounds_only_right_provided(handling_objects: HandlingObjects) -> None:
    """Test _validate_bounds when only right boundary is provided.

    Verifies
    --------
    - Raises ValueError when str_left_bound is None but str_right_bound is provided

    Parameters
    ----------
    handling_objects : HandlingObjects
        Instance of HandlingObjects class

    Returns
    -------
    None
    """
    with pytest.raises(
        ValueError, match="Both str_left_bound and str_right_bound must be provided or both None"
    ):
        handling_objects._validate_bounds(None, "]")


# --------------------------
# Tests for _validate_data_object
# --------------------------
def test_validate_data_object_none(handling_objects: HandlingObjects) -> None:
    """Test _validate_data_object with None input.

    Verifies
    --------
    - Raises ValueError when data_object is None

    Parameters
    ----------
    handling_objects : HandlingObjects
        Instance of HandlingObjects class

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Data object cannot be None"):
        handling_objects._validate_data_object(None)

def test_validate_data_object_empty_string(handling_objects: HandlingObjects) -> None:
    """Test _validate_data_object with empty string.

    Verifies
    --------
    - Raises ValueError when data_object is an empty string

    Parameters
    ----------
    handling_objects : HandlingObjects
        Instance of HandlingObjects class

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Data object cannot be an empty string"):
        handling_objects._validate_data_object("")

def test_validate_data_object_valid_string(handling_objects: HandlingObjects) -> None:
    """Test _validate_data_object with valid string.

    Verifies
    --------
    - No exception is raised for a non-empty string

    Parameters
    ----------
    handling_objects : HandlingObjects
        Instance of HandlingObjects class

    Returns
    -------
    None
    """
    handling_objects._validate_data_object("[1, 2, 3]")
    assert True

def test_validate_data_object_non_string(handling_objects: HandlingObjects) -> None:
    """Test _validate_data_object with non-string input.

    Verifies
    --------
    - No exception is raised for non-string valid input

    Parameters
    ----------
    handling_objects : HandlingObjects
        Instance of HandlingObjects class

    Returns
    -------
    None
    """
    handling_objects._validate_data_object(123)
    assert True


# --------------------------
# Tests for literal_eval_data
# --------------------------
def test_literal_eval_data_valid_list(
    handling_objects: HandlingObjects, valid_list_string: str
) -> None:
    """Test literal_eval_data with valid list string and no boundaries.

    Verifies
    --------
    - Correctly converts string representation to list
    - Returns expected list [1, 2, 3]

    Parameters
    ----------
    handling_objects : HandlingObjects
        Instance of HandlingObjects class
    valid_list_string : str
        String containing "[1, 2, 3]"

    Returns
    -------
    None
    """
    result = handling_objects.literal_eval_data(valid_list_string)
    assert result == [1, 2, 3]
    assert isinstance(result, list)

def test_literal_eval_data_valid_dict(
    handling_objects: HandlingObjects, valid_dict_string: str
) -> None:
    r"""Test literal_eval_data with valid dict string and no boundaries.

    Verifies
    --------
    - Correctly converts string representation to dict
    - Returns expected dict {"key": "value"}

    Parameters
    ----------
    handling_objects : HandlingObjects
        Instance of HandlingObjects class
    valid_dict_string : str
        String containing "{\"key\": \"value\"}"

    Returns
    -------
    None
    """
    result = handling_objects.literal_eval_data(valid_dict_string)
    assert result == {"key": "value"}
    assert isinstance(result, dict)

def test_literal_eval_data_with_boundaries(
    handling_objects: HandlingObjects, mocker: MockerFixture
) -> None:
    """Test literal_eval_data with boundary extraction.

    Verifies
    --------
    - Correctly extracts and converts substring between boundaries
    - StrHandler.get_between is called with correct arguments
    - Returns expected list [1, 2, 3]

    Parameters
    ----------
    handling_objects : HandlingObjects
        Instance of HandlingObjects class
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_get_between = mocker.patch.object(StrHandler, "get_between", return_value="[1, 2, 3]")
    result = handling_objects.literal_eval_data("prefix[1, 2, 3]suffix", "[", "]")
    assert result == [1, 2, 3]
    mock_get_between.assert_called_once_with("prefix[1, 2, 3]suffix", "[", "]")

def test_literal_eval_data_invalid_syntax(handling_objects: HandlingObjects) -> None:
    """Test literal_eval_data with invalid syntax.

    Verifies
    --------
    - Raises SyntaxError for invalid Python literal
    - Includes appropriate error message

    Parameters
    ----------
    handling_objects : HandlingObjects
        Instance of HandlingObjects class

    Returns
    -------
    None
    """
    with pytest.raises(SyntaxError, match="Failed to evaluate data object"):
        handling_objects.literal_eval_data("invalid_syntax")

def test_literal_eval_data_empty_string_with_boundaries(
    handling_objects: HandlingObjects, mocker: MockerFixture
) -> None:
    """Test literal_eval_data with empty string after boundary extraction.

    Verifies
    --------
    - Raises SyntaxError when extracted string is empty
    - StrHandler.get_between is called correctly

    Parameters
    ----------
    handling_objects : HandlingObjects
        Instance of HandlingObjects class
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_get_between = mocker.patch.object(StrHandler, "get_between", return_value="")
    with pytest.raises(SyntaxError, match="Failed to evaluate data object"):
        handling_objects.literal_eval_data("prefix[]suffix", "[", "]")
    mock_get_between.assert_called_once_with("prefix[]suffix", "[", "]")

def test_literal_eval_data_unicode_input(handling_objects: HandlingObjects) -> None:
    """Test literal_eval_data with unicode string input.

    Verifies
    --------
    - Correctly handles unicode string conversion
    - Returns expected dict with unicode characters

    Parameters
    ----------
    handling_objects : HandlingObjects
        Instance of HandlingObjects class

    Returns
    -------
    None
    """
    result = handling_objects.literal_eval_data("{\"key\": \"värld\"}")
    assert result == {"key": "värld"}
    assert isinstance(result, dict)

def test_literal_eval_data_numeric_input(handling_objects: HandlingObjects) -> None:
    """Test literal_eval_data with numeric string input.

    Verifies
    --------
    - Correctly converts string representation of number
    - Returns expected numeric value

    Parameters
    ----------
    handling_objects : HandlingObjects
        Instance of HandlingObjects class

    Returns
    -------
    None
    """
    result = handling_objects.literal_eval_data("42")
    assert result == 42
    assert isinstance(result, int)

def test_literal_eval_data_reload(mocker: MockerFixture) -> None:
    """Test HandlingObjects behavior after module reload.

    Verifies
    --------
    - Class maintains functionality after importlib.reload
    - No state is unexpectedly preserved
    - Conversion still works correctly

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    import importlib
    import sys
    mocker.patch.object(StrHandler, "get_between", return_value="[1, 2, 3]")
    importlib.reload(sys.modules["stpstone.utils.parsers.object"])
    handling_objects = HandlingObjects()
    result = handling_objects.literal_eval_data("prefix[1, 2, 3]suffix", "[", "]")
    assert result == [1, 2, 3]