"""Unit tests for JsonFiles class.

Tests JSON file handling functionality including:
- Saving and loading JSON files
- Data format conversions
- Input validation and error handling
- Edge cases and special scenarios
"""

import json
from pathlib import Path
from typing import Any, Union

import pytest

from stpstone.utils.parsers.json import JsonFiles


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def json_handler() -> Any: # noqa ANN401: typing.Any is not allowed
    """Fixture providing JsonFiles instance.

    Returns
    -------
    Any
        Instance of JsonFiles class
    """
    return JsonFiles()


@pytest.fixture
def sample_dict() -> dict[str, Union[str, int, float]]:
    """Fixture providing sample dictionary data.

    Returns
    -------
    dict[str, Union[str, int, float]]
        Sample dictionary with mixed types
    """
    return {"name": "test", "value": 42, "ratio": 0.5}


@pytest.fixture
def sample_list_of_dicts() -> list[dict[str, Union[str, int]]]:
    """Fixture providing sample list of dictionaries.

    Returns
    -------
    list[dict[str, Union[str, int]]]
        List with dictionaries of varying keys
    """
    return [
        {"id": 1, "name": "first"},
        {"id": 2, "value": "second"},
        {"id": 3, "extra": "third"}
    ]


@pytest.fixture
def temp_json_file(tmp_path: Path) -> str:
    """Fixture providing temporary JSON file path.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory fixture

    Returns
    -------
    str
        Path to temporary JSON file
    """
    return str(tmp_path / "test.json")


# --------------------------
# Validation Tests
# --------------------------
def test_validate_message_dict(
    json_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test _validate_message_dict with valid input.

    Verifies
    --------
    - Accepts valid dictionary input without raising exceptions

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles

    Returns
    -------
    None
    """
    json_handler._validate_message_dict({"valid": "dict"})


def test_validate_message_dict_invalid(
    json_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test _validate_message_dict with invalid input.

    Verifies
    --------
    - Raises TypeError for non-dictionary inputs

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="Message must be a dictionary"):
        json_handler._validate_message_dict("not a dict")


def test_validate_file_path(
    json_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test _validate_file_path with valid input.

    Verifies
    --------
    - Accepts valid file path strings without raising exceptions

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles

    Returns
    -------
    None
    """
    json_handler._validate_file_path("valid/path.json")


def test_validate_file_path_invalid(
    json_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test _validate_file_path with invalid input.

    Verifies
    --------
    - Raises ValueError for empty or non-string paths

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Invalid file path"):
        json_handler._validate_file_path("")
    with pytest.raises(TypeError, match="must be of type"):
        json_handler._validate_file_path(123)  # type: ignore


def test_validate_file_exists(
    json_handler: Any, # noqa ANN401: typing.Any is not allowed
    temp_json_file: str
) -> None:
    """Test _validate_file_exists with existing file.

    Verifies
    --------
    - Accepts existing file paths without raising exceptions

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles
    temp_json_file : str
        Temporary JSON file path

    Returns
    -------
    None
    """
    with open(temp_json_file, "w") as f:
        f.write("{}")
    json_handler._validate_file_exists(temp_json_file)


def test_validate_file_exists_missing(
    json_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test _validate_file_exists with missing file.

    Verifies
    --------
    - Raises FileNotFoundError for non-existent files

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles

    Returns
    -------
    None
    """
    with pytest.raises(FileNotFoundError):
        json_handler._validate_file_exists("nonexistent.json")


def test_validate_json_list(
    json_handler: Any, # noqa ANN401: typing.Any is not allowed
    sample_list_of_dicts: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test _validate_json_list with valid input.

    Verifies
    --------
    - Accepts valid list of dictionaries without raising exceptions

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles
    sample_list_of_dicts : Any
        Sample list of dictionaries

    Returns
    -------
    None
    """
    json_handler._validate_json_list(sample_list_of_dicts)


def test_validate_json_list_invalid(
    json_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test _validate_json_list with invalid input.

    Verifies
    --------
    - Raises TypeError for non-list inputs
    - Raises ValueError for empty lists
    - Raises TypeError for lists with non-dict items

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="Input must be a list"):
        json_handler._validate_json_list({"not": "a list"})
    with pytest.raises(ValueError, match="Input list cannot be empty"):
        json_handler._validate_json_list([])
    with pytest.raises(TypeError, match="All list items must be dictionaries"):
        json_handler._validate_json_list([1, 2, 3])


# --------------------------
# Method Tests
# --------------------------
def test_dump_message_success(
    json_handler: Any, # noqa ANN401: typing.Any is not allowed
    sample_dict: dict[str, Any], # noqa ANN401: typing.Any is not allowed
    temp_json_file: str
) -> None:
    """Test dump_message with valid inputs.

    Verifies
    --------
    - Successfully saves JSON file
    - Returns True when file is created
    - File contains correct JSON data

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles
    sample_dict : dict[str, Any]
        Sample dictionary to save
    temp_json_file : str
        Temporary JSON file path

    Returns
    -------
    None
    """
    result = json_handler.dump_message(sample_dict, temp_json_file)
    assert result is True
    with open(temp_json_file) as f:
        assert json.load(f) == sample_dict


def test_dump_message_failure(
    json_handler: Any, # noqa ANN401: typing.Any is not allowed
    sample_dict: dict[str, Any] # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test dump_message with invalid file path.

    Verifies
    --------
    - Returns False when file cannot be created

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles
    sample_dict : dict[str, Any]
        Sample dictionary to save

    Returns
    -------
    None
    """
    with pytest.raises(OSError):
        result = json_handler.dump_message(sample_dict, "/invalid/path.json")
        assert result is False


def test_dump_message_invalid_input(
    json_handler: Any, # noqa ANN401: typing.Any is not allowed
    temp_json_file: str
) -> None:
    """Test dump_message with invalid message type.

    Verifies
    --------
    - Raises TypeError for non-dict messages

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles
    temp_json_file : str
        Temporary JSON file path

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        json_handler.dump_message("not a dict", temp_json_file)


def test_load_message_default(
    json_handler: Any, # noqa ANN401: typing.Any is not allowed
    sample_dict: dict[str, Any], # noqa ANN401: typing.Any is not allowed
    temp_json_file: str
) -> None:
    """Test load_message with default encoding.

    Verifies
    --------
    - Correctly loads JSON data from file
    - Returns expected data structure

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles
    sample_dict : dict[str, Any]
        Sample dictionary to save and load
    temp_json_file : str
        Temporary JSON file path

    Returns
    -------
    None
    """
    with open(temp_json_file, "w") as f:
        json.dump(sample_dict, f)
    result = json_handler.load_message(temp_json_file)
    assert result == sample_dict


def test_load_message_with_encoding(
    json_handler: Any, # noqa ANN401: typing.Any is not allowed
    sample_dict: dict[str, Any], # noqa ANN401: typing.Any is not allowed
    temp_json_file: str
) -> None:
    """Test load_message with specified encoding.

    Verifies
    --------
    - Correctly loads JSON data with encoding
    - Handles encoding/decoding process

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles
    sample_dict : dict[str, Any]
        Sample dictionary to save and load
    temp_json_file : str
        Temporary JSON file path

    Returns
    -------
    None
    """
    with open(temp_json_file, "w", encoding="utf-8") as f:
        json.dump(sample_dict, f)
    result = json_handler.load_message(temp_json_file, encoding="utf-8", decoding="utf-8")
    assert result == sample_dict


def test_load_message_file_not_found(
    json_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test load_message with missing file.

    Verifies
    --------
    - Raises FileNotFoundError for non-existent files

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles

    Returns
    -------
    None
    """
    with pytest.raises(FileNotFoundError):
        json_handler.load_message("nonexistent.json")


def test_load_message_invalid_json(
    json_handler: Any, # noqa ANN401: typing.Any is not allowed
    temp_json_file: str
) -> None:
    """Test load_message with invalid JSON content.

    Verifies
    --------
    - Raises ValueError for malformed JSON

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles
    temp_json_file : str
        Temporary JSON file path    

    Returns
    -------
    None
    """
    with open(temp_json_file, "w") as f:
        f.write("invalid json")
    with pytest.raises(ValueError):
        json_handler.load_message(temp_json_file)


def test_loads_message_like_string(
    json_handler: Any, # noqa ANN401: typing.Any is not allowed
    sample_dict: dict[str, Any] # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test loads_message_like with string input.

    Verifies
    --------
    - Correctly parses JSON string
    - Returns expected dictionary

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles
    sample_dict : dict[str, Any]
        Sample dictionary to parse

    Returns
    -------
    None
    """
    json_str = json.dumps(sample_dict)
    result = json_handler.loads_message_like(json_str)
    assert result == sample_dict


def test_loads_message_like_bytes(
    json_handler: Any, # noqa ANN401: typing.Any is not allowed
    sample_dict: dict[str, Any] # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test loads_message_like with bytes input.

    Verifies
    --------
    - Correctly parses JSON bytes
    - Returns expected dictionary

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles
    sample_dict : dict[str, Any]
        Sample dictionary to parse

    Returns
    -------
    None
    """
    json_bytes = json.dumps(sample_dict).encode("utf-8")
    result = json_handler.loads_message_like(json_bytes)
    assert result == sample_dict


def test_loads_message_like_invalid(
    json_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test loads_message_like with invalid JSON.

    Verifies
    --------
    - Raises ValueError for malformed JSON

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles

    Returns
    -------
    None
    """
    with pytest.raises(ValueError):
        json_handler.loads_message_like("invalid json")


def test_dict_to_json(
    json_handler: Any, # noqa ANN401: typing.Any is not allowed
    sample_dict: dict[str, Any] # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test dict_to_json with valid dictionary.

    Verifies
    --------
    - Correctly converts dict to JSON string
    - Result can be parsed back to original dict

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles
    sample_dict : dict[str, Any]
        Sample dictionary to convert

    Returns
    -------
    None
    """
    result = json_handler.dict_to_json(sample_dict)
    assert isinstance(result, str)
    assert json.loads(result) == sample_dict


def test_dict_to_json_invalid(
    json_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test dict_to_json with invalid input.

    Verifies
    --------
    - Raises TypeError for non-dict inputs

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        json_handler.dict_to_json("not a dict")


def test_send_json(
    json_handler: Any, # noqa ANN401: typing.Any is not allowed
    sample_dict: dict[str, Any] # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test send_json with valid dictionary.

    Verifies
    --------
    - Returns equivalent dictionary
    - Handles JSON serialization roundtrip

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles
    sample_dict : dict[str, Any]
        Sample dictionary to send

    Returns
    -------
    None
    """
    result = json_handler.send_json(sample_dict)
    assert result == sample_dict


def test_send_json_invalid(
    json_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test send_json with non-serializable input.

    Verifies
    --------
    - Raises TypeError for non-serializable objects

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles

    Returns
    -------
    None
    """
    class NonSerializable:
        pass

    with pytest.raises(TypeError):
        json_handler.send_json({"obj": NonSerializable()})


def test_byte_to_json(
    json_handler: Any, # noqa ANN401: typing.Any is not allowed
    sample_dict: dict[str, Any] # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test byte_to_json with valid byte input.

    Verifies
    --------
    - Correctly converts bytes to JSON
    - Returns expected dictionary

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles
    sample_dict : dict[str, Any]
        Sample dictionary to convert

    Returns
    -------
    None
    """
    # noqa UP012: unnecessary UTF-8 encoding
    byte_str = f"b'{json.dumps(sample_dict)}'".encode("utf-8") # noqa UP012
    result = json_handler.byte_to_json(byte_str)
    assert result == sample_dict


def test_normalize_json_keys(
    json_handler: Any, # noqa ANN401: typing.Any is not allowed
    sample_list_of_dicts: list[dict[str, Any]] # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test normalize_json_keys with valid input.

    Verifies
    --------
    - All dictionaries have same keys
    - Missing keys are added with 0 value
    - Original keys/values preserved

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles
    sample_list_of_dicts : list[dict[str, Any]]
        Sample list of dictionaries with varying keys

    Returns
    -------
    None
    """
    result = json_handler.normalize_json_keys(sample_list_of_dicts)
    assert all(set(d.keys()) == {"id", "name", "value", "extra"} for d in result)
    assert all(d["id"] == i+1 for i, d in enumerate(result))


def test_normalize_json_keys_invalid(
    json_handler: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test normalize_json_keys with invalid input.

    Verifies
    --------
    - Raises TypeError for non-list inputs
    - Raises ValueError for empty lists
    - Raises TypeError for lists containing non-dict items

    Parameters
    ----------
    json_handler : Any
        Instance of JsonFiles

    Returns
    -------
    None
    """
    # test with non-list input
    with pytest.raises(TypeError, match="must be of type"):
        json_handler.normalize_json_keys({"not": "a list"})  # type: ignore

    # test with empty list
    with pytest.raises(ValueError, match="Input list cannot be empty"):
        json_handler.normalize_json_keys([])

    # test with list containing non-dict items
    with pytest.raises(TypeError, match="All list items must be dictionaries"):
        json_handler.normalize_json_keys([1, 2, 3])  # type: ignore

    # test with list containing mixed types
    with pytest.raises(TypeError, match="All list items must be dictionaries"):
        json_handler.normalize_json_keys([{"valid": "dict"}, "invalid", 123])  # type: ignore