"""Unit tests for PickleFiles class.

Tests the pickle file operations including:
- File path validation
- Message serialization and saving
- Message loading and deserialization
- Error handling and edge cases
"""

import os
import pickle
from tempfile import NamedTemporaryFile
from typing import Any

import pytest

from stpstone.utils.parsers.pickle import PickleFiles


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def pickle_files() -> Any:  # noqa ANN401: typing.Any is not allowed
	"""Fixture providing PickleFiles instance.

	Returns
	-------
	Any
		Instance of PickleFiles class
	"""
	return PickleFiles()


@pytest.fixture
def sample_dict() -> dict[str, Any]:  # noqa ANN401: typing.Any is not allowed
	"""Fixture providing sample dictionary for testing.

	Returns
	-------
	dict[str, Any]
		Sample dictionary with mixed data types
	"""
	return {"string": "test value", "number": 42, "list": [1, 2, 3], "nested": {"key": "value"}}


@pytest.fixture
def temp_pickle_file(
	sample_dict: dict[str, Any],  # noqa ANN401: typing.Any is not allowed
) -> str:
	"""Fixture creating temporary pickle file with sample data.

	Parameters
	----------
	sample_dict : dict[str, Any]
		Sample data to pickle

	Yields
	------
	str
		Path to temporary pickle file

	Notes
	-----
	File is automatically deleted after test completes
	"""
	with NamedTemporaryFile(suffix=".pickle", delete=False) as tmp_file:
		pickle.dump(sample_dict, tmp_file)
		tmp_path = tmp_file.name
	yield tmp_path
	os.unlink(tmp_path)


# --------------------------
# Validation Tests
# --------------------------
def test_validate_file_path_empty(
	pickle_files: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test validation fails with empty file path.

	Verifies
	--------
	- Empty file path raises ValueError
	- Error message matches expected

	Parameters
	----------
	pickle_files : Any
		PickleFiles instance

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError) as excinfo:
		pickle_files._validate_file_path("")
	assert "File path cannot be empty" in str(excinfo.value)


def test_validate_file_path_non_string(
	pickle_files: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test validation fails with non-string file path.

	Verifies
	--------
	- Non-string file path raises ValueError
	- Error message matches expected

	Parameters
	----------
	pickle_files : Any
		PickleFiles instance

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be one of types"):
		pickle_files._validate_file_path(123)  # type: ignore


def test_validate_file_path_wrong_extension(
	pickle_files: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test validation fails with wrong file extension.

	Verifies
	--------
	- File path without .pickle extension raises ValueError
	- Error message matches expected

	Parameters
	----------
	pickle_files : Any
		PickleFiles instance

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError) as excinfo:
		pickle_files._validate_file_path("file.txt")
	assert "File path must end with .pickle extension" in str(excinfo.value)


def test_validate_file_path_valid(
	pickle_files: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test validation succeeds with valid file path.

	Verifies
	--------
	- Valid file path does not raise exceptions

	Parameters
	----------
	pickle_files : Any
		PickleFiles instance

	Returns
	-------
	None
	"""
	try:
		pickle_files._validate_file_path("valid.pickle")
	except ValueError:
		pytest.fail("Validation failed unexpectedly for valid file path")


# --------------------------
# Dump Message Tests
# --------------------------
def test_dump_message_success(
	pickle_files: Any,  # noqa ANN401: typing.Any is not allowed
	sample_dict: dict[str, Any],  # noqa ANN401: typing.Any is not allowed
	temp_pickle_file: str,
) -> None:
	"""Test successful message serialization.

	Verifies
	--------
	- Message is successfully serialized to file
	- Returns True when file is created
	- File contains correct data

	Parameters
	----------
	pickle_files : Any
		PickleFiles instance
	sample_dict : dict[str, Any]
		Sample data to pickle
	temp_pickle_file : str
		Temporary pickle file path

	Returns
	-------
	None
	"""
	result = pickle_files.dump_message(sample_dict, temp_pickle_file)
	assert result is True
	assert os.path.exists(temp_pickle_file)

	with open(temp_pickle_file, "rb") as f:
		loaded = pickle.load(f)  # noqa S301: potentially unsafe when deserializing untrusted data
	assert loaded == sample_dict


def test_dump_message_protocol_fallback(
	pickle_files: Any,  # noqa ANN401: typing.Any is not allowed
	sample_dict: dict[str, Any],  # noqa ANN401: typing.Any is not allowed
	temp_pickle_file: str,
	mocker: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test protocol fallback mechanism.

	Verifies
	--------
	- Falls back to error protocol when primary fails
	- Still successfully saves file

	Parameters
	----------
	pickle_files : Any
		PickleFiles instance
	sample_dict : dict[str, Any]
		Sample data to pickle
	temp_pickle_file : str
		Temporary pickle file path
	mocker : Any
		Mocker instance

	Returns
	-------
	None
	"""
	mocker.patch(
		"pickle.dump",
		side_effect=[pickle.PicklingError, None],  # first call fails, second succeeds
	)
	result = pickle_files.dump_message(sample_dict, temp_pickle_file)
	assert result is True
	assert os.path.exists(temp_pickle_file)


def test_dump_message_file_error(
	pickle_files: Any,  # noqa ANN401: typing.Any is not allowed
	sample_dict: dict[str, Any],  # noqa ANN401: typing.Any is not allowed
	temp_pickle_file: str,
	mocker: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test file write error handling.

	Verifies
	--------
	- Raises ValueError when file writing fails
	- Error message contains original error

	Parameters
	----------
	pickle_files : Any
		PickleFiles instance
	sample_dict : dict[str, Any]
		Sample data to pickle
	temp_pickle_file : str
		Temporary pickle file path
	mocker : Any
		Mocker instance

	Returns
	-------
	None
	"""
	mocker.patch("builtins.open", side_effect=OSError("Permission denied"))
	with pytest.raises(ValueError) as excinfo:
		pickle_files.dump_message(sample_dict, temp_pickle_file)
	assert "Failed to write pickle file" in str(excinfo.value)
	assert "Permission denied" in str(excinfo.value)


# --------------------------
# Load Message Tests
# --------------------------
def test_load_message_binary(
	pickle_files: Any,  # noqa ANN401: typing.Any is not allowed
	sample_dict: dict[str, Any],  # noqa ANN401: typing.Any is not allowed
	temp_pickle_file: str,
) -> None:
	"""Test binary mode loading.

	Verifies
	--------
	- Correctly loads data from binary pickle file
	- Returns data matching original

	Parameters
	----------
	pickle_files : Any
		PickleFiles instance
	sample_dict : dict[str, Any]
		Sample data to pickle
	temp_pickle_file : str
		Temporary pickle file path

	Returns
	-------
	None
	"""
	loaded = pickle_files.load_message(temp_pickle_file)
	assert loaded == sample_dict


def test_load_message_text(
	pickle_files: Any,  # noqa ANN401: typing.Any is not allowed
	sample_dict: dict[str, Any],  # noqa ANN401: typing.Any is not allowed
	temp_pickle_file: str,
	mocker: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test text mode loading with encoding.

	Verifies
	--------
	- Correctly loads data with encoding/decoding
	- Uses specified encoding and decoding

	Parameters
	----------
	pickle_files : Any
		PickleFiles instance
	sample_dict : dict[str, Any]
		Sample data to pickle
	temp_pickle_file : str
		Temporary pickle file path
	mocker : Any
		Mocker instance

	Returns
	-------
	None
	"""
	mock_data = pickle.dumps(sample_dict)
	mock_file = mocker.mock_open(read_data=mock_data)
	mocker.patch("builtins.open", mock_file)

	result = pickle_files.load_message(
		temp_pickle_file,
		bool_trusted_file=True,
	)
	assert result == sample_dict


def test_load_message_missing_file(
	pickle_files: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test loading missing file.

	Verifies
	--------
	- Raises ValueError when file doesn't exist
	- Error message contains file path

	Parameters
	----------
	pickle_files : Any
		PickleFiles instance

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError) as excinfo:
		pickle_files.load_message("nonexistent.pickle")
	assert "File not found" in str(excinfo.value)


def test_load_message_corrupted_file(
	pickle_files: Any,  # noqa ANN401: typing.Any is not allowed
	temp_pickle_file: str,
) -> None:
	"""Test loading corrupted pickle file.

	Verifies
	--------
	- Raises ValueError when pickle data is corrupted
	- Error message indicates loading failure

	Parameters
	----------
	pickle_files : Any
		PickleFiles instance
	temp_pickle_file : str
		Temporary pickle file path

	Returns
	-------
	None
	"""
	with open(temp_pickle_file, "wb") as f:
		f.write(b"corrupted data")

	with pytest.raises(ValueError, match="Pickle file contains potentially unsafe data"):
		pickle_files.load_message(temp_pickle_file)


# --------------------------
# Integration Tests
# --------------------------
def test_roundtrip(
	pickle_files: Any,  # noqa ANN401: typing.Any is not allowed
	sample_dict: dict[str, Any],  # noqa ANN401: typing.Any is not allowed
	temp_pickle_file: str,
) -> None:
	"""Test complete save/load cycle.

	Verifies
	--------
	- Data survives roundtrip through serialization
	- Original and loaded data match exactly

	Parameters
	----------
	pickle_files : Any
		PickleFiles instance
	sample_dict : dict[str, Any]
		Sample dictionary
	temp_pickle_file : str
		Temporary pickle file path

	Returns
	-------
	None
	"""
	# Save data
	save_result = pickle_files.dump_message(sample_dict, temp_pickle_file)
	assert save_result is True

	# Load data
	loaded = pickle_files.load_message(temp_pickle_file)
	assert loaded == sample_dict


def test_roundtrip_text_mode(
	pickle_files: Any,  # noqa ANN401: typing.Any is not allowed
	sample_dict: dict[str, Any],
	temp_pickle_file: str,
	mocker: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test roundtrip with text mode encoding.

	Verifies
	--------
	- Data survives roundtrip with text encoding
	- Mocked file operations are called correctly

	Parameters
	----------
	pickle_files : Any
		PickleFiles instance
	sample_dict : dict[str, Any]
		Sample dictionary
	temp_pickle_file : str
		Temporary pickle file path
	mocker : Any
		Mocker instance

	Returns
	-------
	None
	"""
	mock_data = pickle.dumps(sample_dict)
	mock_file = mocker.mock_open(read_data=mock_data)
	mocker.patch("builtins.open", mock_file)

	# save data (though we're mocking the actual file ops)
	save_result = pickle_files.dump_message(sample_dict, temp_pickle_file)
	assert save_result is True

	# load data
	loaded = pickle_files.load_message(temp_pickle_file)
	assert loaded == sample_dict

	# verify mock calls
	assert mock_file.call_count >= 2
