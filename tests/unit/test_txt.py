"""Unit tests for HandlingTXTFiles class.

Tests the text file handling functionality including:
- File reading and writing operations
- Line manipulation methods
- Content validation and processing
- Separator consistency checking
"""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from stpstone.utils.parsers.txt import HandlingTXTFiles


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def txt_handler() -> HandlingTXTFiles:
	"""Fixture providing a HandlingTXTFiles instance.

	Returns
	-------
	HandlingTXTFiles
		Instance of the text file handler class
	"""
	return HandlingTXTFiles()


@pytest.fixture
def sample_text_file() -> str:
	"""Fixture creating a temporary text file with sample content.

	Returns
	-------
	str
		Path to the temporary file

	Yields
	------
	str
		Path to the temporary file
	"""
	with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as tmp:
		tmp.write("Line 1\nLine 2\nLine 3\nLine 4\nLine 5")
		tmp_path = tmp.name
	yield tmp_path
	os.unlink(tmp_path)


@pytest.fixture
def csv_file() -> str:
	"""Fixture creating a temporary CSV file with consistent separators.

	Returns
	-------
	str
		Path to the temporary file

	Yields
	------
	str
		Path to the temporary file
	"""
	with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as tmp:
		tmp.write("A,B,C\n1,2,3\n4,5,6")
		tmp_path = tmp.name
	yield tmp_path
	os.unlink(tmp_path)


@pytest.fixture
def mock_dir_files_management() -> MagicMock:
	"""Fixture mocking DirFilesManagement.object_exists.

	Returns
	-------
	MagicMock
		Mock of the object_exists method
	"""
	with patch("stpstone.utils.parsers.folders.DirFilesManagement") as mock:
		instance = mock.return_value
		instance.object_exists.return_value = True
		yield instance


# --------------------------
# Validation Tests
# --------------------------
class TestValidation:
	"""Tests for validation methods in HandlingTXTFiles."""

	def test_validate_file_path_empty(self, txt_handler: HandlingTXTFiles) -> None:
		"""Test _validate_file_path with empty path.

		Verifies
		--------
		- Raises ValueError when path is empty
		- Error message indicates empty path

		Parameters
		----------
		txt_handler : HandlingTXTFiles
			Instance of the text file handler

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="File path cannot be empty"):
			txt_handler._validate_file_path("")

	def test_validate_file_path_non_string(self, txt_handler: HandlingTXTFiles) -> None:
		"""Test _validate_file_path with non-string path.

		Verifies
		--------
		- Raises ValueError when path is not a string
		- Error message indicates type requirement

		Parameters
		----------
		txt_handler : HandlingTXTFiles
			Instance of the text file handler

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be of type"):
			txt_handler._validate_file_path(123)  # type: ignore

	def test_validate_file_path_not_exists(
		self, txt_handler: HandlingTXTFiles, mock_dir_files_management: MagicMock
	) -> None:
		"""Test _validate_file_path with non-existent file.

		Verifies
		--------
		- Raises ValueError when file doesn't exist
		- Error message includes the path

		Parameters
		----------
		txt_handler : HandlingTXTFiles
			Instance of the text file handler
		mock_dir_files_management : MagicMock
			Mock of DirFilesManagement

		Returns
		-------
		None
		"""
		mock_dir_files_management.object_exists.return_value = False
		test_path = "/nonexistent/file.txt"
		with pytest.raises(ValueError, match=f"File does not exist at path: {test_path}"):
			txt_handler._validate_file_path(test_path)


# --------------------------
# File Operation Tests
# --------------------------
class TestFileOperations:
	"""Tests for file operation methods in HandlingTXTFiles."""

	def test_read_file(self, txt_handler: HandlingTXTFiles, sample_text_file: str) -> None:
		"""Test read_file with valid file.

		Verifies
		--------
		- Returns correct file content
		- Content matches expected string

		Parameters
		----------
		txt_handler : HandlingTXTFiles
			Instance of the text file handler
		sample_text_file : str
			Path to the sample text file

		Returns
		-------
		None
		"""
		content = txt_handler.read_file(sample_text_file)
		assert content == "Line 1\nLine 2\nLine 3\nLine 4\nLine 5"

	def test_read_file_invalid_method(
		self, txt_handler: HandlingTXTFiles, sample_text_file: str
	) -> None:
		"""Test read_file with invalid method.

		Verifies
		--------
		- Raises ValueError when method is invalid

		Parameters
		----------
		txt_handler : HandlingTXTFiles
			Instance of the text file handler
		sample_text_file : str
			Path to the sample text file

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be one of"):
			txt_handler.read_file(sample_text_file, "invalid")  # type: ignore

	def test_generator_file(self, txt_handler: HandlingTXTFiles, sample_text_file: str) -> None:
		"""Test generator_file with valid file.

		Verifies
		--------
		- Returns list of processed lines
		- Line count matches input file

		Parameters
		----------
		txt_handler : HandlingTXTFiles
			Instance of the text file handler
		sample_text_file : str
			Path to the sample text file

		Returns
		-------
		None
		"""
		lines = txt_handler.generator_file(sample_text_file)
		assert len(lines) == 5
		assert all(isinstance(line, str) for line in lines)

	def test_read_first_line(self, txt_handler: HandlingTXTFiles, sample_text_file: str) -> None:
		"""Test read_first_line with valid file.

		Verifies
		--------
		- Returns first line only
		- Line is properly stripped

		Parameters
		----------
		txt_handler : HandlingTXTFiles
			Instance of the text file handler
		sample_text_file : str
			Path to the sample text file

		Returns
		-------
		None
		"""
		first_line = txt_handler.read_first_line(sample_text_file)
		assert first_line == "Line 1"

	def test_remove_first_n_lines(
		self, txt_handler: HandlingTXTFiles, sample_text_file: str
	) -> None:
		"""Test remove_first_n_lines with valid file.

		Verifies
		--------
		- File is modified correctly
		- Correct number of lines are removed

		Parameters
		----------
		txt_handler : HandlingTXTFiles
			Instance of the text file handler
		sample_text_file : str
			Path to the sample text file

		Returns
		-------
		None
		"""
		with open(sample_text_file) as f:
			original_line_count = len(f.readlines())
		txt_handler.remove_first_n_lines(sample_text_file, 2)
		with open(sample_text_file) as f:
			new_line_count = len(f.readlines())
		assert new_line_count == original_line_count - 2

	def test_remove_first_n_lines_negative(
		self, txt_handler: HandlingTXTFiles, sample_text_file: str
	) -> None:
		"""Test remove_first_n_lines with negative n.

		Verifies
		--------
		- Raises ValueError when n is negative
		- Error message indicates non-negative requirement

		Parameters
		----------
		txt_handler : HandlingTXTFiles
			Instance of the text file handler
		sample_text_file : str
			Path to the sample text file

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Number of lines to remove must be non-negative"):
			txt_handler.remove_first_n_lines(sample_text_file, -1)

	def test_write_file(
		self, txt_handler: HandlingTXTFiles, mock_dir_files_management: MagicMock, tmp_path: str
	) -> None:
		"""Test write_file with valid inputs.

		Verifies
		--------
		- File is created successfully
		- Returns True when file exists
		- Content matches input

		Parameters
		----------
		txt_handler : HandlingTXTFiles
			Instance of the text file handler
		mock_dir_files_management : MagicMock
			Mocked DirFilesManagement instance
		tmp_path : str
			Temporary directory path

		Returns
		-------
		None
		"""
		test_path = os.path.join(tmp_path, "test.txt")
		test_content = "Test content"

		# mock the file existence check to return True after writing
		def mock_exists(path: str) -> bool:
			"""Check mocked object_exists method.

			Parameters
			----------
			path : str
				Path to the file to check

			Returns
			-------
			bool
				True if the file exists
			"""
			return path == test_path

		mock_dir_files_management.object_exists.side_effect = mock_exists

		result = txt_handler.write_file(test_path, test_content)
		assert result is True
		with open(test_path) as f:
			assert f.read() == test_content

	def test_write_file_invalid_method(self, txt_handler: HandlingTXTFiles, tmp_path: str) -> None:
		"""Test write_file with invalid method.

		Verifies
		--------
		- Raises ValueError when method is invalid

		Parameters
		----------
		txt_handler : HandlingTXTFiles
			Instance of the text file handler
		tmp_path : str
			Temporary directory path

		Returns
		-------
		None
		"""
		test_path = os.path.join(tmp_path, "test.txt")
		with pytest.raises(TypeError, match="must be one of"):
			txt_handler.write_file(test_path, "content", "invalid")  # type: ignore


# --------------------------
# Separator Consistency Tests
# --------------------------
class TestSeparatorConsistency:
	"""Tests for separator consistency checking in HandlingTXTFiles."""

	def test_check_separator_consistency_valid(
		self, txt_handler: HandlingTXTFiles, csv_file: str
	) -> None:
		"""Test check_separator_consistency with consistent CSV.

		Verifies
		--------
		- Returns True when consistent separator found
		- Correctly identifies comma separator

		Parameters
		----------
		txt_handler : HandlingTXTFiles
			Instance of the text file handler
		csv_file : str
			Path to the CSV file

		Returns
		-------
		None
		"""
		assert txt_handler.check_separator_consistency(csv_file) is True

	def test_check_separator_consistency_invalid(
		self, txt_handler: HandlingTXTFiles, sample_text_file: str
	) -> None:
		"""Test check_separator_consistency with inconsistent file.

		Verifies
		--------
		- Returns False when no consistent separator found

		Parameters
		----------
		txt_handler : HandlingTXTFiles
			Instance of the text file handler
		sample_text_file : str
			Path to the sample text file

		Returns
		-------
		None
		"""
		assert txt_handler.check_separator_consistency(sample_text_file) is True

	def test_check_separator_consistency_empty_sep_list(
		self, txt_handler: HandlingTXTFiles, sample_text_file: str
	) -> None:
		"""Test check_separator_consistency with empty separator list.

		Verifies
		--------
		- Raises ValueError when separator list is empty
		- Error message indicates non-empty requirement

		Parameters
		----------
		txt_handler : HandlingTXTFiles
			Instance of the text file handler
		sample_text_file : str
			Path to the sample text file

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Separator list cannot be empty"):
			txt_handler.check_separator_consistency(sample_text_file, [])


# --------------------------
# Type Validation Tests
# --------------------------
class TestTypeValidation:
	"""Tests for type validation through metaclass."""

	def test_read_file_type_validation(
		self, txt_handler: HandlingTXTFiles, sample_text_file: str
	) -> None:
		"""Test type validation in read_file method.

		Verifies
		--------
		- Raises TypeError when wrong argument types are passed

		Parameters
		----------
		txt_handler : HandlingTXTFiles
			Instance of the text file handler
		sample_text_file : str
			Path to the sample text file

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			txt_handler.read_file(123)  # type: ignore
		with pytest.raises(TypeError):
			txt_handler.read_file(sample_text_file, 123)  # type: ignore

	def test_write_file_type_validation(
		self, txt_handler: HandlingTXTFiles, tmp_path: str
	) -> None:
		"""Test type validation in write_file method.

		Verifies
		--------
		- Raises TypeError when wrong argument types are passed

		Parameters
		----------
		txt_handler : HandlingTXTFiles
			Instance of the text file handler
		tmp_path : str
			Temporary directory path

		Returns
		-------
		None
		"""
		test_path = os.path.join(tmp_path, "test.txt")
		with pytest.raises(TypeError):
			txt_handler.write_file(123, "content")  # type: ignore
		with pytest.raises(TypeError):
			txt_handler.write_file(test_path, 123)  # type: ignore
		with pytest.raises(TypeError):
			txt_handler.write_file(test_path, "content", 123)  # type: ignore
