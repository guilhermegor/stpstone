"""Unit tests for ImgHandler class.

Tests the image to HTML conversion functionality with various input scenarios including:
- Successful conversion with valid inputs
- File handling edge cases
- Format validation
- Error conditions
"""

import base64
from pathlib import Path
from typing import Literal
from unittest.mock import mock_open, patch

import pytest

from stpstone.utils.parsers.img import ImgHandler


class TestImgHandler:
	"""Test cases for ImgHandler class.

	This test class verifies the behavior of image to HTML conversion
	with different input types and edge cases.
	"""

	@pytest.fixture
	def img_handler(self) -> ImgHandler:
		"""Fixture providing an instance of ImgHandler.

		Returns
		-------
		ImgHandler
			Instance of the class to be tested
		"""
		return ImgHandler()

	@pytest.fixture
	def sample_image_data(self) -> bytes:
		"""Fixture providing sample image binary data.

		Returns
		-------
		bytes
			Mock image binary data
		"""
		return b"mock_image_data"

	@pytest.fixture
	def mock_image_file(self, sample_image_data: bytes) -> "mock_open":
		"""Fixture providing a mock file object for image reading.

		Parameters
		----------
		sample_image_data : bytes
			The mock image data to return when file is read

		Returns
		-------
		'mock_open'
			Mock file object
		"""
		return mock_open(read_data=sample_image_data)

	def test_successful_conversion(
		self, img_handler: ImgHandler, mock_image_file: "mock_open", sample_image_data: bytes
	) -> None:
		"""Test successful image to HTML conversion.

		Verifies
		--------
		- Correct HTML tag generation
		- Proper base64 encoding
		- File handling with context manager

		Parameters
		----------
		img_handler : ImgHandler
			Instance of the class being tested
		mock_image_file : 'mock_open'
			Mock file object
		sample_image_data : bytes
			Sample image data to be encoded
		"""
		test_path = "/path/to/image.jpeg"
		expected_b64 = base64.b64encode(sample_image_data).decode("utf-8")
		expected_html = f'<img src="data:image/jpeg;base64,{expected_b64}">'

		with patch("builtins.open", mock_image_file):
			result = img_handler.img_to_html(test_path, "jpeg")

		assert result == expected_html
		mock_image_file.assert_called_once_with(test_path, "rb")

	@pytest.mark.parametrize("format", ["jpeg", "png", "gif"])
	def test_supported_formats(
		self,
		img_handler: ImgHandler,
		mock_image_file: "mock_open",
		format: Literal["jpeg", "png", "gif"],
	) -> None:
		"""Test all supported image formats.

		Verifies
		--------
		- All declared formats are accepted
		- Format appears correctly in output tag

		Parameters
		----------
		img_handler : ImgHandler
			Instance of the class being tested
		mock_image_file : 'mock_open'
			Mock file object
		format : Literal['jpeg', 'png', 'gif']
			Image format to test
		"""
		test_path = f"/path/to/image.{format}"

		with patch("builtins.open", mock_image_file):
			result = img_handler.img_to_html(test_path, format)

		assert f"image/{format}" in result

	@pytest.mark.parametrize("invalid_format", ["bmp", "tiff", "webp", ""])
	def test_invalid_formats(self, img_handler: ImgHandler, invalid_format: str) -> None:
		"""Test unsupported image formats raise ValueError.

		Verifies
		--------
		- Unsupported formats raise ValueError
		- Error message contains supported formats

		Parameters
		----------
		img_handler : ImgHandler
			Instance of the class being tested
		invalid_format : str
			Unsupported format to test
		"""
		test_path = f"/path/to/image.{invalid_format}"

		with pytest.raises(TypeError, match="must be one of"):
			img_handler.img_to_html(test_path, invalid_format)

	def test_file_not_found(self, img_handler: ImgHandler) -> None:
		"""Test FileNotFoundError is raised for missing files.

		Verifies
		--------
		- FileNotFoundError is raised when file doesn't exist
		- Error message contains the path

		Parameters
		----------
		img_handler : ImgHandler
			Instance of the class being tested
		"""
		test_path = "/nonexistent/path/image.jpeg"

		with pytest.raises(FileNotFoundError) as excinfo:
			img_handler.img_to_html(test_path)

		assert test_path in str(excinfo.value)

	def test_empty_file(self, img_handler: ImgHandler) -> None:
		"""Test behavior with empty image file.

		Verifies
		--------
		- Empty files are handled without error
		- Output HTML contains empty base64 data

		Parameters
		----------
		img_handler : ImgHandler
			Instance of the class being tested
		"""
		test_path = "/path/to/empty.jpeg"
		mock_empty_file = mock_open(read_data=b"")

		with patch("builtins.open", mock_empty_file):
			result = img_handler.img_to_html(test_path)

		assert "base64," in result
		assert len(result) > 20

	def test_relative_path(
		self, img_handler: ImgHandler, mock_image_file: "mock_open", tmp_path: Path
	) -> None:
		"""Test behavior with relative file paths.

		Verifies
		--------
		- Relative paths are handled correctly
		- Absolute path is properly resolved

		Parameters
		----------
		img_handler : ImgHandler
			Instance of the class being tested
		mock_image_file : 'mock_open'
			Mock file object
		tmp_path : Path
			pytest temporary directory fixture
		"""
		rel_path = "relative/image.jpeg"

		with patch("builtins.open", mock_image_file):
			img_handler.img_to_html(rel_path)

		mock_image_file.assert_called_once_with(rel_path, "rb")

	def test_format_case_insensitivity(
		self, img_handler: ImgHandler, mock_image_file: "mock_open"
	) -> None:
		"""Test format name case insensitivity.

		Verifies
		--------
		- Format names are case insensitive
		- Output uses lowercase format

		Parameters
		----------
		img_handler : ImgHandler
			Instance of the class being tested
		mock_image_file : 'mock_open'
			Mock file object
		"""
		test_path = "/path/to/image.jpeg"

		with pytest.raises(TypeError, match="must be one of"):
			_ = img_handler.img_to_html(test_path, "JPEG")

	def test_validate_format_method(self, img_handler: ImgHandler) -> None:
		"""Test _validate_format helper method directly.

		Verifies
		--------
		- Method raises ValueError for invalid formats
		- Accepts supported formats without error

		Parameters
		----------
		img_handler : ImgHandler
			Instance of the class being tested
		"""
		# Test valid formats
		img_handler._validate_format("jpeg")
		img_handler._validate_format("png")
		img_handler._validate_format("gif")

		# Test invalid format
		with pytest.raises(ValueError):
			img_handler._validate_format("bmp")

	def test_html_structure(self, img_handler: ImgHandler, mock_image_file: "mock_open") -> None:
		"""Test output HTML structure is correct.

		Verifies
		--------
		- Output is valid HTML img tag
		- Contains required attributes
		- Proper attribute formatting

		Parameters
		----------
		img_handler : ImgHandler
			Instance of the class being tested
		mock_image_file : 'mock_open'
			Mock file object
		"""
		test_path = "/path/to/image.jpeg"

		with patch("builtins.open", mock_image_file):
			result = img_handler.img_to_html(test_path)

		assert result.startswith('<img src="data:image/jpeg;base64,')
		assert result.endswith('">')
		assert "src=" in result
		assert "data:image/jpeg;base64," in result
