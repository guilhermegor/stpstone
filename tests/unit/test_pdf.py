"""Unit tests for PDFHandler class.

Tests the PDF handling functionality including:
- PDF metadata and text extraction
- Table data extraction
- PDF to base64 conversion
- Text to PDF conversion
"""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from fpdf import FPDF
import pytest

from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.pdf import PDFHandler


@pytest.fixture
def pdf_handler() -> Any:  # noqa ANN401: typing.Any is not allowed
	"""Fixture providing a PDFHandler instance.

	Returns
	-------
	Any
		Instance of PDFHandler class
	"""
	return PDFHandler()


@pytest.fixture
def sample_pdf_path(tmp_path: Path) -> Path:
	"""Fixture creating a sample PDF file for testing.

	Parameters
	----------
	tmp_path : Path
		Pytest temporary directory fixture

	Returns
	-------
	Path
		Path to the sample PDF file
	"""
	pdf_path = tmp_path / "test.pdf"
	pdf = FPDF()
	pdf.add_page()
	pdf.set_font("Arial", size=12)
	pdf.cell(200, 10, txt="Test PDF", ln=True)
	pdf.output(pdf_path)
	return pdf_path


@pytest.fixture
def sample_text() -> str:
	"""Fixture providing sample text for PDF conversion.

	Returns
	-------
	str
		Sample text for PDF conversion
	"""
	return "This is a test string\nwith multiple lines\nfor PDF conversion"


def test_validate_pdf_filename_empty(
	pdf_handler: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test validation with empty filename.

	Parameters
	----------
	pdf_handler : Any
		Instance of PDFHandler class

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Filename cannot be empty"):
		pdf_handler._validate_pdf_filename("")


def test_validate_pdf_filename_non_pdf(
	pdf_handler: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test validation with non-PDF extension.

	Parameters
	----------
	pdf_handler : Any
		Instance of PDFHandler class

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Filename must have .pdf extension"):
		pdf_handler._validate_pdf_filename("test.txt")


def test_validate_pdf_filename_valid(
	pdf_handler: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test validation with valid PDF filename.

	Parameters
	----------
	pdf_handler : Any
		Instance of PDFHandler class

	Returns
	-------
	None
	"""
	pdf_handler._validate_pdf_filename("test.pdf")


def test_fetch_num_pages(
	pdf_handler: Any,  # noqa ANN401: typing.Any is not allowed
	sample_pdf_path: Path,
) -> None:
	"""Test fetching number of pages from PDF.

	Parameters
	----------
	pdf_handler : Any
		Instance of PDFHandler class
	sample_pdf_path : Path
		Path to sample PDF file

	Returns
	-------
	None
	"""
	with patch("pypdf.PdfReader") as mock_reader:
		mock_reader.return_value.pages = [MagicMock()] * 1
		result = pdf_handler.fetch(str(sample_pdf_path), "rb", "num_pages")
		assert result == 1


def test_fetch_text_pages(
	pdf_handler: Any,  # noqa ANN401: typing.Any is not allowed
	sample_pdf_path: Path,
) -> None:
	"""Test fetching text from PDF pages.

	Parameters
	----------
	pdf_handler : Any
		Instance of PDFHandler class
	sample_pdf_path : Path
		Path to sample PDF file

	Returns
	-------
	None
	"""
	with patch("pypdf.PdfReader") as mock_reader:
		mock_page = MagicMock()
		mock_page.extract_text.return_value = "sample text"
		mock_reader.return_value.pages = [mock_page, mock_page]  # 2 pages
		result = pdf_handler.fetch(str(sample_pdf_path), "rb", "text_pages")
		assert result == ["Test PDF"]


def test_fetch_invalid_return_type(
	pdf_handler: Any,  # noqa ANN401: typing.Any is not allowed
	sample_pdf_path: Path,
) -> None:
	"""Test fetch with invalid return type.

	Parameters
	----------
	pdf_handler : Any
		Instance of PDFHandler class
	sample_pdf_path : Path
		Path to sample PDF file

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be one of"):
		pdf_handler.fetch(str(sample_pdf_path), "rb", "invalid_type")


def test_fetch_file_not_found(
	pdf_handler: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test fetch with non-existent file.

	Parameters
	----------
	pdf_handler : Any
		Instance of PDFHandler class

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Failed to read PDF file"):
		pdf_handler.fetch("nonexistent.pdf", "rb", "num_pages")


def test_extract_tables_success(
	pdf_handler: Any,  # noqa ANN401: typing.Any is not allowed
	sample_pdf_path: Path,
) -> None:
	"""Test successful table extraction.

	Parameters
	----------
	pdf_handler : Any
		Instance of PDFHandler class
	sample_pdf_path : Path
		Path to sample PDF file

	Returns
	-------
	None
	"""
	with patch("tabula.read_pdf") as mock_read:
		mock_read.return_value = [{"data": "sample"}]
		result = pdf_handler.extract_tables(str(sample_pdf_path), "1")
		assert len(result) == 1


def test_extract_tables_no_tables(
	pdf_handler: Any,  # noqa ANN401: typing.Any is not allowed
	sample_pdf_path: Path,
) -> None:
	"""Test table extraction with no tables found.

	Parameters
	----------
	pdf_handler : Any
		Instance of PDFHandler class
	sample_pdf_path : Path
		Path to sample PDF file

	Returns
	-------
	None
	"""
	with patch("tabula.read_pdf") as mock_read:
		mock_read.return_value = []
		with pytest.raises(ValueError, match="No tables found in specified pages"):
			pdf_handler.extract_tables(str(sample_pdf_path), "1")


def test_extract_tables_error(
	pdf_handler: Any,  # noqa ANN401: typing.Any is not allowed
	sample_pdf_path: Path,
) -> None:
	"""Test table extraction with error.

	Parameters
	----------
	pdf_handler : Any
		Instance of PDFHandler class
	sample_pdf_path : Path
		Path to sample PDF file

	Returns
	-------
	None
	"""
	with patch("tabula.read_pdf") as mock_read:
		mock_read.side_effect = Exception("Tabula error")
		with pytest.raises(ValueError, match="Failed to extract tables"):
			pdf_handler.extract_tables(str(sample_pdf_path), "1")


def test_pdf_to_base64_success(
	pdf_handler: Any,  # noqa ANN401: typing.Any is not allowed
	sample_pdf_path: Path,
) -> None:
	"""Test successful PDF to base64 conversion.

	Parameters
	----------
	pdf_handler : Any
		Instance of PDFHandler class
	sample_pdf_path : Path
		Path to sample PDF file

	Returns
	-------
	None
	"""
	result = pdf_handler.pdf_to_base64(str(sample_pdf_path))
	assert isinstance(result, str)
	assert len(result) > 0


def test_pdf_to_base64_error(
	pdf_handler: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test PDF to base64 with error.

	Parameters
	----------
	pdf_handler : Any
		Instance of PDFHandler class

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Failed to convert PDF to base64"):
		pdf_handler.pdf_to_base64("nonexistent.pdf")


def test_text_to_pdf_success(
	pdf_handler: Any,  # noqa ANN401: typing.Any is not allowed
	sample_text: str,
	tmp_path: Path,
) -> None:
	"""Test successful text to PDF conversion.

	Parameters
	----------
	pdf_handler : Any
		Instance of PDFHandler class
	sample_text : str
		Sample text to convert
	tmp_path : Path
		Temporary directory path

	Returns
	-------
	None
	"""
	output_path = tmp_path / "output.pdf"
	with patch.object(DirFilesManagement, "object_exists", return_value=True):
		result = pdf_handler.text_to_pdf(sample_text, str(output_path))
		assert result is True


def test_text_to_pdf_empty_text(
	pdf_handler: Any,  # noqa ANN401: typing.Any is not allowed
	tmp_path: Path,
) -> None:
	"""Test text_to_pdf with empty text.

	Parameters
	----------
	pdf_handler : Any
		Instance of PDFHandler class
	tmp_path : Path
		Temporary directory path

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Input text cannot be empty"):
		pdf_handler.text_to_pdf("", str(tmp_path / "empty.pdf"))


def test_text_to_pdf_creation_error(
	pdf_handler: Any,  # noqa ANN401: typing.Any is not allowed
	sample_text: str,
	tmp_path: Path,
) -> None:
	"""Test text_to_pdf with creation error.

	Parameters
	----------
	pdf_handler : Any
		Instance of PDFHandler class
	sample_text : str
		Sample text to convert
	tmp_path : Path
		Temporary directory path

	Returns
	-------
	None
	"""
	with (
		patch.object(FPDF, "output", side_effect=Exception("PDF error")),
		pytest.raises(ValueError, match="Failed to create PDF"),
	):
		pdf_handler.text_to_pdf(sample_text, str(tmp_path / "error.pdf"))


def test_text_to_pdf_file_not_created(
	pdf_handler: Any,  # noqa ANN401: typing.Any is not allowed
	sample_text: str,
	tmp_path: Path,
) -> None:
	"""Test text_to_pdf when file is not created.

	Parameters
	----------
	pdf_handler : Any
		Instance of PDFHandler class
	sample_text : str
		Sample text to convert
	tmp_path : Path
		Temporary directory path

	Returns
	-------
	None
	"""
	output_path = tmp_path / "not_created.pdf"
	with patch.object(DirFilesManagement, "object_exists", return_value=False):
		result = pdf_handler.text_to_pdf(sample_text, str(output_path))
		assert result is False
