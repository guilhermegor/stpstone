"""PDF handling utilities for extraction, conversion and table processing.

This module provides functionality for working with PDF files including:
- Text extraction from PDF pages
- Table data extraction
- PDF to base64 conversion
- Text to PDF conversion with customizable formatting
"""

from base64 import b64encode
import textwrap
from typing import Literal, Union

from fpdf import FPDF
from pypdf import PdfReader
import tabula

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.folders import DirFilesManagement


class PDFHandler(metaclass=TypeChecker):
    """Handler class for PDF file operations including extraction and conversion."""

    def _validate_pdf_filename(self, filename: str) -> None:
        """Validate PDF filename and extension.

        Parameters
        ----------
        filename : str
            Path to PDF file

        Raises
        ------
        ValueError
            If filename is empty
            If filename does not end with .pdf extension
        """
        if not filename:
            raise ValueError("Filename cannot be empty")
        if not filename.lower().endswith(".pdf"):
            raise ValueError("Filename must have .pdf extension")

    def fetch(
        self,
        complete_pdf_name: str,
        method_open: Literal['rb', 'r'] = "rb",
        str_return_num_pages_text: Literal['num_pages', 'text_pages'] = "num_pages"
    ) -> Union[int, list[str]]:
        """Fetch PDF metadata or text content.

        Parameters
        ----------
        complete_pdf_name : str
            Path to PDF file
        method_open : Literal['rb', 'r']
            File opening mode (default: 'rb')
        str_return_num_pages_text : Literal['num_pages', 'text_pages']
            Return type selector (default: "num_pages")

        Returns
        -------
        Union[int, list[str]]
            Number of pages or list of extracted text per page

        Raises
        ------
        ValueError
            If PDF file cannot be read
            If invalid return type specified
        """
        self._validate_pdf_filename(complete_pdf_name)

        try:
            with open(complete_pdf_name, method_open) as pdf_file_object:
                pdf_reader = PdfReader(pdf_file_object)
                
                if str_return_num_pages_text == "num_pages":
                    return len(pdf_reader.pages)
                elif str_return_num_pages_text == "text_pages":
                    return [
                        pdf_reader.pages[num_page].extract_text()
                        for num_page in range(len(pdf_reader.pages))
                    ]
                raise ValueError("Invalid return type specified")
        except Exception as err:
            raise ValueError(f"Failed to read PDF file: {str(err)}") from err

    def extract_tables(self, complete_pdf_name: str, str_num_pages: str) -> list[object]:
        """Extract table data from specified PDF pages.

        Parameters
        ----------
        complete_pdf_name : str
            Path to PDF file
        str_num_pages : str
            Page range specification (e.g. '1-3,5')

        Returns
        -------
        list[object]
            list of extracted tables

        Raises
        ------
        ValueError
            If PDF file cannot be read
            If no tables found
        """
        self._validate_pdf_filename(complete_pdf_name)

        try:
            tables = tabula.read_pdf(complete_pdf_name, pages=str_num_pages)
            if not tables:
                raise ValueError("No tables found in specified pages")
            return tables
        except Exception as err:
            raise ValueError(f"Failed to extract tables: {str(err)}") from err

    def pdf_to_base64(self, filename: str) -> str:
        """Convert PDF file to base64 encoded string.

        Parameters
        ----------
        filename : str
            Path to PDF file

        Returns
        -------
        str
            Base64 encoded string representation of PDF

        Raises
        ------
        ValueError
            If file cannot be read
        """
        self._validate_pdf_filename(filename)

        try:
            with open(filename, "rb") as pdf_file:
                return b64encode(pdf_file.read()).decode()
        except Exception as err:
            raise ValueError(f"Failed to convert PDF to base64: {str(err)}") from err

    def text_to_pdf(
        self,
        text: str,
        filename: str,
        a4_width_mm: int = 210,
        pt_to_mm: float = 0.35,
        fontsize_pt: int = 10,
        margin_bottom_mm: int = 10,
        character_width: int = 7,
        orientation: Literal['P', 'L'] = "P",
        unit: Literal['mm', 'pt', 'cm', 'in'] = "mm",
        format: str = "A4",
        font_family: str = "Courier",
        output_file: Literal['F', 'S'] = "F"
    ) -> bool:
        """Convert plain text to formatted PDF file.

        Parameters
        ----------
        text : str
            Input text to convert
        filename : str
            Output PDF filename
        a4_width_mm : int
            Page width in mm (default: 210)
        pt_to_mm : float
            Point to mm conversion factor (default: 0.35)
        fontsize_pt : int
            Font size in points (default: 10)
        margin_bottom_mm : int
            Bottom margin in mm (default: 10)
        character_width : int
            Character width (default: 7)
        orientation : Literal['P', 'L']
            Page orientation (default: "P")
        unit : Literal['mm', 'pt', 'cm', 'in']
            Measurement unit (default: "mm")
        format : str
            Page format (default: "A4")
        font_family : str
            Font family (default: "Courier")
        output_file : Literal['F', 'S']
            Output destination (default: 'F')

        Returns
        -------
        bool
            True if PDF was created successfully

        Raises
        ------
        ValueError
            If text is empty
            If PDF creation fails

        References
        ----------
        .. [1] https://stackoverflow.com/questions/10112244/convert-plain-text-to-pdf-in-python
        """
        if not text:
            raise ValueError("Input text cannot be empty")
        
        try:
            fontsize_mm = fontsize_pt * pt_to_mm
            character_width_mm = character_width * pt_to_mm
            width_text = a4_width_mm / character_width_mm

            pdf = FPDF(orientation=orientation, unit=unit, format=format)
            pdf.set_auto_page_break(True, margin=margin_bottom_mm)
            pdf.add_page()
            pdf.set_font(family=font_family, size=fontsize_pt)

            split = text.split("\n")
            for line in split:
                lines = textwrap.wrap(line, width_text)
                if len(lines) == 0:
                    pdf.ln()
                for wrap in lines:
                    pdf.cell(0, fontsize_mm, wrap, ln=1)

            pdf.output(filename, output_file)
            return DirFilesManagement().object_exists(filename)
        except Exception as err:
            raise ValueError(f"Failed to create PDF: {str(err)}") from err