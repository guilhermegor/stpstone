### MODULE TO DEAL WITH PDF, FROM EXTRACTION TO HANDLING DATA ###

import tabula
import textwrap
from PyPDF2 import PdfFileReader
from fpdf import FPDF
from base64 import b64encode
from stpstone.utils.parsers.folders import DirFilesManagement


class PDFHandler:

    def fetch(self, complete_pdf_name, method_open='rb', str_return_num_pages_text='num_pages'):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # import to memory pdf object
        pdf_file_object = open(complete_pdf_name, method_open)
        # pdf reader object
        pdf_reader = PdfFileReader(pdf_file_object)
        # return number of pages and text
        if str_return_num_pages_text == 'num_pages':
            return pdf_reader.numPages
        elif str_return_num_pages_text == 'text_pages':
            return [pdf_reader.getPage(num_page).extractText() for num_page in range(
                1, pdf_reader.numPages)]

    def extract_tables(self, complete_pdf_name, str_num_pages):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # returning table data withn pages of pdf file
        return tabula.read_pdf(complete_pdf_name, pages=str_num_pages)

    def pdf_to_base64(self, filename):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS
        """
        with open(filename, 'rb') as pdf_file:
            return b64encode(pdf_file.read()).decode()

    def text_to_pdf(text, filename, a4_width_mm=210, pt_to_mm=0.35, fontsize_pt=10,
                    margin_bottom_mm=10,
                    character_width=7, orientation='P', unit='mm', format='A4', font_family='Courier',
                    output_file='F'):
        """
        REFERENCES: https://stackoverflow.com/questions/10112244/convert-plain-text-to-pdf-in-python
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # setting variables
        fontsize_mm = fontsize_pt * pt_to_mm
        character_width_mm = character_width * pt_to_mm
        width_text = a4_width_mm / character_width_mm
        # set pdf layout
        pdf = FPDF(orientation=orientation, unit=unit, format=format)
        pdf.set_auto_page_break(True, margin=margin_bottom_mm)
        pdf.add_page()
        pdf.set_font(family=font_family, size=fontsize_pt)
        splitted = text.split('\n')
        # loop through lines of text
        for line in splitted:
            lines = textwrap.wrap(line, width_text)
            if len(lines) == 0:
                pdf.ln()
            for wrap in lines:
                pdf.cell(0, fontsize_mm, wrap, ln=1)
        pdf.output(filename, output_file)
        # checking wheter the file was created
        return DirFilesManagement().object_exists(filename)
