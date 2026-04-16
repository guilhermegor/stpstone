"""Unit tests for HTML handling and building utilities.

Tests the functionality of HtmlHandler and HtmlBuilder classes including:
- HTML parsing with BeautifulSoup and lxml
- XPath query execution
- HTML tree manipulation and output
- HTML to text conversion
- Programmatic HTML tag building
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

from bs4 import BeautifulSoup
from lxml import html
import pytest
from requests import HTTPError, Response

from stpstone.utils.parsers.html import HtmlBuilder, HtmlHandler


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_response() -> Response:
	"""Fixture providing a mock Response object with HTML content.

	Returns
	-------
	Response
		Mocked Response object with sample HTML content
	"""
	mock_response = MagicMock(spec=Response)
	mock_response.content = b"<html><body><p>Test</p></body></html>"
	return mock_response


@pytest.fixture
def sample_html_element() -> html.HtmlElement:
	"""Fixture providing a parsed lxml HTML element.

	Returns
	-------
	html.HtmlElement
		Parsed HTML element from sample HTML string
	"""
	return html.fromstring("<html><body><p>Test</p></body></html>")


@pytest.fixture
def html_handler() -> HtmlHandler:
	"""Fixture providing an instance of HtmlHandler.

	Returns
	-------
	HtmlHandler
		Instance of HtmlHandler class
	"""
	return HtmlHandler()


@pytest.fixture
def html_builder() -> HtmlBuilder:
	"""Fixture providing an instance of HtmlBuilder.

	Returns
	-------
	HtmlBuilder
		Instance of HtmlBuilder class
	"""
	return HtmlBuilder()


# --------------------------
# HtmlHandler Tests
# --------------------------
class TestHtmlHandler:
	"""Test cases for HtmlHandler class."""

	def test_bs_parser_success(self, html_handler: HtmlHandler, sample_response: Response) -> None:
		"""Test successful BeautifulSoup parsing.

		Verifies
		--------
		- Returns BeautifulSoup object with correct content
		- Parses the response content correctly

		Parameters
		----------
		html_handler : HtmlHandler
			Instance of HtmlHandler to test
		sample_response : Response
			Mocked HTTP response object with sample HTML content

		Returns
		-------
		None
		"""
		result = html_handler.bs_parser(sample_response)
		assert isinstance(result, BeautifulSoup)
		assert result.p.string == "Test"

	def test_bs_parser_http_error(
		self, html_handler: HtmlHandler, sample_response: Response
	) -> None:
		"""Test BeautifulSoup parser with HTTP error.

		Verifies
		--------
		- Returns error message string when HTTPError occurs
		- Message contains the original error

		Parameters
		----------
		html_handler : HtmlHandler
			Instance of HtmlHandler to test
		sample_response : Response
			Mocked HTTP response object with sample HTML content

		Returns
		-------
		None
		"""
		with patch("stpstone.utils.parsers.html.BeautifulSoup") as mock_bs:
			mock_bs.side_effect = HTTPError("Test error")
			result = html_handler.bs_parser(sample_response)
			assert isinstance(result, str)
			assert "HTTP Error: Test error" in result

	def test_lxml_parser_success(
		self, html_handler: HtmlHandler, sample_response: Response
	) -> None:
		"""Test successful lxml parsing.

		Verifies
		--------
		- Returns HtmlElement object
		- Parses the response content correctly

		Parameters
		----------
		html_handler : HtmlHandler
			Instance of HtmlHandler to test
		sample_response : Response
			Mocked HTTP response object with sample HTML content

		Returns
		-------
		None
		"""
		result = html_handler.lxml_parser(sample_response)
		assert isinstance(result, html.HtmlElement)
		assert result.xpath("//p/text()")[0] == "Test"

	def test_lxml_xpath_query(
		self, html_handler: HtmlHandler, sample_html_element: html.HtmlElement
	) -> None:
		"""Test XPath query execution.

		Verifies
		--------
		- Returns list of matching elements
		- Correctly finds elements matching XPath

		Parameters
		----------
		html_handler : HtmlHandler
			Instance of HtmlHandler to test
		sample_html_element : html.HtmlElement
			Parsed HTML element to query

		Returns
		-------
		None
		"""
		result = html_handler.lxml_xpath(sample_html_element, "//p")
		assert isinstance(result, list)
		assert len(result) == 1
		assert result[0].tag == "p"

	def test_html_tree_to_file(
		self, html_handler: HtmlHandler, sample_html_element: html.HtmlElement, tmp_path: Path
	) -> None:
		"""Test HTML tree output to file.

		Verifies
		--------
		- Creates file at specified path
		- File contains correct HTML content

		Parameters
		----------
		html_handler : HtmlHandler
			Instance of HtmlHandler to test
		sample_html_element : html.HtmlElement
			Parsed HTML element to write
		tmp_path : Path
			Temporary directory for file creation

		Returns
		-------
		None
		"""
		test_file = tmp_path / "test.html"
		html_handler.html_tree(sample_html_element, str(test_file))
		assert test_file.exists()
		assert "<p>Test</p>" in test_file.read_text()

	def test_html_tree_to_stdout(
		self,
		html_handler: HtmlHandler,
		sample_html_element: html.HtmlElement,
		capsys: pytest.CaptureFixture,
	) -> None:
		"""Test HTML tree output to stdout.

		Verifies
		--------
		- Prints HTML content to stdout
		- Output contains expected HTML

		Parameters
		----------
		html_handler : HtmlHandler
			Instance of HtmlHandler to test
		sample_html_element : html.HtmlElement
			Parsed HTML element to print
		capsys : pytest.CaptureFixture
			Fixture for capturing stdout

		Returns
		-------
		None
		"""
		html_handler.html_tree(sample_html_element)
		captured = capsys.readouterr()
		assert "<p>Test</p>" in captured.out

	def test_to_txt_conversion(self, html_handler: HtmlHandler) -> None:
		"""Test HTML to text conversion.

		Verifies
		--------
		- Returns BeautifulSoup object
		- Correctly parses input HTML

		Parameters
		----------
		html_handler : HtmlHandler
			Instance of HtmlHandler to test

		Returns
		-------
		None
		"""
		result = html_handler.to_txt("<p>Test</p>")
		assert isinstance(result, BeautifulSoup)
		assert result.p.string == "Test"

	def test_parse_html_to_string(self, html_handler: HtmlHandler) -> None:
		"""Test HTML to formatted string conversion.

		Verifies
		--------
		- Returns string output
		- Correctly formats table-like structures
		- Handles line breaks appropriately

		Parameters
		----------
		html_handler : HtmlHandler
			Instance of HtmlHandler to test

		Returns
		-------
		None
		"""
		html_input = "<table><tr><td>A</td><td>B</td></tr><tr><td>C</td><td>D</td></tr></table>"
		result = html_handler.parse_html_to_string(html_input)
		assert isinstance(result, str)
		assert "A|B" in result
		assert "C|D" in result
		assert "\n" in result


# --------------------------
# HtmlBuilder Tests
# --------------------------
class TestHtmlBuilder:
	"""Test cases for HtmlBuilder class."""

	def test_tag_with_content(self, html_builder: HtmlBuilder) -> None:
		"""Test tag creation with content.

		Verifies
		--------
		- Returns proper HTML tag with content
		- Handles multiple content items

		Parameters
		----------
		html_builder : HtmlBuilder
			Instance of HtmlBuilder to test

		Returns
		-------
		None
		"""
		result = html_builder.tag("p", "Hello", "World")
		assert result == "<p>Hello</p>\n<p>World</p>"

	def test_tag_with_class(self, html_builder: HtmlBuilder) -> None:
		"""Test tag creation with class attribute.

		Verifies
		--------
		- Includes class attribute in output
		- Formats attribute correctly

		Parameters
		----------
		html_builder : HtmlBuilder
			Instance of HtmlBuilder to test

		Returns
		-------
		None
		"""
		result = html_builder.tag("div", cls="container")
		assert result == '<div class="container" />'

	def test_tag_with_attributes(self, html_builder: HtmlBuilder) -> None:
		"""Test tag creation with multiple attributes.

		Verifies
		--------
		- Includes all specified attributes
		- Sorts attributes alphabetically

		Parameters
		----------
		html_builder : HtmlBuilder
			Instance of HtmlBuilder to test

		Returns
		-------
		None
		"""
		result = html_builder.tag("a", href="#", id="link")
		assert result == '<a href="#" id="link" />'

	def test_void_tag(self, html_builder: HtmlBuilder) -> None:
		"""Test void tag creation.

		Verifies
		--------
		- Creates self-closing tag for void elements
		- Omits content section

		Parameters
		----------
		html_builder : HtmlBuilder
			Instance of HtmlBuilder to test

		Returns
		-------
		None
		"""
		result = html_builder.tag("br")
		assert result == "<br />"


# --------------------------
# Type Validation Tests
# --------------------------
class TestTypeValidation:
	"""Test cases for type validation."""

	def test_bs_parser_type_validation(self, html_handler: HtmlHandler) -> None:
		"""Test bs_parser parameter type validation.

		Verifies
		--------
		- Raises TypeError for invalid response type

		Parameters
		----------
		html_handler : HtmlHandler
			Instance of HtmlHandler to test

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			html_handler.bs_parser("not_a_response")  # type: ignore

	def test_lxml_parser_type_validation(self, html_handler: HtmlHandler) -> None:
		"""Test lxml_parser parameter type validation.

		Verifies
		--------
		- Raises TypeError for invalid response type

		Parameters
		----------
		html_handler : HtmlHandler
			Instance of HtmlHandler to test

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			html_handler.lxml_parser("not_a_response")  # type: ignore

	def test_lxml_xpath_type_validation(
		self, html_handler: HtmlHandler, sample_html_element: html.HtmlElement
	) -> None:
		"""Test lxml_xpath parameter type validation.

		Verifies
		--------
		- Raises TypeError for invalid xpath type

		Parameters
		----------
		html_handler : HtmlHandler
			Instance of HtmlHandler to test
		sample_html_element : html.HtmlElement
			Parsed HTML element to query

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			html_handler.lxml_xpath(sample_html_element, 123)  # type: ignore

	def test_html_tree_type_validation(
		self, html_handler: HtmlHandler, sample_html_element: html.HtmlElement
	) -> None:
		"""Test html_tree parameter type validation.

		Verifies
		--------
		- Raises TypeError for invalid file_path type

		Parameters
		----------
		html_handler : HtmlHandler
			Instance of HtmlHandler to test
		sample_html_element : html.HtmlElement
			Parsed HTML element to manipulate

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			html_handler.html_tree(sample_html_element, 123)  # type: ignore

	def test_tag_type_validation(self, html_builder: HtmlBuilder) -> None:
		"""Test tag parameter type validation.

		Verifies
		--------
		- Raises TypeError for invalid attribute types

		Parameters
		----------
		html_builder : HtmlBuilder
			Instance of HtmlBuilder to test

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			html_builder.tag("div", cls=123)  # type: ignore
