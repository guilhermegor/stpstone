"""Unit tests for XMLFiles class.

Tests the XML file handling functionality including:
- Path validation
- XML parsing with ElementTree and BeautifulSoup
- Attribute and text extraction
- Error handling for invalid inputs
"""

from xml.etree.ElementTree import Element

from bs4 import BeautifulSoup, Tag
from defusedxml.ElementTree import fromstring
import pytest

from stpstone.utils.parsers.xml import XMLFiles


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def xml_files_instance() -> XMLFiles:
    """Fixture providing XMLFiles instance.

    Returns
    -------
    XMLFiles
        Instance of XMLFiles class
    """
    return XMLFiles()


@pytest.fixture
def sample_xml_content() -> str:
    """Fixture providing sample XML content.

    Returns
    -------
    str
        Sample XML string
    """
    return """<?xml version="1.0"?>
    <root>
        <element attr="value">Text content</element>
        <element>Other content</element>
    </root>"""


@pytest.fixture
def sample_et_element(sample_xml_content: str) -> Element:
    """Fixture providing parsed ElementTree element.

    Parameters
    ----------
    sample_xml_content : str
        Sample XML content from fixture

    Returns
    -------
    Element
        Parsed ElementTree root element
    """
    return fromstring(sample_xml_content)


@pytest.fixture
def sample_soup(sample_xml_content: str) -> BeautifulSoup:
    """Fixture providing parsed BeautifulSoup object.

    Parameters
    ----------
    sample_xml_content : str
        Sample XML content from fixture

    Returns
    -------
    BeautifulSoup
        Parsed BeautifulSoup object
    """
    return BeautifulSoup(sample_xml_content, 'xml')


# --------------------------
# Validation Tests
# --------------------------
def test_validate_path_empty(xml_files_instance: XMLFiles) -> None:
    """Test _validate_path with empty path.

    Verifies
    --------
    - Empty path raises ValueError
    - Error message is correct

    Parameters
    ----------
    xml_files_instance : XMLFiles
        Instance of the XMLFiles class

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Path cannot be empty"):
        xml_files_instance._validate_path("")


def test_validate_path_non_string(xml_files_instance: XMLFiles) -> None:
    """Test _validate_path with non-string input.

    Verifies
    --------
    - Non-string path raises ValueError
    - Error message is correct

    Parameters
    ----------
    xml_files_instance : XMLFiles
        Instance of the XMLFiles class

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        xml_files_instance._validate_path(123)


def test_validate_soup_invalid(xml_files_instance: XMLFiles) -> None:
    """Test _validate_soup with invalid input.

    Verifies
    --------
    - Non-BeautifulSoup input raises ValueError
    - Error message is correct

    Parameters
    ----------
    xml_files_instance : XMLFiles
        Instance of the XMLFiles class

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        xml_files_instance._validate_soup("not a soup")


# --------------------------
# ElementTree Tests
# --------------------------
def test_fetch_et_success(
    xml_files_instance: XMLFiles,
    sample_xml_content: str,
    tmp_path: pytest.TempPathFactory
) -> None:
    """Test fetch_et with valid XML file.

    Verifies
    --------
    - Returns correct ElementTree root element
    - Element has expected structure

    Parameters
    ----------
    xml_files_instance : XMLFiles
        Instance of the XMLFiles class
    sample_xml_content : str
        Sample XML content from fixture
    tmp_path : pytest.TempPathFactory
        Temporary path fixture

    Returns
    -------
    None
    """
    test_file = tmp_path / "test.xml"
    test_file.write_text(sample_xml_content)
    
    result = xml_files_instance.fetch_et(str(test_file))
    assert isinstance(result, Element)
    assert result.tag == "root"
    assert len(result) == 2


def test_fetch_et_invalid_path(xml_files_instance: XMLFiles) -> None:
    """Test fetch_et with invalid file path.

    Verifies
    --------
    - Invalid path raises ValueError
    - Error message contains failure details

    Parameters
    ----------
    xml_files_instance : XMLFiles
        Instance of the XMLFiles class

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Failed to parse XML file"):
        xml_files_instance.fetch_et("nonexistent.xml")


def test_get_attrib_node_et_success(
    xml_files_instance: XMLFiles,
    sample_et_element: Element
) -> None:
    """Test get_attrib_node_et with valid attribute.

    Verifies
    --------
    - Returns correct attribute value
    - Returns None for non-existent attribute

    Parameters
    ----------
    xml_files_instance : XMLFiles
        Instance of the XMLFiles class
    sample_et_element : Element
        Sample ElementTree element from fixture

    Returns
    -------
    None
    """
    node = sample_et_element[0]  # first element with attr
    assert xml_files_instance.get_attrib_node_et(node, "attr") == "value"
    assert xml_files_instance.get_attrib_node_et(node, "missing") is None


def test_get_attrib_node_et_invalid_node(xml_files_instance: XMLFiles) -> None:
    """Test get_attrib_node_et with invalid node.

    Verifies
    --------
    - Non-Element input raises ValueError
    - Error message is correct

    Parameters
    ----------
    xml_files_instance : XMLFiles
        Instance of the XMLFiles class

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        xml_files_instance.get_attrib_node_et("not an element", "attr")


# --------------------------
# BeautifulSoup Tests
# --------------------------
def test_parser_success(
    xml_files_instance: XMLFiles,
    sample_xml_content: str,
    tmp_path: pytest.TempPathFactory
) -> None:
    """Test parser with valid XML file.

    Verifies
    --------
    - Returns BeautifulSoup object
    - Correctly parses XML structure

    Parameters
    ----------
    xml_files_instance : XMLFiles
        Instance of the XMLFiles class
    sample_xml_content : str
        Sample XML content from fixture
    tmp_path : pytest.TempPathFactory
        Temporary path fixture

    Returns
    -------
    None
    """
    test_file = tmp_path / "test.xml"
    test_file.write_text(sample_xml_content)
    
    result = xml_files_instance.parser(str(test_file))
    assert isinstance(result, BeautifulSoup)
    assert result.find("root") is not None


def test_parser_file_error(xml_files_instance: XMLFiles) -> None:
    """Test parser with invalid file.

    Verifies
    --------
    - Invalid file raises ValueError
    - Error message contains failure details

    Parameters
    ----------
    xml_files_instance : XMLFiles
        Instance of the XMLFiles class

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Failed to parse XML file"):
        xml_files_instance.parser("nonexistent.xml")


def test_memory_parser_success(
    xml_files_instance: XMLFiles,
    sample_xml_content: str
) -> None:
    """Test memory_parser with valid XML content.

    Verifies
    --------
    - Returns BeautifulSoup object
    - Correctly parses XML from string

    Parameters
    ----------
    xml_files_instance : XMLFiles
        Instance of the XMLFiles class
    sample_xml_content : str
        Sample XML content from fixture

    Returns
    -------
    None
    """
    result = xml_files_instance.memory_parser(sample_xml_content)
    assert isinstance(result, BeautifulSoup)
    assert result.find("root") is not None


def test_memory_parser_empty(xml_files_instance: XMLFiles) -> None:
    """Test memory_parser with empty content.

    Verifies
    --------
    - Empty content raises ValueError
    - Error message is correct

    Parameters
    ----------
    xml_files_instance : XMLFiles
        Instance of the XMLFiles class

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Cache cannot be empty"):
        xml_files_instance.memory_parser("")


def test_find_success(
    xml_files_instance: XMLFiles,
    sample_soup: BeautifulSoup
) -> None:
    """Test find with existing tag.

    Verifies
    --------
    - Returns correct Tag object
    - Returns None for non-existent tag

    Parameters
    ----------
    xml_files_instance : XMLFiles
        Instance of the XMLFiles class
    sample_soup : BeautifulSoup
        Sample BeautifulSoup object from fixture

    Returns
    -------
    None
    """
    result = xml_files_instance.find(sample_soup, "element")
    assert isinstance(result, Tag)
    assert result.text == "Text content"
    assert xml_files_instance.find(sample_soup, "nonexistent") is None


def test_find_invalid_soup(xml_files_instance: XMLFiles) -> None:
    """Test find with invalid soup.

    Verifies
    --------
    - Non-BeautifulSoup input raises ValueError
    - Error message is correct

    Parameters
    ----------
    xml_files_instance : XMLFiles
        Instance of the XMLFiles class

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        xml_files_instance.find("not a soup", "tag")


def test_find_all_success(
    xml_files_instance: XMLFiles,
    sample_soup: BeautifulSoup
) -> None:
    """Test find_all with existing tag.

    Verifies
    --------
    - Returns list of Tag objects
    - Correct number of matches

    Parameters
    ----------
    xml_files_instance : XMLFiles
        Instance of the XMLFiles class
    sample_soup : BeautifulSoup
        Sample BeautifulSoup object from fixture

    Returns
    -------
    None
    """
    results = xml_files_instance.find_all(sample_soup, "element")
    assert isinstance(results, list)
    assert len(results) == 2
    assert all(isinstance(tag, Tag) for tag in results)


def test_get_text_success(
    xml_files_instance: XMLFiles,
    sample_soup: BeautifulSoup
) -> None:
    """Test get_text with valid element.

    Verifies
    --------
    - Returns correct text content
    - Works with both BeautifulSoup and Tag

    Parameters
    ----------
    xml_files_instance : XMLFiles
        Instance of the XMLFiles class
    sample_soup : BeautifulSoup
        Sample BeautifulSoup object from fixture

    Returns
    -------
    None
    """
    element = sample_soup.find("element")
    assert xml_files_instance.get_text(element) == "Text content"
    assert isinstance(xml_files_instance.get_text(sample_soup), str)


def test_get_text_invalid(xml_files_instance: XMLFiles) -> None:
    """Test get_text with invalid input.

    Verifies
    --------
    - Non-BeautifulSoup/Tag input raises ValueError
    - Error message is correct

    Parameters
    ----------
    xml_files_instance : XMLFiles
        Instance of the XMLFiles class

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be one of types"):
        xml_files_instance.get_text("invalid input")


# --------------------------
# Error Handling Tests
# --------------------------
def test_parser_encoding_error(
    xml_files_instance: XMLFiles,
    tmp_path: pytest.TempPathFactory
) -> None:
    """Test parser with encoding error.

    Verifies
    --------
    - Handles encoding errors properly
    - Raises ValueError with descriptive message

    Parameters
    ----------
    xml_files_instance : XMLFiles
        Instance of the XMLFiles class
    tmp_path : pytest.TempPathFactory
        Temporary path factory

    Returns
    -------
    None
    """
    test_file = tmp_path / "test.xml"
    test_file.write_bytes(b"\xff\xfe")  # invalid UTF-8
    
    with pytest.raises(ValueError, match="Failed to parse XML file"):
        xml_files_instance.parser(str(test_file))


def test_memory_parser_invalid_xml(xml_files_instance: XMLFiles) -> None:
    """Test memory_parser with invalid XML.

    Verifies
    --------
    - Handles malformed XML content
    - Raises ValueError with descriptive message

    Parameters
    ----------
    xml_files_instance : XMLFiles
        Instance of the XMLFiles class

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Failed to parse XML from memory"):
        xml_files_instance.memory_parser("<invalid>fdeet243<xml>11124")