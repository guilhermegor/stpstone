"""XML file_path handling utilities.

This module provides classes for parsing and processing XML files using both
ElementTree (via defusedxml for security) and BeautifulSoup libraries.
It includes methods for fetching, parsing, and extracting data from XML documents
with proper error handling.
"""

from io import StringIO
from typing import Optional, Union
from xml.etree.ElementTree import Element

from bs4 import BeautifulSoup, Tag
from defusedxml.ElementTree import fromstring, parse

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class XMLFiles(metaclass=TypeChecker):
	"""Class for handling XML file_path operations using ElementTree and BeautifulSoup."""

	def _validate_path(self, path_xml: str) -> None:
		"""Validate XML file_path path.

		Parameters
		----------
		path_xml : str
			Path to XML file_path

		Raises
		------
		ValueError
			If path is empty
			If path is not a string
		"""
		if not path_xml:
			raise ValueError("Path cannot be empty")
		if not isinstance(path_xml, str):
			raise ValueError("Path must be a string")

	def _validate_soup(self, soup_xml: BeautifulSoup) -> None:
		"""Validate BeautifulSoup object.

		Parameters
		----------
		soup_xml : BeautifulSoup
			BeautifulSoup object to validate

		Raises
		------
		ValueError
			If soup_xml is not a BeautifulSoup instance
		"""
		if not isinstance(soup_xml, BeautifulSoup):
			raise ValueError("Input must be a BeautifulSoup instance")

	def fetch_et(self, path_xml: str) -> Element:
		"""Fetch XML file_path and return root element using defused ElementTree.

		Parameters
		----------
		path_xml : str
			Path to XML file_path

		Returns
		-------
		Element
			Root element of XML tree

		Raises
		------
		ValueError
			If path is invalid or file_path cannot be parsed
		"""
		self._validate_path(path_xml)
		try:
			xtree = parse(path_xml)
			return xtree.getroot()
		except Exception as err:
			raise ValueError(f"Failed to parse XML file_path: {str(err)}") from err

	def get_attrib_node_et(self, node: Element, attrib_name: str) -> Optional[str]:
		"""Get attribute value from ElementTree node.

		Parameters
		----------
		node : Element
			XML node element
		attrib_name : str
			Attribute name to retrieve

		Returns
		-------
		Optional[str]
			Attribute value if exists, None otherwise

		Raises
		------
		ValueError
			If node is not an ElementTree Element
		"""
		if not isinstance(node, Element):
			raise ValueError("Node must be an ElementTree Element")
		return node.attrib.get(attrib_name)

	def parser(self, file_path: str) -> BeautifulSoup:
		"""Parse XML file_path using BeautifulSoup.

		Parameters
		----------
		file_path : str
			Path to XML file_path

		Returns
		-------
		BeautifulSoup
			Parsed XML document

		Raises
		------
		ValueError
			If file_path cannot be opened or parsed
		"""
		self._validate_path(file_path)
		try:
			with open(file_path, encoding="UTF-8") as f:
				contents = f.read()
				# validate xml before parsing with BeautifulSoup, raising an error for invalid ones
				fromstring(contents)
				return BeautifulSoup(contents, "xml")
		except Exception as err:
			raise ValueError(f"Failed to parse XML file_path: {str(err)}") from err

	def memory_parser(self, cache: Union[str, StringIO]) -> BeautifulSoup:
		"""Parse XML content from memory using BeautifulSoup.

		Parameters
		----------
		cache : Union[str, StringIO]
			XML content as string or StringIO object

		Returns
		-------
		BeautifulSoup
			Parsed XML document

		Raises
		------
		ValueError
			If cache is empty or parsing fails
		"""
		if not cache:
			raise ValueError("Cache cannot be empty")

		content = cache.getvalue() if isinstance(cache, StringIO) else cache

		try:
			if isinstance(content, str):
				content_bytes = content.encode("utf-8")
			else:
				raise ValueError("Cache must be a string or StringIO object")

			# validate XML before parsing with BeautifulSoup
			fromstring(content_bytes)
			return BeautifulSoup(content, "xml")
		except Exception as err:
			raise ValueError(f"Failed to parse XML from memory: {str(err)}") from err

	def find(self, soup_xml: BeautifulSoup, tag: str) -> Optional[Tag]:
		"""Find first element with given tag in BeautifulSoup XML.

		Parameters
		----------
		soup_xml : BeautifulSoup
			Parsed XML document
		tag : str
			Tag name to search for

		Returns
		-------
		Optional[Tag]
			First matching element if found, None otherwise
		"""
		self._validate_soup(soup_xml)
		return soup_xml.find(tag)

	def find_all(self, soup_xml: BeautifulSoup, tag: str) -> list[Tag]:
		"""Find all elements with given tag in BeautifulSoup XML.

		Parameters
		----------
		soup_xml : BeautifulSoup
			Parsed XML document
		tag : str
			Tag name to search for

		Returns
		-------
		list[Tag]
			List of matching elements
		"""
		self._validate_soup(soup_xml)
		return soup_xml.find_all(tag)

	def get_text(self, soup_xml: Union[BeautifulSoup, Tag]) -> str:
		"""Get text content from BeautifulSoup XML element.

		Parameters
		----------
		soup_xml : Union[BeautifulSoup, Tag]
			XML element or document

		Returns
		-------
		str
			Text content

		Raises
		------
		ValueError
			If input is not a BeautifulSoup or Tag instance
		"""
		if not isinstance(soup_xml, (BeautifulSoup, Tag)):
			raise ValueError("Input must be BeautifulSoup or Tag instance")
		return soup_xml.get_text()
