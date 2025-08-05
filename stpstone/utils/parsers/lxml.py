"""HTML data extraction utilities using lxml and requests.

This module provides a class for fetching and parsing HTML content from URLs
using lxml for XPath-based data extraction and requests for HTTP operations.
"""

from typing import Literal

from lxml import html
from lxml.etree import _Element
from requests import request

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class HandlingLXML(metaclass=TypeChecker):
    """Class for handling HTML data extraction using lxml."""

    def _validate_url(self, url: str) -> None:
        """Validate URL format and content.

        Parameters
        ----------
        url : str
            URL to validate

        Raises
        ------
        ValueError
            If URL is empty or invalid
        """
        if not url:
            raise ValueError("URL cannot be empty")
        if not isinstance(url, str):
            raise ValueError("URL must be a string")
        if not (url.startswith("http://") or url.startswith("https://")):
            raise ValueError("URL must start with http:// or https://")

    def fetch(self, url: str, method: Literal["get", "post"] = "get") -> _Element:
        """Fetch and parse HTML document for XPath selection.

        Parameters
        ----------
        url : str
            URL to fetch HTML content from
        method : Literal['get', 'post']
            HTTP request method (default: "get")

        Returns
        -------
        _Element
            Parsed HTML document as an lxml _Element

        Raises
        ------
        ValueError
            If URL is invalid or request fails

        References
        ----------
        .. [1] https://stackoverflow.com/questions/26944078/extracting-value-of-url-source-by-xpath-in-python
        """
        self._validate_url(url)
        try:
            content = request(method, url, timeout=(200, 200)).content
            if not content:
                raise ValueError("Received empty response from URL")
            return html.fromstring(content)
        except Exception as err:
            raise ValueError(f"Failed to fetch or parse URL: {str(err)}") from err