"""HTML handling and building utilities.

This module provides classes for parsing HTML content using BeautifulSoup and lxml,
and for building HTML tags programmatically. Includes error handling and various
output formatting options.
"""

from typing import Optional, Union

from bs4 import BeautifulSoup
from lxml import etree, html
from requests import HTTPError, Response

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class HtmlHandler(metaclass=TypeChecker):
    """Handler for parsing and processing HTML content.

    Provides methods for parsing HTML using different libraries and converting
    HTML to formatted text representations.
    """

    def bs_parser(
        self, 
        resp_req: Response, 
        parser: str = "html.parser"
    ) -> Union[BeautifulSoup, str]:
        """Parse HTML content using BeautifulSoup.

        Parameters
        ----------
        resp_req : Response
            HTTP response object containing HTML content
        parser : str, optional
            BeautifulSoup parser to use (default: "html.parser")

        Returns
        -------
        Union[BeautifulSoup, str]
            Parsed BeautifulSoup object or error message string
        """
        try:
            return BeautifulSoup(resp_req.content, parser)
        except HTTPError as e:
            return f"HTTP Error: {e}"
        except Exception as e:
            return f"Error: {e}"

    def lxml_parser(self, resp_req: Response) -> html.HtmlElement:
        """Parse HTML content using lxml.

        Parameters
        ----------
        resp_req : Response
            HTTP response object containing HTML content

        Returns
        -------
        html.HtmlElement
            Parsed HTML element tree
        """
        page = resp_req.content
        return html.fromstring(page)

    def lxml_xpath(self, html_content: html.HtmlElement, str_xpath: str) -> list:
        """Execute XPath query on HTML content.

        Parameters
        ----------
        html_content : html.HtmlElement
            Parsed HTML element tree
        str_xpath : str
            XPath expression to evaluate

        Returns
        -------
        list
            List of elements matching the XPath query
        """
        return html_content.xpath(str_xpath)

    def html_tree(self, html_root: html.HtmlElement, file_path: Optional[str] = None) -> None:
        """Convert HTML tree to string and optionally save to file.

        Parameters
        ----------
        html_root : html.HtmlElement
            Root element of HTML tree
        file_path : Optional[str]
            Path to save HTML content (if None, prints to stdout)
        """
        html_string = etree.tostring(html_root, pretty_print=True, encoding="unicode")
        if file_path:
            with open(file_path, "w", encoding="utf-8") as file:
                file.write(html_string)
        else:
            print(html_string)

    def to_txt(self, html_: str) -> BeautifulSoup:
        """Convert HTML to text using BeautifulSoup.

        Parameters
        ----------
        html_ : str
            HTML content to parse

        Returns
        -------
        BeautifulSoup
            Parsed BeautifulSoup object
        """
        return BeautifulSoup(html_, features="lxml")

    def parse_html_to_string(
        self,
        html_: str,
        parsing_lib: str = "html.parser",
        str_body_html: str = "",
        join_td_character: str = "|",
        td_size_ajust_character: str = " "
    ) -> str:
        """Parse HTML content to formatted string representation.

        Parameters
        ----------
        html_ : str
            HTML content to parse
        parsing_lib : str, optional
            Parser to use (default: "html.parser")
        str_body_html : str, optional
            Initial string to append results to (default: "")
        join_td_character : str, optional
            Character to join table cells (default: "|")
        td_size_ajust_character : str, optional
            Character used for padding cells (default: " ")

        Returns
        -------
        str
            Formatted string representation of HTML content
        """
        soup = BeautifulSoup(html_, parsing_lib)
    
        # handle tables specifically
        for table in soup.find_all('table'):
            rows = []
            for tr in table.find_all('tr'):
                cells = []
                for td in tr.find_all(['td', 'th']):
                    text = td.get_text().strip()
                    cells.append(text)
                if cells:  # only add rows with cells
                    rows.append(join_td_character.join(cells))
            
            if rows:
                str_body_html += "\n".join(rows) + "\n\n"
        
        # handle non-table content
        for element in soup.find_all(True):
            if element.name not in ['table', 'tr', 'td', 'th'] \
                and not element.find_parent('table'):
                text = element.get_text().strip()
                if text:
                    str_body_html += text + "\n\n"
        
        return str_body_html.strip()


class HtmlBuilder(metaclass=TypeChecker):
    """Builder for creating HTML tags programmatically."""

    def tag(self, name: str, *content: str, cls: Optional[str] = None, **attrs: str) -> str:
        """Create HTML tag with specified content and attributes.

        References
        ----------
        .. [1] Fluent Python by Luciano Ramalho (O'Reilly). Copyright 2015 Luciano Ramalho,
           978-1-491-94600-8.

        Parameters
        ----------
        name : str
            Tag name (e.g., "div", "span")
        *content : str
            Content to place inside the tag
        cls : Optional[str]
            Class attribute value (special handling for Python keyword)
        **attrs : str
            Additional attributes as keyword arguments

        Returns
        -------
        str
            Formatted HTML tag string
        """
        if cls is not None:
            attrs["class"] = cls

        attr_str = "".join(f' {attr}="{value}"' for attr, value in sorted(attrs.items())) \
            if attrs else ""

        if content:
            return "\n".join(f"<{name}{attr_str}>{c}</{name}>" for c in content)
        return f"<{name}{attr_str} />"