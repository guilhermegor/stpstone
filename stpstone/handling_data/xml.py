### HANDLING XML FILES ###

import xml.etree.ElementTree as et
from bs4 import BeautifulSoup


class XMLFiles:

    def fetch_et(self, path_xml):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # import xtree to memory
        xtree = et.parse(path_xml)
        # return root
        return xtree.getroot()

    def get_attrib_node_et(self, node, attrib_name):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return node.attrib.get(attrib_name)

    def xml_parser(self, xml_file):
        """
        DOCSTRING: XML PARSER THROUGH BEAUTIFULSOUP
        INPUTS: XML FILE COMPLETE PATH
        OUTPUTS: SOUP
        """
        infile = open(xml_file, 'r', encoding='UTF-8')
        contents = infile.read()
        return BeautifulSoup(contents, 'xml')

    def xml_memory_parser(self, xml_cache):
        """
        DOCSTRING: FETCH XML
        INPUTS: XML_CACHE
        OUTPUTS: XML IN MEMORY - SOUP
        """
        return BeautifulSoup(xml_cache, 'xml')

    def xml_find(self, soup_xml, tag):
        """
        DOCSTRING: GET SOUP ELEMENT WITHIN A TAG
        INPUTS: SOUP_XML, TAG
        OUTPUTS: XML ELEMENT
        """
        return soup_xml.find(tag)

    def xml_find_all(self, soup_xml, tag):
        """
        DOCSTRING: GET A LIST OF ELEMENTS IN SOUP XML THAT RESPECT A GIVEN TAG
        INPUTS: SOUP_XML, TAG
        OUTPUTS: XML ELEMENT        
        """
        return soup_xml.find_all(tag)

    def xml_get_text(self, soup_xml):
        """
        DOCSTRING: GET TEXT WITHIN AN XML ELEMENT
        INPUTS: SOUP_XML
        OUTPUTS: STRING
        """
        return soup_xml.get_text()
