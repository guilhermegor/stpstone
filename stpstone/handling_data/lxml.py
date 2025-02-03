### HANDLING LXML TO EXTRACT HTML DATA ###

from lxml import html
from requests import request


class HandlingLXML:

    def fetch(self, url, method='get'):
        """
        REFENCES: https://stackoverflow.com/questions/26944078/extracting-value-of-url-source-by-xpath-in-python
        DOCSTRING: FETCHING HTML DOCUMENT TO BE SELECTED BY XPATH
        INPUTS: URL
        OUTPUTS: -
        """
        content = request(method, url).content
        tree = html.fromstring(content)
        return tree
