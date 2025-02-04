### API TO CONNECT IN REUTES DATABASE ###

from requests import request
from urllib.parse import urljoin
import sys
sys.path.append(
    r'C:\Users\Guilherme\OneDrive\Dev\Python\Packages')
from stpstone.handling_data.object import HandlingObjects


class Reuters:

    def fetch_data(self, app, payload={}, method='GET',
                   endpoint='https://apiservice.reuters.com/api/'):
        """
        REFERENCES: https://www.reuters.com/markets/currencies
        DOCSTRING: FETCHING DATA FROM RETURES CHARTS
        INPUTS: ENDPOINT (DEFAULT) AND METHOD (DEFAULT)
        OUTPUTS: JSON (ACCESS TOKEN, TOKEN TYPE AND EXPIRES IN KEYS)
        """
        # requesting info from reuters api
        return request(
            method=method, url=urljoin(endpoint, app), params=payload, verify=False).text

    def token(self, api_key, deviceid, app='service/modtoken',
              method='GET'):
        """
        REFERENCES: https://www.reuters.com/markets/currencies
        DOCSTRING: GET TOKEN FROM REUTERS API
        INPUTS: APP (DEFAULT) AND METHOD (DEFAULT)
        OUTPUTS: JSON (ACCESS TOKEN, TOKEN TYPE AND EXPIRES IN KEYS)
        """
        # parameters for the endpoint
        payload = {
            'method': 'get',
            'format': 'json',
            'callback': 'getChartData',
            'apikey': api_key,
            'deviceid': deviceid
        }
        # requesting info from reuters api
        json_response = self.fetch_data(app, payload)
        # the api returns a byte (b') type of information, then its transformed to a text
        #   representation and used a literal eval to reach a
        return HandlingObjects().literal_eval_data(json_response, 'getChartData(', ')')

    def quotes(self, currency, app='getFetchQuotes/{}',
               endpoint='https://www.reuters.com/companies/api/'):
        """
        REFERENCES: https://www.reuters.com/markets/currencies
        DOCSTRING: QUOUTES FROM REUTERS
        INPUTS: CURRENCY, APP (DEFAULT) AND ENDPOINT (DEFAULT)
        OUTPUTS: JSON
        """
        return HandlingObjects().literal_eval_data(self.fetch_data(urljoin(app, currency),
                                                                   endpoint=endpoint))
