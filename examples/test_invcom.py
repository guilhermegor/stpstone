# pypi.org libs
import os
from requests import request
from typing import List, Dict, Any
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.utils.cals.handling_dates import DatesBR


class MDInvestingDotCom:

    def ticker_reference_investing_com(
        self, str_ticker:str,
        str_host:str='https://tvc4.investing.com/725910b675af9252224ca6069a1e73cc/1631836267/1/1/8/symbols?symbol={}',
        str_method:str='GET', bl_verify=True, key_ticker='ticker',
        dict_headers:dict={
            'User-Agent': 'Mozilla/5.0 (Windowns NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36'
        }) -> List[Dict[str, Any]]:
        """
        DOCSTRING: TICKER REFERENCE FROM INVESTING.COM
        INPUTS: TICKER, HOST (DEFAULT), METHOD (DEFAULT), BOOLEAN VERIFY (DEFAULT),
            KEY TICKER (DEFAULT), HEADERS (DEFAULT)
        OUTPUTS: STRING
        """
        # collect content from rest
        resp_req = request(
            str_method,
            str_host.format(str_ticker),
            verify=bl_verify,
            headers=dict_headers
        )
        resp_req.raise_for_status()
        # turning to desired format - loading json to memopry
        json_ = resp_req.json()
        # return named ticker form investing.com
        return json_[key_ticker]

print(MDInvestingDotCom().ticker_reference_investing_com('PETR4'))
