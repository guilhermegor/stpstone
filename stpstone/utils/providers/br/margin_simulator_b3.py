### API TO GRANT ACCESS TO B3 MARGIN SIMULATOR ###

# pypi.org libs
import numpy as np
from requests import request
from typing import List, Dict, Union
# local libs
from stpstone.utils.parsers.json import JsonFiles


class MarginSimulatorB3:

    def __init__(self, dict_payload:List[Dict[str, Union[str, int, float]]],
                 token:str='79a4413f55d7d982b61c669e6dd35eea',
                 host:str='https://simulador.b3.com.br/api/cors-app'):
        """
        INPUTS: DATA PAYLOAD PORTFOLIO EXEMPLE - dict_payload = [
                {
                    'Security': {'symbol': 'ABCBF160'}
                    'SecurityGroup': {'positionTypeCode': 0},
                    'Position': {'longQuantity':100,'shortQuantity': 0,'longPrice': 0,'shortPrice': 0}
                },
                {
                    'Security': {'symbol': 'ABCBF179'},
                    'SecurityGroup': {'positionTypeCode': 0},
                    'Position': {'longQuantity': 100,'shortQuantity': 0,'longPrice': 0,'shortPrice': 0}
                },
                {
                    'Security': {'symbol': 'ABCBF182'},
                    'SecurityGroup': {'positionTypeCode': 0},
                    'Position': {'longQuantity': 0,'shortQuantity':200,'longPrice': 0,'shortPrice': 0}
                }
            ]
        """
        self.dict_payload = dict_payload
        self.token = token,
        self.host = host

    @property
    def total_deficit_surplus(self, method:str='POST', app:str='/web/V1.0/RiskCalculation',
                              value_liquidity_resource:np.int64=4_700_000_000, bl_verify:bool=False,
                              bl_parse_dict_payload_data:bool=True) \
                                -> List[Dict[str, Union[str, int, float]]]:
        """
        REFERENCES: https://simulador.b3.com.br/
        DOCSTRING: TOTAL DEFICIT SURPLUS B3 MARGIN CALL CALCULATION
        INPUTS: METHOD (DEFAULT), KEY HEADER (DEFAULT), URL AUTHENTIFICATION
            HEADER (DEFAULT)
        OUTPUTS: STRING
        """
        # requesting authorization authheader
        dict_headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        # payload
        dict_payload = {
            'ReferenceData': {'referenceDataToken': '{}'.format(self.token)},
            'LiquidityResource': {'value': value_liquidity_resource},
            'RiskPositionList': self.dict_payload
        }
        # check wheter it is needed to parse the params dictionary
        if bl_parse_dict_payload_data == True:
            if dict_payload != None:
                dict_payload = JsonFiles().dict_to_json(dict_payload)
        # resquet host REST information
        resp_req = request(method=method, host=self.host + app,
                           headers=dict_headers, data=dict_payload, verify=bl_verify)
        # raises exception when not a 2xx response
        resp_req.raise_for_status()
        # getting authheader
        return resp_req.json()
