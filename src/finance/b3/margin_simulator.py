### API TO GRANT ACCESS TO B3 MARGIN SIMULATOR ###

from requests import request
from stpstone.handling_data.json import JsonFiles


class MarginSimulatorB3:

    def __init__(self, data_payload_portfolio, token='79a4413f55d7d982b61c669e6dd35eea',
                 hostname_api_margin_simulator_b3='https://simulador.b3.com.br/api/cors-app'):
        '''
        INPUTS: DATA PAYLOAD PORTFOLIO EXEMPLE - data_payload_portfolio = [
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
        '''
        self.data_payload_portfolio = data_payload_portfolio
        self.token = token,
        self.hostname_api_margin_simulator_b3 = hostname_api_margin_simulator_b3

    @property
    def total_deficit_surplus(self, method='POST', app='/web/V1.0/RiskCalculation',
                              value_liquidity_resource=4700000000, bl_verify=False,
                              bl_parse_dict_payload_data=True):
        '''
        REFERENCES: https://simulador.b3.com.br/
        DOCSTRING: TOTAL DEFICIT SURPLUS B3 MARGIN CALL CALCULATION
        INPUTS: METHOD (DEFAULT), KEY HEADER (DEFAULT), URL AUTHENTIFICATION
            HEADER (DEFAULT)
        OUTPUTS: STRING
        '''
        # requesting authorization authheader
        dict_headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        # payload
        dict_payload = {
            'ReferenceData': {'referenceDataToken': '{}'.format(self.token)},
            'LiquidityResource': {'value': value_liquidity_resource},
            'RiskPositionList': self.data_payload_portfolio
        }
        # check wheter it is needed to parse the params dictionary
        if bl_parse_dict_payload_data == True:
            if dict_payload != None:
                dict_payload = JsonFiles().dict_to_json(dict_payload)
        # resquet host REST information
        resp_req = request(method=method, url=self.hostname_api_margin_simulator_b3 + app,
                           headers=dict_headers, data=dict_payload, verify=bl_verify)
        # raises exception when not a 2xx response
        resp_req.raise_for_status()
        # getting authheader
        return resp_req.json()
