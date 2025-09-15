from stpstone.utils.providers.br.margin_simulator_b3 import MarginSimulatorB3


dict_payload : list[dict] = \
    [
        {
            'Security': {'symbol': 'ABCBF160'},
            'SecurityGroup': {'positionTypeCode': 0},
            'Position': {
                'longQuantity': 100,
                'shortQuantity': 0,
                'longPrice': 0,
                'shortPrice': 0
            }
        },
    ]

cls_margin_simulator_b3 = MarginSimulatorB3(dict_payload, "79a4413f55d7d982b61c669e6dd35eea")
json_margin_call = cls_margin_simulator_b3.total_deficit_surplus()
print(f"MARGIN CALL B3: {json_margin_call}")