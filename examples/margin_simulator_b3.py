from stpstone.utils.providers.br.margin_simulator_b3 import (
    MarginSimulatorB3,
    ResultMarginSimulatorB3Payload,
)


dict_payload : ResultMarginSimulatorB3Payload = \
    {
        "LiquidityResource": {
            "value": 7500000000
        },
        "RiskPositionList": [
            {
                "Security": {
                    "symbol": "DI1F35"
                },
                "SecurityGroup": {
                    "positionTypeCode": 0
                },
                "Position": {
                    "longQuantity": 1,
                    "shortQuantity": 0,
                    "longPrice": 0,
                    "shortPrice": 0
                }
            }, 
            {
                "Security": {
                    "symbol": "DI1F27"
                },
                "SecurityGroup": {
                    "positionTypeCode": 0
                },
                "Position": {
                    "longQuantity": 0,
                    "shortQuantity": 1,
                    "longPrice": 0,
                    "shortPrice": 0
                }
            }
        ]
    }

cls_margin_simulator_b3 = MarginSimulatorB3(dict_payload=dict_payload)
json_margin_call = cls_margin_simulator_b3.risk_calculation()
print(f"MARGIN CALL B3: {json_margin_call}")