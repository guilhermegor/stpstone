
### PRICING FORWARD CONTRACTS ###

class ForwardBR:

    def forward_contract_pricing(self, spot, bid_forward_contract, rate_cost_period, leverage,
                                 number_contracts):
        """
        DOCSTRING: FORWARD CONTRACT FEATURES
        INPUTS: SPOT PRICE, BID AND RATE COST FOR THE PERIOD
        OUTPUTS: DICTIONARY (MTM, PCT RETURN AND CONTRACT NOTIONAL)
        """
        return {
            'mtm': (float(spot) - float(bid_forward_contract) * (
                1.0 + rate_cost_period)) * float(leverage) * float(number_contracts),
            'pct_retun': (float(spot) - float(bid_forward_contract) * (
                1.0 + rate_cost_period)) * float(leverage) / float(bid_forward_contract),
            'notional': float(spot) * float(leverage) * float(number_contracts)
        }
