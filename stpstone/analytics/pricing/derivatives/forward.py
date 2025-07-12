"""Forward Contract Pricing."""

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class ForwardBR(metaclass=TypeChecker):
    """Forward contract pricing."""

    def forward_contract_pricing(
        self, 
        spot: float, 
        bid_forward_contract: float, 
        rate_cost_period: float, 
        leverage: float, 
        number_contracts: int
    ) -> dict[str, float]:
        """Forward contract pricing.

        Parameters
        ----------
        spot : float
            Spot price
        bid_forward_contract : float
            Bid forward contract price
        rate_cost_period : float
            Rate cost period
        leverage : float
            Leverage multiplier
        number_contracts : int
            Number of contracts

        Returns
        -------
        dict
            Dictionary containing:
            - mtm: Mark-to-market value
            - pct_return: Percentage return
            - notional: Notional value
        """
        return {
            'mtm': (float(spot) - float(bid_forward_contract) * (
                1.0 + rate_cost_period)) * float(leverage) * float(number_contracts),
            'pct_retun': (float(spot) - float(bid_forward_contract) * (
                1.0 + rate_cost_period)) * float(leverage) / float(bid_forward_contract),
            'notional': float(spot) * float(leverage) * float(number_contracts)
        }
