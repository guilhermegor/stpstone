import numpy as np
from typing import Dict, Union

class ROEDecomposition:
    """
    A class to perform ROE decomposition using DuPont Analysis (3-step and 5-step).
    """

    def __init__(self):
        pass

    def dupont_analysis(
        self,
        float_ni: float,
        float_net_revenue: float,
        float_avg_ta: float,
        float_avg_te: float,
        float_ebt: float,
        float_ebit: float,
    ) -> Dict[str, Union[float, Dict[str, float]]]:
        """
        Perform DuPont Analysis (3-step and 5-step) to decompose ROE
        Args:
            float_ni (float): Net Income
            float_net_revenue (float): Net Revenue
            float_avg_ta (float): Average Total Assets
            float_avg_te (float): Average Total Shareholder's Equity
            float_ebt (float): Earnings Before Taxes (EBT)
            float_ebit (float): Earnings Before Interest and Taxes (EBIT)
        Returns:
            Dict[str, Union[float, Dict[str, float]]]: A dictionary containing:
                - 3-step DuPont ROE
                - 5-step DuPont ROE
                - Intermediate metrics (Net Profit Margin, Asset Turnover, etc.)
        """
        # input validation
        if any(x <= 0 for x in [float_net_revenue, float_avg_ta, float_avg_te, float_ebt, float_ebit]):
            raise ValueError("Inputs must be positive and non-zero.")
        # 3-Step DuPont Analysis
        net_profit_margin = float_ni / float_net_revenue
        asset_turnover = float_net_revenue / float_avg_ta
        equity_multiplier = float_avg_ta / float_avg_te
        roe_3_step = net_profit_margin * asset_turnover * equity_multiplier
        # 5-Step DuPont Analysis
        tax_burden = float_ni / float_ebt
        interest_burden = float_ebt / float_ebit
        operating_margin = float_ebit / float_net_revenue
        roe_5_step = tax_burden * interest_burden * operating_margin * asset_turnover * equity_multiplier
        # intermediate metrics
        intermediate_metrics = {
            "Net Profit Margin": net_profit_margin,
            "Asset Turnover": asset_turnover,
            "Equity Multiplier": equity_multiplier,
            "Tax Burden": tax_burden,
            "Interest Burden": interest_burden,
            "Operating Margin": operating_margin,
        }
        return {
            "3_step_dupont_roe": roe_3_step,
            "5_step_dupont_roe": roe_5_step,
            "intermediate_metrics": intermediate_metrics,
        }
