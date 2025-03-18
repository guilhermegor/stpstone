from typing import Optional, Union, Literal


class LiquidityRatios:
    """
    Metadata: https://www.investopedia.com/terms/l/liquidityratios.asp,
    https://www.toppr.com/guides/principles-and-practice-of-accounting/accounting-ratios/liquidity-ratios/
    """

    def current_ratio(self, float_curr_assets: float, float_curr_liabilities: float):
        """
        Calculates the current ratio, which is a liquidity ratio that measures a company's ability
        to pay short-term obligations or those due within one year. The ratio indicates the extent
        to which current assets cover current liabilities.

        Args:
            float_curr_assets (float): Total current assets.
            float_curr_liabilities (float): Total current liabilities.

        Returns:
            float: The current ratio, calculated as current assets divided by current liabilities.
        """
        return float_curr_assets / float_curr_liabilities

    def quick_ratio(self, float_curr_assets: float, float_inventories: float,
                    float_curr_liabilities: float):
        """
        Calculates the quick ratio, which is a liquidity ratio that measures a company's ability
        to pay short-term obligations or those due within one year. The ratio indicates the extent
        to which current assets, excluding inventory, cover current liabilities.

        Args:
            float_curr_assets (float): Total current assets.
            float_inventories (float): Inventories.
            float_curr_liabilities (float): Total current liabilities.

        Returns:
            float: The quick ratio, calculated as current assets minus inventories divided by
            current liabilities.
        """
        return (float_curr_assets - float_inventories) / float_curr_liabilities

    def dso(self, float_avg_accounts_rec:float, float_rev:float,
            int_wdy: Optional[Union[int, Literal[252, 365, 360]]]=365):
        """
        Calculates the days sales outstanding (DSO), which is the average number of days that
        a company takes to collect its accounts receivable. The DSO is used to measure the
        efficiency of a company's accounts receivable management, and it is often used to
        evaluate the creditworthiness of a company.

        Args:
            float_avg_accounts_rec (float): Average accounts receivable.
            float_rev (float): Total revenue.
            int_wdy (Optional[Union[int, Literal[252, 365, 360]]], optional): Number of days in
                the period. Defaults to 365.

        Returns:
            float: The DSO, calculated as the number of days divided by the revenue minus the
            average accounts receivable.
        """
        return float(int_wdy) / (float_rev / float_avg_accounts_rec)

    def cash_ratio(self, float_cash_eq: float, float_curr_liabilities: float):
        """
        Calculates the cash ratio, which is a liquidity ratio that measures a company's ability
        to pay off its short-term liabilities with its most liquid assets, cash and cash equivalents.

        Args:
            float_cash_eq (float): Total cash and cash equivalents.
            float_curr_liabilities (float): Total current liabilities.

        Returns:
            float: The cash ratio, calculated as cash and cash equivalents divided by current liabilities.
        """
        return float_cash_eq / float_curr_liabilities


class SolvencyRatios:
    """
    Metadata: https://www.investopedia.com/terms/s/solvencyratio.asp,
        https://medium.com/quant-factory/calculating-altman-z-score-with-python-3c6697ee7aee
    """

    def interest_coverage_ratio(self, float_ebit: float, float_int_exp: float):
        """
        Calculates the interest coverage ratio, which is a solvency ratio that measures a company's
        ability to pay interest on its debt. The interest coverage ratio is calculated by dividing
        earnings before interest and taxes (EBIT) by interest expenses.

        Args:
            float_ebit (float): Earnings before interest and taxes.
            float_int_exp (float): Interest expenses.

        Returns:
            float: The interest coverage ratio, calculated as EBIT divided by interest expenses.
        """
        return float_ebit / float_int_exp

    def debt_to_assets_ratio(self, float_debt: float, float_assets: float):
        return float_debt / float_assets

    def equiy_ratio(self, float_tse: float, float_assets: float):
        """
        Calculates the equity ratio, which is a solvency ratio that measures the proportion of a
        company's total assets financed by shareholders' equity. The equity ratio is an indicator of
        financial stability, showing the percentage of assets owned by shareholders.

        Args:
            float_tse (float): Total shareholders' equity.
            float_assets (float): Total assets.

        Returns:
            float: The equity ratio, calculated as total shareholders' equity divided by total assets.
        """
        return float_tse / float_assets

    def debt_to_equity_ratio(self, float_debt: float, float_equity: float):
        return float_debt / float_equity

    def altmans_z_score(self, float_nwk_cap: float, float_total_assets: float,
                        float_ret_earnings: float, float_ebit: float,
                        float_mkt_cap: float, float_total_liabilities: float, float_sales: float):
        """
        Calculates the Altman Z-score, which is a credit-strength test that assesses a company's
        likelihood of bankruptcy. The Z-score is calculated from five financial ratios: working
        capital to total assets, retained earnings to total assets, earnings before interest and
        taxes (EBIT) to total assets, market capitalization to total liabilities, and sales to total
        assets.

        Args:
            float_nwk_cap (float): Working capital.
            float_total_assets (float): Total assets.
            float_ret_earnings (float): Retained earnings.
            float_ebit (float): Earnings before interest and taxes.
            float_mkt_cap (float): Market capitalization.
            float_total_liabilities (float): Total liabilities.
            float_sales (float): Sales.

        Returns:
            float: The Altman Z-score.
        """
        return 1.2 * (float_nwk_cap / float_total_assets) \
            + 1.4 * (float_ret_earnings / float_total_assets) \
            + 3.3 * (float_ebit / float_total_assets) \
            + 0.6 * (float_mkt_cap / float_total_liabilities) \
            + 1.0 * (float_sales / float_total_assets)
