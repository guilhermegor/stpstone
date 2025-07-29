"""Financial ratios calculation module.

This module provides classes for calculating liquidity and solvency ratios to assess a company's
financial health. It includes robust input validation and adheres to strict type checking \
    standards.
"""

from typing import Literal, Optional

import numpy as np

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class LiquidityRatios(metaclass=TypeChecker):
    """Calculate liquidity ratios to assess a company's ability to meet short-term obligations.

    References
    ----------
    .. [1] https://www.investopedia.com/terms/l/liquidityratios.asp
    """

    def _validate_positive_float(self, value: float, name: str) -> None:
        """Validate that a value is positive and finite.

        Parameters
        ----------
        value : float
            Value to validate
        name : str
            Variable name for error messages

        Raises
        ------
        ValueError
            If value is not positive or not finite
        """
        if not np.isfinite(value):
            raise ValueError(f"{name} must be finite, got {value}")
        if value <= 0:
            raise ValueError(f"{name} must be positive, got {value}")

    def current_ratio(self, current_assets: float, current_liabilities: float) -> float:
        """Calculate the current ratio to measure ability to pay short-term obligations.

        Parameters
        ----------
        current_assets : float
            Total current assets
        current_liabilities : float
            Total current liabilities

        Returns
        -------
        float
            The current ratio

        References
        ----------
        .. [1] https://www.investopedia.com/terms/l/liquidityratios.asp
        """
        self._validate_positive_float(current_assets, "current_assets")
        self._validate_positive_float(current_liabilities, "current_liabilities")
        
        return current_assets / current_liabilities

    def quick_ratio(
        self, 
        current_assets: float, 
        inventories: float, 
        current_liabilities: float
    ) -> float:
        """Calculate the quick ratio.
         
        Measure ability to pay short-term obligations excluding inventory.

        Parameters
        ----------
        current_assets : float
            Total current assets
        inventories : float
            Inventories
        current_liabilities : float
            Total current liabilities

        Returns
        -------
        float
            The quick ratio

        Raises
        ------
        ValueError
            If current_assets is less than inventories

        References
        ----------
        .. [1] https://www.investopedia.com/terms/l/liquidityratios.asp
        """
        self._validate_positive_float(current_assets, "current_assets")
        self._validate_positive_float(inventories, "inventories")
        self._validate_positive_float(current_liabilities, "current_liabilities")
        if current_assets < inventories:
            raise ValueError("current_assets must be >= inventories")

        return (current_assets - inventories) / current_liabilities

    def dso(
        self, 
        avg_accounts_receivable: float, 
        revenue: float, 
        days: Optional[Literal[252, 360, 365]] = 365
    ) -> float:
        """Calculate days sales outstanding to measure accounts receivable collection efficiency.

        Parameters
        ----------
        avg_accounts_receivable : float
            Average accounts receivable
        revenue : float
            Total revenue
        days : Optional[Literal[252, 360, 365]]
            Number of days in the period (default: 365)

        Returns
        -------
        float
            The days sales outstanding

        References
        ----------
        .. [1] https://www.investopedia.com/terms/l/liquidityratios.asp
        """
        self._validate_positive_float(avg_accounts_receivable, "avg_accounts_receivable")
        self._validate_positive_float(revenue, "revenue")
        
        return float(days) / (revenue / avg_accounts_receivable)

    def cash_ratio(self, cash_equivalents: float, current_liabilities: float) -> float:
        """Calculate the cash ratio to measure ability to pay short-term liabilities with cash.

        Parameters
        ----------
        cash_equivalents : float
            Total cash and cash equivalents
        current_liabilities : float
            Total current liabilities

        Returns
        -------
        float
            The cash ratio

        References
        ----------
        .. [1] https://www.investopedia.com/terms/l/liquidityratios.asp
        """
        self._validate_positive_float(cash_equivalents, "cash_equivalents")
        self._validate_positive_float(current_liabilities, "current_liabilities")
        
        return cash_equivalents / current_liabilities


class SolvencyRatios:
    """Calculate solvency ratios to assess a company's long-term financial stability.

    References
    ----------
    .. [1] https://www.investopedia.com/terms/s/solvencyratio.asp
    .. [2] https://medium.com/quant-factory/calculating-altman-z-score-with-python-3c6697ee7aee
    """

    def _validate_positive_float(self, value: float, name: str) -> None:
        """Validate that a value is positive and finite.

        Parameters
        ----------
        value : float
            Value to validate
        name : str
            Variable name for error messages

        Raises
        ------
        ValueError
            If value is not positive or not finite
        """
        if not np.isfinite(value):
            raise ValueError(f"{name} must be finite, got {value}")
        if value <= 0:
            raise ValueError(f"{name} must be positive, got {value}")

    def _validate_float(self, value: float, name: str) -> None:
        """Validate that a value is finite.

        Parameters
        ----------
        value : float
            Value to validate
        name : str
            Variable name for error messages

        Raises
        ------
        ValueError
            If value is not finite
        """
        if not np.isfinite(value):
            raise ValueError(f"{name} must be finite, got {value}")

    def interest_coverage_ratio(self, ebit: float, interest_expenses: float) -> float:
        """Calculate the interest coverage ratio to measure ability to pay interest on debt.

        Parameters
        ----------
        ebit : float
            Earnings before interest and taxes
        interest_expenses : float
            Interest expenses

        Returns
        -------
        float
            The interest coverage ratio

        References
        ----------
        .. [1] https://www.investopedia.com/terms/s/solvencyratio.asp
        """
        self._validate_float(ebit, "ebit")
        self._validate_positive_float(interest_expenses, "interest_expenses")
        
        return ebit / interest_expenses

    def debt_to_assets_ratio(self, debt: float, assets: float) -> float:
        """Calculate the debt to assets ratio to measure financial leverage.

        Parameters
        ----------
        debt : float
            Total debt
        assets : float
            Total assets

        Returns
        -------
        float
            The debt to assets ratio

        References
        ----------
        .. [1] https://www.investopedia.com/terms/s/solvencyratio.asp
        """
        self._validate_positive_float(debt, "debt")
        self._validate_positive_float(assets, "assets")
        
        return debt / assets

    def equity_ratio(self, total_shareholders_equity: float, assets: float) -> float:
        """Calculate the equity ratio to measure proportion of assets financed by equity.

        Parameters
        ----------
        total_shareholders_equity : float
            Total shareholders' equity
        assets : float
            Total assets

        Returns
        -------
        float
            The equity ratio

        References
        ----------
        .. [1] https://www.investopedia.com/terms/s/solvencyratio.asp
        """
        self._validate_positive_float(total_shareholders_equity, "total_shareholders_equity")
        self._validate_positive_float(assets, "assets")
        
        return total_shareholders_equity / assets

    def debt_to_equity_ratio(self, debt: float, equity: float) -> float:
        """Calculate the debt to equity ratio to measure financial leverage.

        Parameters
        ----------
        debt : float
            Total debt
        equity : float
            Total equity

        Returns
        -------
        float
            The debt to equity ratio

        References
        ----------
        .. [1] https://www.investopedia.com/terms/s/solvencyratio.asp
        """
        self._validate_positive_float(debt, "debt")
        self._validate_positive_float(equity, "equity")
        
        return debt / equity

    def altmans_z_score(
        self,
        working_capital: float,
        total_assets: float,
        retained_earnings: float,
        ebit: float,
        market_capitalization: float,
        total_liabilities: float,
        sales: float
    ) -> float:
        """Calculate the Altman Z-score to assess bankruptcy risk.

        Parameters
        ----------
        working_capital : float
            Working capital
        total_assets : float
            Total assets
        retained_earnings : float
            Retained earnings
        ebit : float
            Earnings before interest and taxes
        market_capitalization : float
            Market capitalization
        total_liabilities : float
            Total liabilities
        sales : float
            Sales

        Returns
        -------
        float
            The Altman Z-score
        
        References
        ----------
        .. [1] https://www.investopedia.com/terms/s/solvencyratio.asp
        .. [2] https://medium.com/quant-factory/calculating-altman-z-score-with-python-3c6697ee7aee
        """
        self._validate_float(working_capital, "working_capital")
        self._validate_positive_float(total_assets, "total_assets")
        self._validate_float(retained_earnings, "retained_earnings")
        self._validate_float(ebit, "ebit")
        self._validate_positive_float(market_capitalization, "market_capitalization")
        self._validate_positive_float(total_liabilities, "total_liabilities")
        self._validate_positive_float(sales, "sales")

        return (
            1.2 * (working_capital / total_assets)
            + 1.4 * (retained_earnings / total_assets)
            + 3.3 * (ebit / total_assets)
            + 0.6 * (market_capitalization / total_liabilities)
            + 1.0 * (sales / total_assets)
        )