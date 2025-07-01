"""Stock return calculations and performance metrics.

This module provides various methods for calculating stock returns and performance metrics,
including continuous (log) returns, discrete returns, and strategy performance calculations.
"""

from typing import Literal

import numpy as np
import pandas as pd

from stpstone.analytics.perf_metrics.financial_math import FinancialMath
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class FinancialReturns(metaclass=TypeChecker):
    """A class for calculating various stock return metrics and performance measures.

    Provides methods for continuous and discrete returns, pandas-based return calculations,
    short fee costs, and strategy pricing metrics.
    """

    def continuous_return(self, stock_d0: float, stock_d1: float) -> float:
        """Calculate the continuous (log) return between two stock prices.

        Parameters
        ----------
        stock_d0 : float or str
            Initial stock price (will be converted to float)
        stock_d1 : float or str
            Subsequent stock price (will be converted to float)

        Returns
        -------
        float
            The natural logarithm of the price ratio (log return)

        Examples
        --------
        >>> dd = FinancialReturns()
        >>> dd.continuous_return(100, 110)
        0.09531017980432493
        """
        stock_d0 = float(stock_d0)
        stock_d1 = float(stock_d1)
        return np.log(stock_d1 / stock_d0)

    def discrete_return(self, stock_d0: float, stock_d1: float) -> float:
        """Calculate the discrete (simple) return between two stock prices.

        Parameters
        ----------
        stock_d0 : float or str
            Initial stock price (will be converted to float)
        stock_d1 : float or str
            Subsequent stock price (will be converted to float)

        Returns
        -------
        float
            The simple return (d1/d0 - 1)

        Examples
        --------
        >>> dd = FinancialReturns()
        >>> dd.discrete_return(100, 110)
        0.1
        """
        stock_d0 = float(stock_d0)
        stock_d1 = float(stock_d1)
        return stock_d1 / stock_d0 - 1

    def calc_returns_from_prices(
        self,
        list_prices: list[float],
        type_return: Literal["ln_return", "stnd_return"]="ln_return"
    ) -> list[float]:
        """Calculate a series of returns from a list of prices.

        Parameters
        ----------
        list_prices : list or array-like
            Sequence of stock prices
        type_return : {'ln_return', 'stnd_return'}, optional
            Type of return to calculate (default is 'ln_return')

        Returns
        -------
        list
            List of calculated returns

        Raises
        ------
        Exception
            If invalid return type is specified

        Examples
        --------
        >>> dd = FinancialReturns()
        >>> prices = [100, 110, 105, 120]
        >>> dd.calc_returns_from_prices(prices)
        [0.09531017980432493, -0.04879016416943205, 0.13353139262452263]
        """
        if type_return == "ln_return":
            return [self.continuous_return(list_prices[i - 1], list_prices[i])
                    for i in range(1, len(list_prices))]
        elif type_return == "stnd_return":
            return [self.discrete_return(list_prices[i - 1], list_prices[i])
                    for i in range(1, len(list_prices))]
        else:
            raise ValueError(
                "Type of return calculation must be either 'ln_return' or 'stnd_return'")

    def pandas_returns_from_spot_prices(
        self,
        df_: pd.DataFrame,
        col_prices: str,
        col_dt_date: str,
        col_lag_close: str = "lag_close",
        col_first_occurrence_ticker: str = "first_occ_ticker",
        col_stock_returns: str = "returns",
        type_return: str = "ln_return"
    ) -> pd.DataFrame:
        """Calculate returns from spot prices in a pandas DataFrame.

        Parameters
        ----------
        df_ : pandas.DataFrame
            DataFrame containing price data
        col_prices : str
            Column name containing price values
        col_dt_date : str
            Column name containing date values
        col_lag_close : str, optional
            Name for new column with lagged prices (default 'lag_close')
        col_first_occurrence_ticker : str, optional
            Name for new column marking first occurrence (default 'first_occ_ticker')
        col_stock_returns : str, optional
            Name for new column containing returns (default 'returns')
        type_return : {'ln_return', 'stnd_return'}, optional
            Type of return to calculate (default 'ln_return')

        Returns
        -------
        pandas.DataFrame
            DataFrame with added return calculations

        Examples
        --------
        >>> import pandas as pd
        >>> df = pd.DataFrame({
        ...     'date': pd.date_range('2023-01-01', periods=3),
        ...     'price': [100, 110, 105]
        ... })
        >>> dd = FinancialReturns()
        >>> result = dd.pandas_returns_from_spot_prices(df, 'price', 'date')
        """
        # creating column with first occurrence of a ticker
        df_[col_first_occurrence_ticker] = np.where(
            df_[col_dt_date] == np.min(df_[col_dt_date]),
            'OK', 'NOK'
        )
        # creating column with lag prices
        df_[col_lag_close] = df_[col_prices].shift(periods=-1)
        # calculating returns
        if type_return == 'ln_return':
            df_[col_stock_returns] = df_.apply(
                lambda row: np.log(row[col_prices] / row[col_lag_close])
                if row[col_first_occurrence_ticker] == 'NOK' else 0,
                axis=1
            )
        else:
            df_[col_stock_returns] = df_.apply(
                lambda row: row[col_prices] / row[col_lag_close] - 1.0
                if row[col_first_occurrence_ticker] == 'NOK' else 0,
                axis=1
            )
        return df_

    def short_fee_cost(
        self,
        fee_short: float,
        nper_cd: int,
        short_price: float,
        quantities: float,
        year_cd: int = 360
    ) -> float:
        """Calculate the cost of a short position including fees.

        Parameters
        ----------
        fee_short : float
            Short fee rate
        nper_cd : int
            Number of calendar days
        short_price : float
            Price at which asset was shorted
        quantities : float
            Number of shares/units shorted
        year_cd : int, optional
            Number of calendar days in year (default 360)

        Returns
        -------
        float
            Total cost of the short position

        Examples
        --------
        >>> dd = FinancialReturns()
        >>> dd.short_fee_cost(0.05, 30, 100, 10)
        50.41666666666667
        """
        return FinancialMath().compound_r(
            fee_short, nper_cd, year_cd
        ) * short_price * quantities

    def pricing_strategy(
        self,
        long_price: float,
        short_price: float,
        leverage: float,
        operational_costs: float = 0,
        type_return: Literal["ln_return", "stnd_return"] = "ln_return"
    ) -> dict[str, float]:
        """Calculate performance metrics for a trading strategy.

        Parameters
        ----------
        long_price : float
            Price of long position
        short_price : float
            Price of short position
        leverage : float
            Leverage multiplier
        operational_costs : float, optional
            Additional operational costs (default 0)
        type_return : {'ln_return', 'stnd_return'}, optional
            Type of return calculation (default 'ln_return')

        Returns
        -------
        dict
            Dictionary containing:
            - mtm: Mark-to-market value
            - pct_return: Percentage return
            - notional: Notional value

        Raises
        ------
        ValueError
            If invalid return type is specified

        Examples
        --------
        >>> dd = FinancialReturns()
        >>> dd.pricing_strategy(100, 110, 2)
        {'mtm': 20.0, 'pct_retun': 0.09531017980432493, 'notional': 110.0}
        """
        if type_return == 'ln_return':
            return {
                'mtm': (float(short_price) - float(long_price)) * float(leverage) \
                    - float(operational_costs),
                'pct_return': self.continuous_return(float(short_price), float(long_price)),
                'notional': float(short_price)
            }
        elif type_return == 'stnd_return':
            return {
                'mtm': (float(short_price) - float(long_price)) * float(leverage) \
                    - float(operational_costs),
                'pct_return': float(long_price) / float(short_price) - 1,
                'notional': float(short_price)
            }
        else:
            raise ValueError(
                "Type of return calculation must be either 'ln_return' or 'stnd_return'")
