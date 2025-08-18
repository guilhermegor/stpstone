"""Stock return calculations and performance metrics.

This module provides various methods for calculating stock returns and performance metrics,
including continuous (log) returns, discrete returns, and strategy performance calculations.
"""

from typing import Literal, TypedDict, Union

import numpy as np
from numpy.typing import NDArray
import pandas as pd

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class ResultPricingStrategy(TypedDict, metaclass=TypeChecker):
    """A typed dictionary for holding strategy performance results."""

    mtm: float
    pct_return: float
    notional: float


class FinancialReturns(metaclass=TypeChecker):
    """A class for calculating various stock return metrics and performance measures.

    Provides methods for continuous and discrete returns, pandas-based return calculations,
    short fee costs, and strategy pricing metrics.
    """

    def continuous_return(self, stock_d0: Union[float, int], stock_d1: Union[float, int]) -> float:
        """Calculate the continuous (log) return between two stock prices.

        Parameters
        ----------
        stock_d0 : Union[float, int]
            Initial stock price (will be converted to float)
        stock_d1 : Union[float, int]
            Subsequent stock price (will be converted to float)

        Returns
        -------
        float
            The natural logarithm of the price ratio (log return)

        Raises
        ------
        ValueError
            If the initial price is zero
            If the price ratio is negative

        Examples
        --------
        >>> dd = FinancialReturns()
        >>> dd.continuous_return(100, 110)
        0.09531017980432493
        """
        stock_d0 = float(stock_d0)
        stock_d1 = float(stock_d1)
        if stock_d0 == 0:
            raise ValueError("Initial price cannot be zero")
        if not (stock_d1 / stock_d0) > 0:
            raise ValueError("Price ratio must be greater than zero")
        return np.log(stock_d1 / stock_d0)

    def discrete_return(self, stock_d0: Union[float, int], stock_d1: Union[float, int]) -> float:
        """Calculate the discrete (simple) return between two stock prices.

        Parameters
        ----------
        stock_d0 : Union[float, int]
            Initial stock price (will be converted to float)
        stock_d1 : Union[float, int]
            Subsequent stock price (will be converted to float)

        Returns
        -------
        float
            The simple return (d1/d0 - 1)

        Raises
        ------
        ValueError
            If the initial price is zero

        Examples
        --------
        >>> dd = FinancialReturns()
        >>> dd.discrete_return(100, 110)
        0.1
        """
        stock_d0 = float(stock_d0)
        stock_d1 = float(stock_d1)
        if stock_d0 == 0:
            raise ValueError("Initial price cannot be zero")
        return stock_d1 / stock_d0 - 1

    def calc_returns_from_prices(
        self,
        array_prices: Union[list[float], NDArray[np.float64]],
        type_return: Literal["ln_return", "stnd_return"] = "ln_return"
    ) -> NDArray[np.float64]:
        """Calculate a series of returns from a list of prices.

        Parameters
        ----------
        array_prices : Union[list[float], NDArray[np.float64]]
            Sequence of stock prices
        type_return : Literal['ln_return', 'stnd_return']
            Type of return to calculate, default 'ln_return'

        Returns
        -------
        NDArray[np.float64]
            Array of calculated returns

        Raises
        ------
        ValueError
            If invalid return type is specified

        Examples
        --------
        >>> dd = FinancialReturns()
        >>> prices = [100, 110, 105, 120]
        >>> dd.calc_returns_from_prices(prices)
        array([ 0.09531018, -0.04879016,  0.13353139])
        """
        if len(array_prices) < 2:
            return np.array([], dtype=np.float64)
        if type_return == "ln_return":
            return np.array([self.continuous_return(array_prices[i - 1], array_prices[i])
                    for i in range(1, len(array_prices))])
        elif type_return == "stnd_return":
            return np.array([self.discrete_return(array_prices[i - 1], array_prices[i])
                    for i in range(1, len(array_prices))])
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
        type_return: Literal["ln_return", "stnd_return"] = "ln_return"
    ) -> pd.DataFrame:
        """Calculate returns from spot prices in a pandas DataFrame.

        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame containing price data
        col_prices : str
            Column name containing price values
        col_dt_date : str
            Column name containing date values
        col_lag_close : str
            Name for new column with lagged prices, default "lag_close"
        col_first_occurrence_ticker : str
            Name for new column marking first occurrence, default "first_occ_ticker"
        col_stock_returns : str
            Name for new column containing returns, default "returns"
        type_return : Literal['ln_return', 'stnd_return']
            Type of return to calculate, default 'ln_return'

        Returns
        -------
        pd.DataFrame
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
            "OK", "NOK"
        )
        # creating column with lag prices
        df_[col_lag_close] = df_[col_prices].shift(periods=1)
        # calculating returns
        if type_return == "ln_return":
            df_[col_stock_returns] = df_.apply(
                lambda row: np.log(row[col_prices] / row[col_lag_close])
                if row[col_first_occurrence_ticker] == "NOK" else 0,
                axis=1
            )
        else:
            df_[col_stock_returns] = df_.apply(
                lambda row: row[col_prices] / row[col_lag_close] - 1.0
                if row[col_first_occurrence_ticker] == "NOK" else 0,
                axis=1
            )
        return df_

    def short_fee_cost(
        self,
        fee_short: Union[float, int],
        nper_cd: Union[float, int],
        short_price: Union[float, int],
        quantities: Union[float, int],
        year_cd: Union[float, int] = 360
    ) -> float:
        """Calculate the cost of a short position including fees.

        Parameters
        ----------
        fee_short : Union[float, int]
            Short fee rate
        nper_cd : Union[float, int]
            Number of periods per year
        short_price : Union[float, int]
            Price of short position
        quantities : Union[float, int]
            Quantity of short position
        year_cd : Union[float, int]
            Number of days per year, default 360
        
        Returns
        -------
        float
            Total cost of the short position
        
        Raises
        ------
        ValueError
            If any of the inputs are negative

        Examples
        --------
        >>> dd = FinancialReturns()
        >>> dd.short_fee_cost(0.05, 30, 100, 10)
        50.41666666666667
        """
        fee_short = float(fee_short)
        nper_cd = float(nper_cd)
        short_price = float(short_price)
        quantities = float(quantities)
        year_cd = float(year_cd)
        
        if any(x < 0 for x in [fee_short, nper_cd, short_price, quantities, year_cd]):
            raise ValueError("All inputs must be non-negative")
            
        daily_fee = fee_short / year_cd
        return daily_fee * nper_cd * short_price * quantities

    def pricing_strategy(
        self,
        long_price: Union[float, int],
        short_price: Union[float, int],
        leverage: Union[float, int],
        operational_costs: Union[float, int] = 0,
        type_return: Literal["ln_return", "stnd_return"] = "ln_return"
    ) -> ResultPricingStrategy:
        """Calculate performance metrics for a trading strategy.

        Parameters
        ----------
        long_price : Union[float, int]
            Price of long position
        short_price : Union[float, int]
            Price of short position
        leverage : Union[float, int]
            Leverage
        operational_costs : Union[float, int]
            Operational costs, default 0
        type_return : Literal['ln_return', 'stnd_return']
            Type of return to calculate, default 'ln_return'

        Returns
        -------
        ResultPricingStrategy
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
        {'mtm': 20.0, 'pct_return': 0.09531017980432493, 'notional': 110.0}
        """
        long_price = float(long_price)
        short_price = float(short_price)
        leverage = float(leverage)
        operational_costs = float(operational_costs)

        if type_return == "ln_return":
            func_r = self.continuous_return
        elif type_return == "stnd_return":
            func_r = self.discrete_return
        else:
            raise ValueError(
                "Type of return calculation must be either 'ln_return' or 'stnd_return'")
        return {
            "mtm": (short_price - long_price) * leverage - operational_costs,
            "pct_return": func_r(long_price, short_price),
            "notional": short_price
        }