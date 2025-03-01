### FORMULAS FOR CALCULATING A STOCK RETURN ###

import numpy as np
import sys
sys.path.append(r'C:\Users\Guilherme\OneDrive\Dev\Python\Packages')
from stpstone.finance.performance_apprraisal.financial_math import FinancialMath


class DataDeltas:

    def continuous_return(self, stock_d0, stock_d1):
        """
        DOCSTRING: CALCULATING LN RETURN FOR A STOCK - CONTINUOS RETURN
        INPUTS: STOCK D0 VALUE AND D1 VALUE
        OUTPUTS: FLOAT LN RETURN
        """
        stock_d0 = float(stock_d0)
        stock_d1 = float(stock_d1)
        return np.log(stock_d1 / stock_d0)

    def discrete_return(self, stock_d0, stock_d1):
        """
        DOCSTRING: CALCULATING STANDARD RETURN OF A STOCK BETWEEN TWO DATES - DISCRETE RETURN
        INPUTS: STOCK D0 VALUE AND D1 VALUE
        OUTPUTS: FLOAT RETURN
        """
        stock_d0 = float(stock_d0)
        stock_d1 = float(stock_d1)
        return stock_d1 / stock_d0 - 1

    def calc_returns_from_prices(self, list_prices, type_return='ln_return'):
        """
        DOCSTRING: LIST OF RETURS FROM A GIVEN LIST OF PRICES
        INPUTS: LIST OF PRICES AND TYPE OF RETURN CALCULATION (LN_RETURN AS STANDARD)
        OUPUTS: LIST OF RETURNS
        """
        if type_return == 'ln_return':
            return [self.continuous_return(list_prices[i - 1], list_prices[i])
                    for i in range(1, len(list_prices))]
        elif type_return == 'stnd_return':
            return [self.stnd_return(list_prices[i - 1], list_prices[i])
                    for i in range(1, len(list_prices))]
        else:
            raise Exception(
                'Type of return calculation ought be ln_return or stnd_return')

    def pandas_returns_from_spot_prices(self, df_, col_prices, col_dt_date,
                                        col_lag_close='lag_close',
                                        col_first_occurrence_ticker='first_occ_ticker',
                                        col_stock_returns='returns',
                                        type_return='ln_return'):
        """
        DOCSTRING: PANDAS RETURNS FROM SPOT PRICES OF SECURITIES WITHIN THE DATAFRAME
        INPUTS: DATAFRAME, COL PRICES, COL DATES (DATETIME FORMAT)
        OUTPUTS: DATAFRAME
        """
        # creating column with first occurrence of a ticker
        df_[col_first_occurrence_ticker] = np.where(df_[col_dt_date] == np.min(df_[col_dt_date]),
                                                    'OK', 'NOK')
        # creating column with lag prices, in order to calculate return
        df_[col_lag_close] = df_[col_prices].shift(periods=-1)
        # calculating returns (continuous or dicrete methods allowed)
        if type_return == 'ln_return':
            df_[col_stock_returns] = \
                df_.apply(lambda row: np.log(row[col_prices] / row[col_lag_close])
                          if row[col_first_occurrence_ticker] == 'NOK' else 0, axis=1)
        else:
            df_[col_stock_returns] = \
                df_.apply(lambda row: row[col_prices] / row[col_lag_close] - 1.0
                          if row[col_first_occurrence_ticker] == 'NOK' else 0, axis=1)
        # returning dataframe
        return df_

    def short_fee_cost(self, fee_short, nper_cd, short_price, quantities, year_cd=360):
        """
        DOCSTRING: SHORT STRATEGY FEE COST
        INPUTS: FEE, NPER CALENDAR DAYS, SHORT PRICE, QUANTITITES, YEAR CALENDAR DAYS
        OUTPUTS: FLOAT
        """
        return FinancialMath().compound_interest(fee_short, nper_cd,
                                                 year_cd) * short_price * quantities

    def pricing_strategy(self, long_price, short_price, leverage, operational_costs=0,
                         type_return='ln_return'):
        """
        DOCSTRING: PNL STOCK STRATEGIES (BUY & HOLD, LONG & SHORT)
        INPUTS: LONGE PRICE, SHORT PRICE, LEVERAGE, OPERATIONAL COSTS
        OUPUTS: DICTIONARY (MTM, PERCENTAGE RETURN, NOTIONAL)
        """
        if type_return == 'ln_return':
            return {
                'mtm': (float(short_price) - float(long_price)) * float(leverage) - float(
                    operational_costs),
                'pct_retun': self.continuous_return(float(short_price), float(long_price)),
                'notional': float(short_price)
            }
        elif type_return == 'stnd_return':
            return {
                'mtm': (float(short_price) - float(long_price)) * float(leverage) - float(
                    operational_costs),
                'pct_retun': float(long_price) / float(short_price) - 1,
                'notional': float(short_price)
            }
        else:
            raise Exception(
                'Type of return calculation ought be ln_return or stnd_return')


# print(self.calc_returns_from_prices(
#     np.array([2.13, 5.7, 3.1, 4.35, 9.2, 3.7, 4.8])))
# # output
# [0.9843441951191709, -0.6090640633494039, 0.3387737336094921, 0.7490276389544019, -0.9108706644048157, 0.26028309826366636]
