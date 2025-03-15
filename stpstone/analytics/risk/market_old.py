### HANDLING RISK OF ASSETS TRADED IN EXCHANGE MARKETS ###

import math
import numpy as np
import pandas as pd
import cvxopt as opt
import plotly.graph_objs as go
from itertools import combinations
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Any
from stpstone.quantitative_methods.prob_distributions import NormalDistribution
from stpstone.quantitative_methods.linear_algebra import LinearAlgebra
from stpstone.utils.parsers.json import JsonFiles
from stpstone.finance.spot.stocks import ValuingStocks
from stpstone.handling_data.lists import HandlingLists
from stpstone.handling_data.numbers import NumHandler
from stpstone.finance.b3.search_by_trading import TradingFilesB3


class MarketRiskManagement:

    def prices_base_normalizer(self, list_returns, base=100):
        """
        DOCSTRING: RETURNS AS PRICES, PRESERVING DISTANCES THAT WOULD PRESENT THE GIVEN RETURNS
        INPUTS: RETURNS AND BASE
        OUTPUTS: ARRAY
        """
        # Converts returns into prices
        s = [base]
        for i in range(len(list_returns)):
            s.append(base * (1 + list_returns[i]))
        return np.array(s)

    def beta(self, list_returns, list_market):
        """
        REFERENCES: http://www.turingfinance.com/computational-investing-with-python-week-one/
        DOCSTRING: BETA OR CORRELATION TO A MARKET BENCHMARK
        INPUTS: LIST OF STOCK RETURNS AND MARKET RETURNS WITH THE SAME SIZE THROUGH THE SAME PERIOD
        OUTPUTS: FLOAT
        """
        # Create a matrix of [returns, market]
        m = np.matrix([list_returns, list_market])
        # Return the covariance of m divided by the standard deviation of the market returns
        return np.cov(m)[0][1] / np.std(list_market)

    def lower_partial_moment(self, list_returns, threshold, order=2):
        """
        REFERENCES: http://www.turingfinance.com/computational-investing-with-python-week-one/,
            https://breakingdownfinance.com/finance-topics/performance-measurement/lower-partial-moment/
        DOCSTRING: A MEASURE OF DOWNSIDE RISK COMPUTED AS THE AVERAGE OF THE SQUARED DEVIATIONS
            BELOW A TARGET RETURN. THIS MEASURE OF DOWNSIDE RISK IS MORE GENERAL THAN SEMI-
            VARIANCE WHICH IS COMPUTED AS THE AVERAGE OF THE SQUARED DEVIATIONS BELOW THE MEAN
            RETURN
        INPUTS: RETURNS, THRESHOLD AND ORDER (2 AS DEFAULT)
        OUTPUTS: FLOAT
        """
        # this method returns a lower partial moment of the returns
        # create an array he same length as returns containing the minimum return threshold
        threshold_array = np.empty(len(list_returns))
        threshold_array.fill(threshold)
        # calculate the difference between the threshold and the returns
        diff = threshold_array - list_returns
        # set the minimum of each to 0
        diff = diff.clip(min=0)
        # return the sum of the different to the power of order
        return np.sum(diff ** order) / len(list_returns)

    def higher_partial_moment(self, list_returns, threshold, order):
        """
        DOCSTRING: A MEASURE OF DOWNSIDE RISK COMPUTED AS THE AVERAGE OF THE SQUARED DEVIATIONS
            ABOVE A TARGET RETURN. THIS MEASURE OF DOWNSIDE RISK IS MORE GENERAL THAN SEMI-
            VARIANCE WHICH IS COMPUTED AS THE AVERAGE OF THE SQUARED DEVIATIONS BELOW THE MEAN
            RETURN
        INPUTS: RETURNS, THRESHOLD AND ORDER (2 AS DEFAULT)
        OUTPUTS: FLOAT
        """
        # this method returns a higher partial moment of the returns
        # create an array he same length as returns containing the minimum return threshold
        threshold_array = np.empty(len(list_returns))
        threshold_array.fill(threshold)
        # calculate the difference between the returns and the threshold
        diff = list_returns - threshold_array
        # set the minimum of each to 0
        diff = diff.clip(min=0)
        # return the sum of the different to the power of order
        return np.sum(diff ** order) / len(list_returns)

    def parametric_var(self, std_deviation, t, confidence_level, float_mu=0, h=252, ro=None,
                       port_financial_value=1000):
        """
        DOCSTRING: VALUE AT RISK, OR THE STATISTICAL RISK MANAGEMENT TECHINIQUE MEASURING THE
            MAXIMUM LOSS THAT AN INVESTMENTT PORTFOLIO IS LIKELY TO FACE WITHIN A SPECIFIC TIME
            FRAME WITH A CERTAIN DEGREE OF CONFIDENCE
        INPUTS: PORTFOLIO FINANCIAL VALUE, STANDARD DEVIATION, T (NPER), COFIDENCE LEVEL (1-ALFA,
            OR SIGNIFICANCE LEVEL), MU (0 AS DEFAULT), H (NOMINAL TIME HORIZON FOR SCALING PURPOSES,
            NUMBER OF WORKING DAYS IN A YEAR, 252 AS DEFAULT) AND RO (AUTOCORRELATION,
            NONE AS DEFAULT)
        OUTPUTS: FINANCIAL POTENCIAL LOSS AND ITS PERCENTAGE OF THE PORTFOLIO (DICTIONARY WITH
            FINANCIAL VAR, PERCENTUAL VAR AND DRIFTED ADJUSTMENT, OR ERROR, THAT WILL BE POSITIVE
            IF THE EXPECTED PORTFOLIO RETURN IS GREATER THAN RISK FREE RATE OF RETURN, AND NEGATIVE
            OTHERWISE)
        """
        # wheter or not to consider a correlation to correct the transformation among risk
        #   time periods
        if ro:
            # calculating the scaling time horizon
            h_corrected = h + 2 * ro * \
                (1 - ro) ** (-2) * ((h - 1) * (1 - ro) - ro * (1 - ro ** (h - 1)))
        else:
            h_corrected = t / h
        # returning result of parametric var
        return {
            'financial_var': port_financial_value * (std_deviation * np.sqrt(
                h_corrected) * NormalDistribution().inv_norm(
                confidence_level) - (t / h) * float_mu),
            'percentual_var': std_deviation * np.sqrt(
                h_corrected) * NormalDistribution().inv_norm(
                confidence_level) - (t / h) * float_mu,
            'drifted_adjustment_to_var': std_deviation * np.sqrt(
                h_corrected) * NormalDistribution().inv_norm(
                confidence_level) - (std_deviation * np.sqrt(
                    h_corrected) * NormalDistribution().inv_norm(
                    confidence_level) - (t / h) * float_mu)
        }

    def equity_var(self, std_deviation, t, confidence_level, list_betas,
                   list_financial_exposures, float_mu=0, h=252, ro=None):
        """
        DOCSTRING: EQUITY VAR, OR SENSITIVITY OF ITS RISK FACTORS PORTFOLIO
        INPUTS: STANDARD DEVIATION, T (NPER), COFIDENCE LEVEL (1-ALFA, OR SIGNIFICANCE LEVEL),
            MU (0 AS DEFAULT), H (NOMINAL TIME HORIZON FOR SCALING PURPOSES, NUMBER OF WORKING
            DAYS IN A YEAR, 252 AS DEFAULT) AND RO (AUTOCORRELATION, NONE AS DEFAULT)
        OUTPUTS: DICTIONARY WITH FINANCIAL VAR AND PERCENTUAL VAR
        """
        # calculating the beta of the portfolio
        beta_portfolio = NumHandler().sumproduct(list_betas, list_financial_exposures) \
            / sum(list_financial_exposures)
        # returning the expected loss of the portfolio regarding a not estressed scenario
        return {
            'financial_var': MarketRiskManagement().parametric_var(
                std_deviation, t, confidence_level, float_mu, h, ro)[
                    'percentual_var'] * beta_portfolio * sum(list_financial_exposures),
            'percentual_var': MarketRiskManagement().parametric_var(
                std_deviation, t, confidence_level, float_mu, h, ro)['percentual_var'] * beta_portfolio
        }

    def normal_linear_interest_rate_var(self, confidence_level, array_pv01, array_yields, t, h,
                                        correl_yields=0, float_mu=0):
        """
        DOCSTRING: LINEAR RISK CASH FLOW MODEL, OR CASH FLOW MAP, IS THE INTEREST RATE VAR OF
            BONDS, SWAPS AND LOAN PORTFOLIOS THAT CAN BE REPRESENTED AS A SERIES OF CASH FLOWS
        INPUTS: CONFIDENCE LEVEL, ARRAY PV01, ARRAY DERIVATIVE PV01, ARRAY YIELDS, MU (0 AS DEFAULT)
        OUPUTS: FLOAT
        """
        # covariance matrix of array of yields
        cov_mtx = np.zeros((len(array_yields), len(array_yields)))
        for i in range(len(array_yields)):
            for j in range(len(array_yields)):
                if i == j:
                    cov_mtx[i, j] = array_yields[i] ** (len(array_yields))
                else:
                    cov_mtx[i, j] = correl_yields * \
                        array_yields[i] * array_yields[j]
        cov_mtx = (t / h) * cov_mtx
        # float_mu vector
        float_mu = np.array([float_mu] * len(array_pv01))
        # returning the normal linear interest rate var
        return NormalDistribution().inv_norm(confidence_level) * np.sqrt(
            LinearAlgebra().matrix_multiplication(LinearAlgebra().matrix_multiplication(
                array_pv01, cov_mtx), LinearAlgebra().transpose_matrix(array_pv01))) \
            + LinearAlgebra().matrix_multiplication(float_mu, LinearAlgebra().transpose_matrix(
                array_pv01))

    def expected_tail_loss(self, nth_lowest=10, port_financial_value=1000,
                           list_prices=None, list_returns=None):
        """
        DOCSTRING: EXPECTED TAIL LOSS FOR A NTH LOWEST AVERAGE LIST OF RETURNS
        INPUTS: NTH LOWEST (10 AS DEFAULT), PORTFOLIO FINANCIAL VALUE (1000 AS VALUE), 
            LIST OF PRICES (NONE AS DEFAULT), LIST OF RETURNS (NONE AS DEFAULT)
        OUTPUTS: DICTIONAY (PERCENTAGE ETL AND FINANCIAL ETL)
        """
        # checking parameters
        if any(all([x != None for x in [list_prices, list_returns]]),
               all([x is None for x in [list_prices, list_returns]])):
            raise Exception('Please revisit original prices and returns arguments; only one '
                            'of them ought be different from none')
        # defining returns
        if list_prices:
            list_cacl_returns = ValuingStocks().calc_returns_from_prices(list_prices)
        elif list_returns:
            list_cacl_returns = list_returns
        else:
            raise Exception('Please revisit original prices and returns arguments; only one '
                            'of them ought be different from none')
        # nth lowest returns
        return {
            'percentage_etl': np.average(HandlingLists().nth_smallest_numbers(
                list_cacl_returns, nth_lowest)),
            'financial_etl': np.average(HandlingLists().nth_smallest_numbers(
                list_cacl_returns, nth_lowest)) * port_financial_value
        }

    def drawdown(self, tau, list_original_prices=None, list_returns=None, prices_base=100):
        """
        DOCSTRING: DRAWDOWN, OR HIGHEST DECREASE OF RETURN FOR A GIVEN PORTFOLIO, AMID A MOVING
            TIME RANGE, PRESERVING ITS SIZE
        INPUTS: RETURNS, TAU (TIME PERIOD) AND PRICES BASE (100 AS DEFAULT)
        OUTPUTS: FLOAT
        """
        # returns the drawdown given time period tau
        if list_returns:
            values = MarketRiskManagement().prices_base_normalizer(list_returns, prices_base)
        if list_original_prices:
            values = list_original_prices
        if all([x != None for x in [list_original_prices, list_returns]]):
            raise Exception('Please revisit original prices and returns arguments; only one '
                            'of them ought be different from none')
        pos = len(values) - 1
        pre = pos - tau
        drawdown = float('+inf')
        # find the maximum drawdown given tau
        while pre >= 0:
            dd_i = (values[pos] / values[pre]) - 1
            if dd_i < drawdown:
                drawdown = dd_i
            pos, pre = pos - 1, pre - 1
        # drawdown should be positive
        return abs(drawdown)

    def max_drawdown(self, list_original_prices=None, list_returns=None, prices_base=100):
        """
        DOCSTRING: MAXIMUM DRAW DOWN FOR A GIVEN PORTFOLIO
        INPUTS: RETURNS
        OUTPUTS: FLOAT
        """
        # returns the maximum drawdown for any tau in (0, T) where T is the length of
        #   the return series
        max_drawdown = float('-inf')
        # number of occurrencies in the sample
        if list_original_prices:
            len_observations_data = len(list_original_prices)
        elif list_returns:
            len_observations_data = len(list_returns)
        else:
            raise Exception(
                'List of original prices or returns ought be provided')
        for i in range(len_observations_data):
            drawdown_i = MarketRiskManagement().drawdown(i, list_original_prices,
                                                         list_returns, prices_base)
            if drawdown_i > max_drawdown:
                max_drawdown = drawdown_i
        # max drawdown should be positive
        return abs(max_drawdown)

    def ewma(self, list_daily_returns, int_wdy=252, accuracy=0.01):
        """
        REFERENCES: https://www.investopedia.com/articles/07/ewma.asp
        DOCSTRING: EXPONENTIALLY WEIGHTED MOVING AVERAGE
        INPUTS: DAILY RETURNS, LAMBDA SMOOTHING PARAMETER (BY DEFAULT 94%, ACCORDING TO
            RISKMETRICS)
        OUTPUTS:
        """
        # number of observations precision-wise
        list_accuracy_0_01 = [44, 49, 55, 63, 74, 90, 113, 151, 228, 458]
        list_accuracy_0_001 = [66, 73, 83, 95, 112, 135, 169, 227, 342]
        list_accuracy_0_0001 = [87, 98, 110, 127, 149, 180, 226, 302, 456, 916]
        list_lambdas = [x / 100 for x in range(90, 100)]
        # lambda due to precision and number of observations
        curr_accuracy_list = locals()[
            'list_accuracy_' + str(accuracy).replace('.', '_')]
        # finding lambda according to number of observations and accuracy
        if accuracy in [0.01, 0.001, 0.0001]:
            if len(list_daily_returns) > curr_accuracy_list[-1]:
                lamba_smoothing_parameter = list_lambdas[-1]
            else:
                lamba_smoothing_parameter = list_lambdas[curr_accuracy_list.index(
                    HandlingLists().closest_bound(curr_accuracy_list, len(list_daily_returns)))]
        else:
            return 'Poor defined variable accuracy, please make sure its within ' \
                + '[0.01, 0.001, 0.0001] possible values'
        # periodic returns
        u_ln_returns = [ValuingStocks().continuous_return(
            list_daily_returns[p - 1],
            list_daily_returns[p]) for p in range(1, len(list_daily_returns))]
        # array_weights
        array_weights = [(1 - lamba_smoothing_parameter) * lamba_smoothing_parameter ** (d - 1) for
                         d in range(1, len(list_daily_returns))]
        array_weights.reverse()
        # ewma daily contributions
        if len(u_ln_returns) == len(array_weights):
            ewma_daily_contributions_list = [array_weights[i] * u_ln_returns[i] ** 2 for i in
                                             range(len(u_ln_returns))]
        else:
            return 'Please check wheter daily returns and array_weights list are the same size and ' \
                'try again.'
        # variance and standard deviation ewma-wise
        dict_message = {
            'variance_ewma_daily': sum(ewma_daily_contributions_list),
            'std_ewma_daily': math.sqrt(sum(ewma_daily_contributions_list)),
            'variance_ewma_yearly': (math.sqrt(sum(
                ewma_daily_contributions_list)) * math.sqrt(int_wdy)) ** 2,
            'std_ewma_yearly': math.sqrt(sum(
                ewma_daily_contributions_list)) * math.sqrt(int_wdy)
        }
        return JsonFiles().send_json(dict_message)

    def systematic_specific_risk(self, variance_portfolio, std_market, array_stocks_weights,
                                 array_stocks_beta):
        """
        DOCSTRING: SYSTEMATIC AND SPECIFIC RISK FOR A GIVEN PORTFOLIO
        INPUTS: STD PORTFOLIO, ARRAY STOCKS WEIGHTS AND ARRAY STOCKS BETA
        OUTPUTS: DICTIONARY WITH STD PORTFOLIO, SYSTEMATIC RISK AND SPECIFIC RISK
        """
        beta_portfolio = \
            LinearAlgebra().matrix_multiplication(np.array(array_stocks_weights),
                                                  LinearAlgebra().transpose_matrix(
                                                      np.array(array_stocks_beta)))
        systematic_risk = math.pow(beta_portfolio, 2) * \
            math.pow(std_market, 2)
        specific_risk = variance_portfolio - systematic_risk
        return {
            'variance_portfolio': variance_portfolio,
            'systematic_risk': systematic_risk,
            'specifici_risk': specific_risk
        }