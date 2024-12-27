### HANDLING RISK OF ASSETS TRADED IN EXCHANGE MARKETS ###

import math
import datetime
import numpy as np
import pandas as pd
import cvxopt as opt
import plotly.graph_objs as go
from itertools import combinations
from typing import List, Tuple, Optional
from stpstone.quantitative_methods.prob_distributions import NormalDistribution
from stpstone.quantitative_methods.linear_algebra import LinearAlgebra
from stpstone.handling_data.json import JsonFiles
from stpstone.finance.spot.stocks import ValuingStocks
from stpstone.handling_data.lists import HandlingLists
from stpstone.handling_data.numbers import NumHandler


class MarketRiskManagement:

    def prices_base_normalizer(self, list_returns, base=100):
        '''
        DOCSTRING: RETURNS AS PRICES, PRESERVING DISTANCES THAT WOULD PRESENT THE GIVEN RETURNS
        INPUTS: RETURNS AND BASE
        OUTPUTS: ARRAY
        '''
        # Converts returns into prices
        s = [base]
        for i in range(len(list_returns)):
            s.append(base * (1 + list_returns[i]))
        return np.array(s)

    def beta(self, list_returns, list_market):
        '''
        REFERENCES: http://www.turingfinance.com/computational-investing-with-python-week-one/
        DOCSTRING: BETA OR CORRELATION TO A MARKET BENCHMARK
        INPUTS: LIST OF STOCK RETURNS AND MARKET RETURNS WITH THE SAME SIZE THROUGH THE SAME PERIOD
        OUTPUTS: FLOAT
        '''
        # Create a matrix of [returns, market]
        m = np.matrix([list_returns, list_market])
        # Return the covariance of m divided by the standard deviation of the market returns
        return np.cov(m)[0][1] / np.std(list_market)

    def lower_partial_moment(self, list_returns, threshold, order=2):
        '''
        REFERENCES: http://www.turingfinance.com/computational-investing-with-python-week-one/,
            https://breakingdownfinance.com/finance-topics/performance-measurement/lower-partial-moment/
        DOCSTRING: A MEASURE OF DOWNSIDE RISK COMPUTED AS THE AVERAGE OF THE SQUARED DEVIATIONS
            BELOW A TARGET RETURN. THIS MEASURE OF DOWNSIDE RISK IS MORE GENERAL THAN SEMI-
            VARIANCE WHICH IS COMPUTED AS THE AVERAGE OF THE SQUARED DEVIATIONS BELOW THE MEAN
            RETURN
        INPUTS: RETURNS, THRESHOLD AND ORDER (2 AS DEFAULT)
        OUTPUTS: FLOAT
        '''
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
        '''
        DOCSTRING: A MEASURE OF DOWNSIDE RISK COMPUTED AS THE AVERAGE OF THE SQUARED DEVIATIONS
            ABOVE A TARGET RETURN. THIS MEASURE OF DOWNSIDE RISK IS MORE GENERAL THAN SEMI-
            VARIANCE WHICH IS COMPUTED AS THE AVERAGE OF THE SQUARED DEVIATIONS BELOW THE MEAN
            RETURN
        INPUTS: RETURNS, THRESHOLD AND ORDER (2 AS DEFAULT)
        OUTPUTS: FLOAT
        '''
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

    def parametric_var(self, std_deviation, t, confidence_level, mu=0, h=252, ro=None,
                       port_financial_value=1000):
        '''
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
        '''
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
                confidence_level) - (t / h) * mu),
            'percentual_var': std_deviation * np.sqrt(
                h_corrected) * NormalDistribution().inv_norm(
                confidence_level) - (t / h) * mu,
            'drifted_adjustment_to_var': std_deviation * np.sqrt(
                h_corrected) * NormalDistribution().inv_norm(
                confidence_level) - (std_deviation * np.sqrt(
                    h_corrected) * NormalDistribution().inv_norm(
                    confidence_level) - (t / h) * mu)
        }

    def equity_var(self, std_deviation, t, confidence_level, list_betas,
                   list_financial_exposures, mu=0, h=252, ro=None):
        '''
        DOCSTRING: EQUITY VAR, OR SENSITIVITY OF ITS RISK FACTORS PORTFOLIO
        INPUTS: STANDARD DEVIATION, T (NPER), COFIDENCE LEVEL (1-ALFA, OR SIGNIFICANCE LEVEL),
            MU (0 AS DEFAULT), H (NOMINAL TIME HORIZON FOR SCALING PURPOSES, NUMBER OF WORKING
            DAYS IN A YEAR, 252 AS DEFAULT) AND RO (AUTOCORRELATION, NONE AS DEFAULT)
        OUTPUTS: DICTIONARY WITH FINANCIAL VAR AND PERCENTUAL VAR
        '''
        # calculating the beta of the portfolio
        beta_portfolio = NumHandler().sumproduct(list_betas, list_financial_exposures) \
            / sum(list_financial_exposures)
        # returning the expected loss of the portfolio regarding a not estressed scenario
        return {
            'financial_var': MarketRiskManagement().parametric_var(
                std_deviation, t, confidence_level, mu, h, ro)[
                    'percentual_var'] * beta_portfolio * sum(list_financial_exposures),
            'percentual_var': MarketRiskManagement().parametric_var(
                std_deviation, t, confidence_level, mu, h, ro)['percentual_var'] * beta_portfolio
        }

    def normal_linear_interest_rate_var(self, confidence_level, array_pv01, array_yields, t, h,
                                        correl_yields=0, mu=0):
        '''
        DOCSTRING: LINEAR RISK CASH FLOW MODEL, OR CASH FLOW MAP, IS THE INTEREST RATE VAR OF
            BONDS, SWAPS AND LOAN PORTFOLIOS THAT CAN BE REPRESENTED AS A SERIES OF CASH FLOWS
        INPUTS: CONFIDENCE LEVEL, ARRAY PV01, ARRAY DERIVATIVE PV01, ARRAY YIELDS, MU (0 AS DEFAULT)
        OUPUTS: FLOAT
        '''
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
        # mu vector
        mu = np.array([mu] * len(array_pv01))
        # returning the normal linear interest rate var
        return NormalDistribution().inv_norm(confidence_level) * np.sqrt(
            LinearAlgebra().matrix_multiplication(LinearAlgebra().matrix_multiplication(
                array_pv01, cov_mtx), LinearAlgebra().transpose_matrix(array_pv01))) \
            + LinearAlgebra().matrix_multiplication(mu, LinearAlgebra().transpose_matrix(
                array_pv01))

    def expected_tail_loss(self, nth_lowest=10, port_financial_value=1000,
                           list_prices=None, list_returns=None):
        '''
        DOCSTRING: EXPECTED TAIL LOSS FOR A NTH LOWEST AVERAGE LIST OF RETURNS
        INPUTS: NTH LOWEST (10 AS DEFAULT), PORTFOLIO FINANCIAL VALUE (1000 AS VALUE), 
            LIST OF PRICES (NONE AS DEFAULT), LIST OF RETURNS (NONE AS DEFAULT)
        OUTPUTS: DICTIONAY (PERCENTAGE ETL AND FINANCIAL ETL)
        '''
        # checking parameters
        if any(all([x != None for x in [list_prices, list_returns]]),
               all([x == None for x in [list_prices, list_returns]])):
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
        '''
        DOCSTRING: DRAWDOWN, OR HIGHEST DECREASE OF RETURN FOR A GIVEN PORTFOLIO, AMID A MOVING
            TIME RANGE, PRESERVING ITS SIZE
        INPUTS: RETURNS, TAU (TIME PERIOD) AND PRICES BASE (100 AS DEFAULT)
        OUTPUTS: FLOAT
        '''
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
        '''
        DOCSTRING: MAXIMUM DRAW DOWN FOR A GIVEN PORTFOLIO
        INPUTS: RETURNS
        OUTPUTS: FLOAT
        '''
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
        '''
        REFERENCES: https://www.investopedia.com/articles/07/ewma.asp
        DOCSTRING: EXPONENTIALLY WEIGHTED MOVING AVERAGE
        INPUTS: DAILY RETURNS, LAMBDA SMOOTHING PARAMETER (BY DEFAULT 94%, ACCORDING TO
            RISKMETRICS)
        OUTPUTS:
        '''
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
        '''
        DOCSTRING: SYSTEMATIC AND SPECIFIC RISK FOR A GIVEN PORTFOLIO
        INPUTS: STD PORTFOLIO, ARRAY STOCKS WEIGHTS AND ARRAY STOCKS BETA
        OUTPUTS: DICTIONARY WITH STD PORTFOLIO, SYSTEMATIC RISK AND SPECIFIC RISK
        '''
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


class Markowitz:
    '''
    REFERENCES: https://www.linkedin.com/pulse/python-aplicado-markowitz-e-teoria-nem-tão-moderna-de-paulo-rodrigues/?originalSubdomain=pt
    DOCSTRING: MARKOWITZ RISK-RETURN PLOT OF RANDOM PORTFOLIOS, AIMING TO FIND THE BEST ALLOCATION 
        WITH THE ASSETS PROVIDED
    INPUTS: -
    OUTPUTS: -
    '''

    def __init__(self, list_securities:List[str], float_prtf_notional:float) -> None:
        self.list_securities = list_securities
        self.float_prtf_notional = float_prtf_notional

    def sharpe_ratio(self, mu:float, sigma:float, float_rf:float) -> float:
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        return (float(mu) - float(float_rf)) / float(sigma)

    def sigma_portfolio(self, array_weights:np.array, array_returns:np.array) -> float:
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # covariance between stocks
        array_cov = np.cov(array_returns)
        # returning portfolio standard deviation
        return np.sqrt(np.dot(array_weights.T, np.dot(array_cov, array_weights)))
    
    def min_w_asset(self, df_assets:pd.DataFrame, col_id:str, col_close:str, 
                    col_dt:datetime.datetime, col_min_w:str='min_w', 
                    col_max_date:str='max_date', col_last_close:str='last_close') -> pd.DataFrame:
        '''
        DOCSTRING: MINIMAL WEIGHT ALLOCATION PER ASSET
        INPUTS:
        OUTPUTS:
        '''
        df_assets[col_max_date] = df_assets.groupby([col_id])[col_dt].transform('max')
        df_assets[col_last_close] = df_assets.groupby([
            col_id, 
            col_max_date
        ])[col_close].transform('last')
        df_assets[col_min_w] = df_assets.groupby([
            col_id, 
            col_max_date
        ])[col_close].transform('last') / self.float_prtf_notional
        return df_assets

    def returns_min_w_uids(self, df_assets:pd.DataFrame, col_dt:str, col_id:str, 
                           col_returns:str, col_min_w:str) \
                            -> Tuple[np.ndarray, np.ndarray, List[str]]:
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # filter where returns are not nulls
        df_assets = df_assets[~df_assets[col_returns].isnull()]
        # returns per uids
        array_returns = df_assets.pivot_table(
            index=col_dt,
            columns=col_id,
            values=col_returns
        ).to_numpy()
        array_returns = array_returns.T
        array_returns = np.nan_to_num(array_returns, nan=0.0)
        # minimum weights per uids
        array_min_w = np.array(
            df_assets.groupby(
                col_id
            )[col_min_w].unique(),
            dtype=float
        )
        array_min_w = array_min_w.T
        # list of uids
        list_uids = HandlingLists().remove_duplicates(df_assets[col_id].to_list())
        # return arrays of interet
        return array_returns, array_min_w, list_uids

    def random_weights(self, int_n_assets:int, bl_constraints:bool=False, bl_multiplier:bool=False, 
                       array_min_w:np.array=None, nth_try:int=100, int_idx_val:int=2, 
                       bl_valid_weights:bool=False, i_attempts:int=0, 
                       float_atol_sum:float=1e-4, float_atol_w:float=10000.0) -> np.array:
        '''
        DOCSTRING: RANDOM WEIGHTS - WITH OR WITHOUT CONSTRAINTS
        INPUTS:
            - INT_N_ASSETS: THE NUMBER OF ASSETS IN THE PORTFOLIO
            - BL_CONSTRAINTS: BOOLEAN FLAG TO APPLY CONSTRAINTS OR NOT
            - MIN_INVEST_PER_ASSET: A LIST OF MINIMUM WEIGHTS/INVESTMENTS FOR EACH ASSET
        OUTPUTS:
            - A LIST OF WEIGHTS FOR THE ASSETS THAT SATISFY THE GIVEN CONSTRAINTS, 
                WHERE SUM OF WEIGHTS = 1
        '''
        # adjusting number of assets within the portfolio
        int_idx_val = min(len(array_min_w), int_idx_val)
        # check wheter the constraints are enabled
        if bl_constraints == True:
            #   sanity check for constraints
            if array_min_w is None:
                raise ValueError('MIN_INVEST_PER_ASSET MUST BE PROVIDED AS A LIST WHEN '
                                 + 'CONSTRAINTS ARE ENABLED.')
            if any(isinstance(x, str) for x in array_min_w):
                raise ValueError('MIN_INVEST_PER_ASSET MUST BE A LIST OF NUMBERS.')
            if len(array_min_w) != int_n_assets:
                raise ValueError('THE LENGTH OF MIN_INVEST_PER_ASSET MUST MATCH THE '
                                 + 'NUMBER OF ASSETS.')
            if any(x < 0 for x in array_min_w):
                raise ValueError('MIN_INVEST_PER_ASSET MUST BE POSITIVE.')
            if any(x > 1 for x in array_min_w):
                raise ValueError('MIN_INVEST_PER_ASSET MUST BE BELOW 1.0')
            if any(x == 0 for x in array_min_w):
                raise ValueError('EVERY MIN_INVEST_PER_ASSET MUST BE GREATER THAN 0.')
            #   initializing variables
            bl_valid_weights = False
            list_combs = [
                comb 
                for r in range(2, int_idx_val + 1) 
                for comb in combinations(array_min_w, r)
            ]
            #   recursive call to get valid weights
            while not bl_valid_weights:
                #   increment the try counter
                i_attempts += 1
                #   reseting variables
                array_w = np.zeros(int_n_assets)
                #   check if it's the nth try or all the combinations are greater than one
                if \
                    (i_attempts >= nth_try)\
                    or (all([sum(comb) >= 1.0 for comb in list_combs])):
                    #   return a weight array with one asset having weight 1.0 and others 0.0
                    array_w = np.zeros(int_n_assets)
                    int_idx = np.random.randint(0, int_n_assets)
                    array_w[int_idx] = 1.0
                    return array_w
                #   if multiplier is enabled, build a list of possible indexes combinations in 
                #       order to sum 1.0 or less
                if bl_multiplier == True:
                    #   combinations where sum is less than 1.0 - flatten list
                    list_i = HandlingLists().remove_duplicates([
                        idx
                        for comb in list_combs
                        for x in comb
                        for idx in np.where(array_min_w == x)[0]
                        if sum(comb) <= 1.0
                    ])
                else:
                    list_i = list(range(int_n_assets))
                np.random.shuffle(list_i)
                #   looping through the indexes
                for i in list_i:
                    #   randomly building a float weight
                    float_upper_tol = max(
                        float_atol_w * (1.0 - sum(array_w)), 
                        1.0
                    )
                    #   building the float weight with any given value above the minimum or a 
                    #       multiple of the minimum
                    if bl_multiplier == True:
                        int_max_mult = max(
                            int((1.0 - sum(array_w)) // array_min_w[i]), 
                            1
                        )
                        int_rand_mult = np.random.randint(0, int_max_mult + 1)
                        float_weight = float(int_rand_mult * array_min_w[i])
                    else:
                        float_upper_tol = max(
                            float_atol_w * (1.0 - sum(array_w)), 
                            1.0
                        )
                        random_integer = np.random.randint(
                            0,
                            float_upper_tol
                        )
                        float_weight = float(random_integer) / float_upper_tol
                    #   check if the weight is greater than the minimum
                    if float_weight < array_min_w[i]:
                        array_w[i] = 0
                    else:
                        array_w[i] = float_weight
                    #   check if the sum of weights is equal to 1.0 or greater
                    if sum(array_w) >= 1.0: break
                #   normalize only if the total weight is non-zero, if multiplier is unabled
                if (bl_multiplier == False) or (np.count_nonzero(array_w) == 1):
                    total_weight = np.sum(array_w)
                    if total_weight > 0:
                        array_w /= total_weight
                #   sanity checks for weights:
                #       1 - all weights must be non-negative
                #       2 - sum must be equal to 1
                #       3 - the minimum must be respected, or zero for a given asset
                #       4 - some weight must be positive
                bl_valid_weights = (
                    np.all(array_w >= 0) 
                    and np.isclose(np.sum(array_w), 1, atol=float_atol_sum) 
                    and all([
                        (array_w[i] >= array_min_w[i]) \
                            or (array_w[i] == 0)
                        for i in range(int_n_assets)
                    ]) 
                    and np.any(array_w > 0)
                    and np.all(array_w != 1)
                )
            return array_w
        else:
            # if no constraints are applied, return standard random weights
            k = np.random.rand(int_n_assets)
            return k / sum(k)

    def random_portfolio(self, array_returns:np.ndarray, float_rf:float, bl_constraints:bool=False, 
                        bl_multiplier:bool=False, array_min_w:Optional[np.ndarray]=None, 
                        nth_try:int=100, int_wdy:int=252) \
                            -> Tuple[np.ndarray, np.ndarray, np.ndarray, str]:
        '''
        DOCSTRING: RETURNS THE MEAN AND STANDARD DEVIATION OF RETURNS FROM A RANDOM PORTFOLIO
        INPUTS: MATRIX ASSETS RETURNS, ARRAY EXPECTED RETURNS, FLOAT RISK FREE
        OUTPUTS: TUP OF FLOATS
        '''
        # adjusting variables' types
        array_r = np.asmatrix(array_returns)
        float_rf = float(float_rf)
        # random wieghts for the current portfolio
        array_weights = self.random_weights(array_r.shape[0], bl_constraints, bl_multiplier, 
                                            array_min_w, nth_try)
        # mean returns for assets
        array_returns = np.asmatrix(np.mean(array_r, axis=1))
        # portfolio standard deviation
        array_sigmas = self.sigma_portfolio(array_weights, array_r) * np.sqrt(int_wdy)
        # portfolio expected return
        array_mus = float(array_weights * array_returns) * int_wdy
        # sharpes ratio
        array_sharpes = self.sharpe_ratio(array_mus, array_sigmas, float_rf)
        # changing type of array weights to transform into one value
        array_weights = ' '.join([str(x) for x in array_weights])
        # returning portfolio infos
        return array_mus, array_sigmas, array_sharpes, array_weights

    def random_portfolios(self, df_assets:pd.DataFrame, int_n_portfolios:int, col_id:str, 
                          col_close:str, col_dt:datetime.datetime, col_returns:str, 
                          float_prtf_notional:float, col_min_w:str='min_w', float_rf:float=0.0, 
                          bl_constraints:bool=False, bl_multiplier:bool=False, 
                          nth_try:int=100, int_wdy:int=252) \
                            -> Tuple[np.ndarray, np.ndarray, np.ndarray, 
                                     np.ndarray, np.ndarray, List[str]]:
        '''
        DOCSTRING: RETURNS THE MEAN AND STANDARD DEVIATION OF RETURNS FROM A RANDOM PORTFOLIO
        INPUTS: MATRIX ASSETS RETURNS, ARRAY EXPECTED RETURNS, FLOAT RISK FREE
        OUTPUTS: TUP OF FLOATS
        '''
        # min weights per asset
        df_assets = self.min_w_asset(df_assets, col_id, col_close, col_dt, 
                                             float_prtf_notional)
        # arrays of retunrs and minimum weights per asset
        array_returns, array_min_w, list_uuids = \
            self.returns_min_w_uids(
                df_assets,
                col_dt,
                col_id,
                col_returns,
                col_min_w,
            )
        # generating random portfolios
        array_mus, array_sigmas, array_sharpes, array_weights = \
            np.column_stack([
                self.random_portfolio(
                    array_returns,
                    float_rf,
                    bl_constraints,
                    bl_multiplier,
                    array_min_w,
                    nth_try,
                    int_wdy
                )
                for _ in range(int_n_portfolios)
            ])
        # altering data types
        array_mus = array_mus.astype(float)
        array_sigmas = array_sigmas.astype(float)
        array_sharpes = array_sharpes.astype(float)
        return array_mus, array_sigmas, array_sharpes, array_weights, array_returns, list_uuids

    def optimal_portfolios(self, array_returns:np.ndarray, n_attempts:int=1000,
                           bl_progress_printing_opt:bool=False, int_wdy:int=252) \
                             -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        '''
        DOCSTRING: WEIGHTS RETURNS AND SIGMA FOR EFFICIENT FRONTIER
        INPUTS: MATRIX OF ASSETS' RETURNS
        OUTPUTS: TUP OF ARRAYS
        '''
        # turn on/off progress printing
        opt.solvers.options['show_progress'] = bl_progress_printing_opt
        # configuring data types
        array_returns = np.asmatrix(array_returns)
        # definig the number of portfolios to be created
        n = array_returns.shape[0]
        # calculating first attempt for mu in each portfolio
        mus = [10.0 ** (5.0 * float(t / n_attempts) - 1.0)
               for t in range(n_attempts)]
        # convert to cvxopt matrices
        S = opt.matrix(np.cov(array_returns))
        pbar = opt.matrix(np.mean(array_returns, axis=1))
        # create constraint matrices
        #   negative n x n identity matrix
        G = -opt.matrix(np.eye(n))
        h = opt.matrix(0.0, (n, 1))
        A = opt.matrix(1.0, (1, n))
        b = opt.matrix(1.0)
        # calculate efficient frontier weights using quadratic programming
        list_portfolios = [opt.solvers.qp(mu * S, -pbar, G, h, A, b)['x']
                           for mu in mus]
        # calculating risk and return for efficient frontier
        array_returns = [opt.blas.dot(
            pbar, x) * int_wdy for x in list_portfolios]
        array_sigmas = [np.sqrt(opt.blas.dot(
            x, S * x)) * np.sqrt(int_wdy) for x in list_portfolios]
        # calculate the second degree polynomial of the frontier curve
        m1 = np.polyfit(array_returns, array_sigmas, 2)
        x1 = np.sqrt(m1[2] / m1[0])
        # calculate the optimal portfolio
        wt = opt.solvers.qp(opt.matrix(x1 * S), -pbar, G, h, A, b)['x']
        # returning weights, returns, and sigma from efficient frontier
        return np.asarray(wt), array_returns, array_sigmas

    def eff_frontier(self, array_eff_risks:np.array, array_eff_returns:np.array, 
                     array_weights:np.array, array_mus:np.array, array_sigmas:np.array, 
                     float_rf:float, col_sigma:str='sigma', col_mu:str='mu', col_w:str='weights', 
                     col_sharpe:str='sharpe', array_eff_weights:list=list(), atol:float=1e-2, 
                     int_pace_atol:int=5) -> Tuple[pd.DataFrame, pd.DataFrame]:
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # convert string-based array_weights to a 2D array by splitting the values
        array_weights_2d = np.array([list(map(float, row.split())) for row in array_weights])
        # iterate over the efficient returns and risks
        for _, eff_risk in zip(array_eff_returns, array_eff_risks):
            while True:
                try:
                    #   find the indices in sigmas that correspond to the current risk using 
                    #       np.isclose
                    list_idx_sigma = np.where(np.isclose(array_sigmas, eff_risk, atol=atol))
                    #   get the highest return for the given datasets
                    idx_mu = np.argmax(array_mus[list_idx_sigma])
                    # print(eff_risk, list_idx_sigma, array_mus[list_idx_sigma], idx_mu)
                    # raise Exception('BREAK')
                    #   get the index from mus and append weights
                    array_eff_weights.append(array_weights_2d[idx_mu])
                    #   in case of no error break the loop
                    break
                except ValueError:
                    atol *= int_pace_atol
        # convert to numpy array for final output if needed
        array_eff_weights = np.array(array_eff_weights)
        # create a dataframe
        columns = [f'weight_{i}' for i in range(array_eff_weights.shape[1])]
        df_eff = pd.DataFrame(array_eff_weights, columns=columns)
        df_eff[col_mu] = array_eff_returns
        df_eff[col_sigma] = array_eff_risks
        # calculate sharpe as the difference between array_eff_returns and float_rf 
        #   divided by array_eff_risks
        df_eff[col_sharpe] = (df_eff[col_mu] - float_rf) / df_eff[col_sigma]
        # create a pandas dataframe with returns, weights and mus from the original porfolios
        df_porf = pd.DataFrame({col_mu: array_mus, col_sigma: array_sigmas, col_w: array_weights})
        # output the results
        return df_eff, df_porf
    
    def plot_risk_return_portfolio(self, array_weights:np.array, array_mus:np.array, 
                                   array_sigmas:np.array, array_sharpes:np.array, 
                                   array_eff_risks:np.array, array_eff_returns:np.array, 
                                   array_eff_weights:np.array, bl_debug_mode:bool=False, 
                                   bl_show_plot:bool=False, complete_path_save_fig:Optional[str]=None,
                                   title_text:str='Markowitz Risk x Return Portfolios',
                                   yaxis_title:str='Return (%)', xaxis_title:str='Risk (%)') -> None:
        '''
        REFERENCES: https://plotly.com/python/reference/layout/, 
            https://plotly.com/python-api-reference/generated/plotly.graph_objects.Scatter.html, 
            https://plotly.com/python/builtin-colorscales/
        DOCSTRING: PLOT MARKOWITZ'S EFFICIENT FRONTIER FOR PORTFOLIO MANAGEMENT
        INPUTS: ARRAY WEIGHTS, ARRAY MUS (MEAN RETURNS FOR EACH GIVEN PORTFOLIO, BASED ON EXPCTED 
            RETURNS FOR EACH SECURITY, GIVEN ITS WEIGHT ON THE SYNTHETIC PORTFOLIO), ARRAY OF SHARPES, 
            ARRAY OF EFFECTIVE RISKS, ARRAY OF EFFECTIVE RETURN FOR ALL SECURITIES IN A PORTFOLIO, 
            TITLE, YAXIS NAME AND XAXIS NAME
        OUTPUTS: PLOT
        '''
        # maximum sharpe portfolio
        if bl_debug_mode == True:
            print('### MAXIMUM SHARPE PORTFOLIO ###')
            print('SHARPES ARGMAX: {}'.format(array_sharpes.argmax()))
            print('WEIGHTS: {}'.format(array_weights[array_sharpes.argmax()]))
            print('RISK: {}'.format(array_sigmas[array_sharpes.argmax()]))
            print('RETURN: {}'.format(array_mus[array_sharpes.argmax()]))
            print('SHARPE: {}'.format(array_sharpes[array_sharpes.argmax()]))
        # prepare customdata for scatter plot
        customdata_portfolios = np.array([
            [weights, ', '.join(self.list_securities)] for weights in array_weights
        ], dtype=object)
        # Ppepare the subtitle with the list of securities
        subtitle_text = 'List of securities: ' + ', '.join(self.list_securities)
        # ploting data
        data = [
            go.Scatter(
                x=array_sigmas.flatten(),
                y=array_mus.flatten(),
                mode='markers',
                marker=dict(
                    color=array_sharpes.flatten(),
                    colorscale='Viridis',
                    showscale=True,
                    cmax=array_sharpes.flatten().max(),
                    cmin=0,
                    colorbar=dict(
                        title='Sharpe Ratios'
                    )
                ),
                #   define the hovertemplate to include weights
                hovertemplate=(
                    'Risk: %{x:.2f}<br>' +
                    'Return: %{y:.2f}<br>' +
                    'Sharpe: %{marker.color:.2f}<br>' +
                    'Weight: %{customdata[0]}<extra></extra>'
                ),
                #   weights data for hovertemplate
                customdata=customdata_portfolios,
                name='Portfolios'
            ),
            go.Scatter(
                x=array_eff_risks,
                y=array_eff_returns,
                mode='lines+markers', 
                line=dict(color='red', width=2),
                name='Efficient Frontier',
                hovertemplate=(
                    'Risk: %{x:.2f}<br>' +
                    'Return: %{y:.2f}<br>' +
                    'Weight: %{customdata}<extra></extra>'
                ),
                customdata=array_eff_weights
            ),
        ]
        # configuring title data
        dict_title = {
            'text': title_text,
            'xanchor': 'center',
            'yanchor': 'top',
            'y': 0.95,
            'x': 0.5
        }
        # legend
        dict_leg = {
            'orientation': 'h', 
            'yanchor': 'bottom',
            'y': -0.2,
            'xanchor': 'center', 
            'x': 0.5
        }
        # launching figure with plotly
        fig = go.Figure(data=data)
        # update layout
        fig.update_layout(
            title=dict_title,
            annotations=[
                dict(
                    text=subtitle_text,
                    x=0.53,
                    y=1.08,
                    xref='paper',
                    yref='paper',
                    showarrow=False,
                    font=dict(
                        size=12,
                        color='gray'
                    ),
                    align='center'
                )
            ],
            xaxis_rangeslider_visible=False, width=1280, height=720,
            xaxis_showgrid=True, xaxis_gridwidth=1, xaxis_gridcolor='#E8E8E8',
            yaxis_showgrid=True, yaxis_gridwidth=1, yaxis_gridcolor='#E8E8E8',
            yaxis_title=yaxis_title, xaxis_title=xaxis_title,
            legend=dict_leg,
            plot_bgcolor='rgba(0,0,0,0)',
        )
        # save plot, if is user's interest
        if complete_path_save_fig is not None:
            fig_extension = complete_path_save_fig.split('.')[-1]
            fig.write_image(
                complete_path_save_fig, 
                format=fig_extension, 
                scale=2, 
                width=1280, 
                height=720
            )
        # display plot
        if bl_show_plot == True:
            fig.show()

    def max_sharpe(self, array_sharpes:np.ndarray, array_weights:np.ndarray, array_sigmas:np.ndarray, 
                   array_mus:np.ndarray, array_close:np.ndarray, bl_non_zero_w:bool=False, 
                   int_round_close:int=2) -> dict:
        '''
        DOCSTRING: MAXIMUM SHARPE RATIO PORTFOLIO
        INPUTS:
            - array_sharpes: np.ndarray -> ARRAY CONTAINING SHARPE RATIOS FOR THE PORTFOLIOS
            - array_weights: np.ndarray -> ARRAY CONTAINING ASSET WEIGHTS IN THE PORTFOLIOS
            - array_sigmas: np.ndarray -> ARRAY CONTAINING RISKS (STANDARD DEVIATIONS) OF THE PORTFOLIOS
            - array_mus: np.ndarray -> ARRAY CONTAINING EXPECTED RETURNS OF THE PORTFOLIOS
            - ensure_nonzero_weights: bool -> ENSURE THAT ALL WEIGHTS ARE NON-ZERO
        OUTPUTS:
            - dict -> DICTIONARY CONTAINING INFORMATION ABOUT THE MAXIMUM SHARPE RATIO PORTFOLIO
        '''
        # ensuring that all weights are non-zero, if is user's interest
        if bl_non_zero_w:
            array_valid_indices = np.where((array_weights != 0).all(axis=1))[0]
            if len(array_valid_indices) == 0:
                raise ValueError('No available portfolios with non-zero weights')
            int_argmax_sharpe = array_valid_indices[array_sharpes[array_valid_indices].argmax()]
        else:
            int_argmax_sharpe = array_sharpes.argmax()
        # maximum sharpe ratio portfolio
        array_eff_w = array_weights[array_sharpes.argmax()]
        array_eff_risk = array_sigmas[array_sharpes.argmax()]
        array_eff_mu = array_mus[array_sharpes.argmax()]
        array_eff_sharpe = array_sharpes[array_sharpes.argmax()]
        # efficient quantities
        array_eff_quantities = [
            round(float(w) * self.float_prtf_notional / array_close[i]) 
            for i, w in enumerate(array_eff_w.split())
        ]
        # calculating notional (ensure array_eff_quantities is properly calculated as float)
        array_close = np.round(array_close, int_round_close)
        array_notional = np.array(array_close) * np.array(array_eff_quantities)
        return {
            'list_securities': self.list_securities,
            'argmin_risk': int_argmax_sharpe,
            'eff_weights': array_eff_w,
            'eff_risk': array_eff_risk,
            'eff_mu': array_eff_mu,
            'eff_sharpe': array_eff_sharpe,
            'eff_quantities': array_eff_quantities,
            'close': array_close,
            'notional': array_notional
        }

    def min_sigma(self, array_sharpes:np.ndarray, array_weights:np.ndarray, array_sigmas:np.ndarray, 
             array_mus:np.ndarray, array_close:np.ndarray, bl_non_zero_w:bool=False, 
             int_round_close:int=2) -> dict:
        '''
        DOCSTRING: MINIMUM RISK PORTFOLIO
        INPUTS:
            - array_sharpes: np.ndarray -> ARRAY CONTAINING SHARPE RATIOS FOR THE PORTFOLIOS
            - array_weights: np.ndarray -> ARRAY CONTAINING ASSET WEIGHTS IN THE PORTFOLIOS
            - array_sigmas: np.ndarray -> ARRAY CONTAINING RISKS (STANDARD DEVIATIONS) OF THE PORTFOLIOS
            - array_mus: np.ndarray -> ARRAY CONTAINING EXPECTED RETURNS OF THE PORTFOLIOS
            - bl_non_zero_w: bool -> ENSURE THAT ALL WEIGHTS ARE NON-ZERO
        OUTPUTS:
            - dict -> DICTIONARY CONTAINING INFORMATION ABOUT THE MINIMUM RISK PORTFOLIO
        '''
        # ensuring that all weights are non-zero, if is user's interest
        if bl_non_zero_w:
            array_valid_indices = np.where((array_weights != 0).all(axis=1))[0]
            if len(array_valid_indices) == 0:
                raise ValueError('No available portfolios with non-zero weights')
            int_argmin_risk = array_valid_indices[array_sigmas[array_valid_indices].argmin()]
        else:
            int_argmin_risk = array_sigmas.argmin()
        # minimum risk portfolio
        array_eff_w = array_weights[int_argmin_risk]
        array_eff_risk = array_sigmas[int_argmin_risk]
        array_eff_mu = array_mus[int_argmin_risk]
        array_eff_sharpe = array_sharpes[int_argmin_risk]
        # efficient quantities
        array_eff_quantities = [
            round(float(w) * self.float_prtf_notional / array_close[i]) 
            for i, w in enumerate(array_eff_w.split())
        ]
        # calculating notional (ensure array_eff_quantities is properly calculated as float)
        array_close = np.round(array_close, int_round_close)
        array_notional = np.array(array_close) * np.array(array_eff_quantities)
        return {
            'list_securities': self.list_securities,
            'argmin_risk': int_argmin_risk,
            'eff_weights': array_eff_w,
            'eff_risk': array_eff_risk,
            'eff_mu': array_eff_mu,
            'eff_sharpe': array_eff_sharpe,
            'eff_quantities': array_eff_quantities,
            'close': array_close,
            'notional': array_notional
        }