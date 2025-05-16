import math
import numpy as np
import pandas as pd
import cvxopt as opt
import plotly.graph_objs as go
from itertools import combinations
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Any
from stpstone.utils.parsers.lists import ListHandler
from stpstone.finance.b3.search_by_trading import TradingFilesB3


class MarkowitzEff:
    """
    REFERENCES: https://www.linkedin.com/pulse/python-aplicado-markowitz-e-teoria-nem-tão-moderna-de-paulo-rodrigues/?originalSubdomain=pt
    DOCSTRING: MARKOWITZ RISK-RETURN PLOT OF RANDOM PORTFOLIOS, AIMING TO FIND THE BEST ALLOCATION
        WITH THE ASSETS PROVIDED
    INPUTS: -
    OUTPUTS: -
    """

    def __init__(
            self, df_mktdata:pd.DataFrame, int_n_portfolios:int, float_prtf_notional:float,
            float_rf:float, col_ticker:str='ticker', col_close:str='close', col_dt:datetime='dt_date',
            col_returns:str='daily_return', col_last_close:str='last_close', col_min_w:str='min_w',
            col_max_date:str='max_date', bl_constraints:bool=True, bl_opt_possb_comb:bool=False,
            bl_progress_printing_opt:bool=False, nth_try:str=100, n_attempts_opt_prf:int=10000,
            int_wdy:int=252, int_round_close:int=2, path_fig:Optional[str]=None,
            bl_debug_mode:bool=True, bl_show_plot:bool=True, bl_non_zero_w_eff:bool=False,
            title_text:str='Markowitz Risk x Return Portfolios',
            yaxis_title:str='Return (%)', xaxis_title:str='Risk (%)'
        ) -> None:
        # attributes
        self.float_prtf_notional = float_prtf_notional
        self.df_mktdata = df_mktdata
        self.path_fig = path_fig
        self.int_n_portfolios = int_n_portfolios
        self.n_attempts_opt_prf = n_attempts_opt_prf
        self.col_ticker = col_ticker
        self.col_close = col_close
        self.col_dt = col_dt
        self.col_returns = col_returns
        self.col_min_w = col_min_w
        self.col_last_close = col_last_close
        self.col_max_date = col_max_date
        self.float_rf = float_rf
        self.bl_constraints = bl_constraints
        self.bl_opt_possb_comb = bl_opt_possb_comb
        self.bl_progress_printing_opt = bl_progress_printing_opt
        self.nth_try = nth_try
        self.int_wdy = int_wdy
        self.int_round_close = int_round_close
        self.bl_debug_mode = bl_debug_mode
        self.bl_show_plot = bl_show_plot
        self.bl_non_zero_w_eff = bl_non_zero_w_eff
        self.title_text = title_text
        self.yaxis_title = yaxis_title
        self.xaxis_title = xaxis_title
        # securities
        self.list_securities = df_mktdata[self.col_ticker].unique()
        # last date per asset
        self.df_mktdata[self.col_max_date] = self.df_mktdata.groupby([
            self.col_ticker])[self.col_dt].transform('max')
        # minimum w per asset
        if self.col_min_w not in self.df_mktdata.columns:
            self.df_mktdata[self.col_min_w] = self.df_mktdata.groupby([
                self.col_ticker,
                self.col_max_date
            ])[col_close].transform('last') / self.float_prtf_notional
        # last closing prices
        self.df_mktdata[self.col_last_close] = self.df_mktdata.groupby([
            self.col_ticker,
            self.col_max_date
        ])[col_close].transform('last')
        self.array_close = np.array([
            self.df_mktdata[self.df_mktdata[self.col_ticker] == ticker.replace('.SA', '')][
                self.col_last_close].unique()[0]
            for ticker in self.list_securities
        ])
        # random portfolios
        self.array_mus, self.array_sigmas, self.array_sharpes, self.array_weights, \
            self.array_returns, self.list_uuids = \
                self.random_portfolios(
                    self.df_mktdata, int_n_portfolios, self.col_ticker, self.col_close,
                    self.col_dt, self.col_returns, self.float_prtf_notional, self.col_min_w,
                    self.float_rf, self.bl_constraints, self.bl_opt_possb_comb, self.nth_try,
                    self.int_wdy
                )
        # optimal portfolios
        self.array_eff_weights, self.array_eff_returns, self.array_eff_risks = \
            self.optimal_portfolios(
                self.array_returns, self.n_attempts_opt_prf, self.bl_progress_printing_opt,
                self.int_wdy
            )
        # efficient frontier
        self.df_eff, self.df_porf = self.eff_frontier(
            self.array_eff_risks, self.array_eff_returns, self.array_weights, self.array_mus,
            self.array_sigmas, self.float_rf
        )

    def sharpe_ratio(self, float_mu:float, float_sigma:float, float_rf:float) -> float:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return (float(float_mu) - float(float_rf)) / float(float_sigma)

    def sigma_portfolio(self, array_weights:np.array, array_returns:np.array) -> float:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # covariance between stocks
        array_cov = np.cov(array_returns)
        # returning portfolio standard deviation
        return np.sqrt(np.dot(array_weights.T, np.dot(array_cov, array_weights)))

    def returns_min_w_uids(self, df_assets:pd.DataFrame, col_dt:str, col_id:str,
                           col_returns:str, col_min_w:str) \
                            -> Tuple[np.ndarray, np.ndarray, List[str]]:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
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
        list_uids = ListHandler().remove_duplicates(df_assets[col_id].to_list())
        # return arrays of interet
        return array_returns, array_min_w, list_uids

    def random_weights(self, int_n_assets:int, bl_constraints:bool=False, bl_opt_possb_comb:bool=False,
                       array_min_w:np.array=None, nth_try:int=100, int_idx_val:int=2,
                       bl_valid_weights:bool=False, i_attempts:int=0,
                       float_atol_sum:float=1e-4, float_atol_w:float=10000.0) -> np.array:
        """
        DOCSTRING: RANDOM WEIGHTS - WITH OR WITHOUT CONSTRAINTS
        INPUTS:
            - INT_N_ASSETS: THE NUMBER OF ASSETS IN THE PORTFOLIO
            - BL_CONSTRAINTS: BOOLEAN FLAG TO APPLY CONSTRAINTS OR NOT
            - MIN_INVEST_PER_ASSET: A LIST OF MINIMUM WEIGHTS/INVESTMENTS FOR EACH ASSET
        OUTPUTS:
            - A LIST OF WEIGHTS FOR THE ASSETS THAT SATISFY THE GIVEN CONSTRAINTS,
                WHERE SUM OF WEIGHTS = 1
        """
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
                if (i_attempts >= nth_try)\
                    or (all([sum(comb) >= 1.0 for comb in list_combs])):
                    #   return a weight array with one asset having weight 1.0 and others 0.0
                    array_w = np.zeros(int_n_assets)
                    int_idx = np.random.randint(0, int_n_assets)
                    array_w[int_idx] = 1.0
                    return array_w
                #   if multiplier is enabled, build a list of possible indexes combinations in
                #       order to sum 1.0 or less
                if bl_opt_possb_comb == True:
                    #   combinations where sum is less than 1.0 - flatten list
                    list_i = ListHandler().remove_duplicates([
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
                    if bl_opt_possb_comb == True:
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
                if (bl_opt_possb_comb == False) or (np.count_nonzero(array_w) == 1):
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
                        bl_opt_possb_comb:bool=False, array_min_w:Optional[np.ndarray]=None,
                        nth_try:int=100, int_wdy:int=252) \
                            -> Tuple[np.ndarray, np.ndarray, np.ndarray, str]:
        """
        DOCSTRING: RETURNS THE MEAN AND STANDARD DEVIATION OF RETURNS FROM A RANDOM PORTFOLIO
        INPUTS: MATRIX ASSETS RETURNS, ARRAY EXPECTED RETURNS, FLOAT RISK FREE
        OUTPUTS: TUP OF FLOATS
        """
        # adjusting variables' types
        array_r = np.asmatrix(array_returns)
        float_rf = float(float_rf)
        # random wieghts for the current portfolio
        array_weights = self.random_weights(array_r.shape[0], bl_constraints, bl_opt_possb_comb,
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
                          col_close:str, col_dt:datetime, col_returns:str,
                          float_prtf_notional:float, col_min_w:str='min_w', float_rf:float=0.0,
                          bl_constraints:bool=False, bl_opt_possb_comb:bool=False,
                          nth_try:int=100, int_wdy:int=252) \
                            -> Tuple[np.ndarray, np.ndarray, np.ndarray,
                                     np.ndarray, np.ndarray, List[str]]:
        """
        DOCSTRING: RETURNS THE MEAN AND STANDARD DEVIATION OF RETURNS FROM A RANDOM PORTFOLIO
        INPUTS: MATRIX ASSETS RETURNS, ARRAY EXPECTED RETURNS, FLOAT RISK FREE
        OUTPUTS: TUP OF FLOATS
        """
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
                    bl_opt_possb_comb,
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
        """
        DOCSTRING: WEIGHTS RETURNS AND SIGMA FOR EFFICIENT FRONTIER
        INPUTS: MATRIX OF ASSETS' RETURNS
        OUTPUTS: TUP OF ARRAYS
        """
        # turn on/off progress printing
        opt.solvers.options['show_progress'] = bl_progress_printing_opt
        # configuring data types
        array_returns = np.asmatrix(array_returns)
        # definig the number of portfolios to be created
        n = array_returns.shape[0]
        # calculating first attempt for float_mu in each portfolio
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
        list_portfolios = [opt.solvers.qp(float_mu * S, -pbar, G, h, A, b)['x']
                           for float_mu in mus]
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
                     float_rf:float, col_sigma:str='sigma', col_mu:str='float_mu',
                     col_w:str='weights', col_sharpe:str='sharpe',
                     atol:float=1e-2, int_pace_atol:int=5) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # setting variables
        array_eff_weights = list()
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

    @property
    def plot_risk_return_portfolio(self) -> None:
        """
        REFERENCES: https://plotly.com/python/reference/layout/,
            https://plotly.com/python-api-reference/generated/plotly.graph_objects.Scatter.html,
            https://plotly.com/python/builtin-colorscales/
        DOCSTRING: PLOT MARKOWITZ'S EFFICIENT FRONTIER FOR PORTFOLIO MANAGEMENT
        INPUTS: ARRAY WEIGHTS, ARRAY MUS (MEAN RETURNS FOR EACH GIVEN PORTFOLIO, BASED ON EXPCTED
            RETURNS FOR EACH SECURITY, GIVEN ITS WEIGHT ON THE SYNTHETIC PORTFOLIO), ARRAY OF SHARPES,
            ARRAY OF EFFECTIVE RISKS, ARRAY OF EFFECTIVE RETURN FOR ALL SECURITIES IN A PORTFOLIO,
            TITLE, YAXIS NAME AND XAXIS NAME
        OUTPUTS: PLOT
        """
        # maximum sharpe portfolio
        idx_max_sharpe = self.array_sharpes.argmax()
        max_sharpe_sigma = self.array_sigmas[idx_max_sharpe]
        max_sharpe_mu = self.array_mus[idx_max_sharpe]
        # minimum sigma portfolio
        idx_min_sigma = self.array_sigmas.argmin()
        min_sigma_mu = self.array_mus[idx_min_sigma]
        min_sigma_sigma = self.array_sigmas[idx_min_sigma]
        # maximum sharpe portfolio
        if self.bl_debug_mode == True:
            print('### MAXIMUM SHARPE PORTFOLIO ###')
            print('SHARPES ARGMAX: {}'.format(self.array_sharpes.argmax()))
            print('WEIGHTS: {}'.format(self.array_weights[self.array_sharpes.argmax()]))
            print('RISK: {}'.format(self.array_sigmas[self.array_sharpes.argmax()]))
            print('RETURN: {}'.format(self.array_mus[self.array_sharpes.argmax()]))
            print('SHARPE: {}'.format(self.array_sharpes[self.array_sharpes.argmax()]))
        # prepare customdata for scatter plot
        customdata_portfolios = np.array([
            [weights, ', '.join(self.list_securities)] for weights in self.array_weights
        ], dtype=object)
        # Ppepare the subtitle with the list of securities
        subtitle_text = 'List of securities: ' + ', '.join(self.list_securities)
        # ploting data
        data = [
            go.Scatter(
                x=self.array_sigmas.flatten(),
                y=self.array_mus.flatten(),
                mode='markers',
                marker=dict(
                    color=self.array_sharpes.flatten(),
                    colorscale='Viridis',
                    showscale=True,
                    cmax=self.array_sharpes.flatten().max(),
                    cmin=0,
                    colorbar=dict(
                        title='Sharpe Ratios'
                    )
                ),
                #   define the hovertemplate to include weights
                hovertemplate=(
                    'Risk: %{x:.2f}<br>' +
                    'Returns: %{y:.2f}<br>' +
                    'Sharpe: %{marker.color:.2f}<br>' +
                    'Weight: %{customdata[0]}<extra></extra>'
                ),
                #   weights data for hovertemplate
                customdata=customdata_portfolios,
                name='Portfolios'
            ),
            go.Scatter(
                x=self.array_eff_risks,
                y=self.array_eff_returns,
                mode='lines+markers',
                line=dict(color='red', width=2),
                name='Efficient Frontier',
                hovertemplate=(
                    'Risk: %{x:.2f}<br>' +
                    'Returns: %{y:.2f}<br>' +
                    'Weight: %{customdata}<extra></extra>'
                ),
                customdata=self.array_eff_weights
            ),
            # add a green star for the minimum sigma portfolio
            go.Scatter(
                x=[min_sigma_sigma],
                y=[min_sigma_mu],
                mode='markers',
                marker=dict(size=30, color='green', symbol='star'),
                name='Min Risk Portfolio',
                hovertemplate='Risk: %{x:.2f}<br>Returns: %{y:.2f}<extra></extra>'
            ),
            # add a red star for the maximum sharpe portfolio
            go.Scatter(
                x=[max_sharpe_sigma],
                y=[max_sharpe_mu],
                mode='markers',
                marker=dict(size=30, color='blue', symbol='star'),
                name='Max Sharpe Portfolio',
                hovertemplate='Risk: %{x:.2f}<br>Returns: %{y:.2f}<extra></extra>'
            )
        ]
        # configuring title data
        dict_title = {
            'text': self.title_text,
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
            yaxis_title=self.yaxis_title, xaxis_title=self.xaxis_title,
            legend=dict_leg,
            plot_bgcolor='rgba(0,0,0,0)',
        )
        # save plot, if is user's interest
        if self.path_fig is not None:
            fig_extension = self.path_fig.split('.')[-1]
            fig.write_image(
                self.path_fig,
                format=fig_extension,
                scale=2,
                width=1280,
                height=720
            )
        # display plot
        if self.bl_show_plot == True:
            fig.show()

    @property
    def max_sharpe(self) -> dict:
        """
        DOCSTRING: MAXIMUM SHARPE RATIO PORTFOLIO
        INPUTS:
            - array_sharpes: np.ndarray -> ARRAY CONTAINING SHARPE RATIOS FOR THE PORTFOLIOS
            - array_weights: np.ndarray -> ARRAY CONTAINING ASSET WEIGHTS IN THE PORTFOLIOS
            - array_sigmas: np.ndarray -> ARRAY CONTAINING RISKS (STANDARD DEVIATIONS) OF THE PORTFOLIOS
            - array_mus: np.ndarray -> ARRAY CONTAINING EXPECTED RETURNS OF THE PORTFOLIOS
            - ensure_nonzero_weights: bool -> ENSURE THAT ALL WEIGHTS ARE NON-ZERO
        OUTPUTS:
            - dict -> DICTIONARY CONTAINING INFORMATION ABOUT THE MAXIMUM SHARPE RATIO PORTFOLIO
        """
        # ensuring that all weights are non-zero, if is user's interest
        if self.bl_non_zero_w_eff:
            array_valid_indices = np.where((self.array_weights != 0).all(axis=1))[0]
            if len(array_valid_indices) == 0:
                raise ValueError('No available portfolios with non-zero weights')
            int_argmax_sharpe = array_valid_indices[self.array_sharpes[array_valid_indices].argmax()]
        else:
            int_argmax_sharpe = self.array_sharpes.argmax()
        # maximum sharpe ratio portfolio
        array_eff_w = self.array_weights[self.array_sharpes.argmax()]
        array_eff_mu = self.array_mus[self.array_sharpes.argmax()]
        array_eff_sharpe = self.array_sharpes[self.array_sharpes.argmax()]
        # efficient quantities
        array_eff_quantities = [
            round(float(w) * self.float_prtf_notional / self.array_close[i])
            for i, w in enumerate(array_eff_w.split())
        ]
        # calculating notional (ensure array_eff_quantities is properly calculated as float)
        self.array_close = np.round(self.array_close, self.int_round_close)
        array_notional = np.array(self.array_close) * np.array(array_eff_quantities)
        return {
            'tickers': self.list_securities,
            'argmax_sharpe': int_argmax_sharpe,
            'eff_weights': array_eff_w,
            'eff_mu': array_eff_mu,
            'eff_sharpe': array_eff_sharpe,
            'eff_quantities': array_eff_quantities,
            'close': self.array_close,
            'notional': array_notional,
            'notional_total': array_notional.sum()
        }

    @property
    def min_sigma(self) -> dict:
        """
        DOCSTRING: MINIMUM RISK PORTFOLIO
        INPUTS:
            - array_sharpes: np.ndarray -> ARRAY CONTAINING SHARPE RATIOS FOR THE PORTFOLIOS
            - array_weights: np.ndarray -> ARRAY CONTAINING ASSET WEIGHTS IN THE PORTFOLIOS
            - array_sigmas: np.ndarray -> ARRAY CONTAINING RISKS (STANDARD DEVIATIONS) OF THE PORTFOLIOS
            - array_mus: np.ndarray -> ARRAY CONTAINING EXPECTED RETURNS OF THE PORTFOLIOS
            - bl_non_zero_w_eff: bool -> ENSURE THAT ALL WEIGHTS ARE NON-ZERO
        OUTPUTS:
            - dict -> DICTIONARY CONTAINING INFORMATION ABOUT THE MINIMUM RISK PORTFOLIO
        """

        # ensuring that all weights are non-zero, if is user's interest
        if self.bl_non_zero_w_eff:
            array_valid_indices = np.where((self.array_weights != 0).all(axis=1))[0]
            if len(array_valid_indices) == 0:
                raise ValueError('No available portfolios with non-zero weights')
            int_argmin_risk = array_valid_indices[self.array_sigmas[array_valid_indices].argmin()]
        else:
            int_argmin_risk = self.array_sigmas.argmin()
        # minimum risk portfolio
        array_eff_w = self.array_weights[int_argmin_risk]
        array_eff_risk = self.array_sigmas[int_argmin_risk]
        array_eff_mu = self.array_mus[int_argmin_risk]
        # efficient quantities
        array_eff_quantities = [
            round(float(w) * self.float_prtf_notional / self.array_close[i])
            for i, w in enumerate(array_eff_w.split())
        ]
        # calculating notional (ensure array_eff_quantities is properly calculated as float)
        self.array_close = np.round(self.array_close, self.int_round_close)
        array_notional = np.array(self.array_close) * np.array(array_eff_quantities)
        return {
            'tickers': self.list_securities,
            'argmin_risk': int_argmin_risk,
            'eff_weights': array_eff_w,
            'eff_risk': array_eff_risk,
            'eff_mu': array_eff_mu,
            'eff_quantities': array_eff_quantities,
            'close': self.array_close,
            'notional': array_notional,
            'notional_total': array_notional.sum()
        }

    def lot_shares_ticker_corr(self, dict_allocation:Dict[str, Any]) \
        -> Dict[str, Any]:
        """
        DOCSTRING: LOT OF SHARES TICKER CORRECTION
        INPUTS: DICT ALLOCATION
        OUTPUTS: DICT
        """
        list_ser = list()
        df_trad_sec = TradingFilesB3().tradable_securities
        for ticker in dict_allocation['tickers']:
            float_qty_rnd = df_trad_sec[df_trad_sec['Symbol'] == ticker.replace('.SA', '')][
                'MinOrderQty'].values[0]
            float_qty_quotient = float(dict_allocation['eff_quantities'][
                self.list_securities.index(ticker)]) // float_qty_rnd
            float_qty_remainder = float(dict_allocation['eff_quantities'][
                self.list_securities.index(ticker)]) % float_qty_rnd
            list_ser.append({'ticker': ticker.replace('.SA', ''), 'qty': float_qty_quotient})
            list_ser.append({'ticker': ticker.replace('.SA', '') + 'F', 'qty': float_qty_remainder})
        df_ = pd.DataFrame(list_ser)
        df_ = df_[df_['qty'] != 0]
        df_['close'] = list(dict_allocation['close'])
        df_['notional'] = df_['qty'] * df_['close']
        return df_
