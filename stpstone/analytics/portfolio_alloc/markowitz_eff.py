"""Markowitz Efficient Frontier Class."""

from itertools import combinations
from typing import Optional, TypedDict

import cvxopt as opt
import numpy as np
from numpy.typing import NDArray
import pandas as pd
import plotly.graph_objs as go

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.lists import ListHandler


class ResultMaxSharpe(TypedDict):
	"""TypedDict for ResultMaxSharpe."""

	tickers: list[str]
	argmax_sharpe: int
	eff_weights: NDArray[np.float64]
	eff_mu: NDArray[np.float64]
	eff_sharpe: NDArray[np.float64]
	eff_quantities: NDArray[np.float64]
	close: NDArray[np.float64]
	notional: NDArray[np.float64]
	notional_total: float


class MarkowitzEff(metaclass=TypeChecker):
	"""Markowitz Efficient Frontier Class.

	References
	----------
	- https://www.linkedin.com/pulse/python-aplicado-markowitz-e-teoria-nem-tão-moderna-de-paulo-rodrigues/?originalSubdomain=pt
	"""

	def __init__(
		self,
		df_mktdata: pd.DataFrame,
		int_n_portfolios: int,
		float_prtf_notional: float,
		float_rf: float,
		col_ticker: str = "ticker",
		col_close: str = "close",
		col_dt: str = "dt_date",
		col_returns: str = "daily_return",
		col_last_close: str = "last_close",
		col_min_w: str = "min_w",
		col_max_date: str = "max_date",
		bool_constraints: bool = True,
		bool_opt_possb_comb: bool = False,
		bool_progress_printing_opt: bool = False,
		nth_try: int = 100,
		n_attempts_opt_prf: int = 10000,
		int_wdy: int = 252,
		int_round_close: int = 2,
		path_fig: Optional[str] = None,
		bool_debug_mode: bool = True,
		bool_show_plot: bool = True,
		bool_non_zero_w_eff: bool = False,
		title_text: str = "Markowitz Risk x Return Portfolios",
		yaxis_title: str = "Return (%)",
		xaxis_title: str = "Risk (%)",
	) -> None:
		"""Initialize the MarkowitzEff class with market data and portfolio parameters.

		Parameters
		----------
		df_mktdata : pd.DataFrame
			DataFrame containing market data.
		int_n_portfolios : int
			Number of portfolios to generate.
		float_prtf_notional : float
			Notional value of the portfolio.
		float_rf : float
			Risk-free rate.
		col_ticker : str
			Column name for ticker symbols, by default 'ticker'.
		col_close : str
			Column name for closing prices, by default 'close'.
		col_dt : str
			Column name for date, by default 'dt_date'.
		col_returns : str
			Column name for returns, by default 'daily_return'.
		col_last_close : str
			Column name for last closing price, by default 'last_close'.
		col_min_w : str
			Column name for minimum weight, by default 'min_w'.
		col_max_date : str
			Column name for maximum date, by default 'max_date'.
		bool_constraints : bool
			Flag to include constraints, by default True.
		bool_opt_possb_comb : bool
			Flag to optimize possible combinations, by default False.
		bool_progress_printing_opt : bool
			Flag to print optimization progress, by default False.
		nth_try : int
			Number of attempts for optimization, by default 100.
		n_attempts_opt_prf : int
			Number of attempts to optimize portfolio, by default 10000.
		int_wdy : int
			Number of days in a year, by default 252.
		int_round_close : int
			Number of decimal places for rounding, by default 2.
		path_fig : Optional[str]
			Path to save figure, by default None.
		bool_debug_mode : bool
			Flag to enable debug mode, by default True.
		bool_show_plot : bool
			Flag to show plot, by default True.
		bool_non_zero_w_eff : bool
			Flag to include non-zero weights in efficient frontier, by default False.
		title_text : str
			Title text for plot, by default 'Markowitz Risk x Return Portfolios'.
		yaxis_title : str
			Y-axis title for plot, by default 'Return (%)'.
		xaxis_title : str
			X-axis title for plot, by default 'Risk (%)'.

		Returns
		-------
		None
		"""
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
		self.bool_constraints = bool_constraints
		self.bool_opt_possb_comb = bool_opt_possb_comb
		self.bool_progress_printing_opt = bool_progress_printing_opt
		self.nth_try = nth_try
		self.int_wdy = int_wdy
		self.int_round_close = int_round_close
		self.bool_debug_mode = bool_debug_mode
		self.bool_show_plot = bool_show_plot
		self.bool_non_zero_w_eff = bool_non_zero_w_eff
		self.title_text = title_text
		self.yaxis_title = yaxis_title
		self.xaxis_title = xaxis_title
		# securities
		self.list_securities = df_mktdata[self.col_ticker].unique()
		# last date per asset
		self.df_mktdata[self.col_max_date] = self.df_mktdata.groupby([self.col_ticker])[
			self.col_dt
		].transform("max")
		# minimum w per asset
		if self.col_min_w not in self.df_mktdata.columns:
			self.df_mktdata[self.col_min_w] = (
				self.df_mktdata.groupby([self.col_ticker, self.col_max_date])[col_close].transform(
					"last"
				)
				/ self.float_prtf_notional
			)
		# last closing prices
		self.df_mktdata[self.col_last_close] = self.df_mktdata.groupby(
			[self.col_ticker, self.col_max_date]
		)[col_close].transform("last")
		self.array_close = np.array(
			[
				self.df_mktdata[self.df_mktdata[self.col_ticker] == ticker.replace(".SA", "")][
					self.col_last_close
				].unique()[0]
				for ticker in self.list_securities
			],
			dtype=np.float64,
		)
		# random portfolios
		(
			self.array_mus,
			self.array_sigmas,
			self.array_sharpes,
			self.array_weights,
			self.array_returns,
			self.list_uuids,
		) = self.random_portfolios(
			self.df_mktdata,
			int_n_portfolios,
			self.col_ticker,
			self.col_dt,
			self.col_returns,
			self.col_min_w,
			self.float_rf,
			self.bool_constraints,
			self.bool_opt_possb_comb,
			self.nth_try,
			self.int_wdy,
		)
		# optimal portfolios
		self.array_eff_weights, self.array_eff_returns, self.array_eff_risks = (
			self.optimal_portfolios(
				self.array_returns,
				self.n_attempts_opt_prf,
				self.bool_progress_printing_opt,
				self.int_wdy,
			)
		)
		# efficient frontier
		self.df_eff, self.df_porf = self.eff_frontier(
			self.array_eff_risks,
			self.array_eff_returns,
			self.array_weights,
			self.array_mus,
			self.array_sigmas,
			self.float_rf,
		)
		# validate inputs
		self._validate_inputs()

	def _validate_inputs(self) -> None:
		"""Validate inputs.

		Raises
		------
		ValueError
			If number of portfolios is less than or equal to 0
			If number of portfolilos is not an integer
			If risk free rate is less than zero
			If working days per year is less than or equal to zero
			If portfolio notional is less than or equal to zero
		"""
		if self.int_n_portfolios <= 0:
			raise ValueError("Number of portfolios must be greater than zero")
		if self.int_n_portfolios % 1 != 0:
			raise ValueError("Number of portfolios must be an integer")
		if self.float_rf < 0:
			raise ValueError("Risk free rate must be greater than or equal to zero")
		if self.int_wdy <= 0:
			raise ValueError("Working days per year must be greater than zero")
		if self.float_prtf_notional <= 0:
			raise ValueError("Portfolio notional must be positive")

	def sharpe_ratio(self, float_mu: float, float_sigma: float, float_rf: float) -> float:
		"""Calculate the Sharpe Ratio.

		Parameters
		----------
		float_mu : float
			Expected return.
		float_sigma : float
			Standard deviation.
		float_rf : float
			Risk free rate.

		Returns
		-------
		float
			Sharpe ratio.

		Raises
		------
		ValueError
			If standard deviation is less than or equal to zero
		"""
		if float_sigma <= 0:
			raise ValueError("Standard deviation must be positive")
		return (float(float_mu) - float(float_rf)) / float(float_sigma)

	def sigma_portfolio(
		self, array_weights: NDArray[np.float64], array_returns: NDArray[np.float64]
	) -> float:
		"""Calculate the standard deviation of a portfolio.

		Parameters
		----------
		array_weights : NDArray[np.float64]
			Array of weights.
		array_returns : NDArray[np.float64]
			Array of returns.

		Returns
		-------
		float
			Portfolio standard deviation.
		"""
		# covariance between stocks
		array_cov = np.cov(array_returns)
		# returning portfolio standard deviation  # noqa: ERA001
		return np.sqrt(np.dot(array_weights.T, np.dot(array_cov, array_weights)))

	def returns_min_w_uids(
		self, df_assets: pd.DataFrame, col_dt: str, col_id: str, col_returns: str, col_min_w: str
	) -> tuple[NDArray[np.float64], NDArray[np.float64], list[str]]:
		"""Minimum weights per uids.

		Parameters
		----------
		df_assets : pd.DataFrame
			Assets dataframe.
		col_dt : str
			Date column.
		col_id : str
			Uid column.
		col_returns : str
			Returns column.
		col_min_w : str
			Minimum weight column.

		Returns
		-------
		tuple[NDArray[np.float64], NDArray[np.float64], list[str]]
			Returns, minimum weights and uids.
		"""
		# filter where returns are not nulls  # noqa: ERA001
		df_assets = df_assets[~df_assets[col_returns].isna()]

		# returns per uids  # noqa: ERA001
		array_returns = (
			df_assets.pivot_table(index=col_dt, columns=col_id, values=col_returns).to_numpy().T
		)
		array_returns = np.nan_to_num(array_returns, nan=0.0)

		# minimum weights per uids
		series_min_w = df_assets.groupby(col_id)[col_min_w].first()
		array_min_w = series_min_w.to_numpy(dtype=np.float64)

		# list of uids
		list_uids = ListHandler().remove_duplicates(df_assets[col_id].to_list())

		return array_returns, array_min_w, list_uids

	def random_weights(
		self,
		int_n_assets: int,
		bool_constraints: bool = False,
		bool_opt_possb_comb: bool = False,
		array_min_w: Optional[NDArray[np.float64]] = None,
		nth_try: int = 100,
		int_idx_val: int = 2,
		bool_valid_weights: bool = False,
		i_attempts: int = 0,
		float_atol_sum: float = 1e-4,
		float_atol_w: float = 10000.0,
	) -> NDArray[np.float64]:
		"""Generate random weights for a portfolio.

		Parameters
		----------
		int_n_assets : int
			Number of assets.
		bool_constraints : bool
			Enable constraints (default is False).
		bool_opt_possb_comb : bool
			Enable optimization of possible combinations (default is False).
		array_min_w : Optional[NDArray[np.float64]]
			Minimum weights per asset (default is None).
		nth_try : int
			Number of attempts (default is 100).
		int_idx_val : int
			Index value (default is 2).
		bool_valid_weights : bool
			Check if weights are valid (default is False).
		i_attempts : int
			Number of attempts (default is 0).
		float_atol_sum : float
			Tolerance for sum of weights (default is 1e-4).
		float_atol_w : float
			Tolerance for weights (default is 10000.0).

		Returns
		-------
		NDArray[np.float64]
			Random weights.

		Raises
		------
		ValueError
			If constraints are enabled and min invest per asset is not provided.
		"""
		if int_n_assets <= 0:
			raise ValueError("Number of assets must be positive.")
		# adjusting number of assets within the portfolio
		if array_min_w is not None:
			int_idx_val = min(len(array_min_w), int_idx_val)
		else:
			int_idx_val = int_idx_val
		# check whether the constraints are enabled
		if bool_constraints:
			#   sanity check for constraints
			if array_min_w is None:
				raise ValueError(
					"Min invest per asset must be provided as a list when "
					+ "constraints are enabled."
				)
			if any(isinstance(x, str) for x in array_min_w):
				raise ValueError("Min invest per asset must be a list of numbers.")
			if len(array_min_w) != int_n_assets:
				raise ValueError(
					"The length of min invest per asset must match the" + "number of assets."
				)
			if any(x < 0 for x in array_min_w):
				raise ValueError("Min invest per asset must be positive.")
			if any(x > 1 for x in array_min_w):
				raise ValueError("Min invest per asset must be below 1.0.")
			if any(x == 0 for x in array_min_w):
				raise ValueError("Every min invest per asset must be greater than 0.")
			#   initializing variables
			bool_valid_weights = False
			list_combs = [
				comb for r in range(2, int_idx_val + 1) for comb in combinations(array_min_w, r)
			]
			#   recursive call to get valid weights
			while not bool_valid_weights:
				#   increment the try counter
				i_attempts += 1
				#   resetting variables
				array_w = np.zeros(int_n_assets)
				#   check if it's the nth try or all the combinations are greater than one
				if (i_attempts >= nth_try) or (all([sum(comb) >= 1.0 for comb in list_combs])):
					#   return a weight array with one asset having weight 1.0 and others 0.0  # noqa: ERA001,E501
					array_w = np.zeros(int_n_assets)
					int_idx = np.random.randint(0, int_n_assets)
					array_w[int_idx] = 1.0
					return array_w
				#   if multiplier is enabled, build a list of possible indexes combinations in
				#       order to sum 1.0 or less
				if bool_opt_possb_comb:
					#   combinations where sum is less than 1.0 - flatten list
					list_i = ListHandler().remove_duplicates(
						[
							idx
							for comb in list_combs
							for x in comb
							for idx in np.where(array_min_w == x)[0]
							if sum(comb) <= 1.0
						]
					)
				else:
					list_i = list(range(int_n_assets))
				np.random.shuffle(list_i)
				#   looping through the indexes
				for i in list_i:
					#   randomly building a float weight
					float_upper_tol = max(float_atol_w * (1.0 - sum(array_w)), 1.0)
					#   building the float weight with any given value above the minimum or a
					#       multiple of the minimum
					if bool_opt_possb_comb:
						int_max_mult = max(int((1.0 - sum(array_w)) // array_min_w[i]), 1)
						int_rand_mult = np.random.randint(0, int_max_mult + 1)
						float_weight = float(int_rand_mult * array_min_w[i])
					else:
						float_upper_tol = max(float_atol_w * (1.0 - sum(array_w)), 1.0)
						random_integer = np.random.randint(0, float_upper_tol)
						float_weight = float(random_integer) / float_upper_tol
					#   check if the weight is greater than the minimum
					if float_weight < array_min_w[i]:
						array_w[i] = 0
					else:
						array_w[i] = float_weight
					#   check if the sum of weights is equal to 1.0 or greater
					if sum(array_w) >= 1.0:
						break
				#   normalize only if the total weight is non-zero, if multiplier is disabled
				if not bool_opt_possb_comb or np.count_nonzero(array_w) == 1:
					total_weight = np.sum(array_w)
					if total_weight > 0:
						array_w /= total_weight
				#   sanity checks for weights:  # noqa: ERA001
				#       1 - all weights must be non-negative
				#       2 - sum must be equal to 1
				#       3 - the minimum must be respected, or zero for a given asset
				#       4 - some weight must be positive
				bool_valid_weights = (
					np.all(array_w >= 0)
					and np.isclose(np.sum(array_w), 1, atol=float_atol_sum)
					and all(
						[
							(array_w[i] >= array_min_w[i]) or (array_w[i] == 0)
							for i in range(int_n_assets)
						]
					)
					and np.any(array_w > 0)
					and np.all(array_w != 1)
				)
			return array_w
		else:
			# if no constraints are applied, return standard random weights  # noqa: ERA001
			k = np.random.rand(int_n_assets)
			return k / sum(k)

	def random_portfolio(
		self,
		array_returns: NDArray[np.float64],
		float_rf: float,
		bool_constraints: bool = False,
		bool_opt_possb_comb: bool = False,
		array_min_w: Optional[NDArray[np.float64]] = None,
		nth_try: int = 100,
		int_wdy: int = 252,
	) -> tuple[NDArray[np.float64], NDArray[np.float64], NDArray[np.float64], str]:
		"""Generate a random portfolio with specified parameters.

		Parameters
		----------
		array_returns : NDArray[np.float64]
			Asset returns.
		float_rf : float
			Risk free rate.
		bool_constraints : bool
			Whether to apply constraints, by default False.
		bool_opt_possb_comb : bool
			Whether to optimize the possible combinations of weights, by default False.
		array_min_w : Optional[NDArray[np.float64]]
			Minimum weights, by default None.
		nth_try : int
			Number of attempts to find a valid portfolio, by default 100.
		int_wdy : int
			Working days per year, by default 252.

		Returns
		-------
		tuple[NDArray[np.float64], NDArray[np.float64], NDArray[np.float64], str]
			Returns, standard deviation, sharpes ratio and weights.
		"""
		# adjusting variables' types
		array_r = np.array(array_returns)
		float_rf = float(float_rf)
		# random weights for the current portfolio
		array_weights = self.random_weights(
			array_r.shape[0], bool_constraints, bool_opt_possb_comb, array_min_w, nth_try
		)
		# mean returns for assets  # noqa: ERA001
		array_returns = np.array(np.mean(array_r, axis=1))
		# portfolio standard deviation
		array_sigmas = self.sigma_portfolio(array_weights, array_r) * np.sqrt(int_wdy)
		# portfolio expected return  # noqa: ERA001
		array_mus = np.dot(array_weights, array_returns) * int_wdy
		# sharpes ratio
		array_sharpes = self.sharpe_ratio(array_mus, array_sigmas, float_rf)
		# changing type of array weights to transform into one value
		array_weights = " ".join([str(x) for x in array_weights])
		# returning portfolio infos  # noqa: ERA001
		return array_mus, array_sigmas, array_sharpes, array_weights

	def random_portfolios(
		self,
		df_assets: pd.DataFrame,
		int_n_portfolios: int,
		col_id: str,
		col_dt: str,
		col_returns: str,
		col_min_w: str = "min_w",
		float_rf: float = 0.0,
		bool_constraints: bool = False,
		bool_opt_possb_comb: bool = False,
		nth_try: int = 100,
		int_wdy: int = 252,
	) -> tuple[
		NDArray[np.float64],
		NDArray[np.float64],
		NDArray[np.float64],
		NDArray[np.float64],
		NDArray[np.float64],
		list[str],
	]:
		"""Generate random portfolios based on market data.

		Parameters
		----------
		df_assets : pd.DataFrame
			Market data.
		int_n_portfolios : int
			Number of portfolios to generate.
		col_id : str
			Asset identifier column.
		col_dt : str
			Date column.
		col_returns : str
			Returns column.
		col_min_w : str
			Minimum weights column, by default 'min_w'.
		float_rf : float
			Risk free rate, by default 0.0.
		bool_constraints : bool
			Whether to apply constraints, by default False.
		bool_opt_possb_comb : bool
			Whether to optimize the possible combinations of weights, by default False.
		nth_try : int
			Number of attempts to find a valid portfolio, by default 100.
		int_wdy : int
			Working days per year, by default 252.

		Returns
		-------
		tuple[NDArray[np.float64], NDArray[np.float64], NDArray[np.float64],
		NDArray[np.float64], NDArray[np.float64], list[str]]
			Returns, standard deviation, sharpes ratio, weights, returns and asset identifiers.

		Raises
		------
		ValueError
			Number of portfolios must be greater than zero
		"""
		if self.int_n_portfolios <= 0:
			raise ValueError("Number of portfolios must be greater than zero")
		# arrays of returns and minimum weights per asset  # noqa: ERA001
		array_returns, array_min_w, list_uuids = self.returns_min_w_uids(
			df_assets,
			col_dt,
			col_id,
			col_returns,
			col_min_w,
		)
		# generating random portfolios
		array_mus, array_sigmas, array_sharpes, array_weights = np.column_stack(
			[
				self.random_portfolio(
					array_returns,
					float_rf,
					bool_constraints,
					bool_opt_possb_comb,
					array_min_w,
					nth_try,
					int_wdy,
				)
				for _ in range(int_n_portfolios)
			]
		)
		# altering data types
		array_mus = array_mus.astype(float)
		array_sigmas = array_sigmas.astype(float)
		array_sharpes = array_sharpes.astype(float)
		return array_mus, array_sigmas, array_sharpes, array_weights, array_returns, list_uuids

	def optimal_portfolios(
		self,
		array_returns: NDArray[np.float64],
		n_attempts: int = 1000,
		bool_progress_printing_opt: bool = False,
		int_wdy: int = 252,
	) -> tuple[NDArray[np.float64], NDArray[np.float64], NDArray[np.float64]]:
		"""Calculate optimal portfolios using quadratic programming.

		Parameters
		----------
		array_returns : NDArray[np.float64]
			Array of returns.
		n_attempts : int
			Number of attempts to find a valid portfolio, by default 1000.
		bool_progress_printing_opt : bool
			Whether to print progress, by default False.
		int_wdy : int
			Working days per year, by default 252.

		Returns
		-------
		tuple[NDArray[np.float64], NDArray[np.float64], NDArray[np.float64]]
			Returns, standard deviation and sharpes ratio.
		"""
		# turn on/off progress printing
		opt.solvers.options["show_progress"] = bool_progress_printing_opt
		# configuring data types
		array_returns = np.array(array_returns)
		# definig the number of portfolios to be created
		n = array_returns.shape[0]
		# calculating first attempt for float_mu in each portfolio
		mus = [10.0 ** (5.0 * float(t / n_attempts) - 1.0) for t in range(n_attempts)]
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
		list_portfolios = [
			opt.solvers.qp(float_mu * S, -pbar, G, h, A, b)["x"] for float_mu in mus
		]
		# calculating risk and return for efficient frontier  # noqa: ERA001
		array_returns = np.array([opt.blas.dot(pbar, x) * int_wdy for x in list_portfolios])
		array_sigmas = np.array(
			[np.sqrt(opt.blas.dot(x, S * x)) * np.sqrt(int_wdy) for x in list_portfolios]
		)
		# calculate the second degree polynomial of the frontier curve
		m1 = np.polyfit(array_returns, array_sigmas, 2)
		x1 = np.sqrt(m1[2] / m1[0])
		# calculate the optimal portfolio
		wt = opt.solvers.qp(opt.matrix(x1 * S), -pbar, G, h, A, b)["x"]
		# returning weights, returns, and sigma from efficient frontier  # noqa: ERA001
		return np.asarray(wt).flatten(), array_returns, array_sigmas

	def eff_frontier(
		self,
		array_eff_risks: NDArray[np.float64],
		array_eff_returns: NDArray[np.float64],
		array_weights: NDArray[np.float64],
		array_mus: NDArray[np.float64],
		array_sigmas: NDArray[np.float64],
		float_rf: float,
		col_sigma: str = "sigma",
		col_mu: str = "float_mu",
		col_w: str = "weights",
		col_sharpe: str = "sharpe",
		float_atol: float = 1e-2,
		int_pace_atol: int = 5,
	) -> tuple[pd.DataFrame, pd.DataFrame]:
		"""Generate the efficient frontier.

		Parameters
		----------
		array_eff_risks : NDArray[np.float64]
			Array of risks.
		array_eff_returns : NDArray[np.float64]
			Array of returns.
		array_weights : NDArray[np.float64]
			Array of weights.
		array_mus : NDArray[np.float64]
			Array of mus.
		array_sigmas : NDArray[np.float64]
			Array of sigmas.
		float_rf : float
			Risk free rate.
		col_sigma : str
			Column name for sigma, by default 'sigma'.
		col_mu : str
			Column name for mu, by default 'float_mu'.
		col_w : str
			Column name for weights, by default 'weights'.
		col_sharpe : str
			Column name for sharpe ratio, by default 'sharpe'.
		float_atol : float
			Tolerance, by default 1e-2.
		int_pace_atol : int
			Pace of tolerance, by default 5.

		Returns
		-------
		tuple[pd.DataFrame, pd.DataFrame]
			Returns and weights.
		"""
		# setting variables
		array_eff_weights = list()
		# convert string-based array_weights to a 2D array by splitting the values
		array_weights_2d = np.array(
			[list(map(float, row.split())) for row in array_weights], dtype=np.float64
		)
		# iterate over the efficient returns and risks  # noqa: ERA001
		for _, eff_risk in zip(array_eff_returns, array_eff_risks):
			while True:
				try:
					#   find the indices in sigmas that correspond to the current risk using
					#       np.isclose
					list_idx_sigma = np.where(np.isclose(array_sigmas, eff_risk, atol=float_atol))
					#   get the highest return for the given datasets  # noqa: ERA001
					idx_mu = np.argmax(array_mus[list_idx_sigma])
					#   get the index from mus and append weights
					array_eff_weights.append(array_weights_2d[idx_mu])
					#   in case of no error break the loop  # noqa: ERA001
					break
				except ValueError:
					float_atol *= int_pace_atol
		# convert to numpy array for final output if needed
		array_eff_weights = np.array(array_eff_weights, dtype=np.float64)
		# create a dataframe
		columns = [f"weight_{i}" for i in range(array_eff_weights.shape[1])]
		df_eff = pd.DataFrame(array_eff_weights, columns=columns)
		df_eff[col_mu] = array_eff_returns
		df_eff[col_sigma] = array_eff_risks
		# calculate sharpe as the difference between array_eff_returns and float_rf  # noqa: ERA001
		#   divided by array_eff_risks
		df_eff[col_sharpe] = (df_eff[col_mu] - float_rf) / df_eff[col_sigma]
		# create a pandas dataframe with returns, weights and mus from the original porfolios  # noqa: ERA001,E501
		df_porf = pd.DataFrame({col_mu: array_mus, col_sigma: array_sigmas, col_w: array_weights})
		# output the results
		return df_eff, df_porf

	def plot_risk_return_portfolio(self) -> None:
		"""Plot Markowitz's Efficient Frontier for Portfolio Management.

		References
		----------
		- https://plotly.com/python/reference/layout/
		- https://plotly.com/python-api-reference/generated/plotly.graph_objects.scatter.html
		- https://plotly.com/python/builtin-colorscales/
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
		if self.bool_debug_mode:
			print("### MAXIMUM SHARPE PORTFOLIO ###")
			print(f"SHARPES ARGMAX: {self.array_sharpes.argmax()}")
			print(f"WEIGHTS: {self.array_weights[self.array_sharpes.argmax()]}")
			print(f"RISK: {self.array_sigmas[self.array_sharpes.argmax()]}")
			print(f"RETURN: {self.array_mus[self.array_sharpes.argmax()]}")
			print(f"SHARPE: {self.array_sharpes[self.array_sharpes.argmax()]}")
		# prepare customdata for scatter plot
		customdata_portfolios = np.array(
			[[weights, ", ".join(self.list_securities)] for weights in self.array_weights],
			dtype=object,
		)
		# prepare the subtitle with the list of securities
		subtitle_text = "list of securities: " + ", ".join(self.list_securities)
		# plotting data
		data = [
			go.Scatter(
				x=self.array_sigmas.flatten(),
				y=self.array_mus.flatten(),
				mode="markers",
				marker=dict(
					color=self.array_sharpes.flatten(),
					colorscale="Viridis",
					showscale=True,
					cmax=self.array_sharpes.flatten().max(),
					cmin=0,
					colorbar=dict(title="Sharpe Ratios"),
				),
				#   define the hovertemplate to include weights
				hovertemplate=(
					"Risk: %{x:.2f}<br>"
					+ "Returns: %{y:.2f}<br>"
					+ "Sharpe: %{marker.color:.2f}<br>"
					+ "Weight: %{customdata[0]}<extra></extra>"
				),
				#   weights data for hovertemplate
				customdata=customdata_portfolios,
				name="Portfolios",
			),
			go.Scatter(
				x=self.array_eff_risks,
				y=self.array_eff_returns,
				mode="lines+markers",
				line=dict(color="red", width=2),
				name="Efficient Frontier",
				hovertemplate=(
					"Risk: %{x:.2f}<br>"
					+ "Returns: %{y:.2f}<br>"
					+ "Weight: %{customdata}<extra></extra>"
				),
				customdata=self.array_eff_weights,
			),
			# add a green star for the minimum sigma portfolio
			go.Scatter(
				x=[min_sigma_sigma],
				y=[min_sigma_mu],
				mode="markers",
				marker=dict(size=30, color="green", symbol="star"),
				name="Min Risk Portfolio",
				hovertemplate="Risk: %{x:.2f}<br>Returns: %{y:.2f}<extra></extra>",
			),
			# add a red star for the maximum sharpe portfolio
			go.Scatter(
				x=[max_sharpe_sigma],
				y=[max_sharpe_mu],
				mode="markers",
				marker=dict(size=30, color="blue", symbol="star"),
				name="Max Sharpe Portfolio",
				hovertemplate="Risk: %{x:.2f}<br>Returns: %{y:.2f}<extra></extra>",
			),
		]
		# configuring title data
		dict_title = {
			"text": self.title_text,
			"xanchor": "center",
			"yanchor": "top",
			"y": 0.95,
			"x": 0.5,
		}
		# legend
		dict_leg = {
			"orientation": "h",
			"yanchor": "bottom",
			"y": -0.2,
			"xanchor": "center",
			"x": 0.5,
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
					xref="paper",
					yref="paper",
					showarrow=False,
					font=dict(size=12, color="gray"),
					align="center",
				)
			],
			xaxis_rangeslider_visible=False,
			width=1280,
			height=720,
			xaxis_showgrid=True,
			xaxis_gridwidth=1,
			xaxis_gridcolor="#E8E8E8",
			yaxis_showgrid=True,
			yaxis_gridwidth=1,
			yaxis_gridcolor="#E8E8E8",
			yaxis_title=self.yaxis_title,
			xaxis_title=self.xaxis_title,
			legend=dict_leg,
			plot_bgcolor="rgba(0,0,0,0)",
		)
		# save plot, if is user's interest
		if self.path_fig is not None:
			fig_extension = self.path_fig.split(".")[-1]
			fig.write_image(self.path_fig, format=fig_extension, scale=2, width=1280, height=720)
		# display plot
		if self.bool_show_plot:
			fig.show()

	def max_sharpe(self) -> ResultMaxSharpe:
		"""Maximum Sharpe Portfolio.

		Returns
		-------
		ResultMaxSharpe
			Maximum sharpe ratio portfolio

		Raises
		------
		ValueError
			No available portfolios with non-zero weights
		"""
		# ensuring that all weights are non-zero, if is user's interest
		if self.bool_non_zero_w_eff:
			array_valid_indices = np.where((self.array_weights != 0).all(axis=1))[0]
			if len(array_valid_indices) == 0:
				raise ValueError("No available portfolios with non-zero weights")
			int_argmax_sharpe = array_valid_indices[
				self.array_sharpes[array_valid_indices].argmax()
			]
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
		# calculating notional (ensure array_eff_quantities is properly calculated as float)  # noqa: ERA001,E501
		self.array_close = np.round(self.array_close, self.int_round_close)
		array_notional = np.array(self.array_close) * np.array(array_eff_quantities)
		return {
			"tickers": list(self.list_securities),
			"argmax_sharpe": int(int_argmax_sharpe),
			"eff_weights": array_eff_w,
			"eff_mu": array_eff_mu,
			"eff_sharpe": array_eff_sharpe,
			"eff_quantities": array_eff_quantities,
			"close": self.array_close,
			"notional": array_notional,
			"notional_total": array_notional.sum(),
		}

	def min_sigma(self) -> ResultMaxSharpe:
		"""Minimum Risk Portfolio.

		Returns
		-------
		ResultMaxSharpe
			Minimum risk portfolio

		Raises
		------
		ValueError
			No available portfolios with non-zero weights
		"""
		# ensuring that all weights are non-zero, if is user's interest
		if self.bool_non_zero_w_eff:
			array_valid_indices = np.where((self.array_weights != 0).all(axis=1))[0]
			if len(array_valid_indices) == 0:
				raise ValueError("No available portfolios with non-zero weights")
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
		# calculating notional (ensure array_eff_quantities is properly calculated as float)  # noqa: ERA001,E501
		self.array_close = np.round(self.array_close, self.int_round_close)
		array_notional = np.array(self.array_close) * np.array(array_eff_quantities)
		return {
			"tickers": self.list_securities,
			"argmin_risk": int_argmin_risk,
			"eff_weights": array_eff_w,
			"eff_risk": array_eff_risk,
			"eff_mu": array_eff_mu,
			"eff_quantities": array_eff_quantities,
			"close": self.array_close,
			"notional": array_notional,
			"notional_total": array_notional.sum(),
		}
