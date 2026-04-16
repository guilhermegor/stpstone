"""Risk measurement and portfolio analysis tools.

This module provides classes for calculating various risk metrics including Value at Risk (VaR),
Conditional VaR (CVaR), portfolio statistics, and performance measures. It supports multiple
calculation methods including historical, parametric, and Monte Carlo approaches.
"""

from typing import Literal, Optional, TypedDict, Union

import numpy as np
from numpy.typing import NDArray
import pandas as pd
from scipy.stats import norm

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.arrays import Arrays


class ReturnDescriptiveStats(TypedDict):
	"""Return type for descriptive statistics.

	Parameters
	----------
	mu : float
		Mean return
	std : float
		Standard deviation of returns
	ewma_std : float
		Exponentially weighted moving average standard deviation
	"""

	mu: float
	std: float
	ewma_std: float


class RiskStats(metaclass=TypeChecker):
	"""Class for calculating basic risk statistics."""

	def __init__(self, array_r: Union[NDArray[np.float64], pd.Series, list[float]]) -> None:
		"""Initialize RiskStats with return series.

		Parameters
		----------
		array_r : Union[NDArray[np.float64], pd.Series, list[float]]
			Sequence of returns ordered by dates in descending order

		Returns
		-------
		None
		"""
		self.array_r = Arrays().to_array(array_r)
		self._validate_array_r()

	def _validate_array_r(self) -> None:
		"""Validate input return array.

		Returns
		-------
		None

		Raises
		------
		ValueError
			If return array is empty or contains NaN or infinite values
		"""
		if len(self.array_r) == 0:
			raise ValueError("Return array cannot be empty")
		if not np.all(np.isfinite(self.array_r)):
			raise ValueError("Return array contains NaN or infinite values")

	def _validate_lambda(self, float_lambda: float) -> None:
		"""Validate EWMA lambda parameter.

		Parameters
		----------
		float_lambda : float
			Smoothing factor for EWMA

		Returns
		-------
		None

		Raises
		------
		ValueError
			If lambda is not between 0 and 1
		"""
		if not 0 <= float_lambda <= 1:
			raise ValueError(f"Lambda must be between 0 and 1, got {float_lambda}")

	def variance_ewma(self, float_lambda: Optional[float] = 0.94) -> float:
		"""Calculate Exponentially Weighted Moving Average variance.

		Parameters
		----------
		float_lambda : Optional[float]
			Smoothing factor (default: 0.94)

		Returns
		-------
		float
			EWMA variance

		References
		----------
		.. [1] https://corporatefinanceinstitute.com/resources/career-map/sell-side/capital-markets/exponentially-weighted-moving-average-ewma/
		"""
		self._validate_lambda(float_lambda)
		array_ewma = np.zeros_like(self.array_r)
		array_ewma[0] = self.array_r[0]

		for t in range(1, len(self.array_r)):
			array_ewma[t] = float_lambda * self.array_r[t] + (1 - float_lambda) * array_ewma[t - 1]
		return np.sum(array_ewma)

	def descriptive_stats(self, float_lambda: Optional[float] = 0.94) -> ReturnDescriptiveStats:
		"""Calculate descriptive statistics for returns.

		Parameters
		----------
		float_lambda : Optional[float]
			Smoothing factor for EWMA (default: 0.94)

		Returns
		-------
		ReturnDescriptiveStats
			Dictionary containing mean, std and EWMA std
		"""
		stats = {
			"mu": np.mean(self.array_r),
			"std": np.std(self.array_r),
		}

		ewma_var = self.variance_ewma(float_lambda)
		stats["ewma_std"] = np.sqrt(ewma_var**2) if np.isfinite(ewma_var) else np.nan

		return stats


class MarkowitzPortf(metaclass=TypeChecker):
	"""Portfolio analysis using Markowitz mean-variance framework."""

	def __init__(
		self,
		array_r: Union[NDArray[np.float64], pd.DataFrame],
		array_w: Union[NDArray[np.float64], pd.DataFrame],
		float_lambda: Optional[float] = 0.94,
		bool_validate_w: Optional[bool] = True,
		float_atol: Optional[float] = 1e-4,
	) -> None:
		"""Initialize portfolio with returns and weights.

		Parameters
		----------
		array_r : Union[NDArray[np.float64], pd.DataFrame]
			Array of asset returns
		array_w : Union[NDArray[np.float64], pd.DataFrame]
			Array of portfolio weights
		float_lambda : Optional[float]
			EWMA smoothing factor (default: 0.94)
		bool_validate_w : Optional[bool]
			Flag to validate weights (default: True)
		float_atol : Optional[float]
			Absolute tolerance for weight sum validation (default: 1e-4)

		Returns
		-------
		None
		"""
		self.float_lambda = float_lambda
		self.array_r = Arrays().to_array(array_r)
		self.array_w = Arrays().to_array(array_w)

		if bool_validate_w:
			self._validate_weights(float_atol)

	def _validate_weights(self, float_atol: float) -> None:
		"""Validate portfolio weights sum to 1.

		Parameters
		----------
		float_atol : float
			Absolute tolerance for sum comparison

		Returns
		-------
		None

		Raises
		------
		ValueError
			If weights don't sum to 1 within tolerance
			If weights are not 1D or 2D
		"""
		if self.array_w.ndim == 1:
			if not np.isclose(np.sum(self.array_w), 1.0, atol=float_atol):
				raise ValueError("Portfolio weights must sum to 1.")
		elif self.array_w.ndim == 2:
			for i, row in enumerate(self.array_w):
				if not np.isclose(np.sum(row), 1.0, atol=float_atol):
					raise ValueError(f"Portfolio weights in row {i} must sum to 1.")
		else:
			raise ValueError("Portfolio weights must be either 1D or 2D array.")

	def mu(self) -> float:
		"""Calculate portfolio mean return.

		Returns
		-------
		float
			Portfolio mean return
		"""
		if self.array_r.ndim == 1:
			return np.sum(self.array_r * self.array_w)
		return np.mean(np.sum(self.array_r * self.array_w, axis=1))

	def cov(self) -> NDArray[np.float64]:
		"""Calculate portfolio covariance matrix.

		Returns
		-------
		NDArray[np.float64]
			Covariance matrix

		References
		----------
		.. [1] https://www.ime.usp.br/~rvicente/Aula2_VaROverviewParte2.pdf, page 27
		"""
		if self.array_r.ndim == 1:
			return np.array([[np.var(self.array_r)]])

		array_cov = np.cov(self.array_r, rowvar=False)
		if self.float_lambda is None:
			return array_cov

		for t in range(1, self.array_r.shape[0]):
			array_r_t = self.array_r[t]
			array_cov = self.float_lambda * array_cov + (1 - self.float_lambda) * np.dot(
				array_r_t, array_r_t.T
			)
		return array_cov

	def sigma(self) -> float:
		"""Calculate portfolio standard deviation.

		Returns
		-------
		float
			Portfolio standard deviation
		"""
		cov = self.cov()
		if self.array_w.ndim == 1:
			if cov.ndim == 2:
				return np.sqrt(np.dot(self.array_w.T, np.dot(cov, self.array_w)))
			return np.sqrt(cov[0, 0]) * np.linalg.norm(self.array_w)

		array_sigmas = np.zeros(self.array_w.shape[0])
		for i, array_new_w in enumerate(self.array_w):
			array_sigmas[i] = np.sqrt(np.dot(array_new_w.T, np.dot(cov, array_new_w)))
		return float(array_sigmas[0])

	def sharpe_ratio(self, float_rf: float) -> float:
		"""Calculate Sharpe ratio.

		Parameters
		----------
		float_rf : float
			Risk-free rate

		Returns
		-------
		float
			Sharpe ratio
		"""
		return (self.mu() - float_rf) / self.sigma()


class VaR(metaclass=TypeChecker):
	"""Base class for Value at Risk calculations."""

	def __init__(
		self,
		float_mu: float,
		float_sigma: float,
		array_r: Optional[Union[NDArray[np.float64], pd.Series, list[float]]] = None,
		float_cl: Optional[float] = 0.95,
		int_t: Optional[int] = 1,
		float_lambda: Optional[float] = 0.94,
	) -> None:
		"""Initialize VaR calculator.

		Parameters
		----------
		float_mu : float
			Mean return
		float_sigma : float
			Standard deviation of returns
		array_r : Optional[Union[NDArray[np.float64], pd.Series, list[float]]]
			Historical returns (default: None)
		float_cl : Optional[float]
			Confidence level (default: 0.95)
		int_t : Optional[int]
			Time horizon (default: 1)
		float_lambda : Optional[float]
			EWMA smoothing factor (default: 0.94)

		Returns
		-------
		None
		"""
		self._validate_cl(float_cl)
		self._validate_time_horizon(int_t)

		self.float_mu = float_mu
		self.float_sigma = float_sigma
		self.float_cl = float_cl
		self.int_t = int_t
		self.float_lambda = float_lambda
		self.array_r = Arrays().to_array(array_r) if array_r is not None else None
		self.float_z = norm.ppf(float_cl)

	def _validate_cl(self, float_cl: float) -> None:
		"""Validate confidence level.

		Parameters
		----------
		float_cl : float
			Confidence level

		Returns
		-------
		None

		Raises
		------
		ValueError
			If confidence level is not between 0 and 1
		"""
		if not 0 < float_cl < 1:
			raise ValueError(f"Confidence level must be between 0 and 1, got {float_cl}")

	def _validate_time_horizon(self, int_t: int) -> None:
		"""Validate time horizon.

		Parameters
		----------
		int_t : int
			Time horizon

		Returns
		-------
		None

		Raises
		------
		ValueError
			If time horizon is not positive
		"""
		if int_t <= 0:
			raise ValueError(f"Time horizon must be positive, got {int_t}")

	def historic_var(self) -> float:
		"""Calculate historical VaR.

		Returns
		-------
		float
			Historical VaR

		Raises
		------
		ValueError
			If historical returns are not provided
		"""
		if self.array_r is None:
			raise ValueError("Historical returns not provided")
		return np.percentile(self.array_r, (1 - self.float_cl) * 100)

	def historic_var_stress_test(
		self, float_shock: float, str_shock_type: Literal["absolute", "relative"] = "relative"
	) -> float:
		"""Calculate stressed historical VaR.

		Parameters
		----------
		float_shock : float
			Shock magnitude
		str_shock_type : Literal['absolute', 'relative']
			Shock type (default: "relative")

		Returns
		-------
		float
			Stressed VaR

		Raises
		------
		ValueError
			If historical returns are not provided
			If shock type is invalid
		"""
		if self.array_r is None:
			raise ValueError("Historical returns not provided")

		if str_shock_type == "relative":
			array_r_shock = self.array_r * (1 + float_shock)
		elif str_shock_type == "absolute":
			array_r_shock = self.array_r + float_shock
		else:
			raise ValueError(
				f"Invalid shock type {str_shock_type}. Must be 'relative' or 'absolute'"
			)
		return np.percentile(array_r_shock, (1 - self.float_cl) * 100) * self.int_t

	def parametric_var(self) -> float:
		"""Calculate parametric VaR using normal distribution.

		Returns
		-------
		float
			Parametric VaR
		"""
		return self.float_mu * self.int_t - self.float_z * self.float_sigma * np.sqrt(self.int_t)

	def cvar(self) -> float:
		"""Calculate Conditional Value at Risk (Expected Shortfall).

		Returns
		-------
		float
			CVaR

		Raises
		------
		ValueError
			If historical returns are not provided

		Notes
		-----
		The CVaR is the mean loss of the left tail of the distribution below the VaR.
		"""
		if self.array_r is None:
			raise ValueError("Historical returns not provided")

		float_var = np.percentile(self.array_r, (1 - self.float_cl) * 100)
		array_cvar = self.array_r[self.array_r <= float_var]
		return np.mean(array_cvar) * self.int_t

	def monte_carlo_var(
		self, int_simulations: Optional[int] = 10_000, float_portf_nv: Optional[float] = 1_000_000
	) -> float:
		"""Calculate Monte Carlo VaR.

		Parameters
		----------
		int_simulations : Optional[int]
			Number of simulations (default: 10,000)
		float_portf_nv : Optional[float]
			Portfolio notional value (default: 1,000,000)

		Returns
		-------
		float
			Monte Carlo VaR
		"""
		array_simulated_r = np.random.normal(
			loc=self.float_mu, scale=self.float_sigma, size=(int_simulations, self.int_t)
		)
		array_portfs_nv = float_portf_nv * np.cumprod(1 + array_simulated_r, axis=1)
		array_portfs_nv = np.sort(array_portfs_nv)
		int_percentile_idx = int((1 - self.float_cl) * int_simulations)
		return float(float_portf_nv - array_portfs_nv[int_percentile_idx].item()) * self.int_t


class RiskMeasures(VaR):
	"""Extended risk measures calculator."""

	def __init__(
		self,
		float_mu: float,
		float_sigma: float,
		array_r: Optional[Union[NDArray[np.float64], pd.Series, list[float]]] = None,
		float_cl: Optional[float] = 0.95,
		int_t: Optional[int] = 1,
		float_lambda: Optional[float] = 0.94,
	) -> None:
		"""Initialize risk measures calculator.

		Parameters
		----------
		float_mu : float
			Mean return
		float_sigma : float
			Standard deviation of returns
		array_r : Optional[Union[NDArray[np.float64], pd.Series, list[float]]]
			Historical returns (default: None)
		float_cl : Optional[float]
			Confidence level (default: 0.95)
		int_t : Optional[int]
			Time horizon (default: 1)
		float_lambda : Optional[float]
			EWMA smoothing factor (default: 0.94)

		Returns
		-------
		None
		"""
		super().__init__(
			float_mu=float_mu,
			float_sigma=float_sigma,
			array_r=array_r,
			float_cl=float_cl,
			int_t=int_t,
			float_lambda=float_lambda,
		)

	def drawdown(self) -> float:
		"""Calculate maximum drawdown.

		Returns
		-------
		float
			Maximum drawdown

		Raises
		------
		ValueError
			If historical returns are not provided
		"""
		if self.array_r is None:
			raise ValueError("Historical returns not provided")

		array_cum_r = np.cumprod(1 + self.array_r)
		array_cummax_r = np.maximum.accumulate(array_cum_r)
		array_drawdown = (array_cum_r - array_cummax_r) / array_cummax_r
		return np.min(array_drawdown)

	def tracking_error(
		self,
		array_portf_r: Union[NDArray[np.float64], pd.Series, list[float]],
		array_benchmark_r: Union[NDArray[np.float64], pd.Series, list[float]],
		float_ddof: Optional[float] = 1,
	) -> float:
		"""Calculate tracking error between portfolio and benchmark.

		Parameters
		----------
		array_portf_r : Union[NDArray[np.float64], pd.Series, list[float]]
			Portfolio returns
		array_benchmark_r : Union[NDArray[np.float64], pd.Series, list[float]]
			Benchmark returns
		float_ddof : Optional[float]
			Delta degrees of freedom (default: 1)

		Returns
		-------
		float
			Tracking error
		"""
		array_portf_r = Arrays().to_array(array_portf_r)
		array_benchmark_r = Arrays().to_array(array_benchmark_r)
		array_active_r = array_portf_r - array_benchmark_r
		return np.std(array_active_r, ddof=float_ddof)

	def sharpe(self, float_rf: float) -> float:
		"""Calculate Sharpe ratio.

		Parameters
		----------
		float_rf : float
			Risk-free rate

		Returns
		-------
		float
			Sharpe ratio
		"""
		return (self.float_mu - float_rf) / self.float_sigma

	def beta(
		self,
		array_market_r: Union[NDArray[np.float64], pd.Series, list[float]],
		float_ddof: Optional[float] = 1,
	) -> float:
		"""Calculate portfolio beta.

		Parameters
		----------
		array_market_r : Union[NDArray[np.float64], pd.Series, list[float]]
			Market returns
		float_ddof : Optional[float]
			Delta degrees of freedom (default: 1)

		Returns
		-------
		float
			Portfolio beta
		"""
		array_market_r = Arrays().to_array(array_market_r)
		array_cov = np.cov(self.array_r, array_market_r, ddof=float_ddof)
		return array_cov[0, 1] / array_cov[1, 1]


class QuoteVar(VaR):
	"""VaR calculator for single asset/quotes."""

	def __init__(
		self,
		array_r: Union[NDArray[np.float64], pd.Series, list[float]],
		str_method_str: Literal["std", "ewma_std"] = "std",
		float_cl: Optional[float] = 0.95,
		int_t: Optional[int] = 1,
		float_lambda: Optional[float] = 0.94,
	) -> None:
		"""Initialize quote VaR calculator.

		Parameters
		----------
		array_r : Union[NDArray[np.float64], pd.Series, list[float]]
			Asset returns
		str_method_str : Literal['std', 'ewma_std']
			Standard deviation method (default: "std")
		float_cl : Optional[float]
			Confidence level (default: 0.95)
		int_t : Optional[int]
			Time horizon (default: 1)
		float_lambda : Optional[float]
			EWMA smoothing factor (default: 0.94)

		Returns
		-------
		None
		"""
		self.dict_desc_stats = RiskStats(array_r).descriptive_stats(float_lambda=float_lambda)
		super().__init__(
			float_mu=self.dict_desc_stats["mu"],
			float_sigma=self.dict_desc_stats[str_method_str],
			array_r=array_r,
			float_cl=float_cl,
			int_t=int_t,
			float_lambda=float_lambda,
		)


class PortfVar(VaR):
	"""Portfolio VaR calculator."""

	def __init__(
		self,
		array_r: Union[NDArray[np.float64], pd.DataFrame],
		array_w: Union[NDArray[np.float64], pd.DataFrame],
		float_cl: Optional[float] = 0.95,
		int_t: Optional[int] = 1,
		float_lambda: Optional[float] = 0.94,
		bool_validate_w: Optional[bool] = True,
		float_atol: Optional[float] = 1e-4,
	) -> None:
		"""Initialize portfolio VaR calculator.

		Parameters
		----------
		array_r : Union[NDArray[np.float64], pd.DataFrame]
			Asset returns
		array_w : Union[NDArray[np.float64], pd.DataFrame]
			Portfolio weights
		float_cl : Optional[float]
			Confidence level (default: 0.95)
		int_t : Optional[int]
			Time horizon (default: 1)
		float_lambda : Optional[float]
			EWMA smoothing factor (default: 0.94)
		bool_validate_w : Optional[bool]
			Flag to validate weights (default: True)
		float_atol : Optional[float]
			Absolute tolerance for weight validation (default: 1e-4)

		Returns
		-------
		None

		Raises
		------
		ValueError
			If number of weights does not match number of assets
			If return and weight arrays have different shapes
			If number of weights does not match number of assets
		"""
		array_r = Arrays().to_array(array_r)
		array_w = Arrays().to_array(array_w)

		# validate shapes
		if array_w.ndim == 1:
			if array_r.ndim == 2 and array_r.shape[1] != array_w.shape[0]:
				raise ValueError(
					f"Number of weights ({array_w.shape[0]}) must match number "
					f"of assets ({array_r.shape[1]})"
				)
			elif array_r.ndim == 1 and array_r.shape[0] != array_w.shape[0]:
				raise ValueError(
					f"Number of weights ({array_w.shape[0]}) must match number "
					f"of returns ({array_r.shape[0]})"
				)
		elif array_w.ndim == 2 and array_r.shape != array_w.shape:
			raise ValueError("Return and weight arrays must have the same shape.")

		self.cls_markowitz = MarkowitzPortf(
			array_r, array_w, float_lambda, bool_validate_w, float_atol
		)
		super().__init__(
			float_mu=self.cls_markowitz.mu(),
			float_sigma=self.cls_markowitz.sigma(),
			array_r=array_r,
			float_cl=float_cl,
			int_t=int_t,
			float_lambda=float_lambda,
		)
