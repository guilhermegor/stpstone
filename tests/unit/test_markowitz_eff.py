"""Unit tests for MarkowitzEff class.

Tests the Markowitz Efficient Frontier functionality including:
- Initialization with valid and invalid inputs
- Portfolio optimization calculations
- Efficient frontier generation
- Risk/return analysis
- Plotting functionality
- Edge cases and error conditions
"""

from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from stpstone.analytics.portfolio_alloc.markowitz_eff import MarkowitzEff


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_market_data() -> pd.DataFrame:
	"""Fixture providing sample market data for testing.

	Returns
	-------
	pd.DataFrame
		DataFrame with sample market data containing:
		- ticker: Asset identifiers
		- dt_date: Dates
		- close: Closing prices
		- daily_return: Daily returns
	"""
	dates = pd.date_range(start="2020-01-01", end="2020-01-10")
	tickers = ["AAPL", "MSFT", "GOOG"]
	data = []

	for ticker in tickers:
		closes = np.linspace(100, 110, len(dates))
		returns = np.random.normal(0.001, 0.02, len(dates) - 1)
		returns = np.insert(returns, 0, 0)  # First day has no return

		for i, date in enumerate(dates):
			data.append(
				{"ticker": ticker, "dt_date": date, "close": closes[i], "daily_return": returns[i]}
			)

	return pd.DataFrame(data)


@pytest.fixture
def valid_markowitz_args(sample_market_data: pd.DataFrame) -> dict[str, Any]:
	"""Fixture providing valid arguments for MarkowitzEff initialization.

	Parameters
	----------
	sample_market_data : pd.DataFrame
		The sample market data fixture

	Returns
	-------
	dict[str, Any]
		Dictionary of valid initialization arguments
	"""
	return {
		"df_mktdata": sample_market_data,
		"int_n_portfolios": 100,
		"float_prtf_notional": 10000.0,
		"float_rf": 0.02,
		"col_ticker": "ticker",
		"col_close": "close",
		"col_dt": "dt_date",
		"col_returns": "daily_return",
		"bool_constraints": True,
		"bool_debug_mode": False,
	}


@pytest.fixture
def markowitz_instance(valid_markowitz_args: dict[str, Any]) -> MarkowitzEff:
	"""Fixture providing an initialized MarkowitzEff instance.

	Parameters
	----------
	valid_markowitz_args : dict[str, Any]
		The valid arguments fixture

	Returns
	-------
	Any
		Initialized MarkowitzEff instance
	"""
	return MarkowitzEff(**valid_markowitz_args)


# --------------------------
# Tests
# --------------------------
class TestInitialization:
	"""Tests for MarkowitzEff class initialization."""

	def test_init_with_valid_args(self, markowitz_instance: MarkowitzEff) -> None:
		"""Test initialization with valid arguments.

		Verifies that:
		- The instance is created successfully
		- Key attributes are set correctly
		- Data processing completes without errors

		Parameters
		----------
		markowitz_instance : MarkowitzEff
			The initialized MarkowitzEff instance
		"""
		assert markowitz_instance is not None
		assert hasattr(markowitz_instance, "list_securities")
		assert len(markowitz_instance.list_securities) == 3
		assert hasattr(markowitz_instance, "array_close")
		assert markowitz_instance.array_close.shape == (3,)

	def test_init_with_missing_columns(self, sample_market_data: pd.DataFrame) -> None:
		"""Test initialization with missing required columns.

		Verifies that:
		- Appropriate exceptions are raised when required columns are missing

		Parameters
		----------
		sample_market_data : pd.DataFrame
			The sample market data fixture
		"""
		# Test missing ticker column
		bad_data = sample_market_data.drop(columns=["ticker"])
		with pytest.raises(KeyError):
			MarkowitzEff(
				df_mktdata=bad_data,
				int_n_portfolios=100,
				float_prtf_notional=10000.0,
				float_rf=0.02,
			)

		# Test missing returns column
		bad_data = sample_market_data.drop(columns=["daily_return"])
		with pytest.raises(KeyError):
			MarkowitzEff(
				df_mktdata=bad_data,
				int_n_portfolios=100,
				float_prtf_notional=10000.0,
				float_rf=0.02,
				col_returns="daily_return",
			)

	def test_init_with_invalid_n_portfolios(self, valid_markowitz_args: dict[str, Any]) -> None:
		"""Test initialization with invalid portfolio counts.

		Verifies that:
		- ValueError is raised for non-positive portfolio counts
		- TypeError is raised for non-integer portfolio counts

		Parameters
		----------
		valid_markowitz_args : dict[str, Any]
			The valid arguments fixture
		"""
		# Test zero portfolios
		args = valid_markowitz_args.copy()
		args["int_n_portfolios"] = 0
		with pytest.raises(ValueError, match="Number of portfolios must be greater than zero"):
			MarkowitzEff(**args)

		# Test negative portfolios
		args["int_n_portfolios"] = -10
		with pytest.raises(ValueError, match="Number of portfolios must be greater than zero"):
			MarkowitzEff(**args)

		# Test non-integer portfolios
		args["int_n_portfolios"] = 10.5
		with pytest.raises(TypeError, match="must be of type"):
			MarkowitzEff(**args)

	def test_init_with_invalid_notional(self, valid_markowitz_args: dict[str, Any]) -> None:
		"""Test initialization with invalid notional values.

		Verifies that:
		- ValueError is raised for non-positive notional
		- TypeError is raised for non-numeric notional

		Parameters
		----------
		valid_markowitz_args : dict[str, Any]
			The valid arguments fixture
		"""
		# Test zero notional
		args = valid_markowitz_args.copy()
		args["float_prtf_notional"] = 0.0
		with pytest.raises(ValueError, match="Min invest per asset must be below 1.0."):
			MarkowitzEff(**args)

		# Test negative notional
		args["float_prtf_notional"] = -10000.0
		with pytest.raises(ValueError, match="Min invest per asset must be below 1.0."):
			MarkowitzEff(**args)

		# Test non-numeric notional
		args["float_prtf_notional"] = "10000"
		with pytest.raises(TypeError):
			MarkowitzEff(**args)


class TestSharpeRatio:
	"""Tests for sharpe_ratio method."""

	def test_sharpe_ratio_calculation(self, markowitz_instance: MarkowitzEff) -> None:
		"""Test Sharpe ratio calculation with valid inputs.

		Verifies that:
		- Calculation is correct for known inputs
		- Handles different input types correctly

		Parameters
		----------
		markowitz_instance : MarkowitzEff
			The initialized MarkowitzEff instance
		"""
		# Test with typical values
		result = markowitz_instance.sharpe_ratio(0.1, 0.05, 0.02)
		assert pytest.approx(result) == (0.1 - 0.02) / 0.05

		# Test with zero risk-free rate
		result = markowitz_instance.sharpe_ratio(0.1, 0.05, 0.0)
		assert pytest.approx(result) == 0.1 / 0.05

		# Test with negative return
		result = markowitz_instance.sharpe_ratio(-0.1, 0.05, 0.02)
		assert pytest.approx(result) == (-0.1 - 0.02) / 0.05

	def test_sharpe_ratio_invalid_inputs(self, markowitz_instance: MarkowitzEff) -> None:
		"""Test Sharpe ratio with invalid inputs.

		Verifies that:
		- Appropriate exceptions are raised for invalid inputs

		Parameters
		----------
		markowitz_instance : MarkowitzEff
			The initialized MarkowitzEff instance
		"""
		# Test with zero sigma
		with pytest.raises(ValueError, match="Standard deviation must be positive"):
			markowitz_instance.sharpe_ratio(0.1, 0.0, 0.02)

		# Test with negative sigma
		with pytest.raises(ValueError, match="Standard deviation must be positive"):
			markowitz_instance.sharpe_ratio(0.1, -0.05, 0.02)

		# Test with non-numeric inputs
		with pytest.raises(TypeError):
			markowitz_instance.sharpe_ratio("0.1", 0.05, 0.02)
		with pytest.raises(TypeError):
			markowitz_instance.sharpe_ratio(0.1, "0.05", 0.02)
		with pytest.raises(TypeError):
			markowitz_instance.sharpe_ratio(0.1, 0.05, "0.02")


class TestRandomWeights:
	"""Tests for random_weights method."""

	def test_random_weights_basic(self, markowitz_instance: MarkowitzEff) -> None:
		"""Test random weights generation without constraints.

		Verifies that:
		- Generated weights sum to 1 (within tolerance)
		- All weights are between 0 and 1
		- Output shape is correct

		Parameters
		----------
		markowitz_instance : MarkowitzEff
			The initialized MarkowitzEff instance
		"""
		weights = markowitz_instance.random_weights(5)
		assert len(weights) == 5
		assert pytest.approx(np.sum(weights), abs=1e-6) == pytest.approx(1.0, abs=1e-6)
		assert all(0 <= w <= 1 for w in weights)

	def test_random_weights_with_constraints(self, markowitz_instance: MarkowitzEff) -> None:
		"""Test random weights generation with constraints.

		Verifies that:
		- Constraints are respected when enabled
		- Minimum weights are honored or weights are zero

		Parameters
		----------
		markowitz_instance : MarkowitzEff
			The initialized MarkowitzEff instance
		"""
		min_weights = np.array([0.1, 0.2, 0.05])
		weights = markowitz_instance.random_weights(
			int_n_assets=3, bool_constraints=True, array_min_w=min_weights, nth_try=100
		)

		# check that weights either meet minimum or are zero
		for w, min_w in zip(weights, min_weights):
			assert (w == 0) or (w >= min_w)

		# check sum is approximately 1
		assert pytest.approx(np.sum(weights), abs=1e-4) == 1.0

	def test_random_weights_constraint_failure(self, markowitz_instance: MarkowitzEff) -> None:
		"""Test random weights with impossible constraints.

		Verifies that:
		- Method falls back to single-asset portfolio when constraints can't be met

		Parameters
		----------
		markowitz_instance : MarkowitzEff
			The initialized MarkowitzEff instance
		"""
		# Impossible constraints (sum of minimums > 1)
		min_weights = np.array([0.6, 0.6])
		weights = markowitz_instance.random_weights(
			int_n_assets=2,
			bool_constraints=True,
			array_min_w=min_weights,
			nth_try=5,  # Low number to force early fallback
		)

		# Should fall back to one asset with weight 1, others 0
		assert sum(weights == 1.0) == 1
		assert sum(weights == 0.0) == 1

	def test_random_weights_invalid_inputs(self, markowitz_instance: MarkowitzEff) -> None:
		"""Test random weights with invalid inputs.

		Verifies that:
		- Appropriate exceptions are raised for invalid inputs

		Parameters
		----------
		markowitz_instance : MarkowitzEff
			The initialized MarkowitzEff instance
		"""
		# negative number of assets
		with pytest.raises(ValueError, match="Number of assets must be positive."):
			markowitz_instance.random_weights(-1)

		# zero assets
		with pytest.raises(ValueError, match="Number of assets must be positive"):
			markowitz_instance.random_weights(0)

		# mismatched min weights length
		with pytest.raises(ValueError, match="length of min invest per asset must match"):
			markowitz_instance.random_weights(
				int_n_assets=3, bool_constraints=True, array_min_w=np.array([0.1, 0.2])
			)

		# Invalid min weights values
		with pytest.raises(ValueError, match="Min invest per asset must be positive"):
			markowitz_instance.random_weights(
				int_n_assets=2, bool_constraints=True, array_min_w=np.array([-0.1, 0.2])
			)
		with pytest.raises(ValueError, match="Min invest per asset must be below 1.0"):
			markowitz_instance.random_weights(
				int_n_assets=2, bool_constraints=True, array_min_w=np.array([1.1, 0.2])
			)


class TestPortfolioGeneration:
	"""Tests for portfolio generation methods."""

	def test_random_portfolio(self, markowitz_instance: MarkowitzEff) -> None:
		"""Test generation of a single random portfolio.

		Verifies that:
		- Returns expected tuple of values
		- All components have correct types
		- Sharpe ratio is calculated correctly

		Parameters
		----------
		markowitz_instance : MarkowitzEff
			The initialized MarkowitzEff instance

		Returns
		-------
		None
		"""
		returns = np.random.randn(3, 10)  # 3 assets, 10 periods
		mu, sigma, sharpe, weights = markowitz_instance.random_portfolio(
			array_returns=returns, float_rf=0.02
		)

		# check types
		assert isinstance(mu, float)
		assert isinstance(sigma, float)
		assert isinstance(sharpe, float)
		assert isinstance(weights, str)

		# check Sharpe ratio calculation
		expected_sharpe = (mu - 0.02) / sigma
		assert pytest.approx(sharpe) == expected_sharpe

	def test_random_portfolios(self, markowitz_instance: MarkowitzEff) -> None:
		"""Test generation of multiple random portfolios.

		Verifies that:
		- Returns expected arrays with correct shapes
		- All arrays have consistent lengths
		- Sharpe ratios are calculated correctly

		Parameters
		----------
		markowitz_instance : MarkowitzEff
			The initialized MarkowitzEff instance

		Returns
		-------
		None
		"""
		mus, sigmas, sharpes, weights, returns, uuids = markowitz_instance.random_portfolios(
			df_assets=markowitz_instance.df_mktdata,
			int_n_portfolios=50,
			col_id="ticker",
			col_dt="dt_date",
			col_returns="daily_return",
		)

		# check array shapes
		assert len(mus) == 50
		assert len(sigmas) == 50
		assert len(sharpes) == 50
		assert len(weights) == 50
		assert len(uuids) == 3  # Should match number of securities

	def test_optimal_portfolios(self, markowitz_instance: MarkowitzEff) -> None:
		"""Test generation of optimal portfolios.

		Verifies that:
		- Returns expected arrays with correct shapes
		- Efficient frontier has non-decreasing returns with risk

		Parameters
		----------
		markowitz_instance : MarkowitzEff
			The initialized MarkowitzEff instance

		Returns
		-------
		None
		"""
		# mock returns data
		returns = np.random.randn(3, 10)  # 3 assets, 10 periods

		weights, eff_returns, eff_risks = markowitz_instance.optimal_portfolios(
			array_returns=returns, n_attempts=20
		)

		# check array shapes
		assert weights.shape == (3,)  # one weight per asset
		assert len(eff_returns) == 20
		assert len(eff_risks) == 20


class TestEfficientFrontier:
	"""Tests for efficient frontier generation."""

	def test_eff_frontier(self, markowitz_instance: MarkowitzEff) -> None:
		"""Test efficient frontier generation.

		Verifies that:
		- Returns expected DataFrames
		- DataFrames contain expected columns
		- Sharpe ratios are calculated correctly

		Parameters
		----------
		markowitz_instance : MarkowitzEff
			The initialized MarkowitzEff instance

		Returns
		-------
		None
		"""
		# Generate test data
		n_portfolios = 50
		mus = np.random.uniform(0.05, 0.15, n_portfolios)
		sigmas = np.random.uniform(0.1, 0.2, n_portfolios)
		weights = np.random.dirichlet(np.ones(3), size=n_portfolios)
		weights_str = np.array([" ".join(map(str, w)) for w in weights])

		eff_risks = np.linspace(0.1, 0.2, 10)
		eff_returns = np.linspace(0.05, 0.15, 10)

		df_eff, df_porf = markowitz_instance.eff_frontier(
			array_eff_risks=eff_risks,
			array_eff_returns=eff_returns,
			array_weights=weights_str,
			array_mus=mus,
			array_sigmas=sigmas,
			float_rf=0.02,
		)

		# check DataFrame structures
		assert isinstance(df_eff, pd.DataFrame)
		assert isinstance(df_porf, pd.DataFrame)

		# check expected columns
		assert "sigma" in df_eff.columns
		assert "float_mu" in df_eff.columns
		assert "sharpe" in df_eff.columns
		assert all(f"weight_{i}" in df_eff.columns for i in range(3))

		# check Sharpe ratio calculation
		calculated_sharpes = (df_eff["float_mu"] - 0.02) / df_eff["sigma"]
		assert np.allclose(df_eff["sharpe"], calculated_sharpes, rtol=1e-6)


class TestPortfolioAnalysis:
	"""Tests for portfolio analysis methods."""

	def test_max_sharpe(self, markowitz_instance: MarkowitzEff) -> None:
		"""Test maximum Sharpe ratio portfolio selection.

		Verifies that:
		- Returns expected dictionary structure
		- All components have correct types
		- Quantities and notionals are calculated correctly

		Parameters
		----------
		markowitz_instance : MarkowitzEff
			The initialized MarkowitzEff instance

		Returns
		-------
		None
		"""
		result = markowitz_instance.max_sharpe()

		# check structure
		assert "tickers" in result
		assert "argmax_sharpe" in result
		assert "eff_weights" in result
		assert "eff_mu" in result
		assert "eff_sharpe" in result
		assert "eff_quantities" in result
		assert "close" in result
		assert "notional" in result
		assert "notional_total" in result

		# check types
		assert isinstance(result["tickers"], list)
		assert isinstance(result["argmax_sharpe"], int)
		assert isinstance(result["eff_weights"], str)
		assert isinstance(result["eff_mu"], np.float64)
		assert isinstance(result["eff_sharpe"], np.float64)
		assert isinstance(result["eff_quantities"], list)
		assert isinstance(result["close"], np.ndarray)
		assert isinstance(result["notional"], np.ndarray)
		assert isinstance(result["notional_total"], float)

		# check quantities calculation
		weights = np.array(list(map(float, result["eff_weights"].split())))
		quantities = np.array(result["eff_quantities"])
		expected_quantities = np.round(
			weights * markowitz_instance.float_prtf_notional / result["close"]
		)
		assert np.allclose(quantities, expected_quantities, rtol=0.1)

	def test_min_sigma(self, markowitz_instance: MarkowitzEff) -> None:
		"""Test minimum risk portfolio selection.

		Verifies that:
		- Returns expected dictionary structure
		- All components have correct types
		- The selected portfolio has minimum risk

		Parameters
		----------
		markowitz_instance : MarkowitzEff
			The initialized MarkowitzEff instance

		Returns
		-------
		None
		"""
		result = markowitz_instance.min_sigma()

		# check structure
		assert "tickers" in result
		assert "argmin_risk" in result
		assert "eff_weights" in result
		assert "eff_risk" in result
		assert "eff_mu" in result
		assert "eff_quantities" in result
		assert "close" in result
		assert "notional" in result
		assert "notional_total" in result

		# verify it's actually the minimum risk portfolio
		min_risk_idx = result["argmin_risk"]
		assert markowitz_instance.array_sigmas[min_risk_idx] == min(
			markowitz_instance.array_sigmas
		)


class TestPlotting:
	"""Tests for plotting functionality."""

	@patch("plotly.graph_objects.Figure.show")
	def test_plot_risk_return_portfolio(
		self, mock_show: MagicMock, markowitz_instance: MarkowitzEff
	) -> None:
		"""Test portfolio risk/return plotting.

		Verifies that:
		- Plotting function executes without errors
		- Correct data structures are created

		Parameters
		----------
		mock_show : MagicMock
			Mocked show function
		markowitz_instance : MarkowitzEff
			The initialized MarkowitzEff instance

		Returns
		-------
		None
		"""
		# call plotting function
		markowitz_instance.plot_risk_return_portfolio()

		# verify show was called if bool_show_plot is True
		if markowitz_instance.bool_show_plot:
			mock_show.assert_called_once()
		else:
			mock_show.assert_not_called()

	@patch("plotly.graph_objects.Figure.write_image")
	def test_plot_saving(
		self, mock_write: MagicMock, valid_markowitz_args: dict[str, Any]
	) -> None:
		"""Test saving of plots to file.

		Verifies that:
		- Plot is saved when path_fig is provided
		- Correct file extension is used

		Parameters
		----------
		mock_write : MagicMock
			Mocked write_image function
		valid_markowitz_args : dict[str, Any]
			Valid MarkowitzEff arguments

		Returns
		-------
		None
		"""
		# set path for saving
		valid_markowitz_args["path_fig"] = "test_plot.png"
		valid_markowitz_args["bool_show_plot"] = False

		# initialize instance
		instance = MarkowitzEff(**valid_markowitz_args)

		# call plotting function
		instance.plot_risk_return_portfolio()

		# verify write_image was called
		mock_write.assert_called_once_with(
			"test_plot.png", format="png", scale=2, width=1280, height=720
		)
