"""Unit tests for risk measurement and portfolio analysis tools.

Tests the functionality of risk metrics calculation including:
- Basic risk statistics (RiskStats)
- Portfolio analysis (MarkowitzPortf)
- Value at Risk calculations (VaR, QuoteVar, PortfVar)
- Extended risk measures (RiskMeasures)
"""

from typing import Literal

import numpy as np
from numpy.typing import NDArray
import pandas as pd
import pytest

from stpstone.analytics.risk.market import (
    MarkowitzPortf,
    PortfVar,
    QuoteVar,
    RiskMeasures,
    RiskStats,
    VaR,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_returns() -> NDArray[np.float64]:
    """Fixture providing sample returns data.

    Returns
    -------
    NDArray[np.float64]
        Array of sample returns (10 elements)
    """
    return np.random.normal(0.001, 0.02, size=(10, 10))


@pytest.fixture
def sample_weights() -> NDArray[np.float64]:
    """Fixture providing sample portfolio weights.

    Returns
    -------
    NDArray[np.float64]
        Array of equal weights (10 elements)
    """
    return np.full(10, 0.1)


@pytest.fixture
def sample_returns_df(sample_returns: NDArray[np.float64]) -> pd.DataFrame:
    """Fixture providing sample returns as DataFrame.

    Parameters
    ----------
    sample_returns : NDArray[np.float64]
        Sample returns array from fixture

    Returns
    -------
    pd.DataFrame
        DataFrame containing sample returns
    """
    return pd.DataFrame(sample_returns.reshape(-1, 1))


@pytest.fixture
def sample_weights_df(sample_weights: NDArray[np.float64]) -> pd.DataFrame:
    """Fixture providing sample weights as DataFrame.

    Parameters
    ----------
    sample_weights : NDArray[np.float64]
        Sample weights array from fixture

    Returns
    -------
    pd.DataFrame
        DataFrame containing sample weights
    """
    return pd.DataFrame([sample_weights])


@pytest.fixture
def sample_portfolio_returns() -> NDArray[np.float64]:
    """Fixture providing sample portfolio returns (10 assets, 100 periods).
    
    Returns
    -------
    NDArray[np.float64]
        Array of sample portfolio returns
    """
    return np.random.normal(0.001, 0.02, size=(100, 10))

@pytest.fixture 
def sample_portfolio_weights() -> NDArray[np.float64]:
    """Fixture providing sample portfolio weights (10 assets).
    
    Returns
    -------
    NDArray[np.float64]
        Array of sample portfolio weights
    """
    weights = np.random.rand(10)
    return weights / weights.sum()


@pytest.fixture
def risk_stats(sample_returns: NDArray[np.float64]) -> RiskStats:
    """Fixture providing initialized RiskStats instance.

    Parameters
    ----------
    sample_returns : NDArray[np.float64]
        Sample returns array from fixture

    Returns
    -------
    RiskStats
        Initialized RiskStats instance
    """
    return RiskStats(sample_returns)


@pytest.fixture
def markowitz_portf(
    sample_portfolio_returns: NDArray[np.float64],
    sample_portfolio_weights: NDArray[np.float64]
) -> MarkowitzPortf:
    """Fixture providing initialized MarkowitzPortf instance.

    Parameters
    ----------
    sample_portfolio_returns : NDArray[np.float64]
        Sample returns array from fixture
    sample_portfolio_weights : NDArray[np.float64]
        Sample weights array from fixture

    Returns
    -------
    MarkowitzPortf
        Initialized MarkowitzPortf instance
    """
    return MarkowitzPortf(sample_portfolio_returns, sample_portfolio_weights)


@pytest.fixture
def var_instance(sample_returns: NDArray[np.float64]) -> VaR:
    """Fixture providing initialized VaR instance.

    Parameters
    ----------
    sample_returns : NDArray[np.float64]
        Sample returns array from fixture

    Returns
    -------
    VaR
        Initialized VaR instance
    """
    return VaR(
        float_mu=0.001,
        float_sigma=0.02,
        array_r=sample_returns,
        float_cl=0.95,
        int_t=1
    )


# --------------------------
# RiskStats Tests
# --------------------------
class TestRiskStats:
    """Test cases for RiskStats class."""

    def test_init_with_valid_returns(self, sample_returns: NDArray[np.float64]) -> None:
        """Test initialization with valid returns array.

        Verifies
        --------
        - Instance is created without errors
        - Array is properly stored

        Parameters
        ----------
        sample_returns : NDArray[np.float64]
            Sample returns array from fixture

        Returns
        -------
        None
        """
        risk_stats = RiskStats(sample_returns)
        assert np.array_equal(risk_stats.array_r, sample_returns)

    def test_init_with_pandas_series(self) -> None:
        """Test initialization with pandas Series.

        Verifies
        --------
        - Pandas Series is properly converted to numpy array

        Returns
        -------
        None
        """
        series = pd.Series([0.01, -0.02, 0.03])
        risk_stats = RiskStats(series)
        assert isinstance(risk_stats.array_r, np.ndarray)

    def test_init_with_list(self) -> None:
        """Test initialization with python list.

        Verifies
        --------
        - List is properly converted to numpy array

        Returns
        -------
        None
        """
        lst = [0.01, -0.02, 0.03]
        risk_stats = RiskStats(lst)
        assert isinstance(risk_stats.array_r, np.ndarray)

    def test_init_with_empty_array(self) -> None:
        """Test initialization with empty array raises ValueError.

        Verifies
        --------
        - ValueError is raised when array is empty

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Return array is empty"):
            RiskStats(np.array([]))

    def test_init_with_nan_values(self) -> None:
        """Test initialization with array containing NaN raises ValueError.

        Verifies
        --------
        - ValueError is raised when array contains NaN

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="contains NaN or infinite values"):
            RiskStats(np.array([0.01, np.nan, 0.03]))

    def test_variance_ewma_valid_lambda(self, risk_stats: RiskStats) -> None:
        """Test EWMA variance calculation with valid lambda.

        Verifies
        --------
        - Calculation completes without errors
        - Result is finite and non-negative

        Parameters
        ----------
        risk_stats : RiskStats
            RiskStats instance from fixture

        Returns
        -------
        None
        """
        result = risk_stats.variance_ewma(0.94)
        assert np.isfinite(result)
        assert abs(result) >= 0

    @pytest.mark.parametrize("lambda_val", [-0.1, 1.1, 2.0])
    def test_variance_ewma_invalid_lambda(
        self,
        risk_stats: RiskStats,
        lambda_val: float
    ) -> None:
        """Test EWMA variance with invalid lambda values.

        Verifies
        --------
        - ValueError is raised for invalid lambda values

        Parameters
        ----------
        risk_stats : RiskStats
            RiskStats instance from fixture
        lambda_val : float
            Invalid lambda value to test

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Lambda must be between 0 and 1"):
            risk_stats.variance_ewma(lambda_val)

    def test_descriptive_stats(self, risk_stats: RiskStats) -> None:
        """Test descriptive statistics calculation.

        Verifies
        --------
        - Returns dict with expected keys
        - All values are finite
        - Standard deviation is non-negative

        Parameters
        ----------
        risk_stats : RiskStats
            RiskStats instance from fixture

        Returns
        -------
        None
        """
        stats = risk_stats.descriptive_stats()
        assert set(stats.keys()) == {"mu", "std", "ewma_std"}
        assert all(np.isfinite(val) for val in stats.values())
        assert stats["std"] >= 0
        assert stats["ewma_std"] >= 0


# --------------------------
# MarkowitzPortf Tests
# --------------------------
class TestMarkowitzPortf:
    """Test cases for MarkowitzPortf class."""

    def test_init_with_valid_inputs(
        self,
        sample_returns: NDArray[np.float64],
        sample_weights: NDArray[np.float64]
    ) -> None:
        """Test initialization with valid returns and weights.

        Verifies
        --------
        - Instance is created without errors
        - Arrays are properly stored

        Parameters
        ----------
        sample_returns : NDArray[np.float64]
            Sample returns array from fixture
        sample_weights : NDArray[np.float64]
            Sample weights array from fixture

        Returns
        -------
        None
        """
        portf = MarkowitzPortf(sample_returns, sample_weights)
        assert np.array_equal(portf.array_r, sample_returns)
        assert np.array_equal(portf.array_w, sample_weights)

    def test_init_with_dataframes(
        self,
        sample_returns_df: pd.DataFrame,
        sample_weights_df: pd.DataFrame
    ) -> None:
        """Test initialization with pandas DataFrames.

        Verifies
        --------
        - DataFrames are properly converted to numpy arrays

        Parameters
        ----------
        sample_returns_df : pd.DataFrame
            Sample returns DataFrame from fixture
        sample_weights_df : pd.DataFrame
            Sample weights DataFrame from fixture

        Returns
        -------
        None
        """
        portf = MarkowitzPortf(
            sample_returns_df, 
            sample_weights_df,
            bool_validate_w=True
        )
        assert isinstance(portf.array_r, np.ndarray)
        assert isinstance(portf.array_w, np.ndarray)

    def test_weight_validation_enabled(
        self,
        sample_returns: NDArray[np.float64]
    ) -> None:
        """Test weight validation when enabled.

        Verifies
        --------
        - ValueError is raised when weights don't sum to 1

        Parameters
        ----------
        sample_returns : NDArray[np.float64]
            Sample returns array from fixture

        Returns
        -------
        None
        """
        invalid_weights = np.full(10, 0.11)  # Sums to 1.1
        with pytest.raises(ValueError, match="must sum to 1"):
            MarkowitzPortf(sample_returns, invalid_weights, bool_validate_w=True)

    def test_weight_validation_disabled(
        self,
        sample_returns: NDArray[np.float64]
    ) -> None:
        """Test weight validation when disabled.

        Verifies
        --------
        - No error is raised when validation is disabled

        Parameters
        ----------
        sample_returns : NDArray[np.float64]
            Sample returns array from fixture

        Returns
        -------
        None
        """
        invalid_weights = np.full(10, 0.11)  # Sums to 1.1
        portf = MarkowitzPortf(sample_returns, invalid_weights, bool_validate_w=False)
        assert portf is not None

    def test_mu_calculation(self, markowitz_portf: MarkowitzPortf) -> None:
        """Test portfolio mean return calculation.

        Verifies
        --------
        - Calculation returns finite value
        - Value is within expected range

        Parameters
        ----------
        markowitz_portf : MarkowitzPortf
            MarkowitzPortf instance from fixture

        Returns
        -------
        None
        """
        mu = markowitz_portf.mu()
        assert np.isfinite(mu)
        assert -1 <= mu <= 1  # reasonable bounds for returns

    def test_cov_calculation(self, markowitz_portf: MarkowitzPortf) -> None:
        """Test covariance matrix calculation.

        Verifies
        --------
        - Returns square matrix
        - Matrix is symmetric
        - Diagonal elements are non-negative

        Parameters
        ----------
        markowitz_portf : MarkowitzPortf
            MarkowitzPortf instance from fixture

        Returns
        -------
        None
        """
        cov = markowitz_portf.cov()
        assert cov.shape[0] == cov.shape[1]  # square matrix
        assert np.allclose(cov, cov.T)  # symmetric
        assert np.all(np.diag(cov) >= 0)  # non-negative variances

    def test_sigma_calculation(self, markowitz_portf: MarkowitzPortf) -> None:
        """Test portfolio standard deviation calculation.

        Verifies
        --------
        - Returns finite value
        - Value is non-negative

        Parameters
        ----------
        markowitz_portf : MarkowitzPortf
            MarkowitzPortf instance from fixture

        Returns
        -------
        None
        """
        sigma = markowitz_portf.sigma()
        assert np.isfinite(sigma)
        assert sigma >= 0

    def test_sharpe_ratio_calculation(self, markowitz_portf: MarkowitzPortf) -> None:
        """Test Sharpe ratio calculation.

        Verifies
        --------
        - Returns finite value
        - Value makes sense given inputs

        Parameters
        ----------
        markowitz_portf : MarkowitzPortf
            MarkowitzPortf instance from fixture

        Returns
        -------
        None
        """
        sharpe = markowitz_portf.sharpe_ratio(0.01)
        assert np.isfinite(sharpe)


# --------------------------
# VaR Tests
# --------------------------
class TestVaR:
    """Test cases for VaR class."""

    def test_init_with_valid_params(
        self,
        sample_portfolio_returns: NDArray[np.float64],
    ) -> None:
        """Test initialization with valid parameters.

        Verifies
        --------
        - Instance is created without errors
        - Parameters are properly stored

        Parameters
        ----------
        sample_portfolio_returns : NDArray[np.float64]
            Sample portfolio returns array from fixture

        Returns
        -------
        None
        """
        var_instance = VaR(
            float_mu=np.mean(sample_portfolio_returns),
            float_sigma=np.std(sample_portfolio_returns),
            array_r=sample_portfolio_returns
        )
        assert var_instance is not None
        assert np.isclose(var_instance.float_mu, np.mean(sample_portfolio_returns))
        assert np.isclose(var_instance.float_sigma, np.std(sample_portfolio_returns))

    @pytest.mark.parametrize("cl", [-0.1, 0.0, 1.0, 1.1])
    def test_invalid_confidence_level(self, cl: float) -> None:
        """Test initialization with invalid confidence levels.

        Verifies
        --------
        - ValueError is raised for invalid confidence levels

        Parameters
        ----------
        cl : float
            Invalid confidence level to test

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Confidence level must be between"):
            VaR(float_mu=0.0, float_sigma=0.1, float_cl=cl)

    @pytest.mark.parametrize("t", [0, -1])
    def test_invalid_time_horizon(self, t: int) -> None:
        """Test initialization with invalid time horizons.

        Verifies
        --------
        - ValueError is raised for non-positive time horizons

        Parameters
        ----------
        t : int
            Invalid time horizon to test

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Time horizon must be positive"):
            VaR(float_mu=0.0, float_sigma=0.1, int_t=t)

    def test_historic_var_calculation(self, var_instance: VaR) -> None:
        """Test historical VaR calculation.

        Verifies
        --------
        - Returns finite value
        - Value is within reasonable bounds

        Parameters
        ----------
        var_instance : VaR
            VaR instance from fixture

        Returns
        -------
        None
        """
        var = var_instance.historic_var()
        assert np.isfinite(var)
        assert -1 <= var <= 1

    def test_historic_var_without_returns(self) -> None:
        """Test historical VaR without returns raises ValueError.

        Verifies
        --------
        - ValueError is raised when no returns are provided

        Returns
        -------
        None
        """
        var = VaR(float_mu=0.0, float_sigma=0.1)
        with pytest.raises(ValueError, match="Historical returns not provided"):
            var.historic_var()

    @pytest.mark.parametrize("shock_type,shock", [
        ("relative", 0.1),
        ("absolute", 0.01)
    ])
    def test_historic_var_stress_test(
        self,
        var_instance: VaR,
        shock_type: Literal["absolute", "relative"],
        shock: float
    ) -> None:
        """Test stressed historical VaR calculation.

        Verifies
        --------
        - Returns finite value
        - Value is within reasonable bounds

        Parameters
        ----------
        var_instance : VaR
            VaR instance from fixture
        shock_type : Literal['absolute', 'relative']
            Type of shock to apply
        shock : float
            Shock magnitude

        Returns
        -------
        None
        """
        stressed_var = var_instance.historic_var_stress_test(shock, shock_type)
        assert np.isfinite(stressed_var)
        assert -1 <= stressed_var <= 1

    def test_historic_var_stress_test_invalid_type(
        self,
        var_instance: VaR
    ) -> None:
        """Test stressed VaR with invalid shock type raises ValueError.

        Verifies
        --------
        - ValueError is raised for invalid shock type

        Parameters
        ----------
        var_instance : VaR
            VaR instance from fixture

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be one of"):
            var_instance.historic_var_stress_test(0.1, "invalid_type")

    def test_parametric_var_calculation(self, var_instance: VaR) -> None:
        """Test parametric VaR calculation.

        Verifies
        --------
        - Returns finite value
        - Value is within reasonable bounds

        Parameters
        ----------
        var_instance : VaR
            VaR instance from fixture

        Returns
        -------
        None
        """
        var = var_instance.parametric_var()
        assert np.isfinite(var)
        assert -1 <= var <= 1

    def test_cvar_calculation(self, var_instance: VaR) -> None:
        """Test conditional VaR calculation.

        Verifies
        --------
        - Returns finite value
        - Value is within reasonable bounds

        Parameters
        ----------
        var_instance : VaR
            VaR instance from fixture

        Returns
        -------
        None
        """
        cvar = var_instance.cvar()
        assert np.isfinite(cvar)
        assert -1 <= cvar <= 1

    def test_monte_carlo_var_calculation(self, var_instance: VaR) -> None:
        """Test Monte Carlo VaR calculation.

        Verifies
        --------
        - Returns finite value
        - Value is within reasonable bounds

        Parameters
        ----------
        var_instance : VaR
            VaR instance from fixture

        Returns
        -------
        None
        """
        mc_var = var_instance.monte_carlo_var(int_simulations=1000)
        assert np.isfinite(mc_var)


# --------------------------
# RiskMeasures Tests
# --------------------------
class TestRiskMeasures:
    """Test cases for RiskMeasures class."""

    @pytest.fixture
    def risk_measures(self, sample_returns: NDArray[np.float64]) -> RiskMeasures:
        """Fixture providing initialized RiskMeasures instance.

        Returns
        -------
        RiskMeasures
            Initialized RiskMeasures instance

        Parameters
        ----------
        sample_returns : NDArray[np.float64]
            Sample returns array from fixture

        Returns
        -------
        RiskMeasures
        """
        return RiskMeasures(
            float_mu=0.001,
            float_sigma=0.02,
            array_r=sample_returns,
            float_cl=0.95,
            int_t=1
        )

    def test_drawdown_calculation(self, risk_measures: RiskMeasures) -> None:
        """Test maximum drawdown calculation.

        Verifies
        --------
        - Returns finite value
        - Value is within [0, 1] range (negative returns)

        Parameters
        ----------
        risk_measures : RiskMeasures
            RiskMeasures instance from fixture

        Returns
        -------
        None
        """
        drawdown = risk_measures.drawdown()
        assert np.isfinite(drawdown)
        assert drawdown <= 0

    def test_tracking_error_calculation(
        self,
        risk_measures: RiskMeasures,
        sample_returns: NDArray[np.float64]
    ) -> None:
        """Test tracking error calculation.

        Verifies
        --------
        - Returns finite value
        - Value is non-negative

        Parameters
        ----------
        risk_measures : RiskMeasures
            RiskMeasures instance from fixture
        sample_returns : NDArray[np.float64]
            Sample returns array from fixture

        Returns
        -------
        None
        """
        tracking_error = risk_measures.tracking_error(
            sample_returns,
            sample_returns * 0.9
        )
        assert np.isfinite(tracking_error)
        assert tracking_error >= 0

    def test_sharpe_calculation(self, risk_measures: RiskMeasures) -> None:
        """Test Sharpe ratio calculation.

        Verifies
        --------
        - Returns finite value

        Parameters
        ----------
        risk_measures : RiskMeasures
            RiskMeasures instance from fixture

        Returns
        -------
        None
        """
        sharpe = risk_measures.sharpe(0.01)
        assert np.isfinite(sharpe)

    def test_beta_calculation(
        self,
        risk_measures: RiskMeasures,
        sample_returns: NDArray[np.float64]
    ) -> None:
        """Test beta calculation.

        Verifies
        --------
        - Returns finite value

        Parameters
        ----------
        risk_measures : RiskMeasures
            RiskMeasures instance from fixture
        sample_returns : NDArray[np.float64]
            Sample returns array from fixture

        Returns
        -------
        None
        """
        beta = risk_measures.beta(sample_returns * 1.1)
        assert np.isfinite(beta)


# --------------------------
# QuoteVar Tests
# --------------------------
class TestQuoteVar:
    """Test cases for QuoteVar class."""

    def test_init_with_valid_params(self, sample_returns: NDArray[np.float64]) -> None:
        """Test initialization with valid parameters.

        Verifies
        --------
        - Instance is created without errors

        Parameters
        ----------
        sample_returns : NDArray[np.float64]
            Sample returns array from fixture

        Returns
        -------
        None
        """
        quote_var = QuoteVar(sample_returns)
        assert quote_var is not None

    @pytest.mark.parametrize("method", ["std", "ewma_std"])
    def test_different_std_methods(
        self,
        sample_returns: NDArray[np.float64],
        method: Literal["std", "ewma_std"]
    ) -> None:
        """Test initialization with different std methods.

        Verifies
        --------
        - Instance is created without errors for both methods

        Parameters
        ----------
        sample_returns : NDArray[np.float64]
            Sample returns array from fixture
        method : Literal['std', 'ewma_std']
            Standard deviation method to test

        Returns
        -------
        None
        """
        quote_var = QuoteVar(sample_returns, str_method_str=method)
        assert quote_var is not None

    def test_invalid_std_method(self, sample_returns: NDArray[np.float64]) -> None:
        """Test initialization with invalid std method raises ValueError.

        Verifies
        --------
        - ValueError is raised for invalid method

        Parameters
        ----------
        sample_returns : NDArray[np.float64]
            Sample returns array from fixture

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be one of"):
            QuoteVar(sample_returns, str_method_str="invalid_method")


# --------------------------
# PortfVar Tests
# --------------------------
class TestPortfVar:
    """Test cases for PortfVar class."""

    def test_init_with_valid_params(
        self,
        sample_returns: NDArray[np.float64],
        sample_weights: NDArray[np.float64]
    ) -> None:
        """Test initialization with valid parameters.

        Verifies
        --------
        - Instance is created without errors

        Parameters
        ----------
        sample_returns : NDArray[np.float64]
            Sample returns array from fixture
        sample_weights : NDArray[np.float64]
            Sample weights array from fixture

        Returns
        -------
        None
        """
        portf_var = PortfVar(sample_returns, sample_weights)
        assert portf_var is not None

    def test_mismatched_shapes_raises_error(
        self,
        sample_returns: NDArray[np.float64]
    ) -> None:
        """Test initialization with mismatched shapes raises ValueError.

        Verifies
        --------
        - ValueError is raised when shapes don't match

        Parameters
        ----------
        sample_returns : NDArray[np.float64]
            Sample returns array from fixture

        Returns
        -------
        None
        """
        invalid_weights = np.array([0.5, 0.5])
        with pytest.raises(ValueError, match="must match number of assets"):
            PortfVar(sample_returns, invalid_weights)