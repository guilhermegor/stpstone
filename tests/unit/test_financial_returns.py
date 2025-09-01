"""Unit tests for FinancialReturns class.

Tests the stock return calculations and performance metrics functionality with various input 
scenarios.
"""

import numpy as np
import pandas as pd
import pytest

from stpstone.analytics.perf_metrics.financial_returns import FinancialReturns


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def financial_returns() -> FinancialReturns:
    """Fixture providing a FinancialReturns instance.
    
    Returns
    -------
    FinancialReturns
        An instance of FinancialReturns.
    """
    return FinancialReturns()


@pytest.fixture
def sample_prices() -> list[float]:
    """Fixture providing sample price data.
    
    Returns
    -------
    list[float]
        A list of sample prices.
    """
    return [100.0, 110.0, 105.0, 120.0]


@pytest.fixture
def sample_price_df() -> pd.DataFrame:
    """Fixture providing sample price DataFrame.
    
    Returns
    -------
    pd.DataFrame
        A DataFrame containing sample price data.
    """
    return pd.DataFrame({
        'date': pd.date_range('2023-01-01', periods=4),
        'price': [100.0, 110.0, 105.0, 120.0]
    })


# --------------------------
# Tests for continuous_return
# --------------------------
def test_continuous_return_valid_inputs(financial_returns: FinancialReturns) -> None:
    """Test continuous_return with valid numeric inputs.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    result = financial_returns.continuous_return(100, 110)
    assert np.isclose(result, 0.09531017980432493)


def test_continuous_return_string_inputs(financial_returns: FinancialReturns) -> None:
    """Test continuous_return with string inputs that can be converted to float.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.
    
    Returns
    -------
    None
    """
    result = financial_returns.continuous_return(100, 110)
    assert np.isclose(result, 0.09531017980432493)


def test_continuous_return_same_price(financial_returns: FinancialReturns) -> None:
    """Test continuous_return when prices are equal.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    result = financial_returns.continuous_return(100, 100)
    assert result == 0.0


def test_continuous_return_invalid_string_input(financial_returns: FinancialReturns) -> None:
    """Test continuous_return raises ValueError with non-numeric strings.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be one of"):
        financial_returns.continuous_return("abc", 100)


def test_continuous_return_zero_initial_price(financial_returns: FinancialReturns) -> None:
    """Test continuous_return raises ValueError when initial price is zero.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Initial price cannot be zero"):
        financial_returns.continuous_return(0, 100)


def test_continuous_return_negative_prices(financial_returns: FinancialReturns) -> None:
    """Test continuous_return raises ValueError with negative prices.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Price ratio must be greater than zero"):
        financial_returns.continuous_return(-100, 100)


# --------------------------
# Tests for discrete_return
# --------------------------
def test_discrete_return_valid_inputs(financial_returns: FinancialReturns) -> None:
    """Test discrete_return with valid numeric inputs.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    result = financial_returns.discrete_return(100, 110)
    assert pytest.approx(result, abs=1e-4) == pytest.approx(0.1, abs=1e-4)


def test_discrete_return_string_inputs(financial_returns: FinancialReturns) -> None:
    """Test discrete_return with string inputs that can be converted to float.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    result = financial_returns.discrete_return(100, 110)
    assert pytest.approx(result, abs=1e-4) == pytest.approx(0.1, abs=1e-4)


def test_discrete_return_same_price(financial_returns: FinancialReturns) -> None:
    """Test discrete_return when prices are equal.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    result = financial_returns.discrete_return(100, 100)
    assert result == 0.0


def test_discrete_return_invalid_string_input(financial_returns: FinancialReturns) -> None:
    """Test discrete_return raises ValueError with non-numeric strings.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be one of"):
        financial_returns.discrete_return("abc", 100)


def test_discrete_return_zero_initial_price(financial_returns: FinancialReturns) -> None:
    """Test discrete_return raises ValueError when initial price is zero.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Initial price cannot be zero"):
        financial_returns.discrete_return(0, 100)


# --------------------------
# Tests for calc_returns_from_prices
# --------------------------
def test_calc_returns_from_prices_ln_return(
    financial_returns: FinancialReturns, 
    sample_prices: list[float]
) -> None:
    """Test calc_returns_from_prices with ln_return type.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.
    sample_prices : list[float]
        A list of stock prices.

    Returns
    -------
    None
    """
    result = financial_returns.calc_returns_from_prices(sample_prices, "ln_return")
    print(f"result test_calc_returns_from_prices_ln_return: {result}")
    expected = [
        0.09531018,
        -0.04652002,
        0.13353139
    ]
    assert np.allclose(result, expected, atol=1e-4)


def test_calc_returns_from_prices_stnd_return(
    financial_returns: FinancialReturns,
    sample_prices: list[float]
) -> None:
    """Test calc_returns_from_prices with stnd_return type.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.
    sample_prices : list[float]
        A list of stock prices.

    Returns
    -------
    None
    """
    result = financial_returns.calc_returns_from_prices(sample_prices, "stnd_return")
    expected = [0.1, -0.045454545, 0.142857143]
    assert np.allclose(result, expected, rtol=1e-6)


def test_calc_returns_from_prices_numpy_array(
    financial_returns: FinancialReturns,
    sample_prices: list[float]
) -> None:
    """Test calc_returns_from_prices with numpy array input.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.
    sample_prices : list[float]
        A list of stock prices.

    Returns
    -------
    None
    """
    arr = np.array(sample_prices, dtype=np.float64)
    result = financial_returns.calc_returns_from_prices(arr, "ln_return")
    assert isinstance(result, np.ndarray)


def test_calc_returns_from_prices_single_price(
    financial_returns: FinancialReturns
) -> None:
    """Test calc_returns_from_prices with single price (should return empty list).
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    result = financial_returns.calc_returns_from_prices([100.0], "ln_return")
    assert result.size == 0


def test_calc_returns_from_prices_invalid_return_type(
    financial_returns: FinancialReturns,
    sample_prices: list[float]
) -> None:
    """Test calc_returns_from_prices raises ValueError with invalid return type.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.
    sample_prices : list[float]
        A list of stock prices.

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be one of"):
        financial_returns.calc_returns_from_prices(sample_prices, "invalid_type")


# --------------------------
# Tests for pandas_returns_from_spot_prices
# --------------------------
def test_pandas_returns_from_spot_prices_ln_return(
    financial_returns: FinancialReturns,
    sample_price_df: pd.DataFrame
) -> None:
    """Test pandas_returns_from_spot_prices with ln_return type.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.
    sample_price_df : pd.DataFrame
        A DataFrame containing price data.

    Returns
    -------
    None
    """
    result = financial_returns.pandas_returns_from_spot_prices(
        sample_price_df, 'price', 'date', type_return="ln_return"
    )
    assert "returns" in result.columns
    assert np.isclose(result['returns'].iloc[1], np.log(110/100))
    assert result['returns'].iloc[0] == 0


def test_pandas_returns_from_spot_prices_stnd_return(
    financial_returns: FinancialReturns,
    sample_price_df: pd.DataFrame
) -> None:
    """Test pandas_returns_from_spot_prices with stnd_return type.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.
    sample_price_df : pd.DataFrame
        A DataFrame containing price data.

    Returns
    -------
    None
    """
    result = financial_returns.pandas_returns_from_spot_prices(
        sample_price_df, 'price', 'date', type_return="stnd_return"
    )
    assert "returns" in result.columns
    assert np.isclose(result['returns'].iloc[1], 0.1)
    assert result['returns'].iloc[0] == 0


def test_pandas_returns_from_spot_prices_custom_column_names(
    financial_returns: FinancialReturns,
    sample_price_df: pd.DataFrame
) -> None:
    """Test pandas_returns_from_spot_prices with custom column names.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.
    sample_price_df : pd.DataFrame
        A DataFrame containing price data.

    Returns
    -------
    None
    """
    result = financial_returns.pandas_returns_from_spot_prices(
        sample_price_df,
        col_prices='price',
        col_dt_date='date',
        col_lag_close='custom_lag',
        col_first_occurrence_ticker='custom_first',
        col_stock_returns='custom_returns'
    )
    assert "custom_lag" in result.columns
    assert "custom_first" in result.columns
    assert "custom_returns" in result.columns


def test_pandas_returns_from_spot_prices_missing_columns(
    financial_returns: FinancialReturns,
    sample_price_df: pd.DataFrame
) -> None:
    """Test pandas_returns_from_spot_prices raises KeyError with missing columns.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.
    sample_price_df : pd.DataFrame
        A DataFrame containing price data.

    Returns
    -------
    None
    """
    with pytest.raises(KeyError):
        financial_returns.pandas_returns_from_spot_prices(
            sample_price_df, 'missing_price', 'date'
        )


# --------------------------
# Tests for short_fee_cost
# --------------------------
def test_short_fee_cost_valid_inputs(financial_returns: FinancialReturns) -> None:
    """Test short_fee_cost with valid inputs.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    result = financial_returns.short_fee_cost(0.05, 30, 100, 10)
    assert np.isclose(result, 4.1666667, atol=1e-4)


def test_short_fee_cost_zero_fee(financial_returns: FinancialReturns) -> None:
    """Test short_fee_cost with zero fee.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    result = financial_returns.short_fee_cost(0, 30, 100, 10)
    assert result == 0


def test_short_fee_cost_zero_days(financial_returns: FinancialReturns) -> None:
    """Test short_fee_cost with zero days.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    result = financial_returns.short_fee_cost(0.05, 0, 100, 10)
    assert result == 0


def test_short_fee_cost_negative_inputs(financial_returns: FinancialReturns) -> None:
    """Test short_fee_cost raises ValueError with negative inputs.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    with pytest.raises(ValueError):
        financial_returns.short_fee_cost(-0.05, 30, 100, 10)


# --------------------------
# Tests for pricing_strategy
# --------------------------
def test_pricing_strategy_ln_return(financial_returns: FinancialReturns) -> None:
    """Test pricing_strategy with ln_return type.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    result = financial_returns.pricing_strategy(100, 110, 2, type_return="ln_return")
    assert isinstance(result, dict)
    assert np.isclose(result['mtm'], 20.0)
    assert np.isclose(result['pct_return'], 0.09531017980432493)
    assert result['notional'] == 110.0


def test_pricing_strategy_stnd_return(financial_returns: FinancialReturns) -> None:
    """Test pricing_strategy with stnd_return type.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    result = financial_returns.pricing_strategy(100, 110, 2, type_return="stnd_return")
    assert isinstance(result, dict)
    assert np.isclose(result['mtm'], 20.0, atol=1e-4)
    assert np.isclose(result['pct_return'], 0.100000, atol=1e-4)
    assert pytest.approx(result['notional'], abs=1e-4) == pytest.approx(110.0, abs=1e-4)


def test_pricing_strategy_with_operational_costs(financial_returns: FinancialReturns) -> None:
    """Test pricing_strategy with operational costs.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    result = financial_returns.pricing_strategy(100, 110, 2, 5, "ln_return")
    assert np.isclose(result['mtm'], 15.0)


def test_pricing_strategy_invalid_return_type(financial_returns: FinancialReturns) -> None:
    """Test pricing_strategy raises ValueError with invalid return type.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be one of"):
        financial_returns.pricing_strategy(100, 110, 2, type_return="invalid_type")


def test_pricing_strategy_string_inputs(financial_returns: FinancialReturns) -> None:
    """Test pricing_strategy with string inputs that can be converted to float.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    result = financial_returns.pricing_strategy(100, 110, 2, type_return="ln_return")
    assert isinstance(result, dict)
    assert np.isclose(result['mtm'], 20.0)


# --------------------------
# Type Validation Tests
# --------------------------
def test_type_checker_enforcement() -> None:
    """Test that TypeChecker metaclass enforces type hints.
    
    Returns
    -------
    None
    """
    # this would normally be caught by static type checkers
    # we'll verify that invalid types raise errors at runtime
    fr = FinancialReturns()
    
    # test with invalid types that can't be converted to float
    with pytest.raises(TypeError):
        fr.continuous_return([], 100)  # type: ignore
    
    with pytest.raises(TypeError):
        fr.discrete_return({}, 100)  # type: ignore


def test_return_type_annotations(financial_returns: FinancialReturns) -> None:
    """Test that methods return the correct types.
    
    Parameters
    ----------
    financial_returns : FinancialReturns
        An instance of FinancialReturns.

    Returns
    -------
    None
    """
    result = financial_returns.continuous_return(100, 110)
    assert isinstance(result, float)
    
    result = financial_returns.discrete_return(100, 110)
    assert isinstance(result, float)
    
    result = financial_returns.calc_returns_from_prices([100.0, 110.0], "ln_return")
    assert isinstance(result, np.ndarray)
    
    df_ = pd.DataFrame({'date': ['2023-01-01', '2023-01-02'], 'price': [100, 110]})
    result = financial_returns.pandas_returns_from_spot_prices(df_, 'price', 'date')
    assert isinstance(result, pd.DataFrame)
    
    result = financial_returns.short_fee_cost(0.05, 30, 100, 10)
    assert isinstance(result, float)
    
    result = financial_returns.pricing_strategy(100, 110, 2)
    assert isinstance(result, dict)
    assert all(isinstance(v, float) for v in result.values())