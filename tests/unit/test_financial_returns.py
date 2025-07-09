"""Test module for FinancialReturns class."""

import numpy as np
import pandas as pd
import pytest

from stpstone.analytics.perf_metrics.financial_returns import FinancialReturns


@pytest.fixture
def returns_calculator() -> FinancialReturns:
    """Fixture to provide an instance of FinancialReturns for testing."""
    return FinancialReturns()

def test_continuous_return_normal(returns_calculator: FinancialReturns) -> None:
    """Test continuous_return with normal inputs."""
    assert pytest.approx(returns_calculator.continuous_return(100.0, 110.0)) \
        == 0.09531017980432493
    assert pytest.approx(returns_calculator.continuous_return(50.0, 75.0)) == np.log(1.5)
    assert pytest.approx(returns_calculator.continuous_return(100.0, 100.0)) == 0.0

def test_continuous_return_string_input(returns_calculator: FinancialReturns) -> None:
    """Test continuous_return with string inputs that can be converted to float."""
    with pytest.raises(TypeError, match="must be of type"):
        pytest.approx(returns_calculator.continuous_return("100", "110"))
        pytest.approx(returns_calculator.continuous_return("50.5", "75.75"))

def test_continuous_return_edge_cases(returns_calculator: FinancialReturns) -> None:
    """Test edge cases for continuous_return."""
    # very small price change
    assert pytest.approx(returns_calculator.continuous_return(100.0, 100.0001), abs=1e-4) \
        == pytest.approx(9.99950003e-07, abs=1e-4)
    # large price change
    assert pytest.approx(returns_calculator.continuous_return(10.0, 1000.0)) == np.log(100)
    # price drop to very low value
    assert pytest.approx(returns_calculator.continuous_return(100.0, 0.01)) == np.log(0.0001)

def test_continuous_return_error_conditions(returns_calculator: FinancialReturns) -> None:
    """Test error conditions for continuous_return."""
    with pytest.raises(ZeroDivisionError):
        returns_calculator.continuous_return(0.0, 100.0)  # division by zero
    with pytest.raises(TypeError, match="must be of type"):
        returns_calculator.continuous_return("invalid", 100.0)  # invalid string

def test_discrete_return_normal(returns_calculator: FinancialReturns) -> None:
    """Test discrete_return with normal inputs."""
    assert pytest.approx(returns_calculator.discrete_return(100.0, 110.0), abs=1e-4) \
        == pytest.approx(0.1, abs=1e-4)
    assert pytest.approx(returns_calculator.discrete_return(50.0, 75.0), abs=1e-4) \
        == pytest.approx(0.5, abs=1e-4)
    assert pytest.approx(returns_calculator.discrete_return(100.0, 100.0), abs=1e-4) \
        == pytest.approx(0.0, abs=1e-4)

def test_discrete_return_string_input(returns_calculator: FinancialReturns) -> None:
    """Test discrete_return with string inputs that can be converted to float."""
    with pytest.raises(TypeError, match="must be of type"):
        returns_calculator.discrete_return("100", "110")
        returns_calculator.discrete_return("50.5", "75.75")

def test_discrete_return_edge_cases(returns_calculator: FinancialReturns) -> None:
    """Test edge cases for discrete_return."""
    # very small price change
    assert pytest.approx(returns_calculator.discrete_return(100.0, 100.0001), abs=1e-4) \
        == pytest.approx(0.000001, abs=1e-4)
    # large price change
    assert pytest.approx(returns_calculator.discrete_return(10.0, 1000.0), abs=1e-4) \
        == pytest.approx(99.0, abs=1e-4)
    # price drop to very low value
    assert pytest.approx(returns_calculator.discrete_return(100.0, 0.01), abs=1e-4) \
        == pytest.approx(-0.9999, abs=1e-4)

def test_discrete_return_error_conditions(returns_calculator: FinancialReturns) -> None:
    """Test error conditions for discrete_return."""
    with pytest.raises(ZeroDivisionError):
        returns_calculator.discrete_return(0.0, 100.0)  # division by zero
    with pytest.raises(TypeError, match="must be of type"):
        returns_calculator.discrete_return("invalid", 100.0)  # invalid string

def test_calc_returns_from_prices_ln(returns_calculator: FinancialReturns) -> None:
    """Test calc_returns_from_prices with log returns."""
    prices = [100.0, 110.0, 105.0, 120.0]
    expected = [np.log(110/100), np.log(105.0/110.0), np.log(120.0/105.0)]
    result = returns_calculator.calc_returns_from_prices(prices, "ln_return")
    assert all(pytest.approx(a) == b for a, b in zip(expected, result))

def test_calc_returns_from_prices_stnd(returns_calculator: FinancialReturns) -> None:
    """Test calc_returns_from_prices with standard returns."""
    prices = [100.0, 110.0, 105.0, 120.0]
    expected = [0.1, -0.045454545, 0.142857143]
    result = returns_calculator.calc_returns_from_prices(prices, "stnd_return")
    assert all(pytest.approx(a, rel=1e-6) == b for a, b in zip(expected, result))

def test_calc_returns_from_prices_invalid_type(returns_calculator: FinancialReturns) -> None:
    """Test calc_returns_from_prices with invalid return type."""
    with pytest.raises(TypeError):
        returns_calculator.calc_returns_from_prices([100.0, 110.0], "invalid_type")

def test_pandas_returns_from_spot_prices_ln(returns_calculator: FinancialReturns) -> None:
    """Test pandas_returns_from_spot_prices with log returns."""
    df_ = pd.DataFrame({
        'date': pd.date_range('2023-01-01', periods=3),
        'price': [100.0, 110.0, 105.0]
    })
    result = returns_calculator.pandas_returns_from_spot_prices(
        df_, 'price', 'date', type_return='ln_return'
    )
    assert 'returns' in result.columns
    assert pytest.approx(result['returns'].iloc[1], abs=1e-4) \
        == pytest.approx(np.log(110.0/100.0), abs=1e-4)
    assert result['returns'].iloc[0] == 0  # first occurrence should be 0

def test_pandas_returns_from_spot_prices_stnd(returns_calculator: FinancialReturns) -> None:
    """Test pandas_returns_from_spot_prices with standard returns."""
    df_ = pd.DataFrame({
        'date': pd.date_range('2023-01-01', periods=3),
        'price': [100.0, 110.0, 105.0]
    })
    result = returns_calculator.pandas_returns_from_spot_prices(
        df_, 'price', 'date', type_return='stnd_return'
    )
    assert 'returns' in result.columns
    assert pytest.approx(result['returns'].iloc[1]) == 110.0/100.0 - 1.0
    assert result['returns'].iloc[0] == 0.0  # first occurrence should be 0

def test_pandas_returns_from_spot_prices_empty_df(returns_calculator: FinancialReturns) -> None:
    """Test with empty DataFrame."""
    df_ = pd.DataFrame(columns=['date', 'price'])
    result = returns_calculator.pandas_returns_from_spot_prices(df_, 'price', 'date')
    assert 'returns' in result.columns
    assert len(result) == 0

def test_short_fee_cost_normal(returns_calculator: FinancialReturns) -> None:
    """Test short_fee_cost with normal inputs."""
    # 5% fee for 30 days on $1000 position (100 shares at $10)
    result = returns_calculator.short_fee_cost(0.05, 30, 10.0, 100.0)
    expected = 4.0741237836483535
    assert pytest.approx(result, abs=1e-4) == pytest.approx(expected, abs=1e-4)

def test_short_fee_cost_edge_cases(returns_calculator: FinancialReturns) -> None:
    """Test edge cases for short_fee_cost."""
    # zero fee
    assert returns_calculator.short_fee_cost(0.0, 30, 10.0, 100.0) == 0
    # zero days
    assert returns_calculator.short_fee_cost(0.05, 0, 10.0, 100.0) == 0
    # very high fee
    assert returns_calculator.short_fee_cost(1.0, 360, 10.0, 100.0) > 0

def test_pricing_strategy_ln(returns_calculator: FinancialReturns) -> None:
    """Test pricing_strategy with log returns."""
    result = returns_calculator.pricing_strategy(100.0, 110.0, 2.0, type_return='ln_return')
    assert pytest.approx(result['mtm']) == (110.0 - 100.0) * 2.0
    assert pytest.approx(result['pct_return']) == np.log(100.0/110.0)
    assert result['notional'] == 110.0

def test_pricing_strategy_stnd(returns_calculator: FinancialReturns) -> None:
    """Test pricing_strategy with standard returns."""
    result = returns_calculator.pricing_strategy(100.0, 110.0, 2.0, type_return='stnd_return')
    assert pytest.approx(result['mtm']) == (110.0 - 100.0) * 2.0
    assert pytest.approx(result['pct_return']) == 100.0/110.0 - 1.0
    assert result['notional'] == 110.0

def test_pricing_strategy_operational_costs(returns_calculator: FinancialReturns) -> None:
    """Test pricing_strategy with operational costs."""
    result = returns_calculator.pricing_strategy(100.0, 110.0, 2.0, operational_costs=5.0)
    assert pytest.approx(result['mtm'], abs=1e-4) \
        == pytest.approx((110.0 - 100.0) * 2.0 - 5.0, abs=1e-4)

def test_pricing_strategy_invalid_type(returns_calculator: FinancialReturns) -> None:
    """Test pricing_strategy with invalid return type."""
    with pytest.raises(TypeError):
        returns_calculator.pricing_strategy(100.0, 110.0, 2.0, type_return='invalid_type')

def test_type_validation(returns_calculator: FinancialReturns) -> None:
    """Test type validation for all methods."""
    # Test with invalid types that can't be converted to float
    with pytest.raises((ValueError, TypeError)):
        returns_calculator.continuous_return("abc", 100.0)
    with pytest.raises((ValueError, TypeError)):
        returns_calculator.discrete_return(None, 100.0)
    
    # Test with invalid DataFrame input
    with pytest.raises((ValueError, TypeError)):
        returns_calculator.pandas_returns_from_spot_prices("not a dataframe", 'price', 'date')
    
    # Test with invalid list input
    with pytest.raises((ValueError, TypeError)):
        returns_calculator.calc_returns_from_prices("not a list", "ln_return")