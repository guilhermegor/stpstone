"""Test module for American options pricing models."""

import numpy as np
import pytest

from stpstone.analytics.pricing.derivatives.american_options import PricingModels


@pytest.fixture
def pricing_model() -> PricingModels:
    """Fixture to provide a PricingModels instance for testing."""
    return PricingModels()


def test_set_parameters_valid(pricing_model: PricingModels) -> None:
    """Test normal parameter setting."""
    params = pricing_model.set_parameters(100.0, 105.0, 0.05, 1.0, 10, 1.1, 0.9, opt_style="call")
    assert len(params) == 7
    assert all(isinstance(x, (float, int)) for x in params)


def test_set_parameters_invalid_option_style(pricing_model: PricingModels) -> None:
    """Test invalid option style."""
    with pytest.raises(TypeError, match="must be one of"):
        pricing_model.set_parameters(100.0, 105.0, 0.05, 1.0, 10, 1.1, 0.9, opt_style="invalid")


def test_binomial_call(pricing_model: PricingModels) -> None:
    """Test normal binomial call option pricing."""
    price = pricing_model.binomial(100.0, 105.0, 0.05, 1.0, 10, 1.1, 0.9, "call")
    assert isinstance(price, float)
    assert price > 0


def test_binomial_put(pricing_model: PricingModels) -> None:
    """Test normal binomial put option pricing."""
    price = pricing_model.binomial(100.0, 105.0, 0.05, 1.0, 10, 1.1, 0.9, "put")
    assert isinstance(price, float)
    assert price > 0


def test_binomial_barriers(pricing_model: PricingModels) -> None:
    """Test binomial with barriers."""
    price = pricing_model.binomial(
        100.0, 105.0, 0.05, 1.0, 10, 1.1, 0.9, "call", h_upper=150.0, h_lower=80.0
    )
    assert isinstance(price, float)


def test_binomial_edge_zero_steps(pricing_model: PricingModels) -> None:
    """Test edge case with zero time steps."""
    with pytest.raises(ZeroDivisionError):
        pricing_model.binomial(100.0, 105.0, 0.05, 1.0, 0, 1.1, 0.9, "call")


def test_binomial_edge_zero_time(pricing_model: PricingModels) -> None:
    """Test edge case with zero time to maturity."""
    price = pricing_model.binomial(100.0, 105.0, 0.05, 0.0, 10, 1.1, 0.9, "call")
    assert price == max(100.0 - 105.0, 0)  # should equal intrinsic value


def test_binomial_edge_high_volatility(pricing_model: PricingModels) -> None:
    """Test edge case with high volatility."""
    price = pricing_model.binomial(100.0, 105.0, 0.05, 1.0, 10, 2.0, 0.5, "call")
    assert price > 0


def test_binomial_type_validation(pricing_model: PricingModels) -> None:
    """Test type validation."""
    with pytest.raises(TypeError, match="must be of type float"):
        pricing_model.binomial("invalid", 105.0, 0.05, 1.0, 10, 1.1, 0.9, "call")


def test_binomial_invalid_barriers(pricing_model: PricingModels) -> None:
    """Test invalid barrier conditions."""
    with pytest.raises(ValueError):
        pricing_model.binomial(
            100.0, 105.0, 0.05, 1.0, 10, 1.1, 0.9, "call", h_upper=50.0, h_lower=150.0
        )


def test_binomial_negative_values(pricing_model: PricingModels) -> None:
    """Test negative input values."""
    with pytest.raises(ValueError):
        pricing_model.binomial(-100.0, 105.0, 0.05, 1.0, 10, 1.1, 0.9, "call")


def test_binomial_large_steps(pricing_model: PricingModels) -> None:
    """Test with large number of steps."""
    price = pricing_model.binomial(100.0, 105.0, 0.05, 1.0, 1000, 1.1, 0.9, "call")
    assert isinstance(price, float)
    assert price > 0


def test_binomial_put_call_parity(pricing_model: PricingModels) -> None:
    """Test put-call parity approximation."""
    call_price = pricing_model.binomial(100.0, 100.0, 0.05, 1.0, 100, 1.1, 0.9, "call")
    put_price = pricing_model.binomial(100.0, 100.0, 0.05, 1.0, 100, 1.1, 0.9, "put")
    # put-call parity: C - P ≈ S - K*exp(-rT)
    parity_diff = call_price - put_price
    expected_diff = 100.0 - 100.0 * np.exp(-0.05 * 1.0)
    assert abs(parity_diff - expected_diff) < 1.0