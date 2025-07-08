"""Unit tests for European options pricing models."""

import pytest

from stpstone.analytics.pricing.derivatives.european_options import (
    BlackScholesMerton,
    EuropeanOptions,
    Greeks,
    IterativeMethods,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def bsm_model() -> BlackScholesMerton:
    """Fixture providing BlackScholesMerton instance."""
    return BlackScholesMerton()


@pytest.fixture
def greeks_model() -> Greeks:
    """Fixture providing Greeks instance."""
    return Greeks()


@pytest.fixture
def iterative_model() -> IterativeMethods:
    """Fixture providing IterativeMethods instance."""
    return IterativeMethods()


@pytest.fixture
def euro_options_model() -> EuropeanOptions:
    """Fixture providing EuropeanOptions instance."""
    return EuropeanOptions()


@pytest.fixture
def standard_params() -> dict:
    """Standard parameters for option pricing."""
    return {
        "s": 100.0,    # spot price
        "k": 100.0,     # strike price
        "r": 0.05,      # risk-free rate
        "t": 1.0,       # time to maturity (years)
        "sigma": 0.2,   # volatility
        "q": 0.01,      # dividend yield
        "b": 0.05,      # cost of carry
        "opt_type": "call"  # option type
    }


# --------------------------
# BlackScholesMerton Tests
# --------------------------
class TestBlackScholesMerton:
    """Test cases for BlackScholesMerton class."""

    def test_d1_calculation(self, bsm_model: BlackScholesMerton, standard_params: dict):
        """Test d1 calculation with standard parameters."""
        s = standard_params["s"]
        k = standard_params["k"]
        b = standard_params["b"]
        t = standard_params["t"]
        sigma = standard_params["sigma"]
        q = standard_params["q"]
        
        expected = 0.35
        result = bsm_model.d1(s, k, b, t, sigma, q)
        assert pytest.approx(result, abs=1e-2) == expected

    def test_d2_calculation(self, bsm_model: BlackScholesMerton, standard_params: dict):
        """Test d2 calculation with standard parameters."""
        s = standard_params["s"]
        k = standard_params["k"]
        b = standard_params["b"]
        t = standard_params["t"]
        sigma = standard_params["sigma"]
        q = standard_params["q"]
        
        expected = 0.15
        result = bsm_model.d2(s, k, b, t, sigma, q)
        assert pytest.approx(result, abs=1e-2) == expected

    def test_call_price_calculation(self, bsm_model: BlackScholesMerton, standard_params: dict):
        """Test call price calculation with standard parameters."""
        params = standard_params.copy()
        expected = 9.925  # Expected value from external calculator
        result = bsm_model.general_opt_price(**params)
        assert pytest.approx(result, abs=1e-3) == expected

    def test_put_price_calculation(self, bsm_model: BlackScholesMerton, standard_params: dict):
        """Test put price calculation with standard parameters."""
        params = standard_params.copy()
        params["opt_type"] = "put"
        expected = 5.571  # Expected value from external calculator
        result = bsm_model.general_opt_price(**params)
        assert pytest.approx(result, abs=1e-3) == expected

    def test_invalid_option_type(self, bsm_model: BlackScholesMerton, standard_params: dict):
        """Test invalid option type raises exception."""
        params = standard_params.copy()
        params["opt_type"] = "invalid"
        with pytest.raises(Exception, match="Option ought be a call or a put"):
            bsm_model.general_opt_price(**params)

    def test_type_validation(self, bsm_model: BlackScholesMerton):
        """Test type validation for parameters."""
        with pytest.raises(Exception):
            bsm_model.general_opt_price("100", 100, 0.05, 1.0, 0.2, 0.01, 0.05, "call")

    def test_edge_case_zero_time(self, bsm_model: BlackScholesMerton, standard_params: dict):
        """Test edge case with zero time to maturity."""
        params = standard_params.copy()
        params["t"] = 0.0
        # At maturity, call price should be max(S-K, 0)
        expected = max(params["s"] - params["k"], 0)
        result = bsm_model.general_opt_price(**params)
        assert pytest.approx(result) == expected


# --------------------------
# Greeks Tests
# --------------------------
class TestGreeks:
    """Test cases for Greeks class."""

    def test_delta_call(self, greeks_model: Greeks, standard_params: dict):
        """Test delta calculation for call option."""
        params = standard_params.copy()
        expected = 0.6368
        result = greeks_model.delta(**params)
        assert pytest.approx(result, abs=1e-4) == expected

    def test_delta_put(self, greeks_model: Greeks, standard_params: dict):
        """Test delta calculation for put option."""
        params = standard_params.copy()
        params["opt_type"] = "put"
        expected = -0.3632
        result = greeks_model.delta(**params)
        assert pytest.approx(result, abs=1e-4) == expected

    def test_gamma(self, greeks_model: Greeks, standard_params: dict):
        """Test gamma calculation."""
        params = {k: v for k, v in standard_params.items() if k != "opt_type"}
        expected = 0.0188
        result = greeks_model.gamma(**params)
        assert pytest.approx(result, abs=1e-4) == expected

    def test_theta_call(self, greeks_model: Greeks, standard_params: dict):
        """Test theta calculation for call option."""
        params = standard_params.copy()
        expected = -4.570  # Expected value from external calculator
        result = greeks_model.theta(**params)
        assert pytest.approx(result, abs=1e-3) == expected

    def test_theta_put(self, greeks_model: Greeks, standard_params: dict):
        """Test theta calculation for put option."""
        params = standard_params.copy()
        params["opt_type"] = "put"
        expected = -1.561  # Expected value from external calculator
        result = greeks_model.theta(**params)
        assert pytest.approx(result, abs=1e-3) == expected

    def test_vega(self, greeks_model: Greeks, standard_params: dict):
        """Test vega calculation."""
        params = {k: v for k, v in standard_params.items() if k != "opt_type"}
        expected = 37.579  # Expected value from external calculator
        result = greeks_model.vega(**params)
        assert pytest.approx(result, abs=1e-3) == expected

    def test_rho_call(self, greeks_model: Greeks, standard_params: dict):
        """Test rho calculation for call option."""
        params = standard_params.copy()
        expected = 53.108  # Expected value from external calculator
        result = greeks_model.rho(**params)
        assert pytest.approx(result, abs=1e-3) == expected

    def test_rho_put(self, greeks_model: Greeks, standard_params: dict):
        """Test rho calculation for put option."""
        params = standard_params.copy()
        params["opt_type"] = "put"
        expected = -41.164  # Expected value from external calculator
        result = greeks_model.rho(**params)
        assert pytest.approx(result, abs=1e-3) == expected

    def test_greeks_edge_case_zero_volatility(self, greeks_model: Greeks, standard_params: dict):
        """Test edge case with zero volatility."""
        params = standard_params.copy()
        params["sigma"] = 0.0
        # With zero volatility, delta should be 1 for ITM call, 0 for OTM call
        if params["s"] > params["k"]:
            expected = 1.0
        else:
            expected = 0.0
        result = greeks_model.delta(**params)
        assert pytest.approx(result) == expected


# --------------------------
# IterativeMethods Tests
# --------------------------
class TestIterativeMethods:
    """Test cases for IterativeMethods class."""

    def test_binomial_pricing(self, iterative_model: IterativeMethods, standard_params: dict):
        """Test binomial pricing model with standard parameters."""
        params = standard_params.copy()
        n = 100  # number of steps
        u = 1.1  # up factor
        d = 0.9  # down factor
        
        # Calculate BSM price for comparison
        bsm_price = BlackScholesMerton().general_opt_price(**params)
        
        # Binomial price should approximate BSM price
        binomial_price = iterative_model.binomial_pricing_model(
            params["s"], params["k"], params["r"], params["t"], 
            n, u, d, params["opt_type"]
        )
        
        # Allow 1% difference due to discretization
        assert pytest.approx(binomial_price, rel=0.01) == bsm_price

    def test_crr_method(self, iterative_model: IterativeMethods, standard_params: dict):
        """Test CRR method with standard parameters."""
        params = standard_params.copy()
        n = 100  # number of steps
        
        # Calculate BSM price for comparison
        bsm_price = BlackScholesMerton().general_opt_price(**params)
        
        # CRR price should approximate BSM price
        crr_price = iterative_model.crr_method(
            params["s"], params["k"], params["r"], params["t"], 
            n, params["sigma"], params["opt_type"]
        )
        
        # Allow 1% difference due to discretization
        assert pytest.approx(crr_price, rel=0.01) == bsm_price

    def test_jr_method(self, iterative_model: IterativeMethods, standard_params: dict):
        """Test JR method with standard parameters."""
        params = standard_params.copy()
        n = 100  # number of steps
        
        # Calculate BSM price for comparison
        bsm_price = BlackScholesMerton().general_opt_price(**params)
        
        # JR price should approximate BSM price
        jr_price = iterative_model.jr_method(
            params["s"], params["k"], params["r"], params["t"], 
            n, params["sigma"], params["opt_type"]
        )
        
        # Allow 1% difference due to discretization
        assert pytest.approx(jr_price, rel=0.01) == bsm_price

    def test_edge_case_zero_steps(self, iterative_model: IterativeMethods, standard_params: dict):
        """Test edge case with zero steps."""
        params = standard_params.copy()
        n = 0  # zero steps
        
        with pytest.raises(ZeroDivisionError):
            iterative_model.crr_method(
                params["s"], params["k"], params["r"], params["t"], 
                n, params["sigma"], params["opt_type"]
            )


# --------------------------
# EuropeanOptions Tests
# --------------------------
class TestEuropeanOptions:
    """Test cases for EuropeanOptions class."""

    def test_implied_volatility_newton_raphson(self, euro_options_model: EuropeanOptions, standard_params: dict):
        """Test implied volatility calculation using Newton-Raphson method."""
        params = standard_params.copy()
        # Calculate option price first
        option_price = BlackScholesMerton().general_opt_price(**params)
        
        # Now calculate implied volatility from this price
        iv, _ = euro_options_model.implied_volatility(
            params["s"], params["k"], params["r"], params["t"], 
            params["sigma"], params["q"], params["b"], option_price, 
            params["opt_type"], method="newton_raphson"
        )
        
        # Should recover original volatility
        assert pytest.approx(iv, abs=1e-4) == params["sigma"]

    def test_implied_volatility_bisection(self, euro_options_model: EuropeanOptions, standard_params: dict):
        """Test implied volatility calculation using bisection method."""
        params = standard_params.copy()
        # Calculate option price first
        option_price = BlackScholesMerton().general_opt_price(**params)
        
        # Now calculate implied volatility from this price
        iv, _ = euro_options_model.implied_volatility(
            params["s"], params["k"], params["r"], params["t"], 
            params["sigma"], params["q"], params["b"], option_price, 
            params["opt_type"], method="bisection"
        )
        
        # Should recover original volatility
        assert pytest.approx(iv, abs=1e-4) == params["sigma"]

    def test_implied_volatility_fsolve(self, euro_options_model: EuropeanOptions, standard_params: dict):
        """Test implied volatility calculation using fsolve method."""
        params = standard_params.copy()
        # Calculate option price first
        option_price = BlackScholesMerton().general_opt_price(**params)
        
        # Now calculate implied volatility from this price
        iv, _ = euro_options_model.implied_volatility(
            params["s"], params["k"], params["r"], params["t"], 
            params["sigma"], params["q"], params["b"], option_price, 
            params["opt_type"], method="fsolve"
        )
        
        # Should recover original volatility
        assert pytest.approx(iv[0], abs=1e-4) == params["sigma"]

    def test_moneyness_calculation(self, euro_options_model: EuropeanOptions, standard_params: dict):
        """Test moneyness calculation."""
        params = standard_params.copy()
        expected = 0.25  # (d1 + d2)/2 for standard params
        result = euro_options_model.moneyness(
            params["s"], params["k"], params["r"], params["t"], 
            params["sigma"], params["q"]
        )
        assert pytest.approx(result, abs=1e-2) == expected

    def test_iaotm_classification(self, euro_options_model: EuropeanOptions, standard_params: dict):
        """Test ITM/ATM/OTM classification."""
        params = standard_params.copy()
        
        # ATM case
        params["k"] = 100.0
        assert euro_options_model.iaotm(**{k: v for k, v in params.items() if k != "opt_type"}) == "ATM"
        
        # ITM call case
        params["k"] = 90.0
        assert euro_options_model.iaotm(**{k: v for k, v in params.items() if k != "opt_type"}) == "ITM"
        
        # OTM call case
        params["k"] = 110.0
        assert euro_options_model.iaotm(**{k: v for k, v in params.items() if k != "opt_type"}) == "OTM"
        
        # Test put options
        params["opt_type"] = "put"
        
        # ITM put case
        params["k"] = 110.0
        assert euro_options_model.iaotm(**{k: v for k, v in params.items() if k != "opt_type"}) == "ITM"
        
        # OTM put case
        params["k"] = 90.0
        assert euro_options_model.iaotm(**{k: v for k, v in params.items() if k != "opt_type"}) == "OTM"

    def test_implied_volatility_invalid_method(self, euro_options_model: EuropeanOptions, standard_params: dict):
        """Test invalid method for implied volatility calculation."""
        params = standard_params.copy()
        option_price = BlackScholesMerton().general_opt_price(**params)
        
        with pytest.raises(Exception, match="Method to return the root of the non-linear equation is not recognized"):
            euro_options_model.implied_volatility(
                params["s"], params["k"], params["r"], params["t"], 
                params["sigma"], params["q"], params["b"], option_price, 
                params["opt_type"], method="invalid_method"
            )

    def test_implied_volatility_max_iter(self, euro_options_model: EuropeanOptions, standard_params: dict):
        """Test implied volatility with max iterations reached."""
        params = standard_params.copy()
        option_price = BlackScholesMerton().general_opt_price(**params)
        
        # Use very tight tolerance and low max_iter to force max_iter being hit
        iv, max_iter_hit = euro_options_model.implied_volatility(
            params["s"], params["k"], params["r"], params["t"], 
            params["sigma"], params["q"], params["b"], option_price, 
            params["opt_type"], tolerance=1e-10, max_iter=5
        )
        
        assert max_iter_hit is True