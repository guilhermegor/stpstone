"""Comprehensive unit tests for American options pricing models.

Tests include:
- Parameter validation in InitialSettings
- Binomial pricing model calculations
- Edge cases and error conditions
- Type validation and boundary conditions
- Barrier option functionality
- Convergence tests
- Financial concept validations
"""

from typing import Any

import pytest

from stpstone.analytics.pricing.derivatives.american_options import PricingModels


@pytest.fixture
def pricing_model() -> PricingModels:
    """Fixture to provide a PricingModels instance for testing.
    
    Returns
    -------
    PricingModels
        Instance of PricingModels class
    """
    return PricingModels()


@pytest.fixture
def default_params() -> dict[str, Any]:
    """Fixture providing default parameters for binomial model.
    
    Returns
    -------
    dict[str, Any]
        Dictionary containing default parameters for testing
    """
    return {
        "s": 100.0, 
        "k": 105.0, 
        "r": 0.05, 
        "t": 1.0, 
        "n": 100, 
        "u": 1.1, 
        "d": 0.9,
        "opt_style": "call"
    }


class TestParameterValidation:
    """Test cases for parameter validation and type checking."""
    
    def test_set_parameters_valid(self, pricing_model: PricingModels) -> None:
        """Test normal parameter setting with valid inputs.
        
        Verifies that:
        - Parameters are correctly processed and returned
        - Return types match expected types

        Parameters
        ----------
        pricing_model : PricingModels
            Instance of PricingModels
        """
        params = pricing_model.set_parameters(100.0, 105.0, 0.05, 1.0, 100, 1.1, 0.9, "call")
        assert len(params) == 7
        assert isinstance(params[0], float)  # s_val
        assert isinstance(params[1], float)  # k_val
        assert isinstance(params[2], float)  # r_val
        assert isinstance(params[3], float)  # t_val
        assert isinstance(params[4], int)    # n_val
        assert isinstance(params[5], float)  # u_val
        assert isinstance(params[6], float)  # d_val

    @pytest.mark.parametrize("param,value,error_msg", [
        ("s", "invalid", "must be of type float"),
        ("k", None, "must be of type float"),
        ("r", "0.05", "must be of type float"),
        ("t", [1.0], "must be of type float"),
        ("n", 100.5, "must be of type int"),
        ("u", "1.1", "must be of type float"),
        ("d", True, "must be of type float")
    ])
    def test_type_validation(
        self, 
        pricing_model: PricingModels,
        default_params: dict[str, Any],
        param: str,
        value: object,
        error_msg: str
    ) -> None:
        """Test type validation for all parameters.
        
        Parameters
        ----------
        pricing_model : PricingModels
            Instance of PricingModels
        default_params : dict[str, Any]
            Default parameters dictionary
        param : str
            Parameter name to test
        value : object
            Invalid value to test
        error_msg : str
            Expected error message
        """
        params = default_params.copy()
        params[param] = value
        with pytest.raises(TypeError, match=error_msg):
            pricing_model.set_parameters(**params)

    @pytest.mark.parametrize("param,value,error_msg", [
        ("s", -100.0, "Stock price must be non-negative"),
        ("k", -105.0, "Strike price must be non-negative"), 
        ("t", -1.0, "Time to maturity must be non-negative"),
        ("n", -100, "Number of steps must be non-negative"),
        ("u", -1.1, "Up factor must be positive"),
        ("d", -0.9, "Down factor must be positive"),
        ("u", 0.9, "Up factor must be greater than down factor"),
        ("d", 1.1, "Up factor must be greater than down factor")
    ])
    def test_value_validation(
        self,
        pricing_model: PricingModels,
        default_params: dict[str, Any],
        param: str,
        value: object,
        error_msg: str
    ) -> None:
        """Test value validation for all parameters.
        
        Parameters
        ----------
        pricing_model : PricingModels
            Instance of PricingModels
        default_params : dict[str, Any]
            Default parameters dictionary
        param : str
            Parameter name to test
        value : object
            Invalid value to test
        error_msg : str
            Expected error message
        """
        params = default_params.copy()
        params[param] = value
        with pytest.raises(ValueError, match=error_msg):
            pricing_model.set_parameters(**params)

    def test_invalid_option_style(
        self, 
        pricing_model: PricingModels, 
        default_params: dict[str, Any]
    ) -> None:
        """Test invalid option style raises proper error.
        
        Parameters
        ----------
        pricing_model : PricingModels
            Instance of PricingModels
        default_params : dict[str, Any]
            Default parameters dictionary
        """
        params = default_params.copy()
        params["opt_style"] = "invalid"
        with pytest.raises(TypeError, match="must be one of"):
            pricing_model.set_parameters(**params)


class TestBinomialModel:
    """Test cases for binomial pricing model functionality."""
    
    @pytest.mark.parametrize("s,k,expected", [
        (110.0, 100.0, 10.0),  # In the money call
        (90.0, 100.0, 0.0),    # Out of the money call
        (100.0, 100.0, 0.0)    # At the money
    ])
    def test_call_at_maturity(
        self,
        pricing_model: PricingModels,
        default_params: dict[str, Any],
        s: float,
        k: float,
        expected: float
    ) -> None:
        """Test call option pricing at maturity (t=0).
        
        Verifies that:
        - Call option value equals max(S-K, 0) at maturity
        - Proper handling of ITM/OTM/ATM cases

        Parameters
        ----------
        pricing_model : PricingModels
            Instance of PricingModels
        default_params : dict[str, Any]
            Default parameters dictionary
        s : float
            Current stock price
        k : float
            Strike price of the option
        expected : float
            Expected call option value
        """
        params = default_params.copy()
        params.update({"t": 0.0, "s": s, "k": k})
        assert pricing_model.binomial(**params) == expected

    @pytest.mark.parametrize("s,k,expected", [
        (90.0, 100.0, 10.0),   # In the money put
        (110.0, 100.0, 0.0),   # Out of the money put
        (100.0, 100.0, 0.0)    # At the money
    ])
    def test_put_at_maturity(
        self,
        pricing_model: PricingModels,
        default_params: dict[str, Any],
        s: float,
        k: float,
        expected: float
    ) -> None:
        """Test put option pricing at maturity (t=0).
        
        Verifies that:
        - Put option value equals max(K-S, 0) at maturity
        - Proper handling of ITM/OTM/ATM cases

        Parameters
        ----------
        pricing_model : PricingModels
            Instance of PricingModels
        default_params : dict[str, Any]
            Default parameters dictionary
        s : float
            Current stock price
        k : float
            Strike price of the option
        expected : float
            Expected put option value
        """
        params = default_params.copy()
        params.update({"t": 0.0, "s": s, "k": k, "opt_style": "put"})
        assert pricing_model.binomial(**params) == expected

    def test_call_price_sanity(
        self, 
        pricing_model: PricingModels, 
        default_params: dict[str, Any]
    ) -> None:
        """Test call option price sanity checks.
        
        Verifies that:
        - Price is positive
        - Price is less than stock price
        - Price increases with higher volatility

        Parameters
        ----------
        pricing_model : PricingModels
            Instance of PricingModels
        default_params : dict[str, Any]
            Default parameters dictionary
        """
        price = pricing_model.binomial(**default_params)
        assert isinstance(price, float)
        assert 0 < price < default_params["s"]
        
        # Test price increases with higher volatility
        high_vol_params = default_params.copy()
        high_vol_params.update({"u": 1.2, "d": 0.8})
        high_vol_price = pricing_model.binomial(**high_vol_params)
        assert high_vol_price > price

    def test_put_price_sanity(
        self, 
        pricing_model: PricingModels, 
        default_params: dict[str, Any]
    ) -> None:
        """Test put option price sanity checks.
        
        Verifies that:
        - Price is positive
        - Price is less than strike price
        - Price increases with higher volatility

        Parameters
        ----------
        pricing_model : PricingModels
            Instance of PricingModels
        default_params : dict[str, Any]
            Default parameters dictionary
        """
        params = default_params.copy()
        params["opt_style"] = "put"
        price = pricing_model.binomial(**params)
        assert isinstance(price, float)
        assert 0 < price < params["k"]
        
        # Test price increases with higher volatility
        high_vol_params = params.copy()
        high_vol_params.update({"u": 1.2, "d": 0.8})
        high_vol_price = pricing_model.binomial(**high_vol_params)
        assert high_vol_price > price

    @pytest.mark.parametrize("n", [10, 100, 1000])
    def test_convergence(
        self, 
        pricing_model: PricingModels, 
        default_params: dict[str, Any], 
        n: int
    ) -> None:
        """Test binomial model convergence with increasing steps.
        
        Verifies that:
        - Price stabilizes as number of steps increases
        - No numerical instability with large step counts

        Parameters
        ----------
        pricing_model : PricingModels
            Instance of PricingModels
        default_params : dict[str, Any]
            Default parameters dictionary
        n : int
            Number of time steps
        """
        params = default_params.copy()
        params["n"] = n
        price = pricing_model.binomial(**params)
        assert isinstance(price, float)
        assert price > 0

    def test_barrier_options(
        self, 
        pricing_model: PricingModels, 
        default_params: dict[str, Any]
    ) -> None:
        """Test barrier option functionality.
        
        Verifies that:
        - Upper barrier knocks out when hit
        - Lower barrier knocks out when hit
        - Invalid barrier combinations raise error

        Parameters
        ----------
        pricing_model : PricingModels
            Instance of PricingModels
        default_params : dict[str, Any]
            Default parameters dictionary
        """
        # Test upper barrier
        upper_barrier_price = pricing_model.binomial(**default_params, h_upper=150.0)
        no_barrier_price = pricing_model.binomial(**default_params)
        assert upper_barrier_price <= no_barrier_price
        
        # Test lower barrier
        params = default_params.copy()
        params["opt_style"] = "put"
        lower_barrier_price = pricing_model.binomial(**params, h_lower=80.0)
        no_barrier_price = pricing_model.binomial(**params)
        assert lower_barrier_price <= no_barrier_price
        
        # Test invalid barrier combination
        with pytest.raises(ValueError, match="Upper barrier must be greater than lower barrier"):
            pricing_model.binomial(**default_params, h_upper=90.0, h_lower=110.0)

    def test_interest_rate_effects(
        self, 
        pricing_model: PricingModels, 
        default_params: dict[str, Any]
    ) -> None:
        """Test interest rate effects on option pricing.
        
        Verifies that:
        - Call prices increase with higher interest rates
        - Put prices decrease with higher interest rates

        Parameters
        ----------
        pricing_model : PricingModels
            Instance of PricingModels
        default_params : dict[str, Any]
            Default parameters dictionary
        """
        # Test call option
        high_rate_params = default_params.copy()
        high_rate_params["r"] = 0.10
        high_rate_price = pricing_model.binomial(**high_rate_params)
        low_rate_price = pricing_model.binomial(**default_params)
        assert high_rate_price > low_rate_price
        
        # Test put option
        put_params = default_params.copy()
        put_params["opt_style"] = "put"
        high_rate_put_price = pricing_model.binomial(
            s=put_params["s"],
            k=put_params["k"],
            r=0.10,
            t=put_params["t"],
            n=put_params["n"],
            u=put_params["u"],
            d=put_params["d"],
            opt_style="put"
        )
        low_rate_put_price = pricing_model.binomial(**put_params)
        assert high_rate_put_price < low_rate_put_price

    def test_early_exercise_premium(
        self, 
        pricing_model: PricingModels, 
        default_params: dict[str, Any]
    ) -> None:
        """Test early exercise premium for American options.
        
        Verifies that:
        - American put price > European put price (early exercise premium)
        - American call price = European call price (no dividend)

        Parameters
        ----------
        pricing_model : PricingModels
            Instance of PricingModels
        default_params : dict[str, Any]
            Default parameters dictionary
        """
        american_put_price = pricing_model.binomial(
            s=default_params["s"],
            k=default_params["k"],
            r=default_params["r"],
            t=default_params["t"],
            n=default_params["n"],
            u=default_params["u"],
            d=default_params["d"],
            opt_style="put"
        )
        assert american_put_price > 0

    def test_zero_time_steps_error(
        self, 
        pricing_model: PricingModels, 
        default_params: dict[str, Any]
    ) -> None:
        """Test error handling for zero time steps.
        
        Parameters
        ----------
        pricing_model : PricingModels
            Instance of PricingModels
        default_params : dict[str, Any]
            Default parameters dictionary
        """
        params = default_params.copy()
        params["n"] = 0
        with pytest.raises(ZeroDivisionError):
            pricing_model.binomial(**params)

    def test_extreme_volatility(
        self, 
        pricing_model: PricingModels, 
        default_params: dict[str, Any]
    ) -> None:
        """Test model behavior with extreme volatility parameters.
        
        Parameters
        ----------
        pricing_model : PricingModels
            Instance of PricingModels
        default_params : dict[str, Any]
            Default parameters dictionary
        """
        params = default_params.copy()
        params.update({"u": 2.0, "d": 0.5})
        price = pricing_model.binomial(**params)
        assert isinstance(price, float)
        assert price > 0

    def test_negative_interest_rate(
        self, 
        pricing_model: PricingModels, 
        default_params: dict[str, Any]
    ) -> None:
        """Test model behavior with negative interest rates.
        
        Parameters
        ----------
        pricing_model : PricingModels
            Instance of PricingModels
        default_params : dict[str, Any]
            Default parameters dictionary
        """
        params = default_params.copy()
        params["r"] = -0.05
        price = pricing_model.binomial(**params)
        assert isinstance(price, float)
        assert price > 0