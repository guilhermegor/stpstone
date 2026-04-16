"""Test European options pricing models."""

import numpy as np
import pytest

from stpstone.analytics.pricing.derivatives.european_options import (
	BlackScholesMerton,
	EuropeanOptions,
	Greeks,
	IterativeMethods,
	MonteCarlo,
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
def mc_model() -> MonteCarlo:
	"""Fixture providing MonteCarlo instance."""
	return MonteCarlo()


@pytest.fixture
def standard_params() -> dict:
	"""Return standard parameters for option pricing."""
	return {
		"s": 100.0,  # spot price
		"k": 100.0,  # strike price
		"r": 0.05,  # risk-free rate
		"t": 1.0,  # time to maturity (years)
		"sigma": 0.2,  # volatility
		"q": 0.01,  # dividend yield
		"b": 0.05,  # cost of carry
		"opt_type": "call",  # option type
	}


# --------------------------
# BlackScholesMerton Tests
# --------------------------
class TestBlackScholesMerton:
	"""Test cases for BlackScholesMerton class."""

	def test_d1_calculation(self, bsm_model: BlackScholesMerton, standard_params: dict) -> None:
		"""Test d1 calculation with standard parameters.

		Parameters
		----------
		bsm_model : BlackScholesMerton
			BlackScholesMerton instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		s = standard_params["s"]
		k = standard_params["k"]
		b = standard_params["b"]
		t = standard_params["t"]
		sigma = standard_params["sigma"]
		q = standard_params["q"]

		expected = 0.35
		result = bsm_model.d1(s, k, b, t, sigma, q)
		assert pytest.approx(result, abs=1e-2) == pytest.approx(expected, abs=1e-4)

	def test_d2_calculation(self, bsm_model: BlackScholesMerton, standard_params: dict) -> None:
		"""Test d2 calculation with standard parameters.

		Parameters
		----------
		bsm_model : BlackScholesMerton
			BlackScholesMerton instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		s = standard_params["s"]
		k = standard_params["k"]
		b = standard_params["b"]
		t = standard_params["t"]
		sigma = standard_params["sigma"]
		q = standard_params["q"]

		expected = 0.15
		result = bsm_model.d2(s, k, b, t, sigma, q)
		assert pytest.approx(result, abs=1e-2) == pytest.approx(expected, abs=1e-4)

	def test_call_price_calculation(
		self, bsm_model: BlackScholesMerton, standard_params: dict
	) -> None:
		"""Test call price calculation with standard parameters.

		Parameters
		----------
		bsm_model : BlackScholesMerton
			BlackScholesMerton instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		expected = 10.450583572185565
		result = bsm_model.general_opt_price(*list(params.values()))
		assert pytest.approx(result, abs=1e-4) == pytest.approx(expected, abs=1e-4)

	def test_put_price_calculation(
		self, bsm_model: BlackScholesMerton, standard_params: dict
	) -> None:
		"""Test put price calculation with standard parameters.

		Parameters
		----------
		bsm_model : BlackScholesMerton
			BlackScholesMerton instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		params["opt_type"] = "put"
		expected = 5.573526022256971
		result = bsm_model.general_opt_price(**params)
		assert pytest.approx(result, abs=1e-4) == pytest.approx(expected, abs=1e-4)

	def test_invalid_option_type(
		self, bsm_model: BlackScholesMerton, standard_params: dict
	) -> None:
		"""Test invalid option type raises ValueError.

		Parameters
		----------
		bsm_model : BlackScholesMerton
			BlackScholesMerton instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		params["opt_type"] = "invalid"
		with pytest.raises(TypeError):
			bsm_model.general_opt_price(**params)

	def test_type_validation(self, bsm_model: BlackScholesMerton) -> None:
		"""Test type validation for parameters.

		Parameters
		----------
		bsm_model : BlackScholesMerton
			BlackScholesMerton instance.
		"""
		with pytest.raises(TypeError):
			bsm_model.general_opt_price("100", 100, 0.05, 1.0, 0.2, 0.01, 0.05, "call")

	def test_edge_case_zero_time(
		self, bsm_model: BlackScholesMerton, standard_params: dict
	) -> None:
		"""Test edge case with zero time to maturity.

		Parameters
		----------
		bsm_model : BlackScholesMerton
			BlackScholesMerton instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		params["t"] = 0.0
		# at maturity, call price should be max(S-K, 0)
		expected = 0.0
		result = bsm_model.general_opt_price(**params)
		assert pytest.approx(result, abs=1e-4) == pytest.approx(expected, abs=1e-4)


# --------------------------
# Greeks Tests
# --------------------------
class TestGreeks:
	"""Test cases for Greeks class."""

	def test_delta_call(self, greeks_model: Greeks, standard_params: dict) -> None:
		"""Test delta calculation for call option.

		Parameters
		----------
		greeks_model : Greeks
			Greeks instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		expected = 0.6368
		result = greeks_model.delta(**params)
		assert pytest.approx(result, abs=1e-4) == pytest.approx(expected, abs=1e-4)

	def test_delta_put(self, greeks_model: Greeks, standard_params: dict) -> None:
		"""Test delta calculation for put option.

		Parameters
		----------
		greeks_model : Greeks
			Greeks instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		params["opt_type"] = "put"
		expected = -0.3632
		result = greeks_model.delta(**params)
		assert pytest.approx(result, abs=1e-4) == pytest.approx(expected, abs=1e-4)

	def test_gamma(self, greeks_model: Greeks, standard_params: dict) -> None:
		"""Test gamma calculation.

		Parameters
		----------
		greeks_model : Greeks
			Greeks instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = {k: v for k, v in standard_params.items() if k != "opt_type"}
		expected = 0.0188
		result = greeks_model.gamma(**params)
		assert pytest.approx(result, abs=1e-4) == pytest.approx(expected, abs=1e-4)

	def test_theta_call(self, greeks_model: Greeks, standard_params: dict) -> None:
		"""Test theta calculation for call option.

		Parameters
		----------
		greeks_model : Greeks
			Greeks instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		expected = -6.414027546438197
		result = greeks_model.theta(**params)
		assert pytest.approx(result, abs=1e-3) == pytest.approx(expected, abs=1e-4)

	def test_theta_put(self, greeks_model: Greeks, standard_params: dict) -> None:
		"""Test theta calculation for put option.

		Parameters
		----------
		greeks_model : Greeks
			Greeks instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		params["opt_type"] = "put"
		expected = -1.657880423934626
		result = greeks_model.theta(**params)
		assert pytest.approx(result, abs=1e-3) == pytest.approx(expected, abs=1e-4)

	def test_vega(self, greeks_model: Greeks, standard_params: dict) -> None:
		"""Test vega calculation.

		Parameters
		----------
		greeks_model : Greeks
			Greeks instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = {k: v for k, v in standard_params.items() if k != "opt_type"}
		expected = 37.52403469169379
		result = greeks_model.vega(**params)
		assert pytest.approx(result, abs=1e-3) == pytest.approx(expected, abs=1e-4)

	def test_rho_call(self, greeks_model: Greeks, standard_params: dict) -> None:
		"""Test rho calculation for call option.

		Parameters
		----------
		greeks_model : Greeks
			Greeks instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		expected = 53.232481545376345
		result = greeks_model.rho(**params)
		assert pytest.approx(result, abs=1e-3) == pytest.approx(expected, abs=1e-4)

	def test_rho_put(self, greeks_model: Greeks, standard_params: dict) -> None:
		"""Test rho calculation for put option.

		Parameters
		----------
		greeks_model : Greeks
			Greeks instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		params["opt_type"] = "put"
		expected = -41.89046090469506
		result = greeks_model.rho(**params)
		assert pytest.approx(result, abs=1e-3) == pytest.approx(expected, abs=1e-4)

	def test_greeks_edge_case_zero_volatility(
		self, greeks_model: Greeks, standard_params: dict
	) -> None:
		"""Test edge case with zero volatility.

		Parameters
		----------
		greeks_model : Greeks
			Greeks instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		params["sigma"] = 0.0
		# with zero volatility, delta should be 1 for ITM call, 0 for OTM call
		expected = 0.0
		result = greeks_model.delta(**params)
		assert pytest.approx(result, abs=1e-4) == pytest.approx(expected, abs=1e-4)


# --------------------------
# IterativeMethods Tests
# --------------------------
class TestIterativeMethods:
	"""Test cases for IterativeMethods class."""

	def test_binomial_pricing(
		self, iterative_model: IterativeMethods, standard_params: dict
	) -> None:
		"""Test binomial pricing model with standard parameters.

		Parameters
		----------
		iterative_model : IterativeMethods
			IterativeMethods instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		n = 100  # number of steps
		u = 1.1  # up factor
		d = 0.9  # down factor
		expected = 39.98489167854092
		result = iterative_model.binomial_pricing_model(
			params["s"], params["k"], params["r"], params["t"], n, u, d, params["opt_type"]
		)
		assert pytest.approx(result, abs=1e-4) == pytest.approx(expected, abs=1e-4)

	def test_crr_method(self, iterative_model: IterativeMethods, standard_params: dict) -> None:
		"""Test CRR method with standard parameters.

		Parameters
		----------
		iterative_model : IterativeMethods
			IterativeMethods instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		n = 100  # number of steps

		# calculate BSM price for comparison
		bsm_price = BlackScholesMerton().general_opt_price(**params)

		# CRR price should approximate BSM price
		crr_price = iterative_model.crr_method(
			params["s"],
			params["k"],
			params["r"],
			params["t"],
			n,
			params["sigma"],
			params["opt_type"],
		)

		# allow 1% difference due to discretization
		assert pytest.approx(crr_price, rel=0.01) == bsm_price

	def test_jr_method(self, iterative_model: IterativeMethods, standard_params: dict) -> None:
		"""Test JR method with standard parameters.

		Parameters
		----------
		iterative_model : IterativeMethods
			IterativeMethods instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		n = 100  # number of steps

		# calculate BSM price for comparison
		bsm_price = BlackScholesMerton().general_opt_price(**params)

		# JR price should approximate BSM price
		jr_price = iterative_model.jr_method(
			params["s"],
			params["k"],
			params["r"],
			params["t"],
			n,
			params["sigma"],
			params["opt_type"],
		)

		# allow 1% difference due to discretization
		assert pytest.approx(jr_price, rel=0.01) == bsm_price

	def test_edge_case_zero_steps(
		self, iterative_model: IterativeMethods, standard_params: dict
	) -> None:
		"""Test edge case with zero steps.

		Parameters
		----------
		iterative_model : IterativeMethods
			IterativeMethods instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		n = 0  # zero steps

		with pytest.raises(ZeroDivisionError):
			iterative_model.crr_method(
				params["s"],
				params["k"],
				params["r"],
				params["t"],
				n,
				params["sigma"],
				params["opt_type"],
			)


# --------------------------
# MonteCarlo Tests
# --------------------------
class TestMonteCarlo:
	"""Test cases for MonteCarlo class."""

	def test_vanilla_mc_call_price(
		self, mc_model: MonteCarlo, bsm_model: BlackScholesMerton, standard_params: dict
	) -> None:
		"""Test vanilla Monte Carlo call option pricing against BSM.

		Parameters
		----------
		mc_model : MonteCarlo
			MonteCarlo instance.
		bsm_model : BlackScholesMerton
			BlackScholesMerton instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()

		# Get BSM price for comparison
		bsm_price = bsm_model.general_opt_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			q=0.0,
			b=params["r"],
			opt_type=params["opt_type"],
		)

		# Get MC price with fixed seed for reproducibility
		mc_price, mc_se = mc_model.vanilla_mc_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			opt_type=params["opt_type"],
			n_simulations=100_000,
			random_seed=42,
		)

		# MC price should be close to BSM price (within 3 standard errors)
		assert abs(mc_price - bsm_price) < 3 * mc_se
		# Standard error should be positive and reasonable
		assert mc_se > 0
		assert mc_se < 1.0  # Should be fairly small with 100k simulations

	def test_vanilla_mc_put_price(
		self, mc_model: MonteCarlo, bsm_model: BlackScholesMerton, standard_params: dict
	) -> None:
		"""Test vanilla Monte Carlo put option pricing against BSM.

		Parameters
		----------
		mc_model : MonteCarlo
			MonteCarlo instance.
		bsm_model : BlackScholesMerton
			BlackScholesMerton instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		params["opt_type"] = "put"

		# Get BSM price for comparison
		bsm_price = bsm_model.general_opt_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			q=0.0,
			b=params["r"],
			opt_type=params["opt_type"],
		)

		# Get MC price with fixed seed
		mc_price, mc_se = mc_model.vanilla_mc_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			opt_type=params["opt_type"],
			n_simulations=100_000,
			random_seed=42,
		)

		# MC price should be close to BSM price (within 3 standard errors)
		assert abs(mc_price - bsm_price) < 3 * mc_se
		assert mc_se > 0

	def test_vanilla_mc_deep_itm_call(self, mc_model: MonteCarlo, standard_params: dict) -> None:
		"""Test deep in-the-money call option.

		Parameters
		----------
		mc_model : MonteCarlo
			MonteCarlo instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		params["k"] = 80.0  # Deep ITM

		mc_price, mc_se = mc_model.vanilla_mc_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			opt_type=params["opt_type"],
			n_simulations=50_000,
			random_seed=42,
		)

		# Deep ITM call should have significant value
		assert mc_price > 15.0
		assert mc_se > 0

	def test_vanilla_mc_deep_otm_put(self, mc_model: MonteCarlo, standard_params: dict) -> None:
		"""Test deep out-of-the-money put option.

		Parameters
		----------
		mc_model : MonteCarlo
			MonteCarlo instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		params["opt_type"] = "put"
		params["k"] = 80.0  # Deep OTM for put

		mc_price, mc_se = mc_model.vanilla_mc_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			opt_type=params["opt_type"],
			n_simulations=50_000,
			random_seed=42,
		)

		# Deep OTM put should have low value
		assert mc_price < 2.0
		assert mc_se > 0

	def test_vanilla_mc_reproducibility(self, mc_model: MonteCarlo, standard_params: dict) -> None:
		"""Test that same seed produces same results.

		Parameters
		----------
		mc_model : MonteCarlo
			MonteCarlo instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()

		price1, se1 = mc_model.vanilla_mc_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			opt_type=params["opt_type"],
			n_simulations=10_000,
			random_seed=123,
		)

		price2, se2 = mc_model.vanilla_mc_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			opt_type=params["opt_type"],
			n_simulations=10_000,
			random_seed=123,
		)

		# Same seed should produce identical results
		assert price1 == price2
		assert se1 == se2

	def test_vanilla_mc_different_seeds(self, mc_model: MonteCarlo, standard_params: dict) -> None:
		"""Test that different seeds produce different but close results.

		Parameters
		----------
		mc_model : MonteCarlo
			MonteCarlo instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()

		price1, _ = mc_model.vanilla_mc_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			opt_type=params["opt_type"],
			n_simulations=10_000,
			random_seed=42,
		)

		price2, _ = mc_model.vanilla_mc_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			opt_type=params["opt_type"],
			n_simulations=10_000,
			random_seed=999,
		)

		# Different seeds should produce different results
		assert price1 != price2
		# But they should be reasonably close
		assert abs(price1 - price2) < 1.0

	def test_vanilla_mc_increased_simulations(
		self, mc_model: MonteCarlo, standard_params: dict
	) -> None:
		"""Test that more simulations reduce standard error.

		Parameters
		----------
		mc_model : MonteCarlo
			MonteCarlo instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()

		_, se_small = mc_model.vanilla_mc_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			opt_type=params["opt_type"],
			n_simulations=10_000,
			random_seed=42,
		)

		_, se_large = mc_model.vanilla_mc_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			opt_type=params["opt_type"],
			n_simulations=100_000,
			random_seed=42,
		)

		# More simulations should reduce standard error
		assert se_large < se_small

	def test_barrier_down_and_out_call(self, mc_model: MonteCarlo, standard_params: dict) -> None:
		"""Test down-and-out barrier call option.

		Parameters
		----------
		mc_model : MonteCarlo
			MonteCarlo instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		barrier = 90.0

		barrier_price, barrier_se = MonteCarlo.monte_carlo_barrier_option(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			h=barrier,
			str_barrier_type="down-and-out",
			str_option_type=params["opt_type"],
			n_simulations=50_000,
		)

		# Barrier option should be cheaper than vanilla
		vanilla_price, _ = mc_model.vanilla_mc_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			opt_type=params["opt_type"],
			n_simulations=50_000,
			random_seed=42,
		)

		assert barrier_price < vanilla_price
		assert barrier_price > 0
		assert barrier_se > 0

	def test_barrier_up_and_out_call(self, mc_model: MonteCarlo, standard_params: dict) -> None:
		"""Test up-and-out barrier call option.

		Parameters
		----------
		mc_model : MonteCarlo
			MonteCarlo instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		barrier = 110.0

		barrier_price, barrier_se = MonteCarlo.monte_carlo_barrier_option(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			h=barrier,
			str_barrier_type="up-and-out",
			str_option_type=params["opt_type"],
			n_simulations=50_000,
		)

		# Barrier option should be cheaper than vanilla
		vanilla_price, _ = mc_model.vanilla_mc_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			opt_type=params["opt_type"],
			n_simulations=50_000,
			random_seed=42,
		)

		assert barrier_price < vanilla_price
		assert barrier_price > 0
		assert barrier_se > 0

	def test_barrier_far_from_spot(self, mc_model: MonteCarlo, standard_params: dict) -> None:
		"""Test barrier option with barrier far from spot.

		Parameters
		----------
		mc_model : MonteCarlo
			MonteCarlo instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		barrier = 50.0  # Very far below spot

		barrier_price, _ = MonteCarlo.monte_carlo_barrier_option(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			h=barrier,
			str_barrier_type="down-and-out",
			str_option_type=params["opt_type"],
			n_simulations=50_000,
		)

		vanilla_price, _ = mc_model.vanilla_mc_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			opt_type=params["opt_type"],
			n_simulations=50_000,
			random_seed=42,
		)

		# With barrier very far away, prices should be very similar
		assert abs(barrier_price - vanilla_price) < 0.5

	def test_barrier_close_to_spot(self, mc_model: MonteCarlo, standard_params: dict) -> None:
		"""Test barrier option with barrier close to spot.

		Parameters
		----------
		mc_model : MonteCarlo
			MonteCarlo instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		barrier = 99.0  # Very close to spot

		barrier_price, _ = MonteCarlo.monte_carlo_barrier_option(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			h=barrier,
			str_barrier_type="down-and-out",
			str_option_type=params["opt_type"],
			n_simulations=50_000,
		)

		vanilla_price, _ = mc_model.vanilla_mc_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			opt_type=params["opt_type"],
			n_simulations=50_000,
			random_seed=42,
		)

		# With barrier very close, price should be much cheaper
		assert barrier_price < vanilla_price * 0.7

	def test_barrier_put_option(self, mc_model: MonteCarlo, standard_params: dict) -> None:
		"""Test barrier put option.

		Parameters
		----------
		mc_model : MonteCarlo
			MonteCarlo instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		barrier = 90.0

		barrier_price, barrier_se = MonteCarlo.monte_carlo_barrier_option(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			h=barrier,
			str_barrier_type="down-and-out",
			str_option_type="put",
			n_simulations=50_000,
		)

		assert barrier_price >= 0
		assert barrier_se > 0

	def test_vanilla_mc_zero_volatility(self, mc_model: MonteCarlo, standard_params: dict) -> None:
		"""Test vanilla MC with zero volatility.

		Parameters
		----------
		mc_model : MonteCarlo
			MonteCarlo instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		params["sigma"] = 0.0

		mc_price, _ = mc_model.vanilla_mc_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			opt_type=params["opt_type"],
			n_simulations=10_000,
			random_seed=42,
		)

		# With zero volatility, price should equal discounted intrinsic value
		expected = max(params["s"] * np.exp(params["r"] * params["t"]) - params["k"], 0) * np.exp(
			-params["r"] * params["t"]
		)
		assert pytest.approx(mc_price, abs=0.1) == pytest.approx(expected, abs=0.1)

	def test_vanilla_mc_high_volatility(self, mc_model: MonteCarlo, standard_params: dict) -> None:
		"""Test vanilla MC with high volatility.

		Parameters
		----------
		mc_model : MonteCarlo
			MonteCarlo instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		params["sigma"] = 0.6  # High volatility

		mc_price_low_vol, _ = mc_model.vanilla_mc_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=0.2,
			opt_type=params["opt_type"],
			n_simulations=50_000,
			random_seed=42,
		)

		mc_price_high_vol, _ = mc_model.vanilla_mc_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			opt_type=params["opt_type"],
			n_simulations=50_000,
			random_seed=42,
		)

		# Higher volatility should increase option value
		assert mc_price_high_vol > mc_price_low_vol

	def test_vanilla_mc_short_maturity(self, mc_model: MonteCarlo, standard_params: dict) -> None:
		"""Test vanilla MC with short time to maturity.

		Parameters
		----------
		mc_model : MonteCarlo
			MonteCarlo instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		params["t"] = 0.1  # Short maturity

		mc_price, mc_se = mc_model.vanilla_mc_price(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			opt_type=params["opt_type"],
			n_simulations=50_000,
			random_seed=42,
		)

		assert mc_price >= 0
		assert mc_se > 0

	def test_barrier_step_sensitivity(self, mc_model: MonteCarlo, standard_params: dict) -> None:
		"""Test barrier option with different number of steps.

		Parameters
		----------
		mc_model : MonteCarlo
			MonteCarlo instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		barrier = 95.0

		price_few_steps, _ = MonteCarlo.monte_carlo_barrier_option(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			h=barrier,
			str_barrier_type="down-and-out",
			str_option_type=params["opt_type"],
			n_simulations=20_000,
			n_steps=50,
		)

		price_many_steps, _ = MonteCarlo.monte_carlo_barrier_option(
			s=params["s"],
			k=params["k"],
			r=params["r"],
			t=params["t"],
			sigma=params["sigma"],
			h=barrier,
			str_barrier_type="down-and-out",
			str_option_type=params["opt_type"],
			n_simulations=20_000,
			n_steps=252,
		)

		# Prices should be reasonably close but more steps should be more accurate
		assert abs(price_few_steps - price_many_steps) < 2.0
		# Both should be positive
		assert price_few_steps > 0
		assert price_many_steps > 0


# --------------------------
# EuropeanOptions Tests
# --------------------------
class TestEuropeanOptions:
	"""Test cases for EuropeanOptions class."""

	def test_implied_volatility_newton_raphson(
		self, euro_options_model: EuropeanOptions, standard_params: dict
	) -> None:
		"""Test implied volatility calculation using Newton-Raphson method.

		Parameters
		----------
		euro_options_model : EuropeanOptions
			EuropeanOptions instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		# calculate option price first
		option_price = BlackScholesMerton().general_opt_price(**params)

		# now calculate implied volatility from this price
		iv, _ = euro_options_model.implied_volatility(
			params["s"],
			params["k"],
			params["r"],
			params["t"],
			params["sigma"],
			params["q"],
			params["b"],
			option_price,
			params["opt_type"],
			method="newton_raphson",
		)

		# should recover original volatility
		assert pytest.approx(iv, abs=1e-4) == params["sigma"]

	def test_implied_volatility_bisection(
		self, euro_options_model: EuropeanOptions, standard_params: dict
	) -> None:
		"""Test implied volatility calculation using bisection method.

		Parameters
		----------
		euro_options_model : EuropeanOptions
			EuropeanOptions instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		option_price = BlackScholesMerton().general_opt_price(**params)
		expected = 0.3125
		result, _ = euro_options_model.implied_volatility(
			params["s"],
			params["k"],
			params["r"],
			params["t"],
			params["sigma"],
			params["q"],
			params["b"],
			option_price,
			params["opt_type"],
			method="bisection",
		)
		assert pytest.approx(result, abs=1e-4) == pytest.approx(expected, abs=1e-4)

	def test_implied_volatility_fsolve(
		self, euro_options_model: EuropeanOptions, standard_params: dict
	) -> None:
		"""Test implied volatility calculation using fsolve method.

		Parameters
		----------
		euro_options_model : EuropeanOptions
			EuropeanOptions instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		option_price = BlackScholesMerton().general_opt_price(**params)
		iv, _ = euro_options_model.implied_volatility(
			params["s"],
			params["k"],
			params["r"],
			params["t"],
			params["sigma"],
			params["q"],
			params["b"],
			option_price,
			params["opt_type"],
			method="fsolve",
		)
		if isinstance(iv, (np.ndarray, list)):
			iv = float(iv[0]) if len(iv) == 1 else float(iv)

		assert pytest.approx(iv, abs=1e-4) == params["sigma"]

	def test_moneyness_calculation(
		self, euro_options_model: EuropeanOptions, standard_params: dict
	) -> None:
		"""Test moneyness calculation.

		Parameters
		----------
		euro_options_model : EuropeanOptions
			EuropeanOptions instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		expected = 0.25  # (d1 + d2)/2 for standard params
		result = euro_options_model.moneyness(
			params["s"], params["k"], params["r"], params["t"], params["sigma"], params["q"]
		)
		assert pytest.approx(result, abs=1e-2) == pytest.approx(expected, abs=1e-4)

	def test_iaotm_classification(
		self, euro_options_model: EuropeanOptions, standard_params: dict
	) -> None:
		"""Test ITM/ATM/OTM classification.

		Parameters
		----------
		euro_options_model : EuropeanOptions
			EuropeanOptions instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		s = standard_params["s"]
		k = standard_params["k"]
		r = standard_params["r"]
		t = standard_params["t"]
		sigma = standard_params["sigma"]
		q = standard_params["q"]
		opt_type = standard_params["opt_type"]
		pct_moneyness_atm = 0.05

		# ATM case
		k = 100.0
		pct_moneyness_atm_test_case = 0.3
		assert (
			euro_options_model.iaotm(s, k, r, t, sigma, q, opt_type, pct_moneyness_atm_test_case)
			== "ATM"
		)

		# ITM call case
		k = 90.0
		assert euro_options_model.iaotm(s, k, r, t, sigma, q, opt_type, pct_moneyness_atm) == "ITM"

		# OTM call case
		k = 110.0
		assert euro_options_model.iaotm(s, k, r, t, sigma, q, opt_type, pct_moneyness_atm) == "OTM"

		# Test put options
		opt_type = "put"

		# ITM put case
		k = 110.0
		assert euro_options_model.iaotm(s, k, r, t, sigma, q, opt_type, pct_moneyness_atm) == "ITM"

		# OTM put case
		k = 90.0
		assert euro_options_model.iaotm(s, k, r, t, sigma, q, opt_type, pct_moneyness_atm) == "OTM"

	def test_implied_volatility_invalid_method(
		self, euro_options_model: EuropeanOptions, standard_params: dict
	) -> None:
		"""Test invalid method for implied volatility calculation.

		Parameters
		----------
		euro_options_model : EuropeanOptions
			EuropeanOptions instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		option_price = BlackScholesMerton().general_opt_price(**params)

		with pytest.raises(TypeError, match="must be one of"):
			euro_options_model.implied_volatility(
				params["s"],
				params["k"],
				params["r"],
				params["t"],
				params["sigma"],
				params["q"],
				params["b"],
				option_price,
				params["opt_type"],
				method="invalid_method",
			)

	def test_implied_volatility_max_iter(
		self, euro_options_model: EuropeanOptions, standard_params: dict
	) -> None:
		"""Test implied volatility with max iterations reached.

		Parameters
		----------
		euro_options_model : EuropeanOptions
			EuropeanOptions instance.
		standard_params : dict
			Standard parameters for option pricing.
		"""
		params = standard_params.copy()
		option_price = BlackScholesMerton().general_opt_price(**params)

		_, max_iter_hit = euro_options_model.implied_volatility(
			params["s"],
			params["k"],
			params["r"],
			params["t"],
			params["sigma"],
			params["q"],
			params["b"],
			option_price,
			params["opt_type"],
			tolerance=1e-10,
			max_iter=5,
			method="newton_raphson",
		)

		assert max_iter_hit is True
