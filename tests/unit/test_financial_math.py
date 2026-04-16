"""Unit tests for FinancialMath class.

Tests financial calculations including time value of money, cash flow analysis,
and various interest rate calculations.
"""

import numpy as np
from numpy.typing import NDArray
import numpy_financial as npf
import pytest

from stpstone.analytics.perf_metrics.financial_math import FinancialMath


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def financial_math() -> FinancialMath:
	"""Fixture providing an instance of FinancialMath."""
	return FinancialMath()


@pytest.fixture
def sample_cash_flows() -> NDArray[np.float64]:
	"""Fixture providing sample cash flows for testing."""
	return np.array([-1000.0, 300.0, 400.0, 500.0, 200.0], dtype=np.float64)


@pytest.fixture
def empty_cash_flows() -> NDArray[np.float64]:
	"""Fixture providing empty cash flows for testing."""
	return np.array([], dtype=np.float64)


@pytest.fixture
def invalid_cash_flows() -> NDArray[np.float64]:
	"""Fixture providing invalid cash flows for testing."""
	return np.array([[1, 2], [3, 4]], dtype=np.float64)  # 2D array


@pytest.fixture
def all_positive_cash_flows() -> NDArray[np.float64]:
	"""Fixture providing all positive cash flows for testing."""
	return np.array([100.0, 200.0, 300.0], dtype=np.float64)


@pytest.fixture
def all_negative_cash_flows() -> NDArray[np.float64]:
	"""Fixture providing all negative cash flows for testing."""
	return np.array([-100.0, -200.0, -300.0], dtype=np.float64)


# --------------------------
# Tests for compound_r
# --------------------------
def test_compound_r_valid_input(financial_math: FinancialMath) -> None:
	"""Test compound_r with valid inputs.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	result = financial_math.compound_r(0.1, 5, 2)
	expected = (1.0 + 0.1) ** (5 / 2) - 1.0
	assert result == pytest.approx(expected)


def test_compound_r_zero_periods(financial_math: FinancialMath) -> None:
	"""Test compound_r with zero periods.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	result = financial_math.compound_r(0.1, 0, 2)
	assert result == 0.0


def test_compound_r_nan_yield(financial_math: FinancialMath) -> None:
	"""Test compound_r raises ValueError with NaN yield.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	with pytest.raises(ValueError, match="Yield cannot be NaN"):
		financial_math.compound_r(np.nan, 5, 2)


def test_compound_r_inf_yield(financial_math: FinancialMath) -> None:
	"""Test compound_r raises ValueError with infinite yield.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	with pytest.raises(ValueError, match="Yield cannot be infinite"):
		financial_math.compound_r(np.inf, 5, 2)


# --------------------------
# Tests for simple_r
# --------------------------
def test_simple_r_valid_input(financial_math: FinancialMath) -> None:
	"""Test simple_r with valid inputs.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	result = financial_math.simple_r(0.1, 5, 2)
	expected = 0.1 * 5 / 2
	assert result == pytest.approx(expected)


def test_simple_r_zero_periods(financial_math: FinancialMath) -> None:
	"""Test simple_r with zero periods.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	result = financial_math.simple_r(0.1, 0, 2)
	assert result == 0.0


def test_simple_r_nan_yield(financial_math: FinancialMath) -> None:
	"""Test simple_r raises ValueError with NaN yield.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	with pytest.raises(ValueError, match="Yield cannot be NaN"):
		financial_math.simple_r(np.nan, 5, 2)


def test_simple_r_inf_yield(financial_math: FinancialMath) -> None:
	"""Test simple_r raises ValueError with infinite yield.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	with pytest.raises(ValueError, match="Yield cannot be infinite"):
		financial_math.simple_r(np.inf, 5, 2)


# --------------------------
# Tests for pv
# --------------------------
def test_pv_compound_end(financial_math: FinancialMath) -> None:
	"""Test pv with compound capitalization and end payments.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	result = financial_math.pv(0.1, 5, 1000.0, 100.0, "compound", "end")
	expected = npf.pv(0.1, 5, 100, 1000, "end")
	assert result == pytest.approx(expected)


def test_pv_compound_begin(financial_math: FinancialMath) -> None:
	"""Test pv with compound capitalization and begin payments.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	result = financial_math.pv(0.1, 5, 1000.0, 100.0, "compound", "begin")
	expected = npf.pv(0.1, 5, 100, 1000, "begin")
	assert result == pytest.approx(expected)


def test_pv_simple(financial_math: FinancialMath) -> None:
	"""Test pv with simple capitalization.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	result = financial_math.pv(0.1, 5, 1000.0, 0.0, "simple", "end")
	expected = 1000 / (1.0 + financial_math.simple_r(0.1, 5, 1))
	assert result == pytest.approx(expected)


def test_pv_zero_rate(financial_math: FinancialMath) -> None:
	"""Test pv with zero interest rate.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	result = financial_math.pv(0.0, 5, 1000.0, 100.0, "compound", "end")
	assert result == pytest.approx(-1000 - 100 * 5)


def test_pv_invalid_capitalization(financial_math: FinancialMath) -> None:
	"""Test pv raises TypeError with invalid capitalization.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	with pytest.raises(TypeError, match="must be one of"):
		financial_math.pv(0.1, 5, 1000.0, 100.0, "invalid", "end")


# --------------------------
# Tests for fv
# --------------------------
def test_fv_compound_end(financial_math: FinancialMath) -> None:
	"""Test fv with compound capitalization and end payments.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	result = financial_math.fv(0.1, 5, 1000.0, 100.0, "compound", "end")
	expected = npf.fv(0.1, 5, 100, 1000, "end")
	assert result == pytest.approx(expected)


def test_fv_compound_begin(financial_math: FinancialMath) -> None:
	"""Test fv with compound capitalization and begin payments.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	result = financial_math.fv(0.1, 5, 1000.0, 100.0, "compound", "begin")
	expected = npf.fv(0.1, 5, 100, 1000, "begin")
	assert result == pytest.approx(expected)


def test_fv_simple(financial_math: FinancialMath) -> None:
	"""Test fv with simple capitalization.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	result = financial_math.fv(0.1, 5, 1000.0, 0.0, "simple", "end")
	expected = 1000 * (1.0 + financial_math.simple_r(0.1, 5, 1))
	assert result == pytest.approx(expected)


def test_fv_invalid_capitalization(financial_math: FinancialMath) -> None:
	"""Test fv raises TypeError with invalid capitalization.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	with pytest.raises(TypeError, match="must be one of"):
		financial_math.fv(0.1, 5, 1000.0, 100.0, "invalid", "end")


# --------------------------
# Tests for irr
# --------------------------
def test_irr_valid_cash_flows(
	financial_math: FinancialMath, sample_cash_flows: NDArray[np.float64]
) -> None:
	"""Test irr with valid cash flows.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	sample_cash_flows : NDArray[np.float64]
		Sample cash flows
	"""
	result = financial_math.irr(sample_cash_flows)
	expected = npf.irr(sample_cash_flows)
	assert result == pytest.approx(expected)


def test_irr_empty_cash_flows(
	financial_math: FinancialMath, empty_cash_flows: NDArray[np.float64]
) -> None:
	"""Test irr raises ValueError with empty cash flows.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	empty_cash_flows : NDArray[np.float64]
		Empty cash flows
	"""
	with pytest.raises(ValueError, match="Cash flows cannot be empty"):
		financial_math.irr(empty_cash_flows)


def test_irr_invalid_dimension(
	financial_math: FinancialMath, invalid_cash_flows: NDArray[np.float64]
) -> None:
	"""Test irr raises ValueError with 2D cash flows.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	invalid_cash_flows : NDArray[np.float64]
		2D cash flows
	"""
	with pytest.raises(ValueError, match="must be a 1D array"):
		financial_math.irr(invalid_cash_flows)


def test_irr_all_positive(
	financial_math: FinancialMath, all_positive_cash_flows: NDArray[np.float64]
) -> None:
	"""Test irr raises ValueError with all positive cash flows.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	all_positive_cash_flows : NDArray[np.float64]
		All positive cash flows
	"""
	with pytest.raises(ValueError, match="one positive and one negative"):
		financial_math.irr(all_positive_cash_flows)


def test_irr_all_negative(
	financial_math: FinancialMath, all_negative_cash_flows: NDArray[np.float64]
) -> None:
	"""Test irr raises ValueError with all negative cash flows.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	all_negative_cash_flows : NDArray[np.float64]
		All negative cash flows
	"""
	with pytest.raises(ValueError, match="one positive and one negative"):
		financial_math.irr(all_negative_cash_flows)


# --------------------------
# Tests for npv
# --------------------------
def test_npv_valid_input(
	financial_math: FinancialMath, sample_cash_flows: NDArray[np.float64]
) -> None:
	"""Test npv with valid inputs.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	sample_cash_flows : NDArray[np.float64]
		Sample cash flows
	"""
	result = financial_math.npv(0.1, sample_cash_flows)
	expected = npf.npv(0.1, sample_cash_flows)
	assert result == pytest.approx(expected)


def test_npv_empty_cash_flows(
	financial_math: FinancialMath, empty_cash_flows: NDArray[np.float64]
) -> None:
	"""Test npv raises ValueError with empty cash flows.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	empty_cash_flows : NDArray[np.float64]
		Empty cash flows
	"""
	with pytest.raises(ValueError, match="cannot be empty"):
		financial_math.npv(0.1, empty_cash_flows)


def test_npv_invalid_dimension(
	financial_math: FinancialMath, invalid_cash_flows: NDArray[np.float64]
) -> None:
	"""Test npv raises ValueError with 2D cash flows.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	invalid_cash_flows : NDArray[np.float64]
		2D cash flows
	"""
	with pytest.raises(ValueError, match="must be a 1D array"):
		financial_math.npv(0.1, invalid_cash_flows)


# --------------------------
# Tests for pmt
# --------------------------
def test_pmt_valid_input(financial_math: FinancialMath) -> None:
	"""Test pmt with valid inputs.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	result = financial_math.pmt(0.1, 5, 1000.0)
	expected = npf.pmt(0.1, 5, 1000)
	assert result == pytest.approx(expected)


def test_pmt_begin(financial_math: FinancialMath) -> None:
	"""Test pmt with begin payments.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	result = financial_math.pmt(0.1, 5, 1000.0, 0.0, "begin")
	expected = npf.pmt(0.1, 5, 1000, 0, "begin")
	assert result == pytest.approx(expected)


def test_pmt_zero_periods(financial_math: FinancialMath) -> None:
	"""Test pmt raises ValueError with zero periods.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	with pytest.raises(ValueError, match="must be positive"):
		financial_math.pmt(0.1, 0, 1000.0)


def test_pmt_negative_periods(financial_math: FinancialMath) -> None:
	"""Test pmt raises ValueError with negative periods.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	"""
	with pytest.raises(ValueError, match="must be positive"):
		financial_math.pmt(0.1, -5, 1000.0)


# --------------------------
# Tests for pv_cfs
# --------------------------
def test_pv_cfs_valid_input(
	financial_math: FinancialMath, sample_cash_flows: NDArray[np.float64]
) -> None:
	"""Test pv_cfs with valid inputs.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	sample_cash_flows : NDArray[np.float64]
		Sample cash flows
	"""
	periods, discounted = financial_math.pv_cfs(sample_cash_flows, 0.1)
	assert len(periods) == len(sample_cash_flows)
	assert len(discounted) == len(sample_cash_flows)
	for period, cf, disc in zip(periods, sample_cash_flows, discounted):
		expected = financial_math.pv(0.1, int(period), cf)
		assert disc == pytest.approx(expected)


def test_pv_cfs_compound_begin(
	financial_math: FinancialMath, sample_cash_flows: NDArray[np.float64]
) -> None:
	"""Test pv_cfs with compound capitalization and begin payments.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	sample_cash_flows : NDArray[np.float64]
		Sample cash flows
	"""
	periods, discounted = financial_math.pv_cfs(sample_cash_flows, 0.1, "compound", "begin")
	for period, cf, disc in zip(periods, sample_cash_flows, discounted):
		expected = financial_math.pv(0.1, int(period), cf, 0.0, "compound", "begin")
		assert disc == pytest.approx(expected)


def test_pv_cfs_simple(
	financial_math: FinancialMath, sample_cash_flows: NDArray[np.float64]
) -> None:
	"""Test pv_cfs with simple capitalization.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	sample_cash_flows : NDArray[np.float64]
		Sample cash flows
	"""
	periods, discounted = financial_math.pv_cfs(sample_cash_flows, 0.1, "simple")
	for period, cf, disc in zip(periods, sample_cash_flows, discounted):
		expected = financial_math.pv(0.1, int(period), cf, 0.0, "simple")
		assert disc == pytest.approx(expected)


def test_pv_cfs_invalid_dimension(
	financial_math: FinancialMath, invalid_cash_flows: NDArray[np.float64]
) -> None:
	"""Test pv_cfs raises ValueError with 2D cash flows.

	Parameters
	----------
	financial_math : FinancialMath
		Instance of FinancialMath class
	invalid_cash_flows : NDArray[np.float64]
		2D cash flows
	"""
	with pytest.raises(ValueError, match="must be a 1D array"):
		financial_math.pv_cfs(invalid_cash_flows, 0.1)
