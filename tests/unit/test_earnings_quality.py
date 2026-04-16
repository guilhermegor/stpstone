"""Unit tests for EarningsManipulation class.

Tests the Beneish M-Score calculation and related financial metrics.
"""

import math

import pytest

from stpstone.analytics.perf_metrics.earnings_quality import EarningsManipulation


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def earnings_analyzer() -> EarningsManipulation:
	"""Fixture providing an EarningsManipulation instance."""
	return EarningsManipulation()


@pytest.fixture
def normal_financial_values() -> dict[str, float]:
	"""Fixture providing typical financial values for testing."""
	return {
		"float_ar_t": 50000.0,
		"float_sales_t": 1000000.0,
		"float_ar_tm1": 45000.0,
		"float_sales_tm1": 900000.0,
		"float_gp_tm1": 300000.0,
		"float_gp_t": 320000.0,
		"float_ppe_t": 200000.0,
		"float_ca_t": 300000.0,
		"float_lti_t": 100000.0,
		"float_lti_tm1": 90000.0,
		"float_ta_t": 1000000.0,
		"float_ppe_tm1": 180000.0,
		"float_ca_tm1": 280000.0,
		"float_ta_tm1": 950000.0,
		"float_dep_tm1": 20000.0,
		"float_dep_t": 25000.0,
		"float_sga_t": 150000.0,
		"float_sga_tm1": 140000.0,
		"float_inc_cont_op": 100000.0,
		"float_cfo_t": 90000.0,
		"float_tl_t": 400000.0,
		"float_tl_tm1": 380000.0,
	}


@pytest.fixture
def negative_financial_values() -> dict[str, float]:
	"""Fixture providing negative financial values for testing.

	Returns
	-------
	dict[str, float]
		A dictionary containing negative financial values.
	"""
	return {
		"float_ar_t": -50000.0,
		"float_sales_t": 1000000.0,
		"float_ar_tm1": 45000.0,
		"float_sales_tm1": -900000.0,
		"float_gp_tm1": -300000.0,
		"float_gp_t": 320000.0,
		"float_ppe_t": 200000.0,
		"float_ca_t": 300000.0,
		"float_lti_t": 100000.0,
		"float_lti_tm1": 90000.0,
		"float_ta_t": 1000000.0,
		"float_ppe_tm1": 180000.0,
		"float_ca_tm1": 280000.0,
		"float_ta_tm1": 950000.0,
		"float_dep_tm1": 20000.0,
		"float_dep_t": 25000.0,
		"float_sga_t": 150000.0,
		"float_sga_tm1": 140000.0,
		"float_inc_cont_op": -100000.0,
		"float_cfo_t": 90000.0,
		"float_tl_t": 400000.0,
		"float_tl_tm1": 380000.0,
	}


@pytest.fixture
def extreme_financial_values() -> dict[str, float]:
	"""Fixture providing extreme financial values for testing.

	Returns
	-------
	dict[str, float]
		A dictionary containing extreme financial values.
	"""
	return {
		"float_ar_t": 1e12,
		"float_sales_t": 1e15,
		"float_ar_tm1": 1e11,
		"float_sales_tm1": 1e14,
		"float_gp_tm1": 1e14,
		"float_gp_t": 1e14,
		"float_ppe_t": 1e12,
		"float_ca_t": 1e12,
		"float_lti_t": 1e11,
		"float_lti_tm1": 1e10,
		"float_ta_t": 1e15,
		"float_ppe_tm1": 1e12,
		"float_ca_tm1": 1e12,
		"float_ta_tm1": 1e15,
		"float_dep_tm1": 1e10,
		"float_dep_t": 1e10,
		"float_sga_t": 1e13,
		"float_sga_tm1": 1e13,
		"float_inc_cont_op": 1e14,
		"float_cfo_t": 1e14,
		"float_tl_t": 1e14,
		"float_tl_tm1": 1e14,
	}


# --------------------------
# Tests
# --------------------------
def test_normal_case(
	earnings_analyzer: EarningsManipulation, normal_financial_values: dict[str, float]
) -> None:
	"""Test with typical financial values.

	Parameters
	----------
	earnings_analyzer : EarningsManipulation
		An instance of the EarningsManipulation class.
	normal_financial_values : dict[str, float]
		A dictionary containing normal financial values.
	"""
	ratios = earnings_analyzer.inputs_beneish_model(**normal_financial_values)

	# verify all ratios are calculated
	assert len(ratios) == 8
	assert ratios["float_dsr"] == pytest.approx(1.0, abs=0.01)
	assert ratios["float_gmi"] == pytest.approx(1.04, abs=0.01)
	assert ratios["float_aqi"] == pytest.approx(0.95, abs=0.01)
	assert ratios["float_sgi"] == pytest.approx(1.11, abs=0.01)
	assert ratios["float_depi"] == pytest.approx(0.90, abs=0.01)
	assert ratios["float_sgai"] == pytest.approx(0.96, abs=0.01)
	assert ratios["float_tata"] == pytest.approx(0.01, abs=0.01)
	assert ratios["float_lvgi"] == pytest.approx(1.0, abs=0.01)

	# test m-score calculation
	m_score = earnings_analyzer.beneish_model(**ratios)
	assert m_score == pytest.approx(-2.34, abs=0.01)


def test_zero_sales(
	earnings_analyzer: EarningsManipulation, normal_financial_values: dict[str, float]
) -> None:
	"""Test handling of zero sales edge case.

	Parameters
	----------
	earnings_analyzer : EarningsManipulation
		An instance of the EarningsManipulation class.
	normal_financial_values : dict[str, float]
		A dictionary containing normal financial values.
	"""
	with pytest.raises(ZeroDivisionError):
		modified_values = normal_financial_values.copy()
		modified_values["float_sales_t"] = 0.0
		earnings_analyzer.inputs_beneish_model(**modified_values)


def test_negative_values(
	earnings_analyzer: EarningsManipulation, negative_financial_values: dict[str, float]
) -> None:
	"""Test with negative financial values (should work mathematically).

	Parameters
	----------
	earnings_analyzer : EarningsManipulation
		An instance of the EarningsManipulation class.
	negative_financial_values : dict[str, float]
		A dictionary containing negative financial values.
	"""
	ratios = earnings_analyzer.inputs_beneish_model(**negative_financial_values)
	assert len(ratios) == 8


def test_type_validation(
	earnings_analyzer: EarningsManipulation, normal_financial_values: dict[str, float]
) -> None:
	"""Test type validation of input parameters.

	Parameters
	----------
	earnings_analyzer : EarningsManipulation
		An instance of the EarningsManipulation class.
	normal_financial_values : dict[str, float]
		A dictionary containing normal financial values.
	"""
	with pytest.raises(TypeError):
		modified_values = normal_financial_values.copy()
		modified_values["float_ar_t"] = "50000"  # type: ignore
		earnings_analyzer.inputs_beneish_model(**modified_values)


def test_extreme_values(
	earnings_analyzer: EarningsManipulation, extreme_financial_values: dict[str, float]
) -> None:
	"""Test with extremely large/small values.

	Parameters
	----------
	earnings_analyzer : EarningsManipulation
		An instance of the EarningsManipulation class.
	extreme_financial_values : dict[str, float]
		A dictionary containing extreme financial values.
	"""
	ratios = earnings_analyzer.inputs_beneish_model(**extreme_financial_values)
	for val in ratios.values():
		assert not math.isnan(val)
		assert not math.isinf(val)


def test_m_score_interpretation(earnings_analyzer: EarningsManipulation) -> None:
	"""Test M-Score interpretation thresholds.

	Parameters
	----------
	earnings_analyzer : EarningsManipulation
		An instance of the EarningsManipulation class.
	"""
	# below threshold (no manipulation expected)
	low_score = earnings_analyzer.beneish_model(
		float_dsr=1.0,
		float_gmi=1.0,
		float_aqi=1.0,
		float_sgi=1.0,
		float_depi=1.0,
		float_sgai=1.0,
		float_tata=0.0,
		float_lvgi=1.0,
	)
	assert low_score < -1.78

	# above threshold (potential manipulation)
	high_score = earnings_analyzer.beneish_model(
		float_dsr=1.5,
		float_gmi=1.5,
		float_aqi=1.5,
		float_sgi=1.5,
		float_depi=1.5,
		float_sgai=0.5,
		float_tata=0.1,
		float_lvgi=1.5,
	)
	assert high_score > -1.78


def test_nan_handling(
	earnings_analyzer: EarningsManipulation, normal_financial_values: dict[str, float]
) -> None:
	"""Test that NaN inputs raise appropriate exceptions.

	Parameters
	----------
	earnings_analyzer : EarningsManipulation
		An instance of the EarningsManipulation class.
	normal_financial_values : dict[str, float]
		A dictionary containing normal financial values.
	"""
	with pytest.raises(ValueError):
		modified_values = normal_financial_values.copy()
		modified_values["float_ar_t"] = float("nan")
		earnings_analyzer.inputs_beneish_model(**modified_values)
