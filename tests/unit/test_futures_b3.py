"""Unit tests for Pricing B3 Brazilian Exchange Future Contracts.

Tests the functionality of MTMFromDailySettlement, MTMFromDailySettlement, RateFromMTM, and TSIR
classes, covering initialization, pricing calculations, edge cases, and type validation.
"""

from datetime import date
import importlib
import sys

import numpy as np
import pytest
from pytest_mock import MockerFixture

from stpstone.analytics.pricing.derivatives.futures_b3 import (
	TSIR,
	MTMFromDailySettlement,
	MTMFromRate,
	RateFromMTM,
)
from stpstone.analytics.quant.linear_transformations import LinearAlgebra
from stpstone.utils.calendars.calendar_br import DatesBRB3
from stpstone.utils.parsers.lists import ListHandler


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def notional_from_pv() -> MTMFromDailySettlement:
	"""Fixture providing MTMFromDailySettlement instance with default parameters.

	Returns
	-------
	MTMFromDailySettlement
		Instance initialized with default cache settings
	"""
	return MTMFromDailySettlement()


@pytest.fixture
def notional_from_rate() -> MTMFromRate:
	"""Fixture providing MTMFromRate instance with default parameters.

	Returns
	-------
	MTMFromRate
		Instance initialized with default cache settings
	"""
	return MTMFromRate()


@pytest.fixture
def rt_from_pv() -> RateFromMTM:
	"""Fixture providing RateFromMTM instance with default parameters.

	Returns
	-------
	RateFromMTM
		Instance initialized with default cache settings
	"""
	return RateFromMTM()


@pytest.fixture
def tsir() -> TSIR:
	"""Fixture providing TSIR instance with default parameters.

	Returns
	-------
	TSIR
		Instance initialized with default cache settings
	"""
	return TSIR()


@pytest.fixture
def sample_dates() -> tuple[date, date, date]:
	"""Fixture providing sample dates for testing.

	Returns
	-------
	tuple[date, date, date]
		Tuple containing last PMI date, reference date, and next PMI date
	"""
	return (date(2023, 5, 15), date(2023, 5, 20), date(2023, 6, 15))


@pytest.fixture
def sample_nper_rates() -> dict[int, float]:
	"""Fixture providing sample period rates for TSIR testing.

	Returns
	-------
	dict[int, float]
		Dictionary mapping periods to rates
	"""
	return {30: 0.015, 90: 0.018, 180: 0.020, 360: 0.022}


# --------------------------
# Tests for MTMFromDailySettlement
# --------------------------
def test_notional_from_pv_init_valid(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Test MTMFromDailySettlement initialization with valid inputs.

	Verifies
	--------
	- Instance attributes are correctly set
	- DatesBRB3 is properly initialized with passed parameters

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance initialized with valid parameters

	Returns
	-------
	None
	"""
	assert notional_from_pv.bool_persist_cache is True
	assert notional_from_pv.bool_reuse_cache is True
	assert isinstance(notional_from_pv.cls_dates_br_b3, DatesBRB3)


def test_notional_from_pv_init_invalid_types() -> None:
	"""Test MTMFromDailySettlement initialization with invalid types.

	Verifies
	--------
	- TypeError is raised for non-boolean cache parameters
	- Error message contains 'must be of type'

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		MTMFromDailySettlement(bool_persist_cache="invalid", bool_reuse_cache=True)
	with pytest.raises(TypeError, match="must be of type"):
		MTMFromDailySettlement(bool_persist_cache=True, bool_reuse_cache=123)
	with pytest.raises(TypeError, match="must be one of types"):
		MTMFromDailySettlement(bool_persist_cache=True, bool_reuse_cache=True, logger="invalid")


def test_generic_pricing_valid_inputs(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Test generic_pricing with valid inputs.

	Verifies
	--------
	- Correct calculation of notional value
	- Proper handling of optional float_xcg_rt_2 parameter
	- Return type is float

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.generic_pricing(
		float_daily_settlement=1000000.0,
		float_size=0.00025,
		float_qty=50.0,
		float_xcg_rate=1.0,
	)
	assert isinstance(result, float)
	assert result == pytest.approx(12500.0, rel=1e-6)


@pytest.mark.parametrize(
	"invalid_input",
	[
		(None, 0.00025, 50.0, 1.0),
		(1000000.0, "invalid", 50.0, 1.0),
		(1000000.0, 0.00025, None, 1.0),
		(1000000.0, 0.00025, 50.0, "invalid"),
		("invalid", 0.00025, 50.0, 1.0),
	],
)
def test_generic_pricing_invalid_types(
	notional_from_pv: MTMFromDailySettlement, invalid_input: tuple
) -> None:
	"""Test generic_pricing with invalid input types.

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class
	invalid_input : tuple
		Tuple of invalid input parameters

	Verifies
	--------
	- TypeError is raised for invalid input types
	- Error message contains 'must be of type'

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		notional_from_pv.generic_pricing(*invalid_input)


def test_dap_valid_inputs(
	notional_from_pv: MTMFromDailySettlement,
	sample_dates: tuple[date, date, date],
	mocker: MockerFixture,
) -> None:
	"""Test DAP pricing with valid inputs.

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class
	sample_dates : tuple[date, date, date]
		Sample dates for last PMI, reference, and next PMI
	mocker : MockerFixture
		Pytest-mock fixture for mocking dependencies

	Verifies
	--------
	- Correct calculation of DAP notional value
	- Proper handling of date calculations
	- Return type is float

	Returns
	-------
	None
	"""
	date_pmi_last, date_ref, date_pmi_next = sample_dates
	mocker.patch.object(DatesBRB3, "delta_working_days", return_value=21)

	result = notional_from_pv.dap(
		float_daily_settlement=1000000.0,
		float_qty=50.0,
		float_pmi_ipca_mm1=52.3,
		float_pmi_ipca_rt_hat=0.04,
		date_pmi_last=date_pmi_last,
		date_ref=date_ref,
		date_pmi_next=date_pmi_next,
	)
	expected = 1000000.0 * 50.0 * 52.3 * 0.00025 * (1.0 + 0.04) ** (21 / 21)
	assert isinstance(result, float)
	assert result == pytest.approx(expected, rel=1e-6)


def test_dap_invalid_dates(
	notional_from_pv: MTMFromDailySettlement, sample_dates: tuple[date, date, date]
) -> None:
	"""Test DAP pricing with invalid date order.

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class
	sample_dates : tuple[date, date, date]
		Sample dates for last PMI, reference, and next PMI

	Verifies
	--------
	- ValueError is raised when date_pmi_last > date_pmi_next
	- Error message matches expected pattern

	Returns
	-------
	None
	"""
	date_pmi_last, date_ref, date_pmi_next = sample_dates
	with pytest.raises(ValueError, match="Please validate the input"):
		notional_from_pv.dap(
			float_daily_settlement=1000000.0,
			float_qty=50.0,
			float_pmi_ipca_mm1=52.3,
			float_pmi_ipca_rt_hat=0.04,
			date_pmi_last=date_pmi_next,
			date_ref=date_ref,
			date_pmi_next=date_pmi_last,
		)


@pytest.mark.parametrize(
	"invalid_input",
	[
		(None, 50.0, 52.3, 0.04, date(2023, 5, 15), date(2023, 5, 20), date(2023, 6, 15)),
		(
			1000000.0,
			"invalid",
			52.3,
			0.04,
			date(2023, 5, 15),
			date(2023, 5, 20),
			date(2023, 6, 15),
		),
		(1000000.0, 50.0, None, 0.04, date(2023, 5, 15), date(2023, 5, 20), date(2023, 6, 15)),
		(
			1000000.0,
			50.0,
			52.3,
			"invalid",
			date(2023, 5, 15),
			date(2023, 5, 20),
			date(2023, 6, 15),
		),
		(1000000.0, 50.0, 52.3, 0.04, None, date(2023, 5, 20), date(2023, 6, 15)),
		(1000000.0, 50.0, 52.3, 0.04, date(2023, 5, 15), None, date(2023, 6, 15)),
		(1000000.0, 50.0, 52.3, 0.04, date(2023, 5, 15), date(2023, 5, 20), None),
	],
)
def test_dap_invalid_types(notional_from_pv: MTMFromDailySettlement, invalid_input: tuple) -> None:
	"""Test DAP pricing with invalid input types.

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class
	invalid_input : tuple
		Tuple of invalid input parameters

	Verifies
	--------
	- TypeError is raised for invalid input types
	- Error message contains 'must be of type'

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		notional_from_pv.dap(*invalid_input)


# --------------------------
# Tests for MTMFromDailySettlement
# --------------------------
def test_notional_from_rate_init_valid(notional_from_rate: MTMFromDailySettlement) -> None:
	"""Test MTMFromDailySettlement initialization with valid inputs.

	Verifies
	--------
	- Instance attributes are correctly set
	- DatesBRB3 is properly initialized with passed parameters

	Parameters
	----------
	notional_from_rate : MTMFromDailySettlement
		Instance initialized with valid parameters

	Returns
	-------
	None
	"""
	assert notional_from_rate.bool_persist_cache is True
	assert notional_from_rate.bool_reuse_cache is True
	assert isinstance(notional_from_rate.cls_dates_br_b3, DatesBRB3)


def test_di1_valid_inputs(
	notional_from_rate: MTMFromDailySettlement,
	sample_dates: tuple[date, date, date],
	mocker: MockerFixture,
) -> None:
	"""Test DI1 pricing with valid inputs.

	Verifies
	--------
	- Correct calculation of DI1 present value
	- Proper handling of date calculations
	- Return type is float

	Parameters
	----------
	notional_from_rate : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class
	sample_dates : tuple[date, date, date]
		Sample dates for testing
	mocker : MockerFixture
		Pytest-mock fixture for mocking dependencies

	Returns
	-------
	None
	"""
	_, date_ref, date_xpt = sample_dates
	mocker.patch.object(DatesBRB3, "delta_working_days", return_value=2)

	result = notional_from_rate.di1(float_nominal_rate=0.10, date_ref=date_ref, date_xpt=date_xpt)
	expected = 100000.0 / (1.0 + ((1.0 + 0.10) ** (2 / 252) - 1.0))
	assert isinstance(result, float)
	assert result == pytest.approx(expected, rel=1e-6)


@pytest.mark.parametrize(
	"invalid_input",
	[
		(None, date(2023, 5, 20), date(2023, 12, 1)),
		(0.10, None, date(2023, 12, 1)),
		(0.10, date(2023, 5, 20), None),
	],
)
def test_di1_invalid_types(
	notional_from_rate: MTMFromDailySettlement, invalid_input: tuple
) -> None:
	"""Test DI1 pricing with invalid input types.

	Verifies
	--------
	- TypeError is raised for invalid input types
	- Error message contains 'must be of type'

	Parameters
	----------
	notional_from_rate : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class
	invalid_input : tuple
		Tuple of invalid input parameters

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		notional_from_rate.di1(*invalid_input)


# --------------------------
# Tests for RateFromMTM
# --------------------------
def test_rt_from_pv_init_valid(rt_from_pv: RateFromMTM) -> None:
	"""Test RateFromMTM initialization with valid inputs.

	Verifies
	--------
	- Instance attributes are correctly set
	- DatesBRB3 is properly initialized with passed parameters

	Parameters
	----------
	rt_from_pv : RateFromMTM
		Instance initialized with valid parameters

	Returns
	-------
	None
	"""
	assert rt_from_pv.bool_persist_cache is True
	assert rt_from_pv.bool_reuse_cache is True
	assert isinstance(rt_from_pv.cls_dates_br_b3, DatesBRB3)


def test_ddi_valid_inputs(
	rt_from_pv: RateFromMTM, sample_dates: tuple[date, date, date], mocker: MockerFixture
) -> None:
	"""Test DDI pricing with valid inputs.

	Parameters
	----------
	rt_from_pv : RateFromMTM
		Instance of RateFromMTM class
	sample_dates : tuple[date, date, date]
		Sample dates for testing
	mocker : MockerFixture
		Pytest-mock fixture for mocking dependencies

	Verifies
	--------
	- Correct calculation of DDI real rate
	- Proper handling of date calculations
	- Return type is float

	Returns
	-------
	None
	"""
	_, date_ref, date_xpt = sample_dates
	mocker.patch.object(DatesBRB3, "delta_calendar_days", return_value=2)

	result = rt_from_pv.ddi(
		float_mtm_di1=95000.0,
		float_mtm_dol=1.0,
		float_ptax_dm1=5.20,
		date_ref=date_ref,
		date_xpt=date_xpt,
	)
	expected = ((95000.0 / 100000.0) / (1.0 / 5.20) - 1.0) * 365 / 2
	assert isinstance(result, float)
	assert result == pytest.approx(expected, rel=1e-6)


@pytest.mark.parametrize(
	"invalid_input",
	[
		(None, 1.0, 5.20, date(2023, 5, 20), date(2023, 12, 1)),
		(95000.0, None, 5.20, date(2023, 5, 20), date(2023, 12, 1)),
		(95000.0, 1.0, None, date(2023, 5, 20), date(2023, 12, 1)),
		(95000.0, 1.0, 5.20, None, date(2023, 12, 1)),
		(95000.0, 1.0, 5.20, date(2023, 5, 20), None),
	],
)
def test_ddi_invalid_types(rt_from_pv: RateFromMTM, invalid_input: tuple) -> None:
	"""Test DDI pricing with invalid input types.

	Parameters
	----------
	rt_from_pv : RateFromMTM
		Instance of RateFromMTM class
	invalid_input : tuple
		Tuple of invalid input parameters

	Verifies
	--------
	- TypeError is raised for invalid input types
	- Error message contains 'must be of type'

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		rt_from_pv.ddi(*invalid_input)


# --------------------------
# Tests for TSIR
# --------------------------
def test_tsir_init_valid(tsir: TSIR) -> None:
	"""Test TSIR initialization with valid inputs.

	Verifies
	--------
	- Instance attributes are correctly set
	- DatesBRB3 is properly initialized with passed parameters

	Parameters
	----------
	tsir : TSIR
		Instance initialized with valid parameters

	Returns
	-------
	None
	"""
	assert isinstance(tsir.cls_list_handler, ListHandler)


def test_flat_forward_valid_inputs(tsir: TSIR, sample_nper_rates: dict[int, float]) -> None:
	"""Test flat_forward with valid inputs.

	Parameters
	----------
	tsir : TSIR
		Instance of TSIR class
	sample_nper_rates : dict[int, float]
		Sample period rates for testing

	Verifies
	--------
	- Correct calculation of flat forward rates
	- Return type is dict[int, float]
	- All keys are integers and values are floats

	Returns
	-------
	None
	"""
	result = tsir.flat_forward(sample_nper_rates)
	assert isinstance(result, dict)
	assert all(isinstance(k, int) for k in result)
	assert all(isinstance(v, float) for v in result.values())
	assert len(result) == 331  # From 30 to 360 inclusive
	assert result[30] == pytest.approx(0.015, rel=1e-6)


@pytest.mark.parametrize(
	"invalid_input",
	[
		{30: "invalid", 90: 0.018},
		{"30": 0.015, 90: 0.018},
		{30: 0.015},  # Too few points
	],
)
def test_flat_forward_invalid_inputs(tsir: TSIR, invalid_input: dict) -> None:
	"""Test flat_forward with invalid inputs.

	Parameters
	----------
	tsir : TSIR
		Instance of TSIR class
	invalid_input : dict
		Invalid input dictionary

	Verifies
	--------
	- TypeError for invalid types
	- ValueError for insufficient points

	Returns
	-------
	None
	"""
	with pytest.raises((TypeError, ValueError)):
		tsir.flat_forward(invalid_input)


def test_cubic_spline_valid_inputs(tsir: TSIR, sample_nper_rates: dict[int, float]) -> None:
	"""Test cubic_spline with valid inputs.

	Parameters
	----------
	tsir : TSIR
		Instance of TSIR class
	sample_nper_rates : dict[int, float]
		Sample period rates for testing

	Verifies
	--------
	- Correct interpolation of rates
	- Return type is dict[int, float]
	- All keys are integers and values are floats

	Returns
	-------
	None
	"""
	result = tsir.cubic_spline(sample_nper_rates)
	assert isinstance(result, dict)
	assert all(np.issubdtype(type(k), np.integer) for k in result)
	assert all(isinstance(v, float) for v in result.values())
	assert len(result) == 331  # From 30 to 360 inclusive
	assert result[30] == pytest.approx(0.015, rel=1e-6)


@pytest.mark.parametrize(
	"invalid_input",
	[
		{30: 0.015, 90: 0.018},  # Too few points
		{90: 0.018, 30: 0.015},  # Unsorted keys
		{30: "invalid", 90: 0.018, 180: 0.020},
		{"30": 0.015, 90: 0.018, 180: 0.020},
	],
)
def test_cubic_spline_invalid_inputs(tsir: TSIR, invalid_input: dict) -> None:
	"""Test cubic_spline with invalid inputs.

	Parameters
	----------
	tsir : TSIR
		Instance of TSIR class
	invalid_input : dict
		Invalid input dictionary

	Verifies
	--------
	- ValueError for insufficient points or unsorted keys
	- TypeError for invalid types

	Returns
	-------
	None
	"""
	with pytest.raises((ValueError, TypeError)):
		tsir.cubic_spline(invalid_input)


def test_third_degree_polynomial_cubic_spline_valid(tsir: TSIR) -> None:
	"""Test third_degree_polynomial_cubic_spline with valid inputs.

	Parameters
	----------
	tsir : TSIR
		Instance of TSIR class

	Verifies
	--------
	- Correct evaluation of polynomial segments
	- Proper handling of lower and upper segments
	- Return type is float

	Returns
	-------
	None
	"""
	coeffs = [0.02, 0.001, -0.0001, 0.00001, 0.025, -0.0005, 0.00002, -0.000001]
	result_lower = tsir.third_degree_polynomial_cubic_spline(coeffs, 30, False)
	result_upper = tsir.third_degree_polynomial_cubic_spline(coeffs, 45, True)
	assert isinstance(result_lower, float)
	assert isinstance(result_upper, float)
	assert result_lower == pytest.approx(0.23, rel=1e-6)
	assert result_upper == pytest.approx(-0.0481249999, rel=1e-6)


def test_third_degree_polynomial_cubic_spline_invalid_length(tsir: TSIR) -> None:
	"""Test third_degree_polynomial_cubic_spline with invalid coefficient length.

	Parameters
	----------
	tsir : TSIR
		Instance of TSIR class

	Verifies
	--------
	- Exception is raised for incorrect number of coefficients
	- Error message matches expected pattern

	Returns
	-------
	None
	"""
	with pytest.raises(Exception, match="Poor defined list of constants"):
		tsir.third_degree_polynomial_cubic_spline([0.02, 0.001], 30, False)


@pytest.mark.parametrize(
	"invalid_input",
	[
		([0.02, 0.001, -0.0001, 0.00001, 0.025, -0.0005, 0.00002, -0.000001], "invalid", False),
		([0.02, 0.001, -0.0001, 0.00001, 0.025, -0.0005, 0.00002, -0.000001], 30, "invalid"),
		("invalid", 30, False),
	],
)
def test_third_degree_polynomial_cubic_spline_invalid_types(
	tsir: TSIR, invalid_input: tuple
) -> None:
	"""Test third_degree_polynomial_cubic_spline with invalid input types.

	Parameters
	----------
	tsir : TSIR
		Instance of TSIR class
	invalid_input : tuple
		Tuple of invalid input parameters

	Verifies
	--------
	- TypeError is raised for invalid input types
	- Error message contains 'must be of type'

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		tsir.third_degree_polynomial_cubic_spline(*invalid_input)


def test_literal_cubic_spline_valid(
	tsir: TSIR, sample_nper_rates: dict[int, float], mocker: MockerFixture
) -> None:
	"""Test literal_cubic_spline with valid inputs.

	Parameters
	----------
	tsir : TSIR
		Instance of TSIR class
	sample_nper_rates : dict[int, float]
		Sample period rates for testing
	mocker : MockerFixture
		Pytest-mock fixture for mocking dependencies

	Verifies
	--------
	- Correct interpolation of rates
	- Return type is dict[int, float]
	- All keys are integers and values are floats

	Returns
	-------
	None
	"""
	mocker.patch.object(
		ListHandler,
		"get_lower_mid_upper_bound",
		return_value={
			"lower_bound": 30,
			"middle_bound": 90,
			"upper_bound": 180,
			"end_of_list": False,
		},
	)
	mocker.patch.object(
		LinearAlgebra,
		"matrix_multiplication",
		return_value=[0.02, 0.001, -0.0001, 0.00001, 0.025, -0.0005, 0.00002, -0.000001],
	)

	result = tsir.literal_cubic_spline(sample_nper_rates)
	assert isinstance(result, dict)
	assert all(isinstance(k, int) for k in result)
	assert all(isinstance(v, float) for v in result.values())
	assert len(result) == 331  # From 30 to 360 inclusive


def test_literal_cubic_spline_invalid_bound(
	tsir: TSIR, sample_nper_rates: dict[int, float], mocker: MockerFixture
) -> None:
	"""Test literal_cubic_spline with invalid bound dictionary.

	Parameters
	----------
	tsir : TSIR
		Instance of TSIR class
	sample_nper_rates : dict[int, float]
		Sample period rates for testing
	mocker : MockerFixture
		Pytest-mock fixture for mocking dependencies

	Verifies
	--------
	- Exception is raised for invalid bound dictionary
	- Error message matches expected pattern

	Returns
	-------
	None
	"""
	mocker.patch.object(ListHandler, "get_lower_mid_upper_bound", return_value={})
	with pytest.raises(Exception, match="Dimension-wise the list ought have 4 positions"):
		tsir.literal_cubic_spline(sample_nper_rates)


def test_nelson_siegel_valid(
	tsir: TSIR, sample_nper_rates: dict[int, float], mocker: MockerFixture
) -> None:
	"""Test nelson_siegel with valid inputs.

	Parameters
	----------
	tsir : TSIR
		Instance of TSIR class
	sample_nper_rates : dict[int, float]
		Sample period rates for testing
	mocker : MockerFixture
		Pytest-mock fixture for mocking dependencies

	Verifies
	--------
	- Correct interpolation of rates
	- Return type is dict[int, float]
	- All keys are integers and values are floats

	Returns
	-------
	None
	"""
	mocker.patch(
		"nelson_siegel_svensson.calibrate.calibrate_ns_ols",
		return_value=(lambda x: np.array([0.015] * len(x)), None),
	)

	result = tsir.nelson_siegel(sample_nper_rates)
	assert isinstance(result, dict)
	assert all(isinstance(k, int) for k in result)
	assert all(isinstance(v, float) for v in result.values())
	assert len(result) == 330  # From 30 to 359 inclusive


# --------------------------
# Reload Tests
# --------------------------
def test_module_reload() -> None:
	"""Test module reloading behavior.

	Verifies
	--------
	- Module can be reloaded without errors
	- Classes are properly redefined
	- No state corruption after reload

	Returns
	-------
	None
	"""
	original_notional = MTMFromDailySettlement()
	importlib.reload(sys.modules["stpstone.analytics.pricing.derivatives.futures_b3"])
	reloaded_notional = MTMFromDailySettlement()
	assert isinstance(reloaded_notional, MTMFromDailySettlement)
	assert reloaded_notional.bool_persist_cache == original_notional.bool_persist_cache


# --------------------------
# Edge Cases
# --------------------------
def test_dap_zero_values(
	notional_from_pv: MTMFromDailySettlement,
	sample_dates: tuple[date, date, date],
	mocker: MockerFixture,
) -> None:
	"""Test DAP pricing with zero values.

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class
	sample_dates : tuple[date, date, date]
		Sample dates for testing
	mocker : MockerFixture
		Pytest-mock fixture

	Verifies
	--------
	- Proper handling of zero inputs
	- Correct calculation with zero values

	Returns
	-------
	None
	"""
	mocker.patch("locale.setlocale")
	date_pmi_last, date_ref, date_pmi_next = sample_dates
	result = notional_from_pv.dap(
		float_daily_settlement=0.0,
		float_qty=0.0,
		float_pmi_ipca_mm1=0.0,
		float_pmi_ipca_rt_hat=0.0,
		date_pmi_last=date_pmi_last,
		date_ref=date_ref,
		date_pmi_next=date_pmi_next,
	)
	assert result == 0.0


def test_cubic_spline_edge_cases(tsir: TSIR) -> None:
	"""Test cubic_spline with edge case inputs.

	Parameters
	----------
	tsir : TSIR
		Instance of TSIR class

	Verifies
	--------
	- Proper handling of minimum points
	- Correct behavior at boundary points

	Returns
	-------
	None
	"""
	minimal_rates = {1: 0.01, 2: 0.02, 3: 0.03}
	result = tsir.cubic_spline(minimal_rates)
	assert len(result) == 3
	assert result[1] == pytest.approx(0.01, rel=1e-6)
	assert result[3] == pytest.approx(0.03, rel=1e-6)


# --------------------------
# Example Tests from Docstrings
# --------------------------
def test_dap_docstring_example(
	notional_from_pv: MTMFromDailySettlement, mocker: MockerFixture
) -> None:
	"""Test DAP pricing using example from docstring.

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class
	mocker : MockerFixture
		Pytest-mock fixture for mocking dependencies

	Verifies
	--------
	- Matches docstring example output
	- Correct calculation using example inputs

	Returns
	-------
	None
	"""
	mocker.patch.object(DatesBRB3, "delta_working_days", return_value=21)

	result = notional_from_pv.dap(
		float_daily_settlement=1_000_000.0,
		float_qty=50.0,
		float_pmi_ipca_mm1=52.3,
		float_pmi_ipca_rt_hat=0.04,
		date_pmi_last=date(2023, 5, 15),
		date_ref=date(2023, 5, 20),
		date_pmi_next=date(2023, 6, 15),
	)
	assert result == pytest.approx(679900.0, rel=1e-6)


def test_di1_docstring_example(
	notional_from_rate: MTMFromDailySettlement, mocker: MockerFixture
) -> None:
	"""Test DI1 pricing using example from docstring.

	Parameters
	----------
	notional_from_rate : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class
	mocker : MockerFixture
		Pytest-mock fixture for mocking dependencies

	Verifies
	--------
	- Matches docstring example output
	- Correct calculation using example inputs

	Returns
	-------
	None
	"""
	mocker.patch.object(DatesBRB3, "delta_working_days", return_value=2)

	result = notional_from_rate.di1(
		float_nominal_rate=0.10, date_ref=date(2023, 5, 20), date_xpt=date(2023, 12, 1)
	)
	assert result == pytest.approx(99924.38560226014, rel=1e-6)


def test_ddi_docstring_example(rt_from_pv: RateFromMTM, mocker: MockerFixture) -> None:
	"""Test DDI pricing using example from docstring.

	Parameters
	----------
	rt_from_pv : RateFromMTM
		Instance of RateFromMTM class
	mocker : MockerFixture
		Pytest-mock fixture for mocking dependencies

	Verifies
	--------
	- Matches docstring example output
	- Correct calculation using example inputs

	Returns
	-------
	None
	"""
	mocker.patch.object(DatesBRB3, "delta_calendar_days", return_value=2)

	result = rt_from_pv.ddi(
		float_mtm_di1=95000.0,
		float_mtm_dol=1.0,
		float_ptax_dm1=5.20,
		date_ref=date(2023, 5, 20),
		date_xpt=date(2023, 12, 1),
	)
	assert result == pytest.approx(719.0500000000001, rel=1e-6)


def test_third_degree_polynomial_cubic_spline_docstring_example(tsir: TSIR) -> None:
	"""Test third_degree_polynomial_cubic_spline using example from docstring.

	Parameters
	----------
	tsir : TSIR
		Instance of TSIR class

	Verifies
	--------
	- Matches docstring example output
	- Correct calculation using example inputs

	Returns
	-------
	None
	"""
	coeffs = [0.02, 0.001, -0.0001, 0.00001, 0.025, -0.0005, 0.00002, -0.000001]
	result_lower = tsir.third_degree_polynomial_cubic_spline(coeffs, 30, False)
	result_upper = tsir.third_degree_polynomial_cubic_spline(coeffs, 45, True)
	assert result_lower == pytest.approx(0.23, rel=1e-6)
	assert result_upper == pytest.approx(-0.0481249999, rel=1e-6)


# --------------------------
# Examples of Delta Daily MTMs
# --------------------------


def test_abevo_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for ABEVOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for ABEVOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.abevo(
		float_daily_settlement=12.45, float_qty=1.0
	) - notional_from_pv.abevo(float_daily_settlement=12.35, float_qty=1.0)
	expected = 0.10
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_afs_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for AFSU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for AFSU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.afs(
		float_daily_settlement=17_656.70, float_qty=1.0, float_xcg_zarbrl=0.3074
	) - notional_from_pv.afs(
		float_daily_settlement=17_706.80, float_qty=1.0, float_xcg_zarbrl=0.3074
	)
	expected = 154.00  # correct: 154.10
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_arb_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for ARBU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for ARBU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.arb(
		float_daily_settlement=4.08, float_qty=1.0
	) - notional_from_pv.arb(float_daily_settlement=4.0530, float_qty=1.0)
	expected = 4.05
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_ars_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for ARSU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for ARSU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.ars(
		float_daily_settlement=1_330_099.90,
		float_qty=1.0,
		float_xcg_usdbrl=5.4241,
		float_xcg_parity_usdars=1322.0,
	) - notional_from_pv.ars(
		float_daily_settlement=1_334_418.80,
		float_qty=1.0,
		float_xcg_usdbrl=5.4241,
		float_xcg_parity_usdars=1322.0,
	)
	expected = 177.20  # correct: 174.17
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_aud_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for AUDU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for AUDU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.aud(
		float_daily_settlement=3_563.5880, float_qty=1.0
	) - notional_from_pv.aud(float_daily_settlement=3_547.1410, float_qty=1.0)
	expected = 986.82
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_aus_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for AUSU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for AUSU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.aus(
		float_daily_settlement=654.489, float_qty=1.0, float_xcg_usdbrl=5.4241
	) - notional_from_pv.aus(
		float_daily_settlement=653.508, float_qty=1.0, float_xcg_usdbrl=5.4241
	)
	expected = 53.21
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_b3sao_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for B3SAOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for B3SAOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.b3sao(
		float_daily_settlement=13.09, float_qty=1.0
	) - notional_from_pv.b3sao(float_daily_settlement=13.15, float_qty=1.0)
	expected = 0.06
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_bbaso_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for BBASOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for BBASOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.bbaso(
		float_daily_settlement=21.57, float_qty=1.0
	) - notional_from_pv.bbaso(float_daily_settlement=21.24, float_qty=1.0)
	expected = 0.33
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_bbdcp_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for BBDCPU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for BBDCPU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.bbdcp(
		float_daily_settlement=16.97, float_qty=1.0
	) - notional_from_pv.bbdcp(float_daily_settlement=16.94, float_qty=1.0)
	expected = 0.03
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_bgi_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for BGIU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for BGIU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.bgi(
		float_daily_settlement=318.60, float_qty=1.0
	) - notional_from_pv.bgi(float_daily_settlement=316.95, float_qty=1.0)
	expected = 544.50
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_bhiao_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for BHIAOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for BHIAOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.bhiao(
		float_daily_settlement=4.19, float_qty=1.0
	) - notional_from_pv.bhiao(float_daily_settlement=3.42, float_qty=1.0)
	expected = 0.77
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_bit_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for BITU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for BITOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.bit(
		float_daily_settlement=594_749.95, float_qty=1.0
	) - notional_from_pv.bit(float_daily_settlement=615_852.79, float_qty=1.0)
	expected = 211.02
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_bpaci_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for BPACIU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for BPACIU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.bpaci(
		float_daily_settlement=45.23, float_qty=1.0
	) - notional_from_pv.bpaci(float_daily_settlement=44.89, float_qty=1.0)
	expected = 0.34
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_bri_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for BRIV25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for BRIV25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.bri(
		float_daily_settlement=23_965.0, float_qty=1.0
	) - notional_from_pv.bri(float_daily_settlement=23_909.0, float_qty=1.0)
	expected = 560.0
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_cad_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for CADU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for CADU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.cad(
		float_daily_settlement=3_971.3120, float_qty=1.0
	) - notional_from_pv.cad(float_daily_settlement=3_953.7050, float_qty=1.0)
	expected = 1_056.42
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_can_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for CANU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for CANU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.can(
		float_daily_settlement=1_371.465,
		float_qty=1.0,
		float_xcg_usdbrl=5.4241,
		float_xcg_usdcad=1.3731,
	) - notional_from_pv.can(
		float_daily_settlement=1_373.304,
		float_qty=1.0,
		float_xcg_usdbrl=5.4241,
		float_xcg_usdcad=1.3731,
	)
	expected = 72.65  # correct: 72.66
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_ccm_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for CCMU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for CCMU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.ccm(
		float_daily_settlement=65.49, float_qty=1.0
	) - notional_from_pv.ccm(float_daily_settlement=65.12, float_qty=1.0)
	expected = 166.50
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_chf_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for CHFU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for CHFU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.chf(
		float_daily_settlement=6_823.1030, float_qty=1.0
	) - notional_from_pv.chf(float_daily_settlement=6_792.4540, float_qty=1.0)
	expected = 1_532.45
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_chl_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for CHLU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for CHLU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.chl(
		float_daily_settlement=965_280.0,
		float_qty=1.0,
		float_xcg_usdbrl=5.4241,
		float_xcg_usdclp=963.50,
	) - notional_from_pv.chl(
		float_daily_settlement=966_699.0,
		float_qty=1.0,
		float_xcg_usdbrl=5.4241,
		float_xcg_usdclp=963.50,
	)
	expected = 79.88  # correct: 79.69
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_clp_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for CLPU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for CLPU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.clp(
		float_daily_settlement=5_634.8320, float_qty=1.0
	) - notional_from_pv.clp(float_daily_settlement=5_594.5090, float_qty=1.0)
	expected = 1_008.07
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_cmigp_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for CMIGPU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for CMIGPU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.cmigp(
		float_daily_settlement=11.20, float_qty=1.0
	) - notional_from_pv.cmigp(float_daily_settlement=11.22, float_qty=1.0)
	expected = 0.02
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_cnh_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for CNHU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for CNHU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.cnh(
		float_daily_settlement=7_109.673, float_qty=1.0, float_xcg_cnybrl=0.7610
	) - notional_from_pv.cnh(
		float_daily_settlement=7_107.286, float_qty=1.0, float_xcg_cnybrl=0.7610
	)
	expected = 18.1650  # correct: 18.18
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_cnl_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for CHLU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for CHLU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.cnl(
		float_daily_settlement=1_555.0, float_qty=1.0
	) - notional_from_pv.cnl(float_daily_settlement=1_489.23, float_qty=1.0)
	expected = 6_577.0
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_cny_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for CNYU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for CNYU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.cny(
		float_daily_settlement=7_637.1280, float_qty=1.0
	) - notional_from_pv.cny(float_daily_settlement=7_623.9200, float_qty=1.0)
	expected = 462.28
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_cogno_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for COGNOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for COGNOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.cogno(
		float_daily_settlement=2.94, float_qty=1.0
	) - notional_from_pv.cogno(float_daily_settlement=2.95, float_qty=1.0)
	expected = 0.01
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_csano_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for CSANOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for CSANOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.csano(
		float_daily_settlement=5.90, float_qty=1.0
	) - notional_from_pv.csano(float_daily_settlement=5.77, float_qty=1.0)
	expected = 0.13
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_csnao_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for CSNAOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for CSNAOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.csnao(
		float_daily_settlement=7.67, float_qty=1.0
	) - notional_from_pv.csnao(float_daily_settlement=7.80, float_qty=1.0)
	expected = 0.13
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_dap_delta_mtm(notional_from_pv: MTMFromDailySettlement, mocker: MockerFixture) -> None:
	"""Example test for DAPU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for DAPU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class
	mocker : MockerFixture
		Pytest-mock fixture

	Returns
	-------
	None
	"""
	mocker.patch("locale.setlocale")
	result = notional_from_pv.dap(
		float_daily_settlement=99_327.95,
		float_qty=1.0,
		float_pmi_ipca_mm1=7331.98,
		float_pmi_ipca_rt_hat=-0.0012,
		date_pmi_last=date(2025, 8, 12),
		date_ref=date(2025, 8, 29),
		date_pmi_next=date(2025, 9, 10),
	) - notional_from_pv.dap(
		float_daily_settlement=99_327.91,
		float_qty=1.0,
		float_pmi_ipca_mm1=7331.98,
		float_pmi_ipca_rt_hat=-0.0012,
		date_pmi_last=date(2025, 8, 12),
		date_ref=date(2025, 8, 29),
		date_pmi_next=date(2025, 9, 10),
	)
	expected = 0.07
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_dax_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for DAXU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for DAXU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.dax(
		float_daily_settlement=23_958.0,
		float_qty=1.0,
		float_xcg_usdbrl=5.4241,
		float_xcg_parity_eurusd=1.1696,
	) - notional_from_pv.dax(
		float_daily_settlement=24_079.0,
		float_qty=1.0,
		float_xcg_usdbrl=5.4241,
		float_xcg_parity_eurusd=1.1696,
	)
	expected = 3_838.13  # correct: 3_838.46
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_dco_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for DCOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for DCOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.dco(
		float_daily_settlement=100_220.08, float_qty=1.0, float_xcg_usdbrl=5.4241
	) - notional_from_pv.dco(
		float_daily_settlement=99_884.03, float_qty=1.0, float_xcg_usdbrl=5.4241
	)
	expected = 911.38  # correct: 909.26
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_ddi_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for DDIU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for DDIU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.ddi(
		float_daily_settlement=100_220.08, float_qty=1.0, float_xcg_usdbrl=5.4241
	) - notional_from_pv.ddi(
		float_daily_settlement=99_884.03, float_qty=1.0, float_xcg_usdbrl=5.4241
	)
	expected = 911.38  # 909.26
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_di1_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for DI1U25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for DI1U25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.di1(
		float_daily_settlement=99_944.90, float_qty=1.0
	) - notional_from_pv.di1(float_daily_settlement=99_944.87, float_qty=1.0)
	expected = 0.03
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_dol_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for DOLU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for DOLU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.dol(
		float_daily_settlement=5_426.40, float_qty=1.0
	) - notional_from_pv.dol(float_daily_settlement=5_408.2060, float_qty=1.0)
	expected = 909.70
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_eleto_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for ELETOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for ELETOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.eleto(
		float_daily_settlement=45.39, float_qty=1.0
	) - notional_from_pv.eleto(float_daily_settlement=44.81, float_qty=1.0)
	expected = 0.58
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_embro_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for EMBROU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for EMBROU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.embro(
		float_daily_settlement=76.82, float_qty=1.0
	) - notional_from_pv.embro(float_daily_settlement=76.87, float_qty=1.0)
	expected = 0.05
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_enevo_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for ENEVOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for ENEVOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.enevo(
		float_daily_settlement=15.24, float_qty=1.0
	) - notional_from_pv.enevo(float_daily_settlement=15.27, float_qty=1.0)
	expected = 0.03
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_eqtlo_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for EQTLOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for EQTLOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.eqtlo(
		float_daily_settlement=36.87, float_qty=1.0
	) - notional_from_pv.eqtlo(float_daily_settlement=36.96, float_qty=1.0)
	expected = 0.09
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_est_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for ESTU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for ESTU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.est(
		float_daily_settlement=99_898.506,
		float_qty=1.0,
		float_xcg_usdbrl=5.4241,
		float_xcg_parity_eurusd=1.1696,
	) - notional_from_pv.est(
		float_daily_settlement=99_898.567,
		float_qty=1.0,
		float_xcg_usdbrl=5.4241,
		float_xcg_parity_eurusd=1.1696,
	)
	expected = 0.07
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_esx_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for ESXU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for ESXU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.esx(
		float_daily_settlement=5_362.00, float_qty=1.0, float_xcg_eurbrl=6.3467
	) - notional_from_pv.esx(
		float_daily_settlement=5_405.00, float_qty=1.0, float_xcg_eurbrl=6.3467
	)
	expected = 2_729.08  # correct: 2_728.16
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_eth_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for ETHU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for ETHU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.eth(
		float_daily_settlement=2_885.50, float_qty=1.0
	) - notional_from_pv.eth(float_daily_settlement=2_859.00, float_qty=1.0)
	expected = 795.00
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_etr_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for ETRU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for ETRU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.etr(
		float_daily_settlement=4_367.85, float_qty=1.0, float_xcg_usdbrl=5.4241
	) - notional_from_pv.etr(
		float_daily_settlement=4_453.07, float_qty=1.0, float_xcg_usdbrl=5.4241
	)
	expected = 115.56
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_eup_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for EUPU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for EUPU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.eup(
		float_daily_settlement=1_171.178, float_qty=1.0, float_xcg_usdbrl=5.4241
	) - notional_from_pv.eup(
		float_daily_settlement=1_170.144, float_qty=1.0, float_xcg_usdbrl=5.4241
	)
	expected = 56.08
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_eur_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for EURU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for EURU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.eur(
		float_daily_settlement=6_376.8720, float_qty=1.0
	) - notional_from_pv.eur(float_daily_settlement=6_351.368, float_qty=1.0)
	expected = 1_275.20
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_frc_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for FRCX25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for FRCX25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.frc(
		float_daily_settlement=5.38, float_qty=1.0
	) - notional_from_pv.frc(float_daily_settlement=5.34, float_qty=1.0)
	expected = 0.02
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_fro_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for FROX25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for FROX25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.fro(
		float_daily_settlement=5.38, float_qty=1.0
	) - notional_from_pv.fro(float_daily_settlement=5.34, float_qty=1.0)
	expected = 0.02
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_gbp_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for GBPU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for GBPU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.gbp(
		float_daily_settlement=7_356.6240, float_qty=1.0
	) - notional_from_pv.gbp(float_daily_settlement=7_336.4290, float_qty=1.0)
	expected = 706.82
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_gbr_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for GBRU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for GBRU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.gbr(
		float_daily_settlement=1_351.119, float_qty=1.0, float_xcg_usdbrl=5.4241
	) - notional_from_pv.gbr(
		float_daily_settlement=1_351.627, float_qty=1.0, float_xcg_usdbrl=5.4241
	)
	expected = 27.55
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_ggbrp_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for GGBRPU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for GGBRPU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.ggbrp(
		float_daily_settlement=16.78, float_qty=1.0
	) - notional_from_pv.ggbrp(float_daily_settlement=16.93, float_qty=1.0)
	expected = 0.15
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_gld_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for GLDU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for GLDU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.gld(
		float_daily_settlement=3_446.50, float_qty=1.0, float_xcg_usdbrl=5.4241
	) - notional_from_pv.gld(
		float_daily_settlement=3_425.00, float_qty=1.0, float_xcg_usdbrl=5.4241
	)
	expected = 116.61
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_hapvo_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for HAPVOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for HAPVOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.hapvo(
		float_daily_settlement=42.06, float_qty=1.0
	) - notional_from_pv.hapvo(float_daily_settlement=41.26, float_qty=1.0)
	expected = 0.80
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_hsi_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for HSIU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for HSIU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.hsi(
		float_daily_settlement=25_023.0, float_qty=1.0
	) - notional_from_pv.hsi(float_daily_settlement=24_906.0, float_qty=1.0)
	expected = 76.05
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_hypeo_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for HYPEOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for HYPEOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.hypeo(
		float_daily_settlement=24.60, float_qty=1.0
	) - notional_from_pv.hypeo(float_daily_settlement=24.51, float_qty=1.0)
	expected = 0.09
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_icf_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for ICFU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for ICFU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.icf(
		float_daily_settlement=493.45, float_qty=1.0, float_xcg_usdbrl=5.4241
	) - notional_from_pv.icf(float_daily_settlement=481.40, float_qty=1.0, float_xcg_usdbrl=5.4241)
	expected = 6_536.04
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_imv_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for IMVU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for IMVU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.imv(
		float_daily_settlement=2_009_731.00, float_qty=1.0, float_xcg_arsbrl=0.00412
	) - notional_from_pv.imv(
		float_daily_settlement=2_009_731.00, float_qty=1.0, float_xcg_arsbrl=0.00412
	)
	expected = 0.0
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_ind_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for INDU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for INDU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.ind(
		float_daily_settlement=143_883.00, float_qty=1.0
	) - notional_from_pv.ind(float_daily_settlement=143_611.00, float_qty=1.0)
	expected = 272.00
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_isp_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for ISPU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for ISPU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.isp(
		float_daily_settlement=6_472.00, float_qty=1.0, float_xcg_usdbrl=5.4241
	) - notional_from_pv.isp(
		float_daily_settlement=6_518.75, float_qty=1.0, float_xcg_usdbrl=5.4241
	)
	expected = 12_678.83
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_itsap_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for ITSAPU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for ITSAPU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.itsap(
		float_daily_settlement=11.31, float_qty=1.0
	) - notional_from_pv.itsap(float_daily_settlement=11.28, float_qty=1.0)
	expected = 0.03
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_itubp_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for ITUBPU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for ITUBPU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.itubp(
		float_daily_settlement=38.81, float_qty=1.0
	) - notional_from_pv.itubp(float_daily_settlement=38.69, float_qty=1.0)
	expected = 0.12
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_jap_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for JAPU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for JAPU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.jap(
		float_daily_settlement=146_742.169,
		float_qty=1.0,
		float_xcg_usbrl=5.4241,
		float_xcg_parity_usdjpy=146.93,
	) - notional_from_pv.jap(
		float_daily_settlement=146_509.816,
		float_qty=1.0,
		float_xcg_usbrl=5.4241,
		float_xcg_parity_usdjpy=146.93,
	)
	expected = 85.77  # correct: 85.72
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_jpy_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for JPYU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for JPY
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.jpy(
		float_daily_settlement=3_710.4780, float_qty=1.0
	) - notional_from_pv.jpy(float_daily_settlement=3_704.7690, float_qty=1.0)
	expected = 285.45
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_jse_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for JSEU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for JSEU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.jse(
		float_daily_settlement=94_135.0, float_qty=1.0
	) - notional_from_pv.jse(float_daily_settlement=94_246.0, float_qty=1.0)
	expected = 44.40
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_klbni_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for KLBNIU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for KLBNIU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.klbni(
		float_daily_settlement=18.66, float_qty=1.0
	) - notional_from_pv.klbni(float_daily_settlement=18.58, float_qty=1.0)
	expected = 0.08
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_lreno_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for LRENOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for LRENOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.lreno(
		float_daily_settlement=16.39, float_qty=1.0
	) - notional_from_pv.lreno(float_daily_settlement=16.32, float_qty=1.0)
	expected = 0.07
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_mbr_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for MBRU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for MBRU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.mbr(
		float_daily_settlement=1_255.52, float_qty=1.0
	) - notional_from_pv.mbr(float_daily_settlement=1_254.39, float_qty=1.0)
	expected = 11.30
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_mex_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for MEXU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for MEXU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.mex(
		float_daily_settlement=18_681.386,
		float_qty=1.0,
		float_xcg_usdbrl=5.4241,
		float_xcg_parity_mxnusd=18.6522,
	) - notional_from_pv.mex(
		float_daily_settlement=18_664.651,
		float_qty=1.0,
		float_xcg_usdbrl=5.4241,
		float_xcg_parity_mxnusd=18.6522,
	)
	expected = 48.67
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_mgluo_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for MGLUO25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for MGLUO25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.mgluo(
		float_daily_settlement=8.231, float_qty=1.0
	) - notional_from_pv.mgluo(float_daily_settlement=7.89, float_qty=1.0)
	expected = 0.34
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_mxn_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for MXN delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for MXN
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.mxn(
		float_daily_settlement=2_914.5780, float_qty=1.0
	) - notional_from_pv.mxn(float_daily_settlement=2_908.0910, float_qty=1.0)
	expected = 486.52
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_nok_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for NOKU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for NOKU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.nok(
		float_daily_settlement=10_049.742, float_qty=1.0, float_xcg_usdbrl=5.4241
	) - notional_from_pv.nok(
		float_daily_settlement=10_053.541, float_qty=1.0, float_xcg_usdbrl=5.4241
	)
	expected = 20.60  # correct: 20.50
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_nzd_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for NZDU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for NZDU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.nzd(
		float_daily_settlement=3_213.5430, float_qty=1.0
	) - notional_from_pv.nzd(float_daily_settlement=3_196.4610, float_qty=1.0)
	expected = 1_281.15
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_nzl_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for NZLU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for NZLU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.nzl(
		float_daily_settlement=590.20, float_qty=1.0, float_xcg_usdbrl=5.4241
	) - notional_from_pv.nzl(float_daily_settlement=588.90, float_qty=1.0, float_xcg_usdbrl=5.4241)
	expected = 70.51
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_oc1_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for OC1U25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for OC1U25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.oc1(
		float_daily_settlement=99_944.90, float_qty=1.0
	) - notional_from_pv.oc1(float_daily_settlement=99_944.87, float_qty=1.0)
	expected = 0.03
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_pcaro_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for PCAROU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for PCAROU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.pcaro(
		float_daily_settlement=3.58, float_qty=1.0
	) - notional_from_pv.pcaro(float_daily_settlement=3.53, float_qty=1.0)
	expected = 0.05
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_petrp_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for PETRPU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for PETRPU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.petrp(
		float_daily_settlement=31.36, float_qty=1.0
	) - notional_from_pv.petrp(float_daily_settlement=31.20, float_qty=1.0)
	expected = 0.16
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_prioo_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for PRIOOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for PRIOOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.prioo(
		float_daily_settlement=38.18, float_qty=1.0
	) - notional_from_pv.prioo(float_daily_settlement=38.92, float_qty=1.0)
	expected = 0.74
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_pssao_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for PSSAOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for PSSAOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.pssao(
		float_daily_settlement=52.17, float_qty=1.0
	) - notional_from_pv.pssao(float_daily_settlement=53.26, float_qty=1.0)
	expected = 1.09
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_radlo_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for RADLOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for RADLOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.radlo(
		float_daily_settlement=17.68, float_qty=1.0
	) - notional_from_pv.radlo(float_daily_settlement=19.00, float_qty=1.0)
	expected = 1.32
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_railo_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for RAILOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for RAILOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.railo(
		float_daily_settlement=14.65, float_qty=1.0
	) - notional_from_pv.railo(float_daily_settlement=14.72, float_qty=1.0)
	expected = 0.07
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_rdoro_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for RDOROU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for RDOROU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.rdoro(
		float_daily_settlement=39.68, float_qty=1.0
	) - notional_from_pv.rdoro(float_daily_settlement=39.39, float_qty=1.0)
	expected = 0.29
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_rento_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for RENTOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for RENTOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.rento(
		float_daily_settlement=36.14, float_qty=1.0
	) - notional_from_pv.rento(float_daily_settlement=36.01, float_qty=1.0)
	expected = 0.13
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_rub_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for RUBU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for RUBU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.rub(
		float_daily_settlement=80_400.0, float_qty=1.0, float_xcg_rubbrl=0.06749
	) - notional_from_pv.rub(
		float_daily_settlement=80_357.538, float_qty=1.0, float_xcg_rubbrl=0.06749
	)
	expected = 28.66
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_sbspo_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for SBSPOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for SBSPOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.sbspo(
		float_daily_settlement=123.51, float_qty=1.0
	) - notional_from_pv.sbspo(float_daily_settlement=122.20, float_qty=1.0)
	expected = 1.31
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_sek_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for SEKU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for SEKU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.sek(
		float_daily_settlement=9_452.802, float_qty=1.0, float_xcg_sekbrl=0.5731
	) - notional_from_pv.sek(
		float_daily_settlement=9_454.593, float_qty=1.0, float_xcg_sekbrl=0.5731
	)
	expected = 10.27
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_sfr_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for SFRU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for SFRU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.sfr(
		float_daily_settlement=99_773.464, float_qty=1.0, float_xcg_usdbrl=5.4241
	) - notional_from_pv.sfr(
		float_daily_settlement=99_773.099, float_qty=1.0, float_xcg_usdbrl=5.4241
	)
	expected = 0.39
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_sjc_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for SJCX25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for SJCX25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.sjc(
		float_daily_settlement=23.2474, float_qty=1.0, float_xcg_usdbrl=5.4241
	) - notional_from_pv.sjc(
		float_daily_settlement=23.1041, float_qty=1.0, float_xcg_usdbrl=5.4241
	)
	expected = 349.77
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_sml_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for SMLV25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for SMLV25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.sml(
		float_daily_settlement=2_249.72, float_qty=1.0
	) - notional_from_pv.sml(float_daily_settlement=2_242.98, float_qty=1.0)
	expected = 67.40
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_sol_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for SOLU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for SOLU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.sol(
		float_daily_settlement=204.957, float_qty=1.0, float_xcg_usdbrl=5.4241
	) - notional_from_pv.sol(
		float_daily_settlement=210.587, float_qty=1.0, float_xcg_usdbrl=5.4241
	)
	expected = 152.68
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_soy_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for SOYX25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for SOYX25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.soy(
		float_daily_settlement=444.40, float_qty=1.0, float_xcg_usdbrl=5.4241
	) - notional_from_pv.soy(float_daily_settlement=442.00, float_qty=1.0, float_xcg_usdbrl=5.4241)
	expected = 442.60  # correct: 442.79
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_suzbo_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for SUZBOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for SUZBOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.suzbo(
		float_daily_settlement=52.88, float_qty=1.0
	) - notional_from_pv.suzbo(float_daily_settlement=53.30, float_qty=1.0)
	expected = 0.42
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_swi_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for SWIU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for SWIU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.swi(
		float_daily_settlement=798.00,
		float_qty=1.0,
		float_xcg_usdbrl=5.4241,
		float_xcg_parity_usdchf=0.7999,
	) - notional_from_pv.swi(
		float_daily_settlement=799.10,
		float_qty=1.0,
		float_xcg_usdbrl=5.4241,
		float_xcg_parity_usdchf=0.7999,
	)
	expected = 74.60
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_t10_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for T10Z25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for T10Z25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.t10(
		float_daily_settlement=112.500, float_qty=1.0, float_xcg_usdbrl=5.4241
	) - notional_from_pv.t10(
		float_daily_settlement=112.5937, float_qty=1.0, float_xcg_usdbrl=5.4241
	)
	expected = 508.23  # correct: 508.45
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_tie_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for TIEU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for TIEU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.tie(
		float_daily_settlement=99_597.193,
		float_qty=1.0,
		float_xcg_usdbrl=5.4241,
		float_xcg_parity_mxnusd=18.6505,
	) - notional_from_pv.tie(
		float_daily_settlement=99_598.649,
		float_qty=1.0,
		float_xcg_usdbrl=5.4241,
		float_xcg_parity_mxnusd=18.6505,
	)
	expected = 1.27  # correct: 1.26
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_timso_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for TIMSOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for TIMSOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.timso(
		float_daily_settlement=22.92, float_qty=1.0
	) - notional_from_pv.timso(float_daily_settlement=23.27, float_qty=1.0)
	expected = 0.35
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_try_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for TRYU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for TRYU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.try_(
		float_daily_settlement=130.4190, float_qty=1.0
	) - notional_from_pv.try_(float_daily_settlement=130.2690, float_qty=1.0)
	expected = 11.25
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_tuq_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for TUQU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for TUQU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.tuq(
		float_daily_settlement=41_748.80,
		float_qty=1.0,
		float_xcg_usdbrl=5.4241,
		float_xcg_parity_usdtry=41.2057,
	) - notional_from_pv.tuq(
		float_daily_settlement=41_666.50,
		float_qty=1.0,
		float_xcg_usdbrl=5.4241,
		float_xcg_parity_usdtry=41.2057,
	)
	expected = 108.33  # correct: 108.52
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_usima_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for USIMAU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for USIMAU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.usima(
		float_daily_settlement=4.41, float_qty=1.0
	) - notional_from_pv.usima(float_daily_settlement=4.47, float_qty=1.0)
	expected = 0.06
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_valeo_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for VALEOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for VALEOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.valeo(
		float_daily_settlement=56.01, float_qty=1.0
	) - notional_from_pv.valeo(float_daily_settlement=55.89, float_qty=1.0)
	expected = 0.12
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_vbbro_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for VBBROU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for VBBROU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.vbbro(
		float_daily_settlement=24.23, float_qty=1.0
	) - notional_from_pv.vbbro(float_daily_settlement=24.51, float_qty=1.0)
	expected = 0.28
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_vivto_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for VIVTOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for VIVTOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.vivto(
		float_daily_settlement=34.12, float_qty=1.0
	) - notional_from_pv.vivto(float_daily_settlement=34.21, float_qty=1.0)
	expected = 0.09
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_wdo_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for WDOU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for WDOU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.wdo(
		float_daily_settlement=5_426.40, float_qty=1.0
	) - notional_from_pv.wdo(float_daily_settlement=5_408.2060, float_qty=1.0)
	expected = 181.94
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_wegeo_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for WEGEU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for WEGEU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.wegeo(
		float_daily_settlement=37.97, float_qty=1.0
	) - notional_from_pv.wegeo(float_daily_settlement=38.15, float_qty=1.0)
	expected = 0.18
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_weu_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for WEUU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for WEUU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.weu(
		float_daily_settlement=6_376.8720, float_qty=1.0
	) - notional_from_pv.weu(float_daily_settlement=6_351.3680, float_qty=1.0)
	expected = 255.04
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_win_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for WINV25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for WINV25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.win(
		float_daily_settlement=143_883.0, float_qty=1.0
	) - notional_from_pv.win(float_daily_settlement=143_611.0, float_qty=1.0)
	expected = 54.40
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_wsp_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for WSPU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for WSPU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.wsp(
		float_daily_settlement=6_472.00, float_qty=1.0, float_xcg_usdbrl=5.4241
	) - notional_from_pv.wsp(
		float_daily_settlement=6_518.75, float_qty=1.0, float_xcg_usdbrl=5.4241
	)
	expected = 633.94
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_xfi_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for XFIV25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for XFIV25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.xfi(
		float_daily_settlement=3_570.50, float_qty=1.0
	) - notional_from_pv.xfi(float_daily_settlement=3_545.70, float_qty=1.0)
	expected = 248.00
	assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_zar_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
	"""Example test for ZARU25 delta daily MTM calculation.

	Verifies
	--------
	- Correct calculation of delta daily MTM for ZARU25
	- Matches expected output
	- Reference date: 2025-08-29

	Parameters
	----------
	notional_from_pv : MTMFromDailySettlement
		Instance of MTMFromDailySettlement class

	Returns
	-------
	None
	"""
	result = notional_from_pv.zar(
		float_daily_settlement=3_083.7230, float_qty=1.0
	) - notional_from_pv.zar(float_daily_settlement=3_065.4040, float_qty=1.0)
	expected = 641.16
	assert abs(result) == pytest.approx(expected, abs=1e-2)
