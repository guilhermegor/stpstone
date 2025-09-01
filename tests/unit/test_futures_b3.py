"""Unit tests for Pricing B3 Brazilian Exchange Future Contracts.

Tests the functionality of MTMFromDailySettlement, MTMFromDailySettlment, RateFromMTM, and TSIR classes,
covering initialization, pricing calculations, edge cases, and type validation.
"""

from datetime import date
import importlib
from logging import Logger
import math
import sys
from typing import Callable, Optional

import numpy as np
from numpy.typing import NDArray
import pytest
from pytest_mock import MockerFixture

from stpstone.analytics.pricing.derivatives.futures_b3 import (
    TSIR,
    MTMFromDailySettlement,
    MTMFromRate,
    RateFromMTM,
)
from stpstone.analytics.quant.linear_transformations import LinearAlgebra
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.calendars.calendar_br import DatesBRB3
from stpstone.utils.parsers.lists import ListHandler


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def notional_from_pv() -> "MTMFromDailySettlement":
    """Fixture providing MTMFromDailySettlement instance with default parameters.

    Returns
    -------
    MTMFromDailySettlement
        Instance initialized with default cache settings
    """
    return MTMFromDailySettlement()


@pytest.fixture
def notional_from_rate() -> "MTMFromRate":
    """Fixture providing MTMFromRate instance with default parameters.

    Returns
    -------
    MTMFromRate
        Instance initialized with default cache settings
    """
    return MTMFromRate()


@pytest.fixture
def rt_from_pv() -> "RateFromMTM":
    """Fixture providing RateFromMTM instance with default parameters.

    Returns
    -------
    RateFromMTM
        Instance initialized with default cache settings
    """
    return RateFromMTM()


@pytest.fixture
def tsir() -> "TSIR":
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
    return (
        date(2023, 5, 15),
        date(2023, 5, 20),
        date(2023, 6, 15)
    )


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
def test_notional_from_pv_init_valid(notional_from_pv: "MTMFromDailySettlement") -> None:
    """Test MTMFromDailySettlement initialization with valid inputs.

    Verifies
    --------
    - Instance attributes are correctly set
    - DatesBRB3 is properly initialized with passed parameters

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


def test_generic_pricing_valid_inputs(notional_from_pv: "MTMFromDailySettlement") -> None:
    """Test generic_pricing with valid inputs.

    Verifies
    --------
    - Correct calculation of notional value
    - Proper handling of optional float_xcg_rt_2 parameter
    - Return type is float

    Returns
    -------
    None
    """
    result = notional_from_pv.generic_pricing(
        float_daily_settlement=1000000.0,
        float_size=0.00025,
        float_qty=50.0,
        float_xcg_rt_1=1.0,
        float_xcg_rt_2=1.0
    )
    assert isinstance(result, float)
    assert result == pytest.approx(12500.0, rel=1e-6)


@pytest.mark.parametrize("invalid_input", [
    (None, 0.00025, 50.0, 1.0, 1.0),
    (1000000.0, "invalid", 50.0, 1.0, 1.0),
    (1000000.0, 0.00025, None, 1.0, 1.0),
    (1000000.0, 0.00025, 50.0, "invalid", 1.0),
    (1000000.0, 0.00025, 50.0, 1.0, None)
])
def test_generic_pricing_invalid_types(
    notional_from_pv: "MTMFromDailySettlement", 
    invalid_input: tuple
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
    notional_from_pv: "MTMFromDailySettlement", 
    sample_dates: tuple[date, date, date],
    mocker: MockerFixture
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
        date_pmi_next=date_pmi_next
    )
    expected = 1000000.0 * 50.0 * 52.3 * 0.00025 * (1.0 + 0.04) ** (21 / 21)
    assert isinstance(result, float)
    assert result == pytest.approx(expected, rel=1e-6)


def test_dap_invalid_dates(
    notional_from_pv: "MTMFromDailySettlement", 
    sample_dates: tuple[date, date, date]
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
            date_pmi_next=date_pmi_last
        )


@pytest.mark.parametrize("invalid_input", [
    (None, 50.0, 52.3, 0.04, date(2023, 5, 15), date(2023, 5, 20), date(2023, 6, 15)),
    (1000000.0, "invalid", 52.3, 0.04, date(2023, 5, 15), date(2023, 5, 20), date(2023, 6, 15)),
    (1000000.0, 50.0, None, 0.04, date(2023, 5, 15), date(2023, 5, 20), date(2023, 6, 15)),
    (1000000.0, 50.0, 52.3, "invalid", date(2023, 5, 15), date(2023, 5, 20), date(2023, 6, 15)),
    (1000000.0, 50.0, 52.3, 0.04, None, date(2023, 5, 20), date(2023, 6, 15)),
    (1000000.0, 50.0, 52.3, 0.04, date(2023, 5, 15), None, date(2023, 6, 15)),
    (1000000.0, 50.0, 52.3, 0.04, date(2023, 5, 15), date(2023, 5, 20), None)
])
def test_dap_invalid_types(
    notional_from_pv: "MTMFromDailySettlement", 
    invalid_input: tuple
) -> None:
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
# Tests for MTMFromDailySettlment
# --------------------------
def test_notional_from_rate_init_valid(notional_from_rate: "MTMFromDailySettlment") -> None:
    """Test MTMFromDailySettlment initialization with valid inputs.

    Verifies
    --------
    - Instance attributes are correctly set
    - DatesBRB3 is properly initialized with passed parameters

    Returns
    -------
    None
    """
    assert notional_from_rate.bool_persist_cache is True
    assert notional_from_rate.bool_reuse_cache is True
    assert isinstance(notional_from_rate.cls_dates_br_b3, DatesBRB3)


def test_di1_valid_inputs(
    notional_from_rate: "MTMFromDailySettlment",
    sample_dates: tuple[date, date, date],
    mocker: MockerFixture
) -> None:
    """Test DI1 pricing with valid inputs.

    Parameters
    ----------
    notional_from_rate : MTMFromDailySettlment
        Instance of MTMFromDailySettlment class
    sample_dates : tuple[date, date, date]
        Sample dates for testing
    mocker : MockerFixture
        Pytest-mock fixture for mocking dependencies

    Verifies
    --------
    - Correct calculation of DI1 present value
    - Proper handling of date calculations
    - Return type is float

    Returns
    -------
    None
    """
    _, date_ref, date_xpt = sample_dates
    mocker.patch.object(DatesBRB3, "delta_working_days", return_value=2)
    
    result = notional_from_rate.di1(
        float_nominal_rate=0.10,
        date_ref=date_ref,
        date_xpt=date_xpt
    )
    expected = 100000.0 / (1.0 + ((1.0 + 0.10) ** (2 / 252) - 1.0))
    assert isinstance(result, float)
    assert result == pytest.approx(expected, rel=1e-6)


@pytest.mark.parametrize("invalid_input", [
    (None, date(2023, 5, 20), date(2023, 12, 1)),
    (0.10, None, date(2023, 12, 1)),
    (0.10, date(2023, 5, 20), None)
])
def test_di1_invalid_types(
    notional_from_rate: "MTMFromDailySettlment", 
    invalid_input: tuple
) -> None:
    """Test DI1 pricing with invalid input types.

    Parameters
    ----------
    notional_from_rate : MTMFromDailySettlment
        Instance of MTMFromDailySettlment class
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
        notional_from_rate.di1(*invalid_input)


# --------------------------
# Tests for RateFromMTM
# --------------------------
def test_rt_from_pv_init_valid(rt_from_pv: "RateFromMTM") -> None:
    """Test RateFromMTM initialization with valid inputs.

    Verifies
    --------
    - Instance attributes are correctly set
    - DatesBRB3 is properly initialized with passed parameters

    Returns
    -------
    None
    """
    assert rt_from_pv.bool_persist_cache is True
    assert rt_from_pv.bool_reuse_cache is True
    assert isinstance(rt_from_pv.cls_dates_br_b3, DatesBRB3)


def test_ddi_valid_inputs(
    rt_from_pv: "RateFromMTM",
    sample_dates: tuple[date, date, date],
    mocker: MockerFixture
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
        date_xpt=date_xpt
    )
    expected = ((95000.0 / 100000.0) / (1.0 / 5.20) - 1.0) * 365 / 2
    assert isinstance(result, float)
    assert result == pytest.approx(expected, rel=1e-6)


@pytest.mark.parametrize("invalid_input", [
    (None, 1.0, 5.20, date(2023, 5, 20), date(2023, 12, 1)),
    (95000.0, None, 5.20, date(2023, 5, 20), date(2023, 12, 1)),
    (95000.0, 1.0, None, date(2023, 5, 20), date(2023, 12, 1)),
    (95000.0, 1.0, 5.20, None, date(2023, 12, 1)),
    (95000.0, 1.0, 5.20, date(2023, 5, 20), None)
])
def test_ddi_invalid_types(
    rt_from_pv: "RateFromMTM", 
    invalid_input: tuple
) -> None:
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
def test_tsir_init_valid(tsir: "TSIR") -> None:
    """Test TSIR initialization with valid inputs.

    Verifies
    --------
    - Instance attributes are correctly set
    - DatesBRB3 is properly initialized with passed parameters

    Returns
    -------
    None
    """
    assert tsir.bool_persist_cache is True
    assert tsir.bool_reuse_cache is True
    assert isinstance(tsir.cls_dates_br_b3, DatesBRB3)


def test_flat_forward_valid_inputs(
    tsir: "TSIR", 
    sample_nper_rates: dict[int, float]
) -> None:
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
    assert all(isinstance(k, int) for k in result.keys())
    assert all(isinstance(v, float) for v in result.values())
    assert len(result) == 331  # From 30 to 360 inclusive
    assert result[30] == pytest.approx(0.015, rel=1e-6)


@pytest.mark.parametrize("invalid_input", [
    {30: "invalid", 90: 0.018},
    {"30": 0.015, 90: 0.018},
    {30: 0.015}  # Too few points
])
def test_flat_forward_invalid_inputs(tsir: "TSIR", invalid_input: dict) -> None:
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


def test_cubic_spline_valid_inputs(
    tsir: "TSIR", 
    sample_nper_rates: dict[int, float]
) -> None:
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
    assert all(isinstance(k, int) for k in result.keys())
    assert all(isinstance(v, float) for v in result.values())
    assert len(result) == 331  # From 30 to 360 inclusive
    assert result[30] == pytest.approx(0.015, rel=1e-6)


@pytest.mark.parametrize("invalid_input", [
    {30: 0.015, 90: 0.018},  # Too few points
    {90: 0.018, 30: 0.015},  # Unsorted keys
    {30: "invalid", 90: 0.018, 180: 0.020},
    {"30": 0.015, 90: 0.018, 180: 0.020}
])
def test_cubic_spline_invalid_inputs(tsir: "TSIR", invalid_input: dict) -> None:
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


def test_third_degree_polynomial_cubic_spline_valid(
    tsir: "TSIR"
) -> None:
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
    assert result_lower == pytest.approx(0.02371, rel=1e-6)
    assert result_upper == pytest.approx(0.02412, rel=1e-6)


def test_third_degree_polynomial_cubic_spline_invalid_length(
    tsir: "TSIR"
) -> None:
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


@pytest.mark.parametrize("invalid_input", [
    ([0.02, 0.001, -0.0001, 0.00001, 0.025, -0.0005, 0.00002, -0.000001], "invalid", False),
    ([0.02, 0.001, -0.0001, 0.00001, 0.025, -0.0005, 0.00002, -0.000001], 30, "invalid"),
    ("invalid", 30, False)
])
def test_third_degree_polynomial_cubic_spline_invalid_types(
    tsir: "TSIR", 
    invalid_input: tuple
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
    tsir: "TSIR", 
    sample_nper_rates: dict[int, float],
    mocker: MockerFixture
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
    mocker.patch.object(ListHandler, "get_lower_mid_upper_bound", return_value={
        "lower_bound": 30,
        "middle_bound": 90,
        "upper_bound": 180,
        "end_of_list": False
    })
    mocker.patch.object(LinearAlgebra, "matrix_multiplication", return_value=[
        0.02, 0.001, -0.0001, 0.00001, 0.025, -0.0005, 0.00002, -0.000001
    ])
    
    result = tsir.literal_cubic_spline(sample_nper_rates)
    assert isinstance(result, dict)
    assert all(isinstance(k, int) for k in result.keys())
    assert all(isinstance(v, float) for v in result.values())
    assert len(result) == 331  # From 30 to 360 inclusive


def test_literal_cubic_spline_invalid_bound(
    tsir: "TSIR", 
    sample_nper_rates: dict[int, float],
    mocker: MockerFixture
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
    tsir: "TSIR", 
    sample_nper_rates: dict[int, float],
    mocker: MockerFixture
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
        return_value=(lambda x: np.array([0.015] * len(x)), None)
    )
    
    result = tsir.nelson_siegel(sample_nper_rates)
    assert isinstance(result, dict)
    assert all(isinstance(k, int) for k in result.keys())
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
    importlib.reload(sys.modules["stpstone.analytics.quant.pricing_b3"])
    reloaded_notional = MTMFromDailySettlement()
    assert isinstance(reloaded_notional, MTMFromDailySettlement)
    assert reloaded_notional.bool_persist_cache == original_notional.bool_persist_cache


# --------------------------
# Edge Cases
# --------------------------
def test_dap_zero_values(notional_from_pv: "MTMFromDailySettlement", sample_dates: tuple[date, date, date]) -> None:
    """Test DAP pricing with zero values.

    Parameters
    ----------
    notional_from_pv : MTMFromDailySettlement
        Instance of MTMFromDailySettlement class
    sample_dates : tuple[date, date, date]
        Sample dates for testing

    Verifies
    --------
    - Proper handling of zero inputs
    - Correct calculation with zero values

    Returns
    -------
    None
    """
    date_pmi_last, date_ref, date_pmi_next = sample_dates
    result = notional_from_pv.dap(
        float_daily_settlement=0.0,
        float_qty=0.0,
        float_pmi_ipca_mm1=0.0,
        float_pmi_ipca_rt_hat=0.0,
        date_pmi_last=date_pmi_last,
        date_ref=date_ref,
        date_pmi_next=date_pmi_next
    )
    assert result == 0.0


def test_cubic_spline_edge_cases(tsir: "TSIR") -> None:
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
    notional_from_pv: "MTMFromDailySettlement",
    mocker: MockerFixture
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
        date_pmi_next=date(2023, 6, 15)
    )
    assert result == pytest.approx(12500.0, rel=1e-6)


def test_di1_docstring_example(
    notional_from_rate: "MTMFromDailySettlment",
    mocker: MockerFixture
) -> None:
    """Test DI1 pricing using example from docstring.

    Parameters
    ----------
    notional_from_rate : MTMFromDailySettlment
        Instance of MTMFromDailySettlment class
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
        float_nominal_rate=0.10,
        date_ref=date(2023, 5, 20),
        date_xpt=date(2023, 12, 1)
    )
    assert result == pytest.approx(97590.23, rel=1e-6)


def test_ddi_docstring_example(
    rt_from_pv: "RateFromMTM",
    mocker: MockerFixture
) -> None:
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
        date_xpt=date(2023, 12, 1)
    )
    assert result == pytest.approx(0.0652, rel=1e-6)


def test_third_degree_polynomial_cubic_spline_docstring_example(tsir: "TSIR") -> None:
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
    assert result_lower == pytest.approx(0.02371, rel=1e-6)
    assert result_upper == pytest.approx(0.02412, rel=1e-6)


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

    Returns
    -------
    None
    """
    result = notional_from_pv.abevo(float_daily_settlement=12.45, float_qty=1.0) \
        - notional_from_pv.abevo(float_daily_settlement=12.35, float_qty=1.0)
    expected = 0.10
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_afs_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for AFSU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for AFSU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.afs(
        float_daily_settlement=17_656.70, float_qty=1.0, float_xcg_zarbrl=0.3074) \
        - notional_from_pv.afs(
            float_daily_settlement=17_706.80, float_qty=1.0, float_xcg_zarbrl=0.3074)
    expected = 154.0074
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_arb_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for ARBU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for ARBU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.arb(float_daily_settlement=4.08, float_qty=1.0) \
        - notional_from_pv.arb(float_daily_settlement=4.0530, float_qty=1.0)
    expected = 4.05
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_ars_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for ARSU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for ARSU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.ars(
        float_daily_settlement=1_330_099.90, float_qty=1.0, float_xcg_arsbrl=0.00412) \
        - notional_from_pv.ars(
            float_daily_settlement=1_334_418.80, float_qty=1.0, float_xcg_arsbrl=0.00412)
    expected = 177.93868
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_aud_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for AUDU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for AUDU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.aud(float_daily_settlement=3_563.5880, float_qty=1.0) \
        - notional_from_pv.aud(float_daily_settlement=3_547.1410, float_qty=1.0)
    expected = 986.82
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_aus_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for AUSU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for AUSU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.aus(
        float_daily_settlement=654.489, float_qty=1.0, float_xcg_usdbrl=5.4241) \
        - notional_from_pv.aus(
            float_daily_settlement=653.508, float_qty=1.0, float_xcg_usdbrl=5.4241)
    expected = 53.21
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_b3sao_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for B3SAOU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for B3SAOU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.b3sao(float_daily_settlement=13.09, float_qty=1.0) \
        - notional_from_pv.b3sao(float_daily_settlement=13.15, float_qty=1.0)
    expected = 0.06
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_bbaso_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for BBASOU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for BBASOU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.bbaso(float_daily_settlement=21.57, float_qty=1.0) \
        - notional_from_pv.bbaso(float_daily_settlement=21.24, float_qty=1.0)
    expected = 0.33
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_bbdcp_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for BBDCPU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for BBDCPU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.bbdcp(float_daily_settlement=16.97, float_qty=1.0) \
        - notional_from_pv.bbdcp(float_daily_settlement=16.94, float_qty=1.0)
    expected = 0.03
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_bgi_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for BGIU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for BGIU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.bgi(float_daily_settlement=318.60, float_qty=1.0) \
        - notional_from_pv.bgi(float_daily_settlement=316.95, float_qty=1.0)
    expected = 544.50
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_bhiao_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for BHIAOU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for BHIAOU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.bhiao(float_daily_settlement=4.19, float_qty=1.0) \
        - notional_from_pv.bhiao(float_daily_settlement=3.42, float_qty=1.0)
    expected = 0.77
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_bit_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for BITU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for BITOU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.bit(float_daily_settlement=594_749.95, float_qty=1.0) \
        - notional_from_pv.bit(float_daily_settlement=615_852.79, float_qty=1.0)
    expected = 211.02
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_bpaci_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for BPACIU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for BPACIU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.bpaci(float_daily_settlement=45.23, float_qty=1.0) \
        - notional_from_pv.bpaci(float_daily_settlement=44.89, float_qty=1.0)
    expected = 0.34
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_bri_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for BRIV25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for BRIV25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.bri(float_daily_settlement=23_965.0, float_qty=1.0) \
        - notional_from_pv.bri(float_daily_settlement=23_909.0, float_qty=1.0)
    expected = 560.0
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_cad_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for CADU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for CADU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.cad(float_daily_settlement=3_971.3120, float_qty=1.0) \
        - notional_from_pv.cad(float_daily_settlement=3_953.7050, float_qty=1.0)
    expected = 1_056.42
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_can_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for CANU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for CANU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.can(
        float_daily_settlement=1_371.465, float_qty=1.0, float_xcg_cadbrl=3.9534) \
        - notional_from_pv.can(
            float_daily_settlement=1_373.304, float_qty=1.0, float_xcg_cadbrl=3.9534)
    expected = 72.70 # 72.66
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_ccm_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for CCMU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for CCMU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.ccm(float_daily_settlement=65.49, float_qty=1.0) \
        - notional_from_pv.ccm(float_daily_settlement=65.12, float_qty=1.0)
    expected = 166.50
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_chf_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for CHFU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for CHFU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.chf(float_daily_settlement=6_823.1030, float_qty=1.0) \
        - notional_from_pv.chf(float_daily_settlement=6_792.4540, float_qty=1.0)
    expected = 1_532.45
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_chl_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for CHLU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for CHLU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.chl(
        float_daily_settlement=965_280.0, float_qty=1.0, float_xcg_clpbrl=0.005633) \
        - notional_from_pv.chl(
            float_daily_settlement=966_699.0, float_qty=1.0, float_xcg_clpbrl=0.005633)
    expected = 79.93 # 79.69
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_clp_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for CLPU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for CLPU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.clp(float_daily_settlement=5_634.8320, float_qty=1.0) \
        - notional_from_pv.clp(float_daily_settlement=5_594.5090, float_qty=1.0)
    expected = 1008.07
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_cmigp_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for CMIGPU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for CMIGPU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.cmigp(float_daily_settlement=11.20, float_qty=1.0) \
        - notional_from_pv.cmigp(float_daily_settlement=11.22, float_qty=1.0)
    expected = 0.02
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_cnh_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for CNHU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for CNHU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.cnh(
        float_daily_settlement=7_109.673, float_qty=1.0, float_xcg_cnybrl=0.7610) \
        - notional_from_pv.cnh(
            float_daily_settlement=7_107.286, float_qty=1.0, float_xcg_cnybrl=0.7610)
    expected = 18.1650 # 18.18
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_cnl_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for CHLU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for CHLU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.cnl(float_daily_settlement=1_555.0, float_qty=1.0) \
        - notional_from_pv.cnl(float_daily_settlement=1_489.23, float_qty=1.0)
    expected = 6_577.0
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_cny_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for CNYU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for CNYU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.cny(float_daily_settlement=7_637.1280, float_qty=1.0) \
        - notional_from_pv.cny(float_daily_settlement=7_623.9200, float_qty=1.0)
    expected = 462.28
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_cogno_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for COGNOU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for COGNOU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.cogno(float_daily_settlement=2.94, float_qty=1.0) \
        - notional_from_pv.cogno(float_daily_settlement=2.95, float_qty=1.0)
    expected = 0.01
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_csano_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for CSANOU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for CSANOU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.csano(float_daily_settlement=5.90, float_qty=1.0) \
        - notional_from_pv.csano(float_daily_settlement=5.77, float_qty=1.0)
    expected = 0.13
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_csnao_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for CSNAOU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for CSNAOU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.csnao(float_daily_settlement=7.67, float_qty=1.0) \
        - notional_from_pv.csnao(float_daily_settlement=7.80, float_qty=1.0)
    expected = 0.13
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_dap_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for DAPU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for DAPU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.dap(
        float_daily_settlement=99_327.95,
        float_qty=1.0,
        float_pmi_ipca_mm1=7331.98,
        float_pmi_ipca_rt_hat=-0.0012,
        date_pmi_last=date(2025, 8, 12),
        date_ref=date(2025, 8, 29),
        date_pmi_next=date(2025, 9, 10)
    ) - notional_from_pv.dap(
        float_daily_settlement=99_327.91,
        float_qty=1.0,
        float_pmi_ipca_mm1=7331.98,
        float_pmi_ipca_rt_hat=-0.0012,
        date_pmi_last=date(2025, 8, 12),
        date_ref=date(2025, 8, 29),
        date_pmi_next=date(2025, 9, 10)
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

    Returns
    -------
    None
    """
    result = notional_from_pv.dax(
        float_daily_settlement=23_958.00, float_qty=1.0, float_xcg_usdbl=5.4241, 
        float_xcg_parity_eurusd=1.1701) \
        - notional_from_pv.dax(
            float_daily_settlement=24_079.00, float_qty=1.0, float_xcg_usdbl=5.4241, 
            float_xcg_parity_eurusd=1.1701)
    expected = 3_839.77 # 3_838.46
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_dco_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for DCOU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for DCOU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.dco(float_daily_settlement=100_220.08, float_qty=1.0, 
                                  float_xcg_usdbrl=5.4241) \
        - notional_from_pv.dco(float_daily_settlement=99_884.03, float_qty=1.0, 
                               float_xcg_usdbrl=5.4241)
    expected = 911.38 # 909.26
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_ddi_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for DDIU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for DDIU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.ddi(
        float_daily_settlement=100_220.08,
        float_qty=1.0,
        float_xcg_usdbrl=5.4241,
    ) - notional_from_pv.ddi(
        float_daily_settlement=99_884.03,
        float_qty=1.0,
        float_xcg_usdbrl=5.4241,
    )
    expected = 911.38 # 909.26
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_di1_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for DI1U25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for DI1U25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.di1(float_daily_settlement=99_944.90, float_qty=1.0) \
        - notional_from_pv.di1(float_daily_settlement=99_944.87, float_qty=1.0)
    expected = 0.03
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_dol_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for DOLU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for DOLU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.dol(float_daily_settlement=5_426.40, float_qty=1.0) \
        - notional_from_pv.dol(float_daily_settlement=5_408.2060, float_qty=1.0)
    expected = 909.70
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_eleto_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for ELETOU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for ELETOU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.eleto(float_daily_settlement=45.39, float_qty=1.0) \
        - notional_from_pv.eleto(float_daily_settlement=44.81, float_qty=1.0)
    expected = 0.58
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_embro_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for EMBROU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for EMBROU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.embro(float_daily_settlement=76.82, float_qty=1.0) \
        - notional_from_pv.embro(float_daily_settlement=76.87, float_qty=1.0)
    expected = 0.05
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_enevo_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for ENEVOU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for ENEVOU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.enevo(float_daily_settlement=15.24, float_qty=1.0) \
        - notional_from_pv.enevo(float_daily_settlement=15.27, float_qty=1.0)
    expected = 0.03
    assert abs(result) == pytest.approx(expected, abs=1e-2)


def test_eqtlo_delta_mtm(notional_from_pv: MTMFromDailySettlement) -> None:
    """Example test for EQTLOU25 delta daily MTM calculation.

    Verifies
    --------
    - Correct calculation of delta daily MTM for EQTLOU25
    - Matches expected output
    - Reference date: 2025-08-29

    Returns
    -------
    None
    """
    result = notional_from_pv.eqtlo(float_daily_settlement=36.87, float_qty=1.0) \
        - notional_from_pv.eqtlo(float_daily_settlement=36.96, float_qty=1.0)
    expected = 0.09
    assert abs(result) == pytest.approx(expected, abs=1e-2)