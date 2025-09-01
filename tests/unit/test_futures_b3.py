"""Unit tests for Pricing B3 Brazilian Exchange Future Contracts.

Tests the functionality of NotionalFromPV, NotionalFromRate, RtFromPV, and TSIR classes,
covering initialization, pricing calculations, edge cases, and type validation.
"""

from datetime import datetime
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
    NotionalFromPV,
    NotionalFromRate,
    RtFromPV,
)
from stpstone.analytics.quant.linear_transformations import LinearAlgebra
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.calendars.calendar_br import DatesBRB3
from stpstone.utils.parsers.lists import ListHandler


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def notional_from_pv() -> "NotionalFromPV":
    """Fixture providing NotionalFromPV instance with default parameters.

    Returns
    -------
    NotionalFromPV
        Instance initialized with default cache settings
    """
    return NotionalFromPV()


@pytest.fixture
def notional_from_rate() -> "NotionalFromRate":
    """Fixture providing NotionalFromRate instance with default parameters.

    Returns
    -------
    NotionalFromRate
        Instance initialized with default cache settings
    """
    return NotionalFromRate()


@pytest.fixture
def rt_from_pv() -> "RtFromPV":
    """Fixture providing RtFromPV instance with default parameters.

    Returns
    -------
    RtFromPV
        Instance initialized with default cache settings
    """
    return RtFromPV()


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
def sample_dates() -> tuple[datetime, datetime, datetime]:
    """Fixture providing sample dates for testing.

    Returns
    -------
    tuple[datetime, datetime, datetime]
        Tuple containing last PMI date, reference date, and next PMI date
    """
    return (
        datetime(2023, 5, 15),
        datetime(2023, 5, 20),
        datetime(2023, 6, 15)
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
# Tests for NotionalFromPV
# --------------------------
def test_notional_from_pv_init_valid(notional_from_pv: "NotionalFromPV") -> None:
    """Test NotionalFromPV initialization with valid inputs.

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
    """Test NotionalFromPV initialization with invalid types.

    Verifies
    --------
    - TypeError is raised for non-boolean cache parameters
    - Error message contains 'must be of type'

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        NotionalFromPV(bool_persist_cache="invalid", bool_reuse_cache=True)
    with pytest.raises(TypeError, match="must be of type"):
        NotionalFromPV(bool_persist_cache=True, bool_reuse_cache=123)
    with pytest.raises(TypeError, match="must be one of types"):
        NotionalFromPV(bool_persist_cache=True, bool_reuse_cache=True, logger="invalid")


def test_generic_pricing_valid_inputs(notional_from_pv: "NotionalFromPV") -> None:
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
        float_pv=1000000.0,
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
    notional_from_pv: "NotionalFromPV", 
    invalid_input: tuple
) -> None:
    """Test generic_pricing with invalid input types.

    Parameters
    ----------
    notional_from_pv : NotionalFromPV
        Instance of NotionalFromPV class
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
    notional_from_pv: "NotionalFromPV", 
    sample_dates: tuple[datetime, datetime, datetime],
    mocker: MockerFixture
) -> None:
    """Test DAP pricing with valid inputs.

    Parameters
    ----------
    notional_from_pv : NotionalFromPV
        Instance of NotionalFromPV class
    sample_dates : tuple[datetime, datetime, datetime]
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
    dt_pmi_last, date_ref, dt_pmi_next = sample_dates
    mocker.patch.object(DatesBRB3, "delta_working_days", return_value=21)
    
    result = notional_from_pv.dap(
        float_pv=1000000.0,
        float_qty=50.0,
        float_pmi_idx_mm1=52.3,
        float_pmi_ipca_rt_hat=0.04,
        dt_pmi_last=dt_pmi_last,
        date_ref=date_ref,
        dt_pmi_next=dt_pmi_next
    )
    expected = 1000000.0 * 50.0 * 52.3 * 0.00025 * (1.0 + 0.04) ** (21 / 21)
    assert isinstance(result, float)
    assert result == pytest.approx(expected, rel=1e-6)


def test_dap_invalid_dates(
    notional_from_pv: "NotionalFromPV", 
    sample_dates: tuple[datetime, datetime, datetime]
) -> None:
    """Test DAP pricing with invalid date order.

    Parameters
    ----------
    notional_from_pv : NotionalFromPV
        Instance of NotionalFromPV class
    sample_dates : tuple[datetime, datetime, datetime]
        Sample dates for last PMI, reference, and next PMI

    Verifies
    --------
    - ValueError is raised when dt_pmi_last > dt_pmi_next
    - Error message matches expected pattern

    Returns
    -------
    None
    """
    dt_pmi_last, date_ref, _ = sample_dates
    with pytest.raises(ValueError, match="Please validate the input"):
        notional_from_pv.dap(
            float_pv=1000000.0,
            float_qty=50.0,
            float_pmi_idx_mm1=52.3,
            float_pmi_ipca_rt_hat=0.04,
            dt_pmi_last=dt_pmi_next,
            date_ref=date_ref,
            dt_pmi_next=dt_pmi_last
        )


@pytest.mark.parametrize("invalid_input", [
    (None, 50.0, 52.3, 0.04, datetime(2023, 5, 15), datetime(2023, 5, 20), datetime(2023, 6, 15)),
    (1000000.0, "invalid", 52.3, 0.04, datetime(2023, 5, 15), datetime(2023, 5, 20), datetime(2023, 6, 15)),
    (1000000.0, 50.0, None, 0.04, datetime(2023, 5, 15), datetime(2023, 5, 20), datetime(2023, 6, 15)),
    (1000000.0, 50.0, 52.3, "invalid", datetime(2023, 5, 15), datetime(2023, 5, 20), datetime(2023, 6, 15)),
    (1000000.0, 50.0, 52.3, 0.04, None, datetime(2023, 5, 20), datetime(2023, 6, 15)),
    (1000000.0, 50.0, 52.3, 0.04, datetime(2023, 5, 15), None, datetime(2023, 6, 15)),
    (1000000.0, 50.0, 52.3, 0.04, datetime(2023, 5, 15), datetime(2023, 5, 20), None)
])
def test_dap_invalid_types(
    notional_from_pv: "NotionalFromPV", 
    invalid_input: tuple
) -> None:
    """Test DAP pricing with invalid input types.

    Parameters
    ----------
    notional_from_pv : NotionalFromPV
        Instance of NotionalFromPV class
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
# Tests for NotionalFromRate
# --------------------------
def test_notional_from_rate_init_valid(notional_from_rate: "NotionalFromRate") -> None:
    """Test NotionalFromRate initialization with valid inputs.

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
    notional_from_rate: "NotionalFromRate",
    sample_dates: tuple[datetime, datetime, datetime],
    mocker: MockerFixture
) -> None:
    """Test DI1 pricing with valid inputs.

    Parameters
    ----------
    notional_from_rate : NotionalFromRate
        Instance of NotionalFromRate class
    sample_dates : tuple[datetime, datetime, datetime]
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
    (None, datetime(2023, 5, 20), datetime(2023, 12, 1)),
    (0.10, None, datetime(2023, 12, 1)),
    (0.10, datetime(2023, 5, 20), None)
])
def test_di1_invalid_types(
    notional_from_rate: "NotionalFromRate", 
    invalid_input: tuple
) -> None:
    """Test DI1 pricing with invalid input types.

    Parameters
    ----------
    notional_from_rate : NotionalFromRate
        Instance of NotionalFromRate class
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
# Tests for RtFromPV
# --------------------------
def test_rt_from_pv_init_valid(rt_from_pv: "RtFromPV") -> None:
    """Test RtFromPV initialization with valid inputs.

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
    rt_from_pv: "RtFromPV",
    sample_dates: tuple[datetime, datetime, datetime],
    mocker: MockerFixture
) -> None:
    """Test DDI pricing with valid inputs.

    Parameters
    ----------
    rt_from_pv : RtFromPV
        Instance of RtFromPV class
    sample_dates : tuple[datetime, datetime, datetime]
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
        float_pv_di=95000.0,
        float_fut_dol=1.0,
        float_ptax_dm1=5.20,
        date_ref=date_ref,
        date_xpt=date_xpt
    )
    expected = ((95000.0 / 100000.0) / (1.0 / 5.20) - 1.0) * 365 / 2
    assert isinstance(result, float)
    assert result == pytest.approx(expected, rel=1e-6)


@pytest.mark.parametrize("invalid_input", [
    (None, 1.0, 5.20, datetime(2023, 5, 20), datetime(2023, 12, 1)),
    (95000.0, None, 5.20, datetime(2023, 5, 20), datetime(2023, 12, 1)),
    (95000.0, 1.0, None, datetime(2023, 5, 20), datetime(2023, 12, 1)),
    (95000.0, 1.0, 5.20, None, datetime(2023, 12, 1)),
    (95000.0, 1.0, 5.20, datetime(2023, 5, 20), None)
])
def test_ddi_invalid_types(
    rt_from_pv: "RtFromPV", 
    invalid_input: tuple
) -> None:
    """Test DDI pricing with invalid input types.

    Parameters
    ----------
    rt_from_pv : RtFromPV
        Instance of RtFromPV class
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
    original_notional = NotionalFromPV()
    importlib.reload(sys.modules["stpstone.analytics.quant.pricing_b3"])
    reloaded_notional = NotionalFromPV()
    assert isinstance(reloaded_notional, NotionalFromPV)
    assert reloaded_notional.bool_persist_cache == original_notional.bool_persist_cache


# --------------------------
# Edge Cases
# --------------------------
def test_dap_zero_values(notional_from_pv: "NotionalFromPV", sample_dates: tuple[datetime, datetime, datetime]) -> None:
    """Test DAP pricing with zero values.

    Parameters
    ----------
    notional_from_pv : NotionalFromPV
        Instance of NotionalFromPV class
    sample_dates : tuple[datetime, datetime, datetime]
        Sample dates for testing

    Verifies
    --------
    - Proper handling of zero inputs
    - Correct calculation with zero values

    Returns
    -------
    None
    """
    dt_pmi_last, date_ref, dt_pmi_next = sample_dates
    result = notional_from_pv.dap(
        float_pv=0.0,
        float_qty=0.0,
        float_pmi_idx_mm1=0.0,
        float_pmi_ipca_rt_hat=0.0,
        dt_pmi_last=dt_pmi_last,
        date_ref=date_ref,
        dt_pmi_next=dt_pmi_next
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
    notional_from_pv: "NotionalFromPV",
    mocker: MockerFixture
) -> None:
    """Test DAP pricing using example from docstring.

    Parameters
    ----------
    notional_from_pv : NotionalFromPV
        Instance of NotionalFromPV class
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
        float_pv=1_000_000.0,
        float_qty=50.0,
        float_pmi_idx_mm1=52.3,
        float_pmi_ipca_rt_hat=0.04,
        dt_pmi_last=datetime(2023, 5, 15),
        date_ref=datetime(2023, 5, 20),
        dt_pmi_next=datetime(2023, 6, 15)
    )
    assert result == pytest.approx(12500.0, rel=1e-6)


def test_di1_docstring_example(
    notional_from_rate: "NotionalFromRate",
    mocker: MockerFixture
) -> None:
    """Test DI1 pricing using example from docstring.

    Parameters
    ----------
    notional_from_rate : NotionalFromRate
        Instance of NotionalFromRate class
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
        date_ref=datetime(2023, 5, 20),
        date_xpt=datetime(2023, 12, 1)
    )
    assert result == pytest.approx(97590.23, rel=1e-6)


def test_ddi_docstring_example(
    rt_from_pv: "RtFromPV",
    mocker: MockerFixture
) -> None:
    """Test DDI pricing using example from docstring.

    Parameters
    ----------
    rt_from_pv : RtFromPV
        Instance of RtFromPV class
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
        float_pv_di=95000.0,
        float_fut_dol=1.0,
        float_ptax_dm1=5.20,
        date_ref=datetime(2023, 5, 20),
        date_xpt=datetime(2023, 12, 1)
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