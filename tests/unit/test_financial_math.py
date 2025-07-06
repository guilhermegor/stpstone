"""Unit tests for financial_math module."""

import pytest

from stpstone.analytics.perf_metrics.financial_math import FinancialMath


@pytest.fixture
def fm() -> FinancialMath:
    """Fixture providing FinancialMath instance for tests."""
    return FinancialMath()


def test_compound_r_normal(fm: FinancialMath) -> None:
    """Test compound rate calculation with normal inputs."""
    result = fm.compound_r(0.05, 10, 2)
    assert round(result, 4) == 0.2763


def test_simple_r_normal(fm: FinancialMath) -> None:
    """Test simple rate calculation with normal inputs."""
    result = fm.simple_r(0.05, 10, 2)
    assert round(result, 4) == 0.25


def test_pv_compound_normal(fm: FinancialMath) -> None:
    """Test present value with compound capitalization."""
    result = fm.pv(0.05, 10, 1000.0, str_capitalization="compound")
    assert round(result, 2) == -613.91


def test_fv_simple_normal(fm: FinancialMath) -> None:
    """Test future value with simple capitalization."""
    result = fm.fv(0.05, 10, 1000.0, str_capitalization="simple")
    assert round(result, 2) == 1500.0


def test_irr_normal(fm: FinancialMath) -> None:
    """Test IRR calculation with normal cash flows."""
    result = fm.irr([-100.0, 60.0, 60.0])
    assert round(result, 2) == 0.13


def test_npv_normal(fm: FinancialMath) -> None:
    """Test NPV calculation with normal cash flows."""
    result = fm.npv(0.05, [-100.0, 60.0, 60.0])
    assert round(result, 2) == 11.56


def test_pmt_normal(fm: FinancialMath) -> None:
    """Test payment calculation with normal inputs."""
    result = fm.pmt(0.05/12, 60, 10000.0)
    assert round(result, 2) == -188.71


def test_pv_cfs_normal(fm: FinancialMath) -> None:
    """Test present value of cash flows with normal inputs."""
    periods, pvs = fm.pv_cfs([100.0, 200.0, 300.0], 0.05)
    assert len(periods) == 3
    assert len(pvs) == 3
    assert round(pvs[0], 2) == -95.24


def test_compound_r_zero_periods(fm: FinancialMath) -> None:
    """Test compound rate with zero periods."""
    result = fm.compound_r(0.05, 0, 2)
    assert result == 0.0


def test_simple_r_zero_rate(fm: FinancialMath) -> None:
    """Test simple rate with zero interest rate."""
    result = fm.simple_r(0.0, 10, 2)
    assert result == 0.0


def test_pv_zero_rate(fm: FinancialMath) -> None:
    """Test present value with zero interest rate."""
    result = fm.pv(0.0, 10, 1000.0)
    assert round(result, 2) == -1000.0


def test_fv_zero_periods(fm: FinancialMath) -> None:
    """Test future value with zero periods."""
    result = fm.fv(0.05, 0, 1000.0)
    assert round(result, 2) == -1000.0


def test_irr_single_flow(fm: FinancialMath) -> None:
    """Test IRR with single cash flow."""
    with pytest.raises(ValueError):
        fm.irr([100.0])


def test_npv_empty_flows(fm: FinancialMath) -> None:
    """Test NPV with empty cash flows."""
    with pytest.raises(ValueError):
        fm.npv(0.05, [])


def test_pv_invalid_capitalization(fm: FinancialMath) -> None:
    """Test PV with invalid capitalization type."""
    with pytest.raises(ValueError):
        fm.pv(0.05, 10, 1000.0, str_capitalization="invalid")


def test_fv_negative_pv(fm: FinancialMath) -> None:
    """Test FV with negative present value."""
    result = fm.fv(0.05, 10, -1000.0)
    assert result > 0


def test_irr_all_positive(fm: FinancialMath) -> None:
    """Test IRR with all positive cash flows."""
    with pytest.raises(ValueError):
        fm.irr([100.0, 200.0, 300.0])


def test_irr_all_negative(fm: FinancialMath) -> None:
    """Test IRR with all negative cash flows."""
    with pytest.raises(ValueError):
        fm.irr([-100.0, -200.0, -300.0])


def test_pmt_zero_periods(fm: FinancialMath) -> None:
    """Test payment calculation with zero periods."""
    with pytest.raises(ValueError):
        fm.pmt(0.05, 0, 10000.0)


def test_compound_r_type_validation(fm: FinancialMath) -> None:
    """Test type validation in compound_r method."""
    with pytest.raises(TypeError):
        fm.compound_r("0.05", 10, 2)


def test_simple_r_type_validation(fm: FinancialMath) -> None:
    """Test type validation in simple_r method."""
    with pytest.raises(TypeError):
        fm.simple_r(0.05, "10", 2)


def test_pv_type_validation(fm: FinancialMath) -> None:
    """Test type validation in pv method."""
    with pytest.raises(TypeError):
        fm.pv(0.05, 10, "1000")


def test_fv_type_validation(fm: FinancialMath) -> None:
    """Test type validation in fv method."""
    with pytest.raises(TypeError):
        fm.fv(0.05, "10", 1000)


def test_irr_type_validation(fm: FinancialMath) -> None:
    """Test type validation in irr method."""
    with pytest.raises(TypeError):
        fm.irr(["-100", "60", "60"])


def test_nan_handling(fm: FinancialMath) -> None:
    """Test handling of NaN values."""
    with pytest.raises(ValueError):
        fm.compound_r(float('nan'), 10, 2)


def test_infinity_handling(fm: FinancialMath) -> None:
    """Test handling of infinity values."""
    with pytest.raises(ValueError):
        fm.simple_r(float('inf'), 10, 2)


def test_extreme_high_rate(fm: FinancialMath) -> None:
    """Test with extremely high interest rate."""
    result = fm.compound_r(10.0, 10, 1)
    assert result > 1000


def test_extreme_long_period(fm: FinancialMath) -> None:
    """Test with extremely long period."""
    result = fm.simple_r(0.05, 1000, 1)
    assert result == 50.0


def test_extreme_cash_flows(fm: FinancialMath) -> None:
    """Test with extremely large cash flows."""
    result = fm.npv(0.05, [-1e12, 1e12])
    assert result == pytest.approx(-4.7619e10, rel=1e-2)


def test_extreme_small_values(fm: FinancialMath) -> None:
    """Test with extremely small values."""
    result = fm.pv(1e-10, 1000, 1e-10)
    assert result == pytest.approx(-9.9999e-11, abs=1e-15)
