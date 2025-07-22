"""Unit tests for ROEDecomposition class.

Tests the DuPont Analysis (3-step and 5-step) ROE decomposition functionality.
"""

import math

import pytest

from stpstone.analytics.perf_metrics.roe_decomposition import ROEDecomposition


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def roe_analyzer() -> ROEDecomposition:
    """Fixture providing an ROEDecomposition instance."""
    return ROEDecomposition()


@pytest.fixture
def normal_financial_values() -> dict[str, float]:
    """Fixture providing typical financial values for testing."""
    return {
        "float_ni": 150000.0,
        "float_net_revenue": 1000000.0,
        "float_avg_ta": 2000000.0,
        "float_avg_te": 500000.0,
        "float_ebt": 200000.0,
        "float_ebit": 250000.0,
    }


@pytest.fixture
def edge_case_values() -> dict[str, float]:
    """Fixture providing edge case financial values for testing."""
    return {
        "float_ni": 1.0,
        "float_net_revenue": 1.0,
        "float_avg_ta": 1.0,
        "float_avg_te": 1.0,
        "float_ebt": 1.0,
        "float_ebit": 1.0,
    }


# --------------------------
# Tests
# --------------------------
def test_normal_operations(
    roe_analyzer: ROEDecomposition, 
    normal_financial_values: dict[str, float]
) -> None:
    """Test DuPont analysis with normal financial values.
    
    Parameters
    ----------
    roe_analyzer : ROEDecomposition
        An instance of the ROEDecomposition class.
    normal_financial_values : dict[str, float]
        A dictionary containing normal financial values.
    """
    result = roe_analyzer.dupont_analysis(**normal_financial_values)
    
    # verify structure of results
    assert set(result.keys()) == {"roe_3_step", "roe_5_step", "intermediate_metrics"}
    assert isinstance(result["roe_3_step"], float)
    assert isinstance(result["roe_5_step"], float)
    assert isinstance(result["intermediate_metrics"], dict)
    
    # verify intermediate metrics
    metrics = result["intermediate_metrics"]
    assert set(metrics.keys()) == {
        "Net Profit Margin", "Asset Turnover", "Equity Multiplier",
        "Tax Burden", "Interest Burden", "Operating Margin"
    }
    
    # verify calculations
    assert metrics["Net Profit Margin"] == pytest.approx(0.15, rel=1e-3)
    assert metrics["Asset Turnover"] == pytest.approx(0.5, rel=1e-3)
    assert metrics["Equity Multiplier"] == pytest.approx(4.0, rel=1e-3)
    assert metrics["Tax Burden"] == pytest.approx(0.75, rel=1e-3)
    assert metrics["Interest Burden"] == pytest.approx(0.8, rel=1e-3)
    assert metrics["Operating Margin"] == pytest.approx(0.25, rel=1e-3)
    
    # verify ROE calculations
    assert result["roe_3_step"] == pytest.approx(0.15 * 0.5 * 4.0, rel=1e-3)
    assert result["roe_5_step"] == pytest.approx(0.75 * 0.8 * 0.25 * 0.5 * 4.0, rel=1e-3)


def test_edge_case_values(
    roe_analyzer: ROEDecomposition, 
    edge_case_values: dict[str, float]
) -> None:
    """Test DuPont analysis with edge case values (all 1.0).
    
    Parameters
    ----------
    roe_analyzer : ROEDecomposition
        An instance of the ROEDecomposition class.
    edge_case_values : dict[str, float]
        A dictionary containing edge case financial values.
    """
    result = roe_analyzer.dupont_analysis(**edge_case_values)
    
    # all metrics should be 1.0 in this case
    for metric in result["intermediate_metrics"].values():
        assert metric == pytest.approx(1.0, rel=1e-3)
    
    # ROE should be 1.0 (1 * 1 * 1)
    assert result["roe_3_step"] == pytest.approx(1.0, rel=1e-3)
    assert result["roe_5_step"] == pytest.approx(1.0, rel=1e-3)


def test_zero_or_negative_values(
    roe_analyzer: ROEDecomposition, 
    normal_financial_values: dict[str, float]
) -> None:
    """Test that zero or negative values raise ValueError.
    
    Parameters
    ----------
    roe_analyzer : ROEDecomposition
        An instance of the ROEDecomposition class.
    normal_financial_values : dict[str, float]
        A dictionary containing normal financial values.
    """
    # test each required positive value
    for key in ["float_net_revenue", "float_avg_ta", "float_avg_te", "float_ebt", "float_ebit"]:
        test_values = normal_financial_values.copy()
        
        # test zero value
        test_values[key] = 0.0
        with pytest.raises(ValueError, match="Inputs must be positive and non-zero"):
            roe_analyzer.dupont_analysis(**test_values)
        
        # test negative value
        test_values[key] = -1.0
        with pytest.raises(ValueError, match="Inputs must be positive and non-zero"):
            roe_analyzer.dupont_analysis(**test_values)


def test_type_validation(
    roe_analyzer: ROEDecomposition, 
    normal_financial_values: dict[str, float]
) -> None:
    """Test that non-float inputs raise TypeError.
    
    Parameters
    ----------
    roe_analyzer : ROEDecomposition
        An instance of the ROEDecomposition class.
    normal_financial_values : dict[str, float]
        A dictionary containing normal financial values.
    """
    for key in normal_financial_values:
        test_values = normal_financial_values.copy()
        
        # test string input
        test_values[key] = "not a float"
        with pytest.raises(TypeError):
            roe_analyzer.dupont_analysis(**test_values)
        
        # test None input
        test_values[key] = None
        with pytest.raises(TypeError):
            roe_analyzer.dupont_analysis(**test_values)


def test_nan_inf_values(
    roe_analyzer: ROEDecomposition, 
    normal_financial_values: dict[str, float]
) -> None:
    """Test that NaN and Inf values are handled properly.
    
    Parameters
    ----------
    roe_analyzer : ROEDecomposition
        An instance of the ROEDecomposition class.
    normal_financial_values : dict[str, float]
        A dictionary containing normal financial values.
    """
    test_values = normal_financial_values.copy()
    
    # test nan
    test_values["float_ni"] = float('nan')
    result = roe_analyzer.dupont_analysis(**test_values)
    assert math.isnan(result["roe_3_step"])
    assert math.isnan(result["roe_5_step"])
    
    # test inf
    test_values["float_ni"] = float('inf')
    result = roe_analyzer.dupont_analysis(**test_values)
    assert math.isinf(result["roe_3_step"])
    assert math.isinf(result["roe_5_step"])


def test_consistency_between_3_and_5_step(
    roe_analyzer: ROEDecomposition, 
    normal_financial_values: dict[str, float]
) -> None:
    """Test that 3-step and 5-step ROE produce consistent results.
    
    Parameters
    ----------
    roe_analyzer : ROEDecomposition
        An instance of the ROEDecomposition class.
    normal_financial_values : dict[str, float]
        A dictionary containing normal financial values.
    """
    result = roe_analyzer.dupont_analysis(**normal_financial_values)
    
    # the two methods should produce the same ROE (within floating point precision)
    assert result["roe_3_step"] == pytest.approx(result["roe_5_step"], rel=1e-9)


def test_extreme_ratios(roe_analyzer: ROEDecomposition) -> None:
    """Test with extreme ratio values.
    
    Parameters
    ----------
    roe_analyzer : ROEDecomposition
        An instance of the ROEDecomposition class.
    """
    extreme_values = {
        "float_ni": 1e12,
        "float_net_revenue": 1.0,
        "float_avg_ta": 1.0,
        "float_avg_te": 1.0,
        "float_ebt": 1e12,
        "float_ebit": 1e12,
    }
    
    result = roe_analyzer.dupont_analysis(**extreme_values)
    
    # verify metrics
    metrics = result["intermediate_metrics"]
    assert metrics["Net Profit Margin"] == pytest.approx(1e12)
    assert metrics["Asset Turnover"] == pytest.approx(1.0)
    assert metrics["Equity Multiplier"] == pytest.approx(1.0)
    assert metrics["Tax Burden"] == pytest.approx(1.0)
    assert metrics["Interest Burden"] == pytest.approx(1.0)
    assert metrics["Operating Margin"] == pytest.approx(1e12)
    
    # verify ROE calculations
    assert result["roe_3_step"] == pytest.approx(1e12)
    assert result["roe_5_step"] == pytest.approx(1e12)