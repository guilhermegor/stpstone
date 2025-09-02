"""Unit tests for Benford's Law in StatisticalDistributionsHT class.

Tests the benford_law method for fraud detection by evaluating first digit
occurrences, covering normal operations, edge cases, and error conditions.
"""

import sys

import numpy as np
from numpy.typing import NDArray
import pytest
from pytest_mock import MockerFixture

from stpstone.analytics.quant.statistical_inference import (
    BenfordResults,
    StatisticalDistributionsHT,
    validate_array,
    validate_scalar,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def stat_dist_ht() -> StatisticalDistributionsHT:
    """Fixture providing StatisticalDistributionsHT instance.

    Returns
    -------
    StatisticalDistributionsHT
        Instance of the StatisticalDistributionsHT class
    """
    return StatisticalDistributionsHT()


@pytest.fixture
def sample_data() -> NDArray[np.float64]:
    """Fixture providing sample data with first digits following Benford's Law.

    Returns
    -------
    NDArray[np.float64]
        Array of numbers with first digits approximately following Benford's Law
    """
    # generated to have roughly Benford-like distribution for first digits
    return np.array([123, 234, 345, 456, 567, 678, 789, 112, 223, 334], dtype=np.float64)


@pytest.fixture
def occurrence_data() -> NDArray[np.float64]:
    """Fixture providing occurrence counts for first digits (1-9).

    Returns
    -------
    NDArray[np.float64]
        Array of counts for first digits 1 to 9
    """
    return np.array([30, 18, 12, 10, 8, 7, 6, 5, 4], dtype=np.float64)


# --------------------------
# Tests for validate_array
# --------------------------
def test_validate_array_not_numpy(stat_dist_ht: StatisticalDistributionsHT) -> None:
    """Test validate_array raises TypeError for non-numpy array.

    Parameters
    ----------
    stat_dist_ht : StatisticalDistributionsHT
        Instance of StatisticalDistributionsHT class

    Returns
    -------
    None

    Verifies
    --------
    That passing a list instead of numpy array raises TypeError with correct message.
    """
    with pytest.raises(TypeError, match="must be of type"):
        validate_array([1, 2, 3], "array_data")


def test_validate_array_wrong_dtype(stat_dist_ht: StatisticalDistributionsHT) -> None:
    """Test validate_array raises TypeError for non-float64 array.

    Parameters
    ----------
    stat_dist_ht : StatisticalDistributionsHT
        Instance of StatisticalDistributionsHT class

    Returns
    -------
    None

    Verifies
    --------
    That passing a numpy array with int32 dtype raises TypeError with correct message.
    """
    with pytest.raises(TypeError, match="array_data must be of type float64"):
        validate_array(np.array([1, 2, 3], dtype=np.int32), "array_data")


def test_validate_array_too_few_samples(stat_dist_ht: StatisticalDistributionsHT) -> None:
    """Test validate_array raises ValueError for insufficient samples.

    Parameters
    ----------
    stat_dist_ht : StatisticalDistributionsHT
        Instance of StatisticalDistributionsHT class

    Returns
    -------
    None

    Verifies
    --------
    That passing an array with fewer than min_samples raises ValueError with correct message.
    """
    with pytest.raises(ValueError, match="array_data must have at least 2 samples"):
        validate_array(np.array([1.0], dtype=np.float64), "array_data", min_samples=2)


def test_validate_array_non_finite(stat_dist_ht: StatisticalDistributionsHT) -> None:
    """Test validate_array raises ValueError for non-finite values.

    Parameters
    ----------
    stat_dist_ht : StatisticalDistributionsHT
        Instance of StatisticalDistributionsHT class

    Returns
    -------
    None

    Verifies
    --------
    That passing an array with NaN or Inf raises ValueError with correct message.
    """
    with pytest.raises(ValueError, match="array_data contains NaN or infinite values"):
        validate_array(np.array([1.0, np.nan, 3.0], dtype=np.float64), "array_data")


# --------------------------
# Tests for validate_scalar
# --------------------------
def test_validate_scalar_not_float(stat_dist_ht: StatisticalDistributionsHT) -> None:
    """Test validate_scalar raises TypeError for non-float value.

    Parameters
    ----------
    stat_dist_ht : StatisticalDistributionsHT
        Instance of StatisticalDistributionsHT class

    Returns
    -------
    None

    Verifies
    --------
    That passing a non-float value raises TypeError with correct message.
    """
    with pytest.raises(TypeError, match="must be of type"):
        validate_scalar("0.05", "alpha")


def test_validate_scalar_non_finite(stat_dist_ht: StatisticalDistributionsHT) -> None:
    """Test validate_scalar raises ValueError for non-finite value.

    Parameters
    ----------
    stat_dist_ht : StatisticalDistributionsHT
        Instance of StatisticalDistributionsHT class

    Returns
    -------
    None

    Verifies
    --------
    That passing NaN or Inf raises ValueError with correct message.
    """
    with pytest.raises(ValueError, match="alpha must be finite"):
        validate_scalar(np.nan, "alpha")


def test_validate_scalar_out_of_bounds(stat_dist_ht: StatisticalDistributionsHT) -> None:
    """Test validate_scalar raises ValueError for out-of-bounds value.

    Parameters
    ----------
    stat_dist_ht : StatisticalDistributionsHT
        Instance of StatisticalDistributionsHT class

    Returns
    -------
    None

    Verifies
    --------
    That passing a value outside specified bounds raises ValueError with correct message.
    """
    with pytest.raises(ValueError, match="alpha must be at least 0.0"):
        validate_scalar(-0.1, "alpha", min_val=0.0, max_val=1.0)


# --------------------------
# Tests for benford_law
# --------------------------
def test_benford_law_valid_raw_data(
    stat_dist_ht: StatisticalDistributionsHT, 
    sample_data: NDArray[np.float64]
) -> None:
    """Test benford_law with valid raw data input.

    Parameters
    ----------
    stat_dist_ht : StatisticalDistributionsHT
        Instance of StatisticalDistributionsHT class
    sample_data : NDArray[np.float64]
        Sample data with first digits following Benford's Law

    Returns
    -------
    None

    Verifies
    --------
    That benford_law processes raw data correctly and returns expected distributions.
    """
    result: BenfordResults = stat_dist_ht.benford_law(sample_data)
    expected_benford = np.array([np.log10(i + 2) - np.log10(i + 1) for i in range(9)])
    assert isinstance(result, dict)
    assert "benford_expected_array" in result
    assert "real_numbers_observed_array" in result
    assert np.allclose(result["benford_expected_array"], expected_benford, rtol=1e-5)
    assert result["real_numbers_observed_array"].shape == (9,)
    assert np.all(result["real_numbers_observed_array"] >= 0)
    assert np.all(result["real_numbers_observed_array"] <= 1)
    assert pytest.approx(np.sum(result["real_numbers_observed_array"]), rel=1e-3) == 1.0


def test_benford_law_valid_occurrence_data(
    stat_dist_ht: StatisticalDistributionsHT, 
    occurrence_data: NDArray[np.float64]
) -> None:
    """Test benford_law with valid occurrence counts input.

    Parameters
    ----------
    stat_dist_ht : StatisticalDistributionsHT
        Instance of StatisticalDistributionsHT class
    occurrence_data : NDArray[np.float64]
        Array of first digit occurrence counts

    Returns
    -------
    None

    Verifies
    --------
    That benford_law processes occurrence counts correctly and returns expected distributions.
    """
    result: BenfordResults = stat_dist_ht.benford_law(occurrence_data, 
                                                      bool_list_number_occurrencies=True)
    expected_benford = np.array([np.log10(i + 2) - np.log10(i + 1) for i in range(9)])
    assert isinstance(result, dict)
    assert "benford_expected_array" in result
    assert "real_numbers_observed_array" in result
    assert np.allclose(result["benford_expected_array"], expected_benford, rtol=1e-5)
    assert result["real_numbers_observed_array"].shape == (9,)
    assert np.allclose(result["real_numbers_observed_array"], 
                       occurrence_data / occurrence_data.sum(), rtol=1e-5)
    assert pytest.approx(np.sum(result["real_numbers_observed_array"]), rel=1e-3) == 1.0


def test_benford_law_non_boolean_flag(
    stat_dist_ht: StatisticalDistributionsHT, 
    sample_data: NDArray[np.float64]
) -> None:
    """Test benford_law raises TypeError for non-boolean flag.

    Parameters
    ----------
    stat_dist_ht : StatisticalDistributionsHT
        Instance of StatisticalDistributionsHT class
    sample_data : NDArray[np.float64]
        Sample data with first digits

    Returns
    -------
    None

    Verifies
    --------
    That passing a non-boolean bool_list_number_occurrencies raises TypeError with correct message.
    """
    with pytest.raises(TypeError, match="bool_list_number_occurrencies must be of type bool"):
        stat_dist_ht.benford_law(sample_data, bool_list_number_occurrencies="True")


def test_benford_law_zero_sum_occurrence(stat_dist_ht: StatisticalDistributionsHT) -> None:
    """Test benford_law raises ValueError for zero-sum occurrence counts.

    Parameters
    ----------
    stat_dist_ht : StatisticalDistributionsHT
        Instance of StatisticalDistributionsHT class

    Returns
    -------
    None

    Verifies
    --------
    That passing an occurrence array summing to zero raises ValueError with correct message.
    """
    invalid_data = np.array([0, 0, 0, 0, 0, 0, 0, 0, 0], dtype=np.float64)
    with pytest.raises(ValueError, match="Sum of array_data cannot be zero"):
        stat_dist_ht.benford_law(invalid_data, bool_list_number_occurrencies=True)


def test_benford_law_all_zero_digits(stat_dist_ht: StatisticalDistributionsHT) -> None:
    """Test benford_law raises ValueError for data with all zero first digits.

    Parameters
    ----------
    stat_dist_ht : StatisticalDistributionsHT
        Instance of StatisticalDistributionsHT class

    Returns
    -------
    None

    Verifies
    --------
    That passing data with all zero first digits raises ValueError with correct message.
    """
    invalid_data = np.array([0, 0, 0], dtype=np.float64)
    with pytest.raises(ValueError, match="No valid non-zero first digits found"):
        stat_dist_ht.benford_law(invalid_data)


def test_benford_law_empty_array(stat_dist_ht: StatisticalDistributionsHT) -> None:
    """Test benford_law raises ValueError for empty array.

    Parameters
    ----------
    stat_dist_ht : StatisticalDistributionsHT
        Instance of StatisticalDistributionsHT class

    Returns
    -------
    None

    Verifies
    --------
    That passing an empty array raises ValueError with correct message.
    """
    with pytest.raises(ValueError, match="array_data must have at least 1 samples"):
        stat_dist_ht.benford_law(np.array([], dtype=np.float64))


def test_benford_law_single_value(stat_dist_ht: StatisticalDistributionsHT) -> None:
    """Test benford_law with single valid value.

    Parameters
    ----------
    stat_dist_ht : StatisticalDistributionsHT
        Instance of StatisticalDistributionsHT class

    Returns
    -------
    None

    Verifies
    --------
    That benford_law processes a single valid value correctly and returns expected distributions.
    """
    data = np.array([123.45], dtype=np.float64)
    result: BenfordResults = stat_dist_ht.benford_law(data)
    expected_benford = np.array([np.log10(i + 2) - np.log10(i + 1) for i in range(9)])
    expected_observed = np.zeros(9)
    expected_observed[0] = 1.0  # first digit is 1
    assert np.allclose(result["benford_expected_array"], expected_benford, rtol=1e-5)
    assert np.allclose(result["real_numbers_observed_array"], expected_observed, rtol=1e-5)


def test_benford_law_negative_values(stat_dist_ht: StatisticalDistributionsHT) -> None:
    """Test benford_law handles negative values correctly.

    Parameters
    ----------
    stat_dist_ht : StatisticalDistributionsHT
        Instance of StatisticalDistributionsHT class

    Returns
    -------
    None

    Verifies
    --------
    That benford_law correctly processes negative values by using absolute values for first digits.
    """
    data = np.array([-123, -234, 345], dtype=np.float64)
    result: BenfordResults = stat_dist_ht.benford_law(data)
    expected_benford = np.array([np.log10(i + 2) - np.log10(i + 1) for i in range(9)])
    expected_observed = np.zeros(9)
    expected_observed[0] = 1/3  # One 1 from -123
    expected_observed[1] = 1/3  # One 2 from -234
    expected_observed[2] = 1/3  # One 3 from 345
    assert np.allclose(result["benford_expected_array"], expected_benford, rtol=1e-5)
    assert np.allclose(result["real_numbers_observed_array"], expected_observed, rtol=1e-5)


def test_benford_law_large_numbers(stat_dist_ht: StatisticalDistributionsHT) -> None:
    """Test benford_law with large numbers.

    Parameters
    ----------
    stat_dist_ht : StatisticalDistributionsHT
        Instance of StatisticalDistributionsHT class

    Returns
    -------
    None

    Verifies
    --------
    That benford_law correctly handles large numbers and returns expected distributions.
    """
    data = np.array([123456789, 234567890, 345678901], dtype=np.float64)
    result: BenfordResults = stat_dist_ht.benford_law(data)
    expected_benford = np.array([np.log10(i + 2) - np.log10(i + 1) for i in range(9)])
    expected_observed = np.zeros(9)
    expected_observed[0] = 1/3  # 123456789
    expected_observed[1] = 1/3  # 234567890
    expected_observed[2] = 1/3  # 345678901
    assert np.allclose(result["benford_expected_array"], expected_benford, rtol=1e-5)
    assert np.allclose(result["real_numbers_observed_array"], expected_observed, rtol=1e-5)


def test_benford_law_non_finite_values(stat_dist_ht: StatisticalDistributionsHT) -> None:
    """Test benford_law raises ValueError for non-finite values.

    Parameters
    ----------
    stat_dist_ht : StatisticalDistributionsHT
        Instance of StatisticalDistributionsHT class

    Returns
    -------
    None

    Verifies
    --------
    That passing an array with non-finite values raises ValueError with correct message.
    """
    invalid_data = np.array([123, np.inf, 456], dtype=np.float64)
    with pytest.raises(ValueError, match="array_data contains NaN or infinite values"):
        stat_dist_ht.benford_law(invalid_data)


def test_benford_law_percentual_occurrence_validation(
    stat_dist_ht: StatisticalDistributionsHT, 
    mocker: MockerFixture
) -> None:
    """Test benford_law validates percentual_occurrence values.

    Parameters
    ----------
    stat_dist_ht : StatisticalDistributionsHT
        Instance of StatisticalDistributionsHT class
    mocker : MockerFixture
        Pytest-mock fixture for mocking validate_scalar

    Returns
    -------
    None

    Verifies
    --------
    That validate_scalar is called for each percentual_occurrence value.
    """
    data = np.array([123, 234, 345], dtype=np.float64)
    mock_validate_scalar = mocker.patch(
        "stpstone.analytics.quant.statistical_inference.validate_scalar")
    stat_dist_ht.benford_law(data)
    assert mock_validate_scalar.call_count == 0


def test_benford_law_reload_module(
    stat_dist_ht: StatisticalDistributionsHT, 
    sample_data: NDArray[np.float64], 
    mocker: MockerFixture
) -> None:
    """Test benford_law after module reload.

    Parameters
    ----------
    stat_dist_ht : StatisticalDistributionsHT
        Instance of StatisticalDistributionsHT class
    sample_data : NDArray[np.float64]
        Sample data with first digits
    mocker : MockerFixture
        Pytest-mock fixture for mocking imports

    Returns
    -------
    None

    Verifies
    --------
    That benford_law works correctly after module reload.
    """
    import importlib
    mocker.patch("stpstone.analytics.quant.statistical_inference.np")
    importlib.reload(sys.modules["stpstone.analytics.quant.statistical_inference"])
    new_instance = StatisticalDistributionsHT()
    result: BenfordResults = new_instance.benford_law(sample_data)
    assert isinstance(result, dict)
    assert "benford_expected_array" in result
    assert "real_numbers_observed_array" in result
    assert result["benford_expected_array"].shape == (9,)
    assert result["real_numbers_observed_array"].shape == (9,)