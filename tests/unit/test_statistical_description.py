"""Unit tests for StatisticalDescription class.

Tests the statistical analysis functionality, covering:
- Descriptive statistics computation
- Error metrics for model fitting
- Quantile calculations
- Input validation and error handling
- Edge cases and type checking
"""

import importlib
import math
import sys

import numpy as np
from numpy.typing import NDArray
import pytest

from stpstone.analytics.quant.statistical_description import StatisticalDescription


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def stats_instance() -> StatisticalDescription:
	"""Fixture providing a StatisticalDescription instance.

	Returns
	-------
	StatisticalDescription
		An instance of the StatisticalDescription class.
	"""
	return StatisticalDescription()


@pytest.fixture
def sample_array() -> NDArray[np.float64]:
	"""Fixture providing a sample array for testing.

	Returns
	-------
	NDArray[np.float64]
		Sample array of real numbers.
	"""
	return np.array([1.0, 2.0, 3.0, 4.0, 5.0], dtype=np.float64)


@pytest.fixture
def sample_arrays_pair() -> tuple[NDArray[np.float64], NDArray[np.float64]]:
	"""Fixture providing a pair of arrays for covariance/correlation tests.

	Returns
	-------
	tuple[NDArray[np.float64], NDArray[np.float64]]
		A pair of arrays.
	"""
	array_1 = np.array([1.0, 2.0, 3.0, 4.0, 5.0], dtype=np.float64)
	array_2 = np.array([2.0, 4.0, 6.0, 8.0, 10.0], dtype=np.float64)
	return array_1, array_2


@pytest.fixture
def observations_predictions() -> tuple[NDArray[np.float64], NDArray[np.float64]]:
	"""Fixture providing observation and prediction arrays for error metrics.

	Returns
	-------
	tuple[NDArray[np.float64], NDArray[np.float64]]
		A pair of arrays.
	"""
	observations = np.array([1.0, 2.0, 3.0], dtype=np.float64)
	predictions = np.array([1.1, 2.2, 2.9], dtype=np.float64)
	return observations, predictions


# --------------------------
# Tests for statistical_description
# --------------------------
def test_statistical_description_valid_input(
	stats_instance: StatisticalDescription, sample_array: NDArray[np.float64]
) -> None:
	"""Test statistical_description with valid input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.
	sample_array : NDArray[np.float64]
		Sample array of real numbers.

	Returns
	-------
	None
	"""
	result = stats_instance.statistical_description(sample_array)
	assert isinstance(result, dict)
	expected_keys = {
		"nobs",
		"minmax",
		"mean",
		"median",
		"mode",
		"variance_sample",
		"standard_deviation_sample",
		"skewness",
		"kurtosis",
	}
	assert set(result.keys()) == expected_keys
	assert result["nobs"] == 5
	assert result["minmax"] == pytest.approx((1.0, 5.0))
	assert result["mean"] == pytest.approx(3.0)
	assert result["median"] == pytest.approx(3.0)
	assert np.allclose(result["mode"][0], np.array([1.0]))
	assert np.allclose(result["mode"][1], np.array([1]))
	assert result["variance_sample"] == pytest.approx(2.5)
	assert result["standard_deviation_sample"] == pytest.approx(math.sqrt(2.5))
	# verify dictionary structure instead of isinstance
	assert all(isinstance(result[key], (int, float, tuple, np.ndarray)) for key in expected_keys)
	assert isinstance(result["mode"], tuple)
	assert isinstance(result["mode"][0], np.ndarray)
	assert isinstance(result["mode"][1], np.ndarray)


def test_statistical_description_empty_input(stats_instance: StatisticalDescription) -> None:
	"""Test statistical_description with empty input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Input array cannot be empty"):
		stats_instance.statistical_description(np.array([], dtype=np.float64))


def test_statistical_description_non_finite_input(stats_instance: StatisticalDescription) -> None:
	"""Test statistical_description with non-finite input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	array_with_nan = np.array([1.0, np.nan, 3.0], dtype=np.float64)
	array_with_inf = np.array([1.0, np.inf, 3.0], dtype=np.float64)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.statistical_description(array_with_nan)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.statistical_description(array_with_inf)


def test_statistical_description_single_element(stats_instance: StatisticalDescription) -> None:
	"""Test statistical_description with single-element input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	array = np.array([1.0], dtype=np.float64)
	result = stats_instance.statistical_description(array)
	assert isinstance(result, dict)
	expected_keys = {
		"nobs",
		"minmax",
		"mean",
		"median",
		"mode",
		"variance_sample",
		"standard_deviation_sample",
		"skewness",
		"kurtosis",
	}
	assert set(result.keys()) == expected_keys
	assert result["nobs"] == 1
	assert result["minmax"] == pytest.approx((1.0, 1.0))
	assert result["mean"] == pytest.approx(1.0)
	assert result["median"] == pytest.approx(1.0)
	assert np.allclose(result["mode"][0], np.array([1.0]))
	assert np.allclose(result["mode"][1], np.array([1]))
	assert result["variance_sample"] == pytest.approx(0.0)
	assert result["standard_deviation_sample"] == pytest.approx(0.0)
	assert all(isinstance(result[key], (int, float, tuple, np.ndarray)) for key in expected_keys)
	assert isinstance(result["mode"], tuple)
	assert isinstance(result["mode"][0], np.ndarray)
	assert isinstance(result["mode"][1], np.ndarray)


# --------------------------
# Tests for standard_deviation_sample
# --------------------------
def test_standard_deviation_sample_valid_input(
	stats_instance: StatisticalDescription, sample_array: NDArray[np.float64]
) -> None:
	"""Test standard_deviation_sample with valid input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.
	sample_array : NDArray[np.float64]
		Sample array of real numbers.

	Returns
	-------
	None
	"""
	result = stats_instance.standard_deviation_sample(sample_array)
	assert result == pytest.approx(math.sqrt(2.5))
	assert isinstance(result, float)


def test_standard_deviation_sample_empty_input(stats_instance: StatisticalDescription) -> None:
	"""Test standard_deviation_sample with empty input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Input array cannot be empty"):
		stats_instance.standard_deviation_sample(np.array([], dtype=np.float64))


def test_standard_deviation_sample_non_finite_input(
	stats_instance: StatisticalDescription,
) -> None:
	"""Test standard_deviation_sample with non-finite input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	array_with_nan = np.array([1.0, np.nan, 3.0], dtype=np.float64)
	array_with_inf = np.array([1.0, np.inf, 3.0], dtype=np.float64)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.standard_deviation_sample(array_with_nan)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.standard_deviation_sample(array_with_inf)


# --------------------------
# Tests for harmonic_mean
# --------------------------
def test_harmonic_mean_valid_input(
	stats_instance: StatisticalDescription, sample_array: NDArray[np.float64]
) -> None:
	"""Test harmonic_mean with valid input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.
	sample_array : NDArray[np.float64]
		Sample array of real numbers.

	Returns
	-------
	None
	"""
	result = stats_instance.harmonic_mean(sample_array)
	expected = 5 / (1 / 1.0 + 1 / 2.0 + 1 / 3.0 + 1 / 4.0 + 1 / 5.0)
	assert result == pytest.approx(expected)
	assert isinstance(result, float)


def test_harmonic_mean_empty_input(stats_instance: StatisticalDescription) -> None:
	"""Test harmonic_mean with empty input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Input array cannot be empty"):
		stats_instance.harmonic_mean(np.array([], dtype=np.float64))


def test_harmonic_mean_non_finite_input(stats_instance: StatisticalDescription) -> None:
	"""Test harmonic_mean with non-finite input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	array_with_nan = np.array([1.0, np.nan, 3.0], dtype=np.float64)
	array_with_inf = np.array([1.0, np.inf, 3.0], dtype=np.float64)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.harmonic_mean(array_with_nan)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.harmonic_mean(array_with_inf)


def test_harmonic_mean_non_positive_input(stats_instance: StatisticalDescription) -> None:
	"""Test harmonic_mean with non-positive input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	array_with_zero = np.array([1.0, 0.0, 3.0], dtype=np.float64)
	array_with_negative = np.array([1.0, -1.0, 3.0], dtype=np.float64)
	with pytest.raises(ValueError, match="Input array must contain positive values"):
		stats_instance.harmonic_mean(array_with_zero)
	with pytest.raises(ValueError, match="Input array must contain positive values"):
		stats_instance.harmonic_mean(array_with_negative)


# --------------------------
# Tests for median_sample
# --------------------------
def test_median_sample_valid_input(
	stats_instance: StatisticalDescription, sample_array: NDArray[np.float64]
) -> None:
	"""Test median_sample with valid input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.
	sample_array : NDArray[np.float64]
		Sample array of real numbers.

	Returns
	-------
	None
	"""
	result = stats_instance.median_sample(sample_array)
	assert result == pytest.approx(3.0)
	assert isinstance(result, float)


def test_median_sample_empty_input(stats_instance: StatisticalDescription) -> None:
	"""Test median_sample with empty input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Input array cannot be empty"):
		stats_instance.median_sample(np.array([], dtype=np.float64))


def test_median_sample_non_finite_input(stats_instance: StatisticalDescription) -> None:
	"""Test median_sample with non-finite input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	array_with_nan = np.array([1.0, np.nan, 3.0], dtype=np.float64)
	array_with_inf = np.array([1.0, np.inf, 3.0], dtype=np.float64)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.median_sample(array_with_nan)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.median_sample(array_with_inf)


# --------------------------
# Tests for mode_sample
# --------------------------
def test_mode_sample_valid_input(
	stats_instance: StatisticalDescription, sample_array: NDArray[np.float64]
) -> None:
	"""Test mode_sample with valid input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.
	sample_array : NDArray[np.float64]
		Sample array of real numbers.

	Returns
	-------
	None
	"""
	result = stats_instance.mode_sample(sample_array)
	assert np.allclose(result[0], np.array([1.0]))
	assert np.allclose(result[1], np.array([1]))
	assert isinstance(result, tuple)
	assert isinstance(result[0], np.ndarray)
	assert isinstance(result[1], np.ndarray)
	assert result[0].dtype == np.float64
	assert result[1].dtype == np.int64


def test_mode_sample_empty_input(stats_instance: StatisticalDescription) -> None:
	"""Test mode_sample with empty input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Input array cannot be empty"):
		stats_instance.mode_sample(np.array([], dtype=np.float64))


def test_mode_sample_non_finite_input(stats_instance: StatisticalDescription) -> None:
	"""Test mode_sample with non-finite input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	array_with_nan = np.array([1.0, np.nan, 3.0], dtype=np.float64)
	array_with_inf = np.array([1.0, np.inf, 3.0], dtype=np.float64)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.mode_sample(array_with_nan)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.mode_sample(array_with_inf)


# --------------------------
# Tests for covariance
# --------------------------
def test_covariance_valid_input(
	stats_instance: StatisticalDescription,
	sample_arrays_pair: tuple[NDArray[np.float64], NDArray[np.float64]],
) -> None:
	"""Test covariance with valid input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.
	sample_arrays_pair : tuple[NDArray[np.float64], NDArray[np.float64]]
		Pair of sample arrays of real numbers.

	Returns
	-------
	None
	"""
	array_1, array_2 = sample_arrays_pair
	result = stats_instance.covariance(array_1, array_2)
	expected = np.cov(array_1, array_2, ddof=1)[0, 1]
	assert result == pytest.approx(expected)
	assert isinstance(result, float)


def test_covariance_empty_input(stats_instance: StatisticalDescription) -> None:
	"""Test covariance with empty input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	empty_array = np.array([], dtype=np.float64)
	with pytest.raises(ValueError, match="Input arrays cannot be empty"):
		stats_instance.covariance(empty_array, empty_array)


def test_covariance_different_lengths(
	stats_instance: StatisticalDescription, sample_array: NDArray[np.float64]
) -> None:
	"""Test covariance with arrays of different lengths.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.
	sample_array : NDArray[np.float64]
		Sample array of real numbers.

	Returns
	-------
	None
	"""
	array_short = np.array([1.0, 2.0], dtype=np.float64)
	with pytest.raises(ValueError, match="Input arrays must have the same length"):
		stats_instance.covariance(sample_array, array_short)


def test_covariance_non_finite_input(stats_instance: StatisticalDescription) -> None:
	"""Test covariance with non-finite input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	array_1 = np.array([1.0, 2.0, 3.0], dtype=np.float64)
	array_with_nan = np.array([1.0, np.nan, 3.0], dtype=np.float64)
	array_with_inf = np.array([1.0, np.inf, 3.0], dtype=np.float64)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.covariance(array_1, array_with_nan)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.covariance(array_1, array_with_inf)


# --------------------------
# Tests for correlation
# --------------------------
def test_correlation_valid_input(
	stats_instance: StatisticalDescription,
	sample_arrays_pair: tuple[NDArray[np.float64], NDArray[np.float64]],
) -> None:
	"""Test correlation with valid input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.
	sample_arrays_pair : tuple[NDArray[np.float64], NDArray[np.float64]]
		Pair of sample arrays of real numbers.

	Returns
	-------
	None
	"""
	array_1, array_2 = sample_arrays_pair
	result = stats_instance.correlation(array_1, array_2)
	assert result == pytest.approx(1.0)
	assert isinstance(result, float)
	assert -1 <= result <= 1


def test_correlation_empty_input(stats_instance: StatisticalDescription) -> None:
	"""Test correlation with empty input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	empty_array = np.array([], dtype=np.float64)
	with pytest.raises(ValueError, match="Input arrays cannot be empty"):
		stats_instance.correlation(empty_array, empty_array)


def test_correlation_different_lengths(
	stats_instance: StatisticalDescription, sample_array: NDArray[np.float64]
) -> None:
	"""Test correlation with arrays of different lengths.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.
	sample_array : NDArray[np.float64]
		Sample array of real numbers.

	Returns
	-------
	None
	"""
	array_short = np.array([1.0, 2.0], dtype=np.float64)
	with pytest.raises(ValueError, match="Input arrays must have the same length"):
		stats_instance.correlation(sample_array, array_short)


def test_correlation_non_finite_input(stats_instance: StatisticalDescription) -> None:
	"""Test correlation with non-finite input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	array_1 = np.array([1.0, 2.0, 3.0], dtype=np.float64)
	array_with_nan = np.array([1.0, np.nan, 3.0], dtype=np.float64)
	array_with_inf = np.array([1.0, np.inf, 3.0], dtype=np.float64)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.correlation(array_1, array_with_nan)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.correlation(array_1, array_with_inf)


def test_correlation_zero_variance(stats_instance: StatisticalDescription) -> None:
	"""Test correlation with zero-variance input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	array_constant = np.array([1.0, 1.0, 1.0], dtype=np.float64)
	array_valid = np.array([1.0, 2.0, 3.0], dtype=np.float64)
	with pytest.raises(ValueError, match="Input arrays must have non-zero variance"):
		stats_instance.correlation(array_constant, array_valid)


# --------------------------
# Tests for mean_squared_error_fitting_precision
# --------------------------
def test_mean_squared_error_fitting_precision_valid_input(
	stats_instance: StatisticalDescription,
	observations_predictions: tuple[NDArray[np.float64], NDArray[np.float64]],
) -> None:
	"""Test mean_squared_error_fitting_precision with valid input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.
	observations_predictions : tuple[NDArray[np.float64], NDArray[np.float64]]
		Pair of arrays of observed and predicted values.

	Returns
	-------
	None
	"""
	obs, pred = observations_predictions
	result = stats_instance.mean_squared_error_fitting_precision(obs, pred)
	expected_mse = np.mean((obs - pred) ** 2)
	assert result["mean_squared_error"] == pytest.approx(expected_mse)
	assert result["sqrt_mean_squared_error"] == pytest.approx(math.sqrt(expected_mse))
	assert result["mean_absolute_squared_error"] is None
	assert result["sqrt_mean_absolute_squared_error"] is None
	# Verify dictionary structure
	expected_keys = {
		"mean_squared_error",
		"sqrt_mean_squared_error",
		"mean_absolute_squared_error",
		"sqrt_mean_absolute_squared_error",
	}
	assert set(result.keys()) == expected_keys
	assert all(isinstance(result[key], (float, type(None))) for key in expected_keys)


def test_mean_squared_error_fitting_precision_empty_input(
	stats_instance: StatisticalDescription,
) -> None:
	"""Test mean_squared_error_fitting_precision with empty input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	empty_array = np.array([], dtype=np.float64)
	with pytest.raises(ValueError, match="Input arrays cannot be empty"):
		stats_instance.mean_squared_error_fitting_precision(empty_array, empty_array)


def test_mean_squared_error_fitting_precision_different_lengths(
	stats_instance: StatisticalDescription, sample_array: NDArray[np.float64]
) -> None:
	"""Test mean_squared_error_fitting_precision with arrays of different lengths.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.
	sample_array : NDArray[np.float64]
		Sample array of real numbers.

	Returns
	-------
	None
	"""
	array_short = np.array([1.0, 2.0], dtype=np.float64)
	with pytest.raises(ValueError, match="Input arrays must have the same length"):
		stats_instance.mean_squared_error_fitting_precision(sample_array, array_short)


def test_mean_squared_error_fitting_precision_non_finite_input(
	stats_instance: StatisticalDescription,
) -> None:
	"""Test mean_squared_error_fitting_precision with non-finite input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	array_1 = np.array([1.0, 2.0, 3.0], dtype=np.float64)
	array_with_nan = np.array([1.0, np.nan, 3.0], dtype=np.float64)
	array_with_inf = np.array([1.0, np.inf, 3.0], dtype=np.float64)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.mean_squared_error_fitting_precision(array_1, array_with_nan)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.mean_squared_error_fitting_precision(array_1, array_with_inf)


# --------------------------
# Tests for mean_absolute_error_fitting_precision
# --------------------------
def test_mean_absolute_error_fitting_precision_valid_input(
	stats_instance: StatisticalDescription,
	observations_predictions: tuple[NDArray[np.float64], NDArray[np.float64]],
) -> None:
	"""Test mean_absolute_error_fitting_precision with valid input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.
	observations_predictions : tuple[NDArray[np.float64], NDArray[np.float64]]
		Pair of arrays of observed and predicted values.

	Returns
	-------
	None
	"""
	obs, pred = observations_predictions
	result = stats_instance.mean_absolute_error_fitting_precision(obs, pred)
	expected_mae = np.mean(np.abs(obs - pred))
	assert result["mean_absolute_squared_error"] == pytest.approx(expected_mae)
	assert result["sqrt_mean_absolute_squared_error"] == pytest.approx(math.sqrt(expected_mae))
	assert result["mean_squared_error"] is None
	assert result["sqrt_mean_squared_error"] is None
	# Verify dictionary structure
	expected_keys = {
		"mean_squared_error",
		"sqrt_mean_squared_error",
		"mean_absolute_squared_error",
		"sqrt_mean_absolute_squared_error",
	}
	assert set(result.keys()) == expected_keys
	assert all(isinstance(result[key], (float, type(None))) for key in expected_keys)


def test_mean_absolute_error_fitting_precision_empty_input(
	stats_instance: StatisticalDescription,
) -> None:
	"""Test mean_absolute_error_fitting_precision with empty input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	empty_array = np.array([], dtype=np.float64)
	with pytest.raises(ValueError, match="Input arrays cannot be empty"):
		stats_instance.mean_absolute_error_fitting_precision(empty_array, empty_array)


def test_mean_absolute_error_fitting_precision_different_lengths(
	stats_instance: StatisticalDescription, sample_array: NDArray[np.float64]
) -> None:
	"""Test mean_absolute_error_fitting_precision with arrays of different lengths.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.
	sample_array : NDArray[np.float64]
		Sample array of real numbers.

	Returns
	-------
	None
	"""
	array_short = np.array([1.0, 2.0], dtype=np.float64)
	with pytest.raises(ValueError, match="Input arrays must have the same length"):
		stats_instance.mean_absolute_error_fitting_precision(sample_array, array_short)


def test_mean_absolute_error_fitting_precision_non_finite_input(
	stats_instance: StatisticalDescription,
) -> None:
	"""Test mean_absolute_error_fitting_precision with non-finite input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	array_1 = np.array([1.0, 2.0, 3.0], dtype=np.float64)
	array_with_nan = np.array([1.0, np.nan, 3.0], dtype=np.float64)
	array_with_inf = np.array([1.0, np.inf, 3.0], dtype=np.float64)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.mean_absolute_error_fitting_precision(array_1, array_with_nan)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.mean_absolute_error_fitting_precision(array_1, array_with_inf)


# --------------------------
# Tests for quantile
# --------------------------
def test_quantile_valid_input(
	stats_instance: StatisticalDescription, sample_array: NDArray[np.float64]
) -> None:
	"""Test quantile with valid input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.
	sample_array : NDArray[np.float64]
		Sample array of real numbers.

	Returns
	-------
	None
	"""
	result = stats_instance.quantile(sample_array, q=0.5, axis=0)
	assert np.allclose(result, np.array([3.0]))
	assert isinstance(result, np.ndarray)
	assert result.dtype == np.float64


def test_quantile_matrix_input(stats_instance: StatisticalDescription) -> None:
	"""Test quantile with matrix input and axis=1.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	matrix = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], dtype=np.float64)
	result = stats_instance.quantile(matrix, q=0.5, axis=1)
	assert np.allclose(result, np.array([2.0, 5.0]))
	assert isinstance(result, np.ndarray)
	assert result.dtype == np.float64


def test_quantile_empty_input(stats_instance: StatisticalDescription) -> None:
	"""Test quantile with empty input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Input array cannot be empty"):
		stats_instance.quantile(np.array([], dtype=np.float64), q=0.5)


def test_quantile_non_finite_input(stats_instance: StatisticalDescription) -> None:
	"""Test quantile with non-finite input.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.

	Returns
	-------
	None
	"""
	array_with_nan = np.array([1.0, np.nan, 3.0], dtype=np.float64)
	array_with_inf = np.array([1.0, np.inf, 3.0], dtype=np.float64)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.quantile(array_with_nan, q=0.5)
	with pytest.raises(ValueError, match="Input array.* contain.* NaN or infinite values"):
		stats_instance.quantile(array_with_inf, q=0.5)


def test_quantile_invalid_q(
	stats_instance: StatisticalDescription, sample_array: NDArray[np.float64]
) -> None:
	"""Test quantile with invalid quantile value.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.
	sample_array : NDArray[np.float64]
		Sample array of real numbers.

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Quantile q must be between 0 and 1, got -0.1"):
		stats_instance.quantile(sample_array, q=-0.1)
	with pytest.raises(ValueError, match="Quantile q must be between 0 and 1, got 1.1"):
		stats_instance.quantile(sample_array, q=1.1)


# --------------------------
# Test for module reload
# --------------------------
def test_module_reload(
	stats_instance: StatisticalDescription, sample_array: NDArray[np.float64]
) -> None:
	"""Test module reloading preserves StatisticalDescription functionality.

	Parameters
	----------
	stats_instance : StatisticalDescription
		An instance of the StatisticalDescription class.
	sample_array : NDArray[np.float64]
		Sample array of real numbers.

	Returns
	-------
	None
	"""
	importlib.reload(sys.modules["stpstone.analytics.quant.statistical_description"])
	new_instance = StatisticalDescription()
	result = new_instance.statistical_description(sample_array)
	assert result["mean"] == pytest.approx(3.0)
	expected_keys = {
		"nobs",
		"minmax",
		"mean",
		"median",
		"mode",
		"variance_sample",
		"standard_deviation_sample",
		"skewness",
		"kurtosis",
	}
	assert set(result.keys()) == expected_keys
	assert all(isinstance(result[key], (int, float, tuple, np.ndarray)) for key in expected_keys)
	assert isinstance(result["mode"], tuple)
	assert isinstance(result["mode"][0], np.ndarray)
	assert isinstance(result["mode"][1], np.ndarray)
