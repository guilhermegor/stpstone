"""Module for statistical analysis of numerical data, providing descriptive statistics."""

import math
from typing import Literal, Optional, TypedDict

import numpy as np
from numpy.typing import NDArray
from scipy import stats
from sklearn.metrics import mean_absolute_error, mean_squared_error


class ReturnStatisticalDescription(TypedDict):
    """Type definition for statistical_description return value."""

    nobs: int
    minmax: tuple[float, float]
    mean: float
    median: float
    mode: tuple[NDArray[np.float64], NDArray[np.int64]]
    variance_sample: float
    standard_deviation_sample: float
    skewness: float
    kurtosis: float


class ReturnErrorMetrics(TypedDict):
    """Type definition for error metric methods return value."""
    
    mean_squared_error: Optional[float]
    sqrt_mean_squared_error: Optional[float]
    mean_absolute_squared_error: Optional[float]
    sqrt_mean_absolute_squared_error: Optional[float]


class StatisticalDescription:
    """Class for computing statistical metrics and error measures on numerical data arrays."""

    def statistical_description(
        self, 
        array_data: NDArray[np.float64]
    ) -> ReturnStatisticalDescription:
        """Compute descriptive statistics for a numerical array.

        Parameters
        ----------
        array_data : NDArray[np.float64]
            Input array of real numbers

        Returns
        -------
        ReturnStatisticalDescription
            Dictionary with number of observations, min/max, mean, median, mode, variance,
            standard deviation, skewness, and kurtosis

        Raises
        ------
        ValueError
            If array_data is empty or contains non-finite values

        Notes
        -----
        Uses scipy.stats.describe for most metrics, with additional calculations for median
        and mode. Handles single-element arrays explicitly.
        """
        if len(array_data) == 0:
            raise ValueError("Input array cannot be empty")
        if not np.all(np.isfinite(array_data)):
            raise ValueError("Input array contains NaN or infinite values")

        array_data = np.array(array_data, dtype=np.float64)
        if len(array_data) == 1:
            return {
                "nobs": 1,
                "minmax": (float(array_data[0]), float(array_data[0])),
                "mean": float(array_data[0]),
                "median": float(array_data[0]),
                "mode": (np.array([array_data[0]]), np.array([1], dtype=np.int64)),
                "variance_sample": 0.0,
                "standard_deviation_sample": 0.0,
                "skewness": 0.0,
                "kurtosis": 0.0
            }
        
        description = stats.describe(array_data)
        mode_result = stats.mode(array_data, axis=None)
        return {
            "nobs": description.nobs,
            "minmax": description.minmax,
            "mean": description.mean,
            "median": np.median(array_data),
            "mode": (np.array(mode_result[0]), np.array(mode_result[1], dtype=np.int64)),
            "variance_sample": description.variance,
            "standard_deviation_sample": math.sqrt(description.variance),
            "skewness": description.skewness,
            "kurtosis": description.kurtosis
        }

    def standard_deviation_sample(self, array_data: NDArray[np.float64]) -> float:
        """Compute the sample standard deviation.

        Parameters
        ----------
        array_data : NDArray[np.float64]
            Input array of real numbers

        Returns
        -------
        float
            Sample standard deviation

        Raises
        ------
        ValueError
            If array_data is empty or contains non-finite values

        Notes
        -----
        Uses scipy.stats.tstd for calculation.
        """
        if len(array_data) == 0:
            raise ValueError("Input array cannot be empty")
        if not np.all(np.isfinite(array_data)):
            raise ValueError("Input array contains NaN or infinite values")

        return float(stats.tstd(np.array(array_data, dtype=np.float64)))

    def harmonic_mean(self, array_data: NDArray[np.float64]) -> float:
        """Compute the harmonic mean of a sample.

        Parameters
        ----------
        array_data : NDArray[np.float64]
            Input array of real numbers (must be positive)

        Returns
        -------
        float
            Harmonic mean

        Raises
        ------
        ValueError
            If array_data is empty, contains non-finite or non-positive values

        Notes
        -----
        Computes harmonic mean as n / sum(1/x) for positive values.
        """
        if len(array_data) == 0:
            raise ValueError("Input array cannot be empty")
        if not np.all(np.isfinite(array_data)):
            raise ValueError("Input array contains NaN or infinite values")
        if not np.all(array_data > 0):
            raise ValueError("Input array must contain positive values")

        array_data = np.array(array_data, dtype=np.float64)
        return float(len(array_data) / np.sum(1.0 / array_data))

    def median_sample(self, array_data: NDArray[np.float64]) -> float:
        """Compute the median of a sample.

        Parameters
        ----------
        array_data : NDArray[np.float64]
            Input array of real numbers

        Returns
        -------
        float
            Median value

        Raises
        ------
        ValueError
            If array_data is empty or contains non-finite values

        Notes
        -----
        Uses numpy.median for calculation.
        """
        if len(array_data) == 0:
            raise ValueError("Input array cannot be empty")
        if not np.all(np.isfinite(array_data)):
            raise ValueError("Input array contains NaN or infinite values")

        return float(np.median(np.array(array_data, dtype=np.float64)))

    def mode_sample(
        self,
        array_data: NDArray[np.float64]
    ) -> tuple[NDArray[np.float64], NDArray[np.int64]]:
        """Compute the mode of a sample.

        Parameters
        ----------
        array_data : NDArray[np.float64]
            Input array of real numbers

        Returns
        -------
        tuple[NDArray[np.float64], NDArray[np.int64]]
            Tuple of mode value(s) and their count(s)

        Raises
        ------
        ValueError
            If array_data is empty or contains non-finite values

        Notes
        -----
        Uses scipy.stats.mode with axis=None.
        """
        if len(array_data) == 0:
            raise ValueError("Input array cannot be empty")
        if not np.all(np.isfinite(array_data)):
            raise ValueError("Input array contains NaN or infinite values")

        array_data = np.array(array_data, dtype=np.float64)
        mode_result = stats.mode(array_data, axis=None)
        return (
            np.array(mode_result[0], dtype=np.float64),
            np.array(mode_result[1], dtype=np.int64)
        )

    def covariance(
        self,
        array_data_1: NDArray[np.float64],
        array_data_2: NDArray[np.float64]
    ) -> float:
        """Compute the covariance between two arrays.

        Parameters
        ----------
        array_data_1 : NDArray[np.float64]
            First input array of real numbers
        array_data_2 : NDArray[np.float64]
            Second input array of real numbers (same length as array_data_1)

        Returns
        -------
        float
            Covariance value

        Raises
        ------
        ValueError
            If arrays are empty, have different lengths, or contain non-finite values

        References
        ----------
        .. [1] https://stackoverflow.com/questions/48105922/numpy-covariance-between-each-column-of-a-matrix-and-a-array
        """
        array_data_1 = np.array(array_data_1, dtype=np.float64)
        array_data_2 = np.array(array_data_2, dtype=np.float64)
        if len(array_data_1) == 0 or len(array_data_2) == 0:
            raise ValueError("Input arrays cannot be empty")
        if len(array_data_1) != len(array_data_2):
            raise ValueError("Input arrays must have the same length")
        if not np.all(np.isfinite(array_data_1)) or not np.all(np.isfinite(array_data_2)):
            raise ValueError("Input arrays contain NaN or infinite values")

        return float(np.dot(array_data_2.T - array_data_2.mean(),
                            array_data_1 - array_data_1.mean()) /
                     (array_data_2.shape[0] - 1))

    def correlation(
        self,
        array_data_1: NDArray[np.float64],
        array_data_2: NDArray[np.float64]
    ) -> float:
        """Compute the Pearson correlation coefficient between two arrays.

        Parameters
        ----------
        array_data_1 : NDArray[np.float64]
            First input array of real numbers
        array_data_2 : NDArray[np.float64]
            Second input array of real numbers (same length as array_data_1)

        Returns
        -------
        float
            Pearson correlation coefficient (between -1 and 1)

        Raises
        ------
        ValueError
            If arrays are empty, have different lengths, contain non-finite values,
            or have zero variance

        References
        ----------
        .. [1] https://stackoverflow.com/questions/48105922/numpy-covariance-between-each-column-of-a-matrix-and-a-array
        """
        array_data_1 = np.array(array_data_1, dtype=np.float64)
        array_data_2 = np.array(array_data_2, dtype=np.float64)
        if len(array_data_1) == 0 or len(array_data_2) == 0:
            raise ValueError("Input arrays cannot be empty")
        if len(array_data_1) != len(array_data_2):
            raise ValueError("Input arrays must have the same length")
        if not np.all(np.isfinite(array_data_1)) or not np.all(np.isfinite(array_data_2)):
            raise ValueError("Input arrays contain NaN or infinite values")
        var_1 = np.var(array_data_1, ddof=1)
        var_2 = np.var(array_data_2, ddof=1)
        if var_1 == 0 or var_2 == 0:
            raise ValueError("Input arrays must have non-zero variance")

        corr = self.covariance(array_data_1, array_data_2) / math.sqrt(var_1 * var_2)
        if not -1 <= corr <= 1:
            raise ValueError(f"Correlation must be between -1 and 1, got {corr}")
        return corr

    def mean_squared_error_fitting_precision(
        self,
        array_observations: NDArray[np.float64],
        array_predictions: NDArray[np.float64]
    ) -> ReturnErrorMetrics:
        """Compute mean squared error to assess fitting precision.

        Parameters
        ----------
        array_observations : NDArray[np.float64]
            Array of observed values
        array_predictions : NDArray[np.float64]
            Array of predicted values (same length as array_observations)

        Returns
        -------
        ReturnErrorMetrics
            Dictionary with mean squared error and its square root

        Raises
        ------
        ValueError
            If arrays are empty, have different lengths, or contain non-finite values

        Notes
        -----
        Uses sklearn.metrics.mean_squared_error for calculation.
        """
        array_observations = np.array(array_observations, dtype=np.float64)
        array_predictions = np.array(array_predictions, dtype=np.float64)
        if len(array_observations) == 0 or len(array_predictions) == 0:
            raise ValueError("Input arrays cannot be empty")
        if len(array_observations) != len(array_predictions):
            raise ValueError("Input arrays must have the same length")
        if not np.all(np.isfinite(array_observations)) \
                or not np.all(np.isfinite(array_predictions)):
            raise ValueError("Input arrays contain NaN or infinite values")

        mse = mean_squared_error(array_observations, array_predictions)
        return {
            "mean_squared_error": mse,
            "sqrt_mean_squared_error": math.sqrt(mse),
            "mean_absolute_squared_error": None,
            "sqrt_mean_absolute_squared_error": None
        }

    def mean_absolute_error_fitting_precision(
        self,
        array_observations: NDArray[np.float64],
        array_predictions: NDArray[np.float64]
    ) -> ReturnErrorMetrics:
        """Compute mean absolute error to assess fitting precision.

        Parameters
        ----------
        array_observations : NDArray[np.float64]
            Array of observed values
        array_predictions : NDArray[np.float64]
            Array of predicted values (same length as array_observations)

        Returns
        -------
        ReturnErrorMetrics
            Dictionary with mean absolute error and its square root

        Raises
        ------
        ValueError
            If arrays are empty, have different lengths, or contain non-finite values

        Notes
        -----
        Uses sklearn.metrics.mean_absolute_error for calculation.
        """
        array_observations = np.array(array_observations, dtype=np.float64)
        array_predictions = np.array(array_predictions, dtype=np.float64)
        if len(array_observations) == 0 or len(array_predictions) == 0:
            raise ValueError("Input arrays cannot be empty")
        if len(array_observations) != len(array_predictions):
            raise ValueError("Input arrays must have the same length")
        if not np.all(np.isfinite(array_observations)) \
                or not np.all(np.isfinite(array_predictions)):
            raise ValueError("Input arrays contain NaN or infinite values")

        mae = mean_absolute_error(array_observations, array_predictions)
        return {
            "mean_squared_error": None,
            "sqrt_mean_squared_error": None,
            "mean_absolute_squared_error": mae,
            "sqrt_mean_absolute_squared_error": math.sqrt(mae)
        }

    def quantile(
        self,
        array_data: NDArray[np.float64],
        q: float,
        axis: Literal[0, 1] = 0
    ) -> NDArray[np.float64]:
        """Compute the quantile of an array along a specified axis.

        Parameters
        ----------
        array_data : NDArray[np.float64]
            Input array of real numbers
        q : float
            Quantile to compute (between 0 and 1)
        axis : Literal[0, 1]
            Axis along which to compute quantile (0 for flattened, 1 for matrix; default: 0)

        Returns
        -------
        NDArray[np.float64]
            Quantile value(s)

        Raises
        ------
        ValueError
            If array_data is empty, contains non-finite values, or q is not between 0 and 1

        Notes
        -----
        Uses numpy.quantile for calculation.
        """
        if len(array_data) == 0:
            raise ValueError("Input array cannot be empty")
        if not np.all(np.isfinite(array_data)):
            raise ValueError("Input array contains NaN or infinite values")
        if not 0 <= q <= 1:
            raise ValueError(f"Quantile q must be between 0 and 1, got {q}")

        result = np.quantile(np.array(array_data, dtype=np.float64), q, axis=axis)
        return np.atleast_1d(result)