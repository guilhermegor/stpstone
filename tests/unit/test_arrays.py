"""Unit tests for Arrays conversion utilities.

Tests the functionality for converting various data structures to NumPy arrays,
including validation checks and type safety enforcement.
"""

import numpy as np
from numpy.typing import NDArray
import pandas as pd
import pytest

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.arrays import Arrays


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def arrays_instance() -> TypeChecker:
	"""Fixture providing an instance of the Arrays class.

	Returns
	-------
	TypeChecker
		Instance of Arrays class for testing
	"""
	return Arrays()


@pytest.fixture
def sample_2d_array() -> NDArray:
	"""Fixture providing a sample 2D numpy array.

	Returns
	-------
	NDArray
		2x3 numpy array with sample values
	"""
	return np.array([[1, 2, 3], [4, 5, 6]])


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
	"""Fixture providing a sample pandas DataFrame.

	Returns
	-------
	pd.DataFrame
		2x3 DataFrame with sample values
	"""
	return pd.DataFrame({"A": [1, 4], "B": [2, 5], "C": [3, 6]})


@pytest.fixture
def sample_1d_array() -> NDArray:
	"""Fixture providing a sample 1D numpy array.

	Returns
	-------
	NDArray
		1D numpy array with sample values
	"""
	return np.array([1, 2, 3, 4])


@pytest.fixture
def sample_series() -> pd.Series:
	"""Fixture providing a sample pandas Series.

	Returns
	-------
	pd.Series
		Series with sample values
	"""
	return pd.Series([1, 2, 3, 4])


@pytest.fixture
def sample_list() -> list[float]:
	"""Fixture providing a sample Python list.

	Returns
	-------
	list[float]
		List with sample values
	"""
	return [1.0, 2.0, 3.0, 4.0]


# --------------------------
# Tests
# --------------------------
class TestValidateArrayNotEmpty:
	"""Tests for _validate_array_not_empty method."""

	def test_valid_non_empty_array(
		self, arrays_instance: Arrays, sample_2d_array: NDArray
	) -> None:
		"""Test validation passes with non-empty numpy array.

		Verifies
		--------
		- Method doesn't raise for non-empty numpy array

		Parameters
		----------
		arrays_instance : Arrays
			Instance of the Arrays class
		sample_2d_array : NDArray
			Sample 2D numpy array to validate

		Returns
		-------
		None
		"""
		arrays_instance._validate_array_not_empty(sample_2d_array)

	def test_valid_non_empty_dataframe(
		self, arrays_instance: Arrays, sample_dataframe: pd.DataFrame
	) -> None:
		"""Test validation passes with non-empty DataFrame.

		Verifies
		--------
		- Method doesn't raise for non-empty DataFrame

		Parameters
		----------
		arrays_instance : Arrays
			Instance of the Arrays class
		sample_dataframe : pd.DataFrame
			Sample DataFrame to validate

		Returns
		-------
		None
		"""
		arrays_instance._validate_array_not_empty(sample_dataframe)

	def test_empty_array_raises(self, arrays_instance: Arrays) -> None:
		"""Test validation raises for empty numpy array.

		Verifies
		--------
		- Method raises ValueError for empty numpy array
		- Error message is correct

		Parameters
		----------
		arrays_instance : Arrays
			Instance of the Arrays class

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Input array cannot be empty"):
			arrays_instance._validate_array_not_empty(np.array([]))

	def test_empty_dataframe_raises(self, arrays_instance: Arrays) -> None:
		"""Test validation raises for empty DataFrame.

		Verifies
		--------
		- Method raises ValueError for empty DataFrame
		- Error message is correct

		Parameters
		----------
		arrays_instance : Arrays
			Instance of the Arrays class

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Input array cannot be empty"):
			arrays_instance._validate_array_not_empty(pd.DataFrame())


class TestToArrayMatrice:
	"""Tests for to_array_matrice method."""

	def test_convert_numpy_array(self, arrays_instance: Arrays, sample_2d_array: NDArray) -> None:
		"""Test numpy array input returns same array.

		Verifies
		--------
		- Returns same array when input is numpy array
		- Output type is NDArray

		Parameters
		----------
		arrays_instance : Arrays
			Instance of the Arrays class
		sample_2d_array : NDArray
			Sample 2D numpy array to convert

		Returns
		-------
		None
		"""
		result = arrays_instance.to_array_matrice(sample_2d_array)
		assert np.array_equal(result, sample_2d_array)
		assert isinstance(result, np.ndarray)

	def test_convert_dataframe(
		self, arrays_instance: Arrays, sample_dataframe: pd.DataFrame, sample_2d_array: NDArray
	) -> None:
		"""Test DataFrame input converts to numpy array.

		Verifies
		--------
		- Returns correct numpy array from DataFrame
		- Output type is NDArray

		Parameters
		----------
		arrays_instance : Arrays
			Instance of the Arrays class
		sample_dataframe : pd.DataFrame
			Sample DataFrame to convert
		sample_2d_array : NDArray
			Sample 2D numpy array for comparison

		Returns
		-------
		None
		"""
		result = arrays_instance.to_array_matrice(sample_dataframe)
		assert np.array_equal(result, sample_2d_array)
		assert isinstance(result, np.ndarray)

	def test_invalid_type_raises(self, arrays_instance: Arrays) -> None:
		"""Test raises for unsupported input type.

		Verifies
		--------
		- Raises ValueError for unsupported types
		- Error message contains correct type info

		Parameters
		----------
		arrays_instance : Arrays
			Instance of the Arrays class

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be one of types"):
			arrays_instance.to_array_matrice("invalid_type")


class TestToArrayVector:
	"""Tests for to_array_vector method."""

	def test_convert_numpy_array(self, arrays_instance: Arrays, sample_1d_array: NDArray) -> None:
		"""Test numpy array input returns same array.

		Verifies
		--------
		- Returns same array when input is numpy array
		- Output type is NDArray

		Parameters
		----------
		arrays_instance : Arrays
			Instance of the Arrays class
		sample_1d_array : NDArray
			Sample 1D numpy array to convert

		Returns
		-------
		None
		"""
		result = arrays_instance.to_array_vector(sample_1d_array)
		assert np.array_equal(result, sample_1d_array)
		assert isinstance(result, np.ndarray)

	def test_convert_series(
		self, arrays_instance: Arrays, sample_series: pd.Series, sample_1d_array: NDArray
	) -> None:
		"""Test Series input converts to numpy array.

		Verifies
		--------
		- Returns correct numpy array from Series
		- Output type is NDArray

		Parameters
		----------
		arrays_instance : Arrays
			Instance of the Arrays class
		sample_series : pd.Series
			Sample Series to convert
		sample_1d_array : NDArray
			Sample 1D numpy array for comparison

		Returns
		-------
		None
		"""
		result = arrays_instance.to_array_vector(sample_series)
		assert np.array_equal(result, sample_1d_array)
		assert isinstance(result, np.ndarray)

	def test_convert_list(
		self, arrays_instance: Arrays, sample_list: list[float], sample_1d_array: NDArray
	) -> None:
		"""Test list input converts to numpy array.

		Verifies
		--------
		- Returns correct numpy array from list
		- Output type is NDArray

		Parameters
		----------
		arrays_instance : Arrays
			Instance of the Arrays class
		sample_list : list[float]
			Sample list to convert
		sample_1d_array : NDArray
			Sample 1D numpy array for comparison

		Returns
		-------
		None
		"""
		result = arrays_instance.to_array_vector(sample_list)
		assert np.array_equal(result, sample_1d_array[: len(sample_list)])
		assert isinstance(result, np.ndarray)

	def test_invalid_type_raises(self, arrays_instance: Arrays) -> None:
		"""Test raises for unsupported input type.

		Verifies
		--------
		- Raises ValueError for unsupported types
		- Error message contains correct type info

		Parameters
		----------
		arrays_instance : Arrays
			Instance of the Arrays class

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be one of types"):
			arrays_instance.to_array_vector("invalid_type")


class TestToArray:
	"""Tests for to_array method."""

	def test_convert_2d_array(self, arrays_instance: Arrays, sample_2d_array: NDArray) -> None:
		"""Test 2D numpy array input returns same array.

		Verifies
		--------
		- Returns same array when input is 2D numpy array
		- Output type is NDArray

		Parameters
		----------
		arrays_instance : Arrays
			Instance of the Arrays class
		sample_2d_array : NDArray
			Sample 2D numpy array to convert

		Returns
		-------
		None
		"""
		result = arrays_instance.to_array(sample_2d_array)
		assert np.array_equal(result, sample_2d_array)
		assert isinstance(result, np.ndarray)

	def test_convert_dataframe(
		self, arrays_instance: Arrays, sample_dataframe: pd.DataFrame, sample_2d_array: NDArray
	) -> None:
		"""Test DataFrame input converts to numpy array.

		Verifies
		--------
		- Returns correct numpy array from DataFrame
		- Output type is NDArray

		Parameters
		----------
		arrays_instance : Arrays
			Instance of the Arrays class
		sample_dataframe : pd.DataFrame
			Sample DataFrame to convert
		sample_2d_array : NDArray
			Sample 2D numpy array for comparison

		Returns
		-------
		None
		"""
		result = arrays_instance.to_array(sample_dataframe)
		assert np.array_equal(result, sample_2d_array)
		assert isinstance(result, np.ndarray)

	def test_convert_1d_array(self, arrays_instance: Arrays, sample_1d_array: NDArray) -> None:
		"""Test 1D numpy array input returns same array.

		Verifies
		--------
		- Returns same array when input is 1D numpy array
		- Output type is NDArray

		Parameters
		----------
		arrays_instance : Arrays
			Instance of the Arrays class
		sample_1d_array : NDArray
			Sample 1D numpy array to convert

		Returns
		-------
		None
		"""
		result = arrays_instance.to_array(sample_1d_array)
		assert np.array_equal(result, sample_1d_array)
		assert isinstance(result, np.ndarray)

	def test_convert_series(
		self, arrays_instance: Arrays, sample_series: pd.Series, sample_1d_array: NDArray
	) -> None:
		"""Test Series input converts to numpy array.

		Verifies
		--------
		- Returns correct numpy array from Series
		- Output type is NDArray

		Parameters
		----------
		arrays_instance : Arrays
			Instance of the Arrays class
		sample_series : pd.Series
			Sample Series to convert
		sample_1d_array : NDArray
			Sample 1D numpy array for comparison

		Returns
		-------
		None
		"""
		result = arrays_instance.to_array(sample_series)
		assert np.array_equal(result, sample_1d_array)
		assert isinstance(result, np.ndarray)

	def test_convert_list(
		self, arrays_instance: Arrays, sample_list: list[float], sample_1d_array: NDArray
	) -> None:
		"""Test list input converts to numpy array.

		Verifies
		--------
		- Returns correct numpy array from list
		- Output type is NDArray

		Parameters
		----------
		arrays_instance : Arrays
			Instance of the Arrays class
		sample_list : list[float]
			Sample list to convert
		sample_1d_array : NDArray
			Sample 1D numpy array for comparison

		Returns
		-------
		None
		"""
		result = arrays_instance.to_array(sample_list)
		assert np.array_equal(result, sample_1d_array[: len(sample_list)])
		assert isinstance(result, np.ndarray)

	def test_invalid_type_raises(self, arrays_instance: Arrays) -> None:
		"""Test raises for unsupported input type.

		Verifies
		--------
		- Raises ValueError for unsupported types
		- Error message contains correct type info

		Parameters
		----------
		arrays_instance : Arrays
			Instance of the Arrays class

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be one of types"):
			arrays_instance.to_array("invalid_type")
