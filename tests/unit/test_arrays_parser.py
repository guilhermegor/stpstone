"""Unit tests for the Arrays parser utility."""

from unittest import TestCase, main

import numpy as np
import pandas as pd

from stpstone.utils.parsers.arrays import Arrays


class TestArrays(TestCase):
	"""Tests for Arrays conversion helpers."""

	def setUp(self) -> None:
		"""Initialise shared fixtures used across test methods."""
		self.arrays = Arrays()
		self.df_ = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
		self.series = pd.Series([1, 2, 3])
		self.np_array = np.array([1, 2, 3])
		self.list_data = [1.0, 2.0, 3.0]

	def test_to_array_matrice_with_dataframe(self) -> None:
		"""Test to_array_matrice converts a DataFrame to a 2-D ndarray."""
		result = self.arrays.to_array_matrice(self.df_)
		self.assertIsInstance(result, np.ndarray)
		np.testing.assert_array_equal(result, self.df_.to_numpy())

	def test_to_array_matrice_with_ndarray(self) -> None:
		"""Test to_array_matrice returns an ndarray unchanged."""
		result = self.arrays.to_array_matrice(self.np_array)
		self.assertIsInstance(result, np.ndarray)
		np.testing.assert_array_equal(result, self.np_array)

	def test_to_array_vector_with_series(self) -> None:
		"""Test to_array_vector converts a Series to a 1-D ndarray."""
		result = self.arrays.to_array_vector(self.series)
		self.assertIsInstance(result, np.ndarray)
		np.testing.assert_array_equal(result, self.series.to_numpy())

	def test_to_array_vector_with_list(self) -> None:
		"""Test to_array_vector converts a list to a 1-D ndarray."""
		result = self.arrays.to_array_vector(self.list_data)
		self.assertIsInstance(result, np.ndarray)
		np.testing.assert_array_equal(result, np.array(self.list_data))

	def test_to_array_vector_with_ndarray(self) -> None:
		"""Test to_array_vector returns an ndarray unchanged."""
		result = self.arrays.to_array_vector(self.np_array)
		self.assertIsInstance(result, np.ndarray)
		np.testing.assert_array_equal(result, self.np_array)

	def test_to_array_with_dataframe(self) -> None:
		"""Test to_array converts a DataFrame to an ndarray."""
		result = self.arrays.to_array(self.df_)
		self.assertIsInstance(result, np.ndarray)
		np.testing.assert_array_equal(result, self.df_.to_numpy())

	def test_to_array_with_series(self) -> None:
		"""Test to_array converts a Series to an ndarray."""
		result = self.arrays.to_array(self.series)
		self.assertIsInstance(result, np.ndarray)
		np.testing.assert_array_equal(result, self.series.to_numpy())

	def test_to_array_with_list(self) -> None:
		"""Test to_array converts a list to an ndarray."""
		result = self.arrays.to_array(self.list_data)
		self.assertIsInstance(result, np.ndarray)
		np.testing.assert_array_equal(result, np.array(self.list_data))

	def test_to_array_with_ndarray(self) -> None:
		"""Test to_array returns an ndarray unchanged."""
		result = self.arrays.to_array(self.np_array)
		self.assertIsInstance(result, np.ndarray)
		np.testing.assert_array_equal(result, self.np_array)

	def test_to_array_with_unsupported_type(self) -> None:
		"""Test to_array raises TypeError for unsupported input types."""
		with self.assertRaises(TypeError):
			self.arrays.to_array({"a": 1, "b": 2})


if __name__ == "__main__":
	main()
