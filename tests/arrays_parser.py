import numpy as np
import pandas as pd
from unittest import TestCase, main
from stpstone.utils.parsers.arrays import Arrays


class TestArrays(TestCase):
    def setUp(self):
        self.arrays = Arrays()
        self.df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        self.series = pd.Series([1, 2, 3])
        self.np_array = np.array([1, 2, 3])
        self.list_data = [1.0, 2.0, 3.0]

    def test_to_array_matrice_with_dataframe(self):
        result = self.arrays.to_array_matrice(self.df)
        self.assertIsInstance(result, np.ndarray)
        np.testing.assert_array_equal(result, self.df.to_numpy())

    def test_to_array_matrice_with_ndarray(self):
        result = self.arrays.to_array_matrice(self.np_array)
        self.assertIsInstance(result, np.ndarray)
        np.testing.assert_array_equal(result, self.np_array)

    def test_to_array_vector_with_series(self):
        result = self.arrays.to_array_vector(self.series)
        self.assertIsInstance(result, np.ndarray)
        np.testing.assert_array_equal(result, self.series.to_numpy())

    def test_to_array_vector_with_list(self):
        result = self.arrays.to_array_vector(self.list_data)
        self.assertIsInstance(result, np.ndarray)
        np.testing.assert_array_equal(result, np.array(self.list_data))

    def test_to_array_vector_with_ndarray(self):
        result = self.arrays.to_array_vector(self.np_array)
        self.assertIsInstance(result, np.ndarray)
        np.testing.assert_array_equal(result, self.np_array)

    def test_to_array_with_dataframe(self):
        result = self.arrays.to_array(self.df)
        self.assertIsInstance(result, np.ndarray)
        np.testing.assert_array_equal(result, self.df.to_numpy())

    def test_to_array_with_series(self):
        result = self.arrays.to_array(self.series)
        self.assertIsInstance(result, np.ndarray)
        np.testing.assert_array_equal(result, self.series.to_numpy())

    def test_to_array_with_list(self):
        result = self.arrays.to_array(self.list_data)
        self.assertIsInstance(result, np.ndarray)
        np.testing.assert_array_equal(result, np.array(self.list_data))

    def test_to_array_with_ndarray(self):
        result = self.arrays.to_array(self.np_array)
        self.assertIsInstance(result, np.ndarray)
        np.testing.assert_array_equal(result, self.np_array)

    def test_to_array_with_unsupported_type(self):
        with self.assertRaises(ValueError):
            self.arrays.to_array({"a": 1, "b": 2})

if __name__ == "__main__":
    main()