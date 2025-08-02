"""Array conversion utilities for numerical data.

This module provides functions for converting various data structures to NumPy arrays,
with validation checks and type safety enforcement.
"""

from typing import Union

import numpy as np
from numpy.typing import NDArray
import pandas as pd

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class Arrays(metaclass=TypeChecker):
    """Conversion utilities for transforming data structures to NumPy arrays.

    Methods provide type-safe conversion from pandas DataFrames/Series and Python lists
    to NumPy arrays with validation checks.
    """

    def _validate_array_not_empty(
        self, 
        array_data: Union[NDArray, pd.DataFrame, pd.Series, list]
    ) -> None:
        """Validate that input array is not empty.

        Parameters
        ----------
        array_data : Union[NDArray, pd.DataFrame, pd.Series, list]
            Input data structure to validate

        Raises
        ------
        ValueError
            If input array is empty
        """
        if len(array_data) == 0:
            raise ValueError("Input array cannot be empty")

    def to_array_matrice(
        self, 
        array_data: Union[NDArray, pd.DataFrame]
    ) -> NDArray:
        """Convert matrix-like data (DataFrame or 2D array) to NumPy array.

        Parameters
        ----------
        array_data : Union[NDArray, pd.DataFrame]
            Input data to convert (2D array or DataFrame)

        Returns
        -------
        NDArray
            Converted NumPy array

        Raises
        ------
        ValueError
            If input is empty or conversion fails
        """
        self._validate_array_not_empty(array_data)
        
        if isinstance(array_data, np.ndarray):
            return array_data
        if isinstance(array_data, pd.DataFrame):
            return array_data.to_numpy()
        raise ValueError(f"Unsupported matrix type: {type(array_data)}")

    def to_array_vector(
        self, 
        array_data: Union[NDArray, pd.Series, list]
    ) -> NDArray:
        """Convert vector-like data (Series, list or 1D array) to NumPy array.

        Parameters
        ----------
        array_data : Union[NDArray, pd.Series, list]
            Input data to convert (1D array, Series or list)

        Returns
        -------
        NDArray
            Converted NumPy array

        Raises
        ------
        ValueError
            If input is empty or conversion fails
        """
        self._validate_array_not_empty(array_data)
        
        if isinstance(array_data, np.ndarray):
            return array_data
        if isinstance(array_data, list):
            return np.array(array_data)
        if isinstance(array_data, pd.Series):
            return array_data.to_numpy()
        raise ValueError(f"Unsupported vector type: {type(array_data)}")

    def to_array(
        self, 
        array_data: Union[NDArray, pd.DataFrame, pd.Series, list]
    ) -> NDArray:
        """Convert various data structures to NumPy array with automatic type detection.

        Parameters
        ----------
        array_data : Union[NDArray, pd.DataFrame, pd.Series, list]
            Input data to convert (array, DataFrame, Series or list)

        Returns
        -------
        NDArray
            Converted NumPy array

        Raises
        ------
        ValueError
            If input type is unsupported or empty
        """
        self._validate_array_not_empty(array_data)
        
        if isinstance(array_data, (np.ndarray, pd.DataFrame)):
            return self.to_array_matrice(array_data)
        if isinstance(array_data, (pd.Series, list)):
            return self.to_array_vector(array_data)
        raise ValueError(f"Unsupported data type: {type(array_data)}")