"""Exploratory Data Analysis (EDA) utilities.

This module provides tools for data exploration and preprocessing, including:
- Monotonicity checks
- Automated binning strategies
- Array reshaping
- Basic dataset exploration

References
----------
.. [1] https://github.com/pankajkalania/IV-WOE/blob/main/iv_woe_code.py
.. [2] https://gaurabdas.medium.com/weight-of-evidence-and-information-value-in-python-from-scratch-a8953d40e34
"""

from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
from numpy.typing import NDArray
import pandas as pd

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class ExploratoryDataAnalysis(metaclass=TypeChecker):
	"""Class containing exploratory data analysis utilities."""

	def is_monotonic(self, array_data: NDArray[np.float64]) -> bool:
		"""Check if array values are monotonically increasing or decreasing.

		A function between ordered sets that preserves or reverses the given order.

		Parameters
		----------
		array_data : NDArray[np.float64]
			1D array to check for monotonicity

		Returns
		-------
		bool
			True if array is monotonic (either increasing or decreasing), False otherwise
		"""
		self._validate_array_non_empty(array_data)
		increasing = all(array_data[i] <= array_data[i + 1] for i in range(len(array_data) - 1))
		decreasing = all(array_data[i] >= array_data[i + 1] for i in range(len(array_data) - 1))
		return increasing or decreasing

	def prepare_bins(
		self,
		df_: pd.DataFrame,
		c_i: str,
		target_col: str,
		max_bins: int,
		force_bin: bool = True,
		binned: bool = False,
		remarks: Optional[str] = None,
		name_bins: str = "_bins",
		remark_binned_monotonically: str = "binned monotonically",
		remark_binned_forcefully: str = "binned forcefully",
		remark_binned_error: str = "could not bin",
	) -> tuple[str, str, pd.DataFrame]:
		"""Create bins for a continuous variable with monotonic target rate.

		Binning method:
		1. Equi-spaced bins with at least 5% of total observations in each bin
		2. Maximum of 20 bins can be set to ensure 5% sample in each class
		3. Event rate for each bin will be monotonically increasing/decreasing
		4. Separate bins will be created for missing values

		Parameters
		----------
		df_ : pd.DataFrame
			Input dataframe containing the variable to bin
		c_i : str
			Column name to bin
		target_col : str
			Target column name
		max_bins : int
			Maximum number of bins to try
		force_bin : bool
			Whether to force binning if monotonicity fails, by default True
		binned : bool
			Whether binning was successful, by default False
		remarks : Optional[str]
			Remarks about binning process, by default None
		name_bins : str
			Suffix for binned column name, by default "_bins"
		remark_binned_monotonically : str
			Remark for successful monotonic binning, by default "binned monotonically"
		remark_binned_forcefully : str
			Remark for forced binning, by default "binned forcefully"
		remark_binned_error : str
			Remark for binning failure, by default "could not bin"

		Returns
		-------
		tuple[str, str, pd.DataFrame]
			tuple containing:
			- Column name (original or binned)
			- Remarks about binning process
			- DataFrame with relevant columns
		"""
		self._validate_dataframe_columns(df_, [c_i, target_col])
		self._validate_positive_integer(max_bins, "max_bins")

		# monotonic binning
		for n_bins in range(max_bins, 2, -1):
			df_[c_i + name_bins] = pd.qcut(df_[c_i], n_bins, duplicates="drop")
			array_data_monotonic = (
				df_.groupby(c_i + name_bins)[target_col].mean().reset_index(drop=True)
			)
			if self.is_monotonic(array_data_monotonic.values):
				force_bin = False
				binned = True
				remarks = remark_binned_monotonically

		# force binning - creating 2 bins forcefully
		if force_bin or (c_i + name_bins in df_ and df_[c_i + name_bins].nunique() < 2):
			_min = df_[c_i].min()
			_mean = df_[c_i].mean()
			_max = df_[c_i].max()
			df_[c_i + name_bins] = pd.cut(df_[c_i], [_min, _mean, _max], include_lowest=True)
			if df_[c_i + name_bins].nunique() == 2:
				binned = True
				remarks = remark_binned_forcefully

		# return binned data  # noqa: ERA001
		if binned:
			return c_i + name_bins, remarks, df_[[c_i, c_i + name_bins, target_col]].copy()
		remarks = remark_binned_error
		return c_i, remarks, df_[[c_i, target_col]].copy()

	def reshape_1d_arrays(self, array_data: NDArray[np.float64]) -> NDArray[np.float64]:
		"""Reshape a 1D array to 2D for feature scaling or linearity tests.

		Parameters
		----------
		array_data : NDArray[np.float64]
			Input array to reshape

		Returns
		-------
		NDArray[np.float64]
			Reshaped 2D array
		"""
		self._validate_array_non_empty(array_data)
		try:
			_ = array_data[:, 0]
			return array_data
		except IndexError:
			return np.reshape(array_data, (-1, 1))

	def eda_database(
		self,
		df_data: pd.DataFrame,
		bins: int = 58,
		figsize: tuple[int, int] = (20, 15),
		bool_show_plots: bool = True,
	) -> None:
		"""Perform basic exploratory data analysis on a dataframe.

		Includes:
		- Displaying dataframe head
		- Showing dataframe info
		- Statistical description
		- Histogram plots

		Parameters
		----------
		df_data : pd.DataFrame
			Dataframe to analyze
		bins : int
			Number of bins for histograms, by default 58
		figsize : tuple[int, int]
			Figure size for plots, by default (20, 15)
		bool_show_plots : bool
			Whether to show plots, by default True

		Returns
		-------
		None
		"""
		self._validate_dataframe_non_empty(df_data)
		self._validate_positive_integer(bins, "bins")

		print("*** HEAD DATAFRAME ***")
		print(df_data.head())
		print("*** INFOS DATAFRAME ***")
		print(df_data.info())
		print("*** DESCRIBE STATISTICAL & PROBABILITY INFOS - DATAFRAME ***")
		print(df_data.describe())

		if bool_show_plots:
			print("*** PLOTTING DATAFRAME ***")
			df_data.hist(bins=bins, figsize=figsize)
			plt.show()

	def _validate_array_non_empty(self, array_data: NDArray[np.float64]) -> None:
		"""Validate that an array is not empty.

		Parameters
		----------
		array_data : NDArray[np.float64]
			Input array to validate

		Returns
		-------
		None

		Raises
		------
		ValueError
			If array is empty
		"""
		if len(array_data) == 0:
			raise ValueError("Input array cannot be empty")

	def _validate_dataframe_non_empty(self, df_: pd.DataFrame) -> None:
		"""Validate that a dataframe is not empty.

		Parameters
		----------
		df_ : pd.DataFrame
			Input dataframe to validate

		Returns
		-------
		None

		Raises
		------
		ValueError
			If dataframe is empty
		"""
		if df_.empty:
			raise ValueError("Dataframe cannot be empty")

	def _validate_dataframe_columns(self, df_: pd.DataFrame, columns: list[str]) -> None:
		"""Validate that specified columns exist in dataframe.

		Parameters
		----------
		df_ : pd.DataFrame
			Input dataframe to validate
		columns : list[str]
			List of column names to validate

		Returns
		-------
		None

		Raises
		------
		ValueError
			If dataframe is missing required columns
		"""
		missing_cols = [col for col in columns if col not in df_.columns]
		if missing_cols:
			raise ValueError(f"Dataframe missing required columns: {missing_cols}")

	def _validate_positive_integer(self, value: int, name: str) -> None:
		"""Validate that a value is a positive integer.

		Parameters
		----------
		value : int
			Input value to validate
		name : str
			Name of the value being validated

		Returns
		-------
		None

		Raises
		------
		ValueError
			If value is not a positive integer
		"""
		if not isinstance(value, int) or value <= 0:
			raise ValueError(f"{name} must be a positive integer, got {value}")
