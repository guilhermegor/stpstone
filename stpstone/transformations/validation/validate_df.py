"""DataFrame validation utilities for pandas DataFrames.

This module provides a class for validating pandas DataFrames, including checks for missing values,
duplicates, range constraints, date order, and categorical values. It uses a custom logger for
reporting validation issues and integrates with a generic pipeline for processing.
"""

from logging import Logger
from numbers import Number
from typing import Optional, TypedDict

import pandas as pd

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.pipelines.generic import generic_pipeline


class ReturnValidateRanges(TypedDict):
	"""Return type for validate_ranges method.

	Parameters
	----------
	valid : bool
		Indicates if the validation passed without issues
	out_of_bounds : pd.DataFrame
		DataFrame containing rows with out-of-range values
	"""

	valid: bool
	out_of_bounds: pd.DataFrame


class ReturnValidateDates(TypedDict):
	"""Return type for validate_dates method.

	Parameters
	----------
	valid : bool
		Indicates if the validation passed without issues
	invalid_dates : pd.DataFrame
		DataFrame containing rows with invalid date order
	"""

	valid: bool
	invalid_dates: pd.DataFrame


class ReturnValidateCategoricalValues(TypedDict):
	"""Return type for validate_categorical_values method.

	Parameters
	----------
	valid : bool
		Indicates if the validation passed without issues
	invalid_values : pd.DataFrame
		DataFrame containing rows with invalid categorical values
	"""

	valid: bool
	invalid_values: pd.DataFrame


class ReturnPipeline(TypedDict):
	"""Return type for pipeline method.

	Parameters
	----------
	valid : bool
		Indicates if all validations passed without issues
	df_ : pd.DataFrame
		Processed DataFrame
	"""

	valid: bool
	df_: pd.DataFrame


class DataFrameValidator(metaclass=TypeChecker):
	"""Validate pandas DataFrames for common data quality issues.

	Parameters
	----------
	df_ : pd.DataFrame
		Input DataFrame to validate
	logger : Logger
		Logger instance for logging validation warnings

	Attributes
	----------
	df_ : pd.DataFrame
		Copy of the input DataFrame
	logger : Logger
		Logger instance for logging validation warnings
	create_log : CreateLog
		Instance for logging utilities
	"""

	def __init__(self, df_: pd.DataFrame, logger: Logger) -> None:
		"""Initialize DataFrameValidator with DataFrame and logger.

		Parameters
		----------
		df_ : pd.DataFrame
			Input DataFrame to validate
		logger : Logger
			Logger instance for logging validation warnings

		Returns
		-------
		None
		"""
		self.df_ = df_
		self.logger = logger
		self.create_log = CreateLog()

	def check_missing_values(self, df_: Optional[pd.DataFrame] = None) -> pd.DataFrame:
		"""Check for missing values in the DataFrame.

		Parameters
		----------
		df_ : Optional[pd.DataFrame]
			DataFrame to check for missing values. If None, uses self.df_

		Returns
		-------
		pd.DataFrame
			The input DataFrame (unchanged)
		"""
		if df_ is None:
			df_ = self.df_

		missing_summary = df_.isna().sum()
		missing_cols = missing_summary[missing_summary > 0]
		if not missing_cols.empty:
			self.create_log.log_message(self.logger, f"Missing Values:\n{missing_cols}", "warning")
		return df_

	def check_duplicates(self, df_: Optional[pd.DataFrame] = None) -> pd.DataFrame:
		"""Check for duplicate rows in the DataFrame.

		Parameters
		----------
		df_ : Optional[pd.DataFrame]
			DataFrame to check for duplicates. If None, uses self.df_

		Returns
		-------
		pd.DataFrame
			The input DataFrame (unchanged)
		"""
		if df_ is None:
			df_ = self.df_

		duplicate_count = df_.duplicated().sum()
		if duplicate_count > 0:
			self.create_log.log_message(
				self.logger, f"Found {duplicate_count} duplicate rows.", "warning"
			)
		return df_

	def validate_ranges(
		self, dict_rng_constraints: dict[str, tuple[Number, Number]]
	) -> ReturnValidateRanges:
		"""Validate numerical columns against specified ranges.

		Parameters
		----------
		dict_rng_constraints : dict[str, tuple[Number, Number]]
			Dictionary mapping column names to (low, high) range tuples

		Returns
		-------
		ReturnValidateRanges
			Dictionary with validation status and out-of-range rows

		Raises
		------
		ValueError
			If dict_rng_constraints is empty or invalid
			If column names are not in DataFrame
			If range bounds are invalid
		"""
		self._validate_rng_constraints(dict_rng_constraints)
		result: ReturnValidateRanges = {"valid": True, "out_of_bounds": pd.DataFrame()}
		for col, (low, high) in dict_rng_constraints.items():
			if low > high:
				raise ValueError(
					f"Lower bound {low} exceeds upper bound {high} " + f"for column '{col}'"
				)
			out_of_bounds = self.df_[(self.df_[col] < low) | (self.df_[col] > high)]
			if not out_of_bounds.empty:
				result["valid"] = False
				result["out_of_bounds"] = pd.concat([result["out_of_bounds"], out_of_bounds])
				self.create_log.log_message(
					self.logger,
					f"{len(out_of_bounds)} values in '{col}' out of range ({low} - {high})",
					"warning",
				)
		return result

	def _validate_rng_constraints(
		self, dict_rng_constraints: dict[str, tuple[Number, Number]]
	) -> None:
		"""Validate range constraints dictionary.

		Parameters
		----------
		dict_rng_constraints : dict[str, tuple[Number, Number]]
			Dictionary mapping column names to (low, high) range tuples

		Raises
		------
		ValueError
			If dict_rng_constraints is empty
			If column names are not in DataFrame
			If bounds are not numeric
		"""
		if not dict_rng_constraints:
			raise ValueError("Range constraints dictionary cannot be empty")
		for col, (low, high) in dict_rng_constraints.items():
			if col not in self.df_.columns:
				raise ValueError(f"Column '{col}' not found in DataFrame")
			if not (isinstance(low, Number) and isinstance(high, Number)):
				raise ValueError(f"Bounds for column '{col}' must be numeric")

	def validate_dates(self, col_start: str, col_sup: str) -> ReturnValidateDates:
		"""Validate date order between two columns.

		Parameters
		----------
		col_start : str
			Start date column name
		col_sup : str
			End date column name

		Returns
		-------
		ReturnValidateDates
			Dictionary with validation status and rows with invalid date order
		"""
		self._validate_date_columns(col_start, col_sup)
		invalid_dates = self.df_[self.df_[col_start] > self.df_[col_sup]]
		result: ReturnValidateDates = {
			"valid": invalid_dates.empty,
			"invalid_dates": invalid_dates,
		}
		if not invalid_dates.empty:
			self.create_log.log_message(
				self.logger,
				f"Found {len(invalid_dates)} rows where {col_start} is after {col_sup}.",
				"warning",
			)
		return result

	def _validate_date_columns(self, col_start: str, col_sup: str) -> None:
		"""Validate date column inputs.

		Parameters
		----------
		col_start : str
			Start date column name
		col_sup : str
			End date column name

		Raises
		------
		ValueError
			If column names are empty or not in DataFrame
			If columns do not contain datetime data
		"""
		if not col_start or not col_sup:
			raise ValueError("Date column names cannot be empty")
		if col_start not in self.df_.columns or col_sup not in self.df_.columns:
			raise ValueError("Date columns not found in DataFrame")
		if not (
			pd.api.types.is_datetime64_any_dtype(self.df_[col_start])
			and pd.api.types.is_datetime64_any_dtype(self.df_[col_sup])
		):
			raise ValueError("Date columns must contain datetime data")

	def validate_categorical_values(
		self, col: str, allowed_values: list[str]
	) -> ReturnValidateCategoricalValues:
		"""Validate categorical column against allowed values.

		Parameters
		----------
		col : str
			Column name to validate
		allowed_values : list[str]
			list of allowed categorical values

		Returns
		-------
		ReturnValidateCategoricalValues
			Dictionary with validation status and rows with invalid values
		"""
		self._validate_categorical_inputs(col, allowed_values)
		invalid_values = self.df_[~self.df_[col].isin(allowed_values)]
		result: ReturnValidateCategoricalValues = {
			"valid": invalid_values.empty,
			"invalid_values": invalid_values,
		}
		if not invalid_values.empty:
			unique_invalid = invalid_values[col].unique()
			self.create_log.log_message(
				self.logger,
				f"Found invalid values in '{col}': {unique_invalid.tolist()}",
				"warning",
			)
		return result

	def _validate_categorical_inputs(self, col: str, allowed_values: list[str]) -> None:
		"""Validate categorical column inputs.

		Parameters
		----------
		col : str
			Column name to validate
		allowed_values : list[str]
			list of allowed categorical values

		Raises
		------
		ValueError
			If column name is empty or not in DataFrame
			If allowed_values is empty
		"""
		if not col:
			raise ValueError("Column name cannot be empty")
		if col not in self.df_.columns:
			raise ValueError(f"Column '{col}' not found in DataFrame")
		if not allowed_values:
			raise ValueError("Allowed values list cannot be empty")

	def pipeline(
		self,
		dict_rng_constraints: Optional[dict[str, tuple[Number, Number]]] = None,
		col_start: Optional[str] = None,
		col_sup: Optional[str] = None,
		list_tup_categorical_constraints: Optional[tuple[str, list[str]]] = None,
	) -> ReturnPipeline:
		"""Execute validation pipeline on the DataFrame.

		Parameters
		----------
		dict_rng_constraints : Optional[dict[str, tuple[Number, Number]]]
			Dictionary mapping column names to (low, high) range tuples
		col_start : Optional[str]
			Start date column name
		col_sup : Optional[str]
			End date column name
		list_tup_categorical_constraints : Optional[tuple[str, list[str]]]
			tuple of column name and allowed categorical values

		Returns
		-------
		ReturnPipeline
			Dictionary with validation status and processed DataFrame
		"""
		valid = True
		steps = [
			self.check_missing_values,
			self.check_duplicates,
		]

		processed_df = generic_pipeline(self.df_, steps)

		missing_summary = self.df_.isna().sum()
		missing_cols = missing_summary[missing_summary > 0]
		if not missing_cols.empty:
			valid = False

		duplicate_count = self.df_.duplicated().sum()
		if duplicate_count > 0:
			valid = False

		if dict_rng_constraints:
			range_result = self.validate_ranges(dict_rng_constraints)
			if not range_result["valid"]:
				valid = False

		if col_start and col_sup:
			date_result = self.validate_dates(col_start, col_sup)
			if not date_result["valid"]:
				valid = False

		if list_tup_categorical_constraints:
			col, allowed_values = list_tup_categorical_constraints
			cat_result = self.validate_categorical_values(col, allowed_values)
			if not cat_result["valid"]:
				valid = False

		return {"valid": valid, "df_": processed_df}
