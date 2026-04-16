"""ML-specific DataFrame standardization.

Extends DFStandardization with machine-learning pipeline helpers:
outlier handling and numeric scaling.
"""

from typing import Any, Literal, Optional

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, StandardScaler

from stpstone.transformations.standardization.standardizer_df import DFStandardization
from stpstone.utils.parsers.str import TypeCaseFrom, TypeCaseTo


class DFStandardizationML(DFStandardization):
	"""Class for standardizing data for machine learning."""

	def __init__(self, **kwargs: dict[Any, Any]) -> None:
		"""Initialize the DFStandardizationML class.

		Parameters
		----------
		**kwargs : Any
			Keyword arguments to pass to the parent class.
		"""
		super().__init__(**kwargs)

	def handle_outliers(
		self, df_: pd.DataFrame, method: Literal["iqr", "zscore"] = "iqr"
	) -> pd.DataFrame:
		"""Handle outliers in the DataFrame.

		Parameters
		----------
		df_ : pd.DataFrame
			The DataFrame to handle outliers in.
		method : Literal['iqr', 'zscore']
			The method to use for handling outliers. Default is "iqr".

		Returns
		-------
		pd.DataFrame
			The DataFrame with outliers handled.
		"""
		df_ = df_.copy()
		numeric_cols = df_.select_dtypes(include=["number"]).columns
		if not numeric_cols.empty:
			df_[numeric_cols] = df_[numeric_cols].fillna(df_[numeric_cols].mean())
			for col_ in numeric_cols:
				if method == "iqr":
					q1, q3 = df_[col_].quantile([0.25, 0.75])
					iqr = q3 - q1
					lower_bound, upper_bound = q1 - 1.5 * iqr, q3 + 1.5 * iqr
					df_[col_] = np.clip(df_[col_], lower_bound, upper_bound)
				elif method == "zscore":
					mean, std = df_[col_].mean(), df_[col_].std(ddof=1)
					lower_bound, upper_bound = mean - 3 * std, mean + 3 * std
					df_[col_] = np.clip(df_[col_], lower_bound, upper_bound)
		return df_

	def scale_numeric_data(
		self, df_: pd.DataFrame, method: Literal["minmax", "standard"] = "minmax"
	) -> pd.DataFrame:
		"""Scale numeric data in the DataFrame.

		Parameters
		----------
		df_ : pd.DataFrame
			The DataFrame to scale numeric data in.
		method : Literal['minmax', 'standard']
			The method to use for scaling numeric data. Default is "minmax".

		Returns
		-------
		pd.DataFrame
			The DataFrame with numeric data scaled.

		Raises
		------
		ValueError
			If the specified scaling method is invalid.
		"""
		numeric_cols = df_.select_dtypes(include=["number"]).columns
		if len(numeric_cols) == 0:
			return df_
		df_ = df_.copy()
		df_[numeric_cols] = df_[numeric_cols].fillna(0)
		if method == "minmax":
			scaler = MinMaxScaler()
		elif method == "standard":
			scaler = StandardScaler()
		else:
			raise ValueError(f"Invalid scaling method: {method}")
		df_[numeric_cols] = scaler.fit_transform(df_[numeric_cols])
		return df_

	def pipeline_ml(
		self,
		df_: pd.DataFrame,
		method_handle_outliers: str = "iqr",
		method_scale_numeric_data: str = "minmax",
		cols_from_case: Optional[TypeCaseFrom] = None,
		cols_to_case: Optional[TypeCaseTo] = None,
		list_cols_drop_dupl: list[str] = None,
		str_fmt_dt: str = "YYYY-MM-DD",
	) -> pd.DataFrame:
		"""Pipeline for standardizing data for machine learning.

		Parameters
		----------
		df_ : pd.DataFrame
			The DataFrame to standardize.
		method_handle_outliers : str
			The method to use for handling outliers. Default is "iqr".
		method_scale_numeric_data : str
			The method to use for scaling numeric data. Default is "minmax".
		cols_from_case : Optional[TypeCaseFrom]
			Case conversion for column names, by default None.
		cols_to_case : Optional[TypeCaseTo]
			Case conversion for column names, by default None.
		list_cols_drop_dupl : list[str]
			List of columns to drop duplicates, by default None.
		str_fmt_dt : str
			Format for date columns, by default "YYYY-MM-DD".

		Returns
		-------
		pd.DataFrame
			The DataFrame with outliers handled and numeric data scaled.
		"""
		self.cols_from_case = cols_from_case
		self.cols_to_case = cols_to_case
		self.list_cols_drop_dupl = list_cols_drop_dupl or []
		self.str_fmt_dt = str_fmt_dt
		df_ = self.pipeline(df_)
		df_ = self.handle_outliers(df_, method_handle_outliers)
		df_ = self.scale_numeric_data(df_, method_scale_numeric_data)
		return df_
