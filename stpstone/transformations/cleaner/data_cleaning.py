"""Data cleaning utilities for machine learning pipelines.

This module provides functions for preparing datasets for machine learning, including:
- Train/test splitting (random, stratified, and hash-based)
- Missing value imputation
- Categorical encoding
- Feature scaling
- Data augmentation
- Noise removal

References
----------
.. [1] Géron, A. (2019). Hands-On Machine Learning with Scikit-Learn, Keras, and TensorFlow.
		O'Reilly Media, Inc. ISBN 978-1-492-03264-9.
"""

from typing import Literal, Optional, TypedDict
from zlib import crc32

import numpy as np
from numpy.typing import NDArray
import pandas as pd
from scipy.ndimage import shift
from sklearn.base import BaseEstimator
from sklearn.compose import ColumnTransformer
from sklearn.impute import KNNImputer, SimpleImputer
from sklearn.model_selection import StratifiedShuffleSplit, train_test_split
from sklearn.preprocessing import (
	LabelEncoder,
	MinMaxScaler,
	OneHotEncoder,
	OrdinalEncoder,
	StandardScaler,
)

from stpstone.transformations.cleaner.eda import ExploratoryDataAnalysis
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class ReturnReplaceNaNValues(TypedDict):
	"""Return type for replace_nan_values function."""

	model: BaseEstimator
	array_replacers: Optional[NDArray[np.float64]]
	array_before_adjustments: NDArray[np.float64]
	array_after_adjustments: NDArray[np.float64]


class ReturnConvertCartegories(TypedDict):
	"""Return type for convert_categories function."""

	array_data_categorized_numbers: NDArray[np.float64]
	array_data_categorized_strings: NDArray[np.str_]


class ReturnFeatureScaling(TypedDict):
	"""Return type for feature_scaling function."""

	model: NDArray[np.float64]
	scale: NDArray[np.float64]
	n_samples_seen: int
	array_original_data: NDArray[np.float64]
	array_scaled_data: NDArray[np.float64]


class ReturnRemoveNoiseFromData(TypedDict):
	"""Return type for remove_noise_from_data function."""

	data_test_original: NDArray[np.float64]
	data_test_enhanced: NDArray[np.float64]
	data_training_original: NDArray[np.float64]
	data_training_enhanced: NDArray[np.float64]


class DataCleaning(metaclass=TypeChecker):
	"""Class for cleaning and preparing datasets for machine learning."""

	def test_set_check_hash(self, identifier: int, test_ratio: float) -> bool:
		"""Generate stable test set using hash of identifier.

		Parameters
		----------
		identifier : int
			Unique identifier for the sample
		test_ratio : float
			Proportion of data to include in test set (0-1)

		Returns
		-------
		bool
			True if sample should be in test set

		References
		----------
		.. [1] Géron, A. (2019). Hands-On Machine Learning with Scikit-Learn, Keras, and \
			TensorFlow.
				O'Reilly Media, Inc. ISBN 978-1-492-03264-9.
		"""
		self._validate_test_ratio(test_ratio)
		return crc32(np.int64(identifier)) & 0xFFFFFFFF < test_ratio * 2**32

	def split_train_test(
		self,
		df_data: pd.DataFrame,
		test_ratio: float = 0.2,
		random_seed: int = 42,
		stratify_col: Optional[str] = None,
	) -> tuple[pd.DataFrame, pd.DataFrame]:
		"""Split data into train and test sets randomly.

		Parameters
		----------
		df_data : pd.DataFrame
			Input dataframe to split
		test_ratio : float
			Proportion of data for test set (default: 0.2)
		random_seed : int
			Random seed for reproducibility (default: 42)
		stratify_col : Optional[str]
			Column to use for stratified splitting

		Returns
		-------
		tuple[pd.DataFrame, pd.DataFrame]
			Train and test dataframes

		References
		----------
		.. [1] Géron, A. (2019). Hands-On Machine Learning with Scikit-Learn, Keras, and \
			TensorFlow.
				O'Reilly Media, Inc. ISBN 978-1-492-03264-9.
		"""
		self._validate_test_ratio(test_ratio)
		stratify = df_data[stratify_col] if stratify_col else None
		return train_test_split(
			df_data,
			test_size=test_ratio,
			random_state=random_seed,
			stratify=stratify,
		)

	def split_stratified_train_test(
		self,
		df_data: pd.DataFrame,
		col_name: str,
		n_splits: int = 1,
		test_size: float = 0.2,
		random_state_seed: int = 42,
	) -> tuple[pd.DataFrame, pd.DataFrame]:
		"""Split data using stratified sampling.

		Parameters
		----------
		df_data : pd.DataFrame
			Input dataframe
		col_name : str
			Column to stratify by
		n_splits : int
			Number of splits (default: 1)
		test_size : float
			Proportion for test set (default: 0.2)
		random_state_seed : int
			Random seed (default: 42)

		Returns
		-------
		tuple[pd.DataFrame, pd.DataFrame]
			Stratified train and test sets

		References
		----------
		.. [1] Géron, A. (2019). Hands-On Machine Learning with Scikit-Learn, Keras, and \
			TensorFlow.
				O'Reilly Media, Inc. ISBN 978-1-492-03264-9.
		"""
		self._validate_test_ratio(test_size)
		split_class = StratifiedShuffleSplit(
			n_splits=n_splits,
			test_size=test_size,
			random_state=random_state_seed,
		)
		for train_index, test_index in split_class.split(df_data, df_data[col_name]):
			df_strat_train_set = df_data.loc[train_index]
			df_strat_test_set = df_data.loc[test_index]
		return df_strat_train_set, df_strat_test_set

	def split_train_test_by_id(
		self,
		df_data: pd.DataFrame,
		test_ratio: float,
		id_column: str,
	) -> tuple[pd.DataFrame, pd.DataFrame]:
		"""Split data using identifier column for stable test sets.

		Parameters
		----------
		df_data : pd.DataFrame
			Input dataframe
		test_ratio : float
			Proportion for test set
		id_column : str
			Column containing unique identifiers

		Returns
		-------
		tuple[pd.DataFrame, pd.DataFrame]
			Train and test dataframes

		References
		----------
		.. [1] Géron, A. (2019). Hands-On Machine Learning with Scikit-Learn, Keras, and \
			TensorFlow.
				O'Reilly Media, Inc. ISBN 978-1-492-03264-9.
		"""
		self._validate_test_ratio(test_ratio)
		ids = df_data[id_column]
		in_test_set = ids.apply(lambda id_: self.test_set_check_hash(id_, test_ratio))
		return df_data.loc[~in_test_set], df_data.loc[in_test_set]

	def create_category_stratified_train_test_set(
		self,
		df_data: pd.DataFrame,
		id_column_original: str,
		id_column_category: str,
		list_bins: list,
		list_labels: list,
	) -> pd.DataFrame:
		"""Create stratified categories for sampling.

		Parameters
		----------
		df_data : pd.DataFrame
			Input dataframe
		id_column_original : str
			Original column to bin
		id_column_category : str
			New column name for categories
		list_bins : list
			Bin edges for categorization
		list_labels : list
			Labels for bins

		Returns
		-------
		pd.DataFrame
			Dataframe with new category column
		"""
		df_data[id_column_category] = pd.cut(
			df_data[id_column_original],
			bins=list_bins,
			labels=list_labels,
		)
		return df_data

	def remove_category_stratified_train_test_set(
		self,
		df_train_set: pd.DataFrame,
		df_test_set: pd.DataFrame,
		id_column_category: str,
	) -> tuple[pd.DataFrame, pd.DataFrame]:
		"""Remove temporary category column after stratification.

		Parameters
		----------
		df_train_set : pd.DataFrame
			Training dataframe
		df_test_set : pd.DataFrame
			Test dataframe
		id_column_category : str
			Column to remove

		Returns
		-------
		tuple[pd.DataFrame, pd.DataFrame]
			Dataframes with column removed
		"""
		df_train_set = df_train_set.drop(id_column_category, axis=1)
		df_test_set = df_test_set.drop(id_column_category, axis=1)
		return df_train_set, df_test_set

	def dataframe_id_column_prportions(
		self,
		df_data: pd.DataFrame,
		id_column: str,
	) -> pd.Series:
		"""Calculate proportions of unique values in column.

		Parameters
		----------
		df_data : pd.DataFrame
			Input dataframe
		id_column : str
			Column to analyze

		Returns
		-------
		pd.Series
			Proportions of each unique value

		References
		----------
		.. [1] Géron, A. (2019). Hands-On Machine Learning with Scikit-Learn, Keras, and \
			TensorFlow.
				O'Reilly Media, Inc. ISBN 978-1-492-03264-9.
		"""
		return df_data[id_column].value_counts() / len(df_data)

	def compare_stratified_random_samples_propotions(
		self,
		df_data_original: pd.DataFrame,
		df_data_random_set: pd.DataFrame,
		df_data_stratified_set: pd.DataFrame,
		id_column: str,
	) -> pd.DataFrame:
		"""Compare sampling method proportions against original data.

		Parameters
		----------
		df_data_original : pd.DataFrame
			Original dataframe
		df_data_random_set : pd.DataFrame
			Randomly sampled dataframe
		df_data_stratified_set : pd.DataFrame
			Stratified sampled dataframe
		id_column : str
			Column to compare

		Returns
		-------
		pd.DataFrame
			Dataframe with comparison metrics

		References
		----------
		.. [1] Géron, A. (2019). Hands-On Machine Learning with Scikit-Learn, Keras, and \
			TensorFlow.
				O'Reilly Media, Inc. ISBN 978-1-492-03264-9.
		"""
		df_compare_props = pd.DataFrame(
			{
				"Overall": self.dataframe_id_column_prportions(df_data_original, id_column),
				"Stratified": self.dataframe_id_column_prportions(
					df_data_stratified_set,
					id_column,
				),
				"Random": self.dataframe_id_column_prportions(df_data_random_set, id_column),
			}
		).sort_index()
		df_compare_props["Rand. %error"] = (
			100 * df_compare_props["Random"] / df_compare_props["Overall"] - 100
		)
		df_compare_props["Strat. %error"] = (
			100 * df_compare_props["Stratified"] / df_compare_props["Overall"] - 100
		)
		return df_compare_props

	def replace_nan_values(
		self,
		array_data: NDArray[np.float64],
		strategy: Optional[Literal["mean", "median", "most_frequent"]] = None,
		missing_values: float = np.nan,
		n_neighbors: Optional[int] = None,
	) -> ReturnReplaceNaNValues:
		"""Impute missing values using specified model.

		Parameters
		----------
		array_data : NDArray[np.float64]
			Input array with missing values
		strategy : Optional[Literal['mean', 'median', 'most_frequent']]
			Imputation model (None for KNN)
		missing_values : float
			Value representing missing data
		n_neighbors : Optional[int]
			Number of neighbors for KNN imputation

		Returns
		-------
		ReturnReplaceNaNValues
			Dictionary with imputation results and statistics
		"""
		array_data_copy = array_data.copy()

		if n_neighbors is None:
			model = SimpleImputer(strategy=strategy, missing_values=missing_values)
		else:
			model = KNNImputer(n_neighbors=n_neighbors, missing_values=np.nan)
		model.fit(array_data_copy)
		array_data_copy = model.transform(array_data_copy)

		dict_ = {
			"model": model,
			"array_before_adjustments": array_data,
			"array_after_adjustments": array_data_copy,
		}
		if isinstance(model, SimpleImputer):
			dict_["array_replacers"] = model.statistics_
		else:
			dict_["array_replacers"] = None

		return dict_

	def convert_categories_from_strings_to_array(
		self,
		array_data: NDArray[np.float64],
		list_idx_target_cols: list,
		encoder_strategy: Literal[
			"one_hot_encoder", "ordinal_encoder", "label_encoder"
		] = "one_hot_encoder",
	) -> ReturnConvertCartegories:
		"""Convert categorical strings to numerical arrays.

		Parameters
		----------
		array_data : NDArray[np.float64]
			Input array with categorical data
		list_idx_target_cols : list
			Column indices to encode
		encoder_strategy :  Literal['one_hot_encoder', 'ordinal_encoder', 'label_encoder']
			Encoding model to use

		Returns
		-------
		ReturnConvertCartegories
			Dictionary with encoded arrays and original data

		Raises
		------
		ValueError
			If encoder_strategy is not one of: 'one_hot_encoder', 'ordinal_encoder', \
				'label_encoder'
		"""
		if encoder_strategy == "one_hot_encoder":
			ct = ColumnTransformer(
				transformers=[("encoders", OneHotEncoder(), list_idx_target_cols)],
				remainder="passthrough",
			)
		elif encoder_strategy == "ordinal_encoder":
			ct = ColumnTransformer(
				transformers=[("encoders", OrdinalEncoder(), list_idx_target_cols)],
				remainder="passthrough",
			)
		elif encoder_strategy == "label_encoder":
			ct = LabelEncoder()
		else:
			raise ValueError(
				"encoder_strategy must be one of: 'one_hot_encoder', 'ordinal_encoder', "
				+ "'label_encoder'"
			)
		array_categoric_numbers = np.array(ct.fit_transform(array_data))
		return {
			"array_data_categorized_numbers": array_categoric_numbers,
			"array_data_categorized_strings": array_data,
		}

	def feature_scaling(
		self,
		array_data: NDArray[np.float64],
		type_scaler: Literal["normalization", "standardisation"] = "normalization",
		tup_feature_range: tuple[float, float] = (0, 1),
	) -> ReturnFeatureScaling:
		"""Scale features using specified method.

		Parameters
		----------
		array_data : NDArray[np.float64]
			Input array to scale
		type_scaler : Literal['normalization', 'standardisation']
			Scaling method
		tup_feature_range : tuple[float, float]
			Range for normalization

		Returns
		-------
		ReturnFeatureScaling
			Dictionary with scaling results and statistics

		Raises
		------
		ValueError
			If type_scaler is not one of: 'normalization', 'standardisation'

		References
		----------
		.. [1] Géron, A. (2019). Hands-On Machine Learning with Scikit-Learn, Keras, and \
			TensorFlow.
				O'Reilly Media, Inc. ISBN 978-1-492-03264-9.
		.. [2] https://stackoverflow.com/questions/40758562/can-anyone-explain-me-standardscaler
		"""
		array_data = ExploratoryDataAnalysis().reshape_1d_arrays(array_data)
		if type_scaler == "normalization":
			model = MinMaxScaler(feature_range=tup_feature_range)
		elif type_scaler == "standardisation":
			model = StandardScaler()
		else:
			raise ValueError("type_scaler must be 'normalization' or 'standardisation'")
		model.fit(array_data)
		array_data_transformed = model.transform(array_data)
		return {
			"model": model,
			"scale": model.scale_,
			"n_samples_seen": model.n_samples_seen_,
			"array_original_data": array_data,
			"array_scaled_data": array_data_transformed,
		}

	def remove_noise_from_data(
		self,
		data_test: NDArray[np.float64],
		data_train: NDArray[np.float64],
	) -> ReturnRemoveNoiseFromData:
		"""Add and remove noise for data augmentation.

		Parameters
		----------
		data_test : NDArray[np.float64]
			Test data
		data_train : NDArray[np.float64]
			Training data

		Returns
		-------
		ReturnRemoveNoiseFromData
			Dictionary with original and enhanced data

		References
		----------
		.. [1] https://colab.research.google.com/github/ageron/handson-ml2/blob/master/03_classification.ipynb#scrollTo=utQpplj4fGwa
		"""
		noise = np.random.randint(0, 100, (len(data_train), 784))
		data_train_original = data_train + noise
		noise = np.random.randint(0, 100, (len(data_test), 784))
		data_test_original = data_test + noise
		return {
			"data_test_original": data_test_original,
			"data_test_enhanced": data_test,
			"data_training_original": data_train_original,
			"data_training_enhanced": data_train,
		}

	def shift_image(
		self,
		image: NDArray[np.float64],
		dx: int,
		dy: int,
	) -> NDArray[np.float64]:
		"""Shift image for data augmentation.

		Parameters
		----------
		image : NDArray[np.float64]
			Input image (28x28)
		dx : int
			Horizontal shift
		dy : int
			Vertical shift

		Returns
		-------
		NDArray[np.float64]
			Shifted image

		References
		----------
		.. [1] https://colab.research.google.com/github/ageron/handson-ml2/blob/master/03_classification.ipynb#scrollTo=vkIfY1tAfGwf
		"""
		image = image.reshape((28, 28))
		shifted_image = shift(image, [dy, dx], cval=0, mode="constant")
		return shifted_image.reshape([-1])

	def _validate_test_ratio(self, test_ratio: float) -> None:
		"""Validate that test ratio is between 0 and 1.

		Parameters
		----------
		test_ratio : float
			Ratio to validate

		Raises
		------
		ValueError
			If ratio is outside [0, 1] range
		"""
		if not 0 <= test_ratio <= 1:
			raise ValueError(f"test_ratio must be between 0 and 1, got {test_ratio}")
