"""Unit tests for DFStandardizationML class.

Tests ML-specific DataFrame transformations including:
- Outlier handling (IQR and z-score)
- Numeric scaling (minmax and standard)
- Full ML pipeline
"""

from datetime import datetime
from typing import Any
import zoneinfo

import numpy as np
import pandas as pd
import pytest
from pytest_mock import MockerFixture

from stpstone.transformations.standardization.standardizer_df_ml import DFStandardizationML


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_df() -> pd.DataFrame:
	"""Fixture providing a sample DataFrame for testing.

	Returns
	-------
	pd.DataFrame
		DataFrame with mixed data types and some missing values
	"""
	return pd.DataFrame(
		{
			"id": [1, 2, 3, None],
			"name": ["Alice", "Bob", "", "Charlie"],
			"value": ["1.234,56", "2.345,67", "3.456,78", "4.567,89"],
			"date": ["2023-01-01", "2023-01-02", None, "2023-01-03"],
			"numeric": [10.5, 20.3, np.nan, 30.1],
		}
	)


@pytest.fixture
def valid_dict_dtypes() -> dict[str, Any]:
	"""Fixture providing valid dictionary of data types.

	Returns
	-------
	dict[str, Any]
		Dictionary mapping column names to their desired data types
	"""
	return {
		"id": "int64",
		"name": "str",
		"value": "float64",
		"date": "date",
		"numeric": "float64",
	}


@pytest.fixture
def ml_standardization_instance(valid_dict_dtypes: dict[str, Any]) -> DFStandardizationML:
	"""Fixture providing a DFStandardizationML instance.

	Parameters
	----------
	valid_dict_dtypes : dict[str, Any]
		Dictionary of valid data types from fixture

	Returns
	-------
	DFStandardizationML
		Initialized DFStandardizationML instance
	"""
	return DFStandardizationML(
		dict_dtypes=valid_dict_dtypes,
		str_fmt_dt="YYYY-MM-DD",
		str_dt_fillna="2099-12-31",
		str_data_fillna="-99999",
	)


# --------------------------
# Tests for DFStandardizationML
# --------------------------
def test_handle_outliers_iqr(
	ml_standardization_instance: DFStandardizationML,
	sample_df: pd.DataFrame,
) -> None:
	"""Test handle_outliers method with IQR method.

	Verifies
	--------
	- Outliers are clipped to IQR bounds
	- Non-numeric columns are unchanged

	Parameters
	----------
	ml_standardization_instance : DFStandardizationML
		The DFStandardizationML instance to test
	sample_df : pd.DataFrame
		The DataFrame to test

	Returns
	-------
	None
	"""
	df_ = sample_df.copy()
	df_["numeric"] = [10.5, 20.3, 100.0, 30.1]  # introduce outlier
	df_["numeric"] = df_["numeric"].fillna(df_["numeric"].mean())
	result = ml_standardization_instance.handle_outliers(df_, method="iqr")
	q1, q3 = df_["numeric"].quantile([0.25, 0.75])
	iqr = q3 - q1
	upper_bound = q3 + 1.5 * iqr
	assert result["numeric"].iloc[2] == pytest.approx(upper_bound, rel=1e-3)
	assert pd.api.types.is_string_dtype(result["name"])


def test_handle_outliers_zscore(
	ml_standardization_instance: DFStandardizationML,
	sample_df: pd.DataFrame,
) -> None:
	"""Test handle_outliers method with z-score method.

	Verifies
	--------
	- Values within ±3σ are not clipped

	Parameters
	----------
	ml_standardization_instance : DFStandardizationML
		The DFStandardizationML instance to test
	sample_df : pd.DataFrame
		The DataFrame to test

	Returns
	-------
	None
	"""
	df_ = sample_df.copy()
	df_["numeric"] = [10.5, 20.3, 100.0, 30.1]  # introduce outlier
	df_["numeric"] = df_["numeric"].fillna(df_["numeric"].mean())
	result = ml_standardization_instance.handle_outliers(df_, method="zscore")
	# 100.0 is within mean ± 3σ for this small dataset, so it should not be clipped
	assert result["numeric"].iloc[2] == pytest.approx(100.0, rel=1e-3)
	assert pd.api.types.is_string_dtype(result["name"])


def test_scale_numeric_data_minmax(
	ml_standardization_instance: DFStandardizationML,
	sample_df: pd.DataFrame,
) -> None:
	"""Test scale_numeric_data method with minmax scaling.

	Verifies
	--------
	- Output range is [0, 1]
	- No NaN values remain after scaling

	Parameters
	----------
	ml_standardization_instance : DFStandardizationML
		The DFStandardizationML instance to test
	sample_df : pd.DataFrame
		The DataFrame to test

	Returns
	-------
	None
	"""
	result = ml_standardization_instance.scale_numeric_data(sample_df, method="minmax")
	assert result["numeric"].min() >= 0
	assert result["numeric"].max() <= 1
	assert not result["numeric"].isna().any()
	assert pd.api.types.is_string_dtype(result["name"])


def test_scale_numeric_data_standard(
	ml_standardization_instance: DFStandardizationML,
	sample_df: pd.DataFrame,
) -> None:
	"""Test scale_numeric_data method with standard scaling.

	Verifies
	--------
	- Mean is approximately 0 after scaling
	- No NaN values remain after scaling

	Parameters
	----------
	ml_standardization_instance : DFStandardizationML
		The DFStandardizationML instance to test
	sample_df : pd.DataFrame
		The DataFrame to test

	Returns
	-------
	None
	"""
	df_ = sample_df.copy()
	df_["numeric"] = df_["numeric"].fillna(0)
	result = ml_standardization_instance.scale_numeric_data(df_, method="standard")
	assert pytest.approx(result["numeric"].mean(), abs=1e-6) == 0
	assert pytest.approx(result["numeric"].std(ddof=1), abs=1e-6) == 1.1547005383792517
	assert not result["numeric"].isna().any()
	assert pd.api.types.is_string_dtype(result["name"])


def test_scale_numeric_data_invalid_method(
	ml_standardization_instance: DFStandardizationML,
	sample_df: pd.DataFrame,
) -> None:
	"""Test scale_numeric_data with invalid scaling method.

	Verifies
	--------
	- TypeError is raised for an unsupported method name

	Parameters
	----------
	ml_standardization_instance : DFStandardizationML
		The DFStandardizationML instance to test
	sample_df : pd.DataFrame
		The DataFrame to test

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="method must be one of"):
		ml_standardization_instance.scale_numeric_data(sample_df, method="invalid")


def test_pipeline_ml(
	ml_standardization_instance: DFStandardizationML,
	sample_df: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test pipeline_ml method.

	Verifies
	--------
	- Full ML pipeline returns scaled numeric columns
	- Date columns are converted to datetime

	Parameters
	----------
	ml_standardization_instance : DFStandardizationML
		The DFStandardizationML instance to test
	sample_df : pd.DataFrame
		The DataFrame to test
	mocker : MockerFixture
		Pytest mocker fixture

	Returns
	-------
	None
	"""
	mocker.patch(
		"stpstone.utils.calendars.calendar_br.DatesBRAnbima.str_date_to_datetime",
		side_effect=lambda x, y: datetime.strptime(x, "%Y-%m-%d").replace(
			tzinfo=zoneinfo.ZoneInfo("UTC")
		)
		if pd.notna(x) and x != "2099-12-31"
		else datetime(2099, 12, 31, tzinfo=zoneinfo.ZoneInfo("UTC")),
	)
	df_ = sample_df.copy()
	df_["id"] = df_["id"].fillna(0)
	df_["numeric"] = df_["numeric"].fillna(0)
	df_["date"] = df_["date"].fillna("2099-12-31")
	df_ = ml_standardization_instance.replace_num_delimiters(df_)
	result = ml_standardization_instance.pipeline_ml(
		df_,
		method_handle_outliers="iqr",
		method_scale_numeric_data="minmax",
	)
	assert result["numeric"].min() >= 0
	assert result["numeric"].max() <= 1
	assert isinstance(result["date"].iloc[0], datetime)
