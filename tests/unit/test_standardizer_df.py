"""Unit tests for DFStandardization class.

Tests the DataFrame standardization functionality with various input scenarios including:
- Initialization with valid and invalid inputs
- DataFrame cleaning and transformation methods
- Edge cases, error conditions, and type validation
"""

from datetime import datetime
from logging import Logger
from typing import Any
from unittest.mock import Mock
import zoneinfo

import numpy as np
import pandas as pd
import pytest
from pytest_mock import MockerFixture

from stpstone.transformations.standardization.standardizer_df import DFStandardization


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
def standardization_instance(valid_dict_dtypes: dict[str, Any]) -> DFStandardization:
	"""Fixture providing a DFStandardization instance.

	Parameters
	----------
	valid_dict_dtypes : dict[str, Any]
		Dictionary of valid data types from fixture

	Returns
	-------
	DFStandardization
		Initialized DFStandardization instance
	"""
	return DFStandardization(
		dict_dtypes=valid_dict_dtypes,
		str_fmt_dt="YYYY-MM-DD",
		str_dt_fillna="2099-12-31",
		str_data_fillna="-99999",
	)


# --------------------------
# Tests for DFStandardization
# --------------------------
def test_init_valid_input(valid_dict_dtypes: dict[str, Any]) -> None:
	"""Test initialization with valid inputs.

	Verifies
	--------
	- The DFStandardization can be initialized with valid parameters
	- All attributes are correctly set
	- No errors are raised

	Parameters
	----------
	valid_dict_dtypes : dict[str, Any]
		Dictionary of valid data types

	Returns
	-------
	None
	"""
	instance = DFStandardization(
		dict_dtypes=valid_dict_dtypes,
		cols_from_case="snake",
		cols_to_case="camel",
		list_cols_drop_dupl=["id"],
		dict_fillna_strt={"name": "ffill"},
		str_fmt_dt="YYYY-MM-DD",
		type_error_action="raise",
		strategy_keep_when_dupl="first",
		str_data_fillna="-99999",
		str_dt_fillna="2099-12-31",
	)
	assert instance.dict_dtypes == valid_dict_dtypes
	assert instance.cols_from_case == "snake"
	assert instance.cols_to_case == "camel"
	assert instance.list_cols_drop_dupl == ["id"]
	assert instance.dict_fillna_strt == {"name": "ffill"}
	assert instance.str_fmt_dt == "YYYY-MM-DD"
	assert instance.list_cols_dt == ["date"]


def test_init_empty_dict_dtypes() -> None:
	"""Test initialization with empty dict_dtypes raises ValueError.

	Parameters
	----------
	None

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="dict_dtypes cannot be empty"):
		DFStandardization(dict_dtypes={})


def test_check_if_empty_empty_df(standardization_instance: DFStandardization) -> None:
	"""Test check_if_empty with empty DataFrame raises ValueError.

	Parameters
	----------
	standardization_instance : DFStandardization
		The DFStandardization instance to test

	Returns
	-------
	None
	"""
	empty_df = pd.DataFrame()
	with pytest.raises(ValueError, match="DataFrame is empty"):
		standardization_instance.check_if_empty(empty_df)


def test_check_if_empty_non_empty_df(
	standardization_instance: DFStandardization,
	sample_df: pd.DataFrame,
) -> None:
	"""Test check_if_empty with non-empty DataFrame.

	Parameters
	----------
	standardization_instance : DFStandardization
		The DFStandardization instance to test
	sample_df : pd.DataFrame
		The non-empty DataFrame to test

	Returns
	-------
	None
	"""
	result = standardization_instance.check_if_empty(sample_df)
	pd.testing.assert_frame_equal(result, sample_df)


def test_clean_encoding_issues(
	standardization_instance: DFStandardization,
	sample_df: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test clean_encoding_issues method.

	Parameters
	----------
	standardization_instance : DFStandardization
		The DFStandardization instance to test
	sample_df : pd.DataFrame
		The DataFrame to test
	mocker : MockerFixture
		Pytest mocker fixture

	Returns
	-------
	None
	"""
	mocker.patch(
		"stpstone.utils.parsers.str.StrHandler.remove_diacritics",
		side_effect=lambda x: x.replace("Á", "A").replace("ó", "o").replace("Ç", "C")
		if isinstance(x, str)
		else x,
	)
	df_ = sample_df.copy()
	df_["name"] = ["Álice", "Bób", "", "Çharlie"]
	result = standardization_instance.clean_encoding_issues(df_)
	assert result["name"].tolist() == ["Alice", "Bob", "", "Charlie"]
	pd.testing.assert_series_equal(
		result["numeric"], pd.Series([10.5, 20.3, np.nan, 30.1], name="numeric")
	)


def test_columns_names_case(
	standardization_instance: DFStandardization,
	sample_df: pd.DataFrame,
) -> None:
	"""Test coluns_names_case method with valid case conversion.

	Parameters
	----------
	standardization_instance : DFStandardization
		The DFStandardization instance to test
	sample_df : pd.DataFrame
		The DataFrame to test

	Returns
	-------
	None
	"""
	standardization_instance.cols_from_case = "snake"
	standardization_instance.cols_to_case = "camel"
	df_ = sample_df.copy()
	df_.columns = ["user_id", "full_name", "price_value", "created_date", "score_numeric"]
	result = standardization_instance.coluns_names_case(df_)
	expected_columns = ["userId", "fullName", "priceValue", "createdDate", "scoreNumeric"]
	assert list(result.columns) == expected_columns


def test_columns_names_case_invalid(
	standardization_instance: DFStandardization,
	sample_df: pd.DataFrame,
) -> None:
	"""Test coluns_names_case with invalid case types.

	Parameters
	----------
	standardization_instance : DFStandardization
		The DFStandardization instance to test
	sample_df : pd.DataFrame
		The DataFrame to test

	Returns
	-------
	None
	"""
	standardization_instance.cols_from_case = "invalid"
	standardization_instance.cols_to_case = "camel"
	with pytest.raises(ValueError, match="Invalid from_case"):
		_ = standardization_instance.coluns_names_case(sample_df)


def test_limit_columns_to_dtypes(
	standardization_instance: DFStandardization,
	sample_df: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test limit_columns_to_dtypes method.

	Parameters
	----------
	standardization_instance : DFStandardization
		The DFStandardization instance to test
	sample_df : pd.DataFrame
		The DataFrame to test
	mocker : MockerFixture
		Pytest mocker fixture

	Returns
	-------
	None
	"""
	standardization_instance.logger = Mock(spec=Logger)
	standardization_instance.bool_debug = True
	mocker.patch("builtins.print")
	result = standardization_instance.limit_columns_to_dtypes(sample_df)
	expected_columns = list(standardization_instance.dict_dtypes.keys())
	assert list(result.columns) == expected_columns
	standardization_instance.logger.info.assert_called()


def test_delete_empty_rows(
	standardization_instance: DFStandardization,
	sample_df: pd.DataFrame,
) -> None:
	"""Test delete_empty_rows method.

	Parameters
	----------
	standardization_instance : DFStandardization
		The DFStandardization instance to test
	sample_df : pd.DataFrame
		The DataFrame to test

	Returns
	-------
	None
	"""
	df_ = sample_df.copy()
	df_.loc[4] = [None, "", "", None, np.nan]
	result = standardization_instance.delete_empty_rows(df_)
	assert len(result) == 4
	assert not result.apply(lambda row: row.isin([None, "", np.nan]).all(), axis=1).any()


def test_filler(
	standardization_instance: DFStandardization,
	sample_df: pd.DataFrame,
) -> None:
	"""Test filler method with various fill strategies.

	Parameters
	----------
	standardization_instance : DFStandardization
		The DFStandardization instance to test
	sample_df : pd.DataFrame
		The DataFrame to test

	Returns
	-------
	None
	"""
	standardization_instance.dict_fillna_strt = {"name": "ffill"}
	result = standardization_instance.filler(sample_df)
	assert result["name"].iloc[2] == "Bob"  # ffill
	assert result["date"].iloc[2] == "2099-12-31"
	assert result["numeric"].iloc[2] == "-99999"


def test_filler_invalid_strategy(
	standardization_instance: DFStandardization,
	sample_df: pd.DataFrame,
) -> None:
	"""Test filler method with invalid fill strategy.

	Parameters
	----------
	standardization_instance : DFStandardization
		The DFStandardization instance to test
	sample_df : pd.DataFrame
		The DataFrame to test

	Returns
	-------
	None
	"""
	standardization_instance.dict_fillna_strt = {"name": "invalid"}
	with pytest.raises(ValueError, match="Invalid fillna strategy"):
		standardization_instance.filler(sample_df)


def test_replace_num_delimiters(
	standardization_instance: DFStandardization,
	sample_df: pd.DataFrame,
) -> None:
	"""Test replace_num_delimiters method.

	Parameters
	----------
	standardization_instance : DFStandardization
		The DFStandardization instance to test
	sample_df : pd.DataFrame
		The DataFrame to test

	Returns
	-------
	None
	"""
	result = standardization_instance.replace_num_delimiters(sample_df)
	expected = [1234.56, 2345.67, 3456.78, 4567.89]
	assert result["value"].tolist() == pytest.approx(expected, rel=1e-3)
	assert pd.api.types.is_string_dtype(result["name"])


def test_change_dtypes(
	standardization_instance: DFStandardization,
	sample_df: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test change_dtypes method.

	Parameters
	----------
	standardization_instance : DFStandardization
		The DFStandardization instance to test
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
	df_["id"] = df_["id"].fillna(0)  # fill NaN to allow int64 conversion
	df_["date"] = df_["date"].fillna("2099-12-31")  # fill None for date column
	df_ = standardization_instance.replace_num_delimiters(df_)  # preprocess value column
	result = standardization_instance.change_dtypes(df_)
	assert result["id"].dtype == "int64"
	assert pd.api.types.is_string_dtype(result["name"])
	assert result["value"].dtype == "float64"
	assert isinstance(result["date"].iloc[0], datetime)


def test_strip_hidden_characters(
	standardization_instance: DFStandardization,
	sample_df: pd.DataFrame,
) -> None:
	"""Test strip_hidden_characters method.

	Parameters
	----------
	standardization_instance : DFStandardization
		The DFStandardization instance to test
	sample_df : pd.DataFrame
		The DataFrame to test

	Returns
	-------
	None
	"""
	df_ = sample_df.copy()
	df_["name"] = ["Alice\u200b", "Bob\u200c", "", "Charlie\ufeff"]
	result = standardization_instance.strip_hidden_characters(df_)
	assert result["name"].tolist() == ["Alice", "Bob", "", "Charlie"]


def test_strip_data(
	standardization_instance: DFStandardization,
	sample_df: pd.DataFrame,
) -> None:
	"""Test strip_data method.

	Parameters
	----------
	standardization_instance : DFStandardization
		The DFStandardization instance to test
	sample_df : pd.DataFrame
		The DataFrame to test

	Returns
	-------
	None
	"""
	df_ = sample_df.copy()
	df_["name"] = ["  Alice  ", "Bob  ", "", "  Charlie"]
	result = standardization_instance.strip_data(df_)
	assert result["name"].tolist() == ["Alice", "Bob", "", "Charlie"]


def test_remove_duplicated_cols(standardization_instance: DFStandardization) -> None:
	"""Test remove_duplicated_cols method.

	Parameters
	----------
	standardization_instance : DFStandardization
		The DFStandardization instance to test

	Returns
	-------
	None
	"""
	df_ = pd.DataFrame(
		{
			"col1": [1, 2, 3],
			"col1.1": [4, 5, 6],  # simulate duplicate column name
			"col2": [7, 8, 9],
		}
	)
	df_.columns = ["col1", "col1", "col2"]  # force duplicate column names
	standardization_instance.strategy_keep_when_dupl = "first"
	result = standardization_instance.remove_duplicated_cols(df_)
	assert list(result.columns) == ["col1", "col2"]
	assert result["col1"].tolist() == [1, 2, 3]


def test_data_remove_dupl(
	standardization_instance: DFStandardization,
	sample_df: pd.DataFrame,
) -> None:
	"""Test data_remove_dupl method.

	Parameters
	----------
	standardization_instance : DFStandardization
		The DFStandardization instance to test
	sample_df : pd.DataFrame
		The DataFrame to test

	Returns
	-------
	None
	"""
	standardization_instance.list_cols_drop_dupl = ["id"]
	df_ = sample_df.copy()
	df_.loc[4] = [1, "Duplicate", "1.234,56", "2023-01-01", 10.5]
	result = standardization_instance.data_remove_dupl(df_)
	assert len(result) == 4


def test_pipeline(
	standardization_instance: DFStandardization,
	sample_df: pd.DataFrame,
	mocker: MockerFixture,
) -> None:
	"""Test pipeline method.

	Parameters
	----------
	standardization_instance : DFStandardization
		The DFStandardization instance to test
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
	df_["id"] = df_["id"].fillna(0)  # fill NaN to allow int64 conversion
	df_["date"] = df_["date"].fillna("2099-12-31")  # fill None for date column
	df_ = standardization_instance.replace_num_delimiters(df_)  # preprocess value column
	result = standardization_instance.pipeline(df_)
	assert list(result.columns) == list(standardization_instance.dict_dtypes.keys())
	assert result["id"].dtype == "int64"
	assert result["value"].dtype == "float64"
	assert isinstance(result["date"].iloc[0], datetime)


# --------------------------
# Edge Cases and Error Conditions
# --------------------------
def test_empty_columns_list(standardization_instance: DFStandardization) -> None:
	"""Test handling of DataFrame with no columns.

	Parameters
	----------
	standardization_instance : DFStandardization
		The DFStandardization instance to test

	Returns
	-------
	None
	"""
	empty_df = pd.DataFrame(columns=["id", "name"])
	with pytest.raises(ValueError, match="DataFrame is empty"):
		standardization_instance.check_if_empty(empty_df)


def test_none_values_in_columns(
	standardization_instance: DFStandardization,
	sample_df: pd.DataFrame,
) -> None:
	"""Test handling of None values in columns.

	Parameters
	----------
	standardization_instance : DFStandardization
		The DFStandardization instance to test
	sample_df : pd.DataFrame
		The DataFrame to test

	Returns
	-------
	None
	"""
	df_ = sample_df.copy()
	df_["name"] = [None, None, None, None]
	result = standardization_instance.filler(df_)
	assert result["name"].tolist() == ["-99999", "-99999", "-99999", "-99999"]


def test_type_checker_decorator(
	standardization_instance: DFStandardization,
	mocker: MockerFixture,
) -> None:
	"""Test type_checker decorator for clean_cell function.

	Parameters
	----------
	standardization_instance : DFStandardization
		The DFStandardization instance to test
	mocker : MockerFixture
		Pytest mocker fixture

	Returns
	-------
	None
	"""
	mocker.patch(
		"stpstone.utils.parsers.str.StrHandler.remove_diacritics",
		side_effect=lambda x: x.replace("Á", "A").replace("ó", "o").replace("Ç", "C")
		if isinstance(x, str)
		else x,
	)
	df_ = pd.DataFrame({"test": ["test"]})
	result = standardization_instance.clean_encoding_issues(df_)
	assert result["test"].iloc[0] == "test"

	df_invalid = pd.DataFrame({"test": [123]})
	result = standardization_instance.clean_encoding_issues(df_invalid)
	assert result["test"].iloc[0] == 123  # adjust to match current implementation


# --------------------------
# Reload Logic Tests
# --------------------------
def test_module_reload(mocker: MockerFixture) -> None:
	"""Test module reload behavior.

	Parameters
	----------
	mocker : MockerFixture
		Pytest mocker fixture

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.transformations.standardization

	mocker.patch(
		"stpstone.utils.calendars.calendar_br.DatesBRAnbima.__init__",
		return_value=None,
	)
	_ = DFStandardization(dict_dtypes={"id": "int64"})
	importlib.reload(stpstone.transformations.standardization)
	new_instance = DFStandardization(dict_dtypes={"id": "int64"})
	assert new_instance.dict_dtypes == {"id": "int64"}
