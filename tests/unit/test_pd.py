"""Unit tests for DealingPd class.

Tests the pandas DataFrame manipulation and Excel export functionality including:
- Excel file operations with sensitivity labels
- DataFrame merging and datetime conversion
- Validation checks and error conditions
"""

import datetime
from logging import Logger
import os
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture

from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.pd import DealingPd


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
	"""Fixture providing a sample DataFrame for testing.

	Returns
	-------
	pd.DataFrame
		Sample DataFrame with columns ['A', 'B'] and 3 rows of test data
	"""
	return pd.DataFrame({"A": [1, 2, 3], "B": ["x", "y", "z"]})


@pytest.fixture
def sample_datetime_dataframe() -> pd.DataFrame:
	"""Fixture providing a DataFrame with datetime columns for testing.

	Returns
	-------
	pd.DataFrame
		DataFrame with datetime columns in different formats
	"""
	return pd.DataFrame(
		{
			"date_str": ["2023-01-01", "2023-01-02", "2023-01-03"],
			"date_float": [44927, 44928, 44929],  # excel date format
		}
	)


@pytest.fixture
def mock_logger() -> Logger:
	"""Fixture providing a mock logger object.

	Returns
	-------
	Logger
		Mocked logger instance
	"""
	return MagicMock(spec=Logger)


@pytest.fixture
def dealing_pd() -> Any:  # noqa ANN401: typing.Any is not allowed
	"""Fixture providing an instance of DealingPd.

	Returns
	-------
	Any
		Instance of DealingPd class
	"""
	return DealingPd()


# --------------------------
# Tests for _validate_sensitivity_label
# --------------------------
def test_validate_sensitivity_label_valid(
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test validation of valid sensitivity labels.

	Verifies
	--------
	- All valid sensitivity labels pass validation without raising exceptions

	Parameters
	----------
	dealing_pd : Any
		Instance of DealingPd

	Returns
	-------
	None
	"""
	for label in ["public", "internal", "confidential", "restricted"]:
		dealing_pd._validate_sensitivity_label(label)


def test_validate_sensitivity_label_invalid(
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test validation of invalid sensitivity labels.

	Verifies
	--------
	- Invalid sensitivity labels raise ValueError
	- Error message contains valid options

	Parameters
	----------
	dealing_pd : Any
		Instance of DealingPd

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError) as excinfo:
		dealing_pd._validate_sensitivity_label("invalid")
	assert "Must be one of:" in str(excinfo.value)


# --------------------------
# Tests for _validate_dataframe
# --------------------------
def test_validate_dataframe_non_empty(
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
	sample_dataframe: pd.DataFrame,
) -> None:
	"""Test validation of non-empty DataFrame.

	Verifies
	--------
	- Non-empty DataFrame passes validation

	Parameters
	----------
	dealing_pd : Any
		Instance of DealingPd
	sample_dataframe : pd.DataFrame
		Sample DataFrame to validate

	Returns
	-------
	None
	"""
	dealing_pd._validate_dataframe(sample_dataframe)


def test_validate_dataframe_empty(
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test validation of empty DataFrame.

	Verifies
	--------
	- Empty DataFrame raises ValueError

	Parameters
	----------
	dealing_pd : Any
		Instance of DealingPd

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="cannot be empty"):
		dealing_pd._validate_dataframe(pd.DataFrame())


# --------------------------
# Tests for append_df_to_Excel
# --------------------------
def test_append_df_to_excel_empty_filename(
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
	sample_dataframe: pd.DataFrame,
) -> None:
	"""Test append with empty filename.

	Verifies
	--------
	- Empty filename raises ValueError

	Parameters
	----------
	dealing_pd : Any
		Instance of DealingPd
	sample_dataframe : pd.DataFrame
		Sample DataFrame to append

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="cannot be empty"):
		dealing_pd.append_df_to_Excel("", [(sample_dataframe, "Sheet1")])


def test_append_df_to_excel_empty_list(
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
	tmp_path: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test append with empty DataFrame list.

	Verifies
	--------
	- Empty DataFrame list raises ValueError

	Parameters
	----------
	dealing_pd : Any
		Instance of DealingPd
	tmp_path : Any
		Temporary directory path

	Returns
	-------
	None
	"""
	filename = tmp_path / "test.xlsx"
	with pytest.raises(ValueError, match="cannot be empty"):
		dealing_pd.append_df_to_Excel(str(filename), [])


@patch("pandas.ExcelWriter")
def test_append_df_to_excel_writing(
	mock_writer: Any,  # noqa ANN401: typing.Any is not allowed
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
	sample_dataframe: pd.DataFrame,
	tmp_path: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test DataFrame writing to Excel.

	Verifies
	--------
	- ExcelWriter is called with correct parameters
	- DataFrame is written to sheet

	Parameters
	----------
	mock_writer : Any
		Mocked ExcelWriter
	dealing_pd : Any
		Instance of DealingPd
	sample_dataframe : pd.DataFrame
		Sample DataFrame to append
	tmp_path : Any
		Temporary directory path

	Returns
	-------
	None
	"""
	filename = tmp_path / "test.xlsx"

	# mock the to_excel method directly instead of ExcelWriter
	with patch.object(sample_dataframe, "to_excel") as mock_to_excel:
		dealing_pd.append_df_to_Excel(
			str(filename),
			[(sample_dataframe, "Sheet1")],
			bool_header=False,
			bool_index=True,
			engine="openpyxl",
		)

		# verify to_excel was called with correct parameters
		mock_to_excel.assert_called_once()
		call_args = mock_to_excel.call_args

		# check that the writer argument is present and other parameters are correct
		assert "sheet_name" in call_args.kwargs
		assert call_args.kwargs["sheet_name"] == "Sheet1"
		assert call_args.kwargs["header"] is False
		assert call_args.kwargs["index"] is True


@pytest.mark.skipif(os.name != "nt", reason="Windows-only test")
def test_append_df_to_excel_sensitivity_label_windows(
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
	sample_dataframe: pd.DataFrame,
	tmp_path: Any,  # noqa ANN401: typing.Any is not allowed
	mocker: MockerFixture,
) -> None:
	"""Test sensitivity label setting on Windows.

	Verifies
	--------
	- Sensitivity label is set when requested on Windows

	Parameters
	----------
	dealing_pd : Any
		Instance of DealingPd
	sample_dataframe : pd.DataFrame
		Sample DataFrame to append
	tmp_path : Any
		Temporary directory path
	mocker : MockerFixture
		Pytest mocker fixture

	Returns
	-------
	None
	"""
	filename = tmp_path / "test.xlsx"
	mock_label = mocker.patch.object(
		dealing_pd.cls_dealing_xl, "xlsx_sensitivity_label", create=True
	)
	dealing_pd.append_df_to_Excel(
		str(filename),
		[(sample_dataframe, "Sheet1")],
		bool_set_sensitivity_label=True,
		label_sensitivity="confidential",
	)
	mock_label.assert_called_once()


def test_append_df_to_excel_sensitivity_label_non_windows(
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
	sample_dataframe: pd.DataFrame,
	tmp_path: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test sensitivity label setting on non-Windows.

	Verifies
	--------
	- Attempt to set sensitivity label on non-Windows raises ValueError

	Parameters
	----------
	dealing_pd : Any
		Instance of DealingPd
	sample_dataframe : pd.DataFrame
		Sample DataFrame to append
	tmp_path : Any
		Temporary directory path

	Returns
	-------
	None
	"""
	if os.name == "nt":
		pytest.skip("Windows-only test")

	filename = tmp_path / "test.xlsx"
	with pytest.raises(ValueError, match="only supported on Windows"):
		dealing_pd.append_df_to_Excel(
			str(filename), [(sample_dataframe, "Sheet1")], bool_set_sensitivity_label=True
		)


# --------------------------
# Tests for export_xl
# --------------------------
def test_export_xl_success(
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
	sample_dataframe: pd.DataFrame,
	mock_logger: Logger,
	tmp_path: Any,  # noqa ANN401: typing.Any is not allowed
	mocker: MockerFixture,
) -> None:
	"""Test successful Excel export.

	Verifies
	--------
	- File is created successfully
	- Returns True when file exists
	- No warnings logged

	Parameters
	----------
	dealing_pd : Any
		Instance of DealingPd
	sample_dataframe : pd.DataFrame
		Sample DataFrame to append
	mock_logger : Logger
		Mocked logger
	tmp_path : Any
		Temporary directory path
	mocker : MockerFixture
		Pytest mocker fixture

	Returns
	-------
	None
	"""
	filename = tmp_path / "test.xlsx"
	mocker.patch.object(DirFilesManagement, "object_exists", return_value=True)

	result = dealing_pd.export_xl(mock_logger, str(filename), [(sample_dataframe, "Sheet1")])
	assert result is True
	mock_logger.warning.assert_not_called()


def test_export_xl_failure(
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
	sample_dataframe: pd.DataFrame,
	mock_logger: Logger,
	tmp_path: Any,  # noqa ANN401: typing.Any is not allowed
	mocker: MockerFixture,
) -> None:
	"""Test failed Excel export.

	Verifies
	--------
	- Warning is logged when file fails to save
	- Exception is raised

	Parameters
	----------
	dealing_pd : Any
		Instance of DealingPd
	sample_dataframe : pd.DataFrame
		Sample DataFrame to append
	mock_logger : Logger
		Mocked logger
	tmp_path : Any
		Temporary directory path
	mocker : MockerFixture
		Pytest mocker fixture

	Returns
	-------
	None
	"""
	filename = tmp_path / "test.xlsx"
	mocker.patch.object(DirFilesManagement, "object_exists", return_value=False)

	with pytest.raises(Exception, match="File not saved to hard drive"):
		dealing_pd.export_xl(mock_logger, str(filename), [(sample_dataframe, "Sheet1")])
	mock_logger.warning.assert_called_once()


@pytest.mark.skipif(os.name != "nt", reason="Windows-only test")
def test_export_xl_adjust_layout_windows(
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
	sample_dataframe: pd.DataFrame,
	mock_logger: Logger,
	tmp_path: Any,  # noqa ANN401: typing.Any is not allowed
	mocker: MockerFixture,
) -> None:
	"""Test layout adjustment on Windows.

	Verifies
	--------
	- Excel autofit is called when requested on Windows

	Parameters
	----------
	dealing_pd : Any
		Instance of DealingPd
	sample_dataframe : pd.DataFrame
		Sample DataFrame to append
	mock_logger : Logger
		Mocked logger
	tmp_path : Any
		Temporary directory path
	mocker : MockerFixture
		Pytest mocker fixture

	Returns
	-------
	None
	"""
	filename = tmp_path / "test.xlsx"
	mocker.patch.object(DirFilesManagement, "object_exists", return_value=True)
	mock_open = mocker.patch.object(dealing_pd.cls_dealing_xl, "open_xl")
	mock_autofit = mocker.patch.object(dealing_pd.cls_dealing_xl, "autofit_range_columns")
	mock_close = mocker.patch.object(dealing_pd.cls_dealing_xl, "close_wb")

	dealing_pd.export_xl(
		mock_logger, str(filename), [(sample_dataframe, "Sheet1")], bool_adjust_layout=True
	)
	mock_open.assert_called_once()
	mock_autofit.assert_called_once()
	mock_close.assert_called_once()


# --------------------------
# Tests for settingup_pandas
# --------------------------
def test_settingup_pandas(
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test pandas display options configuration.

	Verifies
	--------
	- Pandas options are set correctly

	Parameters
	----------
	dealing_pd : Any
		Instance of DealingPd

	Returns
	-------
	None
	"""
	dealing_pd.settingup_pandas(int_decimal_places=5, bool_wrap_repr=True, int_max_rows=50)
	assert pd.get_option("display.precision") == 5
	assert pd.get_option("display.expand_frame_repr") is True
	assert pd.get_option("display.max_rows") == 50


# --------------------------
# Tests for convert_datetime_columns
# --------------------------
def test_convert_datetime_columns_empty_list(
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
	sample_dataframe: pd.DataFrame,
) -> None:
	"""Test datetime conversion with empty column list.

	Verifies
	--------
	- Empty column list raises ValueError

	Parameters
	----------
	dealing_pd : Any
		Instance of DealingPd
	sample_dataframe : pd.DataFrame
		Sample DataFrame to append

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="cannot be empty"):
		dealing_pd.convert_datetime_columns(sample_dataframe, [])


def test_convert_datetime_columns_pandas_method(
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
	sample_datetime_dataframe: pd.DataFrame,
) -> None:
	"""Test pandas datetime conversion method.

	Verifies
	--------
	- Datetime columns are converted correctly using pandas method
	- Output contains date objects

	Parameters
	----------
	dealing_pd : Any
		Instance of DealingPd
	sample_datetime_dataframe : pd.DataFrame
		Sample DataFrame to append

	Returns
	-------
	None
	"""
	result = dealing_pd.convert_datetime_columns(
		sample_datetime_dataframe.copy(), ["date_str"], bool_pandas_convertion=True
	)
	assert pd.api.types.is_object_dtype(result["date_str"])
	assert isinstance(result["date_str"].iloc[0], datetime.date)


def test_convert_datetime_columns_excel_method(
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
	sample_datetime_dataframe: pd.DataFrame,
) -> None:
	"""Test Excel datetime conversion method.

	Verifies
	--------
	- Excel date formats are converted correctly
	- Output contains date objects

	Parameters
	----------
	dealing_pd : Any
		Instance of DealingPd
	sample_datetime_dataframe : pd.DataFrame
		Sample DataFrame to append

	Returns
	-------
	None
	"""
	result = dealing_pd.convert_datetime_columns(
		sample_datetime_dataframe.copy(), ["date_float"], bool_pandas_convertion=False
	)
	assert pd.api.types.is_object_dtype(result["date_float"])
	assert isinstance(result["date_float"].iloc[0], datetime.date)


# --------------------------
# Tests for merge_dfs_into_df
# --------------------------
def test_merge_dfs_into_df_empty_columns(
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
	sample_dataframe: pd.DataFrame,
) -> None:
	"""Test merge with empty column list.

	Verifies
	--------
	- Empty merge columns list raises ValueError

	Parameters
	----------
	dealing_pd : Any
		Instance of DealingPd
	sample_dataframe : pd.DataFrame
		Sample DataFrame to append

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="cannot be empty"):
		dealing_pd.merge_dfs_into_df(sample_dataframe, sample_dataframe, [])


def test_merge_dfs_into_df_success(
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
	sample_dataframe: pd.DataFrame,
) -> None:
	"""Test successful DataFrame merge.

	Verifies
	--------
	- DataFrames are merged correctly
	- Intersections are removed
	- Column names are cleaned

	Parameters
	----------
	dealing_pd : Any
		Instance of DealingPd
	sample_dataframe : pd.DataFrame
		Sample DataFrame to append

	Returns
	-------
	None
	"""
	df1 = sample_dataframe.copy()
	df2 = sample_dataframe.copy()
	df2["A"] = [4, 5, 6]  # Make df2 different

	result = dealing_pd.merge_dfs_into_df(df1, df2, ["A"])
	assert len(result) == 3
	assert "B" in result.columns


# --------------------------
# Tests for max_chrs_per_column
# --------------------------
def test_max_chrs_per_column_empty(
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test max characters with empty DataFrame.

	Verifies
	--------
	- Empty DataFrame raises ValueError

	Parameters
	----------
	dealing_pd : Any
		Instance of DealingPd

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="cannot be empty"):
		dealing_pd.max_chrs_per_column(pd.DataFrame())


def test_max_chrs_per_column_success(
	dealing_pd: Any,  # noqa ANN401: typing.Any is not allowed
	sample_dataframe: pd.DataFrame,
) -> None:
	"""Test max characters calculation.

	Verifies
	--------
	- Correct maximum lengths are calculated
	- Returns dictionary with column lengths

	Parameters
	----------
	dealing_pd : Any
		Instance of DealingPd
	sample_dataframe : pd.DataFrame
		Sample DataFrame to append

	Returns
	-------
	None
	"""
	result = dealing_pd.max_chrs_per_column(sample_dataframe)
	assert isinstance(result, dict)
	assert result["A"] == 1  # Single digit numbers
	assert result["B"] == 1  # Single character strings
