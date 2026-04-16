"""Unit tests for DataFrameValidator class.

Tests the DataFrame validation functionality with various input scenarios including:
- Initialization with valid inputs
- Missing value and duplicate checks
- Range validation with numeric constraints
- Date order validation
- Categorical value validation
- Pipeline execution
- Edge cases and error conditions
"""

import logging
from numbers import Number
from typing import Any
from unittest.mock import Mock, patch

import pandas as pd
from pandas._testing import assert_frame_equal
import pytest

from stpstone.transformations.validation.validate_df import DataFrameValidator
from stpstone.utils.loggs.create_logs import CreateLog


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
	"""Fixture providing sample DataFrame for testing.

	Returns
	-------
	pd.DataFrame
		Sample DataFrame with numeric, date, and categorical columns
	"""
	return pd.DataFrame(
		{
			"numeric_col": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
			"date_start": pd.date_range("2023-01-01", periods=10, freq="D"),
			"date_end": pd.date_range("2023-01-05", periods=10, freq="D"),
			"category_col": ["A", "B", "C", "A", "B", "C", "A", "B", "C", "D"],
			"with_nulls": [1, 2, None, 4, 5, None, 7, 8, 9, 10],
			"duplicates": [1, 2, 3, 1, 2, 3, 1, 2, 3, 4],
		}
	)


@pytest.fixture
def sample_dataframe_with_missing() -> pd.DataFrame:
	"""Fixture providing DataFrame with missing values.

	Returns
	-------
	pd.DataFrame
		DataFrame containing NaN values
	"""
	return pd.DataFrame(
		{
			"col1": [1, 2, None, 4, 5],
			"col2": ["A", "B", None, "D", "E"],
			"col3": [1.1, 2.2, 3.3, None, 5.5],
		}
	)


@pytest.fixture
def sample_dataframe_with_duplicates() -> pd.DataFrame:
	"""Fixture providing DataFrame with duplicate rows.

	Returns
	-------
	pd.DataFrame
		DataFrame containing duplicate rows
	"""
	return pd.DataFrame({"col1": [1, 2, 3, 1, 2], "col2": ["A", "B", "C", "A", "B"]})


@pytest.fixture
def sample_dataframe_no_issues() -> pd.DataFrame:
	"""Fixture providing DataFrame with no validation issues.

	Returns
	-------
	pd.DataFrame
		Clean DataFrame for testing valid scenarios
	"""
	return pd.DataFrame(
		{
			"numeric_col": [1, 2, 3, 4, 5],
			"date_start": pd.date_range("2023-01-01", periods=5, freq="D"),
			"date_end": pd.date_range("2023-01-05", periods=5, freq="D"),
			"category_col": ["A", "B", "C", "A", "B"],
		}
	)


@pytest.fixture
def mock_logger() -> Mock:
	"""Fixture providing mock logger.

	Returns
	-------
	Mock
		Mocked logger instance
	"""
	return Mock(spec=logging.Logger)


@pytest.fixture
def mock_create_log() -> Mock:
	"""Fixture providing mock CreateLog instance.

	Returns
	-------
	Mock
		Mocked CreateLog instance
	"""
	return Mock(spec=CreateLog)


@pytest.fixture
def dataframe_validator(sample_dataframe: pd.DataFrame, mock_logger: Mock) -> DataFrameValidator:
	"""Fixture providing DataFrameValidator instance.

	Parameters
	----------
	sample_dataframe : pd.DataFrame
		Sample DataFrame from fixture
	mock_logger : Mock
		Mocked logger instance

	Returns
	-------
	DataFrameValidator
		Initialized DataFrameValidator instance
	"""
	return DataFrameValidator(sample_dataframe, mock_logger)


@pytest.fixture
def range_constraints() -> dict[str, tuple[Number, Number]]:
	"""Fixture providing range constraints for testing.

	Returns
	-------
	dict[str, tuple[Number, Number]]
		Dictionary with column names and their valid ranges
	"""
	return {"numeric_col": (1, 10)}


@pytest.fixture
def categorical_constraints() -> tuple[str, list[str]]:
	"""Fixture providing categorical constraints for testing.

	Returns
	-------
	tuple[str, list[str]]
		Tuple of column name and allowed values
	"""
	return ("category_col", ["A", "B", "C"])


# --------------------------
# Tests for Initialization
# --------------------------
def test_init_with_valid_inputs(sample_dataframe: pd.DataFrame, mock_logger: Mock) -> None:
	"""Test initialization with valid DataFrame and logger.

	Verifies
	--------
	- The DataFrameValidator can be initialized with valid inputs
	- The DataFrame is correctly stored
	- The logger is correctly stored
	- CreateLog instance is created

	Parameters
	----------
	sample_dataframe : pd.DataFrame
		Sample DataFrame from fixture
	mock_logger : Mock
		Mocked logger instance

	Returns
	-------
	None
	"""
	validator = DataFrameValidator(sample_dataframe, mock_logger)

	assert_frame_equal(validator.df_, sample_dataframe)
	assert validator.logger is mock_logger
	assert isinstance(validator.create_log, CreateLog)


def test_init_with_invalid_dataframe_type(mock_logger: Mock) -> None:
	"""Test initialization raises TypeError with invalid DataFrame type.

	Verifies
	--------
	- TypeError is raised when df_ is not a pandas DataFrame
	- Error message indicates the expected type

	Parameters
	----------
	mock_logger : Mock
		Mocked logger instance

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		DataFrameValidator("not_a_dataframe", mock_logger)  # type: ignore


def test_init_with_invalid_logger_type(sample_dataframe: pd.DataFrame) -> None:
	"""Test initialization raises TypeError with invalid logger type.

	Verifies
	--------
	- TypeError is raised when logger is not a Logger instance
	- Error message indicates the expected type

	Parameters
	----------
	sample_dataframe : pd.DataFrame
		Sample DataFrame from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		DataFrameValidator(sample_dataframe, "not_a_logger")  # type: ignore


# --------------------------
# Tests for Missing Values
# --------------------------
def test_check_missing_values_no_missing(
	sample_dataframe_no_issues: pd.DataFrame, mock_logger: Mock, mock_create_log: Mock
) -> None:
	"""Test check_missing_values with no missing values.

	Verifies
	--------
	- No warning is logged when no missing values are present
	- CreateLog.log_message is not called

	Parameters
	----------
	sample_dataframe_no_issues : pd.DataFrame
		Sample DataFrame with no issues from fixture
	mock_logger : Mock
		Mocked logger instance
	mock_create_log : Mock
		Mocked CreateLog instance

	Returns
	-------
	None
	"""
	validator = DataFrameValidator(sample_dataframe_no_issues, mock_logger)

	with patch.object(validator, "create_log", mock_create_log):
		result = validator.check_missing_values()

	assert_frame_equal(result, sample_dataframe_no_issues)
	mock_create_log.log_message.assert_not_called()


def test_check_missing_values_with_missing(
	sample_dataframe_with_missing: pd.DataFrame, mock_logger: Mock, mock_create_log: Mock
) -> None:
	"""Test check_missing_values with missing values.

	Verifies
	--------
	- Warning is logged when missing values are present
	- CreateLog.log_message is called with correct parameters

	Parameters
	----------
	sample_dataframe_with_missing : pd.DataFrame
		DataFrame with missing values from fixture
	mock_logger : Mock
		Mocked logger instance
	mock_create_log : Mock
		Mocked CreateLog instance

	Returns
	-------
	None
	"""
	validator = DataFrameValidator(sample_dataframe_with_missing, mock_logger)

	with patch.object(validator, "create_log", mock_create_log):
		result = validator.check_missing_values()

	assert_frame_equal(result, sample_dataframe_with_missing)
	mock_create_log.log_message.assert_called_once()
	args, kwargs = mock_create_log.log_message.call_args
	assert args[0] is mock_logger
	assert "Missing Values" in args[1]
	assert args[2] == "warning"


# --------------------------
# Tests for Duplicates
# --------------------------
def test_check_duplicates_no_duplicates(
	sample_dataframe_no_issues: pd.DataFrame, mock_logger: Mock, mock_create_log: Mock
) -> None:
	"""Test check_duplicates with no duplicate rows.

	Verifies
	--------
	- No warning is logged when no duplicates are present
	- CreateLog.log_message is not called

	Parameters
	----------
	sample_dataframe_no_issues : pd.DataFrame
		Sample DataFrame with no issues from fixture
	mock_logger : Mock
		Mocked logger instance
	mock_create_log : Mock
		Mocked CreateLog instance

	Returns
	-------
	None
	"""
	validator = DataFrameValidator(sample_dataframe_no_issues, mock_logger)

	with patch.object(validator, "create_log", mock_create_log):
		result = validator.check_duplicates()

	assert_frame_equal(result, sample_dataframe_no_issues)
	mock_create_log.log_message.assert_not_called()


def test_check_duplicates_with_duplicates(
	sample_dataframe_with_duplicates: pd.DataFrame, mock_logger: Mock, mock_create_log: Mock
) -> None:
	"""Test check_duplicates with duplicate rows.

	Verifies
	--------
	- Warning is logged when duplicates are present
	- CreateLog.log_message is called with correct count

	Parameters
	----------
	sample_dataframe_with_duplicates : pd.DataFrame
		DataFrame with duplicates from fixture
	mock_logger : Mock
		Mocked logger instance
	mock_create_log : Mock
		Mocked CreateLog instance

	Returns
	-------
	None
	"""
	validator = DataFrameValidator(sample_dataframe_with_duplicates, mock_logger)

	with patch.object(validator, "create_log", mock_create_log):
		result = validator.check_duplicates()

	assert_frame_equal(result, sample_dataframe_with_duplicates)
	mock_create_log.log_message.assert_called_once()
	args, kwargs = mock_create_log.log_message.call_args
	assert args[0] is mock_logger
	assert "duplicate rows" in args[1]
	assert args[2] == "warning"


# --------------------------
# Tests for Range Validation
# --------------------------
def test_validate_ranges_valid_data(
	sample_dataframe_no_issues: pd.DataFrame, mock_logger: Mock, mock_create_log: Mock
) -> None:
	"""Test validate_ranges with valid data within constraints.

	Verifies
	--------
	- Returns valid=True when all values are within range
	- Empty DataFrame is returned for out_of_bounds
	- No warning is logged

	Parameters
	----------
	sample_dataframe_no_issues : pd.DataFrame
		Sample DataFrame with no issues from fixture
	mock_logger : Mock
		Mocked logger instance
	mock_create_log : Mock
		Mocked CreateLog instance

	Returns
	-------
	None
	"""
	validator = DataFrameValidator(sample_dataframe_no_issues, mock_logger)
	range_constraints = {"numeric_col": (1, 10)}

	with patch.object(validator, "create_log", mock_create_log):
		result = validator.validate_ranges(range_constraints)

	assert result["valid"] is True
	assert result["out_of_bounds"].empty
	mock_create_log.log_message.assert_not_called()


def test_validate_ranges_out_of_bounds(
	sample_dataframe: pd.DataFrame, mock_logger: Mock, mock_create_log: Mock
) -> None:
	"""Test validate_ranges with values outside constraints.

	Verifies
	--------
	- Returns valid=False when values are outside range
	- Out-of-bounds rows are returned
	- Warning is logged with correct information

	Parameters
	----------
	sample_dataframe : pd.DataFrame
		Sample DataFrame from fixture
	mock_logger : Mock
		Mocked logger instance
	mock_create_log : Mock
		Mocked CreateLog instance

	Returns
	-------
	None
	"""
	# Modify sample data to have out-of-bounds values
	df_with_outliers = sample_dataframe.copy()
	df_with_outliers.loc[0, "numeric_col"] = 0  # Below range
	df_with_outliers.loc[1, "numeric_col"] = 11  # Above range

	validator = DataFrameValidator(df_with_outliers, mock_logger)
	constraints = {"numeric_col": (1, 10)}

	with patch.object(validator, "create_log", mock_create_log):
		result = validator.validate_ranges(constraints)

	assert result["valid"] is False
	assert len(result["out_of_bounds"]) == 2
	mock_create_log.log_message.assert_called_once()
	args, kwargs = mock_create_log.log_message.call_args
	assert "out of range" in args[1]
	assert args[2] == "warning"


def test_validate_ranges_empty_constraints(dataframe_validator: DataFrameValidator) -> None:
	"""Test validate_ranges raises ValueError with empty constraints.

	Verifies
	--------
	- ValueError is raised when constraints dictionary is empty
	- Error message indicates the issue

	Parameters
	----------
	dataframe_validator : DataFrameValidator
		DataFrameValidator instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="cannot be empty"):
		dataframe_validator.validate_ranges({})


def test_validate_ranges_nonexistent_column(
	dataframe_validator: DataFrameValidator, range_constraints: dict[str, tuple[Number, Number]]
) -> None:
	"""Test validate_ranges raises ValueError with nonexistent column.

	Verifies
	--------
	- ValueError is raised when column doesn't exist in DataFrame
	- Error message includes the column name

	Parameters
	----------
	dataframe_validator : DataFrameValidator
		DataFrameValidator instance from fixture
	range_constraints : dict[str, tuple[Number, Number]]
		Range constraints from fixture

	Returns
	-------
	None
	"""
	constraints = {"nonexistent_col": (1, 10)}

	with pytest.raises(ValueError, match="not found in DataFrame"):
		dataframe_validator.validate_ranges(constraints)


def test_validate_ranges_invalid_bounds_type(dataframe_validator: DataFrameValidator) -> None:
	"""Test validate_ranges raises ValueError with non-numeric bounds.

	Verifies
	--------
	- ValueError is raised when bounds are not numeric
	- Error message indicates the issue

	Parameters
	----------
	dataframe_validator : DataFrameValidator
		DataFrameValidator instance from fixture

	Returns
	-------
	None
	"""
	constraints = {"numeric_col": ("not", "numeric")}  # type: ignore

	with pytest.raises(ValueError, match="must be numeric"):
		dataframe_validator.validate_ranges(constraints)


def test_validate_ranges_invalid_bounds_order(dataframe_validator: DataFrameValidator) -> None:
	"""Test validate_ranges raises ValueError with invalid bounds order.

	Verifies
	--------
	- ValueError is raised when lower bound exceeds upper bound
	- Error message includes the column name and bounds

	Parameters
	----------
	dataframe_validator : DataFrameValidator
		DataFrameValidator instance from fixture

	Returns
	-------
	None
	"""
	constraints = {"numeric_col": (10, 1)}  # Lower > Upper

	with pytest.raises(ValueError, match="exceeds upper bound"):
		dataframe_validator.validate_ranges(constraints)


# --------------------------
# Tests for Date Validation
# --------------------------
def test_validate_dates_valid_order(
	dataframe_validator: DataFrameValidator, mock_create_log: Mock
) -> None:
	"""Test validate_dates with valid date order.

	Verifies
	--------
	- Returns valid=True when start dates are before end dates
	- Empty DataFrame is returned for invalid_dates
	- No warning is logged

	Parameters
	----------
	dataframe_validator : DataFrameValidator
		DataFrameValidator instance from fixture
	mock_create_log : Mock
		Mocked CreateLog instance

	Returns
	-------
	None
	"""
	with patch.object(dataframe_validator, "create_log", mock_create_log):
		result = dataframe_validator.validate_dates("date_start", "date_end")

	assert result["valid"] is True
	assert result["invalid_dates"].empty
	mock_create_log.log_message.assert_not_called()


def test_validate_dates_invalid_order(
	sample_dataframe: pd.DataFrame, mock_logger: Mock, mock_create_log: Mock
) -> None:
	"""Test validate_dates with invalid date order.

	Verifies
	--------
	- Returns valid=False when start dates are after end dates
	- Invalid date rows are returned
	- Warning is logged with correct information

	Parameters
	----------
	sample_dataframe : pd.DataFrame
		Sample DataFrame from fixture
	mock_logger : Mock
		Mocked logger instance
	mock_create_log : Mock
		Mocked CreateLog instance

	Returns
	-------
	None
	"""
	# Create invalid date order
	df_invalid_dates = sample_dataframe.copy()
	df_invalid_dates.loc[0, "date_start"] = pd.Timestamp("2023-01-10")  # After end date

	validator = DataFrameValidator(df_invalid_dates, mock_logger)

	with patch.object(validator, "create_log", mock_create_log):
		result = validator.validate_dates("date_start", "date_end")

	assert result["valid"] is False
	assert len(result["invalid_dates"]) == 1
	mock_create_log.log_message.assert_called_once()
	args, kwargs = mock_create_log.log_message.call_args
	assert "is after" in args[1]
	assert args[2] == "warning"


def test_validate_dates_empty_column_names(dataframe_validator: DataFrameValidator) -> None:
	"""Test validate_dates raises ValueError with empty column names.

	Verifies
	--------
	- ValueError is raised when column names are empty
	- Error message indicates the issue

	Parameters
	----------
	dataframe_validator : DataFrameValidator
		DataFrameValidator instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="cannot be empty"):
		dataframe_validator.validate_dates("", "date_end")

	with pytest.raises(ValueError, match="cannot be empty"):
		dataframe_validator.validate_dates("date_start", "")


def test_validate_dates_nonexistent_columns(dataframe_validator: DataFrameValidator) -> None:
	"""Test validate_dates raises ValueError with nonexistent columns.

	Verifies
	--------
	- ValueError is raised when columns don't exist in DataFrame
	- Error message indicates the issue

	Parameters
	----------
	dataframe_validator : DataFrameValidator
		DataFrameValidator instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="not found in DataFrame"):
		dataframe_validator.validate_dates("nonexistent_start", "date_end")

	with pytest.raises(ValueError, match="not found in DataFrame"):
		dataframe_validator.validate_dates("date_start", "nonexistent_end")


def test_validate_dates_non_datetime_columns(
	sample_dataframe: pd.DataFrame, mock_logger: Mock
) -> None:
	"""Test validate_dates raises ValueError with non-datetime columns.

	Verifies
	--------
	- ValueError is raised when columns don't contain datetime data
	- Error message indicates the issue

	Parameters
	----------
	sample_dataframe : pd.DataFrame
		Sample DataFrame from fixture
	mock_logger : Mock
		Mocked logger instance

	Returns
	-------
	None
	"""
	validator = DataFrameValidator(sample_dataframe, mock_logger)

	with pytest.raises(ValueError, match="must contain datetime data"):
		validator.validate_dates("numeric_col", "date_end")

	with pytest.raises(ValueError, match="must contain datetime data"):
		validator.validate_dates("date_start", "numeric_col")


# --------------------------
# Tests for Categorical Validation
# --------------------------
def test_validate_categorical_values_valid_data(
	sample_dataframe_no_issues: pd.DataFrame, mock_logger: Mock, mock_create_log: Mock
) -> None:
	"""Test validate_categorical_values with valid data.

	Verifies
	--------
	- Returns valid=True when all values are in allowed list
	- Empty DataFrame is returned for invalid_values
	- No warning is logged

	Parameters
	----------
	sample_dataframe_no_issues : pd.DataFrame
		Sample DataFrame with no issues from fixture
	mock_logger : Mock
		Mocked logger instance
	mock_create_log : Mock
		Mocked CreateLog instance

	Returns
	-------
	None
	"""
	validator = DataFrameValidator(sample_dataframe_no_issues, mock_logger)
	col = "category_col"
	allowed_values = ["A", "B", "C"]

	with patch.object(validator, "create_log", mock_create_log):
		result = validator.validate_categorical_values(col, allowed_values)

	assert result["valid"] is True
	assert result["invalid_values"].empty
	mock_create_log.log_message.assert_not_called()


def test_validate_categorical_values_invalid_data(
	dataframe_validator: DataFrameValidator, mock_create_log: Mock
) -> None:
	"""Test validate_categorical_values with invalid data.

	Verifies
	--------
	- Returns valid=False when values are not in allowed list
	- Invalid value rows are returned
	- Warning is logged with correct information

	Parameters
	----------
	dataframe_validator : DataFrameValidator
		DataFrameValidator instance from fixture
	mock_create_log : Mock
		Mocked CreateLog instance

	Returns
	-------
	None
	"""
	col = "category_col"
	allowed_values = ["A", "B"]  # Exclude "C" and "D"

	with patch.object(dataframe_validator, "create_log", mock_create_log):
		result = dataframe_validator.validate_categorical_values(col, allowed_values)

	assert result["valid"] is False
	assert len(result["invalid_values"]) == 4  # 3 "C" + 1 "D"
	mock_create_log.log_message.assert_called_once()
	args, kwargs = mock_create_log.log_message.call_args
	assert "invalid values" in args[1]
	assert args[2] == "warning"


def test_validate_categorical_values_empty_column_name(
	dataframe_validator: DataFrameValidator,
) -> None:
	"""Test validate_categorical_values raises ValueError with empty column name.

	Verifies
	--------
	- ValueError is raised when column name is empty
	- Error message indicates the issue

	Parameters
	----------
	dataframe_validator : DataFrameValidator
		DataFrameValidator instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="cannot be empty"):
		dataframe_validator.validate_categorical_values("", ["A", "B"])


def test_validate_categorical_values_nonexistent_column(
	dataframe_validator: DataFrameValidator,
) -> None:
	"""Test validate_categorical_values raises ValueError with nonexistent column.

	Verifies
	--------
	- ValueError is raised when column doesn't exist in DataFrame
	- Error message includes the column name

	Parameters
	----------
	dataframe_validator : DataFrameValidator
		DataFrameValidator instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="not found in DataFrame"):
		dataframe_validator.validate_categorical_values("nonexistent_col", ["A", "B"])


def test_validate_categorical_values_empty_allowed_values(
	dataframe_validator: DataFrameValidator,
) -> None:
	"""Test validate_categorical_values raises ValueError with empty allowed values.

	Verifies
	--------
	- ValueError is raised when allowed_values list is empty
	- Error message indicates the issue

	Parameters
	----------
	dataframe_validator : DataFrameValidator
		DataFrameValidator instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="cannot be empty"):
		dataframe_validator.validate_categorical_values("category_col", [])


# --------------------------
# Tests for Pipeline
# --------------------------
def test_pipeline_all_valid(
	sample_dataframe_no_issues: pd.DataFrame, mock_logger: Mock, mock_create_log: Mock
) -> None:
	"""Test pipeline with all validations passing.

	Verifies
	--------
	- Returns valid=True when all validations pass
	- DataFrame is returned unchanged
	- No warnings are logged

	Parameters
	----------
	sample_dataframe_no_issues : pd.DataFrame
		Sample DataFrame with no issues from fixture
	mock_logger : Mock
		Mocked logger instance
	mock_create_log : Mock
		Mocked CreateLog instance

	Returns
	-------
	None
	"""
	validator = DataFrameValidator(sample_dataframe_no_issues, mock_logger)
	range_constraints = {"numeric_col": (1, 10)}
	col, allowed_values = ("category_col", ["A", "B", "C"])

	with patch.object(validator, "create_log", mock_create_log):
		result = validator.pipeline(
			dict_rng_constraints=range_constraints,
			col_start="date_start",
			col_sup="date_end",
			list_tup_categorical_constraints=(col, allowed_values),
		)

	assert result["valid"] is True
	assert_frame_equal(result["df_"], sample_dataframe_no_issues)
	mock_create_log.log_message.assert_not_called()


def test_pipeline_with_failures(
	sample_dataframe: pd.DataFrame, mock_logger: Mock, mock_create_log: Mock
) -> None:
	"""Test pipeline with validation failures.

	Verifies
	--------
	- Returns valid=False when any validation fails
	- DataFrame is still returned
	- Warnings are logged for failures

	Parameters
	----------
	sample_dataframe : pd.DataFrame
		Sample DataFrame from fixture
	mock_logger : Mock
		Mocked logger instance
	mock_create_log : Mock
		Mocked CreateLog instance

	Returns
	-------
	None
	"""
	# Create data that will fail multiple validations
	df_with_issues = sample_dataframe.copy()
	df_with_issues.loc[0, "numeric_col"] = 0  # Out of range
	df_with_issues.loc[1, "category_col"] = "X"  # Invalid category

	validator = DataFrameValidator(df_with_issues, mock_logger)

	with patch.object(validator, "create_log", mock_create_log):
		result = validator.pipeline(
			dict_rng_constraints={"numeric_col": (1, 10)},
			list_tup_categorical_constraints=("category_col", ["A", "B", "C"]),
		)

	assert result["valid"] is False
	assert_frame_equal(result["df_"], df_with_issues)
	assert mock_create_log.log_message.call_count >= 2  # At least 2 warnings


def test_pipeline_partial_parameters(
	sample_dataframe_no_issues: pd.DataFrame, mock_logger: Mock, mock_create_log: Mock
) -> None:
	"""Test pipeline with only some parameters provided.

	Verifies
	--------
	- Pipeline runs successfully with partial parameters
	- Only the specified validations are performed
	- Returns valid=True if specified validations pass

	Parameters
	----------
	sample_dataframe_no_issues : pd.DataFrame
		Sample DataFrame with no issues from fixture
	mock_logger : Mock
		Mocked logger instance
	mock_create_log : Mock
		Mocked CreateLog instance

	Returns
	-------
	None
	"""
	validator = DataFrameValidator(sample_dataframe_no_issues, mock_logger)

	with patch.object(validator, "create_log", mock_create_log):
		# Test with only range constraints
		result1 = validator.pipeline(dict_rng_constraints={"numeric_col": (1, 10)})

		# Test with only date validation
		result2 = validator.pipeline(col_start="date_start", col_sup="date_end")

		# Test with only categorical validation
		result3 = validator.pipeline(
			list_tup_categorical_constraints=("category_col", ["A", "B", "C"])
		)

	assert result1["valid"] is True
	assert result2["valid"] is True
	assert result3["valid"] is True
	mock_create_log.log_message.assert_not_called()


def test_pipeline_no_optional_parameters(
	sample_dataframe_no_issues: pd.DataFrame, mock_logger: Mock, mock_create_log: Mock
) -> None:
	"""Test pipeline with no optional parameters.

	Verifies
	--------
	- Pipeline runs successfully with only basic validations
	- Only missing values and duplicates are checked
	- Returns valid=True if basic validations pass

	Parameters
	----------
	sample_dataframe_no_issues : pd.DataFrame
		Sample DataFrame with no issues from fixture
	mock_logger : Mock
		Mocked logger instance
	mock_create_log : Mock
		Mocked CreateLog instance

	Returns
	-------
	None
	"""
	validator = DataFrameValidator(sample_dataframe_no_issues, mock_logger)

	with patch.object(validator, "create_log", mock_create_log):
		result = validator.pipeline()

	assert result["valid"] is True
	assert_frame_equal(result["df_"], sample_dataframe_no_issues)
	mock_create_log.log_message.assert_not_called()


# --------------------------
# Tests for Type Validation
# --------------------------
@pytest.mark.parametrize(
	"invalid_input", ["string", 123, [], {}]
)  # Removed None since it's valid for Optional[pd.DataFrame]
def test_methods_type_validation_df_parameter(
	invalid_input: Any,  # noqa ANN401: typing.Any is not allowed
	mock_logger: Mock,
) -> None:
	"""Test type validation for DataFrame parameter in all methods.

	Verifies
	--------
	- TypeError is raised when invalid DataFrame type is provided
	- Error message indicates the expected type

	Parameters
	----------
	invalid_input : Any
		Various invalid input types (excluding None which is valid)
	mock_logger : Mock
		Mocked logger instance

	Returns
	-------
	None
	"""
	valid_df = pd.DataFrame({"col": [1, 2, 3]})
	validator = DataFrameValidator(valid_df, mock_logger)

	with pytest.raises(TypeError, match="must be one of types"):
		validator.check_missing_values(invalid_input)


def test_validate_ranges_type_validation(dataframe_validator: DataFrameValidator) -> None:
	"""Test type validation for validate_ranges parameters.

	Verifies
	--------
	- TypeError is raised when invalid constraint types are provided
	- Error message indicates the expected type

	Parameters
	----------
	dataframe_validator : DataFrameValidator
		DataFrameValidator instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		dataframe_validator.validate_ranges("not_a_dict")  # type: ignore


def test_validate_dates_type_validation(dataframe_validator: DataFrameValidator) -> None:
	"""Test type validation for validate_dates parameters.

	Verifies
	--------
	- TypeError is raised when invalid column name types are provided
	- Error message indicates the expected type

	Parameters
	----------
	dataframe_validator : DataFrameValidator
		DataFrameValidator instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		dataframe_validator.validate_dates(123, "date_end")  # type: ignore

	with pytest.raises(TypeError, match="must be of type"):
		dataframe_validator.validate_dates("date_start", 456)  # type: ignore


def test_validate_categorical_values_type_validation(
	dataframe_validator: DataFrameValidator,
) -> None:
	"""Test type validation for validate_categorical_values parameters.

	Verifies
	--------
	- TypeError is raised when invalid parameter types are provided
	- Error message indicates the expected type

	Parameters
	----------
	dataframe_validator : DataFrameValidator
		DataFrameValidator instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		dataframe_validator.validate_categorical_values(123, ["A", "B"])  # type: ignore

	with pytest.raises(TypeError, match="must be of type"):
		dataframe_validator.validate_categorical_values("category_col", "not_a_list")  # type: ignore


# --------------------------
# Tests for Edge Cases
# --------------------------
def test_empty_dataframe(mock_logger: Mock, mock_create_log: Mock) -> None:
	"""Test with empty DataFrame.

	Verifies
	--------
	- All validations pass with empty DataFrame
	- No warnings are logged
	- Methods return appropriate empty results

	Parameters
	----------
	mock_logger : Mock
		Mocked logger instance
	mock_create_log : Mock
		Mocked CreateLog instance

	Returns
	-------
	None
	"""
	empty_df = pd.DataFrame()
	validator = DataFrameValidator(empty_df, mock_logger)

	with patch.object(validator, "create_log", mock_create_log):
		# Test basic validations
		result_missing = validator.check_missing_values()
		result_duplicates = validator.check_duplicates()

		# Test pipeline
		result = validator.pipeline()

	assert_frame_equal(result_missing, empty_df)
	assert_frame_equal(result_duplicates, empty_df)
	assert result["valid"] is True
	assert result["df_"].empty
	mock_create_log.log_message.assert_not_called()


def test_single_row_dataframe(mock_logger: Mock, mock_create_log: Mock) -> None:
	"""Test with single row DataFrame.

	Verifies
	--------
	- All validations pass with single row DataFrame
	- No warnings are logged
	- Methods return appropriate results

	Parameters
	----------
	mock_logger : Mock
		Mocked logger instance
	mock_create_log : Mock
		Mocked CreateLog instance

	Returns
	-------
	None
	"""
	single_row_df = pd.DataFrame(
		{
			"numeric_col": [5],
			"date_start": [pd.Timestamp("2023-01-01")],
			"date_end": [pd.Timestamp("2023-01-02")],
			"category_col": ["A"],
		}
	)

	validator = DataFrameValidator(single_row_df, mock_logger)

	with patch.object(validator, "create_log", mock_create_log):
		result = validator.pipeline(
			dict_rng_constraints={"numeric_col": (1, 10)},
			col_start="date_start",
			col_sup="date_end",
			list_tup_categorical_constraints=("category_col", ["A", "B"]),
		)

	assert result["valid"] is True
	assert len(result["df_"]) == 1
	mock_create_log.log_message.assert_not_called()


def test_dataframe_with_all_nulls(mock_logger: Mock, mock_create_log: Mock) -> None:
	"""Test with DataFrame containing only null values.

	Verifies
	--------
	- Missing values warning is logged
	- Duplicate values warning is also logged (all null rows are identical)
	- Pipeline returns valid=False if issues found

	Parameters
	----------
	mock_logger : Mock
		Mocked logger instance
	mock_create_log : Mock
		Mocked CreateLog instance

	Returns
	-------
	None
	"""
	all_nulls_df = pd.DataFrame(
		{"col1": [None, None, None], "col2": [None, None, None], "col3": [None, None, None]}
	)

	validator = DataFrameValidator(all_nulls_df, mock_logger)

	with patch.object(validator, "create_log", mock_create_log):
		result = validator.pipeline()

	# Should be invalid due to missing values and duplicates
	assert result["valid"] is False
	assert_frame_equal(result["df_"], all_nulls_df)

	# Should have been called twice: once for missing values, once for duplicates
	assert mock_create_log.log_message.call_count == 2

	# Check that both missing values and duplicates were logged
	call_args_list = mock_create_log.log_message.call_args_list
	messages = [call[0][1] for call in call_args_list]

	# Check that we have both missing values and duplicate warnings
	assert any("Missing Values" in msg for msg in messages)
	assert any("duplicate rows" in msg for msg in messages)

	# All calls should be warnings
	assert all(call[0][2] == "warning" for call in call_args_list)


# --------------------------
# Tests for Return Type Validation
# --------------------------
def test_return_types_match_typeddict(
	dataframe_validator: DataFrameValidator,
	range_constraints: dict[str, tuple[Number, Number]],
	categorical_constraints: tuple[str, list[str]],
) -> None:
	"""Test that return types match the declared TypedDict structures.

	Verifies
	--------
	- All methods return dictionaries with correct structure
	- Return types match the declared TypedDict definitions
	- All required keys are present with correct types

	Parameters
	----------
	dataframe_validator : DataFrameValidator
		DataFrameValidator instance from fixture
	range_constraints : dict[str, tuple[Number, Number]]
		Range constraints from fixture
	categorical_constraints : tuple[str, list[str]]
		Categorical constraints from fixture

	Returns
	-------
	None
	"""
	col, allowed_values = categorical_constraints

	# Test validate_ranges return type
	range_result = dataframe_validator.validate_ranges(range_constraints)
	assert isinstance(range_result, dict)
	assert "valid" in range_result
	assert "out_of_bounds" in range_result
	assert isinstance(range_result["valid"], bool)
	assert isinstance(range_result["out_of_bounds"], pd.DataFrame)

	# Test validate_dates return type
	date_result = dataframe_validator.validate_dates("date_start", "date_end")
	assert isinstance(date_result, dict)
	assert "valid" in date_result
	assert "invalid_dates" in date_result
	assert isinstance(date_result["valid"], bool)
	assert isinstance(date_result["invalid_dates"], pd.DataFrame)

	# Test validate_categorical_values return type
	cat_result = dataframe_validator.validate_categorical_values(col, allowed_values)
	assert isinstance(cat_result, dict)
	assert "valid" in cat_result
	assert "invalid_values" in cat_result
	assert isinstance(cat_result["valid"], bool)
	assert isinstance(cat_result["invalid_values"], pd.DataFrame)

	# Test pipeline return type
	pipeline_result = dataframe_validator.pipeline(
		dict_rng_constraints=range_constraints,
		col_start="date_start",
		col_sup="date_end",
		list_tup_categorical_constraints=(col, allowed_values),
	)
	assert isinstance(pipeline_result, dict)
	assert "valid" in pipeline_result
	assert "df_" in pipeline_result
	assert isinstance(pipeline_result["valid"], bool)
	assert isinstance(pipeline_result["df_"], pd.DataFrame)
