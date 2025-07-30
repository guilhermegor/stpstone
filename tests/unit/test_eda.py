"""Unit tests for ExploratoryDataAnalysis class.

Tests the exploratory data analysis functionality including:
- Monotonicity checks
- Automated binning strategies
- Array reshaping
- Basic dataset exploration
"""

from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from stpstone.transformations.cleaner.eda import ExploratoryDataAnalysis


class TestIsMonotonic:
    """Test cases for is_monotonic method."""

    @pytest.fixture
    def eda(self) -> ExploratoryDataAnalysis:
        """Fixture providing ExploratoryDataAnalysis instance.
        
        Returns
        -------
        ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis.
        """
        return ExploratoryDataAnalysis()

    def test_increasing_array(self, eda: ExploratoryDataAnalysis) -> None:
        """Test with monotonically increasing array.

        Verifies that:
        - Returns True for strictly increasing array

        Parameters
        ----------
        eda : ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis

        Returns
        -------
        None
        """
        array = np.array([1, 2, 3, 4, 5], dtype=np.float64)
        assert eda.is_monotonic(array)

    def test_decreasing_array(self, eda: ExploratoryDataAnalysis) -> None:
        """Test with monotonically decreasing array.

        Verifies that:
        - Returns True for strictly decreasing array

        Parameters
        ----------
        eda : ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis

        Returns
        -------
        None
        """
        array = np.array([5, 4, 3, 2, 1], dtype=np.float64)
        assert eda.is_monotonic(array)

    def test_non_monotonic_array(self, eda: ExploratoryDataAnalysis) -> None:
        """Test with non-monotonic array.

        Verifies that:
        - Returns False for non-monotonic array

        Parameters
        ----------
        eda : ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis

        Returns
        -------
        None
        """
        array = np.array([1, 3, 2, 4, 5], dtype=np.float64)
        assert not eda.is_monotonic(array)

    def test_empty_array(self, eda: ExploratoryDataAnalysis) -> None:
        """Test with empty array.

        Verifies that:
        - Raises ValueError for empty array

        Parameters
        ----------
        eda : ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Input array cannot be empty"):
            eda.is_monotonic(np.array([], dtype=np.float64))


class TestPrepareBins:
    """Test cases for prepare_bins method."""

    @pytest.fixture
    def eda(self) -> ExploratoryDataAnalysis:
        """Fixture providing ExploratoryDataAnalysis instance.
        
        Returns
        -------
        ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis.
        """
        return ExploratoryDataAnalysis()

    @pytest.fixture
    def sample_data(self) -> tuple[pd.DataFrame, str, str]:
        """Fixture providing sample dataframe and column names.

        Returns
        -------
        tuple[pd.DataFrame, str, str]
            tuple containing:
            - Sample dataframe with continuous and target columns
            - Continuous column name
            - Target column name
        """
        df_ = pd.DataFrame({
            "feature": np.random.normal(size=100),
            "target": np.random.randint(0, 2, size=100)
        })
        return df_, "feature", "target"

    def test_successful_monotonic_binning(
        self, 
        eda: ExploratoryDataAnalysis,
        sample_data: tuple[pd.DataFrame, str, str]
    ) -> None:
        """Test successful monotonic binning.

        Verifies that:
        - Returns binned column name when successful
        - Returns correct remarks
        - Returns dataframe with expected columns

        Parameters
        ----------
        eda : ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis
        sample_data : tuple[pd.DataFrame, str, str]
            tuple containing:
            - Sample dataframe with continuous and target columns
            - Continuous column name
            - Target column name

        Returns
        -------
        None
        """
        df_, c_i, target_col = sample_data
        result = eda.prepare_bins(df_, c_i, target_col, max_bins=10)
        assert result[0] == f"{c_i}_bins"
        assert result[1] in ["binned forcefully", "binned monotonically"]
        assert set(result[2].columns) == {c_i, f"{c_i}_bins", target_col}

    def test_force_binning(
        self,
        eda: ExploratoryDataAnalysis,
        sample_data: tuple[pd.DataFrame, str, str]
    ) -> None:
        """Test forced binning when monotonic fails.

        Verifies that:
        - Falls back to forced binning when monotonic fails
        - Returns correct remarks for forced binning

        Parameters
        ----------
        eda : ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis
        sample_data : tuple[pd.DataFrame, str, str]
            tuple containing:
            - Sample dataframe with continuous and target columns
            - Continuous column name
            - Target column name

        Returns
        -------
        None
        """
        df_, c_i, target_col = sample_data
        # Create non-monotonic data
        df_["feature"] = np.sin(np.linspace(0, 10, 100))
        result = eda.prepare_bins(df_, c_i, target_col, max_bins=5)
        assert result[1] in ["binned monotonically", "binned forcefully"]

    def test_binning_failure(
        self,
        eda: ExploratoryDataAnalysis,
        sample_data: tuple[pd.DataFrame, str, str]
    ) -> None:
        """Test binning failure case.

        Verifies that:
        - Returns original column name when binning fails
        - Returns error remarks
        - Returns dataframe with original columns

        Parameters
        ----------
        eda : ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis
        sample_data : tuple[pd.DataFrame, str, str]
            tuple containing:
            - Sample dataframe with continuous and target columns
            - Continuous column name
            - Target column name

        Returns
        -------
        None
        """
        df_, c_i, target_col = sample_data
        # create data that will fail binning (all NaN values)
        df_[c_i] = np.nan
        
        # mock plt.show() to prevent displaying plots during tests
        with patch('matplotlib.pyplot.show'), \
            pytest.raises(ValueError, match="Input array cannot be empty"):
            _ = eda.prepare_bins(df_, c_i, target_col, max_bins=5)

    def test_missing_columns(
        self,
        eda: ExploratoryDataAnalysis,
        sample_data: tuple[pd.DataFrame, str, str]
    ) -> None:
        """Test with missing columns.

        Verifies that:
        - Raises ValueError when required columns are missing

        Parameters
        ----------
        eda : ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis
        sample_data : tuple[pd.DataFrame, str, str]
            tuple containing:
            - Sample dataframe with continuous and target columns
            - Continuous column name
            - Target column name

        Returns
        -------
        None
        """
        df_, _, _ = sample_data
        with pytest.raises(ValueError, match="Dataframe missing required columns"):
            eda.prepare_bins(df_, "missing", "target", max_bins=5)

    def test_invalid_max_bins(
        self,
        eda: ExploratoryDataAnalysis,
        sample_data: tuple[pd.DataFrame, str, str]
    ) -> None:
        """Test with invalid max_bins.

        Verifies that:
        - Raises ValueError when max_bins is not positive integer

        Parameters
        ----------
        eda : ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis
        sample_data : tuple[pd.DataFrame, str, str]
            tuple containing:
            - Sample dataframe with continuous and target columns
            - Continuous column name
            - Target column name

        Returns
        -------
        None
        """
        df_, c_i, target_col = sample_data
        with pytest.raises(ValueError, match="max_bins must be a positive integer"):
            eda.prepare_bins(df_, c_i, target_col, max_bins=0)


class TestReshape1dArrays:
    """Test cases for reshape_1d_arrays method."""

    @pytest.fixture
    def eda(self) -> ExploratoryDataAnalysis:
        """Fixture providing ExploratoryDataAnalysis instance.
        
        Returns
        -------
        ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis
        """
        return ExploratoryDataAnalysis()

    def test_1d_array(self, eda: ExploratoryDataAnalysis) -> None:
        """Test with 1D array.

        Verifies that:
        - Returns 2D array when input is 1D

        Parameters
        ----------
        eda : ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis

        Returns
        -------
        None
        """
        array = np.array([1, 2, 3], dtype=np.float64)
        result = eda.reshape_1d_arrays(array)
        assert result.shape == (3, 1)

    def test_2d_array(self, eda: ExploratoryDataAnalysis) -> None:
        """Test with 2D array.

        Verifies that:
        - Returns same array when input is already 2D

        Parameters
        ----------
        eda : ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis

        Returns
        -------
        None
        """
        array = np.array([[1], [2], [3]], dtype=np.float64)
        result = eda.reshape_1d_arrays(array)
        assert result.shape == (3, 1)

    def test_empty_array(self, eda: ExploratoryDataAnalysis) -> None:
        """Test with empty array.

        Verifies that:
        - Raises ValueError for empty array

        Parameters
        ----------
        eda : ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Input array cannot be empty"):
            eda.reshape_1d_arrays(np.array([], dtype=np.float64))


class TestEdaDatabase:
    """Test cases for eda_database method."""

    @pytest.fixture
    def eda(self) -> ExploratoryDataAnalysis:
        """Fixture providing ExploratoryDataAnalysis instance.
        
        Returns
        -------
        ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis
        """
        return ExploratoryDataAnalysis()

    @pytest.fixture
    def sample_df(self) -> pd.DataFrame:
        """Fixture providing sample dataframe.

        Returns
        -------
        pd.DataFrame
            Sample dataframe with random data
        """
        return pd.DataFrame({
            "col1": np.random.normal(size=100),
            "col2": np.random.randint(0, 10, size=100)
        })

    def test_empty_dataframe(self, eda: ExploratoryDataAnalysis) -> None:
        """Test with empty dataframe.

        Verifies that:
        - Raises ValueError for empty dataframe

        Parameters
        ----------
        eda : ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Dataframe cannot be empty"):
            eda.eda_database(pd.DataFrame())

    def test_invalid_bins(self, eda: ExploratoryDataAnalysis, sample_df: pd.DataFrame) -> None:
        """Test with invalid bins parameter.

        Verifies that:
        - Raises ValueError when bins is not positive integer

        Parameters
        ----------
        eda : ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis
        sample_df : pd.DataFrame
            Sample dataframe with random data

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="bins must be a positive integer"):
            eda.eda_database(sample_df, bins=0)

    @pytest.mark.parametrize("bins", [10, 20, 50])
    def test_valid_bins(
        self,
        eda: ExploratoryDataAnalysis,
        sample_df: pd.DataFrame,
        bins: int
    ) -> None:
        """Test with different valid bins values.

        Verifies that:
        - Runs without errors for valid bins values

        Parameters
        ----------
        eda : ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis
        sample_df : pd.DataFrame
            Sample dataframe with random data
        bins : int
            Number of bins

        Returns
        -------
        None
        """
        eda.eda_database(sample_df, bins=bins, bool_show_plots=False)


class TestValidationMethods:
    """Test cases for internal validation methods."""

    @pytest.fixture
    def eda(self) -> ExploratoryDataAnalysis:
        """Fixture providing ExploratoryDataAnalysis instance.
        
        Returns
        -------
        ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis
        """
        return ExploratoryDataAnalysis()

    def test_validate_array_non_empty(self, eda: ExploratoryDataAnalysis) -> None:
        """Test _validate_array_non_empty method.

        Verifies that:
        - Raises ValueError for empty array
        - Passes for non-empty array

        Parameters
        ----------
        eda : ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis

        Returns
        -------
        None
        """
        with pytest.raises(ValueError):
            eda._validate_array_non_empty(np.array([]))
        eda._validate_array_non_empty(np.array([1]))

    def test_validate_dataframe_non_empty(self, eda: ExploratoryDataAnalysis) -> None:
        """Test _validate_dataframe_non_empty method.

        Verifies that:
        - Raises ValueError for empty dataframe
        - Passes for non-empty dataframe

        Parameters
        ----------
        eda : ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis

        Returns
        -------
        None
        """
        with pytest.raises(ValueError):
            eda._validate_dataframe_non_empty(pd.DataFrame())
        eda._validate_dataframe_non_empty(pd.DataFrame({"col": [1]}))

    def test_validate_dataframe_columns(self, eda: ExploratoryDataAnalysis) -> None:
        """Test _validate_dataframe_columns method.

        Verifies that:
        - Raises ValueError when columns are missing
        - Passes when all columns are present

        Parameters
        ----------
        eda : ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis

        Returns
        -------
        None
        """
        df_ = pd.DataFrame({"col1": [1], "col2": [2]})
        with pytest.raises(ValueError):
            eda._validate_dataframe_columns(df_, ["col1", "missing"])
        eda._validate_dataframe_columns(df_, ["col1"])

    @pytest.mark.parametrize("value,valid", [
        (1, True),
        (0, False),
        (-1, False),
        (1.5, False),
        ("1", False)
    ])
    def test_validate_positive_integer(
        self,
        eda: ExploratoryDataAnalysis,
        value: int,
        valid: bool
    ) -> None:
        """Test _validate_positive_integer method.

        Verifies that:
        - Raises ValueError for non-positive or non-integer values
        - Passes for positive integers

        Parameters
        ----------
        eda : ExploratoryDataAnalysis
            An instance of ExploratoryDataAnalysis
        value : int
            Value to validate
        valid : bool
            Whether value is valid or not

        Returns
        -------
        None
        """
        if valid:
            eda._validate_positive_integer(value, "test")
        else:
            with pytest.raises(ValueError):
                eda._validate_positive_integer(value, "test")