"""Unit tests for DimensionalityReduction class.

Tests the feature selection and dimensionality reduction functionality including:
- Backward and forward feature elimination
- Exhaustive feature selection
- PCA analysis
- IV/WOE calculations
- Input validation and error handling
"""

from unittest.mock import MagicMock, patch

import numpy as np
from numpy.typing import NDArray
import pandas as pd
import pytest

from stpstone.transformations.cleaner.features_selection import DimensionalityReduction


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def dimensionality_reduction() -> DimensionalityReduction:
    """Fixture providing a DimensionalityReduction instance.

    Returns
    -------
    DimensionalityReduction
        Clean instance for each test
    """
    return DimensionalityReduction()


@pytest.fixture
def sample_data() -> tuple[NDArray[np.float64], NDArray[np.float64]]:
    """Fixture providing sample data for feature selection tests.

    Returns
    -------
    tuple[NDArray[np.float64], NDArray[np.float64]]
        Tuple of (X, y) arrays for testing
    """
    X = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]], dtype=np.float64)
    y = np.array([0, 1, 0], dtype=np.float64)
    return X, y


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Fixture providing sample dataframe for IV/WOE tests.

    Returns
    -------
    pd.DataFrame
        DataFrame with numeric and categorical columns
    """
    return pd.DataFrame({
        "numeric": [1.0, 2.0, 3.0, 4.0, 5.0, np.nan],
        "categorical": ["A", "B", "A", "B", "C", "A"],
        "target": [0, 1, 0, 1, 0, 1]
    })


# --------------------------
# Test Validation Methods
# --------------------------
class TestValidationMethods:
    """Tests for private validation methods."""

    def test_validate_array_valid(self, dimensionality_reduction: DimensionalityReduction) -> None:
        """Test _validate_array with valid input.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
            
        Returns
        -------
        None
        """
        arr = np.array([1, 2, 3])
        dimensionality_reduction._validate_array(arr, "test_array")

    def test_validate_array_invalid_type(
        self, 
        dimensionality_reduction: DimensionalityReduction
    ) -> None:
        """Test _validate_array raises TypeError for non-array input.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
            
        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="test_array must be a numpy array"):
            dimensionality_reduction._validate_array([1, 2, 3], "test_array")

    def test_validate_array_empty(
        self, 
        dimensionality_reduction: DimensionalityReduction
    ) -> None:
        """Test _validate_array raises ValueError for empty array.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
            
        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="test_array cannot be empty"):
            dimensionality_reduction._validate_array(np.array([]), "test_array")

    def test_validate_array_non_finite(
        self, 
        dimensionality_reduction: DimensionalityReduction
    ) -> None:
        """Test _validate_array raises ValueError for non-finite values.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
            
        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="test_array contains NaN or infinite values"):
            dimensionality_reduction._validate_array(np.array([1, np.nan, 3]), "test_array")

    def test_validate_estimator_valid(
        self, 
        dimensionality_reduction: DimensionalityReduction
    ) -> None:
        """Test _validate_estimator with valid inputs.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
            
        Returns
        -------
        None
        """
        dimensionality_reduction._validate_estimator("linear_regression")
        dimensionality_reduction._validate_estimator("rf_classifier")

    def test_validate_estimator_invalid(
        self, 
        dimensionality_reduction: DimensionalityReduction
    ) -> None:
        """Test _validate_estimator raises ValueError for invalid estimator.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
            
        Returns
        -------
        None
        """
        with pytest.raises(ValueError):
            dimensionality_reduction._validate_estimator("invalid_estimator")

    def test_validate_cv_valid(
        self, 
        dimensionality_reduction: DimensionalityReduction
    ) -> None:
        """Test _validate_cv with valid inputs.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
            
        Returns
        -------
        None
        """
        dimensionality_reduction._validate_cv(1)
        dimensionality_reduction._validate_cv(5)

    def test_validate_cv_invalid(
        self, 
        dimensionality_reduction: DimensionalityReduction
    ) -> None:
        """Test _validate_cv raises ValueError for invalid cv.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
            
        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Cross-validation folds must be positive"):
            dimensionality_reduction._validate_cv(0)
        with pytest.raises(ValueError, match="Cross-validation folds must be positive"):
            dimensionality_reduction._validate_cv(-1)

    def test_validate_verbose_valid(
        self, 
        dimensionality_reduction: DimensionalityReduction
    ) -> None:
        """Test _validate_verbose with valid inputs.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
            
        Returns
        -------
        None
        """
        dimensionality_reduction._validate_verbose(0)
        dimensionality_reduction._validate_verbose(1)

    def test_validate_verbose_invalid(
        self, 
        dimensionality_reduction: DimensionalityReduction
    ) -> None:
        """Test _validate_verbose raises ValueError for invalid verbose.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
            
        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Verbosity level cannot be negative"):
            dimensionality_reduction._validate_verbose(-1)

    def test_validate_dataframe_valid(
        self, 
        dimensionality_reduction: DimensionalityReduction, 
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test _validate_dataframe with valid input.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing
            
        Returns
        -------
        None
        """
        dimensionality_reduction._validate_dataframe(sample_dataframe, "test_df")

    def test_validate_dataframe_invalid_type(
        self, 
        dimensionality_reduction: DimensionalityReduction
    ) -> None:
        """Test _validate_dataframe raises TypeError for non-DataFrame.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
            
        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="test_df must be a pandas DataFrame"):
            dimensionality_reduction._validate_dataframe([1, 2, 3], "test_df")

    def test_validate_dataframe_empty(
        self, 
        dimensionality_reduction: DimensionalityReduction
    ) -> None:
        """Test _validate_dataframe raises ValueError for empty DataFrame.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
            
        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="test_df cannot be empty"):
            dimensionality_reduction._validate_dataframe(pd.DataFrame(), "test_df")

    def test_validate_column_present(
        self, 
        dimensionality_reduction: DimensionalityReduction, 
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test _validate_column with existing column.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing
            
        Returns
        -------
        None
        """
        dimensionality_reduction._validate_column(sample_dataframe, "numeric", "test_df")

    def test_validate_column_missing(
        self, 
        dimensionality_reduction: DimensionalityReduction, 
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test _validate_column raises ValueError for missing column.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing
            
        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Column missing_col not found in test_df"):
            dimensionality_reduction._validate_column(sample_dataframe, "missing_col", "test_df")

    def test_validate_bins_valid(
        self, 
        dimensionality_reduction: DimensionalityReduction
    ) -> None:
        """Test _validate_bins with valid inputs.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
            
        Returns
        -------
        None
        """
        dimensionality_reduction._validate_bins(1)
        dimensionality_reduction._validate_bins(10)

    def test_validate_bins_invalid(
        self, 
        dimensionality_reduction: DimensionalityReduction
    ) -> None:
        """Test _validate_bins raises ValueError for invalid bins.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
            
        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Number of bins must be positive"):
            dimensionality_reduction._validate_bins(0)
        with pytest.raises(ValueError, match="Number of bins must be positive"):
            dimensionality_reduction._validate_bins(-1)


# --------------------------
# Test Feature Selection Methods
# --------------------------
class TestFeatureSelectionMethods:
    """Tests for feature selection methods."""

    @patch("mlxtend.feature_selection.SequentialFeatureSelector.fit")
    def test_backward_elimination_success(
        self, 
        mock_fit: MagicMock, 
        dimensionality_reduction: DimensionalityReduction,
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]]
    ) -> None:
        """Test backward_elimination with valid inputs.
        
        Parameters
        ----------
        mock_fit : MagicMock
            Mock object for SequentialFeatureSelector.fit
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Sample data for testing
            
        Returns
        -------
        None
        """
        X, y = sample_data
        mock_fit.return_value.k_feature_names_ = ("0", "1")
        mock_fit.return_value.k_feature_idx_ = (0, 1)
        mock_fit.return_value.k_score_ = 0.95

        result = dimensionality_reduction.backward_elimination(X, y)
        
        assert isinstance(result, dict)
        assert set(result.keys()) == {"feature_names", "feature_indexes", "score"}
        assert isinstance(result["feature_names"], tuple)
        assert isinstance(result["feature_indexes"], tuple)
        assert isinstance(result["score"], float)

    def test_backward_elimination_invalid_inputs(
        self, 
        dimensionality_reduction: DimensionalityReduction,
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]]
    ) -> None:
        """Test backward_elimination with various invalid inputs.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Sample data for testing
            
        Returns
        -------
        None
        """
        X, y = sample_data
        
        # invalid array types
        with pytest.raises(TypeError):
            dimensionality_reduction.backward_elimination([1, 2, 3], y)
        with pytest.raises(TypeError):
            dimensionality_reduction.backward_elimination(X, [0, 1, 0])
            
        # empty arrays
        with pytest.raises(ValueError):
            dimensionality_reduction.backward_elimination(np.array([]), y)
        with pytest.raises(ValueError):
            dimensionality_reduction.backward_elimination(X, np.array([]))
            
        # invalid estimator
        with pytest.raises(ValueError):
            dimensionality_reduction.backward_elimination(X, y, str_estimator="invalid")
            
        # invalid cv
        with pytest.raises(ValueError):
            dimensionality_reduction.backward_elimination(X, y, int_cv=0)
            
        # invalid verbose
        with pytest.raises(ValueError):
            dimensionality_reduction.backward_elimination(X, y, int_verbose=-1)

    @patch("mlxtend.feature_selection.SequentialFeatureSelector.fit")
    def test_forward_elimination_success(
        self, 
        mock_fit: MagicMock, 
        dimensionality_reduction: DimensionalityReduction,
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]]
    ) -> None:
        """Test forward_elimination with valid inputs.
        
        Parameters
        ----------
        mock_fit : MagicMock
            Mock object for SequentialFeatureSelector.fit
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Sample data for testing
            
        Returns
        -------
        None
        """
        X, y = sample_data
        mock_fit.return_value.k_feature_names_ = ("0", "2")
        mock_fit.return_value.k_feature_idx_ = (0, 2)
        mock_fit.return_value.k_score_ = 0.92

        result = dimensionality_reduction.forward_elimination(X, y)
        
        assert isinstance(result, dict)
        assert set(result.keys()) == {"feature_names", "feature_indexes", "score"}
        assert isinstance(result["feature_names"], tuple)
        assert isinstance(result["feature_indexes"], tuple)
        assert isinstance(result["score"], float)

    @patch("mlxtend.feature_selection.ExhaustiveFeatureSelector.fit")
    def test_exhaustive_feature_selection_success(
        self, 
        mock_fit: MagicMock, 
        dimensionality_reduction: DimensionalityReduction,
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]]
    ) -> None:
        """Test exhaustive_feature_selection with valid inputs.
        
        Parameters
        ----------
        mock_fit : MagicMock
            Mock object for ExhaustiveFeatureSelector.fit
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Sample data for testing
            
        Returns
        -------
        None
        """
        X, y = sample_data
        mock_fit.return_value.best_feature_names_ = ("0", "2")
        mock_fit.return_value.best_idx_ = (0, 2)
        mock_fit.return_value.best_score_ = 0.98

        result = dimensionality_reduction.exhaustive_feature_selection(X, y)
        
        assert isinstance(result, dict)
        assert set(result.keys()) == {"feature_names", "feature_indexes", "score"}
        assert isinstance(result["feature_names"], tuple)
        assert isinstance(result["feature_indexes"], tuple)
        assert isinstance(result["score"], float)

    def test_exhaustive_feature_selection_invalid_max_features(
        self, 
        dimensionality_reduction: DimensionalityReduction,
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]]
    ) -> None:
        """Test exhaustive_feature_selection with invalid max_features.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Sample data for testing
            
        Returns
        -------
        None
        """
        X, y = sample_data
        with pytest.raises(ValueError, match="max_features must be positive or None"):
            dimensionality_reduction.exhaustive_feature_selection(X, y, max_features=0)


# --------------------------
# Test IV/WOE Methods
# --------------------------
class TestIvWoeMethods:
    """Tests for IV/WOE calculation methods."""

    def test_iv_woe_iter_valid(
        self, 
        dimensionality_reduction: DimensionalityReduction,
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test iv_woe_iter with valid inputs.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing
            
        Returns
        -------
        None
        """
        df_ = sample_dataframe
        result = dimensionality_reduction.iv_woe_iter(df_, "target", "categorical")
        
        assert isinstance(result, pd.DataFrame)
        expected_cols = [
            "feature", "sample_class", "sample_class_label", "sample_count",
            "min_value", "max_value", "non_event_count", "non_event_rate",
            "event_count", "pct_non_event", "pct_event", "woe", "iv"
        ]
        assert all(col in result.columns for col in expected_cols)

    def test_iv_woe_iter_invalid_inputs(
        self, 
        dimensionality_reduction: DimensionalityReduction,
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test iv_woe_iter with various invalid inputs.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing
            
        Returns
        -------
        None
        """
        df_ = sample_dataframe
        
        # invalid dataframe type
        with pytest.raises(TypeError):
            dimensionality_reduction.iv_woe_iter([], "target", "categorical")
            
        # empty dataframe
        with pytest.raises(ValueError):
            dimensionality_reduction.iv_woe_iter(pd.DataFrame(), "target", "categorical")
            
        # missing columns
        with pytest.raises(ValueError):
            dimensionality_reduction.iv_woe_iter(df_, "missing_target", "categorical")
        with pytest.raises(ValueError):
            dimensionality_reduction.iv_woe_iter(df_, "target", "missing_column")

    def test_var_iter_valid(
        self, 
        dimensionality_reduction: DimensionalityReduction,
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test var_iter with valid inputs.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing
            
        Returns
        -------
        None
        """
        df_ = sample_dataframe
        woe_iv, binning_remarks = dimensionality_reduction.var_iter(df_, "target", 3)
        
        assert isinstance(woe_iv, pd.DataFrame)
        assert isinstance(binning_remarks, pd.DataFrame)
        assert "feature" in woe_iv.columns
        assert "feature" in binning_remarks.columns
        assert "remarks" in binning_remarks.columns

    def test_var_iter_invalid_bins(
        self, 
        dimensionality_reduction: DimensionalityReduction,
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test var_iter with invalid bins.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing
            
        Returns
        -------
        None
        """
        df_ = sample_dataframe
        with pytest.raises(ValueError, match="min_bins_monotonic must be positive"):
            dimensionality_reduction.var_iter(df_, "target", 3, min_bins_monotonic=0)

    def test_get_iv_woe_valid(
        self, 
        dimensionality_reduction: DimensionalityReduction,
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test get_iv_woe with valid inputs.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing
            
        Returns
        -------
        None
        """
        df_ = sample_dataframe
        df_iv, df_woe_iv = dimensionality_reduction.get_iv_woe(df_, "target", 3)
        
        assert isinstance(df_iv, pd.DataFrame)
        assert isinstance(df_woe_iv, pd.DataFrame)
        assert "iv" in df_iv.columns
        assert "feature" in df_woe_iv.columns

    def test_iv_label_predictive_power(
        self, 
        dimensionality_reduction: DimensionalityReduction
    ) -> None:
        """Test iv_label_predictive_power with various IV values.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
            
        Returns
        -------
        None
        """
        df_ = pd.DataFrame({
            "feature": ["A", "B", "C", "D", "E"],
            "iv": [0.01, 0.05, 0.2, 0.4, 0.6]
        })
        
        result = dimensionality_reduction.iv_label_predictive_power(df_)
        
        assert "predictive_power" in result.columns
        powers = result["predictive_power"].tolist()
        assert "not useful" in powers
        assert "weak predictor" in powers
        assert "medium predictor" in powers
        assert "strong predictor" in powers
        assert "suspicious" in powers


# --------------------------
# Test PCA Method
# --------------------------
class TestPcaMethod:
    """Tests for PCA method."""

    def test_pca_success(
        self, 
        dimensionality_reduction: DimensionalityReduction,
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]]
    ) -> None:
        """Test pca with valid inputs.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Sample data for testing
            
        Returns
        -------
        None
        """
        X, _ = sample_data
        result = dimensionality_reduction.pca(X)
        
        assert isinstance(result, dict)
        assert set(result.keys()) == {
            "eigenvalues", "eigenvectors", "explained_variance_ratio", "components"
        }
        assert isinstance(result["eigenvalues"], np.ndarray)
        assert isinstance(result["eigenvectors"], np.ndarray)
        assert isinstance(result["explained_variance_ratio"], np.ndarray)
        assert isinstance(result["components"], np.ndarray)

    def test_pca_invalid_input(
        self, 
        dimensionality_reduction: DimensionalityReduction
    ) -> None:
        """Test pca with invalid inputs.
        
        Parameters
        ----------
        dimensionality_reduction : DimensionalityReduction
            Clean instance for each test
            
        Returns
        -------
        None
        """
        # invalid type
        with pytest.raises(TypeError):
            dimensionality_reduction.pca([[1, 2], [3, 4]])
            
        # empty array
        with pytest.raises(ValueError):
            dimensionality_reduction.pca(np.array([]))
            
        # non-finite values
        with pytest.raises(ValueError):
            dimensionality_reduction.pca(np.array([1, np.nan, 3]))