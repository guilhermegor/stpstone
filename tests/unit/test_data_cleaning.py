"""Unit tests for DataCleaning class.

Tests the data cleaning and preparation functionality including:
- Train/test splitting methods (random, stratified, hash-based)
- Missing value imputation
- Categorical encoding
- Feature scaling
- Data augmentation
- Input validation
- Error conditions
"""

import numpy as np
from numpy.typing import NDArray
import pandas as pd
import pytest

from stpstone.transformations.cleaner.data_cleaning import DataCleaning


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def data_cleaner() -> DataCleaning:
    """Fixture providing DataCleaning instance.

    Returns
    -------
    DataCleaning
        Instance of DataCleaning class
    """
    return DataCleaning()


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Fixture providing sample dataframe for testing.

    Returns
    -------
    pd.DataFrame
        Dataframe with 100 rows and 3 columns (id, feature, category)
    """
    np.random.seed(42)
    return pd.DataFrame({
        "id": range(100),
        "feature": np.random.rand(100),
        "category": np.random.choice(["A", "B", "C"], size=100)
    })


@pytest.fixture
def sample_array_with_nans() -> NDArray[np.float64]:
    """Fixture providing sample array with NaN values.

    Returns
    -------
    NDArray[np.float64]
        2D array with shape (10, 3) containing NaN values
    """
    arr = np.random.rand(10, 3)
    arr[[0, 3, 7], [0, 1, 2]] = np.nan
    return arr


@pytest.fixture
def sample_categorical_data() -> NDArray[np.str_]:
    """Fixture providing sample categorical data.

    Returns
    -------
    NDArray[np.str_]
        2D array with shape (10, 2) containing string categories
    """
    return np.array([
        ["A", "X"],
        ["B", "Y"],
        ["A", "Z"],
        ["C", "X"],
        ["B", "Y"],
        ["A", "Z"],
        ["C", "X"],
        ["B", "Y"],
        ["A", "Z"],
        ["C", "X"]
    ])


@pytest.fixture
def sample_image_data() -> NDArray[np.float64]:
    """Fixture providing sample image data.

    Returns
    -------
    NDArray[np.float64]
        1D array representing a 28x28 image (784 elements)
    """
    return np.random.rand(784)


# --------------------------
# Test Cases
# --------------------------
class TestTestSetCheckHash:
    """Tests for test_set_check_hash method."""

    def test_returns_boolean(self, data_cleaner: DataCleaning) -> None:
        """Test returns boolean value.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class

        Returns
        -------
        None
        """
        result = data_cleaner.test_set_check_hash(123, 0.2)
        assert isinstance(result, bool)

    def test_invalid_test_ratio_raises_error(
        self, 
        data_cleaner: DataCleaning
    ) -> None:
        """Test invalid test ratio raises ValueError.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="test_ratio must be between 0 and 1"):
            data_cleaner.test_set_check_hash(123, -0.1)
        with pytest.raises(ValueError, match="test_ratio must be between 0 and 1"):
            data_cleaner.test_set_check_hash(123, 1.1)


class TestSplitTrainTest:
    """Tests for split_train_test method."""

    def test_returns_two_dataframes(
        self, 
        data_cleaner: DataCleaning, 
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test returns tuple of two dataframes.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing

        Returns
        -------
        None
        """
        train, test = data_cleaner.split_train_test(sample_dataframe)
        assert isinstance(train, pd.DataFrame)
        assert isinstance(test, pd.DataFrame)

    def test_proper_split_ratio(
        self, 
        data_cleaner: DataCleaning, 
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test split maintains requested ratio.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing

        Returns
        -------
        None
        """
        train, test = data_cleaner.split_train_test(sample_dataframe, test_ratio=0.3)
        assert len(test) / len(sample_dataframe) == pytest.approx(0.3, abs=0.01)

    def test_stratified_split(
        self, 
        data_cleaner: DataCleaning, 
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test stratified split maintains proportions.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing

        Returns
        -------
        None
        """
        train, test = data_cleaner.split_train_test(
            sample_dataframe, 
            stratify_col="category"
        )
        orig_props = sample_dataframe["category"].value_counts(normalize=True)
        test_props = test["category"].value_counts(normalize=True)
        assert (orig_props - test_props).abs().max() < 0.05


class TestSplitStratifiedTrainTest:
    """Tests for split_stratified_train_test method."""

    def test_returns_two_dataframes(
        self, 
        data_cleaner: DataCleaning, 
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test returns tuple of two dataframes.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing

        Returns
        -------
        None
        """
        train, test = data_cleaner.split_stratified_train_test(
            sample_dataframe, 
            "category"
        )
        assert isinstance(train, pd.DataFrame)
        assert isinstance(test, pd.DataFrame)

    def test_proper_stratification(
        self, 
        data_cleaner: DataCleaning, 
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test maintains category proportions.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing

        Returns
        -------
        None
        """
        train, test = data_cleaner.split_stratified_train_test(
            sample_dataframe, 
            "category"
        )
        orig_props = sample_dataframe["category"].value_counts(normalize=True)
        train_props = train["category"].value_counts(normalize=True)
        test_props = test["category"].value_counts(normalize=True)
        assert (orig_props - train_props).abs().max() < 0.05
        assert (orig_props - test_props).abs().max() < 0.05


class TestSplitTrainTestById:
    """Tests for split_train_test_by_id method."""

    def test_returns_two_dataframes(
        self, 
        data_cleaner: DataCleaning, 
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test returns tuple of two dataframes.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing

        Returns
        -------
        None
        """
        train, test = data_cleaner.split_train_test_by_id(
            sample_dataframe, 
            0.2, 
            "id"
        )
        assert isinstance(train, pd.DataFrame)
        assert isinstance(test, pd.DataFrame)

    def test_stable_split(
        self, 
        data_cleaner: DataCleaning, 
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test produces same split on repeated calls.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing

        Returns
        -------
        None
        """
        train1, test1 = data_cleaner.split_train_test_by_id(
            sample_dataframe, 
            0.2, 
            "id"
        )
        train2, test2 = data_cleaner.split_train_test_by_id(
            sample_dataframe, 
            0.2, 
            "id"
        )
        assert train1.equals(train2)
        assert test1.equals(test2)


class TestCreateCategoryStratifiedTrainTestSet:
    """Tests for create_category_stratified_train_test_set method."""

    def test_adds_category_column(
        self, 
        data_cleaner: DataCleaning, 
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test adds new category column.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing

        Returns
        -------
        None
        """
        result = data_cleaner.create_category_stratified_train_test_set(
            sample_dataframe,
            "feature",
            "new_category",
            [0, 0.33, 0.66, 1],
            ["low", "medium", "high"]
        )
        assert "new_category" in result.columns

    def test_proper_binning(
        self, 
        data_cleaner: DataCleaning, 
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test values are properly binned.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing

        Returns
        -------
        None
        """
        result = data_cleaner.create_category_stratified_train_test_set(
            sample_dataframe,
            "feature",
            "new_category",
            [0, 0.5, 1],
            ["low", "high"]
        )
        assert (result["new_category"] == "low").sum() > 0
        assert (result["new_category"] == "high").sum() > 0


class TestRemoveCategoryStratifiedTrainTestSet:
    """Tests for remove_category_stratified_train_test_set method."""

    def test_removes_category_column(
        self, 
        data_cleaner: DataCleaning, 
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test removes specified column from both dataframes.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing

        Returns
        -------
        None
        """
        df_with_cat = data_cleaner.create_category_stratified_train_test_set(
            sample_dataframe,
            "feature",
            "temp_category",
            [0, 0.5, 1],
            ["low", "high"]
        )
        train, test = data_cleaner.split_train_test(df_with_cat)
        train_out, test_out = data_cleaner.remove_category_stratified_train_test_set(
            train, test, "temp_category"
        )
        assert "temp_category" not in train_out.columns
        assert "temp_category" not in test_out.columns


class TestDataFrameIdColumnProportions:
    """Tests for dataframe_id_column_prportions method."""

    def test_returns_series(
        self, 
        data_cleaner: DataCleaning, 
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test returns pandas Series.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing

        Returns
        -------
        None
        """
        result = data_cleaner.dataframe_id_column_prportions(
            sample_dataframe, 
            "category"
        )
        assert isinstance(result, pd.Series)

    def test_proper_proportions(
        self, 
        data_cleaner: DataCleaning, 
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test proportions sum to 1.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing

        Returns
        -------
        None
        """
        result = data_cleaner.dataframe_id_column_prportions(
            sample_dataframe, 
            "category"
        )
        assert result.sum() == pytest.approx(1.0)


class TestCompareStratifiedRandomSamplesProportions:
    """Tests for compare_stratified_random_samples_propotions method."""

    def test_returns_dataframe(
        self, 
        data_cleaner: DataCleaning, 
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test returns pandas DataFrame.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing

        Returns
        -------
        None
        """
        df_with_cat = data_cleaner.create_category_stratified_train_test_set(
            sample_dataframe,
            "feature",
            "temp_category",
            [0, 0.5, 1],
            ["low", "high"]
        )
        strat_train, strat_test = data_cleaner.split_stratified_train_test(
            df_with_cat, "temp_category"
        )
        rand_train, rand_test = data_cleaner.split_train_test(df_with_cat)
        result = data_cleaner.compare_stratified_random_samples_propotions(
            df_with_cat, rand_test, strat_test, "temp_category"
        )
        assert isinstance(result, pd.DataFrame)

    def test_contains_error_columns(
        self, 
        data_cleaner: DataCleaning, 
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test result contains error percentage columns.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_dataframe : pd.DataFrame
            Sample dataframe for testing

        Returns
        -------
        None
        """
        df_with_cat = data_cleaner.create_category_stratified_train_test_set(
            sample_dataframe,
            "feature",
            "temp_category",
            [0, 0.5, 1],
            ["low", "high"]
        )
        strat_train, strat_test = data_cleaner.split_stratified_train_test(
            df_with_cat, "temp_category"
        )
        rand_train, rand_test = data_cleaner.split_train_test(df_with_cat)
        result = data_cleaner.compare_stratified_random_samples_propotions(
            df_with_cat, rand_test, strat_test, "temp_category"
        )
        assert "Rand. %error" in result.columns
        assert "Strat. %error" in result.columns


class TestReplaceNaNValues:
    """Tests for replace_nan_values method."""

    def test_returns_dict_with_arrays(
        self, 
        data_cleaner: DataCleaning, 
        sample_array_with_nans: NDArray[np.float64]
    ) -> None:
        """Test returns dictionary with numpy arrays.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_array_with_nans : NDArray[np.float64]
            Sample array for testing

        Returns
        -------
        None
        """
        result = data_cleaner.replace_nan_values(sample_array_with_nans, "mean")
        assert isinstance(result, dict)
        assert isinstance(result["array_after_adjustments"], np.ndarray)
        assert not np.isnan(result["array_after_adjustments"]).any()

    def test_knn_imputation(
        self, 
        data_cleaner: DataCleaning, 
        sample_array_with_nans: NDArray[np.float64]
    ) -> None:
        """Test KNN imputation works.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_array_with_nans : NDArray[np.float64]
            Sample array for testing

        Returns
        -------
        None
        """
        result = data_cleaner.replace_nan_values(
            sample_array_with_nans, 
            n_neighbors=3
        )
        assert not np.isnan(result["array_after_adjustments"]).any()

    def test_invalid_strategy_raises_error(
        self, 
        data_cleaner: DataCleaning, 
        sample_array_with_nans: NDArray[np.float64]
    ) -> None:
        """Test invalid imputation strategy raises error.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_array_with_nans : NDArray[np.float64]
            Sample array for testing

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="parameter of SimpleImputer must be a str among"):
            data_cleaner.replace_nan_values(
                sample_array_with_nans, 
                strategy="invalid"
            )


class TestConvertCategoriesFromStringsToArray:
    """Tests for convert_categories_from_strings_to_array method."""

    def test_one_hot_encoding(
        self, 
        data_cleaner: DataCleaning, 
        sample_categorical_data: NDArray[np.str_]
    ) -> None:
        """Test one-hot encoding produces numeric output.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_categorical_data : NDArray[np.str_]
            Sample array for testing

        Returns
        -------
        None
        """
        result = data_cleaner.convert_categories_from_strings_to_array(
            sample_categorical_data,
            [0, 1],
            "one_hot_encoder"
        )
        assert np.issubdtype(
            result["array_data_categorized_numbers"].dtype, 
            np.number
        )

    def test_ordinal_encoding(
        self, 
        data_cleaner: DataCleaning, 
        sample_categorical_data: NDArray[np.str_]
    ) -> None:
        """Test ordinal encoding produces numeric output.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_categorical_data : NDArray[np.str_]
            Sample array for testing

        Returns
        -------
        None
        """
        result = data_cleaner.convert_categories_from_strings_to_array(
            sample_categorical_data,
            [0, 1],
            "ordinal_encoder"
        )
        assert np.issubdtype(
            result["array_data_categorized_numbers"].dtype, 
            np.number
        )

    def test_invalid_encoder_raises_error(
        self, 
        data_cleaner: DataCleaning, 
        sample_categorical_data: NDArray[np.str_]
    ) -> None:
        """Test invalid encoder strategy raises error.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_categorical_data : NDArray[np.str_]
            Sample array for testing

        Returns
        -------
        None
        """
        with pytest.raises(ValueError):
            data_cleaner.convert_categories_from_strings_to_array(
                sample_categorical_data,
                [0, 1],
                "invalid_encoder"
            )


class TestFeatureScaling:
    """Tests for feature_scaling method."""

    def test_normalization(
        self, 
        data_cleaner: DataCleaning, 
        sample_array_with_nans: NDArray[np.float64]
    ) -> None:
        """Test normalization scales to [0,1] range.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_array_with_nans : NDArray[np.float64]
            Sample array for testing

        Returns
        -------
        None
        """
        arr = np.nan_to_num(sample_array_with_nans)
        result = data_cleaner.feature_scaling(arr, "normalization")
        scaled = result["array_scaled_data"]
        assert scaled.min() >= 0
        assert scaled.max() <= 1

    def test_standardization(
        self, 
        data_cleaner: DataCleaning, 
        sample_array_with_nans: NDArray[np.float64]
    ) -> None:
        """Test standardization produces mean~0, std~1.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_array_with_nans : NDArray[np.float64]
            Sample array for testing

        Returns
        -------
        None
        """
        arr = np.nan_to_num(sample_array_with_nans)
        result = data_cleaner.feature_scaling(arr, "standardisation")
        scaled = result["array_scaled_data"]
        assert scaled.mean(axis=0) == pytest.approx(0, abs=0.1)
        assert scaled.std(axis=0) == pytest.approx(1, abs=0.1)

    def test_invalid_scaler_raises_error(
        self, 
        data_cleaner: DataCleaning, 
        sample_array_with_nans: NDArray[np.float64]
    ) -> None:
        """Test invalid scaler type raises error.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_array_with_nans : NDArray[np.float64]
            Sample array for testing

        Returns
        -------
        None
        """
        arr = np.nan_to_num(sample_array_with_nans)
        with pytest.raises(ValueError):
            data_cleaner.feature_scaling(arr, "invalid_scaler")


class TestRemoveNoiseFromData:
    """Tests for remove_noise_from_data method."""

    def test_returns_dict_with_arrays(
        self, 
        data_cleaner: DataCleaning, 
        sample_array_with_nans: NDArray[np.float64]
    ) -> None:
        """Test returns dictionary with numpy arrays.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_array_with_nans : NDArray[np.float64]
            Sample array for testing

        Returns
        -------
        None
        """
        result = data_cleaner.replace_nan_values(sample_array_with_nans, "mean")
        assert isinstance(result, dict)
        assert isinstance(result["array_after_adjustments"], np.ndarray)
        assert not np.isnan(result["array_after_adjustments"]).any()

    def test_output_shapes(
        self, 
        data_cleaner: DataCleaning
    ) -> None:
        """Test output arrays have correct shapes.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class

        Returns
        -------
        None
        """
        train = np.random.rand(10, 784)
        test = np.random.rand(5, 784)
        result = data_cleaner.remove_noise_from_data(test, train)
        assert result["data_training_original"].shape == (10, 784)
        assert result["data_test_original"].shape == (5, 784)


class TestShiftImage:
    """Tests for shift_image method."""

    def test_returns_array(
        self, 
        data_cleaner: DataCleaning, 
        sample_image_data: NDArray[np.float64]
    ) -> None:
        """Test returns numpy array.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_image_data : NDArray[np.float64]
            Sample array for testing

        Returns
        -------
        None
        """
        result = data_cleaner.shift_image(sample_image_data, 1, 1)
        assert isinstance(result, np.ndarray)

    def test_output_shape(
        self, 
        data_cleaner: DataCleaning, 
        sample_image_data: NDArray[np.float64]
    ) -> None:
        """Test output has correct shape.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class
        sample_image_data : NDArray[np.float64]
            Sample array for testing

        Returns
        -------
        None
        """
        result = data_cleaner.shift_image(sample_image_data, 1, 1)
        assert result.shape == (784,)


class TestValidateTestRatio:
    """Tests for _validate_test_ratio method."""

    def test_valid_ratios(
        self, 
        data_cleaner: DataCleaning
    ) -> None:
        """Test valid ratios don't raise errors.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class

        Returns
        -------
        None
        """
        for ratio in [0, 0.5, 1]:
            data_cleaner._validate_test_ratio(ratio)

    def test_invalid_ratios_raise_error(
        self, 
        data_cleaner: DataCleaning
    ) -> None:
        """Test invalid ratios raise ValueError.
        
        Parameters
        ----------
        data_cleaner : DataCleaning
            Instance of DataCleaning class

        Returns
        -------
        None
        """
        with pytest.raises(ValueError):
            data_cleaner._validate_test_ratio(-0.1)
        with pytest.raises(ValueError):
            data_cleaner._validate_test_ratio(1.1)