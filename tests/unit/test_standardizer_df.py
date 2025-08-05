"""Unit tests for DataFrame standardization utilities.

Tests the functionality of DFStandardization and DFStandardizationML classes including:
- Initialization with various configurations
- Individual transformation methods
- Full pipeline execution
- Error handling and edge cases
"""

from typing import Any

import pandas as pd
import pytest

from stpstone.transformations.standardization.standardizer_df import (
    DFStandardization,
    DFStandardizationML,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_data() -> dict[str, Any]: # noqa: ANN401 - typing.Any is not allowed
    """Fixture providing sample data for testing.

    Returns
    -------
    dict[str, Any]
        Dictionary containing:
        - 'df_': Sample DataFrame with mixed data types
        - 'dtypes': Dictionary of column data types
        - 'dt_cols': List of datetime columns
    """
    data = {
        "name": ["Alice", "Bob", "Charlie", None],
        "age": ["25", "30", "35", None],
        "score": ["1.234,56", "2.345,67", "3.456,78", None],
        "join_date": ["2020-01-01", "2021-02-15", "2022-03-30", None],
        "active": ["True", "False", "True", None]
    }
    return {
        "df_": pd.DataFrame(data),
        "dtypes": {
            "name": "str",
            "age": "int64",
            "score": "float64",
            "join_date": "date",
            "active": "bool"
        },
        "dt_cols": ["join_date"]
    }


@pytest.fixture
def standardization_instance(sample_data: dict[str, Any]) -> Any: # noqa: ANN401
    """Fixture providing initialized DFStandardization instance.

    Parameters
    ----------
    sample_data : dict[str, Any]
        Sample data fixture

    Returns
    -------
    Any
        Initialized DFStandardization instance
    """
    return DFStandardization(
        dict_dtypes=sample_data["dtypes"],
        cols_from_case="lower",
        cols_to_case="upper",
        list_cols_drop_dupl=["name"],
        dict_fillna_strt={"age": "ffill"},
        str_fmt_dt="YYYY-MM-DD",
        type_error_action="raise",
        strategy_keep_when_dupl="first",
        str_data_fillna="MISSING",
        str_dt_fillna="1900-01-01",
        encoding="utf-8",
        bool_debug=False,
        logger=None
    )


@pytest.fixture
def ml_standardization_instance(sample_data: dict[str, Any]) -> Any: # noqa: ANN401
    """Fixture providing initialized DFStandardizationML instance.

    Parameters
    ----------
    sample_data : dict[str, Any]
        Sample data fixture

    Returns
    -------
    Any
        Initialized DFStandardizationML instance
    """
    return DFStandardizationML(
        dict_dtypes=sample_data["dtypes"],
        cols_from_case="lower",
        cols_to_case="upper",
        list_cols_drop_dupl=["name"],
        dict_fillna_strt={"age": "ffill"},
        str_fmt_dt="YYYY-MM-DD",
        type_error_action="raise",
        strategy_keep_when_dupl="first",
        str_data_fillna="MISSING",
        str_dt_fillna="1900-01-01",
        encoding="utf-8",
        bool_debug=False,
        logger=None
    )


# --------------------------
# Test Classes
# --------------------------
class TestDFStandardizationInit:
    """Tests for DFStandardization initialization."""

    def test_init_with_valid_params(self, sample_data: dict[str, Any]) -> None: # noqa: ANN401
        """Test initialization with valid parameters.
        
        Parameters
        ----------
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        instance = DFStandardization(
            dict_dtypes=sample_data["dtypes"],
            cols_from_case="lower",
            cols_to_case="upper"
        )
        assert instance.dict_dtypes == sample_data["dtypes"]
        assert instance.cols_from_case == "lower"
        assert instance.cols_to_case == "upper"

    def test_init_with_default_dt_fillna(
        self, 
        sample_data: dict[str, Any] # noqa: ANN401 - typing.Any is not allowed
    ) -> None:
        """Test automatic datetime fillna value selection.
        
        Parameters
        ----------
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        instance = DFStandardization(
            dict_dtypes=sample_data["dtypes"],
            str_fmt_dt="DD/MM/YYYY"
        )
        assert instance.str_dt_fillna == "31/12/2099"

    def test_init_with_empty_dtypes(self) -> None:
        """Test initialization with empty dtype dictionary.
        
        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="dict_dtypes cannot be empty"):
            DFStandardization(dict_dtypes={})


class TestDFStandardizationMethods:
    """Tests for DFStandardization individual methods."""

    def test_check_if_empty_with_non_empty_df(
        self, 
        standardization_instance: DFStandardization, 
        sample_data: dict[str, Any] # noqa: ANN401 - typing.Any is not allowed
    ) -> None:
        """Test check_if_empty with non-empty DataFrame.
        
        Parameters
        ----------
        standardization_instance : Any
            Initialized DFStandardization instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        result = standardization_instance.check_if_empty(sample_data["df_"])
        pd.testing.assert_frame_equal(result, sample_data["df_"])

    def test_check_if_empty_with_empty_df(
        self, 
        standardization_instance: DFStandardization
    ) -> None:
        """Test check_if_empty raises error with empty DataFrame.
        
        Parameters
        ----------
        standardization_instance : Any
            Initialized DFStandardization instance

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="DataFrame is empty"):
            standardization_instance.check_if_empty(pd.DataFrame())

    def test_clean_encoding_issues(
        self, 
        standardization_instance: DFStandardization, 
        sample_data: dict[str, Any] # noqa: ANN401 - typing.Any is not allowed
    ) -> None:
        """Test encoding issue cleaning.
        
        Parameters
        ----------
        standardization_instance : Any
            Initialized DFStandardization instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        df_.loc[0, "name"] = "Alïcé"
        result = standardization_instance.clean_encoding_issues(df_)
        assert result.loc[0, "name"] == "Alc"

    def test_coluns_names_case_conversion(
        self, 
        standardization_instance: DFStandardization, 
        sample_data: dict[str, Any]
    ) -> None:
        """Test column name case conversion.
        
        Parameters
        ----------
        standardization_instance : Any
            Initialized DFStandardization instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        result = standardization_instance.coluns_names_case(df_)
        assert all(col.isupper() for col in result.columns)

    def test_limit_columns_to_dtypes(
        self, 
        standardization_instance: DFStandardization, 
        sample_data: dict[str, Any] # noqa: ANN401 - typing.Any is not allowed
    ) -> None:
        """Test column filtering by dtype specification.
        
        Parameters
        ----------
        standardization_instance : Any
            Initialized DFStandardization instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        result = standardization_instance.limit_columns_to_dtypes(df_)
        assert set(result.columns) == set(sample_data["dtypes"].keys())

    def test_delete_empty_rows(
        self, 
        standardization_instance: DFStandardization, 
        sample_data: dict[str, Any]
    ) -> None:
        """Test empty row removal.
        
        Parameters
        ----------
        standardization_instance : Any
            Initialized DFStandardization instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        result = standardization_instance.delete_empty_rows(df_)
        assert len(result) == 3

    def test_filler_with_ffill(
        self, 
        standardization_instance: DFStandardization, 
        sample_data: dict[str, Any] # noqa: ANN401 - typing.Any is not allowed
    ) -> None:
        """Test forward fill strategy.
        
        Parameters
        ----------
        standardization_instance : Any
            Initialized DFStandardization instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        result = standardization_instance.filler(df_)
        assert result.loc[3, "age"] == "35"

    def test_replace_num_delimiters(
        self, 
        standardization_instance: DFStandardization, 
        sample_data: dict[str, Any] # noqa: ANN401 - typing.Any is not allowed
    ) -> None:
        """Test numeric delimiter replacement.
        
        Parameters
        ----------
        standardization_instance : Any
            Initialized DFStandardization instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        result = standardization_instance.replace_num_delimiters(df_)
        assert isinstance(result.loc[0, "score"], float)

    def test_change_dtypes(
        self, 
        standardization_instance: DFStandardization, 
        sample_data: dict[str, Any] # noqa: ANN401 - typing.Any is not allowed
    ) -> None:
        """Test dtype conversion.
        
        Parameters
        ----------
        standardization_instance : Any
            Initialized DFStandardization instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        result = standardization_instance.change_dtypes(df_)
        assert result["age"].dtype == "int64"
        assert result["score"].dtype == "float64"
        assert pd.api.types.is_datetime64_any_dtype(result["join_date"])

    def test_strip_hidden_characters(
        self, 
        standardization_instance: DFStandardization, 
        sample_data: dict[str, Any] # noqa: ANN401 - typing.Any is not allowed
    ) -> None:
        """Test hidden character stripping.
        
        Parameters
        ----------
        standardization_instance : Any
            Initialized DFStandardization instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        df_.loc[0, "name"] = "Alice\u200B"
        result = standardization_instance.strip_hidden_characters(df_)
        assert result.loc[0, "name"] == "Alice"

    def test_strip_data(
        self, 
        standardization_instance: DFStandardization, 
        sample_data: dict[str, Any] # noqa: ANN401 - typing.Any is not allowed
    ) -> None:
        """Test whitespace stripping.
        
        Parameters
        ----------
        standardization_instance : Any
            Initialized DFStandardization instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        df_.loc[0, "name"] = " Alice "
        result = standardization_instance.strip_data(df_)
        assert result.loc[0, "name"] == "Alice"

    def test_remove_duplicated_cols(
        self, 
        standardization_instance: DFStandardization, 
        sample_data: dict[str, Any] # noqa: ANN401 - typing.Any is not allowed
    ) -> None:
        """Test duplicate column removal.
        
        Parameters
        ----------
        standardization_instance : Any
            Initialized DFStandardization instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        df_["name_dup"] = df_["name"]
        result = standardization_instance.remove_duplicated_cols(df_)
        assert "name_dup" not in result.columns

    def test_data_remove_dupl(
        self, 
        standardization_instance: DFStandardization, 
        sample_data: dict[str, Any] # noqa: ANN401 - typing.Any is not allowed
    ) -> None:
        """Test duplicate row removal.
        
        Parameters
        ----------
        standardization_instance : Any
            Initialized DFStandardization instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        df_ = pd.concat([df_, df_.iloc[[0]]])
        result = standardization_instance.data_remove_dupl(df_)
        assert len(result) == len(df_) - 1


class TestDFStandardizationPipeline:
    """Tests for DFStandardization full pipeline."""

    def test_pipeline_execution(
        self, 
        standardization_instance: DFStandardization, 
        sample_data: dict[str, Any] # noqa: ANN401 - typing.Any is not allowed
    ) -> None:
        """Test full pipeline execution.
        
        Parameters
        ----------
        standardization_instance : DFStandardization
            Initialized DFStandardization instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        result = standardization_instance.pipeline(df_)
        
        # verify column names
        assert all(col.isupper() for col in result.columns)
        
        # verify data types
        assert result["AGE"].dtype == "int64"
        assert result["SCORE"].dtype == "float64"
        assert pd.api.types.is_datetime64_any_dtype(result["JOIN_DATE"])
        
        # verify no nulls
        assert not result.isna().any().any()

    def test_pipeline_with_empty_df(
        self, 
        standardization_instance: DFStandardization
    ) -> None:
        """Test pipeline with empty DataFrame raises error.
        
        Parameters
        ----------
        standardization_instance : Any
            Initialized DFStandardization instance

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="DataFrame is empty"):
            standardization_instance.pipeline(pd.DataFrame())


class TestDFStandardizationML:
    """Tests for DFStandardizationML class."""

    def test_handle_outliers_iqr(
        self, 
        ml_standardization_instance: DFStandardizationML, 
        sample_data: dict[str, Any]
    ) -> None:
        """Test IQR outlier handling.
        
        Parameters
        ----------
        ml_standardization_instance : DFStandardizationML
            Initialized DFStandardizationML instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        df_ = ml_standardization_instance.change_dtypes(df_)
        df_["SCORE"] = [100.0, 150.0, 200.0, 1000.0]
        result = ml_standardization_instance.handle_outliers(df_, method="iqr")
        assert result["SCORE"].max() < 500

    def test_handle_outliers_zscore(
        self, 
        ml_standardization_instance: DFStandardizationML, 
        sample_data: dict[str, Any]
    ) -> None:
        """Test z-score outlier handling.
        
        Parameters
        ----------
        ml_standardization_instance : Any
            Initialized DFStandardizationML instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        df_ = ml_standardization_instance.change_dtypes(df_)
        df_["SCORE"] = [1.0, 1.1, 1.2, 10.0]
        result = ml_standardization_instance.handle_outliers(df_, method="zscore")
        assert result["SCORE"].max() < 5.0

    def test_scale_numeric_data_minmax(
        self, 
        ml_standardization_instance: DFStandardizationML, 
        sample_data: dict[str, Any] # noqa: ANN401 - typing.Any is not allowed
    ) -> None:
        """Test min-max scaling.
        
        Parameters
        ----------
        ml_standardization_instance : Any
            Initialized DFStandardizationML instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        df_ = ml_standardization_instance.change_dtypes(df_)
        result = ml_standardization_instance.scale_numeric_data(df_, method="minmax")
        assert result["SCORE"].min() >= 0.0
        assert result["SCORE"].max() <= 1.0

    def test_scale_numeric_data_standard(
        self, 
        ml_standardization_instance: DFStandardizationML, 
        sample_data: dict[str, Any] # noqa: ANN401 - typing.Any is not allowed
    ) -> None:
        """Test standard scaling.
        
        Parameters
        ----------
        ml_standardization_instance : Any
            Initialized DFStandardizationML instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        df_ = ml_standardization_instance.change_dtypes(df_)
        result = ml_standardization_instance.scale_numeric_data(df_, method="standard")
        assert pytest.approx(result["SCORE"].mean()) == 0.0
        assert pytest.approx(result["SCORE"].std()) == 1.0

    def test_pipeline_ml(
        self, 
        ml_standardization_instance: DFStandardizationML, 
        sample_data: dict[str, Any] # noqa: ANN401 - typing.Any is not allowed
    ) -> None:
        """Test full ML pipeline execution.
        
        Parameters
        ----------
        ml_standardization_instance : Any
            Initialized DFStandardizationML instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        result = ml_standardization_instance.pipeline_ml(df_)
        
        # verify basic standardization
        assert all(col.isupper() for col in result.columns)
        
        # verify scaling was applied
        assert result["SCORE"].min() >= 0.0
        assert result["SCORE"].max() <= 1.0


class TestErrorHandling:
    """Tests for error conditions and edge cases."""

    def test_invalid_fill_strategy(
        self, 
        standardization_instance: DFStandardization, 
        sample_data: dict[str, Any] # noqa: ANN401 - typing.Any is not allowed
    ) -> None:
        """Test invalid fill strategy raises error.
        
        Parameters
        ----------
        standardization_instance : Any
            Initialized DFStandardization instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        standardization_instance.dict_fillna_strt = {"age": "invalid"}
        with pytest.raises(ValueError, match="Invalid fillna strategy"):
            standardization_instance.filler(sample_data["df_"])

    def test_invalid_outlier_method(
        self, 
        ml_standardization_instance: DFStandardizationML, 
        sample_data: dict[str, Any] # noqa: ANN401 - typing.Any is not allowed
    ) -> None:
        """Test invalid outlier method raises error.
        
        Parameters
        ----------
        ml_standardization_instance : Any
            Initialized DFStandardizationML instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        df_ = ml_standardization_instance.change_dtypes(df_)
        with pytest.raises(ValueError):
            ml_standardization_instance.handle_outliers(df_, method="invalid")

    def test_invalid_scale_method(
        self, 
        ml_standardization_instance: DFStandardizationML, 
        sample_data: dict[str, Any] # noqa: ANN401 - typing.Any is not allowed
    ) -> None:
        """Test invalid scaling method raises error.
        
        Parameters
        ----------
        ml_standardization_instance : Any
            Initialized DFStandardizationML instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        df_ = ml_standardization_instance.change_dtypes(df_)
        with pytest.raises(ValueError):
            ml_standardization_instance.scale_numeric_data(df_, method="invalid")

    def test_type_conversion_error(
        self, 
        standardization_instance: DFStandardization, 
        sample_data: dict[str, Any] # noqa: ANN401 - typing.Any is not allowed
    ) -> None:
        """Test type conversion error handling.
        
        Parameters
        ----------
        standardization_instance : Any
            Initialized DFStandardization instance
        sample_data : dict[str, Any]
            Sample data fixture

        Returns
        -------
        None
        """
        df_ = sample_data["df_"].copy()
        df_.loc[0, "age"] = "not_a_number"
        standardization_instance.type_error_action = "raise"
        with pytest.raises(ValueError):
            standardization_instance.change_dtypes(df_)