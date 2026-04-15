"""Dimensionality reduction utilities for feature selection and analysis.

This module provides tools for performing feature selection using backward and forward elimination,
exhaustive feature selection, principal component analysis (PCA), and Information Value (IV) with
Weight of Evidence (WOE) calculations. It includes robust input validation and error handling for
numerical and categorical data processing.
"""

from typing import Literal, Optional, TypedDict

from mlxtend.feature_selection import ExhaustiveFeatureSelector, SequentialFeatureSelector
import numpy as np
from numpy.typing import NDArray
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LinearRegression

from stpstone.transformations.cleaner.eda import ExploratoryDataAnalysis
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class ReturnDimensionalityReductionStandard(TypedDict):
    """Return type for dimensionality_reduction_standard method.

    Returns
    -------
    dict[str, Any]
        Dictionary containing selected feature names, indices, and score.
    """

    feature_names: tuple[str, ...]
    feature_indexes: tuple[int, ...]
    score: float


class ReturnPca(TypedDict):
    """Return type for pca method."""

    eigenvalues: NDArray[np.float64]
    eigenvectors: NDArray[np.float64]
    explained_variance_ratio: NDArray[np.float64]
    components: NDArray[np.float64]


class DimensionalityReduction(metaclass=TypeChecker):
    """Class for performing dimensionality reduction and feature selection techniques."""

    def _validate_array(self, array: NDArray, name: str) -> None:
        """Validate input array properties.

        Parameters
        ----------
        array : NDArray
            Input array to validate
        name : str
            Name of the array for error messages

        Raises
        ------
        ValueError
            If array is empty or contains non-finite values
        TypeError
            If array is not a numpy array
        """
        if not isinstance(array, np.ndarray):
            raise TypeError(f"{name} must be a numpy array")
        if array.size == 0:
            raise ValueError(f"{name} cannot be empty")
        if not np.all(np.isfinite(array)):
            raise ValueError(f"{name} contains NaN or infinite values")

    def _validate_estimator(self, estimator: str) -> None:
        """Validate estimator type.

        Parameters
        ----------
        estimator : str
            Estimator type to validate

        Raises
        ------
        ValueError
            If estimator is not supported
        """
        valid_estimators = ["linear_regression", "rf_classifier"]
        if estimator not in valid_estimators:
            raise ValueError(
                f"Estimator {estimator} not supported, choose from {valid_estimators}"
            )

    def _validate_cv(self, cv: int) -> None:
        """Validate cross-validation fold count.

        Parameters
        ----------
        cv : int
            Number of cross-validation folds

        Raises
        ------
        ValueError
            If cv is less than 1
        """
        if cv < 1:
            raise ValueError("Cross-validation folds must be positive")

    def _validate_verbose(self, verbose: int) -> None:
        """Validate verbosity level.

        Parameters
        ----------
        verbose : int
            Verbosity level

        Raises
        ------
        ValueError
            If verbose is negative
        """
        if verbose < 0:
            raise ValueError("Verbosity level cannot be negative")

    def backward_elimination(
        self,
        array_x: NDArray[np.float64],
        array_y: NDArray[np.float64],
        str_estimator: Literal['linear_regression', 'rf_classifier'] = "linear_regression",
        int_cv: int = 5,
        int_verbose: int = 0,
    ) -> ReturnDimensionalityReductionStandard:
        """Perform backward feature elimination.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Input feature array
        array_y : NDArray[np.float64]
            Target array
        str_estimator : Literal['linear_regression', 'rf_classifier']
            Estimator type (default: "linear_regression")
        int_cv : int
            Number of cross-validation folds (default: 5)
        int_verbose : int
            Verbosity level (default: 0)

        Returns
        -------
        ReturnDimensionalityReductionStandard
            Dictionary containing selected feature names, indices, and score

        References
        ----------
        .. [1] https://rasbt.github.io/mlxtend/api_subpackages/mlxtend.feature_selection/#sequentialfeatureselector
        .. [2] https://rasbt.github.io/mlxtend/user_guide/feature_selection/SequentialFeatureSelector/
        """
        self._validate_array(array_x, "array_x")
        self._validate_array(array_y, "array_y")
        self._validate_estimator(str_estimator)
        self._validate_cv(int_cv)
        self._validate_verbose(int_verbose)

        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)

        if str_estimator == "linear_regression":
            estimator = LinearRegression()
            scoring = "r2"
        else:
            estimator = RandomForestClassifier(n_jobs=-1)
            scoring = "accuracy"

        feature_selection = SequentialFeatureSelector(
            estimator,
            k_features=(1, array_x.shape[1]),
            forward=False,
            floating=False,
            verbose=int_verbose,
            scoring=scoring,
            cv=int_cv,
        ).fit(array_x, array_y)

        return {
            "feature_names": feature_selection.k_feature_names_,
            "feature_indexes": feature_selection.k_feature_idx_,
            "score": feature_selection.k_score_,
        }

    def forward_elimination(
        self,
        array_x: NDArray[np.float64],
        array_y: NDArray[np.float64],
        str_estimator: Literal['linear_regression', 'rf_classifier'] = "linear_regression",
        int_cv: int = 5,
        int_verbose: int = 0,
    ) -> ReturnDimensionalityReductionStandard:
        """Perform forward feature elimination.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Input feature array
        array_y : NDArray[np.float64]
            Target array
        str_estimator : Literal['linear_regression', 'rf_classifier']
            Estimator type (default: "linear_regression")
        int_cv : int
            Number of cross-validation folds (default: 5)
        int_verbose : int
            Verbosity level (default: 0)

        Returns
        -------
        ReturnDimensionalityReductionStandard
            Dictionary containing selected feature names, indices, and score
        
        References
        ----------
        .. [1] https://rasbt.github.io/mlxtend/api_subpackages/mlxtend.feature_selection/#sequentialfeatureselector
        .. [2] https://rasbt.github.io/mlxtend/user_guide/feature_selection/SequentialFeatureSelector/
        """
        self._validate_array(array_x, "array_x")
        self._validate_array(array_y, "array_y")
        self._validate_estimator(str_estimator)
        self._validate_cv(int_cv)
        self._validate_verbose(int_verbose)

        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)

        if str_estimator == "linear_regression":
            estimator = LinearRegression()
            scoring = "r2"
        else:
            estimator = RandomForestClassifier(n_jobs=-1)
            scoring = "accuracy"

        feature_selection = SequentialFeatureSelector(
            estimator,
            k_features=(1, array_x.shape[1]),
            forward=True,
            floating=False,
            verbose=int_verbose,
            scoring=scoring,
            cv=int_cv,
        ).fit(array_x, array_y)

        return {
            "feature_names": feature_selection.k_feature_names_,
            "feature_indexes": feature_selection.k_feature_idx_,
            "score": feature_selection.k_score_,
        }

    def exhaustive_feature_selection(
        self,
        array_x: NDArray[np.float64],
        array_y: NDArray[np.float64],
        str_estimator: Literal['linear_regression', 'rf_classifier'] = "linear_regression",
        max_features: Optional[int] = None,
        int_cv: int = 5,
    ) -> ReturnDimensionalityReductionStandard:
        """Perform exhaustive feature selection.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Input feature array
        array_y : NDArray[np.float64]
            Target array
        str_estimator : Literal['linear_regression', 'rf_classifier']
            Estimator type (default: "linear_regression")
        max_features : Optional[int]
            Maximum number of features to consider (default: None)
        int_cv : int
            Number of cross-validation folds (default: 5)

        Returns
        -------
        ReturnDimensionalityReductionStandard
            Dictionary containing selected feature names, indices, and score

        Raises
        ------
        ValueError
            If max_features is less than 1

        References
        ----------
        .. [1] https://rasbt.github.io/mlxtend/api_subpackages/mlxtend.feature_selection/#exhaustivefeatureselector
        """
        self._validate_array(array_x, "array_x")
        self._validate_array(array_y, "array_y")
        self._validate_estimator(str_estimator)
        self._validate_cv(int_cv)
        if max_features is not None and max_features < 1:
            raise ValueError("max_features must be positive or None")

        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)
        max_features = array_x.shape[1] if max_features is None else max_features

        if str_estimator == "linear_regression":
            estimator = LinearRegression()
            scoring = "r2"
        else:
            estimator = RandomForestClassifier(n_jobs=-1)
            scoring = "accuracy"

        feature_selection = ExhaustiveFeatureSelector(
            estimator,
            min_features=1,
            max_features=max_features,
            scoring=scoring,
            cv=int_cv,
            n_jobs=-1,
        ).fit(array_x, array_y)

        return {
            "feature_names": feature_selection.best_feature_names_,
            "feature_indexes": feature_selection.best_idx_,
            "score": feature_selection.best_score_,
        }

    def _validate_dataframe(self, df_: pd.DataFrame, name: str) -> None:
        """Validate input dataframe properties.

        Parameters
        ----------
        df_ : pd.DataFrame
            Input dataframe to validate
        name : str
            Name of the dataframe for error messages

        Raises
        ------
        ValueError
            If dataframe is empty
        TypeError
            If input is not a pandas DataFrame
        """
        if not isinstance(df_, pd.DataFrame):
            raise TypeError(f"{name} must be a pandas DataFrame")
        if df_.empty:
            raise ValueError(f"{name} cannot be empty")

    def _validate_column(self, df_: pd.DataFrame, column: str, name: str) -> None:
        """Validate column existence in dataframe.

        Parameters
        ----------
        df_ : pd.DataFrame
            Input dataframe
        column : str
            Column name to validate
        name : str
            Name for error messages

        Raises
        ------
        ValueError
            If column does not exist in dataframe
        """
        if column not in df_.columns:
            raise ValueError(f"Column {column} not found in {name}")

    def _validate_bins(self, bins: int) -> None:
        """Validate number of bins.

        Parameters
        ----------
        bins : int
            Number of bins

        Raises
        ------
        ValueError
            If bins is less than 1
        """
        if bins < 1:
            raise ValueError("Number of bins must be positive")

    def iv_woe_iter(
        self,
        df_binned: pd.DataFrame,
        target_col: str,
        class_col: str,
        name_missing_data: str = "Missing",
        name_bins: str = "_bins",
        col_min: str = "min",
        col_max: str = "max",
        col_non_event_count: str = "non_event_count",
        col_sample_class: str = "sample_class",
        col_event_count: str = "event_count",
        col_min_value: str = "min_value",
        col_max_value: str = "max_value",
        col_sample_count: str = "sample_count",
        col_event_rate: str = "event_rate",
        col_non_event_rate: str = "non_event_rate",
        col_feature: str = "feature",
        col_sample_class_label: str = "sample_class_label",
        name_category: str = "category",
        col_pct_non_event: str = "pct_non_event",
        col_pct_event: str = "pct_event",
        col_first: str = "first",
        col_count: str = "count",
        col_sum: str = "sum",
        col_mean: str = "mean",
        col_woe: str = "woe",
        col_iv: str = "iv",
    ) -> pd.DataFrame:
        """Calculate Information Value (IV) and Weight of Evidence (WOE) for binned data.

        Parameters
        ----------
        df_binned : pd.DataFrame
            Binned input dataframe
        target_col : str
            Target column name
        class_col : str
            Class column name
        name_missing_data : str
            Name for missing data category (default: "Missing")
        name_bins : str
            Suffix for binned columns (default: "_bins")
        col_min : str
            Minimum value column name (default: "min")
        col_max : str
            Maximum value column name (default: "max")
        col_non_event_count : str
            Non-event count column name (default: "non_event_count")
        col_sample_class : str
            Sample class column name (default: "sample_class")
        col_event_count : str
            Event count column name (default: "event_count")
        col_min_value : str
            Minimum value column name (default: "min_value")
        col_max_value : str
            Maximum value column name (default: "max_value")
        col_sample_count : str
            Sample count column name (default: "sample_count")
        col_event_rate : str
            Event rate column name (default: "event_rate")
        col_non_event_rate : str
            Non-event rate column name (default: "non_event_rate")
        col_feature : str
            Feature column name (default: "feature")
        col_sample_class_label : str
            Sample class label column name (default: "sample_class_label")
        name_category : str
            Category name (default: "category")
        col_pct_non_event : str
            Percent non-event column name (default: "pct_non_event")
        col_pct_event : str
            Percent event column name (default: "pct_event")
        col_first : str
            First value column name (default: "first")
        col_count : str
            Count column name (default: "count")
        col_sum : str
            Sum column name (default: "sum")
        col_mean : str
            Mean column name (default: "mean")
        col_woe : str
            WOE column name (default: "woe")
        col_iv : str
            IV column name (default: "iv")

        Returns
        -------
        pd.DataFrame
            Dataframe containing WOE and IV calculations
        
        References
        ----------
        .. [1] https://github.com/pankajkalania/IV-WOE/blob/main/iv_woe_code.py
        .. [2] https://gaurabdas.medium.com/weight-of-evidence-and-information-value-in-python-from-scratch-a8953d40e34#:~:text=Information%20Value%20gives%20us%20the,as%20good%20and%20bad%20customers.
        """
        self._validate_dataframe(df_binned, "df_binned")
        self._validate_column(df_binned, target_col, "df_binned")
        self._validate_column(df_binned, class_col, "df_binned")

        if name_bins in class_col:
            df_binned[class_col] = df_binned[class_col].cat.add_categories([name_missing_data])
            df_binned[class_col] = df_binned[class_col].fillna(name_missing_data)
            df_tmp_groupby = df_binned.groupby(class_col).agg(
                {
                    class_col.replace(name_bins, ""): [col_min, col_max],
                    target_col: [col_count, col_sum, col_mean],
                }
            ).reset_index()
        else:
            df_binned[class_col] = df_binned[class_col].fillna(name_missing_data)
            df_tmp_groupby = df_binned.groupby(class_col).agg(
                {
                    class_col: [col_first, col_first],
                    target_col: [col_count, col_sum, col_mean],
                }
            ).reset_index()

        df_tmp_groupby.columns = [
            col_sample_class,
            col_min_value,
            col_max_value,
            col_sample_count,
            col_event_count,
            col_event_rate,
        ]
        df_tmp_groupby[col_non_event_count] = (
            df_tmp_groupby[col_sample_count] - df_tmp_groupby[col_event_count]
        )
        df_tmp_groupby[col_non_event_rate] = 1 - df_tmp_groupby[col_event_rate]
        df_tmp_groupby = df_tmp_groupby[
            [
                col_sample_class,
                col_min_value,
                col_max_value,
                col_sample_count,
                col_non_event_count,
                col_non_event_rate,
                col_event_count,
                col_event_rate,
            ]
        ]

        if name_bins not in class_col and name_missing_data \
            in df_tmp_groupby[col_min_value].to_numpy():
            df_tmp_groupby[col_min_value] = df_tmp_groupby[col_min_value].replace(
                {name_missing_data: np.nan}
            )
            df_tmp_groupby[col_max_value] = df_tmp_groupby[col_max_value].replace(
                {name_missing_data: np.nan}
            )

        df_tmp_groupby[col_feature] = class_col
        if name_bins in class_col:
            df_tmp_groupby[col_sample_class_label] = (
                df_tmp_groupby[col_sample_class]
                .replace({name_missing_data: np.nan})
                .astype(name_category)
                .cat.codes.replace({-1: np.nan})
            )
        else:
            df_tmp_groupby[col_sample_class_label] = np.nan

        df_tmp_groupby = df_tmp_groupby[
            [
                col_feature,
                col_sample_class,
                col_sample_class_label,
                col_sample_count,
                col_min_value,
                col_max_value,
                col_non_event_count,
                col_non_event_rate,
                col_event_count,
                col_event_rate,
            ]
        ]

        df_tmp_groupby[col_pct_non_event] = (
            df_tmp_groupby[col_non_event_count] / df_tmp_groupby[col_non_event_count].sum()
        )
        df_tmp_groupby[col_pct_event] = (
            df_tmp_groupby[col_event_count] / df_tmp_groupby[col_event_count].sum()
        )

        df_tmp_groupby[col_woe] = np.log(
            df_tmp_groupby[col_pct_non_event] / df_tmp_groupby[col_pct_event]
        )
        df_tmp_groupby[col_iv] = (
            df_tmp_groupby[col_pct_non_event] - df_tmp_groupby[col_pct_event]
        ) * df_tmp_groupby[col_woe]

        df_tmp_groupby[col_woe] = df_tmp_groupby[col_woe].replace([np.inf, -np.inf], 0)
        df_tmp_groupby[col_iv] = df_tmp_groupby[col_iv].replace([np.inf, -np.inf], 0)

        return df_tmp_groupby

    def var_iter(
        self,
        df_: pd.DataFrame,
        target_col: str,
        max_bins: int,
        min_bins_monotonic: int = 2,
        col_feature: str = "feature",
        col_remarks: str = "remarks",
        name_categorical: str = "categorical",
        orient_str: str = "records",
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Iterate over features to calculate WOE and IV.

        Parameters
        ----------
        df_ : pd.DataFrame
            Input dataframe
        target_col : str
            Target column name
        max_bins : int
            Maximum number of bins
        min_bins_monotonic : int
            Minimum bins for monotonic features (default: 2)
        col_feature : str
            Feature column name (default: "feature")
        col_remarks : str
            Remarks column name (default: "remarks")
        name_categorical : str
            Name for categorical features (default: "categorical")
        orient_str : str
            Orientation for to_dict (default: "records")

        Returns
        -------
        tuple[pd.DataFrame, pd.DataFrame]
            Tuple of dataframes containing WOE/IV calculations and remarks

        Raises
        ------
        ValueError
            If min_bins_monotonic is less than 1

        References
        ----------
        .. [1] https://github.com/pankajkalania/IV-WOE/blob/main/iv_woe_code.py
        .. [2] https://gaurabdas.medium.com/weight-of-evidence-and-information-value-in-python-from-scratch-a8953d40e34#:~:text=Information%20Value%20gives%20us%20the,as%20good%20and%20bad%20customers.
        """
        self._validate_dataframe(df_, "df_")
        self._validate_column(df_, target_col, "df_")
        self._validate_bins(max_bins)
        if min_bins_monotonic < 1:
            raise ValueError("min_bins_monotonic must be positive")

        list_ser_woe_iv = []
        list_remarks = []

        for col in df_.columns:
            if col != target_col:
                if np.issubdtype(df_[col].dtype, np.number) \
                    and df_[col].nunique() > min_bins_monotonic:
                    class_col, remarks, df_binned = ExploratoryDataAnalysis().prepare_bins(
                        df_[[col, target_col]].copy(), col, target_col, max_bins
                    )
                    df_agg_data = self.iv_woe_iter(df_binned.copy(), target_col, class_col)
                    list_remarks.append({col_feature: col, col_remarks: remarks})
                else:
                    df_agg_data = self.iv_woe_iter(df_[[col, target_col]].copy(), target_col, col)
                    list_remarks.append({col_feature: col, col_remarks: name_categorical})
                list_ser_woe_iv.extend(df_agg_data.to_dict(orient=orient_str))

        return pd.DataFrame(list_ser_woe_iv), pd.DataFrame(list_remarks)

    def get_iv_woe(
        self,
        df_: pd.DataFrame,
        target_col: str,
        max_bins: int,
        name_missing_data: str = "Missing",
        name_bins: str = "_bins",
        col_non_event_count: str = "non_event_count",
        col_sample_class: str = "sample_class",
        col_event_count: str = "event_count",
        col_min_value: str = "min_value",
        col_max_value: str = "max_value",
        col_sample_count: str = "sample_count",
        col_non_event_rate: str = "non_event_rate",
        col_feature: str = "feature",
        col_sample_class_label: str = "sample_class_label",
        col_pct_non_event: str = "pct_non_event",
        col_pct_event: str = "pct_event",
        col_count: str = "count",
        col_woe: str = "woe",
        col_sum: str = "sum",
        col_iv: str = "iv",
        col_remarks: str = "remarks",
        col_number_classes: str = "number_of_classes",
        col_feature_null_pct: str = "feature_null_percent",
        col_iv_sum: str = "iv_sum",
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Calculate aggregated IV values for features.

        Parameters
        ----------
        df_ : pd.DataFrame
            Input dataframe
        target_col : str
            Target column name
        max_bins : int
            Maximum number of bins
        name_missing_data : str
            Name for missing data (default: "Missing")
        name_bins : str
            Suffix for binned columns (default: "_bins")
        col_non_event_count : str
            Non-event count column name (default: "non_event_count")
        col_sample_class : str
            Sample class column name (default: "sample_class")
        col_event_count : str
            Event count column name (default: "event_count")
        col_min_value : str
            Minimum value column name (default: "min_value")
        col_max_value : str
            Maximum value column name (default: "max_value")
        col_sample_count : str
            Sample count column name (default: "sample_count")
        col_non_event_rate : str
            Non-event rate column name (default: "non_event_rate")
        col_feature : str
            Feature column name (default: "feature")
        col_sample_class_label : str
            Sample class label column name (default: "sample_class_label")
        col_pct_non_event : str
            Percent non-event column name (default: "pct_non_event")
        col_pct_event : str
            Percent event column name (default: "pct_event")
        col_count : str
            Count column name (default: "count")
        col_woe : str
            WOE column name (default: "woe")
        col_sum : str
            Sum column name (default: "sum")
        col_iv : str
            IV column name (default: "iv")
        col_remarks : str
            Remarks column name (default: "remarks")
        col_number_classes : str
            Number of classes column name (default: "number_of_classes")
        col_feature_null_pct : str
            Feature null percent column name (default: "feature_null_percent")
        col_iv_sum : str
            IV sum column name (default: "iv_sum")

        Returns
        -------
        tuple[pd.DataFrame, pd.DataFrame]
            Tuple of dataframes containing aggregated IV and WOE/IV calculations

        References
        ----------
        .. [1] https://github.com/pankajkalania/IV-WOE/blob/main/iv_woe_code.py
        .. [2] https://gaurabdas.medium.com/weight-of-evidence-and-information-value-in-python-from-scratch-a8953d40e34#:~:text=Information%20Value%20gives%20us%20the,as%20good%20and%20bad%20customers.
        """
        self._validate_dataframe(df_, "df_")
        self._validate_column(df_, target_col, "df_")
        self._validate_bins(max_bins)

        df_woe_iv, df_binning_remarks = self.var_iter(df_, target_col, max_bins)
        df_woe_iv[col_feature] = df_woe_iv[col_feature].replace(name_bins, "", regex=True)
        df_woe_iv = df_woe_iv[
            [
                col_feature,
                col_sample_class,
                col_sample_class_label,
                col_sample_count,
                col_min_value,
                col_max_value,
                col_non_event_count,
                col_non_event_rate,
                col_event_count,
                col_pct_non_event,
                col_pct_event,
                col_woe,
                col_iv,
            ]
        ]

        df_iv = df_woe_iv.groupby(col_feature)[[col_iv]].agg([col_sum, col_count]).reset_index()
        df_iv.columns = [col_feature, col_iv, col_number_classes]

        df_null_percent = pd.DataFrame(df_.isna().mean()).reset_index()
        df_null_percent.columns = [col_feature, col_feature_null_pct]

        df_iv = df_iv.merge(df_null_percent, on=col_feature, how="left")
        df_iv = df_iv.merge(df_binning_remarks, on=col_feature, how="left")
        df_woe_iv = df_woe_iv.merge(
            df_iv[[col_feature, col_iv, col_remarks]].rename(columns={col_iv: col_iv_sum}),
            on=col_feature,
            how="left",
        )

        df_iv = df_iv.drop_duplicates().sort_values([col_iv], ascending=[False])

        return df_iv, df_woe_iv.replace({name_missing_data: np.nan})

    def iv_label_predictive_power(
        self,
        df_iv: pd.DataFrame,
        col_iv: str = "iv",
        col_predictive_power: str = "predictive_power",
    ) -> pd.DataFrame:
        """Label predictive power of each feature based on IV.

        Parameters
        ----------
        df_iv : pd.DataFrame
            Dataframe containing IV values
        col_iv : str
            IV column name (default: "iv")
        col_predictive_power : str
            Predictive power column name (default: "predictive_power")

        Returns
        -------
        pd.DataFrame
            Dataframe with predictive power labels

        References
        ----------
        .. [1] https://github.com/pankajkalania/IV-WOE/blob/main/iv_woe_code.py
        .. [2] https://gaurabdas.medium.com/weight-of-evidence-and-information-value-in-python-from-scratch-a8953d40e34#:~:text=Information%20Value%20gives%20us%20the,as%20good%20and%20bad%20customers.
        """
        self._validate_dataframe(df_iv, "df_iv")
        self._validate_column(df_iv, col_iv, "df_iv")

        df_iv = df_iv.sort_values([col_iv], ascending=[False]).copy()
        for index, row in df_iv.iterrows():
            iv_value = row[col_iv]
            if iv_value < 0.02:
                df_iv.loc[index, col_predictive_power] = "not useful"
            elif iv_value < 0.1:
                df_iv.loc[index, col_predictive_power] = "weak predictor"
            elif iv_value < 0.3:
                df_iv.loc[index, col_predictive_power] = "medium predictor"
            elif iv_value < 0.5:
                df_iv.loc[index, col_predictive_power] = "strong predictor"
            else:
                df_iv.loc[index, col_predictive_power] = "suspicious"

        return df_iv

    def pca(self, array_data: NDArray[np.float64]) -> ReturnPca:
        """Perform Principal Component Analysis (PCA).

        Parameters
        ----------
        array_data : NDArray[np.float64]
            Input data array

        Returns
        -------
        ReturnPca
            Dictionary containing eigenvalues, eigenvectors, explained variance ratio, \
                and components

        References
        ----------
        .. [1] https://leandrocruvinel.medium.com/pca-na-mão-e-no-python-d559e9c8f053
        """
        self._validate_array(array_data, "array_data")

        model_fitted = PCA(n_components=array_data.shape[1]).fit(array_data)
        return {
            "eigenvalues": model_fitted.explained_variance_,
            "eigenvectors": model_fitted.components_,
            "explained_variance_ratio": model_fitted.explained_variance_ratio_,
            "components": model_fitted.components_,
        }