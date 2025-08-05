"""Unit tests for ProbStatsCharts class.

This module contains comprehensive unit tests for the ProbStatsCharts class, verifying
visualization methods for probability and statistical analysis. Tests cover normal operations,
edge cases, error conditions, and type validation.
"""

import importlib
import sys
from unittest.mock import Mock

import matplotlib.pyplot as plt
import numpy as np
from numpy.typing import NDArray
import pandas as pd
import pytest
from pytest_mock import MockerFixture

from stpstone.analytics.quant.fit_assessment import FitPerformance
from stpstone.analytics.quant.prob_distributions import NormalDistribution
from stpstone.analytics.quant.regression import LogLinearRegressions
from stpstone.analytics.quant.stats_charts import ProbStatsCharts


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def prob_stats_charts() -> ProbStatsCharts:
    """Fixture providing a ProbStatsCharts instance.

    Returns
    -------
    ProbStatsCharts
        Instance of ProbStatsCharts class
    """
    return ProbStatsCharts()


@pytest.fixture
def sample_data() -> tuple[NDArray[np.float64], NDArray[np.float64]]:
    """Fixture providing sample data for testing.

    Returns
    -------
    tuple[NDArray[np.float64], NDArray[np.float64]]
        Tuple of (x, y) arrays with shape (10, 1) and (10,)
    """
    x = np.arange(10).reshape(-1, 1).astype(np.float64)
    y = np.array([0, 0, 0, 0, 1, 1, 1, 1, 1, 1], dtype=np.float64)
    return x, y


@pytest.fixture
def sample_confusion_matrix() -> NDArray[np.float64]:
    """Fixture providing a sample confusion matrix.

    Returns
    -------
    NDArray[np.float64]
        3x3 confusion matrix
    """
    return np.array([[10, 2, 1], [1, 15, 2], [0, 1, 12]], dtype=np.float64)


@pytest.fixture
def sample_ecdf_data() -> list[dict[str, NDArray[np.float64]]]:
    """Fixture providing sample ECDF data.

    Returns
    -------
    list[dict[str, NDArray[np.float64]]]
        List of dictionaries with data and labels
    """
    return [
        {
            "data": np.array([1, 2, 3, 4, 5], dtype=np.float64),
            "x_axis_label": "X Label",
            "y_axis_label": "Y Label",
            "legend": "Dataset 1"
        }
    ]


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Fixture providing a sample DataFrame.

    Returns
    -------
    pd.DataFrame
        DataFrame with two columns of float64 data
    """
    return pd.DataFrame({
        "col1": np.array([1, 2, 3, 4, 5], dtype=np.float64),
        "col2": np.array([2, 4, 6, 8, 10], dtype=np.float64)
    })


@pytest.fixture
def mock_model() -> Mock:
    """Fixture providing a mock fitted model.

    Returns
    -------
    Mock
        Mock object with fit and predict methods
    """
    model = Mock()
    model.fit.return_value = model
    model.predict.return_value = np.array([0, 1, 1, 0, 1], dtype=np.float64)
    return model


@pytest.fixture
def mock_scaler() -> Mock:
    """Fixture providing a mock scaler.

    Returns
    -------
    Mock
        Mock object with transform and inverse_transform methods
    """
    scaler = Mock()
    scaler.transform.return_value = np.array([[1, 2], [3, 4]], dtype=np.float64)
    scaler.inverse_transform.return_value = np.array([[1, 2], [3, 4]], dtype=np.float64)
    return scaler


# --------------------------
# Tests for Validation Methods
# --------------------------
class TestValidationMethods:
    """Test validation methods of ProbStatsCharts."""

    def test_validate_positive_number_valid(self, prob_stats_charts: ProbStatsCharts) -> None:
        """Test _validate_positive_number with valid input.

        Verifies that valid positive float inputs do not raise errors.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts

        Returns
        -------
        None
        """
        prob_stats_charts._validate_positive_number(1.0, "test_value")

    def test_validate_positive_number_zero(self, prob_stats_charts: ProbStatsCharts) -> None:
        """Test _validate_positive_number with zero.

        Verifies that zero input raises ValueError with appropriate message.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="must be positive"):
            prob_stats_charts._validate_positive_number(0, "test_value")

    def test_validate_positive_number_negative(self, prob_stats_charts: ProbStatsCharts) -> None:
        """Test _validate_positive_number with negative value.

        Verifies that negative input raises ValueError with appropriate message.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="test_value must be positive, got -1"):
            prob_stats_charts._validate_positive_number(-1.0, "test_value")

    def test_validate_array_valid(self, prob_stats_charts: ProbStatsCharts) -> None:
        """Test _validate_array with valid input.

        Verifies that valid numeric array does not raise errors.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts

        Returns
        -------
        None
        """
        arr = np.array([1, 2, 3], dtype=np.float64)
        prob_stats_charts._validate_array(arr, "test_array")

    def test_validate_array_empty(self, prob_stats_charts: ProbStatsCharts) -> None:
        """Test _validate_array with empty array.

        Verifies that empty array raises ValueError with appropriate message.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts

        Returns
        -------
        None
        """
        arr = np.array([], dtype=np.float64)
        with pytest.raises(ValueError, match="test_array cannot be empty"):
            prob_stats_charts._validate_array(arr, "test_array")

    def test_validate_array_non_finite(self, prob_stats_charts: ProbStatsCharts) -> None:
        """Test _validate_array with non-finite values.

        Verifies that array with NaN raises ValueError with appropriate message.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts

        Returns
        -------
        None
        """
        arr = np.array([1, np.nan, 3], dtype=np.float64)
        with pytest.raises(ValueError, match="test_array contains NaN or infinite values"):
            prob_stats_charts._validate_array(arr, "test_array")

    def test_validate_array_non_numeric(self, prob_stats_charts: ProbStatsCharts) -> None:
        """Test _validate_array with non-numeric values.

        Verifies that non-numeric array raises ValueError with appropriate message.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts

        Returns
        -------
        None
        """
        arr = np.array(["a", "b", "c"])
        with pytest.raises(ValueError, match="test_array must contain numeric values"):
            prob_stats_charts._validate_array(arr, "test_array")

    def test_validate_range_0_1_valid(self, prob_stats_charts: ProbStatsCharts) -> None:
        """Test _validate_range_0_1 with valid input.

        Verifies that values between 0 and 1 do not raise errors.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts

        Returns
        -------
        None
        """
        prob_stats_charts._validate_range_0_1(0.5, "test_value")

    def test_validate_range_0_1_negative(self, prob_stats_charts: ProbStatsCharts) -> None:
        """Test _validate_range_0_1 with negative value.

        Verifies that negative value raises ValueError with appropriate message.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="test_value must be between 0 and 1, got -0.1"):
            prob_stats_charts._validate_range_0_1(-0.1, "test_value")

    def test_validate_range_0_1_above_one(self, prob_stats_charts: ProbStatsCharts) -> None:
        """Test _validate_range_0_1 with value above 1.

        Verifies that value above 1 raises ValueError with appropriate message.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="test_value must be between 0 and 1, got 1.1"):
            prob_stats_charts._validate_range_0_1(1.1, "test_value")


# --------------------------
# Tests for Visualization Methods
# --------------------------
class TestVisualizationMethods:
    """Test visualization methods of ProbStatsCharts."""

    @pytest.mark.parametrize("c_value", [1.0, 0.1, 10.0])
    def test_confusion_mtx_2x2_valid(
        self,
        prob_stats_charts: ProbStatsCharts,
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]],
        mocker: MockerFixture,
        c_value: float
    ) -> None:
        """Test confusion_mtx_2x2 with valid inputs.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Sample x and y data
        mocker : MockerFixture
            Pytest mock fixture
        c_value : float
            Regularization strength parameter

        Returns
        -------
        None

        Verifies that the method runs without errors and calls plt.show().
        """
        x, y = sample_data
        mocker.patch.object(LogLinearRegressions, "logistic_regression_logit", 
                           return_value={"confusion_matrix": np.array([[1, 0], [0, 1]], 
                                                                      dtype=np.int64)})
        mocker.patch.object(plt, "show")
        prob_stats_charts.confusion_mtx_2x2(x, y, c_positive_floating_point_number=c_value)
        plt.show.assert_called_once()

    def test_confusion_mtx_2x2_invalid_c(
        self,
        prob_stats_charts: ProbStatsCharts,
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]]
    ) -> None:
        """Test confusion_mtx_2x2 with invalid c value.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Sample x and y data

        Returns
        -------
        None

        Verifies that negative c value raises ValueError.
        """
        x, y = sample_data
        with pytest.raises(ValueError, match="c_positive_floating_point_number must be positive"):
            prob_stats_charts.confusion_mtx_2x2(x, y, c_positive_floating_point_number=-1.0)

    def test_confusion_mtx_nxn_valid(
        self,
        prob_stats_charts: ProbStatsCharts,
        sample_confusion_matrix: NDArray[np.float64],
        mocker: MockerFixture
    ) -> None:
        """Test confusion_mtx_nxn with valid inputs.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        sample_confusion_matrix : NDArray[np.float64]
            Sample confusion matrix
        mocker : MockerFixture
            Pytest mock fixture

        Returns
        -------
        None

        Verifies that the method runs without errors and calls plt.show().
        """
        mocker.patch.object(plt, "show")
        prob_stats_charts.confusion_mtx_nxn(sample_confusion_matrix)
        plt.show.assert_called_once()

    def test_confusion_mtx_nxn_zero_row_sums(
        self,
        prob_stats_charts: ProbStatsCharts,
        mocker: MockerFixture
    ) -> None:
        """Test confusion_mtx_nxn with zero row sums.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        mocker : MockerFixture
            Pytest mock fixture

        Returns
        -------
        None

        Verifies that zero row sums raise ValueError when bool_focus_errors is True.
        """
        conf_mx = np.array([[0, 0], [0, 0]], dtype=np.float64)
        with pytest.raises(ValueError, match="Confusion matrix row sums cannot be zero"):
            prob_stats_charts.confusion_mtx_nxn(conf_mx, bool_l_focus_errors=True)

    def test_ecdf_chart_valid(
        self,
        prob_stats_charts: ProbStatsCharts,
        sample_ecdf_data: list[dict[str, NDArray[np.float64]]],
        mocker: MockerFixture
    ) -> None:
        """Test ecdf_chart with valid inputs.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        sample_ecdf_data : list[dict[str, NDArray[np.float64]]]
            Sample ECDF data
        mocker : MockerFixture
            Pytest mock fixture

        Returns
        -------
        None

        Verifies that the method runs without errors and calls plt.show().
        """
        mocker.patch.object(NormalDistribution, "ecdf", 
                            return_value=(np.array([1, 2]), np.array([0.2, 0.8])))
        mocker.patch.object(plt, "show")
        prob_stats_charts.ecdf_chart(sample_ecdf_data)
        plt.show.assert_called_once()

    def test_ecdf_chart_invalid_dict(
        self,
        prob_stats_charts: ProbStatsCharts
    ) -> None:
        """Test ecdf_chart with invalid dictionary input.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts

        Returns
        -------
        None

        Verifies that invalid dictionary raises ValueError.
        """
        invalid_data = [{"wrong_key": np.array([1, 2, 3], dtype=np.float64)}]
        with pytest.raises(ValueError, match="Each element in list_ser_data must be"):
            prob_stats_charts.ecdf_chart(invalid_data)

    def test_boxplot_valid(
        self,
        prob_stats_charts: ProbStatsCharts,
        sample_dataframe: pd.DataFrame,
        mocker: MockerFixture
    ) -> None:
        """Test boxplot with valid inputs.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        sample_dataframe : pd.DataFrame
            Sample DataFrame
        mocker : MockerFixture
            Pytest mock fixture

        Returns
        -------
        None

        Verifies that the method runs without errors and calls plt.show().
        """
        mocker.patch.object(plt, "show")
        prob_stats_charts.boxplot(sample_dataframe, "col1", "col2", "X Label", "Y Label")
        plt.show.assert_called_once()

    def test_scatter_plot_valid(
        self,
        prob_stats_charts: ProbStatsCharts,
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]],
        mocker: MockerFixture
    ) -> None:
        """Test scatter_plot with valid inputs.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Sample x and y data
        mocker : MockerFixture
            Pytest mock fixture

        Returns
        -------
        None

        Verifies that the method runs without errors and calls plt.show().
        """
        x, y = sample_data
        mocker.patch.object(plt, "show")
        prob_stats_charts.scatter_plot(x, y, "X Label", "Y Label")
        plt.show.assert_called_once()

    def test_pandas_histogram_columns_valid(
        self,
        prob_stats_charts: ProbStatsCharts,
        sample_dataframe: pd.DataFrame,
        mocker: MockerFixture
    ) -> None:
        """Test pandas_histogram_columns with valid inputs.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        sample_dataframe : pd.DataFrame
            Sample DataFrame
        mocker : MockerFixture
            Pytest mock fixture

        Returns
        -------
        None

        Verifies that the method runs without errors and calls plt.show().
        """
        mocker.patch.object(plt, "show")
        prob_stats_charts.pandas_histogram_columns(sample_dataframe)

    def test_pandas_histogram_columns_invalid_bins(
        self,
        prob_stats_charts: ProbStatsCharts,
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test pandas_histogram_columns with invalid bins.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        sample_dataframe : pd.DataFrame
            Sample DataFrame

        Returns
        -------
        None

        Verifies that negative bins raise ValueError.
        """
        with pytest.raises(ValueError, match="Number of bins must be positive"):
            prob_stats_charts.pandas_histogram_columns(sample_dataframe, bins=-1)

    def test_plot_precision_recall_vs_threshold_valid(
        self,
        prob_stats_charts: ProbStatsCharts,
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]],
        mock_model: Mock,
        mocker: MockerFixture
    ) -> None:
        """Test plot_precision_recall_vs_threshold with valid inputs.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Sample x and y data
        mock_model : Mock
            Mock fitted model
        mocker : MockerFixture
            Pytest mock fixture

        Returns
        -------
        None

        Verifies that the method runs without errors and calls plt.show().
        """
        x, y = sample_data
        mocker.patch.object(FitPerformance, "cross_validation",
                            return_value={"scores": np.array(
                                [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], 
                                dtype=np.float64)})
        mocker.patch("sklearn.metrics.precision_recall_curve",
                    return_value=(np.array([0.9, 0.8]), 
                                  np.array([0.7, 0.6]), np.array([0.1, 0.2])))
        mocker.patch.object(plt, "show")
        prob_stats_charts.plot_precision_recall_vs_threshold(mock_model, x, y)
        plt.show.assert_called_once()

    def test_plot_roc_curve_valid(
        self,
        prob_stats_charts: ProbStatsCharts,
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]],
        mock_model: Mock,
        mocker: MockerFixture
    ) -> None:
        """Test plot_roc_curve with valid inputs.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Sample x and y data
        mock_model : Mock
            Mock fitted model
        mocker : MockerFixture
            Pytest mock fixture

        Returns
        -------
        None

        Verifies that the method runs without errors and calls plt.show().
        """
        x, y = sample_data
        mocker.patch.object(FitPerformance, "cross_validation",
                            return_value={"scores": np.array(
                                [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], 
                                dtype=np.float64)})
        mocker.patch("sklearn.metrics.roc_curve",
                    return_value=(np.array([0, 0.5, 1]), np.array([0, 0.5, 1]), 
                                  np.array([0.1, 0.2])))
        mocker.patch.object(plt, "show")
        prob_stats_charts.plot_roc_curve(mock_model, x, y)
        plt.show.assert_called_once()

    def test_histogram_valid_single(
        self,
        prob_stats_charts: ProbStatsCharts,
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]],
        mocker: MockerFixture
    ) -> None:
        """Test histogram with valid single plot inputs.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Sample x and y data
        mocker : MockerFixture
            Pytest mock fixture

        Returns
        -------
        None

        Verifies that the method runs without errors and calls plt.show().
        """
        x, _ = sample_data
        mocker.patch.object(plt, "show")
        mocker.patch("matplotlib.figure.Figure.savefig")
        prob_stats_charts.histogram(x, "Test Title")
        plt.show.assert_called_once()

    def test_histogram_valid_multi(
        self,
        prob_stats_charts: ProbStatsCharts,
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]],
        mocker: MockerFixture
    ) -> None:
        """Test histogram with valid multi-plot inputs.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Sample x and y data
        mocker : MockerFixture
            Pytest mock fixture

        Returns
        -------
        None

        Verifies that the method runs without errors and calls plt.show().
        """
        x, _ = sample_data
        mocker.patch.object(plt, "show")
        mocker.patch("matplotlib.figure.Figure.savefig")
        prob_stats_charts.histogram([x, x], "Test Title", ["Sub1", "Sub2"], ncols=2, nrows=1)
        plt.show.assert_called_once()

    def test_histogram_invalid_nbins(
        self,
        prob_stats_charts: ProbStatsCharts,
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]]
    ) -> None:
        """Test histogram with invalid nbins.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Sample x and y data

        Returns
        -------
        None

        Verifies that negative nbins raises ValueError.
        """
        x, _ = sample_data
        with pytest.raises(TypeError, match="must be of type"):
            prob_stats_charts.histogram(x, "Test Title", nbins=-1.0)

    def test_abline_valid(
        self,
        prob_stats_charts: ProbStatsCharts,
        mocker: MockerFixture
    ) -> None:
        """Test abline with valid inputs.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        mocker : MockerFixture
            Pytest mock fixture

        Returns
        -------
        None

        Verifies that the method runs without errors and plots a line.
        """
        mocker.patch.object(plt, "gca", return_value=Mock(get_xlim=lambda: [0, 1]))
        mocker.patch.object(plt, "plot")
        prob_stats_charts.abline(plt, 1.0, 0.0)
        plt.plot.assert_called_once()

    def test_add_identity_valid(
        self,
        prob_stats_charts: ProbStatsCharts,
        mocker: MockerFixture
    ) -> None:
        """Test add_identity with valid inputs.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        mocker : MockerFixture
            Pytest mock fixture

        Returns
        -------
        None

        Verifies that the method runs without errors and returns axes.
        """
        mock_axes = Mock()
        mock_axes.plot.return_value = [Mock()]
        mock_axes.get_xlim.return_value = (0, 1)
        mock_axes.get_ylim.return_value = (0, 1)
        result = prob_stats_charts.add_identity(mock_axes)
        assert result == mock_axes
        mock_axes.plot.assert_called_once()

    def test_qq_plot_valid(
        self,
        prob_stats_charts: ProbStatsCharts,
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]],
        mocker: MockerFixture
    ) -> None:
        """Test qq_plot with valid inputs.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Sample x and y data
        mocker : MockerFixture
            Pytest mock fixture

        Returns
        -------
        None

        Verifies that the method returns correct quantiles and calls plt.show().
        """
        x, y = sample_data
        mocker.patch.object(plt, "show")
        result = prob_stats_charts.qq_plot(x, y)
        assert isinstance(result, dict)
        assert "quantiles" in result
        assert len(result["quantiles"]) == len(y)
        plt.show.assert_called_once()

    def test_plot_learning_curves_valid(
        self,
        prob_stats_charts: ProbStatsCharts,
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]],
        mock_model: Mock,
        mocker: MockerFixture
    ) -> None:
        """Test plot_learning_curves with valid inputs.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Sample x and y data
        mock_model : Mock
            Mock fitted model
        mocker : MockerFixture
            Pytest mock fixture

        Returns
        -------
        None

        Verifies that the method runs without errors and calls plt.show().
        """
        x, y = sample_data
        mocker.patch.object(plt, "show")
        def dynamic_predict(X: NDArray[np.float64]) -> NDArray[np.float64]:
            """Dynamic prediction function.
            
            Parameters
            ----------
            X : NDArray[np.float64]
                Input data

            Returns
            -------
            NDArray[np.float64]
                Predicted values
            """
            return np.ones(len(X), dtype=np.float64)
        mock_model.predict.side_effect = dynamic_predict
        prob_stats_charts.plot_learning_curves(mock_model, x, y)
        plt.show.assert_called_once()

    def test_plot_learning_curves_invalid_test_size(
        self,
        prob_stats_charts: ProbStatsCharts,
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]],
        mock_model: Mock
    ) -> None:
        """Test plot_learning_curves with invalid test size.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Sample x and y data
        mock_model : Mock
            Mock fitted model

        Returns
        -------
        None

        Verifies that invalid test size raises ValueError.
        """
        x, y = sample_data
        with pytest.raises(ValueError, match="float_test_size must be between 0 and 1"):
            prob_stats_charts.plot_learning_curves(mock_model, x, y, float_test_size=1.1)

    def test_classification_plot_2d_ivs_valid(
        self,
        prob_stats_charts: ProbStatsCharts,
        mock_scaler: Mock,
        mock_model: Mock,
        mocker: MockerFixture
    ) -> None:
        """Test classification_plot_2d_ivs with valid inputs.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        mock_scaler : Mock
            Mock scaler object
        mock_model : Mock
            Mock fitted model
        mocker : MockerFixture
            Pytest mock fixture

        Returns
        -------
        None

        Verifies that the method runs without errors and calls plt.show().
        """
        array_x = np.array([[1, 2], [3, 4]], dtype=np.float64)
        array_y = np.array([0, 1], dtype=np.float64)
        # Calculate expected shape for predictions
        x1 = np.arange(array_x[:, 0].min() - 10, array_x[:, 0].max() + 10, 0.25)
        x2 = np.arange(array_x[:, 1].min() - 1000, array_x[:, 1].max() + 1000, 0.25)
        expected_shape = (len(x2), len(x1))
        # Mock predict to return array with correct shape
        mocker.patch.object(mock_model, "predict", return_value=np.zeros(np.prod(expected_shape), 
                                                                         dtype=np.float64))
        mocker.patch.object(plt, "show")
        prob_stats_charts.classification_plot_2d_ivs(
            mock_scaler, array_x, array_y, mock_model, "Yes", "No", "Title", "X", "Y"
        )
        plt.show.assert_called_once()
    
    def test_classification_plot_2d_ivs_invalid_shape(
        self,
        prob_stats_charts: ProbStatsCharts,
        mock_scaler: Mock,
        mock_model: Mock
    ) -> None:
        """Test classification_plot_2d_ivs with invalid array shape.

        Parameters
        ----------
        prob_stats_charts : ProbStatsCharts
            Instance of ProbStatsCharts
        mock_scaler : Mock
            Mock scaler object
        mock_model : Mock
            Mock fitted model

        Returns
        -------
        None

        Verifies that non-2D array_x raises ValueError.
        """
        array_x = np.array([1, 2, 3], dtype=np.float64)
        array_y = np.array([0, 1, 0], dtype=np.float64)
        with pytest.raises(ValueError, match="array_x must be 2D for 2D classification plot"):
            prob_stats_charts.classification_plot_2d_ivs(
                mock_scaler, array_x, array_y, mock_model, "Yes", "No", "Title", "X", "Y"
            )


# --------------------------
# Tests for Module Reload
# --------------------------
def test_module_reload(mocker: MockerFixture) -> None:
    """Test module reloading behavior.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mock fixture

    Returns
    -------
    None

    Verifies that module can be reloaded without errors.
    """
    mocker.patch.object(plt, "show")
    importlib.reload(sys.modules["stpstone.analytics.quant.stats_charts"])
    assert hasattr(sys.modules["stpstone.analytics.quant.stats_charts"], "ProbStatsCharts")