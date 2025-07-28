"""Probability and statistical visualization tools.

This module provides a collection of visualization methods for probability and statistical 
analysis, including confusion matrices, empirical cumulative distribution functions, boxplots, 
scatter plots, histograms, precision-recall curves, ROC curves, Q-Q plots, learning curves, 
and 2D classification plots. 

All methods adhere to strict numpy-style documentation and include input validation.
"""

from numbers import Number
from typing import Any, Optional, TypedDict, Union

from matplotlib.colors import ListedColormap
import matplotlib.pyplot as plt
import numpy as np
from numpy.typing import NDArray
import pandas as pd
import seaborn as sns
from sklearn.metrics import mean_squared_error, precision_recall_curve, roc_curve
from sklearn.model_selection import train_test_split

from stpstone.analytics.quant.fit_assessment import FitPerformance
from stpstone.analytics.quant.prob_distributions import NormalDistribution
from stpstone.analytics.quant.regression import LogLinearRegressions
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class ReturnConfusionMtx2x2(TypedDict):
    """Return type for confusion_mtx_2x2 method.

    Parameters
    ----------
    confusion_matrix : NDArray[np.int64]
        2x2 confusion matrix
    """

class ReturnEcdfChart(TypedDict):
    """Return type for ecdf_chart method.

    Parameters
    ----------
    x_axis : NDArray[np.float64]
        X-axis values for ECDF
    y_axis : NDArray[np.float64]
        Y-axis values for ECDF
    """

class ReturnQqPlot(TypedDict):
    """Return type for qq_plot method.

    Parameters
    ----------
    quantiles : list[float]
        Computed sample quantiles
    """

class ProbStatsCharts(metaclass=TypeChecker):
    """Visualization tools for probability and statistical analysis."""

    def _validate_positive_number(self, value: Number, name: str) -> None:
        """Validate that a value is positive.

        Parameters
        ----------
        value : Number
            Value to validate
        name : str
            Variable name for error messages

        Raises
        ------
        ValueError
            If value is not positive
        """
        if value <= 0:
            raise ValueError(f"{name} must be positive, got {value}")

    def _validate_array(self, arr: NDArray, name: str) -> None:
        """Validate array properties.

        Parameters
        ----------
        arr : NDArray
            Array to validate
        name : str
            Variable name for error messages

        Raises
        ------
        ValueError
            If array is empty, contains non-finite values, or is not numeric
        """
        if len(arr) == 0:
            raise ValueError(f"{name} cannot be empty")
        try:
            if not np.all(np.isfinite(arr)):
                raise ValueError(f"{name} contains NaN or infinite values")
        except TypeError as err:
            raise ValueError(f"{name} must contain numeric values") from err
        if not np.issubdtype(arr.dtype, np.number):
            raise ValueError(f"{name} must contain numeric values")
        
    def _validate_df(self, df_: pd.DataFrame, name: str) -> None:
        """Validate DataFrame properties.

        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame to validate
        name : str
            Variable name for error messages

        Raises
        ------
        ValueError
            If DataFrame is empty
        """
        if len(df_) == 0:
            raise ValueError(f"{name} cannot be empty")

    def _validate_range_0_1(self, value: float, name: str) -> None:
        """Validate that a value is between 0 and 1.

        Parameters
        ----------
        value : float
            Value to validate
        name : str
            Variable name for error messages

        Raises
        ------
        ValueError
            If value is outside [0, 1] range
        """
        if not 0 <= value <= 1:
            raise ValueError(f"{name} must be between 0 and 1, got {value}")

    def confusion_mtx_2x2(
        self,
        x_array_real_numbers: NDArray[np.float64],
        y_vector_real_numbers: NDArray[np.float64],
        c_positive_floating_point_number: float = 1.0
    ) -> None:
        """Create a 2x2 confusion matrix visualization.

        Parameters
        ----------
        x_array_real_numbers : NDArray[np.float64]
            Input features array
        y_vector_real_numbers : NDArray[np.float64]
            Target values array
        c_positive_floating_point_number : float, optional
            Regularization strength (smaller values indicate stronger regularization), \
                by default 1.0

        Returns
        -------
        None
            Displays the confusion matrix plot
        """
        self._validate_array(x_array_real_numbers, "x_array_real_numbers")
        self._validate_array(y_vector_real_numbers, "y_vector_real_numbers")
        self._validate_positive_number(
            c_positive_floating_point_number, "c_positive_floating_point_number")

        cm = LogLinearRegressions().logistic_regression_logit(
            x_array_real_numbers,
            y_vector_real_numbers,
            c_positive_floating_point_number
        )["confusion_matrix"]
        fig, ax = plt.subplots(figsize=(8, 8))
        ax.imshow(cm)
        ax.grid(False)
        ax.xaxis.set(ticks=(0, 1), ticklabels=("Predicted 0s", "Predicted 1s"))
        ax.yaxis.set(ticks=(0, 1), ticklabels=("Actual 0s", "Actual 1s"))
        ax.set_ylim(1.5, -0.5)
        for i in range(2):
            for j in range(2):
                ax.text(j, i, cm[i, j], ha="center", va="center", color="red")
        plt.show()

    def confusion_mtx_nxn(
        self,
        conf_mx: NDArray[np.float64],
        cmap: str = "gray",
        bl_focus_errors: bool = True,
        complete_saving_path: Optional[str] = None,
        int_fill_non_error_values: int = 0
    ) -> None:
        """Create a heatmap visualization of an NxN confusion matrix.

        Parameters
        ----------
        conf_mx : NDArray[np.float64]
            Confusion matrix
        cmap : str, optional
            Colormap name, by default "gray"
        bl_focus_errors : bool, optional
            If True, normalize by row sums and fill diagonal with int_fill_non_error_values, \
                by default True
        complete_saving_path : Optional[str], optional
            Path to save the plot, by default None
        int_fill_non_error_values : int, optional
            Value to fill diagonal when focusing on errors, by default 0

        Returns
        -------
        None
            Displays the confusion matrix heatmap

        Raises
        ------
        ValueError
            If confusion matrix row sums are zero when bl_focus_errors is True
        """
        self._validate_array(conf_mx, "conf_mx")
        if bl_focus_errors:
            row_sums = conf_mx.sum(axis=1, keepdims=True)
            if np.any(row_sums == 0):
                raise ValueError(
                    "Confusion matrix row sums cannot be zero when bl_focus_errors is True")
            conf_mx = conf_mx / row_sums
            np.fill_diagonal(conf_mx, int_fill_non_error_values)
        plt.matshow(conf_mx, cmap=plt.get_cmap(cmap))
        if complete_saving_path is not None:
            plt.savefig(complete_saving_path)
        plt.show()

    def ecdf_chart(
        self,
        list_ser_data: list[dict[str, NDArray[np.float64]]],
        legend_position: str = "lower right"
    ) -> None:
        """Display an empirical cumulative distribution function chart.

        Parameters
        ----------
        list_ser_data : list[dict[str, NDArray[np.float64]]]
            List of dictionaries containing data and labels
        legend_position : str, optional
            Position of the legend, by default "lower right"

        Returns
        -------
        None
            Displays the ECDF plot

        Raises
        ------
        ValueError
            If each element in list_ser_data is not a dictionary with 'data' key
        """
        sns.set_theme()
        for data_dict in list_ser_data:
            if not isinstance(data_dict, dict) or "data" not in data_dict:
                raise ValueError(
                    "Each element in list_ser_data must be a dictionary with 'data' key")
            self._validate_array(data_dict["data"], "data")
            x_axis, y_axis = NormalDistribution().ecdf(data_dict["data"])
            plt.plot(x_axis, y_axis, marker=".", linestyle="none")
            plt.xlabel(data_dict.get("x_axis_label", ""))
            plt.ylabel(data_dict.get("y_axis_label", ""))
        plt.legend([x.get("legend", "") for x in list_ser_data], loc=legend_position)
        plt.show()

    def boxplot(
        self,
        array_data: Union[NDArray[np.float64], pd.DataFrame],
        x_axis_column_name: str,
        y_axis_column_name: str,
        x_label: str,
        y_label: str
    ) -> None:
        """Create a boxplot to evaluate interquartile range and outliers.

        Parameters
        ----------
        array_data : Union[NDArray[np.float64], pd.DataFrame]
            Input data array
        x_axis_column_name : str
            Column name for x-axis
        y_axis_column_name : str
            Column name for y-axis
        x_label : str
            X-axis label
        y_label : str
            Y-axis label

        Returns
        -------
        None
            Displays the boxplot
        """
        self._validate_df(array_data, "array_data")
        sns.boxplot(x=x_axis_column_name, y=y_axis_column_name, data=array_data)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.show()

    def scatter_plot(
        self,
        x_axis_data: NDArray[np.float64],
        y_axis_data: NDArray[np.float64],
        x_axis_label: str,
        y_axis_label: str
    ) -> None:
        """Create a scatter plot to infer data correlation.

        Parameters
        ----------
        x_axis_data : NDArray[np.float64]
            X-axis data
        y_axis_data : NDArray[np.float64]
            Y-axis data
        x_axis_label : str
            X-axis label
        y_axis_label : str
            Y-axis label

        Returns
        -------
        None
            Displays the scatter plot
        """
        self._validate_array(x_axis_data, "x_axis_data")
        self._validate_array(y_axis_data, "y_axis_data")
        sns.set_theme()
        plt.plot(x_axis_data, y_axis_data)
        plt.xlabel(x_axis_label)
        plt.ylabel(y_axis_label)
        plt.show()

    def pandas_histogram_columns(
        self,
        df_: pd.DataFrame,
        bins: Optional[int] = None,
        figsize: tuple[int, int] = (20, 15),
        complete_saving_path: Optional[str] = None
    ) -> None:
        """Create histograms for DataFrame columns.

        Parameters
        ----------
        df_ : pd.DataFrame
            Input DataFrame
        bins : Optional[int], optional
            Number of bins, by default None (calculated as sqrt of rows)
        figsize : tuple[int, int], optional
            Figure size, by default (20, 15)
        complete_saving_path : Optional[str], optional
            Path to save the plot, by default None

        Returns
        -------
        None
            Displays the histograms

        Raises
        ------
        ValueError
            If number of bins is not positive
        TypeError
            If number of bins is not of type int
        """
        self._validate_df(df_, "df_")
        if bins is not None and not isinstance(bins, int):
            raise TypeError(f"bins must be of type int, got {type(bins).__name__}")
        if bins is None:
            bins = int(np.sqrt(df_.shape[0]))
        if bins <= 0:
            raise ValueError(f"Number of bins must be positive, got {bins}")

        sns.set_theme()
        df_.hist(bins=bins, figsize=figsize)
        if complete_saving_path is not None:
            plt.savefig(complete_saving_path)
        plt.show()

    def plot_precision_recall_vs_threshold(
        self,
        model_fitted: object,
        array_data: NDArray[np.float64],
        array_target: NDArray[np.float64],
        cross_validation_folds: int = 3,
        scoring_method: str = "accuracy",
        localization_legend: str = "center right",
        label_precision: str = "Precision",
        label_recall: str = "Recall",
        line_style_precision: str = "b--",
        line_style_recall: str = "g-",
        line_width: int = 2,
        label_axis_x: str = "Threshold",
        label_axis_y: str = "Percentage",
        font_size: int = 16,
        bl_grid: bool = True,
        tup_fig_size: tuple[int, int] = (8, 4),
        complete_saving_path: Optional[str] = None
    ) -> None:
        """Plot precision-recall trade-off for binary classification.

        Parameters
        ----------
        model_fitted : object
            Fitted model
        array_data : NDArray[np.float64]
            Input features
        array_target : NDArray[np.float64]
            Target values
        cross_validation_folds : int, optional
            Number of cross-validation folds, by default 3
        scoring_method : str, optional
            Scoring method, by default "accuracy"
        localization_legend : str, optional
            Legend position, by default "center right"
        label_precision : str, optional
            Precision curve label, by default "Precision"
        label_recall : str, optional
            Recall curve label, by default "Recall"
        line_style_precision : str, optional
            Precision line style, by default "b--"
        line_style_recall : str, optional
            Recall line style, by default "g-"
        line_width : int, optional
            Line width, by default 2
        label_axis_x : str, optional
            X-axis label, by default "Threshold"
        label_axis_y : str, optional
            Y-axis label, by default "Percentage"
        font_size : int, optional
            Font size, by default 16
        bl_grid : bool, optional
            Show grid, by default True
        tup_fig_size : tuple[int, int], optional
            Figure size, by default (8, 4)
        complete_saving_path : Optional[str], optional
            Path to save the plot, by default None

        Returns
        -------
        None
            Displays the precision-recall plot

        References
        ----------
        .. [1] https://colab.research.google.com/github/ageron/handson-ml2/blob/master/03_classification.ipynb#scrollTo=rUZ6ahZ7G0BO
        """
        self._validate_array(array_data, "array_data")
        self._validate_array(array_target, "array_target")
        self._validate_positive_number(cross_validation_folds, "cross_validation_folds")
        array_target_scores = FitPerformance().cross_validation(
            model_fitted, array_data, array_target, cross_validation_folds, scoring_method
        )["scores"]
        precisions, recalls, thresholds = precision_recall_curve(array_target, array_target_scores)
        plt.figure(figsize=tup_fig_size)
        plt.plot(thresholds, precisions[:-1], line_style_precision, label=label_precision, 
                 linewidth=line_width)
        plt.plot(thresholds, recalls[:-1], line_style_recall, label=label_recall, 
                 linewidth=line_width)
        plt.legend(loc=localization_legend, fontsize=font_size)
        plt.xlabel(label_axis_x, fontsize=font_size)
        plt.ylabel(label_axis_y, fontsize=font_size)
        plt.grid(bl_grid)
        if complete_saving_path is not None:
            plt.savefig(complete_saving_path)
        plt.show()

    def plot_roc_curve(
        self,
        model_fitted: object,
        array_data: NDArray[np.float64],
        array_target: NDArray[np.float64],
        cross_validation_folds: int = 3,
        scoring_method: str = "accuracy",
        plot_title: Optional[str] = None,
        label_x_axis: str = "False Positive Rate (Fall-Out)",
        label_y_axis: str = "True Positive Rate (Recall)",
        font_size: int = 16,
        bl_grid: bool = True,
        tup_fig_size: tuple[int, int] = (8, 4),
        complete_saving_path: Optional[str] = None
    ) -> None:
        """Create a Receiver Operating Characteristic (ROC) curve plot.

        Parameters
        ----------
        model_fitted : object
            Fitted model
        array_data : NDArray[np.float64]
            Input features
        array_target : NDArray[np.float64]
            Target values
        cross_validation_folds : int, optional
            Number of cross-validation folds, by default 3
        scoring_method : str, optional
            Scoring method, by default "accuracy"
        plot_title : Optional[str], optional
            Plot title, by default None
        label_x_axis : str, optional
            X-axis label, by default "False Positive Rate (Fall-Out)"
        label_y_axis : str, optional
            Y-axis label, by default "True Positive Rate (Recall)"
        font_size : int, optional
            Font size, by default 16
        bl_grid : bool, optional
            Show grid, by default True
        tup_fig_size : tuple[int, int], optional
            Figure size, by default (8, 4)
        complete_saving_path : Optional[str], optional
            Path to save the plot, by default None

        Returns
        -------
        None
            Displays the ROC curve plot

        References
        ----------
        .. [1] https://colab.research.google.com/github/ageron/handson-ml2/blob/master/03_classification.ipynb#scrollTo=rUZ6ahZ7G0BO
        """
        self._validate_array(array_data, "array_data")
        self._validate_array(array_target, "array_target")
        self._validate_positive_number(cross_validation_folds, "cross_validation_folds")
        cv_result = FitPerformance().cross_validation(
            model_fitted, array_data, array_target, cross_validation_folds, scoring_method
        )
        array_target_scores = cv_result["scores"]
        fpr, tpr, _ = roc_curve(array_target, array_target_scores)
        plt.figure(figsize=tup_fig_size)
        plt.plot(fpr, tpr, linewidth=2, label=plot_title)
        plt.plot([0, 1], [0, 1], "k--")
        plt.axis([0, 1, 0, 1])
        plt.xlabel(label_x_axis, fontsize=font_size)
        plt.ylabel(label_y_axis, fontsize=font_size)
        plt.grid(bl_grid)
        if plot_title is not None:
            plt.title(plot_title, fontsize=font_size)
        if complete_saving_path is not None:
            plt.savefig(complete_saving_path)
        plt.show()

    def histogram(
        self,
        sample_vector: Union[NDArray[np.float64], list[NDArray[np.float64]]],
        suptitle: str,
        subtitle_vector: Optional[list[str]] = None,
        ncols: Optional[int] = None,
        nrows: Optional[int] = None,
        nbins: int = 100,
        limits: tuple[float, float] = (-100, 100),
        tick_label_size: int = 30,
        size: tuple[int, int] = (60, 30),
        suptitle_fontsize: int = 80,
        subtitle_fontsize: int = 40,
        filepath: Optional[str] = None
    ) -> None:
        """Create a histogram visualization.

        Parameters
        ----------
        sample_vector : Union[NDArray[np.float64], list[NDArray[np.float64]]]
            Data to be plotted
        suptitle : str
            Main title of the plot
        subtitle_vector : Optional[list[str]]
            List of subtitles for subplots, by default []
        ncols : Optional[int], optional
            Number of columns for subplots, by default None
        nrows : Optional[int], optional
            Number of rows for subplots, by default None
        nbins : int, optional
            Number of bins, by default 100
        limits : tuple[float, float], optional
            Range for histogram, by default (-100, 100)
        tick_label_size : int, optional
            Tick label size, by default 30
        size : tuple[int, int], optional
            Figure size, by default (60, 30)
        suptitle_fontsize : int, optional
            Super title font size, by default 80
        subtitle_fontsize : int, optional
            Subtitle font size, by default 40
        filepath : Optional[str]
            Path to save the plot, by default "C:/Temp/Teste.png"

        Returns
        -------
        None
            Displays the histogram

        Raises
        ------
        ValueError
            If number of bins is not positive
        """
        if isinstance(sample_vector, list):
            for arr in sample_vector:
                self._validate_array(arr, "sample_vector element")
        else:
            self._validate_array(sample_vector, "sample_vector")
        self._validate_positive_number(nbins, "nbins")
        self._validate_positive_number(tick_label_size, "tick_label_size")
        self._validate_positive_number(suptitle_fontsize, "suptitle_fontsize")
        self._validate_positive_number(subtitle_fontsize, "subtitle_fontsize")

        if subtitle_vector is None:
            subtitle_vector = []

        # convert sample_vector to list if it's a single array
        if not isinstance(sample_vector, list):
            sample_vector = [sample_vector]

        # flatten each array in sample_vector to 1D
        sample_vector = [np.ravel(arr) for arr in sample_vector]

        if ncols is not None and nrows is not None:
            self._validate_positive_number(ncols, "ncols")
            self._validate_positive_number(nrows, "nrows")
            if len(sample_vector) > ncols * nrows:
                raise ValueError(f"Number of histograms ({len(sample_vector)}) exceeds "
                                f"available subplots ({ncols * nrows})")
            fig, ax = plt.subplots(nrows, ncols, figsize=(size[0], size[1]))
            # flatten axes array for consistent iteration
            ax_flat = np.ravel(ax)
            for idx, (data, subtitle) in enumerate(zip(sample_vector, subtitle_vector)):
                if idx >= len(ax_flat):
                    break
                ax_flat[idx].hist(data, nbins, range=limits)
                ax_flat[idx].set_title(subtitle, fontsize=subtitle_fontsize)
                ax_flat[idx].tick_params(labelsize=tick_label_size)
            fig.suptitle(suptitle, fontsize=suptitle_fontsize)
            if filepath is not None:
                fig.savefig(filepath)
        else:
            fig, ax = plt.subplots(figsize=(size[0], size[1]))
            ax.hist(sample_vector[0], nbins, range=limits)
            ax.tick_params(labelsize=tick_label_size)
            fig.suptitle(suptitle, fontsize=suptitle_fontsize)
            if filepath is not None:
                fig.savefig(filepath)
        plt.show()

    def abline(self, plt_: plt, slope: float, intercept: float) -> None:
        """Plot a line from slope and intercept.

        Parameters
        ----------
        plt_ : plt
            Matplotlib.pyplot module
        slope : float
            Slope of the line
        intercept : float
            Intercept of the line

        Returns
        -------
        None
            Plots the line
        """
        axes = plt_.gca()
        x_vals = np.array(axes.get_xlim())
        y_vals = intercept + slope * x_vals
        plt_.plot(x_vals, y_vals, "--")

    def add_identity(
        self, 
        axes: plt.Axes, 
        *line_args: tuple[Any, ...], 
        **line_kwargs: dict[str, Any]
    ) -> plt.Axes:
        """Add an identity line to the plot.

        Parameters
        ----------
        axes : plt.Axes
            Matplotlib axes object
        *line_args : tuple[Any, ...]
            Positional arguments for plot
        **line_kwargs : dict[str, Any]
            Keyword arguments for plot

        Returns
        -------
        plt.Axes
            Updated axes object
        """
        identity, = axes.plot([], [], *line_args, **line_kwargs)

        def callback(axes: plt.Axes) -> None:
            """Update the line when the plot limits change.
            
            Parameters
            ----------
            axes : plt.Axes
                Matplotlib axes object
            
            Returns
            -------
            None
            """
            low, high = axes.get_xlim()
            low_x, high_x = axes.get_xlim()
            low_y, high_y = axes.get_ylim()
            low = max(low_x, low_y)
            high = min(high_x, high_y)
            identity.set_data([low, high], [low, high])
        callback(axes)
        axes.callbacks.connect("xlim_changed", callback)
        axes.callbacks.connect("ylim_changed", callback)
        return axes

    def qq_plot(
        self,
        list_ppf: NDArray[np.float64],
        list_raw_data: NDArray[np.float64],
        chart_title: Optional[str] = None,
        complete_saving_path: Optional[str] = None,
        bl_show_plot: bool = True,
        j: int = 0
    ) -> ReturnQqPlot:
        """Create a Q-Q plot to compare distributions.

        Parameters
        ----------
        list_ppf : NDArray[np.float64]
            Theoretical quantiles (PPF)
        list_raw_data : NDArray[np.float64]
            Sample data
        chart_title : Optional[str], optional
            Chart title, by default None
        complete_saving_path : Optional[str], optional
            Path to save the plot, by default None
        bl_show_plot : bool, optional
            Show the plot, by default True
        j : int, optional
            Starting index (unused, kept for compatibility), by default 0

        Returns
        -------
        ReturnQqPlot
            Dictionary containing computed quantiles

        References
        ----------
        .. [1] https://towardsdatascience.com/understand-q-q-plot-using-simple-python-4f83d5b89f8f
        """
        self._validate_array(list_ppf, "list_ppf")
        self._validate_array(list_raw_data, "list_raw_data")
        list_quantiles = []
        for i in range(1, len(list_raw_data) + 1):
            quantile = np.quantile(list_raw_data, i / len(list_raw_data))
            list_quantiles.append(quantile)
        fig, ax = plt.subplots(figsize=(10, 8))
        plt.plot(list_quantiles, sorted(list_ppf), "x")
        plt.plot([0, 1], [0, 1], "k--", linewidth=1, transform=ax.transAxes)
        if chart_title is not None:
            plt.title(chart_title)
        plt.ylabel("Theoretical Quantiles - PPF")
        plt.xlabel("Sample Quantiles")
        plt.grid()
        if complete_saving_path is not None:
            plt.savefig(complete_saving_path)
        if bl_show_plot:
            plt.show()
        return {"quantiles": list_quantiles}

    def plot_learning_curves(
        self,
        model: object,
        array_data: NDArray[np.float64],
        array_target: NDArray[np.float64],
        complete_path_save_figure: Optional[str] = None,
        float_test_size: float = 0.2,
        list_axis: Optional[list[float]] = None,
        line_type_training_error: str = "r-+",
        line_type_val_error: str = "b-",
        line_width_training_error: int = 2,
        line_width_val_error: int = 3,
        label_training_error: str = "trainig_data",
        label_val_error: str = "validation_data",
        x_label: str = "Training set size",
        y_label: str = "Root Mean Squared Error (RMSE)",
        plt_label: str = "Model Perform",
        legend_plot_position: str = "upper right",
        int_font_size: int = 14
    ) -> None:
        """Plot learning curves to evaluate model performance.

        Parameters
        ----------
        model : object
            Machine learning model
        array_data : NDArray[np.float64]
            Input features
        array_target : NDArray[np.float64]
            Target values
        complete_path_save_figure : Optional[str], optional
            Path to save the figure, by default None
        float_test_size : float, optional
            Test set size proportion, by default 0.2
        list_axis : Optional[list[float]]
            Axis limits [xmin, xmax, ymin, ymax], by default [0, 80, 0, 3]
        line_type_training_error : str, optional
            Training error line style, by default "r-+"
        line_type_val_error : str, optional
            Validation error line style, by default "b-"
        line_width_training_error : int, optional
            Training error line width, by default 2
        line_width_val_error : int, optional
            Validation error line width, by default 3
        label_training_error : str, optional
            Training error label, by default "trainig_data"
        label_val_error : str, optional
            Validation error label, by default "validation_data"
        x_label : str, optional
            X-axis label, by default "Training set size"
        y_label : str, optional
            Y-axis label, by default "Root Mean Squared Error (RMSE)"
        plt_label : str, optional
            Plot title, by default "Model Perform"
        legend_plot_position : str, optional
            Legend position, by default "upper right"
        int_font_size : int, optional
            Font size, by default 14

        Returns
        -------
        None
            Displays the learning curves plot

        References
        ----------
        .. [1] “HANDS-ON MACHINE LEARNING WITH SCIKIT-LEARN, KERAS, AND TENSORFLOW,
               2ND EDITION, BY AURÉLIEN GÉRON (O’REILLY). COPYRIGHT 2019 KIWISOFT S.A.S.,
               978-1-492-03264-9.”
        """
        self._validate_array(array_data, "array_data")
        self._validate_array(array_target, "array_target")
        self._validate_range_0_1(float_test_size, "float_test_size")
        self._validate_positive_number(int_font_size, "int_font_size")

        if list_axis is None:
            list_axis = [0, 80, 0, 3]
        
        X_train, X_val, y_train, y_val = train_test_split(array_data, array_target, 
                                                          test_size=float_test_size)
        train_errors, val_errors = [], []
        for m in range(1, len(X_train)):
            model.fit(X_train[:m], y_train[:m])
            y_train_predict = model.predict(X_train[:m])
            y_val_predict = model.predict(X_val)
            train_errors.append(mean_squared_error(y_train[:m], y_train_predict))
            val_errors.append(mean_squared_error(y_val, y_val_predict))
        plt.title(label=plt_label)
        plt.axis(list_axis)
        plt.plot(
            np.sqrt(train_errors),
            line_type_training_error,
            linewidth=line_width_training_error,
            label=label_training_error
        )
        plt.plot(
            np.sqrt(val_errors),
            line_type_val_error,
            linewidth=line_width_val_error,
            label=label_val_error
        )
        plt.legend(loc=legend_plot_position, fontsize=int_font_size)
        plt.xlabel(x_label, fontsize=int_font_size)
        plt.ylabel(y_label, fontsize=int_font_size)
        if complete_path_save_figure is not None:
            plt.savefig(complete_path_save_figure)
        plt.show()

    def classification_plot_2d_ivs(
        self,
        cls_scaler: object,
        array_x: NDArray[np.float64],
        array_y: NDArray[np.float64],
        cls_model_fiited: object,
        label_yes: str,
        label_no: str,
        str_title: str,
        str_xlabel: str,
        str_ylabel: str,
        complete_path_save_figure: Optional[str] = None,
        str_color_negative: str = "salmon",
        str_color_positive: str = "dodgerblue"
    ) -> None:
        """Create a 2D classification plot with decision boundaries.

        Parameters
        ----------
        cls_scaler : object
            Scaler object for feature scaling
        array_x : NDArray[np.float64]
            Input features (2D)
        array_y : NDArray[np.float64]
            Target values
        cls_model_fiited : object
            Fitted classification model
        label_yes : str
            Label for positive class
        label_no : str
            Label for negative class
        str_title : str
            Plot title
        str_xlabel : str
            X-axis label
        str_ylabel : str
            Y-axis label
        complete_path_save_figure : Optional[str], optional
            Path to save the figure, by default None
        str_color_negative : str, optional
            Color for negative class, by default "salmon"
        str_color_positive : str, optional
            Color for positive class, by default "dodgerblue"

        Returns
        -------
        None
            Displays the classification plot

        Raises
        ------
        ValueError
            If array_x is not 2D

        References
        ----------
        .. [1] https://colab.research.google.com/drive/1-Slk6y5-E3eUnmM4vjtoRrGMoIKvD0hU#scrollTo=_NOjKvZRid5l
        """
        self._validate_array(array_x, "array_x")
        self._validate_array(array_y, "array_y")
        if array_x.ndim != 2 or array_x.shape[1] != 2:
            raise ValueError("array_x must be 2D for 2D classification plot")
        X_set, y_set = cls_scaler.inverse_transform(array_x), array_y
        X1, X2 = np.meshgrid(
            np.arange(start=X_set[:, 0].min() - 10, stop=X_set[:, 0].max() + 10, step=0.25),
            np.arange(start=X_set[:, 1].min() - 1000, stop=X_set[:, 1].max() + 1000, step=0.25)
        )
        plt.contourf(
            X1,
            X2,
            cls_model_fiited.predict(
                cls_scaler.transform(np.array([X1.ravel(), X2.ravel()]).T)
            ).reshape(X1.shape),
            alpha=0.75,
            cmap=ListedColormap((str_color_negative, str_color_positive))
        )
        plt.xlim(X1.min(), X1.max())
        plt.ylim(X2.min(), X2.max())
        for i, j in enumerate(np.unique(y_set)):
            label = label_no if j == 0 else label_yes
            plt.scatter(
                X_set[y_set == j, 0],
                X_set[y_set == j, 1],
                color=ListedColormap((str_color_negative, str_color_positive))(i),
                label=label
            )
        plt.title(str_title)
        plt.xlabel(str_xlabel)
        plt.ylabel(str_ylabel)
        plt.legend()
        if complete_path_save_figure is not None:
            plt.savefig(complete_path_save_figure)
        plt.show()