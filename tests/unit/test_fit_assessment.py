"""Unit tests for FitPerformance class in stpstone package.

Tests the model fitting performance evaluation functionality including:
- Log-likelihood calculations
- Information criteria (AIC/BIC)
- Cross-validation
- Grid search optimization
- Accuracy metrics
- Performance evaluation
"""

import numpy as np
import pytest
from sklearn.base import BaseEstimator
from sklearn.dummy import DummyClassifier, DummyRegressor
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV

from stpstone.analytics.quant.fit_assessment import FitPerformance


@pytest.fixture
def sample_regression_data() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Fixture providing sample regression data.
    
    Returns
    -------
    tuple[np.ndarray, np.ndarray, np.ndarray]
        Feature and target arrays
    """
    np.random.seed(42)
    x = np.random.rand(100, 3)
    y = 2 * x[:, 0] + 3 * x[:, 1] - 1.5 * x[:, 2] + np.random.normal(0, 0.1, 100)
    y_hat = 2.1 * x[:, 0] + 2.9 * x[:, 1] - 1.4 * x[:, 2]
    return x, y, y_hat


@pytest.fixture
def sample_classification_data() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Fixture providing sample classification data.
    
    Returns
    -------
    tuple[np.ndarray, np.ndarray, np.ndarray]
        Feature and target arrays
    """
    np.random.seed(42)
    x = np.random.rand(100, 3)
    y = (x[:, 0] + x[:, 1] - x[:, 2] > 0.5).astype(int)
    y_hat = (x[:, 0] + x[:, 1] - x[:, 2] > 0.4).astype(int)
    return x, y, y_hat


@pytest.fixture
def dummy_regressor() -> BaseEstimator:
    """Fixture providing a dummy regressor model.
    
    Returns
    -------
    BaseEstimator
        A dummy regressor model.
    """
    return DummyRegressor(strategy="mean")


@pytest.fixture
def dummy_classifier() -> BaseEstimator:
    """Fixture providing a dummy classifier model.
    
    Returns
    -------
    BaseEstimator
        A dummy classifier model.
    """
    return DummyClassifier(strategy="most_frequent")


@pytest.fixture
def fit_performance() -> FitPerformance:
    """Fixture providing an instance of FitPerformance.
    
    Returns
    -------
    FitPerformance
        An instance of FitPerformance.
    """
    return FitPerformance()


class TestMaxLLF:
    """Tests for max_llf method."""

    def test_max_llf_returns_float(self, fit_performance: FitPerformance,
                                  sample_regression_data: tuple) -> None:
        """Test that max_llf returns a float value.
        
        Parameters
        ----------
        fit_performance : FitPerformance
            An instance of FitPerformance.
        sample_regression_data : tuple
            A tuple containing feature and target arrays.

        Returns
        -------
        None
        """
        x, y, y_hat = sample_regression_data
        result = fit_performance.max_llf(x, y, y_hat)
        assert isinstance(result, float)

    def test_max_llf_with_perfect_fit(self, fit_performance: FitPerformance) -> None:
        """Test max_llf with perfect predictions.
        
        Parameters
        ----------
        fit_performance : FitPerformance
            An instance of FitPerformance.

        Returns
        -------
        None
        """
        x = np.array([[1], [2], [3]])
        y = np.array([2, 4, 6])
        y_hat = y.copy()
        result = fit_performance.max_llf(x, y, y_hat)
        assert result > 0  # Should be positive for perfect fit

    def test_max_llf_with_random_data(self, fit_performance: FitPerformance) -> None:
        """Test max_llf with random predictions.
        
        Parameters
        ----------
        fit_performance : FitPerformance
            An instance of FitPerformance.

        Returns
        -------
        None
        """
        x = np.random.rand(100, 2)
        y = np.random.rand(100)
        y_hat = np.random.rand(100)
        result = fit_performance.max_llf(x, y, y_hat)
        assert isinstance(result, float)


class TestAIC:
    """Tests for aic method."""

    def test_aic_returns_float(self, fit_performance: FitPerformance,
                              sample_regression_data: tuple) -> None:
        """Test that aic returns a float value.
        
        Parameters
        ----------
        fit_performance : FitPerformance
            An instance of FitPerformance.
        sample_regression_data : tuple
            A tuple containing feature and target arrays.

        Returns
        -------
        None
        """
        x, y, y_hat = sample_regression_data
        result = fit_performance.aic(x, y, y_hat)
        assert isinstance(result, float)

    def test_aic_increases_with_complexity(self, fit_performance: FitPerformance) -> None:
        """Test that AIC increases with model complexity.
        
        Parameters
        ----------
        fit_performance : FitPerformance
            An instance of FitPerformance.

        Returns
        -------
        None
        """
        x_simple = np.random.rand(100, 1)
        x_complex = np.random.rand(100, 5)
        y = np.random.rand(100)
        y_hat = np.random.rand(100)
        
        aic_simple = fit_performance.aic(x_simple, y, y_hat)
        aic_complex = fit_performance.aic(x_complex, y, y_hat)
        assert aic_complex > aic_simple


class TestBIC:
    """Tests for bic method."""

    def test_bic_returns_float(self, fit_performance: FitPerformance,
                              sample_regression_data: tuple) -> None:
        """Test that bic returns a float value.
        
        Parameters
        ----------
        fit_performance : FitPerformance
            An instance of FitPerformance.
        sample_regression_data : tuple
            A tuple containing feature and target arrays.

        Returns
        -------
        None
        """
        x, y, y_hat = sample_regression_data
        result = fit_performance.bic(x, y, y_hat)
        assert isinstance(result, float)

    def test_bic_penalizes_complexity_more_than_aic(self, fit_performance: FitPerformance) -> None:
        """Test that BIC penalizes complexity more than AIC.
        
        Parameters
        ----------
        fit_performance : FitPerformance
            An instance of FitPerformance.

        Returns
        -------
        None
        """
        x = np.random.rand(100, 5)
        y = np.random.rand(100)
        y_hat = np.random.rand(100)
        
        aic = fit_performance.aic(x, y, y_hat)
        bic = fit_performance.bic(x, y, y_hat)
        assert bic > aic


class TestCrossValidation:
    """Tests for cross_validation method."""

    def test_cross_validation_score_returns_dict(self, fit_performance: FitPerformance,
                                                dummy_regressor: BaseEstimator,
                                                sample_regression_data: tuple) -> None:
        """Test cross_validation with score method returns expected dict.
        
        Parameters
        ----------
        fit_performance : FitPerformance
            An instance of FitPerformance.
        dummy_regressor : BaseEstimator
            A dummy regressor.
        sample_regression_data : tuple
            A tuple containing feature and target arrays.

        Returns
        -------
        None
        """
        x, y, _ = sample_regression_data
        result = fit_performance.cross_validation(dummy_regressor, x, y)
        assert isinstance(result, dict)
        assert "scores" in result
        assert "mean" in result
        assert "std" in result

    def test_cross_validation_predict_returns_dict(self, fit_performance: FitPerformance,
                                                  dummy_regressor: BaseEstimator,
                                                  sample_regression_data: tuple) -> None:
        """Test cross_validation with predict method returns expected dict.
        
        Parameters
        ----------
        fit_performance : FitPerformance
            An instance of FitPerformance.
        dummy_regressor : BaseEstimator
            A dummy regressor.
        sample_regression_data : tuple
            A tuple containing feature and target arrays.

        Returns
        -------
        None
        """
        x, y, _ = sample_regression_data
        result = fit_performance.cross_validation(
            dummy_regressor, x, y, cross_val_model="predict"
        )
        assert isinstance(result, dict)
        assert "scores" in result
        assert "mean" in result
        assert "std" in result

    def test_cross_validation_invalid_method_raises(self, fit_performance: FitPerformance,
                                                   dummy_regressor: BaseEstimator,
                                                   sample_regression_data: tuple) -> None:
        """Test cross_validation with invalid method raises ValueError.
        
        Parameters
        ----------
        fit_performance : FitPerformance
            An instance of FitPerformance.
        dummy_regressor : BaseEstimator
            A dummy regressor.
        sample_regression_data : tuple
            A tuple containing feature and target arrays.

        Returns
        -------
        None
        """
        x, y, _ = sample_regression_data
        with pytest.raises(TypeError, match="must be one of"):
            fit_performance.cross_validation(
                dummy_regressor, x, y, cross_val_model="invalid"
            )


class TestGridSearchOptimalHyperparameters:
    """Tests for grid_search_optimal_hyperparameters method."""

    @pytest.fixture
    def sample_param_grid(self) -> dict:
        """Fixture providing sample parameter grid.
        
        Returns
        -------
        dict
            Sample parameter grid.
        """
        return {
            "strategy": ["mean", "median", "quantile"],
            "quantile": [0.25, 0.5, 0.75],
            "constant": [None, 1, 2]
        }

    def test_grid_search_returns_dict(self, fit_performance: FitPerformance,
                                     dummy_regressor: BaseEstimator,
                                     sample_param_grid: dict,
                                     sample_regression_data: tuple) -> None:
        """Test grid_search returns expected dictionary structure.
        
        Parameters
        ----------
        fit_performance : FitPerformance
            An instance of FitPerformance.
        dummy_regressor : BaseEstimator
            A dummy regressor.
        sample_param_grid : dict
            Sample parameter grid.
        sample_regression_data : tuple
            A tuple containing feature and target arrays.

        Returns
        -------
        None
        """
        x, y, _ = sample_regression_data
        result = fit_performance.grid_search_optimal_hyperparameters(
            dummy_regressor, sample_param_grid, "neg_mean_squared_error", x, y
        )
        assert isinstance(result, dict)
        assert "best_parameters" in result
        assert "score" in result
        assert "best_estimator" in result

    def test_randomized_search_returns_dict(self, fit_performance: FitPerformance,
                                           dummy_regressor: BaseEstimator,
                                           sample_param_grid: dict,
                                           sample_regression_data: tuple) -> None:
        """Test randomized search returns expected dictionary structure.
        
        Parameters
        ----------
        fit_performance : FitPerformance
            An instance of FitPerformance.
        dummy_regressor : BaseEstimator
            A dummy regressor.
        sample_param_grid : dict
            Sample parameter grid.
        sample_regression_data : tuple
            A tuple containing feature and target arrays.

        Returns
        -------
        None
        """
        x, y, _ = sample_regression_data
        result = fit_performance.grid_search_optimal_hyperparameters(
            dummy_regressor, sample_param_grid, "neg_mean_squared_error", x, y,
            bl_randomized_search=True
        )
        assert isinstance(result, dict)
        assert isinstance(result["model_regression"], RandomizedSearchCV)

    def test_grid_search_returns_dict_grid(self, fit_performance: FitPerformance,
                                          dummy_regressor: BaseEstimator,
                                          sample_param_grid: dict,
                                          sample_regression_data: tuple) -> None:
        """Test grid search returns expected dictionary structure.
        
        Parameters
        ----------
        fit_performance : FitPerformance
            An instance of FitPerformance.
        dummy_regressor : BaseEstimator
            A dummy regressor.
        sample_param_grid : dict
            Sample parameter grid.
        sample_regression_data : tuple
            A tuple containing feature and target arrays.

        Returns
        -------
        None
        """
        x, y, _ = sample_regression_data
        result = fit_performance.grid_search_optimal_hyperparameters(
            dummy_regressor, sample_param_grid, "neg_mean_squared_error", x, y,
            bl_randomized_search=False
        )
        assert isinstance(result, dict)
        assert isinstance(result["model_regression"], GridSearchCV)


class TestAccuracyPredictions:
    """Tests for accuracy_predictions method."""

    def test_accuracy_predictions_returns_dict(self, fit_performance: FitPerformance,
                                              dummy_classifier: BaseEstimator,
                                              sample_classification_data: tuple) -> None:
        """Test accuracy_predictions returns expected dictionary structure.
        
        Parameters
        ----------
        fit_performance : FitPerformance
            An instance of FitPerformance.
        dummy_classifier : BaseEstimator
            A dummy classifier.
        sample_classification_data : tuple
            A tuple containing feature and target arrays.

        Returns
        -------
        None
        """
        x, y, _ = sample_classification_data
        result = fit_performance.accuracy_predictions(dummy_classifier, y, x)
        assert isinstance(result, dict)
        assert "cross_validation_scores" in result
        assert "confusion_matrix" in result
        assert "precision_score" in result
        assert "recall_score" in result
        assert "f1_score" in result
        assert "roc_auc_score" in result

    def test_accuracy_predictions_with_different_average(self, fit_performance: FitPerformance,
                                                        dummy_classifier: BaseEstimator,
                                                        sample_classification_data: tuple) -> None:
        """Test accuracy_predictions with different f1_score_average options.
        
        Parameters
        ----------
        fit_performance : FitPerformance
            An instance of FitPerformance.
        dummy_classifier : BaseEstimator
            A dummy classifier.
        sample_classification_data : tuple
            A tuple containing feature and target arrays.

        Returns
        -------
        None
        """
        x, y, _ = sample_classification_data
        result = fit_performance.accuracy_predictions(
            dummy_classifier, y, x, f1_score_average="micro"
        )
        assert isinstance(result["f1_score"], float)


class TestFittingPerfEval:
    """Tests for fitting_perf_eval method."""

    def test_fitting_perf_eval_returns_dict(self, fit_performance: FitPerformance,
                                           sample_classification_data: tuple) -> None:
        """Test fitting_perf_eval returns expected dictionary structure.
        
        Parameters
        ----------
        fit_performance : FitPerformance
            An instance of FitPerformance.
        sample_classification_data : tuple
            A tuple containing feature and target arrays.

        Returns
        -------
        None
        """
        _, y, y_hat = sample_classification_data
        result = fit_performance.fitting_perf_eval(y, y_hat)
        assert isinstance(result, dict)
        assert "accuracy" in result
        assert "precision" in result
        assert "recall_sensitivity" in result
        assert "f1_score" in result
        assert "r2_score" in result

    def test_fitting_perf_eval_with_perfect_predictions(self, fit_performance: FitPerformance) \
        -> None:
        """Test fitting_perf_eval with perfect predictions.
        
        Parameters
        ----------
        fit_performance : FitPerformance
            An instance of FitPerformance.

        Returns
        -------
        None
        """
        y = np.array([0, 1, 0, 1])
        y_hat = y.copy()
        result = fit_performance.fitting_perf_eval(y, y_hat)
        assert result["accuracy"] == 1.0
        assert result["precision"] == 1.0
        assert result["recall_sensitivity"] == 1.0
        assert result["f1_score"] == 1.0

    def test_fitting_perf_eval_with_random_predictions(self, fit_performance: FitPerformance) \
        -> None:
        """Test fitting_perf_eval with random predictions.
        
        Parameters
        ----------
        fit_performance : FitPerformance
            An instance of FitPerformance.

        Returns
        -------
        None
        """
        y = np.random.randint(0, 2, 100)
        y_hat = np.random.randint(0, 2, 100)
        result = fit_performance.fitting_perf_eval(y, y_hat)
        assert 0 <= result["accuracy"] <= 1
        assert 0 <= result["precision"] <= 1
        assert 0 <= result["recall_sensitivity"] <= 1
        assert 0 <= result["f1_score"] <= 1


class TestTypeChecking:
    """Tests for type checking functionality."""

    def test_invalid_input_types_raise_errors(self, fit_performance: FitPerformance) -> None:
        """Test that invalid input types raise TypeError.
        
        Parameters
        ----------
        fit_performance : FitPerformance
            An instance of FitPerformance.

        Returns
        -------
        None
        """
        with pytest.raises(TypeError):
            fit_performance.max_llf(np.array([1]), "not an array", np.array([1]))

        with pytest.raises(TypeError):
            fit_performance.aic("not an array", np.array([1]), np.array([1]))
        with pytest.raises(TypeError):
            fit_performance.max_llf("not an array", np.array([1]), np.array([1]))

        with pytest.raises(TypeError):
            fit_performance.aic(np.array([1]), "not an array", np.array([1]))

        with pytest.raises(TypeError):
            fit_performance.fitting_perf_eval(np.array([1]), "not an array")