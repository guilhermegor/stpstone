"""Unit tests for model fitting performance assessment module."""

import numpy as np
import pytest
from sklearn.base import BaseEstimator
from sklearn.dummy import DummyClassifier, DummyRegressor
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LinearRegression, LogisticRegression


@pytest.fixture
def regression_data() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Generate regression test data."""
    np.random.seed(42)
    X = np.random.rand(100, 5)
    y = 2 * X[:, 0] + 3 * X[:, 1] + np.random.normal(0, 0.1, 100)
    y_hat = 2 * X[:, 0] + 3 * X[:, 1]
    return X, y, y_hat


@pytest.fixture
def classification_data() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Generate classification test data."""
    np.random.seed(42)
    X = np.random.rand(100, 5)
    y = (X[:, 0] + X[:, 1] > 1).astype(int)
    y_hat = (X[:, 0] + X[:, 1] > 0.8).astype(int)
    return X, y, y_hat


@pytest.fixture
def dummy_regressor(regression_data) -> tuple[BaseEstimator, np.ndarray, np.ndarray]:
    """Create a dummy regressor model."""
    X, y, _ = regression_data
    model = DummyRegressor(strategy="mean").fit(X, y)
    return model, X, y


@pytest.fixture
def dummy_classifier(classification_data) -> tuple[BaseEstimator, np.ndarray, np.ndarray]:
    """Create a dummy classifier model."""
    X, y, _ = classification_data
    model = DummyClassifier(strategy="most_frequent").fit(X, y)
    return model, X, y


@pytest.fixture
def fit_performance() -> FitPerformance:
    """Create FitPerformance instance."""
    return FitPerformance()


class TestMaxLLF:
    """Tests for max_llf method."""

    def test_max_llf_returns_float(self, fit_performance, regression_data):
        """Test max_llf returns float value."""
        X, y, y_hat = regression_data
        result = fit_performance.max_llf(X, y, y_hat)
        assert isinstance(result, float)

    def test_max_llf_with_perfect_fit(self, fit_performance, regression_data):
        """Test max_llf with perfect predictions."""
        X, y, _ = regression_data
        result = fit_performance.max_llf(X, y, y)
        assert result > -100  # Should be relatively high for perfect fit

    def test_max_llf_with_random_predictions(self, fit_performance, regression_data):
        """Test max_llf with random predictions."""
        X, y, _ = regression_data
        y_random = np.random.rand(*y.shape)
        result = fit_performance.max_llf(X, y, y_random)
        assert result < -100  # Should be relatively low for random predictions

    def test_max_llf_with_empty_inputs(self, fit_performance):
        """Test max_llf with empty arrays."""
        empty_array = np.array([]).reshape(0, 0)
        with pytest.raises(IndexError):
            fit_performance.max_llf(empty_array, np.array([]), np.array([]))


class TestAIC:
    """Tests for aic method."""

    def test_aic_returns_float(self, fit_performance, regression_data):
        """Test aic returns float value."""
        X, y, y_hat = regression_data
        result = fit_performance.aic(X, y, y_hat)
        assert isinstance(result, float)

    def test_aic_with_perfect_fit(self, fit_performance, regression_data):
        """Test aic with perfect predictions."""
        X, y, _ = regression_data
        result = fit_performance.aic(X, y, y)
        assert result < 100  # Should be relatively low for perfect fit

    def test_aic_with_random_predictions(self, fit_performance, regression_data):
        """Test aic with random predictions."""
        X, y, _ = regression_data
        y_random = np.random.rand(*y.shape)
        result = fit_performance.aic(X, y, y_random)
        assert result > 100  # Should be relatively high for random predictions

    def test_aic_increases_with_complexity(self, fit_performance, regression_data):
        """Test aic increases with model complexity."""
        X, y, y_hat = regression_data
        aic_simple = fit_performance.aic(X[:, :1], y, y_hat)
        aic_complex = fit_performance.aic(X, y, y_hat)
        assert aic_complex > aic_simple


class TestBIC:
    """Tests for bic method."""

    def test_bic_returns_float(self, fit_performance, regression_data):
        """Test bic returns float value."""
        X, y, y_hat = regression_data
        result = fit_performance.bic(X, y, y_hat)
        assert isinstance(result, float)

    def test_bic_with_perfect_fit(self, fit_performance, regression_data):
        """Test bic with perfect predictions."""
        X, y, _ = regression_data
        result = fit_performance.bic(X, y, y)
        assert result < 100  # Should be relatively low for perfect fit

    def test_bic_with_random_predictions(self, fit_performance, regression_data):
        """Test bic with random predictions."""
        X, y, _ = regression_data
        y_random = np.random.rand(*y.shape)
        result = fit_performance.bic(X, y, y_random)
        assert result > 100  # Should be relatively high for random predictions

    def test_bic_penalizes_complexity_more_than_aic(self, fit_performance, regression_data):
        """Test bic penalizes complexity more than aic."""
        X, y, y_hat = regression_data
        aic = fit_performance.aic(X, y, y_hat)
        bic = fit_performance.bic(X, y, y_hat)
        assert bic > aic


class TestCrossValidation:
    """Tests for cross_validation method."""

    def test_cross_validation_returns_dict(self, fit_performance, dummy_regressor):
        """Test cross_validation returns dictionary."""
        model, X, y = dummy_regressor
        result = fit_performance.cross_validation(model, X, y)
        assert isinstance(result, dict)

    def test_cross_validation_score_has_expected_keys(self, fit_performance, dummy_regressor):
        """Test cross_validation returns expected keys."""
        model, X, y = dummy_regressor
        result = fit_performance.cross_validation(model, X, y)
        assert set(result.keys()) == {"scores", "mean", "standard_deviation"}

    def test_cross_validation_predict_returns_array(self, fit_performance, dummy_classifier):
        """Test cross_validation with predict returns array."""
        model, X, y = dummy_classifier
        result = fit_performance.cross_validation(
            model, X, y, cross_val_model="predict"
        )
        assert isinstance(result["scores"], np.ndarray)

    def test_cross_validation_invalid_method_raises_error(self, fit_performance, dummy_regressor):
        """Test invalid cross_val_model raises error."""
        model, X, y = dummy_regressor
        with pytest.raises(ValueError):
            fit_performance.cross_validation(model, X, y, cross_val_model="invalid")


class TestGridSearchOptimalHyperparameters:
    """Tests for grid_search_optimal_hyperparameters method."""

    @pytest.fixture
    def param_grid(self) -> dict:
        """Create parameter grid for testing."""
        return {"max_depth": [3, 5], "n_estimators": [10, 20]}

    def test_grid_search_returns_dict(self, fit_performance, regression_data, param_grid):
        """Test grid_search returns dictionary."""
        X, y, _ = regression_data
        model = RandomForestRegressor(random_state=42)
        result = fit_performance.grid_search_optimal_hyperparameters(
            model, param_grid, "neg_mean_squared_error", X, y, bl_randomized_search=False
        )
        assert isinstance(result, dict)

    def test_grid_search_has_expected_keys(self, fit_performance, regression_data, param_grid):
        """Test grid_search returns expected keys."""
        X, y, _ = regression_data
        model = RandomForestRegressor(random_state=42)
        result = fit_performance.grid_search_optimal_hyperparameters(
            model, param_grid, "neg_mean_squared_error", X, y, bl_randomized_search=False
        )
        expected_keys = {
            "best_parameters", "score", "best_estimator", "feature_importance",
            "cv_results", "model_regression", "predict", "mse", "rmse"
        }
        assert set(result.keys()) == expected_keys

    def test_randomized_search_works(self, fit_performance, regression_data, param_grid):
        """Test randomized search works."""
        X, y, _ = regression_data
        model = RandomForestRegressor(random_state=42)
        result = fit_performance.grid_search_optimal_hyperparameters(
            model, param_grid, "neg_mean_squared_error", X, y, bl_randomized_search=True
        )
        assert "best_parameters" in result

    def test_grid_search_with_classifier(self, fit_performance, classification_data, param_grid):
        """Test grid_search with classifier."""
        X, y, _ = classification_data
        model = RandomForestClassifier(random_state=42)
        result = fit_performance.grid_search_optimal_hyperparameters(
            model, param_grid, "accuracy", X, y, bl_randomized_search=False
        )
        assert "best_parameters" in result


class TestAccuracyPredictions:
    """Tests for accuracy_predictions method."""

    def test_accuracy_predictions_returns_dict(self, fit_performance, dummy_classifier):
        """Test accuracy_predictions returns dictionary."""
        model, X, y = dummy_classifier
        result = fit_performance.accuracy_predictions(model, y, X)
        assert isinstance(result, dict)

    def test_accuracy_predictions_has_expected_keys(self, fit_performance, dummy_classifier):
        """Test accuracy_predictions returns expected keys."""
        model, X, y = dummy_classifier
        result = fit_performance.accuracy_predictions(model, y, X)
        expected_keys = {
            "cross_validation_scores", "confusion_matrix", "precision_score",
            "recall_score", "f1_score", "roc_auc_score"
        }
        assert set(result.keys()) == expected_keys

    def test_accuracy_predictions_with_different_f1_average(self, fit_performance, dummy_classifier):
        """Test accuracy_predictions with different f1 averaging."""
        model, X, y = dummy_classifier
        result = fit_performance.accuracy_predictions(
            model, y, X, f1_score_average="weighted"
        )
        assert isinstance(result["f1_score"], float)

    def test_accuracy_predictions_with_predict_proba(self, fit_performance, dummy_classifier):
        """Test accuracy_predictions with predict_proba."""
        model, X, y = dummy_classifier
        result = fit_performance.accuracy_predictions(
            model, y, X, cross_val_model_method="predict_proba"
        )
        assert isinstance(result["cross_validation_scores"], np.ndarray)


class TestFittingPerfEval:
    """Tests for fitting_perf_eval method."""

    def test_fitting_perf_eval_returns_dict(self, fit_performance, regression_data):
        """Test fitting_perf_eval returns dictionary."""
        _, y, y_hat = regression_data
        result = fit_performance.fitting_perf_eval(y, y_hat)
        assert isinstance(result, dict)

    def test_fitting_perf_eval_has_expected_keys(self, fit_performance, regression_data):
        """Test fitting_perf_eval returns expected keys."""
        _, y, y_hat = regression_data
        result = fit_performance.fitting_perf_eval(y, y_hat)
        expected_keys = {
            "accuracy", "precision", "recall_sensitivity", "f1_score", "r2_score"
        }
        assert set(result.keys()) == expected_keys

    def test_fitting_perf_eval_with_perfect_fit(self, fit_performance, regression_data):
        """Test fitting_perf_eval with perfect predictions."""
        _, y, _ = regression_data
        result = fit_performance.fitting_perf_eval(y, y)
        assert result["accuracy"] == 1.0
        assert result["r2_score"] == 1.0

    def test_fitting_perf_eval_with_random_predictions(self, fit_performance, regression_data):
        """Test fitting_perf_eval with random predictions."""
        _, y, _ = regression_data
        y_random = np.random.rand(*y.shape)
        result = fit_performance.fitting_perf_eval(y, y_random)
        assert result["accuracy"] < 1.0
        assert result["r2_score"] < 1.0