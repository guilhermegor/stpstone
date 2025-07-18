"""Unit tests for linear and nonlinear regression implementations.

This module contains tests for all regression methods in the LinearRegressions,
NonLinearRegressions, LogLinearRegressions, and NonLinearEquations classes.
"""

from unittest.mock import MagicMock, patch
import warnings

import numpy as np
import pytest
from scipy.optimize import OptimizeWarning
from sklearn.exceptions import NotFittedError

from stpstone.analytics.quant.regression import (
    LinearRegressions,
    LogLinearRegressions,
    NonLinearEquations,
    NonLinearRegressions,
)


# --------------------------
# Fixtures
# --------------------------


@pytest.fixture
def linear_regressions() -> LinearRegressions:
    """Fixture providing LinearRegressions instance."""
    from stpstone.analytics.quant.regression import LinearRegressions
    return LinearRegressions()


@pytest.fixture
def nonlinear_regressions() -> NonLinearRegressions:
    """Fixture providing NonLinearRegressions instance."""
    return NonLinearRegressions()


@pytest.fixture
def loglinear_regressions() -> LogLinearRegressions:
    """Fixture providing LogLinearRegressions instance."""
    return LogLinearRegressions()


@pytest.fixture
def nonlinear_equations() -> NonLinearEquations:
    """Fixture providing NonLinearEquations instance."""
    return NonLinearEquations()


@pytest.fixture
def sample_1d_data() -> tuple[np.ndarray, np.ndarray]:
    """Fixture providing 1D sample data for regression tests."""
    x = np.array([1, 2, 3, 4, 5]).reshape(-1, 1)
    y = np.array([2.1, 3.9, 6.2, 8.1, 9.8])
    return x, y


@pytest.fixture
def sample_2d_data() -> tuple[np.ndarray, np.ndarray]:
    """Fixture providing 2D sample data for regression tests."""
    x = np.array([[1, 0.5], [2, 1.0], [3, 1.5], [4, 2.0], [5, 2.5]])
    y = np.array([2.1, 3.9, 6.2, 8.1, 9.8])
    return x, y


@pytest.fixture
def sample_classification_data() -> tuple[np.ndarray, np.ndarray]:
    """Fixture providing sample data for classification tests."""
    x = np.array([[1, 0.5], [2, 1.0], [3, 1.5], [4, 2.0], [5, 2.5]])
    y = np.array([0, 0, 1, 1, 1])
    return x, y


@pytest.fixture
def linear_func() -> callable:
    """Fixture providing a simple linear function for curve fitting."""
    def func(x: np.ndarray, a: float, b: float) -> np.ndarray:
        return a * x + b
    return func


@pytest.fixture
def cost_func() -> callable:
    """Fixture providing a simple cost function for optimization."""
    def func(params: np.ndarray) -> float:
        return np.sum(params**2)
    return func


# --------------------------
# LinearRegressions Tests
# --------------------------


class TestLinearRegressions:
    """Test cases for LinearRegressions class."""

    def test_normal_equation_valid_input(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test normal_equation with valid input."""
        x, y = sample_1d_data
        result = linear_regressions.normal_equation(x, y)
        assert isinstance(result, tuple)
        assert len(result) == 4  # theta, residuals, rank, singular_values
        assert isinstance(result[0], np.ndarray)

    def test_normal_equation_non_optimized(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test normal_equation with bl_optimize=False."""
        x, y = sample_1d_data
        result = linear_regressions.normal_equation(x, y, bl_optimize=False)
        assert isinstance(result, np.ndarray)

    def test_normal_equation_invalid_input_type(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test normal_equation raises TypeError with invalid input types."""
        x, y = sample_1d_data
        with pytest.raises(TypeError):
            linear_regressions.normal_equation(list(x), y)
        with pytest.raises(TypeError):
            linear_regressions.normal_equation(x, list(y))

    def test_normal_equation_empty_input(
        self, linear_regressions: LinearRegressions
    ) -> None:
        """Test normal_equation raises ValueError with empty input."""
        with pytest.raises(ValueError):
            linear_regressions.normal_equation(np.array([]), np.array([]))

    def test_batch_gradient_descent_valid_input(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test batch_gradient_descent with valid input."""
        x, y = sample_1d_data
        result = linear_regressions.batch_gradient_descent(x, y)
        assert isinstance(result, np.ndarray)
        assert result.shape == (2, 1)

    def test_batch_gradient_descent_invalid_eta(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test batch_gradient_descent raises ValueError with invalid eta."""
        x, y = sample_1d_data
        with pytest.raises(ValueError):
            linear_regressions.batch_gradient_descent(x, y, eta=-0.1)

    def test_batch_gradient_descent_invalid_max_iter(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test batch_gradient_descent raises ValueError with invalid max_iter."""
        x, y = sample_1d_data
        with pytest.raises(ValueError):
            linear_regressions.batch_gradient_descent(x, y, max_iter=0)

    def test_stochastic_gradient_descent_implemented(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test stochastic_gradient_descent with implemented method."""
        x, y = sample_1d_data
        result = linear_regressions.stochastic_gradient_descent(
            x, y, method="implemented"
        )
        assert isinstance(result, np.ndarray)

    def test_stochastic_gradient_descent_sklearn(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test stochastic_gradient_descent with sklearn method."""
        x, y = sample_1d_data
        result = linear_regressions.stochastic_gradient_descent(x, y)
        assert isinstance(result, dict)
        assert "model_fitted" in result
        assert "predictions" in result

    def test_stochastic_gradient_descent_invalid_method(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test stochastic_gradient_descent raises ValueError with invalid method."""
        x, y = sample_1d_data
        with pytest.raises(ValueError):
            linear_regressions.stochastic_gradient_descent(x, y, method="invalid")

    def test_linear_regression_valid_input(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test linear_regression with valid input."""
        x, y = sample_1d_data
        result = linear_regressions.linear_regression(x, y)
        assert isinstance(result, dict)
        assert "model_fitted" in result
        assert "score" in result
        assert result["score"] >= 0

    def test_k_neighbors_regression_valid_input(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test k_neighbors_regression with valid input."""
        x, y = sample_1d_data
        result = linear_regressions.k_neighbors_regression(x, y)
        assert isinstance(result, dict)
        assert "model_fitted" in result
        assert "score" in result

    def test_polynomial_equations_valid_input(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test polynomial_equations with valid input."""
        x, y = sample_1d_data
        result = linear_regressions.polynomial_equations(x, y)
        assert isinstance(result, dict)
        assert "model_fitted" in result
        assert "poly_features" in result

    def test_polynomial_equations_invalid_degree(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test polynomial_equations raises ValueError with invalid degree."""
        x, y = sample_1d_data
        with pytest.raises(ValueError):
            linear_regressions.polynomial_equations(x, y, int_degree=0)

    def test_ridge_regression_valid_input(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test ridge_regression with valid input."""
        x, y = sample_1d_data
        result = linear_regressions.ridge_regression(x, y)
        assert isinstance(result, dict)
        assert "model_fitted" in result
        assert "predictions" in result

    def test_ridge_regression_invalid_alpha(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test ridge_regression raises ValueError with negative alpha."""
        x, y = sample_1d_data
        with pytest.raises(ValueError):
            linear_regressions.ridge_regression(x, y, alpha=-1.0)

    def test_lasso_regression_valid_input(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test lasso_regression with valid input."""
        x, y = sample_1d_data
        result = linear_regressions.lasso_regression(x, y)
        assert isinstance(result, dict)
        assert "model_fitted" in result
        assert "predictions" in result

    def test_lasso_regression_invalid_alpha(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test lasso_regression raises ValueError with invalid alpha."""
        x, y = sample_1d_data
        with pytest.raises(ValueError):
            linear_regressions.lasso_regression(x, y, alpha=0)

    def test_elastic_net_regression_valid_input(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test elastic_net_regression with valid input."""
        x, y = sample_1d_data
        result = linear_regressions.elastic_net_regression(x, y)
        assert isinstance(result, dict)
        assert "model_fitted" in result
        assert "predictions" in result

    def test_elastic_net_regression_invalid_params(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test elastic_net_regression raises ValueError with invalid params."""
        x, y = sample_1d_data
        with pytest.raises(ValueError):
            linear_regressions.elastic_net_regression(x, y, alpha=0)
        with pytest.raises(ValueError):
            linear_regressions.elastic_net_regression(x, y, l1_ratio=1.1)


# --------------------------
# NonLinearRegressions Tests
# --------------------------


class TestNonLinearRegressions:
    """Test cases for NonLinearRegressions class."""

    def test_decision_tree_regression_valid_input(
        self, nonlinear_regressions: NonLinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test decision_tree_regression with valid input."""
        x, y = sample_1d_data
        result = nonlinear_regressions.decision_tree_regression(x, y)
        assert isinstance(result, dict)
        assert "model_fitted" in result
        assert "score" in result

    def test_random_forest_regression_valid_input(
        self, nonlinear_regressions: NonLinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test random_forest_regression with valid input."""
        x, y = sample_1d_data
        result = nonlinear_regressions.random_forest_regression(x, y)
        assert isinstance(result, dict)
        assert "model_fitted" in result
        assert "features_importance" in result

    def test_random_forest_regression_invalid_n_estimators(
        self, nonlinear_regressions: NonLinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test random_forest_regression raises ValueError with invalid n_estimators."""
        x, y = sample_1d_data
        with pytest.raises(ValueError):
            nonlinear_regressions.random_forest_regression(x, y, n_estimators=0)

    def test_support_vector_regression_valid_input(
        self, nonlinear_regressions: NonLinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test support_vector_regression with valid input."""
        x, y = sample_1d_data
        result = nonlinear_regressions.support_vector_regression(x, y)
        assert isinstance(result, dict)
        assert "model_fitted" in result
        assert "score" in result

    def test_support_vector_regression_invalid_params(
        self, nonlinear_regressions: NonLinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test support_vector_regression raises ValueError with invalid params."""
        x, y = sample_1d_data
        with pytest.raises(ValueError):
            nonlinear_regressions.support_vector_regression(x, y, int_degree=0)
        with pytest.raises(ValueError):
            nonlinear_regressions.support_vector_regression(x, y, c_positive_floating_point_number=0)
        with pytest.raises(ValueError):
            nonlinear_regressions.support_vector_regression(x, y, epsilon=0)


# --------------------------
# LogLinearRegressions Tests
# --------------------------


class TestLogLinearRegressions:
    """Test cases for LogLinearRegressions class."""

    def test_logistic_regression_logit_valid_input(
        self, loglinear_regressions: LogLinearRegressions, sample_classification_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test logistic_regression_logit with valid input."""
        x, y = sample_classification_data
        result = loglinear_regressions.logistic_regression_logit(x, y)
        assert isinstance(result, dict)
        assert "model_fitted" in result
        assert "confusion_matrix" in result

    def test_logistic_regression_logit_invalid_params(
        self, loglinear_regressions: LogLinearRegressions, sample_classification_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test logistic_regression_logit raises ValueError with invalid params."""
        x, y = sample_classification_data
        with pytest.raises(ValueError):
            loglinear_regressions.logistic_regression_logit(
                x, y, c_positive_floating_point_number=0
            )
        with pytest.raises(ValueError):
            loglinear_regressions.logistic_regression_logit(x, y, int_max_iter=0)
        with pytest.raises(ValueError):
            loglinear_regressions.logistic_regression_logit(x, y, float_tolerance=0)


# --------------------------
# NonLinearEquations Tests
# --------------------------


class TestNonLinearEquations:
    """Test cases for NonLinearEquations class."""

    def test_differential_evolution_scipy(
        self, nonlinear_equations: NonLinearEquations, cost_func: callable
    ) -> None:
        """Test differential_evolution with scipy method."""
        bounds = [(-1, 1), (-1, 1)]
        result = nonlinear_equations.differential_evolution(cost_func, bounds)
        assert hasattr(result, "x")  # Check for OptimizeResult attributes
        assert hasattr(result, "success")

    def test_differential_evolution_mystic(
        self, nonlinear_equations: NonLinearEquations, cost_func: callable
    ) -> None:
        """Test differential_evolution with mystic method."""
        bounds = [(-1, 1), (-1, 1)]
        result = nonlinear_equations.differential_evolution(
            cost_func, bounds, method="mystic"
        )
        assert isinstance(result, tuple)
        assert len(result) >= 2  # Check for mystic's return tuple

    def test_differential_evolution_invalid_method(
        self, nonlinear_equations: NonLinearEquations, cost_func: callable
    ) -> None:
        """Test differential_evolution raises ValueError with invalid method."""
        bounds = [(-1, 1), (-1, 1)]
        with pytest.raises(ValueError):
            nonlinear_equations.differential_evolution(
                cost_func, bounds, method="invalid"
            )

    def test_differential_evolution_invalid_bounds(
        self, nonlinear_equations: NonLinearEquations, cost_func: callable
    ) -> None:
        """Test differential_evolution raises ValueError with invalid bounds."""
        with pytest.raises(ValueError):
            nonlinear_equations.differential_evolution(cost_func, [])
        with pytest.raises(ValueError):
            nonlinear_equations.differential_evolution(cost_func, [(-1, 1, 2)])  # Invalid tuple length

    def test_optimize_curve_fit_valid_input(
        self, nonlinear_equations: NonLinearEquations, sample_1d_data: tuple[np.ndarray, np.ndarray], linear_func: callable
    ) -> None:
        """Test optimize_curve_fit with valid input."""
        x, y = sample_1d_data
        popt, pcov = nonlinear_equations.optimize_curve_fit(linear_func, x, y)
        assert isinstance(popt, np.ndarray)
        assert isinstance(pcov, np.ndarray)

    def test_optimize_curve_fit_invalid_func(
        self, nonlinear_equations: NonLinearEquations, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test optimize_curve_fit raises TypeError with invalid function."""
        x, y = sample_1d_data
        with pytest.raises(TypeError):
            nonlinear_equations.optimize_curve_fit("not a function", x, y)

    def test_polynomial_fit_valid_input(
        self, nonlinear_equations: NonLinearEquations, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test polynomial_fit with valid input."""
        x, y = sample_1d_data
        result = nonlinear_equations.polynomial_fit(x, y)
        assert isinstance(result, dict)
        assert "coefficients" in result
        assert "values_interpolated" in result

    def test_polynomial_fit_invalid_input(
        self, nonlinear_equations: NonLinearEquations
    ) -> None:
        """Test polynomial_fit raises ValueError with empty input."""
        with pytest.raises(ValueError):
            nonlinear_equations.polynomial_fit(np.array([]), np.array([]))


# --------------------------
# Edge Case Tests
# --------------------------


class TestEdgeCases:
    """Test cases for edge conditions and error handling."""

    def test_single_point_input(
        self, linear_regressions: LinearRegressions
    ) -> None:
        """Test methods with single data point input."""
        x = np.array([[1]])
        y = np.array([2])
        
        # Test linear regression
        result = linear_regressions.linear_regression(x, y)
        assert result["score"] == 1.0  # Perfect fit with single point
        
        # Test polynomial fit
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", OptimizeWarning)
            result = linear_regressions.polynomial_equations(x, y)
            assert result["score"] == 1.0

    def test_large_input(
        self, linear_regressions: LinearRegressions
    ) -> None:
        """Test methods with large input arrays."""
        x = np.random.rand(1000, 1)
        y = 2 * x.ravel() + 1 + 0.1 * np.random.randn(1000)
        
        result = linear_regressions.linear_regression(x, y)
        assert result["score"] > 0.9  # Should still fit well

    def test_high_dimension_input(
        self, linear_regressions: LinearRegressions
    ) -> None:
        """Test methods with high-dimensional input."""
        x = np.random.rand(100, 10)
        y = np.random.rand(100)
        
        result = linear_regressions.linear_regression(x, y)
        assert result["coefficients"].shape == (10,)

    def test_extreme_values(
        self, linear_regressions: LinearRegressions
    ) -> None:
        """Test methods with extreme numeric values."""
        x = np.array([[1e100], [1e100]])
        y = np.array([1e100, 1e100])
        
        result = linear_regressions.linear_regression(x, y)
        assert np.isfinite(result["coefficients"][0])

    def test_nan_input(
        self, linear_regressions: LinearRegressions
    ) -> None:
        """Test methods with NaN input."""
        x = np.array([[1], [np.nan]])
        y = np.array([2, 3])
        
        with pytest.raises(ValueError):
            linear_regressions.linear_regression(x, y)


# --------------------------
# Type Validation Tests
# --------------------------


class TestTypeValidation:
    """Test cases for type validation."""

    def test_invalid_input_types(
        self, linear_regressions: LinearRegressions
    ) -> None:
        """Test methods with invalid input types."""
        with pytest.raises(TypeError):
            linear_regressions.linear_regression("not an array", np.array([1]))
        with pytest.raises(TypeError):
            linear_regressions.linear_regression(np.array([1]), "not an array")

    def test_return_type_validation(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test that return types match declared types."""
        x, y = sample_1d_data
        
        # Test linear_regression return types
        result = linear_regressions.linear_regression(x, y)
        assert isinstance(result["model_fitted"], LinearRegressions)
        assert isinstance(result["score"], float)
        assert isinstance(result["coefficients"], np.ndarray)
        
        # Test polynomial_equations return types
        result = linear_regressions.polynomial_equations(x, y)
        assert isinstance(result["model_fitted"], LinearRegressions)
        assert isinstance(result["score"], float)
        assert isinstance(result["coefficients"], np.ndarray)


# --------------------------
# Fallback Logic Tests
# --------------------------


class TestFallbackLogic:
    """Test cases for fallback and error recovery logic."""

    @patch("scipy.optimize.curve_fit", side_effect=RuntimeError)
    def test_curve_fit_fallback(
        self, mock_curve_fit: MagicMock, nonlinear_equations: NonLinearEquations, 
        sample_1d_data: tuple[np.ndarray, np.ndarray], linear_func: callable
    ) -> None:
        """Test behavior when curve_fit fails."""
        x, y = sample_1d_data
        with pytest.raises(RuntimeError):
            nonlinear_equations.optimize_curve_fit(linear_func, x, y)

    @patch("sklearn.linear_model.LinearRegressions.fit", side_effect=ValueError)
    def test_linear_regression_fit_failure(
        self, mock_fit: MagicMock, linear_regressions: LinearRegressions, 
        sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test behavior when LinearRegressions.fit fails."""
        x, y = sample_1d_data
        with pytest.raises(ValueError):
            linear_regressions.linear_regression(x, y)

    def test_missing_optional_deps(
        self, linear_regressions: LinearRegressions, sample_1d_data: tuple[np.ndarray, np.ndarray]
    ) -> None:
        """Test behavior when optional dependencies are missing."""
        x, y = sample_1d_data

        # Simulate missing scipy
        with patch.dict("sys.modules", {"scipy.optimize": None}):
            with pytest.raises(ImportError):
            
                _ = NonLinearEquations()
        
        # Simulate missing sklearn
        with patch.dict("sys.modules", {"sklearn.linear_model": None}):
            with pytest.raises(ImportError):
                
                _ = LinearRegressions()