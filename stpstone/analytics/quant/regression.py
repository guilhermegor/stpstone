"""Regression Module.

This module provides classes and functions for performing linear and nonlinear regression
analysis."""

from typing import Any, Callable, Literal, Optional, TypedDict, Union

from mystic.monitors import VerboseMonitor
from mystic.solvers import diffev2
import numpy as np
from numpy.typing import NDArray
from scipy.optimize import OptimizeResult, curve_fit, differential_evolution
from sklearn.base import BaseEstimator
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import (
    ElasticNet,
    Lasso,
    LinearRegression,
    LogisticRegression,
    Ridge,
    SGDRegressor,
)
from sklearn.metrics import (
    class_likelihood_ratios,
    classification_report,
    confusion_matrix,
)
from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import PolynomialFeatures
from sklearn.svm import SVR
from sklearn.tree import DecisionTreeRegressor
from typing_extensions import NotRequired

from stpstone.transformations.cleaner.eda import ExploratoryDataAnalysis
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class LinearRegressionsResult(TypedDict):
    """TypedDict for LinearRegressions results."""
    model_fitted: BaseEstimator
    score: float
    predictions: NDArray[np.float64]
    intercept: NotRequired[NDArray[np.float64]]
    coefficients: NotRequired[NDArray[np.float64]]
    theta_best: NotRequired[NDArray[np.float64]]
    poly_features: NotRequired[NDArray[np.float64]]
    features_importance: NotRequired[NDArray[np.float64]]

class NonLinearRegressionsResult(TypedDict):
    """TypedDict for NonLinearRegressions results."""
    model_fitted: BaseEstimator
    score: float
    predictions: NDArray[np.float64]
    features_importance: NotRequired[NDArray[np.float64]]

class LogLinearRegressionsResult(TypedDict):
    """TypedDict for LogLinearRegressions results."""
    model_fitted: LogisticRegression
    classes: NDArray[np.float64]
    intercept: NDArray[np.float64]
    coefficients: NDArray[np.float64]
    predict_probability: NDArray[np.float64]
    predictions: NDArray[np.float64]
    score: float
    confusion_matrix: NDArray[np.int64]
    classification_report: dict[str, Any]
    log_likelihood: tuple[float, float]

class NonLinearEquationsResult(TypedDict):
    """TypedDict for NonLinearEquations results."""
    coefficients: NDArray[np.float64]
    values_interpolated: NDArray[np.float64]

class LinearRegressions(metaclass=TypeChecker):
    """Class for performing linear regression analysis."""

    def normal_equation(
        self, 
        array_x: np.ndarray, 
        array_y: np.ndarray, 
        bl_optimize: Optional[bool] = True
    ) -> Union[
        NDArray[np.float64], 
        tuple[NDArray[np.float64], NDArray[np.float64], int, NDArray[np.float64]]
    ]:
        """
        Normal equation to find the value of theta that minimizes the cost function.

        Parameters
        ----------
        array_x : np.ndarray
            Input feature array
        array_y : np.ndarray
            Target values
        bl_optimize : bool, optional
            Whether to use optimized calculation (default: True)

        Returns
        -------
        Union[np.ndarray, tuple[np.ndarray, np.ndarray, int, np.ndarray]]
            tuple with best theta vector, residuals, rank if bl_optimize=True,
            else just theta vector

        References
        ----------
        .. [1] "Hands-On Machine Learning with Scikit-Learn, Keras, and TensorFlow,
            2nd Edition", by Aurélien Géron (O'Reilly). Copyright 2019 Kiwisoft S.A.S.,
            978-1-492-03264-9.
        """
        if not isinstance(array_x, np.ndarray) or not isinstance(array_y, np.ndarray):
            raise TypeError("Input arrays must be numpy arrays")
        if array_x.size == 0 or array_y.size == 0:
            raise ValueError("Input arrays cannot be empty")

        if bl_optimize:
            return np.linalg.lstsq(array_x, array_y, rcond=None)
        return np.linalg.inv(array_x.T.dot(array_x)).dot(array_x.T).dot(array_y)

    def batch_gradient_descent(
        self,
        array_x: np.ndarray,
        array_y: np.ndarray,
        max_iter: int = 1000,
        eta: float = 0.1,
        m: int = 100,
        theta: np.ndarray = np.random.randn(2, 1),
    ) -> NDArray[np.float64]:
        """
        Batch gradient descent to find the global minimum of a linear function.

        Parameters
        ----------
        array_x : np.ndarray
            Input feature array
        array_y : np.ndarray
            Target values
        max_iter : int, optional
            Maximum iterations (default: 1000)
        eta : float, optional
            Learning rate (default: 0.1)
        m : int, optional
            Number of instances (default: 100)
        theta : np.ndarray, optional
            Initial theta vector (default: random initialization)

        Returns
        -------
        np.ndarray
            Optimized theta vector

        References
        ----------
        .. [1] "Hands-On Machine Learning with Scikit-Learn, Keras, and TensorFlow,
            2nd Edition", by Aurélien Géron (O'Reilly). Copyright 2019 Kiwisoft S.A.S.,
            978-1-492-03264-9.
        """
        if not isinstance(array_x, np.ndarray) or not isinstance(array_y, np.ndarray):
            raise TypeError("Input arrays must be numpy arrays")
        if array_x.size == 0 or array_y.size == 0:
            raise ValueError("Input arrays cannot be empty")
        if max_iter <= 0:
            raise ValueError("max_iter must be positive")
        if eta <= 0:
            raise ValueError("eta must be positive")

        for _ in range(max_iter):
            gradients = 2 / m * array_x.T.dot(array_x.dot(theta) - array_y)
            theta = theta - eta * gradients
        return theta

    def stochastic_gradient_descent(
        self,
        array_x: np.ndarray,
        array_y: np.ndarray,
        method: Literal["implemented", "sklearn"] = "sklearn",
        n_epochs: int = 1000,
        t0: int = 5,
        t1: int = 50,
        m: int = 100,
        theta: np.ndarray = np.random.randn(2, 1),
        tolerance: float = 1e-3,
        penalty: Optional[str] = None,
        eta0: float = 0.1,
    ) -> Union[NDArray[np.float64], LinearRegressionsResult]:
        """
        Stochastic gradient descent optimization.

        Parameters
        ----------
        array_x : np.ndarray
            Input feature array
        array_y : np.ndarray
            Target values
        method : Literal["implemented", "sklearn"], optional
            Implementation method (default: "sklearn")
        n_epochs : int, optional
            Maximum iterations (default: 1000)
        t0 : int, optional
            Learning schedule hyperparameter (default: 5)
        t1 : int, optional
            Learning schedule hyperparameter (default: 50)
        m : int, optional
            Number of instances (default: 100)
        theta : np.ndarray, optional
            Initial theta vector (default: random initialization)
        tolerance : float, optional
            Optimization tolerance (default: 1e-3)
        penalty : Optional[str], optional
            Regularization penalty (default: None)
        eta0 : float, optional
            Initial learning rate (default: 0.1)

        Returns
        -------
        dict[str, Any]
            Results dictionary containing model and predictions

        References
        ----------
        .. [1] "Hands-On Machine Learning with Scikit-Learn, Keras, and TensorFlow,
            2nd Edition", by Aurélien Géron (O'Reilly). Copyright 2019 Kiwisoft S.A.S.,
            978-1-492-03264-9.
        """
        if not isinstance(array_x, np.ndarray) or not isinstance(array_y, np.ndarray):
            raise TypeError("Input arrays must be numpy arrays")
        if array_x.size == 0 or array_y.size == 0:
            raise ValueError("Input arrays cannot be empty")
        if n_epochs <= 0:
            raise ValueError("n_epochs must be positive")
        if eta0 <= 0:
            raise ValueError("eta0 must be positive")

        if method == "implemented":
            def learning_schedule(t): return t0 / (t + t1)
            for epoch in range(n_epochs):
                for i in range(m):
                    random_index = np.random.randint(m)
                    xi = array_x[random_index:random_index + 1]
                    yi = array_y[random_index:random_index + 1]
                    gradients = 2 * xi.T.dot(xi.dot(theta) - yi)
                    eta = learning_schedule(epoch * m + i)
                    theta = theta - eta * gradients
            return theta
        elif method == "sklearn":
            model = SGDRegressor(
                max_iter=n_epochs, tol=tolerance, penalty=penalty, eta0=eta0
            )
            model.fit(array_x, array_y.ravel())
            array_predictions = model.predict(array_x)
            return {
                "model_fitted": model,
                "intercept": model.intercept_,
                "coefficients": model.coef_,
                "predictions": array_predictions,
            }
        else:
            raise ValueError("Method must be either 'implemented' or 'sklearn'")

    def linear_regression(
        self, 
        array_x: np.ndarray, 
        array_y: np.ndarray
    ) -> LinearRegressionsResult:
        """
        Fit linear regression model to data.

        Parameters
        ----------
        array_x : np.ndarray
            Input feature array
        array_y : np.ndarray
            Target values

        Returns
        -------
        dict[str, Any]
            Results dictionary containing model and metrics
        """
        if not isinstance(array_x, np.ndarray) or not isinstance(array_y, np.ndarray):
            raise TypeError("Input arrays must be numpy arrays")
        if array_x.size == 0 or array_y.size == 0:
            raise ValueError("Input arrays cannot be empty")

        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)
        model = LinearRegression().fit(np.array(array_x), np.array(array_y))

        result = {
            "model_fitted": model,
            "score": model.score(np.array(array_x), np.array(array_y)),
            "coefficients": model.coef_,
            "intercept": model.intercept_,
            "predictions": model.predict(np.array(array_x)),
        }

        if array_x.shape[1] == 1:
            result["theta_best"] = np.linalg.inv(np.array(array_x).T.dot(np.array(array_x))).dot(
                np.array(array_x).T).dot(array_y)
        return result

    def k_neighbors_regression(
        self, 
        array_x: np.ndarray, 
        array_y: np.ndarray
    ) -> LinearRegressionsResult:
        """
        Fit k-nearest neighbors regression model to data.

        Parameters
        ----------
        array_x : np.ndarray
            Input feature array
        array_y : np.ndarray
            Target values

        Returns
        -------
        dict[str, Any]
            Results dictionary containing model and metrics
        """
        if not isinstance(array_x, np.ndarray) or not isinstance(array_y, np.ndarray):
            raise TypeError("Input arrays must be numpy arrays")
        if array_x.size == 0 or array_y.size == 0:
            raise ValueError("Input arrays cannot be empty")

        model = KNeighborsRegressor().fit(np.array(array_x), np.array(array_y))
        return {
            "model_fitted": model,
            "intercept": model.intercept_,
            "coefficients": model.coef_,
            "score": model.score(np.array(array_x), np.array(array_y)),
            "predictions": model.predict(np.array(array_x)),
            "theta_best": np.linalg.inv(np.array(array_x).T.dot(np.array(array_x))).dot(
                np.array(array_x).T
            ).dot(array_y),
        }

    def polynomial_equations(
        self,
        array_x: np.ndarray,
        array_y: np.ndarray,
        int_degree: int = 2,
        bl_include_bias: bool = True,
    ) -> LinearRegressionsResult:
        """
        Fit polynomial regression model to data.

        Parameters
        ----------
        array_x : np.ndarray
            Input feature array
        array_y : np.ndarray
            Target values
        int_degree : int, optional
            Polynomial degree (default: 2)
        bl_include_bias : bool, optional
            Whether to include bias term (default: True)

        Returns
        -------
        dict[str, Any]
            Results dictionary containing model and metrics
        """
        if not isinstance(array_x, np.ndarray) or not isinstance(array_y, np.ndarray):
            raise TypeError("Input arrays must be numpy arrays")
        if array_x.size == 0 or array_y.size == 0:
            raise ValueError("Input arrays cannot be empty")
        if int_degree < 1:
            raise ValueError("Degree must be at least 1")

        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)
        poly_features = PolynomialFeatures(
            degree=int_degree, include_bias=bl_include_bias
        )
        array_x_polynom = poly_features.fit_transform(array_x)
        model = LinearRegression()
        model.fit(array_x_polynom, array_y)
        array_predictions = model.predict(array_x_polynom)

        return {
            "model_fitted": model,
            "intercept": model.intercept_,
            "coefficients": model.coef_,
            "score": model.score(np.array(array_x_polynom), np.array(array_y)),
            "predictions": array_predictions,
            "poly_features": poly_features,
        }

    def ridge_regression(
        self,
        array_x: np.ndarray,
        array_y: np.ndarray,
        alpha: float = 0,
        solver_ridge_regression: str = "cholesky",
    ) -> LinearRegressionsResult:
        """
        Fit ridge regression model to data.

        Parameters
        ----------
        array_x : np.ndarray
            Input feature array
        array_y : np.ndarray
            Target values
        alpha : float, optional
            Regularization strength (default: 0)
        solver_ridge_regression : str, optional
            Solver to use (default: "cholesky")

        Returns
        -------
        dict[str, Any]
            Results dictionary containing model and metrics

        References
        ----------
        .. [1] "Hands-On Machine Learning with Scikit-Learn, Keras, and TensorFlow,
            2nd Edition", by Aurélien Géron (O'Reilly). Copyright 2019 Kiwisoft S.A.S.,
            978-1-492-03264-9.
        """
        if not isinstance(array_x, np.ndarray) or not isinstance(array_y, np.ndarray):
            raise TypeError("Input arrays must be numpy arrays")
        if array_x.size == 0 or array_y.size == 0:
            raise ValueError("Input arrays cannot be empty")
        if alpha < 0:
            raise ValueError("alpha must be non-negative")

        ridge_reg = Ridge(alpha=alpha, solver=solver_ridge_regression)
        ridge_reg.fit(array_x, array_y)
        array_predictions = ridge_reg.predict(array_x)

        return {
            "model_fitted": ridge_reg,
            "intercept": ridge_reg.intercept_,
            "coefficients": ridge_reg.coef_,
            "predictions": array_predictions,
        }

    def lasso_regression(
        self, 
        array_x: np.ndarray, 
        array_y: np.ndarray, 
        alpha: float = 0.1
    ) -> LinearRegressionsResult:
        """
        Fit lasso regression model to data.

        Parameters
        ----------
        array_x : np.ndarray
            Input feature array
        array_y : np.ndarray
            Target values
        alpha : float, optional
            Regularization strength (default: 0.1)

        Returns
        -------
        dict[str, Any]
            Results dictionary containing model and metrics

        References
        ----------
        .. [1] "Hands-On Machine Learning with Scikit-Learn, Keras, and TensorFlow,
            2nd Edition", by Aurélien Géron (O'Reilly). Copyright 2019 Kiwisoft S.A.S.,
            978-1-492-03264-9.
        """
        if not isinstance(array_x, np.ndarray) or not isinstance(array_y, np.ndarray):
            raise TypeError("Input arrays must be numpy arrays")
        if array_x.size == 0 or array_y.size == 0:
            raise ValueError("Input arrays cannot be empty")
        if alpha <= 0:
            raise ValueError("alpha must be positive")

        model = Lasso(alpha=alpha)
        model.fit(array_x, array_y)
        array_predictions = model.predict(array_x)

        return {
            "model_fitted": model,
            "intercept": model.intercept_,
            "coefficients": model.coef_,
            "predictions": array_predictions,
        }

    def elastic_net_regression(
        self,
        array_x: np.ndarray,
        array_y: np.ndarray,
        alpha: float = 0.1,
        l1_ratio: float = 0.5,
    ) -> LinearRegressionsResult:
        """
        Fit elastic net regression model to data.

        Parameters
        ----------
        array_x : np.ndarray
            Input feature array
        array_y : np.ndarray
            Target values
        alpha : float, optional
            Regularization strength (default: 0.1)
        l1_ratio : float, optional
            L1 ratio (default: 0.5)

        Returns
        -------
        dict[str, Any]
            Results dictionary containing model and metrics

        References
        ----------
        .. [1] "Hands-On Machine Learning with Scikit-Learn, Keras, and TensorFlow,
            2nd Edition", by Aurélien Géron (O'Reilly). Copyright 2019 Kiwisoft S.A.S.,
            978-1-492-03264-9.
        """
        if not isinstance(array_x, np.ndarray) or not isinstance(array_y, np.ndarray):
            raise TypeError("Input arrays must be numpy arrays")
        if array_x.size == 0 or array_y.size == 0:
            raise ValueError("Input arrays cannot be empty")
        if alpha <= 0:
            raise ValueError("alpha must be positive")
        if not 0 <= l1_ratio <= 1:
            raise ValueError("l1_ratio must be between 0 and 1")

        model = ElasticNet(alpha=alpha, l1_ratio=l1_ratio)
        model.fit(array_x, array_y)
        array_predictions = model.predict(array_x)

        return {
            "model_fitted": model,
            "intercept": model.intercept_,
            "coefficients": model.coef_,
            "predictions": array_predictions,
        }


class NonLinearRegressions:

    def decision_tree_regression(
        self, 
        array_x: np.ndarray, 
        array_y: np.ndarray, 
        seed: Optional[int] = None
    ) -> NonLinearRegressionsResult:
        """
        Fit decision tree regression model to data.

        Parameters
        ----------
        array_x : np.ndarray
            Input feature array
        array_y : np.ndarray
            Target values
        seed : Optional[int], optional
            Random seed (default: None)

        Returns
        -------
        dict[str, Any]
            Results dictionary containing model and metrics
        """
        if not isinstance(array_x, np.ndarray) or not isinstance(array_y, np.ndarray):
            raise TypeError("Input arrays must be numpy arrays")
        if array_x.size == 0 or array_y.size == 0:
            raise ValueError("Input arrays cannot be empty")

        model = DecisionTreeRegressor(random_state=seed).fit(
            np.array(array_x), np.array(array_y)
        )
        return {
            "model_fitted": model,
            "score": model.score(np.array(array_x), np.array(array_y)),
            "predictions": model.predict(np.array(array_x)),
        }

    def random_forest_regression(
        self,
        array_x: np.ndarray,
        array_y: np.ndarray,
        seed: Optional[int] = None,
        n_estimators: int = 100,
    ) -> NonLinearRegressionsResult:
        """
        Fit random forest regression model to data.

        Parameters
        ----------
        array_x : np.ndarray
            Input feature array
        array_y : np.ndarray
            Target values
        seed : Optional[int], optional
            Random seed (default: None)
        n_estimators : int, optional
            Number of trees (default: 100)

        Returns
        -------
        dict[str, Any]
            Results dictionary containing model and metrics
        """
        if not isinstance(array_x, np.ndarray) or not isinstance(array_y, np.ndarray):
            raise TypeError("Input arrays must be numpy arrays")
        if array_x.size == 0 or array_y.size == 0:
            raise ValueError("Input arrays cannot be empty")
        if n_estimators <= 0:
            raise ValueError("n_estimators must be positive")

        model = RandomForestRegressor(
            random_state=seed, n_estimators=n_estimators
        ).fit(np.array(array_x), np.array(array_y))
        return {
            "model_fitted": model,
            "score": model.score(np.array(array_x), np.array(array_y)),
            "features_importance": model.feature_importances_,
            "predictions": model.predict(np.array(array_x)),
        }

    def support_vector_regression(
        self,
        array_x: np.ndarray,
        array_y: np.ndarray,
        kernel: str = "poly",
        int_degree: int = 2,
        c_positive_floating_point_number: float = 100,
        epsilon: float = 0.1,
    ) -> NonLinearRegressionsResult:
        """
        Fit support vector regression model to data.

        Parameters
        ----------
        array_x : np.ndarray
            Input feature array
        array_y : np.ndarray
            Target values
        kernel : str, optional
            Kernel type (default: "poly")
        int_degree : int, optional
            Polynomial degree (default: 2)
        c_positive_floating_point_number : float, optional
            Regularization parameter (default: 100)
        epsilon : float, optional
            Epsilon in epsilon-SVR model (default: 0.1)

        Returns
        -------
        dict[str, Any]
            Results dictionary containing model and metrics
        """
        if not isinstance(array_x, np.ndarray) or not isinstance(array_y, np.ndarray):
            raise TypeError("Input arrays must be numpy arrays")
        if array_x.size == 0 or array_y.size == 0:
            raise ValueError("Input arrays cannot be empty")
        if int_degree <= 0:
            raise ValueError("Degree must be positive")
        if c_positive_floating_point_number <= 0:
            raise ValueError("C must be positive")
        if epsilon <= 0:
            raise ValueError("epsilon must be positive")

        model = SVR(
            kernel=kernel,
            degree=int_degree,
            C=c_positive_floating_point_number,
            epsilon=epsilon,
        )
        model.fit(np.array(array_x), np.array(array_y))
        return {
            "model_fitted": model,
            "score": model.score(np.array(array_x), np.array(array_y)),
            "predictions": model.predict(np.array(array_x)),
        }


class LogLinearRegressions:

    def logistic_regression_logit(
        self,
        array_x: np.ndarray,
        array_y: np.ndarray,
        c_positive_floating_point_number: float = 1.0,
        l1_ratio: Optional[float] = None,
        int_max_iter: int = 100,
        solver: str = "lbfgs",
        penalty: str = "l2",
        mult_class_classifier: str = "auto",
        float_tolerance: float = 0.0001,
        intercept_scaling: int = 1,
        random_state: int = 0,
        verbose: int = 0,
        bl_fit_intercept: bool = True,
        bl_warm_start: bool = False,
        class_weight: Optional[dict] = None,
        bl_dual: bool = False,
        n_jobs: Optional[int] = None,
    ) -> LogLinearRegressionsResult:
        """
        Fit logistic regression model to data.

        Parameters
        ----------
        array_x : np.ndarray
            Input feature array
        array_y : np.ndarray
            Target values
        c_positive_floating_point_number : float, optional
            Inverse of regularization strength (default: 1.0)
        l1_ratio : Optional[float], optional
            ElasticNet mixing parameter (default: None)
        int_max_iter : int, optional
            Maximum iterations (default: 100)
        solver : str, optional
            Optimization solver (default: "lbfgs")
        penalty : str, optional
            Regularization penalty (default: "l2")
        mult_class_classifier : str, optional
            Multiclass handling (default: "auto")
        float_tolerance : float, optional
            Tolerance for stopping (default: 0.0001)
        intercept_scaling : int, optional
            Intercept scaling (default: 1)
        random_state : int, optional
            Random seed (default: 0)
        verbose : int, optional
            Verbosity level (default: 0)
        bl_fit_intercept : bool, optional
            Whether to fit intercept (default: True)
        bl_warm_start : bool, optional
            Whether to reuse solution (default: False)
        class_weight : Optional[dict], optional
            Class weights (default: None)
        bl_dual : bool, optional
            Dual formulation (default: False)
        n_jobs : Optional[int], optional
            Number of CPU cores (default: None)

        Returns
        -------
        dict[str, Any]
            Results dictionary containing model and metrics

        References
        ----------
        .. [1] https://realpython.com/logistic-regression-python/
        .. [2] https://www.udemy.com/course/machinelearning/
        """
        if not isinstance(array_x, np.ndarray) or not isinstance(array_y, np.ndarray):
            raise TypeError("Input arrays must be numpy arrays")
        if array_x.size == 0 or array_y.size == 0:
            raise ValueError("Input arrays cannot be empty")
        if c_positive_floating_point_number <= 0:
            raise ValueError("C must be positive")
        if int_max_iter <= 0:
            raise ValueError("max_iter must be positive")
        if float_tolerance <= 0:
            raise ValueError("tolerance must be positive")

        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)
        model = LogisticRegression(
            C=c_positive_floating_point_number,
            class_weight=class_weight,
            dual=bl_dual,
            fit_intercept=bl_fit_intercept,
            intercept_scaling=intercept_scaling,
            l1_ratio=l1_ratio,
            max_iter=int_max_iter,
            multi_class=mult_class_classifier,
            n_jobs=n_jobs,
            penalty=penalty,
            random_state=random_state,
            solver=solver,
            tol=float_tolerance,
            verbose=verbose,
            warm_start=bl_warm_start,
        ).fit(array_x, array_y)

        return {
            "model_fitted": model,
            "classes": model.classes_,
            "intercept": model.intercept_,
            "coefficient": model.coef_,
            "predict_probability": model.predict_proba(array_x),
            "predictions": model.predict(array_x),
            "score": model.score(array_x, array_y),
            "confusion_matrix": confusion_matrix(array_y, model.predict(array_x)),
            "classification_report": classification_report(
                array_y, model.predict(array_x), output_dict=True
            ),
            "log_likelihood": class_likelihood_ratios(array_y, model.predict(array_x)),
        }


class NonLinearEquations:

    def differential_evolution(
        self,
        cost_func: callable,
        list_bounds: list[tuple[float, float]],
        method: Literal["scipy", "mystic"] = "scipy",
        max_iter: int = 1000,
        max_iterations_wo_improvement: int = 100,
        int_verbose_monitor: int = 10,
        bl_print_convergence_messages: bool = False,
        bl_print_warning_messages: bool = True,
        bl_inter_monitor: bool = False,
        int_size_trial_solution_population: int = 40,
        tolerance: float = 5e-5,
    ) -> Union[OptimizeResult, tuple[NDArray[np.float64], float, int, int, int]]:
        """
        Differential evolution optimization.

        Parameters
        ----------
        cost_func : callable
            Cost function to minimize
        list_bounds : list[tuple[float, float]]
            List of (min, max) pairs for each parameter
        method : Literal["scipy", "mystic"], optional
            Implementation method (default: "scipy")
        max_iter : int, optional
            Maximum iterations (default: 1000)
        max_iterations_wo_improvement : int, optional
            Maximum iterations without improvement (default: 100)
        int_verbose_monitor : int, optional
            Verbosity interval (default: 10)
        bl_print_convergence_messages : bool, optional
            Whether to print convergence messages (default: False)
        bl_print_warning_messages : bool, optional
            Whether to print warnings (default: True)
        bl_inter_monitor : bool, optional
            Whether to use verbose monitor (default: False)
        int_size_trial_solution_population : int, optional
            Population size (default: 40)
        tolerance : float, optional
            Optimization tolerance (default: 5e-5)

        Returns
        -------
        Any
            Optimization result

        References
        ----------
        .. [1] https://stackoverflow.com/questions/21765794/python-constrained-non-linear-optimization
        .. [2] https://mystic.readthedocs.io/en/latest/mystic.html
        .. [3] https://docs.scipy.org/doc/scipy/reference/optimize.html
        """
        if not callable(cost_func):
            raise TypeError("cost_func must be callable")
        if not list_bounds or not all(
            isinstance(b, tuple) and len(b) == 2 for b in list_bounds
        ):
            raise ValueError("list_bounds must contain (min, max) tuples")
        if max_iter <= 0:
            raise ValueError("max_iter must be positive")
        if tolerance <= 0:
            raise ValueError("tolerance must be positive")

        if method == "scipy":
            return differential_evolution(
                cost_func,
                list_bounds,
                maxiter=max_iter,
                tol=tolerance,
                disp=bl_print_convergence_messages,
            )
        elif method == "mystic":
            if bl_inter_monitor:
                mon = VerboseMonitor(int_verbose_monitor)
                return diffev2(
                    cost_func,
                    x0=list_bounds,
                    bounds=list_bounds,
                    npop=int_size_trial_solution_population,
                    gtol=max_iterations_wo_improvement,
                    disp=bl_print_convergence_messages,
                    full_output=bl_print_warning_messages,
                    itermon=mon,
                    maxiter=max_iter,
                    ftol=tolerance,
                )
            else:
                return diffev2(
                    cost_func,
                    x0=list_bounds,
                    bounds=list_bounds,
                    npop=int_size_trial_solution_population,
                    gtol=max_iterations_wo_improvement,
                    disp=bl_print_convergence_messages,
                    full_output=bl_print_warning_messages,
                    maxiter=max_iter,
                    ftol=tolerance,
                )
        else:
            raise ValueError("Method must be either 'scipy' or 'mystic'")

    def optimize_curve_fit(
        self, 
        func: Callable[..., float], 
        array_x: NDArray[np.float64], 
        array_y: NDArray[np.float64]
    ) -> tuple[NDArray[np.float64], NDArray[np.float64]]:
        """
        Optimize curve fitting.

        Parameters
        ----------
        func : callable
            Function to fit
        array_x : np.ndarray
            Input feature array
        array_y : np.ndarray
            Target values

        Returns
        -------
        tuple[np.ndarray, np.ndarray]
            tuple containing optimal parameters and covariance matrix
        """
        if not callable(func):
            raise TypeError("func must be callable")
        if not isinstance(array_x, np.ndarray) or not isinstance(array_y, np.ndarray):
            raise TypeError("Input arrays must be numpy arrays")
        if array_x.size == 0 or array_y.size == 0:
            raise ValueError("Input arrays cannot be empty")

        return curve_fit(func, xdata=array_x, ydata=array_y)

    def polynomial_fit(
        self, 
        array_x: np.ndarray, 
        array_y: np.ndarray
    ) -> NonLinearEquationsResult:
        """
        Fit polynomial to data.

        Parameters
        ----------
        array_x : np.ndarray
            Input feature array
        array_y : np.ndarray
            Target values

        Returns
        -------
        dict[str, np.ndarray]
            Dictionary containing coefficients and interpolated values
        """
        if not isinstance(array_x, np.ndarray) or not isinstance(array_y, np.ndarray):
            raise TypeError("Input arrays must be numpy arrays")
        if array_x.size == 0 or array_y.size == 0:
            raise ValueError("Input arrays cannot be empty")

        array_coeff = np.polyfit(array_x, array_y)
        return {
            "coefficients": array_coeff,
            "values_interpolated": np.polyval(array_coeff, array_x),
        }