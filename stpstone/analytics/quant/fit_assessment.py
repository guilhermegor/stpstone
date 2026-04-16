"""Module for assessing model fitting performance using various metrics."""

from typing import Literal, TypedDict

import numpy as np
from numpy.typing import NDArray
from sklearn.base import BaseEstimator
from sklearn.metrics import (
	accuracy_score,
	confusion_matrix,
	f1_score,
	mean_squared_error,
	precision_score,
	r2_score,
	recall_score,
	roc_auc_score,
)
from sklearn.model_selection import (
	GridSearchCV,
	RandomizedSearchCV,
	cross_val_predict,
	cross_val_score,
)

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class ResultCrossValidation(TypedDict):
	"""TypedDict for cross-validation results."""

	scores: float
	mean: float
	std: float


class ResultGridSearch(TypedDict):
	"""TypedDict for grid-search results."""

	best_parameters: dict
	score: float
	best_estimator: BaseEstimator
	cv_results: dict
	model_regression: BaseEstimator
	predict: NDArray[np.float64]
	mse: float
	rmse: float


class ResultAccuracyPredictions(TypedDict):
	"""TypedDict for accuracy and predictions results."""

	cross_validation_scores: float
	predictions: NDArray[np.float64]
	confusion_matrix: NDArray[np.float64]
	precision_score: float
	recall_score: float
	f1_score: float
	roc_auc_score: float


class ResultFittingPerformance(TypedDict):
	"""TypedDict for fitting performance results."""

	accuracy: float
	precision: float
	recall_sensitivity: float
	f1_score: float
	r2_score: float


class FitPerformance(metaclass=TypeChecker):
	"""Class for evaluating model fitting performance using various metrics."""

	def max_llf(self, array_x: np.ndarray, array_y: np.ndarray, array_y_hat: np.ndarray) -> float:
		"""Calculate the maximized log-likelihood.

		Parameters
		----------
		array_x : np.ndarray
			Independent variable array
		array_y : np.ndarray
			Dependent variable array
		array_y_hat : np.ndarray
			Array of predictions

		Returns
		-------
		float
			Maximized log-likelihood value

		References
		----------
		.. [1] https://stackoverflow.com/questions/45033980/how-to-compute-aic-for-linear-\
regression-model-in-python
		"""
		nobs = float(array_x.shape[0])
		nobs2 = nobs / 2.0
		resid = array_y - array_y_hat
		ssr = np.sum(resid**2)

		if ssr < 1e-10:  # near perfect fit
			ssr = 1e-10  # small value to avoid log(0)

		llf = -nobs2 * np.log(2 * np.pi) - nobs2 * np.log(ssr / nobs) - nobs2
		return llf

	def aic(self, array_x: np.ndarray, array_y: np.ndarray, array_y_hat: np.ndarray) -> float:
		"""Calculate Akaike's Information Criterion (AIC).

		Evaluates a collection of models that explain the same dependent variable.
		Lower values indicate better models. Better than BIC for prediction purposes.

		Parameters
		----------
		array_x : np.ndarray
			Independent variable array
		array_y : np.ndarray
			Dependent variable array
		array_y_hat : np.ndarray
			Array of predictions

		Returns
		-------
		float
			AIC metric value
		"""
		p = array_x.shape[1] + 2
		llf = self.max_llf(array_x, array_y, array_y_hat)
		return -2.0 * llf + 2.0 * p

	def bic(self, array_x: np.ndarray, array_y: np.ndarray, array_y_hat: np.ndarray) -> float:
		"""Calculate Schwartz's Bayesian Information Criterion (BIC).

		Lower values indicate better models. Better than AIC when goodness of fit is preferred.

		Parameters
		----------
		array_x : np.ndarray
			Independent variable array
		array_y : np.ndarray
			Dependent variable array
		array_y_hat : np.ndarray
			Array of predictions

		Returns
		-------
		float
			BIC metric value

		References
		----------
		.. [1] https://en.wikipedia.org/wiki/Bayesian_information_criterion
		"""
		llf = self.max_llf(array_x, array_y, array_y_hat)
		k = array_x.shape[1] + 2  # number of parameters
		n = array_x.shape[0]  # number of observations
		return -2 * llf + k * np.log(n)

	def cross_validation(
		self,
		model_fitted: object,
		array_x: np.ndarray,
		array_y: np.ndarray,
		cross_validation_folds: int = 3,
		scoring_method: str = "neg_mean_squared_error",
		cross_val_model: Literal["score", "predict"] = "score",
		cross_val_model_method: str = "predict_proba",
	) -> ResultCrossValidation:
		"""Perform cross-validation to measure estimator performance.

		Parameters
		----------
		model_fitted : object
			Fitted model estimator
		array_x : np.ndarray
			Array of real numbers (features)
		array_y : np.ndarray
			Array of real numbers (target)
		cross_validation_folds : int
			Number of cross-validation folds, by default 3
		scoring_method : str
			Scoring method, by default "neg_mean_squared_error"
		cross_val_model : Literal['score', 'predict']
			Cross-validation model type, by default "score"
		cross_val_model_method : str
			Cross-validation model method, by default "predict_proba"

		Returns
		-------
		ResultCrossValidation
			Dictionary containing scores, mean and standard deviation

		Raises
		------
		ValueError
			If cross validation method does not match current possibilities

		References
		----------
		.. [1] https://scikit-learn.org/stable/modules/cross_validation.html
		"""
		if cross_val_model == "score":
			scores = cross_val_score(
				model_fitted,
				array_x,
				array_y,
				cv=cross_validation_folds,
				scoring=scoring_method,
			)
		elif cross_val_model == "predict":
			if not hasattr(model_fitted, cross_val_model_method):
				cross_val_model_method = "predict"
			scores = cross_val_predict(
				model_fitted,
				array_x,
				array_y,
				cv=cross_validation_folds,
				method=cross_val_model_method,
			)
		else:
			raise ValueError(
				f"Cross validation method {cross_val_model} does not match "
				"current possibilities: score and predict"
			)

		return {
			"scores": scores,
			"mean": scores.mean(),
			"std": scores.std(),
		}

	def grid_search_optimal_hyperparameters(
		self,
		model_fitted: object,
		param_grid: dict,
		scoring_method: str,
		array_x_real_numbers: np.ndarray,
		array_y_real_numbers: np.ndarray,
		num_cross_validation_splitting_strategy: int = 5,
		bool_return_train_score: bool = True,
		bool_randomized_search: bool = True,
	) -> ResultGridSearch:
		"""Find optimal hyperparameters using grid search.

		Parameters
		----------
		model_fitted : object
			Model estimator
		param_grid : dict
			Dictionary of parameters to search
		scoring_method : str
			Scoring method
		array_x_real_numbers : np.ndarray
			Array of real numbers (features)
		array_y_real_numbers : np.ndarray
			Array of real numbers (target)
		num_cross_validation_splitting_strategy : int
			Number of CV splits, by default 5
		bool_return_train_score : bool
			Whether to return train scores, by default True
		bool_randomized_search : bool
			Whether to use randomized search, by default True

		Returns
		-------
		ResultGridSearch
			Dictionary containing best parameters, scores, estimator, etc.

		References
		----------
		.. [1] https://colab.research.google.com/github/ageron/handson-ml2/blob/master/\
02_sup_to_sup_machine_learning_project.ipynb#scrollTo=HwzPGGhkEagH
		"""
		if bool_randomized_search:
			grid_search_model = RandomizedSearchCV(
				model_fitted,
				param_grid,
				cv=num_cross_validation_splitting_strategy,
				scoring=scoring_method,
				return_train_score=bool_return_train_score,
			)
		else:
			grid_search_model = GridSearchCV(
				model_fitted,
				param_grid,
				cv=num_cross_validation_splitting_strategy,
				scoring=scoring_method,
				return_train_score=bool_return_train_score,
			)

		grid_search_model.fit(array_x_real_numbers, array_y_real_numbers)
		best_model_prediction = grid_search_model.best_estimator_.predict(array_x_real_numbers)

		result = {
			"best_parameters": grid_search_model.best_params_,
			"score": grid_search_model.best_score_,
			"best_estimator": grid_search_model.best_estimator_,
			"cv_results": grid_search_model.cv_results_,
			"model_regression": grid_search_model,
			"predict": best_model_prediction,
			"mse": mean_squared_error(array_y_real_numbers, best_model_prediction),
			"rmse": np.sqrt(mean_squared_error(array_y_real_numbers, best_model_prediction)),
		}

		# add feature importance only if available  # noqa: ERA001
		if hasattr(grid_search_model.best_estimator_, "feature_importances_"):
			result["feature_importance"] = grid_search_model.best_estimator_.feature_importances_

		return result

	def accuracy_predictions(
		self,
		model: object,
		array_y: np.ndarray,
		array_x: np.ndarray,
		cross_validation_folds: int = 3,
		scoring_method: str = "accuracy",
		f1_score_average: str = "macro",
	) -> ResultAccuracyPredictions:
		"""Evaluate model accuracy using various metrics.

		Parameters
		----------
		model : object
			Fitted model
		array_y : np.ndarray
			Target array
		array_x : np.ndarray
			Feature array
		cross_validation_folds : int
			Number of CV folds, by default 3
		scoring_method : str
			Scoring method, by default "accuracy"
		cross_val_model : str
			Cross-validation model type, by default "score"
		key_scores : str
			Key for scores in output, by default "scores"
		f1_score_average : str
			F1 score averaging method, by default "macro"

		Returns
		-------
		ResultAccuracyPredictions
			Dictionary containing various accuracy metrics

		References
		----------
		.. [1] https://colab.research.google.com/github/ageron/handson-ml2/blob/master/\
03_classification.ipynb#scrollTo=rUZ6ahZ7G0BO
		"""
		# get predictions first
		array_y_hat = cross_val_predict(model, array_x, array_y, cv=cross_validation_folds)

		# calculate scores
		scores = cross_val_score(
			model,
			array_x,
			array_y,
			cv=cross_validation_folds,
			scoring=scoring_method,
		)

		return {
			"cross_validation_scores": scores,
			"predictions": array_y_hat,
			"confusion_matrix": confusion_matrix(array_y, array_y_hat),
			"precision_score": precision_score(array_y, array_y_hat),
			"recall_score": recall_score(array_y, array_y_hat),
			"f1_score": f1_score(array_y, array_y_hat, average=f1_score_average),
			"roc_auc_score": roc_auc_score(array_y, array_y_hat),
		}

	def fitting_perf_eval(
		self, array_y: np.ndarray, array_y_hat: np.ndarray
	) -> ResultFittingPerformance:
		"""Evaluate fitting performance using various metrics.

		Parameters
		----------
		array_y : np.ndarray
			True values
		array_y_hat : np.ndarray
			Predicted values

		Returns
		-------
		ResultFittingPerformance
			Dictionary containing various performance metrics

		References
		----------
		.. [1] https://medium.com/@maxgrossman10/accuracy-recall-precision-f1-score-with-\
python-4f2ee97e0d6
		"""
		return {
			"accuracy": accuracy_score(array_y, array_y_hat),
			"precision": precision_score(array_y, array_y_hat),
			"recall_sensitivity": recall_score(array_y, array_y_hat),
			"f1_score": f1_score(array_y, array_y_hat),
			"r2_score": r2_score(array_y, array_y_hat),
		}
