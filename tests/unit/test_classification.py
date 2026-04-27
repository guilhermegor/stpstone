"""Unit tests for classification analysis module.

Tests cover InputsClassification, Classification, and ImageProcessing classes
with normal operations, edge cases, error conditions, and type validation.
"""

from pathlib import Path
import tempfile
from unittest.mock import MagicMock, patch

import matplotlib


# set non-interactive backend before importing pyplot
matplotlib.use("Agg")
from matplotlib import pyplot as plt
import numpy as np
from numpy.typing import NDArray
import pytest
from sklearn.datasets import make_classification
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import SGDClassifier

from stpstone.analytics.quant.classification import (
	Classification,
	ImageProcessing,
	InputsClassification,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def inputs_class() -> InputsClassification:
	"""Fixture providing InputsClassification instance.

	Returns
	-------
	InputsClassification
		InputsClassification instance
	"""
	return InputsClassification()


@pytest.fixture
def classification() -> Classification:
	"""Fixture providing Classification instance.

	Returns
	-------
	Classification
		Classification instance
	"""
	return Classification()


@pytest.fixture
def image_processing() -> ImageProcessing:
	"""Fixture providing ImageProcessing instance.

	Returns
	-------
	ImageProcessing
		ImageProcessing instance
	"""
	return ImageProcessing()


@pytest.fixture
def sample_image_data() -> NDArray[np.float64]:
	"""Fixture providing sample image data.

	Returns
	-------
	NDArray[np.float64]
		Sample image data
	"""
	return np.random.rand(28, 28)


@pytest.fixture
def sample_classification_data() -> tuple[NDArray[np.float64], NDArray[np.float64]]:
	"""Fixture providing sample classification data.

	Returns
	-------
	tuple[NDArray[np.float64], NDArray[np.float64]]
		Feature and target arrays
	"""
	return make_classification(
		n_samples=100, n_features=20, n_informative=2, n_redundant=2, n_classes=2, random_state=42
	)


@pytest.fixture
def sample_text_corpus() -> list[str]:
	"""Fixture providing sample text corpus.

	Returns
	-------
	list[str]
		Sample text corpus
	"""
	return ["Time flies flies like an arrow.", "Fruit flies like a banana."]


@pytest.fixture
def temp_image_path(sample_image_data: NDArray[np.float64]) -> Path:
	"""Fixture providing temporary image path with sample data.

	Parameters
	----------
	sample_image_data : NDArray[np.float64]
		Sample image data

	Returns
	-------
	Path
		Temporary image path
	"""
	temp_dir = tempfile.mkdtemp()
	path = Path(temp_dir) / "test_image.png"
	plt.imsave(path, sample_image_data)
	return path


# --------------------------
# InputsClassification Tests
# --------------------------
@patch("stpstone.analytics.quant.classification.fetch_openml")
def test_fetch_sklearn_database_default(
	mock_fetch_openml: MagicMock, inputs_class: InputsClassification
) -> None:
	"""Test fetching default sklearn database.

	Parameters
	----------
	mock_fetch_openml : MagicMock
		Mocked fetch_openml to avoid real network calls.
	inputs_class : InputsClassification
		InputsClassification instance
	"""
	mock_result = MagicMock()
	mock_result.__contains__ = lambda self, key: key in ("data", "target")
	mock_result.__iter__ = lambda self: iter(["data", "target"])
	mock_fetch_openml.return_value = mock_result
	result = inputs_class.fetch_sklearn_database()
	mock_fetch_openml.assert_called_once_with("mnist_784", version=1, as_frame=False)
	assert result is mock_result


@patch("stpstone.analytics.quant.classification.fetch_openml")
def test_fetch_sklearn_database_custom(
	mock_fetch_openml: MagicMock, inputs_class: InputsClassification
) -> None:
	"""Test fetching custom sklearn database.

	Parameters
	----------
	mock_fetch_openml : MagicMock
		Mocked fetch_openml to avoid real network calls.
	inputs_class : InputsClassification
		InputsClassification instance
	"""
	mock_result = MagicMock()
	mock_result.__contains__ = lambda self, key: key in ("data", "target")
	mock_fetch_openml.return_value = mock_result
	result = inputs_class.fetch_sklearn_database(database_name="iris", version=1)
	mock_fetch_openml.assert_called_once_with("iris", version=1, as_frame=False)
	assert result is mock_result


def test_show_image_from_dataset(
	inputs_class: InputsClassification, sample_image_data: NDArray[np.float64], tmp_path: Path
) -> None:
	"""Test displaying image from dataset.

	Parameters
	----------
	inputs_class : InputsClassification
		InputsClassification instance
	sample_image_data : NDArray[np.float64]
		Sample image data
	tmp_path : Path
		Temporary directory path
	"""
	save_path = tmp_path / "test.png"
	inputs_class.show_image_from_dataset(
		sample_image_data.flatten(), complete_saving_path=str(save_path)
	)


def test_show_image_from_dataset_with_save(
	inputs_class: InputsClassification, sample_image_data: NDArray[np.float64], tmp_path: Path
) -> None:
	"""Test displaying image with save option.

	Parameters
	----------
	inputs_class : InputsClassification
		InputsClassification instance
	sample_image_data : NDArray[np.float64]
		Sample image data
	tmp_path : Path
		Temporary directory path
	"""
	save_path = tmp_path / "test.png"
	inputs_class.show_image_from_dataset(
		sample_image_data.flatten(), complete_saving_path=str(save_path)
	)
	assert save_path.exists()


# --------------------------
# Classification Tests
# --------------------------
def test_one_hot_vectorizer(classification: Classification, sample_text_corpus: list[str]) -> None:
	"""Test one-hot vectorizer with sample corpus.

	Parameters
	----------
	classification : Classification
		Classification instance
	sample_text_corpus : list[str]
		Sample text corpus
	"""
	result = classification.one_hot_vectorizer(sample_text_corpus)
	assert isinstance(result, dict)
	assert "labels" in result
	assert "one_hot_encoder" in result
	assert len(result["labels"]) == 8
	assert result["one_hot_encoder"].shape == (2, 7)


def test_sgd_classifier(
	classification: Classification,
	sample_classification_data: tuple[NDArray[np.float64], NDArray[np.float64]],
) -> None:
	"""Test SGD classifier with sample data.

	Parameters
	----------
	classification : Classification
		Classification instance
	sample_classification_data : tuple[NDArray[np.float64], NDArray[np.float64]]
		Sample classification data
	"""
	X, y = sample_classification_data
	result = classification.sgd_classifier(X, y)
	assert isinstance(result, dict)
	assert "model_fitted" in result
	assert "predictions" in result
	assert result["accuracy_score"] >= 0.0


def test_svm_best_strategy(
	classification: Classification,
	sample_classification_data: tuple[NDArray[np.float64], NDArray[np.float64]],
) -> None:
	"""Test SVM with best multiclass strategy.

	Parameters
	----------
	classification : Classification
		Classification instance
	sample_classification_data : tuple[NDArray[np.float64], NDArray[np.float64]]
		Sample classification data
	"""
	X, y = sample_classification_data
	result = classification.svm(X, y, multiclass_classification_strategy="best")
	assert isinstance(result, dict)
	assert result["accuracy_score"] >= 0.0


def test_svm_ovr_strategy(
	classification: Classification,
	sample_classification_data: tuple[NDArray[np.float64], NDArray[np.float64]],
) -> None:
	"""Test SVM with one-vs-rest strategy.

	Parameters
	----------
	classification : Classification
		Classification instance
	sample_classification_data : tuple[NDArray[np.float64], NDArray[np.float64]]
		Sample classification data
	"""
	X, y = sample_classification_data
	result = classification.svm(X, y, multiclass_classification_strategy="ovr")
	assert isinstance(result, dict)
	assert result["accuracy_score"] >= 0.0


def test_svm_ovo_strategy(
	classification: Classification,
	sample_classification_data: tuple[NDArray[np.float64], NDArray[np.float64]],
) -> None:
	"""Test SVM with one-vs-one strategy.

	Parameters
	----------
	classification : Classification
		Classification instance
	sample_classification_data : tuple[NDArray[np.float64], NDArray[np.float64]]
		Sample classification data
	"""
	X, y = sample_classification_data
	result = classification.svm(X, y, multiclass_classification_strategy="ovo")
	assert isinstance(result, dict)
	assert result["accuracy_score"] >= 0.0


def test_svm_invalid_strategy(
	classification: Classification,
	sample_classification_data: tuple[NDArray[np.float64], NDArray[np.float64]],
) -> None:
	"""Test SVM with invalid strategy raises error.

	Parameters
	----------
	classification : Classification
		Classification instance
	sample_classification_data : tuple[NDArray[np.float64], NDArray[np.float64]]
		Sample classification data
	"""
	X, y = sample_classification_data
	with pytest.raises(TypeError, match="must be one of"):
		classification.svm(X, y, multiclass_classification_strategy="invalid")


def test_decision_tree(
	classification: Classification,
	sample_classification_data: tuple[NDArray[np.float64], NDArray[np.float64]],
) -> None:
	"""Test decision tree classifier.

	Parameters
	----------
	classification : Classification
		Classification instance
	sample_classification_data : tuple[NDArray[np.float64], NDArray[np.float64]]
		Sample classification data
	"""
	X, y = sample_classification_data
	result = classification.decision_tree(X, y)
	assert isinstance(result, dict)
	assert result["accuracy_score"] >= 0.0


def test_random_forest(
	classification: Classification,
	sample_classification_data: tuple[NDArray[np.float64], NDArray[np.float64]],
) -> None:
	"""Test random forest classifier.

	Parameters
	----------
	classification : Classification
		Classification instance
	sample_classification_data : tuple[NDArray[np.float64], NDArray[np.float64]]
		Sample classification data
	"""
	X, y = sample_classification_data
	result = classification.random_forest(X, y)
	assert isinstance(result, dict)
	assert result["accuracy_score"] >= 0.0


def test_knn_classifier(
	classification: Classification,
	sample_classification_data: tuple[NDArray[np.float64], NDArray[np.float64]],
) -> None:
	"""Test KNN classifier.

	Parameters
	----------
	classification : Classification
		Classification instance
	sample_classification_data : tuple[NDArray[np.float64], NDArray[np.float64]]
		Sample classification data
	"""
	X, y = sample_classification_data
	result = classification.knn_classifier(X, y)
	assert isinstance(result, dict)
	assert result["accuracy_score"] >= 0.0


def test_k_means(
	classification: Classification,
	sample_classification_data: tuple[NDArray[np.float64], NDArray[np.float64]],
) -> None:
	"""Test K-means clustering.

	Parameters
	----------
	classification : Classification
		Classification instance
	sample_classification_data : tuple[NDArray[np.float64], NDArray[np.float64]]
		Sample classification data
	"""
	X, y = sample_classification_data
	result = classification.k_means(n_clusters=2, array_y=y, array_x=X)
	assert isinstance(result, dict)
	assert result["adjusted_rand_score"] >= -1.0


def test_naive_bayes(
	classification: Classification,
	sample_classification_data: tuple[NDArray[np.float64], NDArray[np.float64]],
) -> None:
	"""Test naive Bayes classifier.

	Parameters
	----------
	classification : Classification
		Classification instance
	sample_classification_data : tuple[NDArray[np.float64], NDArray[np.float64]]
		Sample classification data
	"""
	X, y = sample_classification_data
	result = classification.naive_bayes(X, y)
	assert isinstance(result, dict)
	assert result["accuracy_score"] >= 0.0


# --------------------------
# ImageProcessing Tests
# --------------------------
def test_img_dims(image_processing: ImageProcessing, temp_image_path: Path) -> None:
	"""Test getting image dimensions.

	Parameters
	----------
	image_processing : ImageProcessing
		ImageProcessing instance
	temp_image_path : Path
		Temporary image path
	"""
	result = image_processing.img_dims(str(temp_image_path))
	assert isinstance(result, tuple)
	assert len(result) == 3  # height, width, channels


def test_pca_with_var_exp(image_processing: ImageProcessing, temp_image_path: Path) -> None:
	"""Test PCA with variance explained.

	Parameters
	----------
	image_processing : ImageProcessing
		ImageProcessing instance
	temp_image_path : Path
		Temporary image path
	"""
	result = image_processing.pca_with_var_exp(str(temp_image_path), 0.95)
	assert isinstance(result, np.ndarray)
	original_shape = image_processing.img_dims(str(temp_image_path))
	assert result.shape == original_shape


def test_plot_subplot(
	image_processing: ImageProcessing, sample_image_data: NDArray[np.float64]
) -> None:
	"""Test plotting subplot.

	Parameters
	----------
	image_processing : ImageProcessing
		ImageProcessing instance
	sample_image_data : NDArray[np.float64]
		Sample image data
	"""
	plt.figure()
	image_processing.plot_subplot(1, sample_image_data)
	plt.close()


# --------------------------
# Error Cases
# --------------------------
def test_one_hot_vectorizer_empty_corpus(classification: Classification) -> None:
	"""Test one-hot vectorizer with empty corpus.

	Parameters
	----------
	classification : Classification
		Classification instance
	"""
	with pytest.raises(ValueError):
		classification.one_hot_vectorizer([])


def test_classifiers_with_empty_data(classification: Classification) -> None:
	"""Test classifiers with empty data.

	Parameters
	----------
	classification : Classification
		Classification instance
	"""
	empty_X = np.array([])
	empty_y = np.array([])

	with pytest.raises(ValueError):
		classification.sgd_classifier(empty_X, empty_y)

	with pytest.raises(ValueError):
		classification.svm(empty_X, empty_y)

	with pytest.raises(ValueError):
		classification.decision_tree(empty_X, empty_y)


def test_img_dims_invalid_path(image_processing: ImageProcessing) -> None:
	"""Test img_dims with invalid path.

	Parameters
	----------
	image_processing : ImageProcessing
		ImageProcessing instance
	"""
	with pytest.raises(FileNotFoundError):
		image_processing.img_dims("nonexistent_path.png")


# --------------------------
# Type Validation
# --------------------------
def test_type_validation_sgd_classifier(
	classification: Classification,
	sample_classification_data: tuple[NDArray[np.float64], NDArray[np.float64]],
) -> None:
	"""Test type validation in SGD classifier.

	Parameters
	----------
	classification : Classification
		Classification instance
	sample_classification_data : tuple[NDArray[np.float64], NDArray[np.float64]]
		Sample classification data
	"""
	X, y = sample_classification_data

	with pytest.raises(TypeError):
		classification.sgd_classifier(X.tolist(), y)  # type: ignore

	with pytest.raises(TypeError):
		classification.sgd_classifier(X, y.tolist())  # type: ignore


def test_type_validation_one_hot_vectorizer(classification: Classification) -> None:
	"""Test type validation in one-hot vectorizer.

	Parameters
	----------
	classification : Classification
		Classification instance
	"""
	with pytest.raises(TypeError):
		classification.one_hot_vectorizer("not a list")  # type: ignore

	with pytest.raises(TypeError):
		classification.one_hot_vectorizer([1, 2, 3])  # type: ignore


# --------------------------
# Edge Cases
# --------------------------
def test_k_means_single_cluster(
	classification: Classification,
	sample_classification_data: tuple[NDArray[np.float64], NDArray[np.float64]],
) -> None:
	"""Test K-means with single cluster.

	Parameters
	----------
	classification : Classification
		Classification instance
	sample_classification_data : tuple[NDArray[np.float64], NDArray[np.float64]]
		Sample classification data
	"""
	X, y = sample_classification_data
	result = classification.k_means(n_clusters=1, array_y=y, array_x=X)
	assert isinstance(result, dict)
	assert len(np.unique(result["predictions"])) == 1


def test_decision_tree_max_depth(
	classification: Classification,
	sample_classification_data: tuple[NDArray[np.float64], NDArray[np.float64]],
) -> None:
	"""Test decision tree with max depth constraint.

	Parameters
	----------
	classification : Classification
		Classification instance
	sample_classification_data : tuple[NDArray[np.float64], NDArray[np.float64]]
		Sample classification data
	"""
	X, y = sample_classification_data
	result = classification.decision_tree(X, y, int_max_depth=2)
	assert isinstance(result, dict)


def test_random_forest_single_tree(
	classification: Classification,
	sample_classification_data: tuple[NDArray[np.float64], NDArray[np.float64]],
) -> None:
	"""Test random forest with single tree.

	Parameters
	----------
	classification : Classification
		Classification instance
	sample_classification_data : tuple[NDArray[np.float64], NDArray[np.float64]]
		Sample classification data
	"""
	X, y = sample_classification_data
	result = classification.random_forest(X, y, n_estimators=1)
	assert isinstance(result, dict)


# --------------------------
# Performance Tests
# --------------------------
@pytest.mark.timeout(5)
def test_performance_large_data(classification: Classification) -> None:
	"""Test performance with larger dataset.

	Parameters
	----------
	classification : Classification
		Classification instance
	"""
	X, y = make_classification(
		n_samples=1000, n_features=50, n_classes=2, n_informative=3, random_state=42
	)
	result = classification.random_forest(X, y)
	assert isinstance(result, dict)


# --------------------------
# Warning Tests
# --------------------------
def test_sgd_convergence_warning(
	classification: Classification,
	sample_classification_data: tuple[NDArray[np.float64], NDArray[np.float64]],
) -> None:
	"""Test SGD classifier convergence warning.

	Parameters
	----------
	classification : Classification
		Classification instance
	sample_classification_data : tuple[NDArray[np.float64], NDArray[np.float64]]
		Sample classification data
	"""
	X, y = sample_classification_data
	# Artificially small max_iter to trigger convergence warning
	with pytest.warns(ConvergenceWarning):
		model = SGDClassifier(max_iter=1, random_state=42)
		model.fit(X, y)
