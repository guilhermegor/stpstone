"""Module for classification analysis."""

from typing import Literal, Optional, TypedDict, Union

import matplotlib.pyplot as plt
import numpy as np
from numpy.typing import NDArray
import pandas as pd
from scipy.sparse import csr_matrix
from sklearn.base import BaseEstimator
from sklearn.cluster import KMeans
from sklearn.datasets import fetch_openml
from sklearn.decomposition import PCA
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import (
    accuracy_score,
    adjusted_rand_score,
    confusion_matrix,
)
from sklearn.multiclass import OneVsOneClassifier, OneVsRestClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.lists import ListHandler
from stpstone.utils.parsers.str import StrHandler


class ResultFetchSklearn(TypedDict):
    """TypedDict for sklearn fetch_openml results."""

    data: Union[NDArray[np.float64], csr_matrix, pd.DataFrame]
    target: NDArray[np.float64]
    DESCR: str
    feature_names: list[str]
    target_names: list[str]
    categories: Optional[dict]
    details: dict
    frame: pd.DataFrame


class ResultOneHotVectorizer(TypedDict):
    """TypedDict for one-hot vectorizer results."""

    labels: list[str]
    array: NDArray[np.float64]


class ResultClassification(TypedDict):
    """TypedDict for classification results."""

    model_fitted: BaseEstimator
    predictions: NDArray[np.float64]
    adjusted_rand_score: float
    score: float
    accuracy_score: float
    confusion_matrix: NDArray[np.float64]
    classes: NDArray[np.float64]

class InputsClassification(metaclass=TypeChecker):
    """Class for handling input data for classification tasks."""

    def fetch_sklearn_database(
        self, 
        database_name: str = "mnist_784", 
        version: int = 1, 
        bool_asframe: bool = False
    ) -> ResultFetchSklearn:
        """Fetch dataset from sklearn openml repository.

        Parameters
        ----------
        database_name : str
            Name of dataset to fetch, default "mnist_784"
        version : int
            Version of dataset, default 1
        bool_asframe : bool
            Return as pandas DataFrame if True, default False

        Returns
        -------
        ResultFetchSklearn
            Dictionary containing dataset features and targets

        References
        ----------
        scikit-learn fetch_openml documentation
        """
        return fetch_openml(database_name, version=version, as_frame=bool_asframe)

    def show_image_from_dataset(
        self, 
        array_instance: NDArray[np.float64], 
        cmap: str = "binary", 
        shape: tuple = (28, 28),
        bool_axis: str = "off", 
        complete_saving_path: Optional[str] = None
    ) -> None:
        """Display image from dataset array.

        Parameters
        ----------
        array_instance : NDArray[np.float64]
            Array containing image data
        cmap : str
            Color map for display, default "binary"
        shape : tuple
            Shape to reshape image, default (28, 28)
        bool_axis : str
            Show axis if "on", default "off"
        complete_saving_path : Optional[str]
            Path to save image if provided, default None

        References
        ----------
        matplotlib.pyplot.imshow documentation
        """
        array_instance = array_instance.reshape(shape)
        plt.imshow(array_instance, cmap=cmap)
        plt.axis(bool_axis)
        if complete_saving_path is not None:
            plt.savefig(complete_saving_path)
        else:
            plt.show()


class Classification(metaclass=TypeChecker):
    """Class implementing various classification algorithms."""

    def one_hot_vectorizer(self, list_corpus: list[str]) -> ResultOneHotVectorizer:
        """Convert text corpus to one-hot encoded vectors.

        Parameters
        ----------
        list_corpus : list[str]
            list of text documents

        Returns
        -------
        ResultOneHotVectorizer
            Contains labels and one-hot encoded array

        Raises
        ------
        TypeError
            If input is not a list of strings

        References
        ----------
        scikit-learn CountVectorizer documentation
        """
        if not isinstance(list_corpus, list) or not all(isinstance(x, str) for x in list_corpus):
            raise TypeError("Input must be a list of strings")

        list_labels = ListHandler().extend_lists(*[x.split() for x in list_corpus])
        list_labels = [StrHandler().remove_sup_period_marks(x).lower()
                      for x in ListHandler().remove_duplicates(list_labels)]
        list_labels.sort()

        model = CountVectorizer(binary=True)
        array_y_hat = model.fit_transform(list_corpus).toarray()
        return {
            "labels": list_labels,
            "one_hot_encoder": array_y_hat
        }

    def sgd_classifier(
        self, 
        array_x: NDArray[np.float64], 
        array_y: NDArray[np.float64], 
        int_random_state_seed: int = 5
    ) -> ResultClassification:
        """Train SGD classifier and return metrics.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Feature array
        array_y : NDArray[np.float64]
            Target array
        int_random_state_seed : int
            Random seed, default 5

        Returns
        -------
        ResultClassification
            Model and performance metrics

        Raises
        ------
        TypeError
            If inputs are not numpy arrays
        ValueError
            If inputs are empty

        References
        ----------
        scikit-learn SGDClassifier documentation
        """
        if not isinstance(array_x, np.ndarray) or not isinstance(array_y, np.ndarray):
            raise TypeError("Inputs must be numpy arrays")
        if array_x.size == 0 or array_y.size == 0:
            raise ValueError("Input arrays cannot be empty")

        model = SGDClassifier(random_state=int_random_state_seed)
        model.fit(array_x, array_y)
        array_y_hat = model.predict(array_x)

        return {
            "model_fitted": model,
            "predictions": array_y_hat,
            "adjusted_rand_score": adjusted_rand_score(array_y, array_y_hat),
            "score": model.score(array_x, array_y),
            "accuracy_score": accuracy_score(array_y, array_y_hat),
            "confusion_matrix": confusion_matrix(array_y, array_y_hat),
            "classes": model.classes_,
        }

    def svm(
        self, 
        array_x: NDArray[np.float64], 
        array_y: NDArray[np.float64], 
        kernel: str = "rbf",
        float_regularization_parameter: float = 1,
        multiclass_classification_strategy: Literal["best", "ovr", "ovo"] = "best",
        gamma: str = "auto", 
        int_random_state_seed: int = 42
    ) -> ResultClassification:
        """Train SVM classifier with specified multiclass strategy.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Feature array
        array_y : NDArray[np.float64]
            Target array
        kernel : str
            Kernel type, default "rbf"
        float_regularization_parameter : float
            Regularization parameter, default 1
        multiclass_classification_strategy : Literal['best', 'ovr', 'ovo'], optional
            Strategy for multiclass ("best", "ovr", "ovo"), default "best"
        gamma : str
            Kernel coefficient, default "auto"
        int_random_state_seed : int
            Random seed, default 42

        Returns
        -------
        ResultClassification
            Model and performance metrics

        Raises
        ------
        ValueError
            If invalid multiclass strategy is provided

        References
        ----------
        scikit-learn SVC and multiclass classifier documentation
        """
        if multiclass_classification_strategy == "best":
            model = SVC(C=float_regularization_parameter, gamma=gamma,
                      random_state=int_random_state_seed, kernel=kernel)
        elif multiclass_classification_strategy == "ovr":
            model = OneVsRestClassifier(
                SVC(C=float_regularization_parameter, gamma=gamma,
                    random_state=int_random_state_seed, kernel=kernel))
        elif multiclass_classification_strategy == "ovo":
            model = OneVsOneClassifier(
                SVC(C=float_regularization_parameter, gamma=gamma,
                    random_state=int_random_state_seed, kernel=kernel))
        else:
            raise ValueError("Invalid multiclass strategy")
        
        model.fit(array_x, array_y)
        array_y_hat = model.predict(array_x)
        return {
            "model_fitted": model,
            "predictions": array_y_hat,
            "adjusted_rand_score": adjusted_rand_score(array_y, array_y_hat),
            "score": model.score(array_x, array_y),
            "accuracy_score": accuracy_score(array_y, array_y_hat),
            "confusion_matrix": confusion_matrix(array_y, array_y_hat),
            "classes": model.classes_,
        }

    def decision_tree(
        self,
        array_x: NDArray[np.float64],
        array_y: NDArray[np.float64],
        impurity_crit: str = "gini",
        int_max_depth: Optional[int] = None,
        int_random_state_seed: int = 42
    ) -> ResultClassification:
        """Train decision tree classifier.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Feature array
        array_y : NDArray[np.float64]
            Target array
        impurity_crit : str
            Splitting criterion, default "gini"
        int_max_depth : Optional[int]
            Max tree depth, default None
        int_random_state_seed : int
            Random seed, default 42

        Returns
        -------
        ResultClassification
            Model and performance metrics

        References
        ----------
        [1] https://www.datacamp.com/tutorial/decision-tree-classification-python
        scikit-learn DecisionTreeClassifier documentation
        """
        model = DecisionTreeClassifier(
            criterion=impurity_crit,
            max_depth=int_max_depth if int_max_depth is not None else None,
            random_state=int_random_state_seed
        )
        model.fit(array_x, array_y)
        array_y_hat = model.predict(array_x)
        return {
            "model_fitted": model,
            "predictions": array_y_hat,
            "adjusted_rand_score": adjusted_rand_score(array_y, array_y_hat),
            "score": model.score(array_x, array_y),
            "accuracy_score": accuracy_score(array_y, array_y_hat),
            "confusion_matrix": confusion_matrix(array_y, array_y_hat),
            "classes": model.classes_,
        }

    def random_forest(
        self,
        array_x: NDArray[np.float64],
        array_y: NDArray[np.float64],
        n_estimators: int = 100,
        int_random_state_seed: int = 42
    ) -> ResultClassification:
        """Train random forest classifier.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Feature array
        array_y : NDArray[np.float64]
            Target array
        n_estimators : int
            Number of trees, default 100
        int_random_state_seed : int
            Random seed, default 42

        Returns
        -------
        ResultClassification
            Model and performance metrics

        References
        ----------
        [1] https://www.datacamp.com/tutorial/random-forests-classifier-python
        scikit-learn RandomForestClassifier documentation
        """
        model = RandomForestClassifier(
            random_state=int_random_state_seed,
            n_estimators=n_estimators
        )
        model.fit(array_x, array_y)
        array_y_hat = model.predict(array_x)
        return {
            "model_fitted": model,
            "predictions": array_y_hat,
            "adjusted_rand_score": adjusted_rand_score(array_y, array_y_hat),
            "score": model.score(array_x, array_y),
            "accuracy_score": accuracy_score(array_y, array_y_hat),
            "confusion_matrix": confusion_matrix(array_y, array_y_hat),
            "classes": model.classes_,
        }

    def knn_classifier(
        self,
        array_x: NDArray[np.float64],
        array_y: NDArray[np.float64],
        int_n_neighbors: int = 5
    ) -> ResultClassification:
        """Train KNN classifier.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Feature array
        array_y : NDArray[np.float64]
            Target array
        int_n_neighbors : int
            Number of neighbors, default 5

        Returns
        -------
        ResultClassification
            Model and performance metrics

        References
        ----------
        scikit-learn KNeighborsClassifier documentation
        """
        model = KNeighborsClassifier(n_neighbors=int_n_neighbors)
        model.fit(array_x, array_y)
        array_y_hat = model.predict(array_x)
        return {
            "model_fitted": model,
            "predictions": array_y_hat,
            "adjusted_rand_score": adjusted_rand_score(array_y, array_y_hat),
            "score": model.score(array_x, array_y),
            "accuracy_score": accuracy_score(array_y, array_y_hat),
            "confusion_matrix": confusion_matrix(array_y, array_y_hat),
            "classes": model.classes_,
        }

    def k_means(
        self,
        n_clusters: int,
        array_y: NDArray[np.float64],
        array_x: NDArray[np.float64],
        int_random_state_seed: int = 0
    ) -> ResultClassification:
        """Perform K-means clustering.

        Parameters
        ----------
        n_clusters : int
            Number of clusters
        array_y : NDArray[np.float64]
            Target array
        array_x : NDArray[np.float64]
            Feature array
        int_random_state_seed : int
            Random seed, default 0

        Returns
        -------
        ResultClassification
            Model and performance metrics

        References
        ----------
        [1] https://www.hashtagtreinamentos.com/k-means-para-clusterizar-ciencia-dados
        scikit-learn KMeans documentation
        """
        model = KMeans(n_clusters=n_clusters, random_state=int_random_state_seed)
        model.fit(array_x, array_y)
        array_y_hat = model.predict(array_x)
        return {
            "model_fitted": model,
            "predictions": array_y_hat,
            "adjusted_rand_score": adjusted_rand_score(array_y, array_y_hat),
            "score": model.score(array_x, array_y),
            "accuracy_score": accuracy_score(array_y, array_y_hat),
            "confusion_matrix": confusion_matrix(array_y, array_y_hat),
        }

    def naive_bayes(
        self,
        array_x: NDArray[np.float64],
        array_y: NDArray[np.float64]
    ) -> ResultClassification:
        """Train Gaussian Naive Bayes classifier.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Feature array
        array_y : NDArray[np.float64]
            Target array

        Returns
        -------
        ResultClassification
            Model and performance metrics

        References
        ----------
        scikit-learn GaussianNB documentation
        """
        model = GaussianNB()
        model.fit(array_x, array_y)
        array_y_hat = model.predict(array_x)
        return {
            "model_fitted": model,
            "predictions": array_y_hat,
            "adjusted_rand_score": adjusted_rand_score(array_y, array_y_hat),
            "score": model.score(array_x, array_y),
            "accuracy_score": accuracy_score(array_y, array_y_hat),
            "confusion_matrix": confusion_matrix(array_y, array_y_hat),
            "classes": model.classes_,
        }


class ImageProcessing(metaclass=TypeChecker):
    """Class for image processing tasks."""

    def img_dims(self, name_path: str) -> tuple:
        """Get image dimensions.

        Parameters
        ----------
        name_path : str
            Path to image file

        Returns
        -------
        tuple
            Image dimensions (height, width, channels)

        References
        ----------
        matplotlib.pyplot.imread documentation
        """
        return plt.imread(name_path).shape

    def pca_with_var_exp(
        self,
        name_path: str,
        var_exp: float
    ) -> NDArray[np.float64]:
        """Apply PCA with specified variance explained.

        Parameters
        ----------
        name_path : str
            Path to image file
        var_exp : float
            Variance to retain

        Returns
        -------
        NDArray[np.float64]
            Transformed image array

        References
        ----------
        [1] https://leandrocruvinel.medium.com/pca-na-mão-e-no-python-d559e9c8f053
        scikit-learn PCA documentation
        """
        img = plt.imread(name_path)
        # handle both color and grayscale images
        if img.ndim == 3:  # color image (height, width, channels)
            height, width, channels = img.shape
            img_reshaped = img.reshape(-1, channels)
        else:  # grayscale image (height, width)
            height, width = img.shape
            img_reshaped = img.reshape(-1, 1)
        
        model = PCA(var_exp)
        array_transformed = model.fit_transform(img_reshaped)
        reconstructed = model.inverse_transform(array_transformed)
        # reshape back to original dimensions
        if img.ndim == 3:
            return reconstructed.reshape(height, width, channels)
        return reconstructed.reshape(height, width)

    def plot_subplot(
        self,
        int_exp_var_ratio: int,
        *array_x: NDArray[np.float64]
    ) -> None:
        """Plot subplot of image array.

        Parameters
        ----------
        int_exp_var_ratio : int
            Explained variance ratio
        *array_x : NDArray[np.float64]
            Image arrays to plot

        References
        ----------
        [1] https://leandrocruvinel.medium.com/pca-na-mão-e-no-python-d559e9c8f053
        matplotlib.pyplot.subplot documentation
        """
        plt.subplot(3, 2, int_exp_var_ratio)
        # remove single-dimensional entries
        img = np.squeeze(array_x[0])
        plt.imshow(img, cmap="gray")
        plt.xticks([])
        plt.yticks([])