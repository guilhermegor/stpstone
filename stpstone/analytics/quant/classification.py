### HANDLING CLASSIFICATION ISSUES ###

# pypi.org libs
import matplotlib.pyplot as plt
from sklearn.datasets import fetch_openml
from sklearn.linear_model import SGDClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, precision_score, recall_score, f1_score, \
    roc_auc_score, adjusted_rand_score, silhouette_score, accuracy_score
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.multiclass import OneVsOneClassifier, OneVsRestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import PCA
from sklearn.naive_bayes import GaussianNB
# local libs
from stpstone.handling_data.lists import ListHandler
from stpstone.utils.parsers.str import StrHandler


class InputsClassification:

    def fetch_sklearn_database(self, database_name='mnist_784', version=1, bl_asframe=False):
        """
        DOCSTRING: DATASET FROM SCIKIT-LEARN, WHICH NORMALLY HAVE A SIMILAR DICTIONARY
            STRUCTURE: DESCR (DESCRIPTION OF THE DATASET), DATA (ARRAY WITH ONE ROW PER
            INSTANCE AND ONE COLUMN PER FEATURE), AND TARGET (AN ARRAY WITH LABELS)
        INPUTS: DATABASE NAME (MNIST_784 AS DEFAULT) AND VERSION
        OUTPUTS: DICTIONARY WITH DATA, TARGET, FEATURE NAMES, DESCR, DETAILS, CATEGORIES AND
            URL
        """
        return fetch_openml(database_name, version=version, as_frame=bl_asframe)

    def show_image_from_dataset(self, array_instance, cmap='binary', shape=(28, 28),
                                bl_axis='off', complete_saving_path=None):
        """
        DOCSTRING: SHOW IMAGE FROM DATASET
        INPUTS: VECTOR INSTANCE, CMAP (DEFAULT), SHAPE (DEFAULT)
        OUTPUTS: -
        """
        # reshape image
        array_instance = array_instance.reshape(shape)
        # creating image
        plt.imshow(array_instance, cmap=cmap)
        plt.axis(bl_axis)
        # saving the plot, if is user's will
        if complete_saving_path != None:
            plt.savefig(complete_saving_path)
        # show result
        plt.show()


class Classification:

    def one_hot_vectorizer(self, list_corpus):
        """
        DOCSTRING: ONE HOT VECTORIZER, AIMING TO ENCODE A LIST OF STRINGS AND CONVERT TO AN ARRAY
        INPUTS: CORPUS (LIST TYPE)
        OUTPUTS: DICTIONARY (LABLES AND ARRAY OF ONE HOT ENCODER)
        """
        # list of words to vectorizer - alphabetical order
        list_labels = ListHandler().extend_lists(
            *[x.split() for x in list_corpus])
        list_labels = [StrHandler().remove_sup_period_marks(x).lower()
                       for x in ListHandler().remove_duplicates(list_labels)]
        list_labels.sort()
        # one hot vectorizer
        one_hot_vectorizer = CountVectorizer(binary=True)
        array_one_hot = one_hot_vectorizer.fit_transform(list_corpus).toarray()
        # return dictionary with labels and one hot encoder
        return {
            'labels': list_labels,
            'one_hot_encoder': array_one_hot
        }

    def sgd_classifier(self, array_x, array_y, int_random_state_seed=5):
        """
        DOCSTRING: TRAINING A BINARY CLASSIFIER WITH STOCHASTIC GRADIENT DESCENT (SGD),
            TO EAVLUATE WHTEHER OR NOT A NEW ELEMENT BELONGS TO THE SUBSET, WHICH
            HAS THE ADVANTAGE OF BEING CAPABLE OF HANDLING VERY LARGE DATASETS EFFICIENTLY
        INPUTS: ARRAY DATA, ARRAY TARGET, LIST OF DATA TO PREDICT AND RANDOM STATE SEED
            (5 AS DEFAULT)
        OUTPUTS: DICTIONARY WITH KEYS model AND PREDICTIONS (DATALABEL AND WHETHER IT
            BELONGS TO THE ARRAY DATA OR NOT, CONSIDERING A CLASSIFICATION METHOD)
        """
        # estimator
        model = SGDClassifier(random_state=int_random_state_seed)
        # fitting model
        model.fit(array_x, array_y)
        # array predictions
        array_y_hat = model.predict(array_x)
        # returning model fitted and predictions
        return {
            'model_fitted': model,
            'predictions': array_y_hat,
            'adjusted_rand_score': adjusted_rand_score(array_y, array_y_hat),
            'score': model.score(array_x, array_y),
            'accuracy_score': accuracy_score(array_y, array_y_hat),
            'confusion_matrix': confusion_matrix(array_y, array_y_hat),
            'classes': model.classes_,
        }

    def svm(
            self, array_x, array_y, kernel='rbf', float_regularization_parameter=1,
            multiclass_classification_strategy='best',
            gamma='auto', int_random_state_seed=42):
        """
        DOCSTRING: SUPPORT VECTOR MACHINE CLASSIFICATION TO PREDICT, USING ONE VERSUS ONE
            STRATEGY, TRAINING BINARY CLASSIFIERS, GETTING THEIR DECISION SCORES FOR THE DATA,
            AND CHOOSING THE MODEL WHICH HAS AN OPTIMIZED RESPONSE, MATCHING TARGET CLASSIFICATION
            IN MOST CASES
        INPUTS: ARRAY X, ARRAY Y, KERNEL (LINEAR OR RBF ARE THE MOST COMMON), REGUALARIZATION
            PARAMETER (1 AS DEFAULT), MULTICLASS CLASSIFICATION STRATEGY, GAMMA AND RANDOM STATE SEED
        OUTPUTS: DICT
        """
        # classifier to the supporting vector machine classification, regarding multiclass
        #   classification strategy (best, ovr or ovo)
        if multiclass_classification_strategy == 'best':
            model = SVC(C=float_regularization_parameter, gamma=gamma,
                          random_state=int_random_state_seed, kernel=kernel)
        elif multiclass_classification_strategy == 'ovr':
            model = OneVsRestClassifier(
                SVC(C=float_regularization_parameter, gamma=gamma,
                    random_state=int_random_state_seed, kernel=kernel))
        elif multiclass_classification_strategy == 'ovo':
            model = OneVsOneClassifier(
                SVC(C=float_regularization_parameter, gamma=gamma,
                    random_state=int_random_state_seed, kernel=kernel))
        else:
            raise Exception(
                'Multiclass classification strategy ought be wheter best, ovr'
                + '(one-versus-the-rest) or ovo (one-versus-one), nevertheless it was declared'
                + ' {}, which is invalid, please revisit the parameter '.format(
                    multiclass_classification_strategy)
                + 'multiclass_classification_strategy')
        # fitting model
        model.fit(array_x, array_y)
        # array predictions
        array_y_hat = model.predict(array_x)
        # returning model fitted and predictions
        return {
            'model_fitted': model,
            'predictions': array_y_hat,
            'adjusted_rand_score': adjusted_rand_score(array_y, array_y_hat),
            'score': model.score(array_x, array_y),
            'accuracy_score': accuracy_score(array_y, array_y_hat),
            'confusion_matrix': confusion_matrix(array_y, array_y_hat),
            'classes': model.classes_,
        }

    def decision_tree(self, array_x, array_y, impurity_crit='gini',
                      float_max_depth=None, int_random_state_seed=42):
        """
        REFERENCES: https://www.datacamp.com/tutorial/decision-tree-classification-python
        DOCSTRING: DECISION TREE CLASSIFIER
        INPUTS: ARRAY DATA, ARRAY TARGETS AND ARRAY DATA TO BE PREDICTED
        OUTPUTS:
        """
        # classifier
        model = DecisionTreeClassifier(criterion=impurity_crit, max_depth=float_max_depth,
                                       random_state=int_random_state_seed)
        # fitting model
        model.fit(array_x, array_y)
        # array predictions
        array_y_hat = model.predict(array_x)
        # returning model fitted and predictions
        return {
            'model_fitted': model,
            'predictions': array_y_hat,
            'adjusted_rand_score': adjusted_rand_score(array_y, array_y_hat),
            'score': model.score(array_x, array_y),
            'accuracy_score': accuracy_score(array_y, array_y_hat),
            'confusion_matrix': confusion_matrix(array_y, array_y_hat),
            'classes': model.classes_,
        }

    def random_forest(self, array_x, array_y, n_estimators=100, int_random_state_seed=42):
        """
        REFERENCES: https://www.datacamp.com/tutorial/random-forests-classifier-python
        DOCSTRING: RANDOM FOREST CLASSIFIER, A.K.A. DECISION TREE ENSEMBLED
        INPUTS: ARRAY DATA, ARRAY TARGETS AND ARRAY DATA TO BE PREDICTED
        OUTPUTS:
        """
        # fitting model
        model = RandomForestClassifier(random_state=int_random_state_seed, n_estimators=n_estimators)
        # fitting model
        model.fit(array_x, array_y)
        # array predictions
        array_y_hat = model.predict(array_x)
        # returning model fitted and predictions
        return {
            'model_fitted': model,
            'predictions': array_y_hat,
            'adjusted_rand_score': adjusted_rand_score(array_y, array_y_hat),
            'score': model.score(array_x, array_y),
            'accuracy_score': accuracy_score(array_y, array_y_hat),
            'confusion_matrix': confusion_matrix(array_y, array_y_hat),
            'classes': model.classes_,
        }

    def knn_classifier(self, array_x, array_y, int_n_neighbors=5):
        """
        DOCSTRING: K NEIGHBORS CLASSIFIER
        INPUTS: ARRAY DATA, ARRAY TARGETS AND ARRAY DATA TO BE PREDICTED
        OUTPUTS:
        """
        # classifier
        model = KNeighborsClassifier(n_neighbors=int_n_neighbors)
        # fitting model
        model.fit(array_x, array_y)
        # array predictions
        array_y_hat = model.predict(array_x)
        # returning model fitted and predictions
        return {
            'model_fitted': model,
            'predictions': array_y_hat,
            'adjusted_rand_score': adjusted_rand_score(array_y, array_y_hat),
            'score': model.score(array_x, array_y),
            'accuracy_score': accuracy_score(array_y, array_y_hat),
            'confusion_matrix': confusion_matrix(array_y, array_y_hat),
            'classes': model.classes_,
        }

    def k_means(self, n_clusters, array_y, array_x, int_random_state_seed=0):
        """
        REFERENCES: https://www.hashtagtreinamentos.com/k-means-para-clusterizar-ciencia-dados?gad_source=1&gclid=Cj0KCQjwlZixBhCoARIsAIC745Bm8VTK5AMNUKTlV3TpYm6RB6ag2IGUIMEvNNYTmKmAfqN7O5vA6mwaAi6FEALw_wcB
        DOCSTRING: K-MEANS CLUSTERING FOR LABELED DATA
        INPUTS:
        OUTPUTS: DICT (LABELS - INDENTIFICATIONS OF RESPECTIVE CLUSTER, AND ADJUSTED RAND, WHICH
            INDICATES THE SCORE OF CLUSTERIZATION N_CLUSTERS-WISE)
        """
        # classifier
        model = KMeans(n_clusters=n_clusters, random_state=int_random_state_seed)
        # fitting model
        model.fit(array_x, array_y)
        # array predictions
        array_y_hat = model.predict(array_x)
        # returning model fitted and predictions
        return {
            'model_fitted': model,
            'predictions': array_y_hat,
            'adjusted_rand_score': adjusted_rand_score(array_y, array_y_hat),
            'score': model.score(array_x, array_y),
            'accuracy_score': accuracy_score(array_y, array_y_hat),
            'confusion_matrix': confusion_matrix(array_y, array_y_hat),
            'classes': model.classes_,
        }

    def naive_bayes(self, array_x, array_y):
        """
        DOCSTRING: NAIVE BAYES CLASSIFIER
        INPUTS: ARRAY DATA, ARRAY TARGETS AND ARRAY DATA TO BE PREDICTED
        OUTPUTS: DICT
        """
        # classifier
        model = GaussianNB()
        # fitting model
        model.fit(array_x, array_y)
        # array predictions
        array_y_hat = model.predict(array_x)
        # returning model fitted and predictions
        return {
            'model_fitted': model,
            'predictions': array_y_hat,
            'adjusted_rand_score': adjusted_rand_score(array_y, array_y_hat),
            'score': model.score(array_x, array_y),
            'accuracy_score': accuracy_score(array_y, array_y_hat),
            'confusion_matrix': confusion_matrix(array_y, array_y_hat),
            'classes': model.classes_,
        }

class ImageProcessing:

    def img_dims(self, name_path):
        """
        REFERENCES: https://leandrocruvinel.medium.com/pca-na-mão-e-no-python-d559e9c8f053
        DOCSTRING: IMAGE DIMENSIONS
        INPUTS:
        OUTPUTS:
        """
        # image dimensions (height x width x channels)
        return plt.imread(name_path).shape

    def pca_with_var_exp(self, name_path, var_exp):
        """
        REFERENCES: https://leandrocruvinel.medium.com/pca-na-mão-e-no-python-d559e9c8f053
        DOCSTRING: PRINCIPAL COMPONENTS ANALYSIS WITH EXPLAINED VARIANCE RESULT FOR IMAGES
        INPUTS:
        OUTPUTS:
        """
        # defining the model
        model = PCA(var_exp)
        # fit transform
        model = model.fit_transform(self.img_dims(name_path))
        # return lowered dimensioned matrice
        return model.inverse_transform(model)

    def plot_subplot(float_exp_var_ratio, *array_x):
        """
        REFERENCES: https://leandrocruvinel.medium.com/pca-na-mão-e-no-python-d559e9c8f053
        DOCSTRING: PLOT THE ORIGINAL IMAGES AND THE IV VARIANCE REDUCTIONS
        INPUTS:
        OUTPUTS:
        """
        # plot
        plt.subplot(3, 2, float_exp_var_ratio)
        plt.imshow(array_x, cmap='gray')
        plt.xticks([])
        plt.yticks([])


# corpus = ['Time flies flies like an arrow.', 'Fruit flies like a banana.']
# print(BinaryMulticlassClassification().one_hot_vectorizer(corpus))
# # output
# {'labels': ['a', 'an', 'arrow', 'banana', 'flies', 'fruit', 'like', 'time'], 'one_hot_encoder': array([[1, 1, 0, 1, 0, 1, 1],
#        [0, 0, 1, 1, 1, 1, 0]], dtype=int64)}

# corpus = ['Time flies flies like an arrow.', 'Fruit flies like a banana.']
# print(BinaryMulticlassClassification().convert_categories_from_strings_to_array(corpus, True))
