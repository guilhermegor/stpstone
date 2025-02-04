### STATISTICAL DESCRIPTION ###

import numpy as np
from scipy import stats
from sklearn.metrics import mean_squared_error, mean_absolute_error


class StatiscalDescription:

    def statistical_description(self, array_data):
        """
        DOCSTRING: STATISTICAL DISCRIPTION (NOBS, MIN, MAX, MEAN, VARIANCE, SKEWNESS, KURTOSIS)
        INPUTS: array OF REAL NUMBERS
        OUTPUTS: DICTIONARY
        """
        return {
            'nobs': stats.describe(np.array(array_data)).nobs,
            'minmax': stats.describe(np.array(array_data)).minmax,
            'mean': stats.describe(np.array(array_data)).mean,
            'median': stats.median(np.array(array_data)),
            'mode': stats.mode(np.array(array_data)),
            'variance_sample': stats.describe(np.array(array_data)).variance,
            'standard_deviation_sample': stats.describe(np.array(
                array_data)).variance ** 0.5,
            'skewness': stats.describe(np.array(array_data)).skewness,
            'kurtosis': stats.describe(np.array(array_data)).kurtosis
        }

    def standard_deviation_sample(self, array_data):
        """
        DOCSTRING: STATISTICAL DEVIATION OF A SAMPLE
        INPUTS: array OF REAL NUMBERS
        OUTPUTS: STATISTICAL DEVIATION - SAMPLE
        """
        return stats.tstd(np.array(array_data))

    def harmonic_mean(self, array_data):
        """
        DOCSTRING: HARMONIC MEAN OF A SAMPLE
        INPUTS: array OF REAL NUMBERS
        OUTPUTS: HARMONIC MEAN
        """
        return stats.harmonic_mean(np.array(array_data))

    def median_sample(self, array_data):
        """
        DOCSTRING: MEDIAN OF A SAMPLE
        INPUTS: array OF REAL NUMBERS
        OUTPUTS: MEDIAN
        """
        return stats.median(np.array(array_data))

    def mode_sample(self, array_data):
        """
        DOCSTRING: MODE OF A SAMPLE
        INPUTS: array OF REAL NUMBERS
        OUTPUTS: MODE
        """
        return stats.mode(np.array(array_data))

    def covariance(self, array_data_1, array_data_2):
        """
        REFERENCES: https://stackoverflow.com/questions/48105922/numpy-covariance-between-each-column-of-a-matrix-and-a-array
        DOCSTRING: COVARIANCE OF ARRAY
        INPUTS: TWO ARRAIES '[[]]', NO NEED TO IMPORT ARRAY FUNCTION TO RECOGNISE ITS BEHAVIOUR
        OUTPUTS: COVARIANCE - FLOAT
        """
        return np.dot(np.array(array_data_2).T - np.array(
            array_data_2).mean(), np.array(array_data_1) - np.array(
                array_data_1).mean(axis=0)) / (np.array(array_data_2).shape[0] - 1)

    def correlation(self, array_data_1, array_data_2):
        """
        REFERENCES: https://stackoverflow.com/questions/48105922/numpy-covariance-between-each-column-of-a-matrix-and-a-array
        DOCSTRING: TWO ARRAIES '[[]]', NO NEED TO IMPORT ARRAY FUNCTION TO RECOGNISE ITS BEHAVIOUR
        OUTPUTS:
        """
        return self.covariance(array_data_1, array_data_2) / \
            np.sqrt(np.var(np.array(array_data_2), ddof=1) * np.var(
                np.array(array_data_1), axis=0, ddof=1))

    def mean_squared_error_fitting_precision(self, array_observations, array_predictions):
        """
        DOCSTRING: MEAN SQUARED ERROR TO TEST FITTING PRECISION
        INPUTS: LIST OF OBSERVATIONS AND LIST OF PREDICTIONS
        OUTPUTS: DICTIONAY WITH MEAN SQUARED ERROR AND ITS SQUARED ROOT
        """
        float_mean_squared_error = mean_squared_error(array_observations,
                                                      array_predictions)
        return {
            'mean_squared_error': float_mean_squared_error,
            'sqrt_mean_squared_error': np.sqrt(float_mean_squared_error)
        }

    def mean_absolute_error_fitting_precision(self, array_observations, array_predictions):
        """
        DOCSTRING: MEAN ABSOLUTE ERROR FITTING PRECISION, USED FOR MODELS WITH A PLENTY
            OF OUTLIERS
        INPUTS: LIST OF OBSERVATIONS AND LIST OF PREDICTIONS
        OUTPUTS: DICTIONAY WITH MEAN ABSOLUTE SQUARED ERROR AND ITS SQUARED ROOT
        """
        float_mean_absolute_squared_error = mean_absolute_error(array_observations,
                                                                array_predictions)
        return {
            'mean_absolute_squared_error': float_mean_absolute_squared_error,
            'sqrt_mean_absolute_squared_error': np.sqrt(float_mean_absolute_squared_error)
        }

    def quantile(self, array_data, q, axis=0):
        """
        DOCSTRING: QUANTILE
        INPUTS: ARRAY, QUANTILE BETWEEN 0 AND 1, AXIS (0 FOR FLATENNED OR 1 FOR MATRIC)
        OUTPUTS: ARRAY
        """
        return np.quantile(array_data, q, axis=axis)
