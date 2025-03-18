# HANDLING INTERPOLATION LIBRARIES FOR PYTHON

import numpy as np
from scipy.interpolate import interp1d, CubicSpline, lagrange


class Interpolation:

    def set_as_array(self, array_):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return np.array(array_)

    def linear_interpolation(self, array_x, array_y):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return interp1d(self.set_as_array(array_x), self.set_as_array(array_y))

    def cubic_spline(self, array_x, array_y, bc_type='natural'):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return CubicSpline(self.set_as_array(array_x), self.set_as_array(array_y),
                           bc_type=bc_type)

    def lagrange(self, array_x, array_y):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return lagrange(self.set_as_array(array_x), self.set_as_array(array_y))

    def divided_diff_newton_polynomial_interpolation(self, array_x, array_y):
        """
        REFERENCES: PYTHON PROGRAMMING AND NUMERICAL METHODS - A GUIDE FOR ENGINEERS AND SCIENTISTS;
            QINGKAI KON, TIMMY SIAUW AND ALEXANDRE M. BAYERN
            https://pythonnumericalmethods.berkeley.edu/notebooks/chapter17.05-Newtons-Polynomial-Interpolation.html
        DOCSTRING: FUNCTION TO CALCULATE THE DIVIDED DIFFERENCE TABLE
        INPUTS: ARRAY INDEPENDENT AND DEPENDENT
        OUTPUTS: ARRAY
        """
        # setting initial parameters
        n = len(array_y)
        matrix_coef = np.zeros([n, n])
        # the first column is array_y
        matrix_coef[:, 0] = array_y
        # looping through i and j to build the coefficients
        for j in range(1, n):
            for i in range(n - j):
                matrix_coef[i][j] = (matrix_coef[i + 1][j - 1] - matrix_coef[i][j - 1]) \
                    / (array_x[i + j] - array_x[i])
        return matrix_coef

    def newton_polynomial_interpolation(self, array_x, array_y,
                                        array_x_range):
        """
        DOCSTRING: EVALUATE THE NEWTON POLYNOMIAL AT NEW DATA (ARRAY INDEP NEW)
        INPUTS: ARRAY INDEPENDENT, DEPENDENT (OF GIVEN DATA POINTS POINTS) AND ARRAY OF X RANGE
        OUTPUTS: ARRAY OF Y RANGE
        """
        # length of independent old array
        n = len(array_x) - 1
        # matrix of divided different coefficients
        matrix_coef = self.divided_diff_newton_polynomial_interpolation(
            array_x, array_y)
        # setting initial parameters of new dependent array
        array_y_range = matrix_coef[n]
        # looping through all data of array independent
        for k in range(1, n + 1):
            #   creating y axis to plotted
            array_y_range = matrix_coef[n - k] + (array_x_range - array_x[
                n - k]) * array_y_range
        return array_y_range
