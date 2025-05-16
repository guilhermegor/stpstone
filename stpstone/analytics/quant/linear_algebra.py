### LINEAR ALGEBRA ALGORITHMS ###

import numpy as np


class LinearAlgebra:

    def cholesky_decomposition(self, array_data, bool_lower_triangle=True):
        """
        REFERENCE: https://www.quantstart.com/articles/Cholesky-Decomposition-in-Python-and-NumPy/
        DOCSTRING: CHOLESKY DECOMPOSITION, USED TO SOLVE THE EQUATION A = L * Lt, WHERE A IS
            THE INPUT MATRIX
        INPUTS: ARRAY '[[]]', NO NEED TO IMPORT ARRAY FUNCTION TO RECOGNISE ITS BEHAVIOUR; AND
            BOOLEAN LOWER TIANGLE (FALSE WOULD IMPLY IN A UPPER TRIANGLE)
        OUTPUTS: CHOLESKY MATRIX DECOMPOSITION, FOR LOWER OR UPPER TRIANGLE
        """
        return np.linalg.cholesky(np.array(array_data), lower=bool_lower_triangle)

    def eigenvalue_eigenvector(self, array_data):
        """
        DOCSTRING: EIGENVALUE AND EIGENarray FOR LINEAR ALGEBRA ISSUES
        INPUTS: ARRAY '[[]]', NO NEED TO IMPORT ARRAY FUNCTION TO RECOGNISE ITS BEHAVIOUR
        OUTPUTS: EIGENVALUE AND EIGENVECTOR ARRAY
        """
        return np.linalg.eig(np.array(array_data))

    def transpose_matrix(self, array_data):
        """
        DOCSTRING: TRANSPOSE AN ARRAY
        INPUTS: ARRAY '[[]]', NO NEED TO IMPORT ARRAY FUNCTION TO RECOGNISE ITS BEHAVIOUR
        OUTPUTS: TRANSPOSED MATRIX IN AN ARRAY
        """
        return np.transpose(np.array(array_data))

    def matrix_multiplication(self, array_data_1, array_data_2):
        """
        DOCSTRING: MULTIPLY TWO MATRICES
        INPUTS: TWO ARRAIES '[[]]', NO NEED TO IMPORT ARRAY FUNCTION TO RECOGNISE ITS BEHAVIOUR
        OUTPUTS: MULTIPLICATION OF TWO MATRICES
        """
        # transforming data type
        array_data_1, array_data_2 = \
            [np.array(x) for x in [array_data_1, array_data_2]]
        # returing data
        return array_data_1.dot(array_data_2)

    def power_matrix(self, array_data, n):
        """
        DOCSTRING: POWER A MATRIX N TIMES
        INPUTS: ARRAY '[[]]', NO NEED TO IMPORT ARRAY FUNCTION TO RECOGNISE ITS BEHAVIOUR AND
            NTH-POWER
        OUTPUTS: POWERED MATRIX IN AN ARRAY
        """
        return np.linalg.matrix_power(np.array(array_data), n)

    def sqrt_matrix(self, array_data, n):
        """
        DOCSTRING: POWER A MATRIX N TIMES
        INPUTS: ARRAY '[[]]', NO NEED TO IMPORT ARRAY FUNCTION TO RECOGNISE ITS BEHAVIOUR AND
            NTH-SQRT
        OUTPUTS: SQRTED MATRIX IN AN ARRAY
        """
        return np.linalg.sqrtm(np.array(array_data), n)

    def shape_array(self, array_data):
        """
        DOCSTRING: SHAPE OF ARRAY
        INPUTS: ARRAY '[[]]', NO NEED TO IMPORT ARRAY FUNCTION TO RECOGNISE ITS BEHAVIOUR
        OUTPUTS: SHAPE OF ARRAY - TUPLE
        """
        return np.shape(np.array(array_data))

    def covariance_arraies(self, array_data):
        """
        DOCSTRING: COVARIANCE OF ARRAY
        INPUTS: ARRAY '[[]]', NO NEED TO IMPORT ARRAY FUNCTION TO RECOGNISE ITS BEHAVIOUR
        OUTPUTS: FLOAT
        """
        return np.cov(np.array(array_data))

    def euclidian_distance(self, array_data_1, array_data_2):
        """
        DOCSTRING: EUCLIDIAN DISTANCE BETWEEN TWO VECTORS
        INPUTS: ARRAY NUMBER 1 AND 2
        OUTPUTS: FLOAT
        """
        # transforming data type
        array_data_1, array_data_2 = \
            [np.array(x) for x in [array_data_1, array_data_2]]
        # returing data
        return np.linalg.norm(array_data_1 - array_data_2)

    def identity(self, n):
        """
        DOCSTRING: IDENTITY MATRICE
        INPUTS: DIMENSION (N)
        OUTPUTS: FLOAT
        """
        return np.identity(n)

    def angle_between_two_arrays(self, array_data_1, array_data_2):
        """
        DOCSTRING: ANGLE BETWEEN TWO ARRAYS
        INPUTS: ARRAY NUMBER 1 AND 2
        OUTPUTS: FLOAT
        """
        # transforming data type
        array_data_1, array_data_2 = \
            [np.array(x) for x in [array_data_1, array_data_2]]
        # returing data
        return np.arccos(np.dot(array_data_1, array_data_2.T) / (
            np.linalg.norm(array_data_1) * np.linalg.norm(array_data_2)))

    def determinant(self, array_data):
        """
        DOCSTRING: DETERMINANT OF A QUADRATIC MATRIX - A MATRIX THAT THE DETERMINANT IS CLOSE TO ZERO
            IS CALLED ILL-CONDITIONED - ALTHOUGH ILL-CONDITIONED MATRICES HAVE INVERSES, THEY ARE
            PROBLEMATIC NUMERICALLY, RESULTING IN OVERFLOW, UNDERFLOW, OR NUMBERS SMALL ENOUGH TO
            RESULT IN SIGNIFICANT ROUND-OFF ERRORS
        INPUTS: ARRAY '[[]]', NO NEED TO IMPORT ARRAY FUNCTION TO RECOGNISE ITS BEHAVIOUR
        OUTPUTS: SHAPE OF ARRAY - TUPLE
        """
        return np.linalg.det(np.array(array_data))

    def inverse(self, array_data):
        """
        DOCSTRING: INVERSE OF A MATRICE (M * N = I), MATRICES WITHOUT AN INVERTED ONE ARE CALLED
            NONSINGULAR, BEING THE OPPOSITE TRUE AS WELL
        INPUTS: ARRAY '[[]]', NO NEED TO IMPORT ARRAY FUNCTION TO RECOGNISE ITS BEHAVIOUR
        OUTPUTS: SHAPE OF ARRAY - TUPLE
        """
        return np.linalg.inv(np.array(array_data))

    def condition_number(self, array_data):
        """
        DOCSTRING: THE MATCH_PATTERN NUMBER IS A MEASURE OF HOW ILL-CONDITIONED A MATRIX IS: IT IS
            DEFINED AS THE NORM OF THE MATRIX TIMES THE NORM OF THE INVERSE OF THE MATRIX - THE
            HIGHER THE NUMBER, THE CLOSER THE MATRIX IS TO BEING SINGULAR
        INPUTS: ARRAY '[[]]', NO NEED TO IMPORT ARRAY FUNCTION TO RECOGNISE ITS BEHAVIOUR
        OUTPUTS: SHAPE OF ARRAY - TUPLE
        """
        return np.linalg.cond(np.array(array_data))

    def matrix_rank(self, array_data):
        """
        DOCSTRING: THE RANK OF AN MXN MATRIX A IS THE NUMBER OF LINEARLY INDEPEDENT COLUMNS OR ROWS
            OF A AND IS DENOTED BY RANK(A) - THE NUMBER OF LINEARLY INDEPENDENT ROWS IS ALWAYS EQUAL
            TO THE NUMBER OF LINEARLY INDEPENDENT COLUMNS FOR ANY MATRIX - A MATRIX HAS FULL RANK IF
            RANK(A) = MIN(M,N) - THE THE MATRIX A IS ALSO OF FULL RANK IF ALL OF ITS COLUMNS ARE
            LINEARLY INDEPENDENT
        INPUTS: ARRAY '[[]]', NO NEED TO IMPORT ARRAY FUNCTION TO RECOGNISE ITS BEHAVIOUR
        OUTPUTS: SHAPE OF ARRAY - TUPLE
        """
        return np.linalg.matrix_rank(np.array(array_data))

    def linear_equations_solution(self, array_data_1, array_data_2):
        """
        DOCSTRING: LINEAR EQUATION OF A AUGMENTED MATRIX
        INPUTS: ARRAY NUMBER 1 AND 2
        OUTPUTS: ARRAY
        """
        # transforming data type
        array_data_1, array_data_2 = \
            [np.array(x) for x in [array_data_1, array_data_2]]
        # returing data
        return np.linalg.solve(array_data_1, array_data_2)
