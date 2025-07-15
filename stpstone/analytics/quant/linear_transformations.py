"""Linear Algebra."""

from collections.abc import Sequence
from numbers import Number
from typing import Union

import numpy as np
import numpy.typing as npt
from scipy.linalg import sqrtm

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class LinearAlgebra(metaclass=TypeChecker):
    """Class Linear Algebra."""

    def add(
        self, 
        array_1: Union[npt.ArrayLike, Sequence[Number]], 
        array_2: Union[npt.ArrayLike, Sequence[Number], Number]
    ) -> np.ndarray:
        """Element-wise addition of two arrays or vectors.
        
        Parameters
        ----------
        array_1 : ArrayLike
            First input array (can be list, tuple, or numpy array)
        array_2 : ArrayLike
            Second input array (must match dimensions of array_1)
            
        Returns
        -------
        np.ndarray
            The element-wise sum of the inputs
            
        Examples
        --------
        >>> la = LinearAlgebra()
        >>> la.add([1, 2, 3], [4, 5, 6])
        array([5, 7, 9])
        
        >>> la.add(np.array([1.5, 2.5]), np.array([0.5, 1.5]))
        array([2., 4.])
        
        Notes
        -----
        - For matrix addition, both inputs must have identical shapes
        - Broadcasting rules apply as per numpy's standard behavior
        - Converts all inputs to numpy arrays before operation
        """
        arr1 = np.asarray(array_1)
        arr2 = np.asarray(array_2)
        return arr1 + arr2

    def distance(
        self, 
        array_1: Union[npt.ArrayLike, Sequence[Number]], 
        array_2: Union[npt.ArrayLike, Sequence[Number]]
    ) -> float:
        """Compute the Euclidean distance between two vectors.
        
        Parameters
        ----------
        array_1 : ArrayLike
            First input vector
        array_2 : ArrayLike
            Second input vector (must match dimension of array_1)
            
        Returns
        -------
        float
            The Euclidean distance between the vectors
            
        Examples
        --------
        >>> la = LinearAlgebra()
        >>> la.distance([1, 0], [0, 1])
        1.4142135623730951
        
        >>> la.distance(np.array([1, 2, 3]), np.array([4, 5, 6]))
        5.196152422706632
        
        Notes
        -----
        - For n-dimensional vectors, computes sqrt(Σ(x_i - y_i)^2)
        - Equivalent to the L2 norm of the difference vector
        - Works with any array-like input that can be converted to numpy array
        """
        arr1 = np.asarray(array_1)
        arr2 = np.asarray(array_2)
        return float(np.linalg.norm(arr1 - arr2))

    def scalar_multiply(
        self, 
        scalar: Number, 
        array: Union[npt.ArrayLike, Sequence[Number]]
    ) -> np.ndarray:
        """Multiply an array by a scalar value.
        
        Parameters
        ----------
        scalar : Union[int, float]
            The scalar multiplier
        array : ArrayLike
            The array to be multiplied
            
        Returns
        -------
        np.ndarray
            The result of scalar multiplication
            
        Examples
        --------
        >>> la = LinearAlgebra()
        >>> la.scalar_multiply(2, [1, 2, 3])
        array([2, 4, 6])
        
        >>> la.scalar_multiply(0.5, np.array([10., 20., 30.]))
        array([ 5., 10., 15.])
        
        Notes
        -----
        - Works with any numeric scalar (int, float)
        - Preserves the dtype of the input array
        - Returns a new array rather than modifying in-place
        """
        arr = np.asarray(array)
        return scalar * arr

    def cholesky_decomposition(
        self, 
        array_data: np.ndarray, 
        bl_lower_triangle: bool=True
    ) -> np.ndarray:
        """Cholesky decomposition performed on a symmetric, positive-definite matrix.

        Cholesky decomposition is a numerical method that factorizes a given matrix 
        into the product of a lower triangular matrix and its transpose. Specifically, 
        for a symmetric positive-definite matrix A, the decomposition yields:

            A = L @ L.T    if bl_lower_triangle is True, or
            A = U.T @ U    if bl_lower_triangle is False,

        where L is a lower triangular matrix and U is its transpose (an upper triangular matrix).

        This decomposition is widely used in numerical linear algebra for solving systems of 
        linear equations, simulating correlated variables, and efficiently performing 
        matrix inversion in various applications such as Monte Carlo simulations and 
        Kalman filtering.

        Parameters
        ----------
        array_data : np.ndarray
            Symmetric, positive-definite matrix to be decomposed.
        bl_lower_triangle : bool, optional
            If True (default), returns the lower-triangular matrix L such that A = L @ L.T.
            If False, returns the upper-triangular matrix U such that A = U.T @ U.

        Returns
        -------
        np.ndarray
            The lower or upper triangular matrix resulting from the decomposition.

        References
        ----------
        https://www.quantstart.com/articles/Cholesky-Decomposition-in-Python-and-NumPy/
        """
        L = np.linalg.cholesky(array_data)
        return L if bl_lower_triangle else L.T

    def eigenvalue_eigenvector(self, array_data: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """Eigenvector and Eigenvalue.
        
        Parameters
        ----------
        array_data : np.ndarray
            Array to be decomposed.
        
        Returns
        -------
        tuple[np.ndarray, np.ndarray]
            Decomposed array.
        """
        return np.linalg.eig(array_data)

    def transpose_matrix(self, array_data: np.ndarray) -> np.ndarray:
        """Transpose Matrix.
        
        Parameters
        ----------
        array_data : np.ndarray
            Array to be decomposed.
        
        Returns
        -------
        np.ndarray
            Decomposed array.
        """
        return np.transpose(array_data)

    def matrix_multiplication(self, array_1: np.ndarray, array_2: np.ndarray) -> np.ndarray:
        """Matrix Multiplication.
        
        Parameters
        ----------
        array_1 : np.ndarray
            Array to be decomposed.
        array_2 : np.ndarray
            Array to be decomposed.
        
        Returns
        -------
        np.ndarray
            Decomposed array.
        """
        return array_1.dot(array_2)

    def power_matrix(self, array_data: np.ndarray, n: int) -> np.ndarray:
        """Power Matrix.
        
        Parameters
        ----------
        array_data : np.ndarray
            Array to be decomposed.
        n : int
            Array to be decomposed.
        
        Returns
        -------
        np.ndarray
            Decomposed array.
        """
        return np.linalg.matrix_power(array_data, n)

    def sqrt_matrix(self, array_data: np.ndarray) -> np.ndarray:
        """"Square Root Matrix.
        
        Parameters
        ----------
        array_data : np.ndarray
            Array to be decomposed.
        n : int
            Array to be decomposed.
        
        Returns
        -------
        np.ndarray
            Decomposed array.
        """
        return sqrtm(array_data)

    def shape_array(self, array_data: np.ndarray) -> tuple[int, int]:
        """Shape Array.
        
        Parameters
        ----------
        array_data : np.ndarray
            Array to be decomposed.
        
        Returns
        -------
        tuple[int, int]
            Decomposed array.
        """
        if array_data.size == 0:
            raise ValueError("Cannot get the shape of an empty array.")
        return np.shape(array_data)

    def covariance_arrays(self, array_data: np.ndarray) -> np.ndarray:
        """Covariance Arrays.
        
        Parameters
        ----------
        array_data : np.ndarray
            Array to be decomposed.
        
        Returns
        -------
        np.ndarray
        """
        return np.cov(array_data)

    def euclidean_distance(self, array_1: np.ndarray, array_2: np.ndarray) -> float:
        """Euclidean Distance.
        
        Distance between two vectors.
        
        Parameters
        ----------
        array_1 : np.ndarray
            Array to be decomposed.
        array_2 : np.ndarray
            Array to be decomposed.
        
        Returns
        -------
        float
            Decomposed array.
        """
        return np.linalg.norm(array_1 - array_2)

    def identity(self, n: int) -> np.ndarray:
        """Identity Matrix.
        
        Parameters
        ----------
        n : int
            Array to be decomposed.
        
        Returns
        -------
        np.ndarray
            Decomposed array.
        """
        return np.identity(n)

    def angle_between_two_arrays(self, array_1: np.ndarray, array_2: np.ndarray) -> float:
        """Angle between two arrays.
        
        Parameters
        ----------
        array_1 : np.ndarray
            Array to be decomposed.
        array_2 : np.ndarray
            Array to be decomposed.
        
        Returns
        -------
        float
            Decomposed array.
        """
        return np.arccos(np.dot(array_1, array_2.T) / (
            np.linalg.norm(array_1) * np.linalg.norm(array_2)))

    def determinant(self, array_data: np.ndarray) -> float:
        """Matrix determinant.
        
        Parameters
        ----------
        array_data : np.ndarray
            Array to be decomposed.
        
        Returns
        -------
        float
            Decomposed array.
        """
        return np.linalg.det(array_data)

    def inverse(self, array_data: np.ndarray) -> np.ndarray:
        """Matrix inverse.
        
        Parameters
        ----------
        array_data : np.ndarray
            Array to be decomposed.
        
        Returns
        -------
        np.ndarray
            Decomposed array.
        """
        return np.linalg.inv(array_data)

    def condition_number(self, array_data: np.ndarray) -> float:
        """Condition number.
        
        Parameters
        ----------
        array_data : np.ndarray
            Array to be decomposed.
        
        Returns
        -------
        float
            Decomposed array.
        """
        return np.linalg.cond(array_data)

    def matrix_rank(self, array_data: np.ndarray) -> int:
        """Compute the rank of a matrix.
        
        The rank of a matrix is the maximum number of linearly independent row or column vectors
        in the matrix. It represents the dimension of the vector space spanned by its rows or 
        columns.
        
        Parameters
        ----------
        array_data : np.ndarray
            Input matrix of shape (M, N). The matrix can be either:
            - Real-valued (float)
            - Complex-valued (complex)
            - Any numeric type that can be converted to float
        
        Returns
        -------
        int
            The rank of the matrix, which satisfies 0 ≤ rank ≤ min(M, N)
        
        Notes
        -----
        - A full rank matrix has rank = min(M, N)
        - A singular matrix has rank < min(M, N)
        - The rank is computed using singular value decomposition (SVD) with a numerical tolerance
        threshold for determining when a singular value should be considered zero
        
        Examples
        --------
        >>> la = LinearAlgebra()
        >>> # Full rank matrix
        >>> A = np.array([[1, 2], [3, 4]])
        >>> la.matrix_rank(A)
        2
        
        >>> # Rank-deficient matrix
        >>> B = np.array([[1, 2], [2, 4]])  # Second row is 2× first row
        >>> la.matrix_rank(B)
        1
        
        >>> # Empty matrix
        >>> la.matrix_rank(np.array([]))
        0
        
        Applications
        -----------
        - Determining if a system of linear equations has solutions
        - Checking for linear dependence in datasets
        - Analyzing the dimensionality of a transformation
        
        See Also
        --------
        numpy.linalg.matrix_rank : The underlying NumPy function used for computation
        """
        return int(np.linalg.matrix_rank(array_data))

    def linear_equations_solution(self, array_1: np.ndarray, array_2: np.ndarray) -> np.ndarray:
        """Linear Equations Solution.
        
        Parameters
        ----------
        array_1 : np.ndarray
            Array to be decomposed.
        array_2 : np.ndarray
            Array to be decomposed.
        
        Returns
        -------
        np.ndarray
            Decomposed array.
        """
        return np.linalg.solve(array_1, array_2)
