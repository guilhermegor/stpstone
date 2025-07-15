"""Module for handling various interpolation methods in Python."""

from typing import Literal, Union

import numpy as np
from scipy.interpolate import CubicSpline, interp1d, lagrange

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class Interpolation(metaclass=TypeChecker):
    """Class providing various interpolation methods for numerical data."""

    def set_as_array(self, array_: Union[list[float], np.ndarray]) -> np.ndarray:
        """Convert input to numpy array.

        Parameters
        ----------
        array_ : Union[list[float], np.ndarray]
            Input data to be converted to numpy array

        Returns
        -------
        np.ndarray
            Converted numpy array
        """
        if not isinstance(array_, (list, np.ndarray)):
            raise TypeError("Input must be a list or numpy array")
        arr = np.array(array_)
        if len(arr) == 0:
            raise ValueError("Input array cannot be empty")
        
        return np.array(array_)

    def linear_interpolation(
        self,
        array_x: Union[list[float], np.ndarray],
        array_y: Union[list[float], np.ndarray]
    ) -> interp1d:
        """Perform linear interpolation on given data points.

        Parameters
        ----------
        array_x : Union[list[float], np.ndarray]
            Independent variable data points
        array_y : Union[list[float], np.ndarray]
            Dependent variable data points

        Returns
        -------
        interp1d
            Linear interpolation function
        """
        if len(array_x) != len(array_y):
            raise ValueError("Input arrays must have same length")
        if len(array_x) < 2:
            raise ValueError("At least 2 points required for interpolation")
        if len(np.unique(array_x)) != len(array_x):
            raise ValueError("Duplicate x-values detected")
        
        return interp1d(
            self.set_as_array(array_x),
            self.set_as_array(array_y),
            kind="linear",
        )

    def cubic_spline(
        self,
        array_x: Union[list[float], np.ndarray],
        array_y: Union[list[float], np.ndarray],
        bc_type: Literal["natural", "clamped", "not-a-knot", "periodic"] = "natural",
    ) -> CubicSpline:
        """Perform cubic spline interpolation on given data points.

        Parameters
        ----------
        array_x : Union[list[float], np.ndarray]
            Independent variable data points
        array_y : Union[list[float], np.ndarray]
            Dependent variable data points
        bc_type : Literal["natural", "clamped", "not-a-knot", "periodic"]
            Boundary condition type

        Returns
        -------
        CubicSpline
            Cubic spline interpolation function
        """
        if len(array_x) != len(array_y):
            raise ValueError("Input arrays must have same length")
        if len(array_x) < 2:
            raise ValueError("At least 2 points required for interpolation")
        if bc_type == "periodic" and not np.allclose(array_y[0], array_y[-1]):
            raise ValueError(
                "First and last y values must match for periodic boundary conditions"
            )
        
        return CubicSpline(
            self.set_as_array(array_x),
            self.set_as_array(array_y),
            bc_type=bc_type,
        )

    def lagrange(
        self,
        array_x: Union[list[float], np.ndarray],
        array_y: Union[list[float], np.ndarray]
    ) -> np.poly1d:
        """Perform Lagrange polynomial interpolation on given data points.

        Parameters
        ----------
        array_x : Union[list[float], np.ndarray]
            Independent variable data points
        array_y : Union[list[float], np.ndarray]
            Dependent variable data points

        Returns
        -------
        np.poly1d
            Lagrange polynomial interpolation function
        """
        if len(array_x) != len(array_y):
            raise ValueError("Input arrays must have same length")
        if len(array_x) == 0:
            raise ValueError("Input arrays cannot be empty")
        
        return lagrange(self.set_as_array(array_x), self.set_as_array(array_y))

    def divided_diff_newton_polynomial_interpolation(
        self,
        array_x: Union[list[float], np.ndarray],
        array_y: Union[list[float], np.ndarray]
    ) -> np.ndarray:
        """Calculate divided difference table for Newton polynomial interpolation.

        Parameters
        ----------
        array_x : Union[list[float], np.ndarray]
            Independent variable data points
        array_y : Union[list[float], np.ndarray]
            Dependent variable data points

        Returns
        -------
        np.ndarray
            Divided difference coefficients matrix

        References
        ----------
        .. [1] Python Programming and Numerical Methods - A Guide for Engineers and Scientists;
        Qingkai Kong, Timmy Siauw and Alexandre M. Bayern
        https://pythonnumericalmethods.berkeley.edu/notebooks/chapter17.05-Newtons-Polynomial-Interpolation.html
        """
        if len(array_x) != len(array_y):
            raise ValueError("Input arrays must have same length")
        if len(array_x) == 0:
            raise ValueError("Input arrays cannot be empty")
        
        n = len(array_y)
        matrix_coef = np.zeros([n, n])
        matrix_coef[:, 0] = array_y

        for j in range(1, n):
            for i in range(n - j):
                numerator = matrix_coef[i + 1][j - 1] - matrix_coef[i][j - 1]
                denominator = array_x[i + j] - array_x[i]
                matrix_coef[i][j] = numerator / denominator

        return matrix_coef

    def newton_polynomial_interpolation(
        self,
        array_x: Union[list[float], np.ndarray],
        array_y: Union[list[float], np.ndarray],
        array_x_range: Union[list[float], np.ndarray]
    ) -> np.ndarray:
        """Evaluate Newton polynomial at new data points.

        Parameters
        ----------
        array_x : Union[list[float], np.ndarray]
            Original independent variable data points
        array_y : Union[list[float], np.ndarray]
            Original dependent variable data points
        array_x_range : Union[list[float], np.ndarray]
            New independent variable points to evaluate

        Returns
        -------
        np.ndarray
            Interpolated values at new points
        """
        array_x = self.set_as_array(array_x)
        array_y = self.set_as_array(array_y)
        array_x_range = self.set_as_array(array_x_range)
    
        if len(array_x) != len(array_y):
            raise ValueError("Input arrays must have same length")
        if len(array_x) == 0:
            raise ValueError("Input arrays cannot be empty")

        n = len(array_x) - 1
        matrix_coef = self.divided_diff_newton_polynomial_interpolation(array_x, array_y)
        array_y_range = np.full_like(array_x_range, matrix_coef[0, n])

        for k in range(1, n + 1):
            array_y_range = \
                matrix_coef[0, n - k] + (array_x_range - array_x[n - k]) * array_y_range

        return array_y_range