"""Unit tests for interpolation module.

Tests cover all interpolation methods with normal operations, edge cases,
error conditions, and type validation.
"""

import numpy as np
import pytest
from scipy.interpolate import CubicSpline, interp1d

from stpstone.analytics.quant.interpolation import Interpolation


@pytest.fixture
def interpolation() -> Interpolation:
	"""Fixture providing initialized Interpolation instance.

	Returns
	-------
	Interpolation
		Initialized Interpolation instance
	"""
	return Interpolation()


@pytest.fixture
def sample_data() -> tuple[list[float], list[float]]:
	"""Fixture providing sample x and y data points.

	Returns
	-------
	tuple[list[float], list[float]]
		Sample x and y data points
	"""
	return [1.0, 2.0, 3.0, 4.0], [1.0, 4.0, 9.0, 16.0]


@pytest.fixture
def empty_data() -> tuple[list[float], list[float]]:
	"""Fixture providing empty data points.

	Returns
	-------
	tuple[list[float], list[float]]
		Empty x and y data points
	"""
	return [], []


@pytest.fixture
def single_point_data() -> tuple[list[float], list[float]]:
	"""Fixture providing single data point.

	Returns
	-------
	tuple[list[float], list[float]]
		Single x and y data point
	"""
	return [1.0], [1.0]


@pytest.fixture
def duplicate_x_data() -> tuple[list[float], list[float]]:
	"""Fixture providing data with duplicate x values.

	Returns
	-------
	tuple[list[float], list[float]]
		Data with duplicate x values
	"""
	return [1.0, 2.0, 2.0, 3.0], [1.0, 4.0, 5.0, 9.0]


class TestSetAsArray:
	"""Tests for set_as_array method."""

	def test_valid_list_conversion(self, interpolation: Interpolation) -> None:
		"""Test conversion of valid list to numpy array.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance

		Returns
		-------
		None
		"""
		result = interpolation.set_as_array([1.0, 2.0, 3.0])
		assert isinstance(result, np.ndarray)
		assert np.array_equal(result, np.array([1.0, 2.0, 3.0]))

	def test_empty_list_conversion(self, interpolation: Interpolation) -> None:
		"""Test conversion of empty list to numpy array.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Input array cannot be empty"):
			_ = interpolation.set_as_array([])

	def test_none_input(self, interpolation: Interpolation) -> None:
		"""Test behavior with None input.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			interpolation.set_as_array(None)

	def test_invalid_type_input(self, interpolation: Interpolation) -> None:
		"""Test behavior with invalid type input.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			interpolation.set_as_array("invalid")  # type: ignore


class TestLinearInterpolation:
	"""Tests for linear_interpolation method."""

	def test_valid_input(self, interpolation: Interpolation, sample_data: tuple) -> None:
		"""Test linear interpolation with valid input.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance
		sample_data : tuple
			Sample x and y data points

		Returns
		-------
		None
		"""
		x, y = sample_data
		result = interpolation.linear_interpolation(x, y)
		assert isinstance(result, interp1d)
		assert np.allclose(result([1.5, 2.5]), [2.5, 6.5])

	def test_empty_input(self, interpolation: Interpolation, empty_data: tuple) -> None:
		"""Test linear interpolation with empty input.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance
		empty_data : tuple
			Empty x and y data points

		Returns
		-------
		None
		"""
		x, y = empty_data
		with pytest.raises(ValueError):
			interpolation.linear_interpolation(x, y)

	def test_single_point(self, interpolation: Interpolation, single_point_data: tuple) -> None:
		"""Test linear interpolation with single point.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance
		single_point_data : tuple
			Single x and y data point

		Returns
		-------
		None
		"""
		x, y = single_point_data
		with pytest.raises(ValueError):
			interpolation.linear_interpolation(x, y)

	def test_mismatched_lengths(self, interpolation: Interpolation) -> None:
		"""Test linear interpolation with mismatched lengths.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be one of types"):
			interpolation.linear_interpolation([1, 2, 3], [1, 2])

	def test_duplicate_x_values(
		self, interpolation: Interpolation, duplicate_x_data: tuple
	) -> None:
		"""Test linear interpolation with duplicate x values.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance
		duplicate_x_data : tuple
			Data with duplicate x values

		Returns
		-------
		None
		"""
		x, y = duplicate_x_data
		with pytest.raises(ValueError):
			interpolation.linear_interpolation(x, y)


class TestCubicSpline:
	"""Tests for cubic_spline method."""

	@pytest.mark.parametrize("bc_type", ["natural", "clamped", "not-a-knot", "periodic"])
	def test_valid_input_with_bc_types(
		self,
		interpolation: Interpolation,
		sample_data: tuple,
		bc_type: str,
	) -> None:
		"""Test cubic spline with valid input and all boundary condition types.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance
		sample_data : tuple
			Sample x and y data points
		bc_type : str
			Boundary condition type

		Returns
		-------
		None
		"""
		x, y = sample_data
		if bc_type == "periodic":
			y = [1.0, 4.0, 9.0, 1.0]
		result = interpolation.cubic_spline(x, y, bc_type=bc_type)
		assert isinstance(result, CubicSpline)
		assert result.c.shape == (4, 3)

	def test_empty_input(self, interpolation: Interpolation, empty_data: tuple) -> None:
		"""Test cubic spline with empty input.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance
		empty_data : tuple
			Empty x and y data points

		Returns
		-------
		None
		"""
		x, y = empty_data
		with pytest.raises(ValueError):
			interpolation.cubic_spline(x, y)

	def test_single_point(self, interpolation: Interpolation, single_point_data: tuple) -> None:
		"""Test cubic spline with single point.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance
		single_point_data : tuple
			Single x and y data point

		Returns
		-------
		None
		"""
		x, y = single_point_data
		with pytest.raises(ValueError):
			interpolation.cubic_spline(x, y)

	def test_invalid_bc_type(self, interpolation: Interpolation, sample_data: tuple) -> None:
		"""Test cubic spline with invalid boundary condition type.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance
		sample_data : tuple
			Sample x and y data points

		Returns
		-------
		None
		"""
		x, y = sample_data
		with pytest.raises(TypeError, match="must be one of"):
			interpolation.cubic_spline(x, y, bc_type="invalid")  # type: ignore

	def test_periodic_boundary_condition(self, interpolation: Interpolation) -> None:
		"""Test cubic spline with valid periodic data.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance

		Returns
		-------
		None
		"""
		x = [0.0, 1.0, 2.0, 3.0]
		y = [1.0, 0.0, -1.0, 1.0]  # y[0] == y[-1] for periodic condition
		result = interpolation.cubic_spline(x, y, bc_type="periodic")
		assert isinstance(result, CubicSpline)
		assert np.allclose(result(x), y)


class TestLagrange:
	"""Tests for lagrange method."""

	def test_valid_input(self, interpolation: Interpolation, sample_data: tuple) -> None:
		"""Test Lagrange interpolation with valid input.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance
		sample_data : tuple
			Sample x and y data points

		Returns
		-------
		None
		"""
		x, y = sample_data
		result = interpolation.lagrange(x, y)
		assert isinstance(result, np.poly1d)
		# verify exact polynomial matches at sample points
		assert np.allclose(result(x), y)

	def test_empty_input(self, interpolation: Interpolation, empty_data: tuple) -> None:
		"""Test Lagrange interpolation with empty input.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance
		empty_data : tuple
			Empty x and y data points

		Returns
		-------
		None
		"""
		x, y = empty_data
		with pytest.raises(ValueError):
			interpolation.lagrange(x, y)

	def test_single_point(self, interpolation: Interpolation, single_point_data: tuple) -> None:
		"""Test Lagrange interpolation with single point.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance
		single_point_data : tuple
			Single x and y data point

		Returns
		-------
		None
		"""
		x, y = single_point_data
		result = interpolation.lagrange(x, y)
		assert isinstance(result, np.poly1d)
		assert result(1.0) == 1.0


class TestDividedDiffNewtonPolynomial:
	"""Tests for divided_diff_newton_polynomial_interpolation method."""

	def test_valid_input(self, interpolation: Interpolation, sample_data: tuple) -> None:
		"""Test divided difference calculation with valid input.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance
		sample_data : tuple
			Sample x and y data points

		Returns
		-------
		None
		"""
		x, y = sample_data
		result = interpolation.divided_diff_newton_polynomial_interpolation(x, y)
		assert isinstance(result, np.ndarray)
		assert result.shape == (4, 4)
		# verify first column is y values
		assert np.array_equal(result[:, 0], y)

	def test_empty_input(self, interpolation: Interpolation, empty_data: tuple) -> None:
		"""Test divided difference calculation with empty input.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance
		empty_data : tuple
			Empty x and y data points

		Returns
		-------
		None
		"""
		x, y = empty_data
		with pytest.raises(ValueError):
			interpolation.divided_diff_newton_polynomial_interpolation(x, y)

	def test_single_point(self, interpolation: Interpolation, single_point_data: tuple) -> None:
		"""Test divided difference calculation with single point.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance
		single_point_data : tuple
			Single x and y data point

		Returns
		-------
		None
		"""
		x, y = single_point_data
		result = interpolation.divided_diff_newton_polynomial_interpolation(x, y)
		assert isinstance(result, np.ndarray)
		assert result.shape == (1, 1)
		assert result[0, 0] == y[0]


class TestNewtonPolynomialInterpolation:
	"""Tests for newton_polynomial_interpolation method."""

	def test_valid_input(self, interpolation: Interpolation, sample_data: tuple) -> None:
		"""Test Newton polynomial interpolation with valid input.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance
		sample_data : tuple
			Sample x and y data points

		Returns
		-------
		None
		"""
		x, y = sample_data
		x_range = [1.5, 2.5, 3.5]
		result = interpolation.newton_polynomial_interpolation(x, y, x_range)
		assert isinstance(result, np.ndarray)
		assert result.shape == (3,)
		# verify exact match at sample points
		assert np.allclose(interpolation.newton_polynomial_interpolation(x, y, x), y)

	def test_empty_input(self, interpolation: Interpolation, empty_data: tuple) -> None:
		"""Test Newton polynomial interpolation with empty input.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance
		empty_data : tuple
			Empty x and y data points

		Returns
		-------
		None
		"""
		x, y = empty_data
		with pytest.raises(ValueError):
			interpolation.newton_polynomial_interpolation(x, y, [])

	def test_single_point(self, interpolation: Interpolation, single_point_data: tuple) -> None:
		"""Test Newton polynomial interpolation with single point.

		Parameters
		----------
		interpolation : Interpolation
			Initialized Interpolation instance
		single_point_data : tuple
			Single x and y data point

		Returns
		-------
		None
		"""
		x, y = single_point_data
		result = interpolation.newton_polynomial_interpolation(x, y, [1.0])
		assert isinstance(result, np.ndarray)
		assert result[0] == y[0]
