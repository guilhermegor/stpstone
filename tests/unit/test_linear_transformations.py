"""Unit tests for LinearAlgebra class."""

import numpy as np
import pytest

from stpstone.analytics.quant.linear_transformations import LinearAlgebra


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def linear_algebra() -> LinearAlgebra:
    """Fixture providing a LinearAlgebra instance."""
    return LinearAlgebra()

@pytest.fixture
def valid_square_matrix() -> np.ndarray:
    """Fixture providing a valid square matrix."""
    return np.array([[4, 12, -16], [12, 37, -43], [-16, -43, 98]])

@pytest.fixture
def valid_vector() -> np.ndarray:
    """Fixture providing a valid vector."""
    return np.array([1, 2, 3])

@pytest.fixture
def valid_matrix_2x3() -> np.ndarray:
    """Fixture providing a valid 2x3 matrix."""
    return np.array([[1, 2, 3], [4, 5, 6]])

@pytest.fixture
def valid_matrix_3x2() -> np.ndarray:
    """Fixture providing a valid 3x2 matrix."""
    return np.array([[1, 2], [3, 4], [5, 6]])

@pytest.fixture
def singular_matrix() -> np.ndarray:
    """Fixture providing a singular matrix."""
    return np.array([[1, 2], [1, 2]])

# --------------------------
# Test Add Method
# --------------------------
def test_add_normal_vectors(linear_algebra: LinearAlgebra) -> None:
    """Test vector addition with valid inputs."""
    # Test with numpy arrays
    result = linear_algebra.add(np.array([1, 2, 3]), np.array([4, 5, 6]))
    assert isinstance(result, np.ndarray)
    assert np.array_equal(result, np.array([5, 7, 9]))
    
    # Test with lists
    result = linear_algebra.add([1, 2], [3, 4])
    assert isinstance(result, np.ndarray)
    assert np.array_equal(result, np.array([4, 6]))

def test_add_normal_matrices(linear_algebra: LinearAlgebra) -> None:
    """Test matrix addition with valid inputs."""
    mat1 = np.array([[1, 2], [3, 4]])
    mat2 = np.array([[5, 6], [7, 8]])
    result = linear_algebra.add(mat1, mat2)
    expected = np.array([[6, 8], [10, 12]])
    assert np.array_equal(result, expected)

def test_add_broadcasting(linear_algebra: LinearAlgebra) -> None:
    """Test broadcasting in addition operation."""
    # Vector + scalar broadcasting
    result = linear_algebra.add([1, 2, 3], 5)
    assert np.array_equal(result, np.array([6, 7, 8]))
    
    # Matrix + vector broadcasting
    mat = np.array([[1, 2], [3, 4]])
    vec = np.array([10, 20])
    result = linear_algebra.add(mat, vec)
    expected = np.array([[11, 22], [13, 24]])
    assert np.array_equal(result, expected)

def test_add_incompatible_shapes(linear_algebra: LinearAlgebra) -> None:
    """Test addition with incompatible shapes."""
    with pytest.raises(ValueError):
        linear_algebra.add([1, 2, 3], [4, 5])  # Different lengths
        
    with pytest.raises(ValueError):
        linear_algebra.add([[1, 2], [3, 4]], [5, 6, 7])  # Incompatible shapes

def test_add_invalid_types(linear_algebra: LinearAlgebra) -> None:
    """Test addition with invalid input types."""
    with pytest.raises(TypeError):
        linear_algebra.add([1, 2], ["a", "b"])  # type: ignore
        
    with pytest.raises(TypeError):
        linear_algebra.add("not_an_array", [1, 2])  # type: ignore

def test_add_empty_arrays(linear_algebra: LinearAlgebra) -> None:
    """Test addition with empty arrays."""
    result = linear_algebra.add([], [])
    assert isinstance(result, np.ndarray)
    assert result.size == 0

# --------------------------
# Test Distance Method
# --------------------------
def test_distance_normal_vectors(linear_algebra: LinearAlgebra) -> None:
    """Test distance calculation with valid vectors."""
    # Simple 2D case
    result = linear_algebra.distance([1, 0], [0, 1])
    assert isinstance(result, float)
    assert pytest.approx(result) == np.sqrt(2)
    
    # 3D case
    result = linear_algebra.distance([1, 2, 3], [4, 5, 6])
    assert pytest.approx(result) == np.sqrt(27)

def test_distance_zero_distance(linear_algebra: LinearAlgebra) -> None:
    """Test distance between identical vectors."""
    vec = [1, 2, 3]
    result = linear_algebra.distance(vec, vec)
    assert result == 0.0

def test_distance_different_lengths(linear_algebra: LinearAlgebra) -> None:
    """Test distance with vectors of different lengths."""
    with pytest.raises(ValueError):
        linear_algebra.distance([1, 2, 3], [4, 5])

def test_distance_invalid_types(linear_algebra: LinearAlgebra) -> None:
    """Test distance with invalid input types."""
    with pytest.raises(TypeError):
        linear_algebra.distance([1, 2], ["a", "b"])  # type: ignore
        
    with pytest.raises(TypeError):
        linear_algebra.distance("not_an_array", [1, 2])  # type: ignore

def test_distance_empty_vectors(linear_algebra: LinearAlgebra) -> None:
    """Test distance with empty vectors."""
    result = linear_algebra.distance([], [])
    assert result == 0.0

# --------------------------
# Test Scalar Multiply Method
# --------------------------
def test_scalar_multiply_normal(linear_algebra: LinearAlgebra) -> None:
    """Test scalar multiplication with valid inputs."""
    # Integer scalar
    result = linear_algebra.scalar_multiply(2, [1, 2, 3])
    assert np.array_equal(result, np.array([2, 4, 6]))
    
    # Float scalar
    result = linear_algebra.scalar_multiply(0.5, [10, 20, 30])
    assert np.array_equal(result, np.array([5, 10, 15]))
    
    # Matrix multiplication
    mat = np.array([[1, 2], [3, 4]])
    result = linear_algebra.scalar_multiply(3, mat)
    expected = np.array([[3, 6], [9, 12]])
    assert np.array_equal(result, expected)

def test_scalar_multiply_zero(linear_algebra: LinearAlgebra) -> None:
    """Test multiplication by zero."""
    result = linear_algebra.scalar_multiply(0, [1, 2, 3])
    assert np.array_equal(result, np.array([0, 0, 0]))

def test_scalar_multiply_one(linear_algebra: LinearAlgebra) -> None:
    """Test multiplication by one (identity operation)."""
    vec = [1, 2, 3]
    result = linear_algebra.scalar_multiply(1, vec)
    assert np.array_equal(result, np.array(vec))

def test_scalar_multiply_invalid_scalar(linear_algebra: LinearAlgebra) -> None:
    """Test with invalid scalar types."""
    with pytest.raises(TypeError):
        linear_algebra.scalar_multiply("2", [1, 2, 3])  # type: ignore
        
    with pytest.raises(TypeError):
        linear_algebra.scalar_multiply([1, 2], [1, 2, 3])  # type: ignore

def test_scalar_multiply_invalid_array(linear_algebra: LinearAlgebra) -> None:
    """Test with invalid array types."""
    with pytest.raises(TypeError):
        linear_algebra.scalar_multiply(2, "not_an_array")  # type: ignore
        
    with pytest.raises(TypeError):
        linear_algebra.scalar_multiply(2, [1, "a"])  # type: ignore

def test_scalar_multiply_empty_array(linear_algebra: LinearAlgebra) -> None:
    """Test with empty array."""
    result = linear_algebra.scalar_multiply(5, [])
    assert isinstance(result, np.ndarray)
    assert result.size == 0

# --------------------------
# Test Mixed Type Operations
# --------------------------
def test_mixed_input_types(linear_algebra: LinearAlgebra) -> None:
    """Test operations with mixed input types."""
    # Add with mixed list and array
    result = linear_algebra.add([1, 2], np.array([3, 4]))
    assert np.array_equal(result, np.array([4, 6]))
    
    # Distance with mixed types
    result = linear_algebra.distance(np.array([1, 2]), [4, 5])
    assert pytest.approx(result) == np.sqrt(18)
    
    # Scalar multiply with different array types
    result = linear_algebra.scalar_multiply(2, (1, 2, 3))  # type: ignore
    assert np.array_equal(result, np.array([2, 4, 6]))

# --------------------------
# Test Numerical Precision
# --------------------------
def test_numerical_precision(linear_algebra: LinearAlgebra) -> None:
    """Test numerical precision of operations."""
    # Small numbers
    small_vec = [1e-10, 2e-10]
    result = linear_algebra.add(small_vec, small_vec)
    assert np.allclose(result, [2e-10, 4e-10])
    
    # Large numbers
    large_vec = [1e10, 2e10]
    result = linear_algebra.scalar_multiply(1e-10, large_vec)
    assert np.allclose(result, [1, 2])
    
    # Floating point distance
    vec1 = [1.0000001, 2.0000001]
    vec2 = [1.0000002, 2.0000002]
    result = linear_algebra.distance(vec1, vec2)
    expected = np.sqrt(2e-14)
    assert abs(result - expected) < 1e-15

# --------------------------
# Cholesky Decomposition
# --------------------------
def test_cholesky_decomposition_normal(
    linear_algebra: LinearAlgebra, valid_square_matrix: np.ndarray
) -> None:
    """Test Cholesky decomposition with valid input."""
    result = linear_algebra.cholesky_decomposition(valid_square_matrix)
    assert isinstance(result, np.ndarray)
    # verify L * L.T reconstructs the original matrix
    reconstructed = result @ result.T
    assert np.allclose(reconstructed, valid_square_matrix)

def test_cholesky_decomposition_non_positive_definite(
    linear_algebra: LinearAlgebra
) -> None:
    """Test Cholesky decomposition with non-positive definite matrix."""
    non_pd_matrix = np.array([[1, 2], [2, 1]])
    with pytest.raises(np.linalg.LinAlgError):
        linear_algebra.cholesky_decomposition(non_pd_matrix)

def test_cholesky_decomposition_invalid_type(
    linear_algebra: LinearAlgebra
) -> None:
    """Test Cholesky decomposition with invalid input type."""
    with pytest.raises(TypeError):
        linear_algebra.cholesky_decomposition([[1, 2], [3, 4]])  # type: ignore

# --------------------------
# Eigenvalue/Eigenvector
# --------------------------
def test_eigenvalue_eigenvector_normal(
    linear_algebra: LinearAlgebra, valid_square_matrix: np.ndarray
) -> None:
    """Test eigenvalue/eigenvector decomposition with valid input."""
    eigenvalues, eigenvectors = linear_algebra.eigenvalue_eigenvector(valid_square_matrix)
    assert isinstance(eigenvalues, np.ndarray)
    assert isinstance(eigenvectors, np.ndarray)
    # verify A*v = λ*v for each eigenvalue/eigenvector pair
    for i in range(len(eigenvalues)):
        assert np.allclose(
            valid_square_matrix @ eigenvectors[:, i],
            eigenvalues[i] * eigenvectors[:, i]
        )

def test_eigenvalue_eigenvector_non_square(
    linear_algebra: LinearAlgebra, valid_matrix_2x3: np.ndarray
) -> None:
    """Test eigenvalue/eigenvector with non-square matrix."""
    with pytest.raises(np.linalg.LinAlgError):
        linear_algebra.eigenvalue_eigenvector(valid_matrix_2x3)

# --------------------------
# Matrix Multiplication
# --------------------------
def test_matrix_multiplication_normal(
    linear_algebra: LinearAlgebra, valid_matrix_2x3: np.ndarray, valid_matrix_3x2: np.ndarray
) -> None:
    """Test matrix multiplication with valid inputs."""
    result = linear_algebra.matrix_multiplication(valid_matrix_2x3, valid_matrix_3x2)
    assert isinstance(result, np.ndarray)
    assert result.shape == (2, 2)

def test_matrix_multiplication_incompatible_shapes(
    linear_algebra: LinearAlgebra, valid_matrix_2x3: np.ndarray
) -> None:
    """Test matrix multiplication with incompatible shapes."""
    with pytest.raises(ValueError):
        linear_algebra.matrix_multiplication(valid_matrix_2x3, valid_matrix_2x3)

# --------------------------
# Power Matrix
# --------------------------
def test_power_matrix_normal(
    linear_algebra: LinearAlgebra, valid_square_matrix: np.ndarray
) -> None:
    """Test matrix power with valid input."""
    result = linear_algebra.power_matrix(valid_square_matrix, 3)
    assert isinstance(result, np.ndarray)
    # verify result matches manual multiplication
    expected = valid_square_matrix @ valid_square_matrix @ valid_square_matrix
    assert np.allclose(result, expected)

def test_power_matrix_negative_exponent(
    linear_algebra: LinearAlgebra, valid_square_matrix: np.ndarray
) -> None:
    """Test matrix power with negative exponent."""
    result = linear_algebra.power_matrix(valid_square_matrix, -1)
    assert isinstance(result, np.ndarray)
    # verify result matches inverse
    expected = np.linalg.inv(valid_square_matrix)
    assert np.allclose(result, expected)

# --------------------------
# Square Root Matrix
# --------------------------
def test_sqrt_matrix_normal(
    linear_algebra: LinearAlgebra, 
    valid_square_matrix: np.ndarray
) -> None:
    """Test matrix square root with valid input."""
    result = linear_algebra.sqrt_matrix(valid_square_matrix)
    assert isinstance(result, np.ndarray)
    assert np.allclose(result @ result, valid_square_matrix, atol=1e-6)

# --------------------------
# Shape Array
# --------------------------
def test_shape_array_normal(
    linear_algebra: LinearAlgebra, valid_matrix_2x3: np.ndarray
) -> None:
    """Test array shape with valid input."""
    result = linear_algebra.shape_array(valid_matrix_2x3)
    assert result == (2, 3)

# --------------------------
# Covariance
# --------------------------
def test_covariance_arrays_normal(
    linear_algebra: LinearAlgebra, valid_matrix_2x3: np.ndarray
) -> None:
    """Test covariance calculation with valid input."""
    result = linear_algebra.covariance_arrays(valid_matrix_2x3)
    assert isinstance(result, np.ndarray)
    assert result.shape == (2, 2)

# --------------------------
# Euclidean Distance
# --------------------------
def test_euclidean_distance_normal(
    linear_algebra: LinearAlgebra, valid_vector: np.ndarray
) -> None:
    """Test Euclidean distance with valid inputs."""
    vector2 = valid_vector * 2
    result = linear_algebra.euclidean_distance(valid_vector, vector2)
    assert isinstance(result, float)
    assert pytest.approx(result) == np.sqrt(14)  # sqrt(1^2 + 2^2 + 3^2)

# --------------------------
# Angle Between Arrays
# --------------------------
def test_angle_between_two_arrays_normal(
    linear_algebra: LinearAlgebra, valid_vector: np.ndarray
) -> None:
    """Test angle between vectors with valid inputs."""
    vector2 = np.array([4, 5, 6])
    result = linear_algebra.angle_between_two_arrays(valid_vector, vector2)
    assert isinstance(result, float)
    # verify angle calculation
    dot_product = np.dot(valid_vector, vector2)
    norm_product = np.linalg.norm(valid_vector) * np.linalg.norm(vector2)
    expected = np.arccos(dot_product / norm_product)
    assert pytest.approx(result) == expected

# --------------------------
# Determinant
# --------------------------
def test_determinant_normal(
    linear_algebra: LinearAlgebra, valid_square_matrix: np.ndarray
) -> None:
    """Test determinant calculation with valid input."""
    result = linear_algebra.determinant(valid_square_matrix)
    assert isinstance(result, float)
    assert result != 0  # Our test matrix is non-singular

def test_determinant_singular(
    linear_algebra: LinearAlgebra, singular_matrix: np.ndarray
) -> None:
    """Test determinant calculation with singular matrix."""
    result = linear_algebra.determinant(singular_matrix)
    assert result == 0

# --------------------------
# Inverse
# --------------------------
def test_inverse_normal(
    linear_algebra: LinearAlgebra, valid_square_matrix: np.ndarray
) -> None:
    """Test matrix inverse with valid input."""
    result = linear_algebra.inverse(valid_square_matrix)
    assert isinstance(result, np.ndarray)
    # verify A * A^-1 = I
    identity = np.eye(valid_square_matrix.shape[0])
    assert np.allclose(valid_square_matrix @ result, identity)

def test_inverse_singular(
    linear_algebra: LinearAlgebra, singular_matrix: np.ndarray
) -> None:
    """Test matrix inverse with singular matrix."""
    with pytest.raises(np.linalg.LinAlgError):
        linear_algebra.inverse(singular_matrix)

# --------------------------
# Condition Number
# --------------------------
def test_condition_number_normal(
    linear_algebra: LinearAlgebra, valid_square_matrix: np.ndarray
) -> None:
    """Test condition number calculation with valid input."""
    result = linear_algebra.condition_number(valid_square_matrix)
    assert isinstance(result, float)
    assert result > 0

# --------------------------
# Matrix Rank
# --------------------------
def test_matrix_rank_normal(
    linear_algebra: LinearAlgebra, valid_square_matrix: np.ndarray
) -> None:
    """Test matrix rank with valid input."""
    result = linear_algebra.matrix_rank(valid_square_matrix)
    assert isinstance(result, int)
    assert result == 3

def test_matrix_rank_singular(
    linear_algebra: LinearAlgebra, singular_matrix: np.ndarray
) -> None:
    """Test matrix rank with singular matrix."""
    result = linear_algebra.matrix_rank(singular_matrix)
    assert result == 1

# --------------------------
# Linear Equations Solution
# --------------------------
def test_linear_equations_solution_normal(
    linear_algebra: LinearAlgebra, valid_square_matrix: np.ndarray, valid_vector: np.ndarray
) -> None:
    """Test linear equations solution with valid inputs."""
    b = valid_square_matrix @ valid_vector  # create solvable system
    result = linear_algebra.linear_equations_solution(valid_square_matrix, b)
    assert isinstance(result, np.ndarray)
    assert np.allclose(result, valid_vector)

def test_linear_equations_solution_singular(
    linear_algebra: LinearAlgebra, singular_matrix: np.ndarray
) -> None:
    """Test linear equations solution with singular matrix."""
    b = np.array([1, 1])
    with pytest.raises(np.linalg.LinAlgError):
        linear_algebra.linear_equations_solution(singular_matrix, b)

# --------------------------
# Edge Cases
# --------------------------
def test_empty_array(linear_algebra: LinearAlgebra) -> None:
    """Test with empty array."""
    empty_array = np.array([])
    with pytest.raises(ValueError):
        linear_algebra.shape_array(empty_array)

def test_1d_array(linear_algebra: LinearAlgebra) -> None:
    """Test with 1D array."""
    arr = np.array([1, 2, 3])
    assert linear_algebra.shape_array(arr) == (3,)

# --------------------------
# Type Validation
# --------------------------
def test_invalid_input_types(linear_algebra: LinearAlgebra) -> None:
    """Test with invalid input types."""
    with pytest.raises(TypeError):
        linear_algebra.transpose_matrix([[1, 2], [3, 4]])  # type: ignore

    with pytest.raises(TypeError):
        linear_algebra.power_matrix(np.array([[1, 2], [3, 4]]), "2")  # type: ignore

def test_none_input(linear_algebra: LinearAlgebra) -> None:
    """Test with None input."""
    with pytest.raises(TypeError):
        linear_algebra.determinant(None)  # type: ignore