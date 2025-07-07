"""Comprehensive type-hinted tests for the Fraction class using pytest."""

from fractions import Fraction as StdFraction

import pytest

from stpstone.analytics.arithmetic.fraction import Fraction


# --------------------------
# Test Fraction Initialization
# --------------------------
class TestFractionInitialization:
    """Test cases for Fraction initialization."""
    
    def test_normal_initialization(self) -> None:
        """Test normal fraction initialization."""
        f = Fraction(3, 4)
        assert f.numerator == 3
        assert f.denominator == 4
    
    def test_automatic_reduction(self) -> None:
        """Test that fractions are automatically reduced."""
        f = Fraction(6, 8)
        assert f.numerator == 3
        assert f.denominator == 4
    
    def test_negative_numerator(self) -> None:
        """Test initialization with negative numerator."""
        f = Fraction(-3, 4)
        assert f.numerator == -3
        assert f.denominator == 4
    
    def test_negative_denominator(self) -> None:
        """Test initialization with negative denominator."""
        f = Fraction(3, -4)
        assert f.numerator == -3
        assert f.denominator == 4
    
    def test_both_negative(self) -> None:
        """Test initialization with both negative numerator and denominator."""
        f = Fraction(-3, -4)
        assert f.numerator == 3
        assert f.denominator == 4
    
    def test_zero_numerator(self) -> None:
        """Test initialization with zero numerator."""
        f = Fraction(0, 5)
        assert f.numerator == 0
        assert f.denominator == 1
    
    def test_zero_denominator_error(self) -> None:
        """Test that zero denominator raises ValueError."""
        with pytest.raises(ValueError) as excinfo:
            Fraction(1, 0)
        assert str(excinfo.value) == "Denominator cannot be zero."
    
    def test_whole_number(self) -> None:
        """Test initialization of whole numbers."""
        f = Fraction(5, 1)
        assert f.numerator == 5
        assert f.denominator == 1
    
    def test_improper_fraction(self) -> None:
        """Test initialization of improper fractions."""
        f = Fraction(7, 3)
        assert f.numerator == 7
        assert f.denominator == 3
    
    def test_large_numbers(self) -> None:
        """Test initialization with large numbers."""
        f = Fraction(1000000, 2000000)
        assert f.numerator == 1
        assert f.denominator == 2


# --------------------------
# Test Fraction Getters
# --------------------------
class TestFractionGetters:
    """Test cases for Fraction getter methods."""
    
    def test_get_num(self) -> None:
        """Test get_num method."""
        f = Fraction(3, 4)
        assert f.get_num() == 3
    
    def test_get_den(self) -> None:
        """Test get_den method."""
        f = Fraction(3, 4)
        assert f.get_den() == 4
    
    def test_getters_with_reduced_fraction(self) -> None:
        """Test getters return reduced values."""
        f = Fraction(6, 8)
        assert f.get_num() == 3
        assert f.get_den() == 4


# --------------------------
# Test Fraction Arithmetic
# --------------------------
class TestFractionArithmetic:
    """Test cases for Fraction arithmetic operations."""
    
    def test_addition_normal(self) -> None:
        """Test normal fraction addition."""
        f1 = Fraction(1, 2)
        f2 = Fraction(1, 3)
        result = f1 + f2
        assert result.numerator == 5
        assert result.denominator == 6
    
    def test_addition_same_denominator(self) -> None:
        """Test addition with same denominator."""
        f1 = Fraction(1, 4)
        f2 = Fraction(2, 4)
        result = f1 + f2
        assert result.numerator == 3
        assert result.denominator == 4
    
    def test_addition_with_zero(self) -> None:
        """Test addition with zero."""
        f1 = Fraction(1, 2)
        f2 = Fraction(0, 1)
        result = f1 + f2
        assert result.numerator == 1
        assert result.denominator == 2
    
    def test_addition_negative(self) -> None:
        """Test addition with negative fractions."""
        f1 = Fraction(1, 2)
        f2 = Fraction(-1, 3)
        result = f1 + f2
        assert result.numerator == 1
        assert result.denominator == 6
    
    def test_right_addition_with_integer(self) -> None:
        """Test right addition with integer."""
        f = Fraction(1, 2)
        result = 5 + f
        assert result.numerator == 11
        assert result.denominator == 2
    
    def test_in_place_addition(self) -> None:
        """Test in-place addition."""
        f1 = Fraction(1, 2)
        f2 = Fraction(1, 3)
        f1 += f2
        assert f1.numerator == 5
        assert f1.denominator == 6
    
    def test_subtraction_normal(self) -> None:
        """Test normal fraction subtraction."""
        f1 = Fraction(1, 2)
        f2 = Fraction(1, 3)
        result = f1 - f2
        assert result.numerator == 1
        assert result.denominator == 6
    
    def test_subtraction_negative_result(self) -> None:
        """Test subtraction resulting in negative."""
        f1 = Fraction(1, 3)
        f2 = Fraction(1, 2)
        result = f1 - f2
        assert result.numerator == -1
        assert result.denominator == 6
    
    def test_multiplication_normal(self) -> None:
        """Test normal fraction multiplication."""
        f1 = Fraction(1, 2)
        f2 = Fraction(2, 3)
        result = f1 * f2
        assert result.numerator == 1
        assert result.denominator == 3
    
    def test_multiplication_with_zero(self) -> None:
        """Test multiplication with zero."""
        f1 = Fraction(1, 2)
        f2 = Fraction(0, 1)
        result = f1 * f2
        assert result.numerator == 0
        assert result.denominator == 1
    
    def test_multiplication_negative(self) -> None:
        """Test multiplication with negative fractions."""
        f1 = Fraction(-1, 2)
        f2 = Fraction(2, 3)
        result = f1 * f2
        assert result.numerator == -1
        assert result.denominator == 3
    
    def test_division_normal(self) -> None:
        """Test normal fraction division."""
        f1 = Fraction(1, 2)
        f2 = Fraction(2, 3)
        result = f1 / f2
        assert result.numerator == 3
        assert result.denominator == 4
    
    def test_division_by_zero_error(self) -> None:
        """Test division by zero raises ValueError."""
        f1 = Fraction(1, 2)
        f2 = Fraction(0, 1)
        with pytest.raises(ValueError) as excinfo:
            f1 / f2
        assert str(excinfo.value) == "Cannot divide by zero."
    
    def test_division_negative(self) -> None:
        """Test division with negative fractions."""
        f1 = Fraction(1, 2)
        f2 = Fraction(-2, 3)
        result = f1 / f2
        assert result.numerator == -3
        assert result.denominator == 4


# --------------------------
# Test Fraction Comparisons
# --------------------------
class TestFractionComparisons:
    """Test cases for Fraction comparison operations."""
    
    def test_equality_same_values(self) -> None:
        """Test equality with same values."""
        f1 = Fraction(1, 2)
        f2 = Fraction(1, 2)
        assert f1 == f2
    
    def test_equality_equivalent_fractions(self) -> None:
        """Test equality with equivalent fractions."""
        f1 = Fraction(1, 2)
        f2 = Fraction(2, 4)
        assert f1 == f2
    
    def test_equality_different_values(self) -> None:
        """Test equality with different values."""
        f1 = Fraction(1, 2)
        f2 = Fraction(1, 3)
        assert f1 != f2
    
    def test_inequality(self) -> None:
        """Test inequality operator."""
        f1 = Fraction(1, 2)
        f2 = Fraction(1, 3)
        assert f1 != f2
    
    def test_greater_than(self) -> None:
        """Test greater than comparison."""
        f1 = Fraction(1, 2)
        f2 = Fraction(1, 3)
        assert f1 > f2
        assert not (f2 > f1)
    
    def test_greater_than_equal(self) -> None:
        """Test greater than or equal comparison."""
        f1 = Fraction(1, 2)
        f2 = Fraction(1, 3)
        f3 = Fraction(1, 2)
        assert f1 >= f2
        assert f1 >= f3
        assert not (f2 >= f1)
    
    def test_less_than(self) -> None:
        """Test less than comparison."""
        f1 = Fraction(1, 3)
        f2 = Fraction(1, 2)
        assert f1 < f2
        assert not (f2 < f1)
    
    def test_less_than_equal(self) -> None:
        """Test less than or equal comparison."""
        f1 = Fraction(1, 3)
        f2 = Fraction(1, 2)
        f3 = Fraction(1, 3)
        assert f1 <= f2
        assert f1 <= f3
        assert not (f2 <= f1)
    
    def test_comparison_with_negative(self) -> None:
        """Test comparisons with negative fractions."""
        f1 = Fraction(-1, 2)
        f2 = Fraction(1, 3)
        assert f1 < f2
        assert not (f1 > f2)
    
    def test_comparison_both_negative(self) -> None:
        """Test comparisons with both negative fractions."""
        f1 = Fraction(-1, 2)
        f2 = Fraction(-1, 3)
        assert f1 < f2  # -1/2 < -1/3
        assert not (f1 > f2)


# --------------------------
# Test Fraction String Representation
# --------------------------
class TestFractionStringRepresentation:
    """Test cases for Fraction string representations."""
    
    def test_str_representation(self) -> None:
        """Test string representation."""
        f = Fraction(1, 2)
        assert str(f) == "1/2"
    
    def test_repr_representation(self) -> None:
        """Test repr representation."""
        f = Fraction(1, 2)
        assert repr(f) == "Fraction(1, 2)"
    
    def test_str_negative_numerator(self) -> None:
        """Test string representation with negative numerator."""
        f = Fraction(-3, 4)
        assert str(f) == "-3/4"
    
    def test_str_whole_number(self) -> None:
        """Test string representation of whole number."""
        f = Fraction(5, 1)
        assert str(f) == "5/1"
    
    def test_str_zero(self) -> None:
        """Test string representation of zero."""
        f = Fraction(0, 1)
        assert str(f) == "0/1"


# --------------------------
# Test Fraction Edge Cases
# --------------------------
class TestFractionEdgeCases:
    """Test cases for edge cases and boundary conditions."""
    
    def test_very_large_numbers(self) -> None:
        """Test with very large numbers."""
        f1 = Fraction(10**18, 10**18 + 1)
        f2 = Fraction(1, 2)
        result = f1 + f2
        assert isinstance(result, Fraction)
    
    def test_gcd_edge_cases(self) -> None:
        """Test GCD calculation edge cases."""
        # test with numbers that have large GCD
        f = Fraction(12345678, 24691356)
        assert f.numerator == 1
        assert f.denominator == 2
    
    def test_arithmetic_overflow_protection(self) -> None:
        """Test arithmetic operations don't cause overflow issues."""
        f1 = Fraction(10**10, 1)
        f2 = Fraction(1, 10**10)
        result = f1 * f2
        assert result.numerator == 1
        assert result.denominator == 1
    
    def test_precision_maintenance(self) -> None:
        """Test that precision is maintained through operations."""
        # create a fraction that would lose precision as float
        f = Fraction(1, 3)
        result = f + f + f
        assert result.numerator == 1
        assert result.denominator == 1
    
    def test_chained_operations(self) -> None:
        """Test chained arithmetic operations."""
        f1 = Fraction(1, 2)
        f2 = Fraction(1, 3)
        f3 = Fraction(1, 4)
        result = f1 + f2 - f3
        expected = Fraction(7, 12)
        assert result == expected


# --------------------------
# Test Fraction Error Conditions
# --------------------------
class TestFractionErrorConditions:
    """Test cases for error conditions and exception handling."""
    
    @pytest.mark.parametrize("num", [1, -1, 0, 100, -100])
    def test_zero_denominator_various_numerators(self, num: int) -> None:
        """Test zero denominator with various numerators."""
        with pytest.raises(ValueError):
            Fraction(num, 0)
    
    @pytest.mark.parametrize("zero_f", [
        Fraction(0, 1), 
        Fraction(0, 5), 
        Fraction(0, 100)
    ])
    def test_division_by_zero_various_cases(self, zero_f: Fraction) -> None:
        """Test division by zero in various scenarios."""
        f = Fraction(1, 2)
        with pytest.raises(ValueError):
            f / zero_f
    
    def test_invalid_right_addition(self) -> None:
        """Test right addition with invalid types."""
        f = Fraction(1, 2)
        result = f.__radd__("invalid")
        assert result == NotImplemented
    
    def test_invalid_in_place_addition(self) -> None:
        """Test in-place addition with invalid types."""
        f = Fraction(1, 2)
        result = f.__iadd__("invalid")
        assert result == NotImplemented


# --------------------------
# Test Fraction type Validation
# --------------------------
class TestFractionTypeValidation:
    """Test cases for type validation through metaclass."""
    
    def test_type_validation_works(self) -> None:
        """Test that type validation prevents invalid types."""
        Fraction(1, 2)
        with pytest.raises(TypeError):
            Fraction("1", 2)
            
        with pytest.raises(TypeError):
            Fraction(1, "2")


# --------------------------
# Test Fraction Mathematical Properties
# --------------------------
class TestFractionMathematicalProperties:
    """Test mathematical properties and invariants."""
    
    def test_associativity_addition(self) -> None:
        """Test associativity of addition: (a + b) + c = a + (b + c)."""
        a = Fraction(1, 2)
        b = Fraction(1, 3)
        c = Fraction(1, 4)
        
        left = (a + b) + c
        right = a + (b + c)
        assert left == right
    
    def test_commutativity_addition(self) -> None:
        """Test commutativity of addition: a + b = b + a."""
        a = Fraction(1, 2)
        b = Fraction(1, 3)
        assert a + b == b + a
    
    def test_commutativity_multiplication(self) -> None:
        """Test commutativity of multiplication: a * b = b * a."""
        a = Fraction(1, 2)
        b = Fraction(2, 3)
        assert a * b == b * a
    
    def test_identity_element_addition(self) -> None:
        """Test additive identity: a + 0 = a."""
        a = Fraction(1, 2)
        zero = Fraction(0, 1)
        assert a + zero == a
    
    def test_identity_element_multiplication(self) -> None:
        """Test multiplicative identity: a * 1 = a."""
        a = Fraction(1, 2)
        one = Fraction(1, 1)
        assert a * one == a
    
    def test_inverse_element_addition(self) -> None:
        """Test additive inverse: a + (-a) = 0."""
        a = Fraction(1, 2)
        neg_a = Fraction(-1, 2)
        zero = Fraction(0, 1)
        assert a + neg_a == zero
    
    def test_inverse_element_multiplication(self) -> None:
        """Test multiplicative inverse: a * (1/a) = 1."""
        a = Fraction(2, 3)
        inv_a = Fraction(3, 2)
        one = Fraction(1, 1)
        assert a * inv_a == one
    
    def test_distributivity(self) -> None:
        """Test distributivity: a * (b + c) = a * b + a * c."""
        a = Fraction(1, 2)
        b = Fraction(1, 3)
        c = Fraction(1, 4)
        
        left = a * (b + c)
        right = a * b + a * c
        assert left == right


# --------------------------
# Test Fraction Compatibility
# --------------------------
class TestFractionCompatibility:
    """Test compatibility with Python's standard library fractions."""
    
    @pytest.mark.parametrize("num1,den1,num2,den2", [
        (1, 2, 1, 3),  # 1/2 + 1/3
        (3, 4, 2, 5),  # 3/4 * 2/5
        (5, 6, 1, 2),  # 5/6 - 1/2
        (2, 3, 3, 4),  # 2/3 / 3/4
    ])
    def test_comparison_with_standard_fraction(
        self,
        num1: int,
        den1: int,
        num2: int,
        den2: int
    ) -> None:
        """Compare results with Python's standard Fraction class."""
        # our implementation
        f1 = Fraction(num1, den1)
        f2 = Fraction(num2, den2)
        
        # standard library
        sf1 = StdFraction(num1, den1)
        sf2 = StdFraction(num2, den2)
        
        # compare addition
        our_add = f1 + f2
        std_add = sf1 + sf2
        assert our_add.numerator == std_add.numerator
        assert our_add.denominator == std_add.denominator
        
        # compare multiplication
        our_mul = f1 * f2
        std_mul = sf1 * sf2
        assert our_mul.numerator == std_mul.numerator
        assert our_mul.denominator == std_mul.denominator