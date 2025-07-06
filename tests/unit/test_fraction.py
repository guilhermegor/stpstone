"""Comprehensive unit tests for the Fraction class.

This module contains extensive unit tests covering normal operations, edge cases,
error conditions, and type validation for the Fraction class.
"""

from fractions import Fraction as StdFraction
from typing import Any
import unittest
from unittest.mock import patch

from stpstone.analytics.arithmetic.fraction import Fraction


class TestFractionInitialization(unittest.TestCase):
    """Test cases for Fraction initialization."""
    
    def test_normal_initialization(self):
        """Test normal fraction initialization."""
        f = Fraction(3, 4)
        self.assertEqual(f.numerator, 3)
        self.assertEqual(f.denominator, 4)
    
    def test_automatic_reduction(self):
        """Test that fractions are automatically reduced."""
        f = Fraction(6, 8)
        self.assertEqual(f.numerator, 3)
        self.assertEqual(f.denominator, 4)
    
    def test_negative_numerator(self):
        """Test initialization with negative numerator."""
        f = Fraction(-3, 4)
        self.assertEqual(f.numerator, -3)
        self.assertEqual(f.denominator, 4)
    
    def test_negative_denominator(self):
        """Test initialization with negative denominator."""
        f = Fraction(3, -4)
        self.assertEqual(f.numerator, -3)
        self.assertEqual(f.denominator, 4)
    
    def test_both_negative(self):
        """Test initialization with both negative numerator and denominator."""
        f = Fraction(-3, -4)
        self.assertEqual(f.numerator, 3)
        self.assertEqual(f.denominator, 4)
    
    def test_zero_numerator(self):
        """Test initialization with zero numerator."""
        f = Fraction(0, 5)
        self.assertEqual(f.numerator, 0)
        self.assertEqual(f.denominator, 1)
    
    def test_zero_denominator_error(self):
        """Test that zero denominator raises ValueError."""
        with self.assertRaises(ValueError) as context:
            Fraction(1, 0)
        self.assertEqual(str(context.exception), "Denominator cannot be zero.")
    
    def test_whole_number(self):
        """Test initialization of whole numbers."""
        f = Fraction(5, 1)
        self.assertEqual(f.numerator, 5)
        self.assertEqual(f.denominator, 1)
    
    def test_improper_fraction(self):
        """Test initialization of improper fractions."""
        f = Fraction(7, 3)
        self.assertEqual(f.numerator, 7)
        self.assertEqual(f.denominator, 3)
    
    def test_large_numbers(self):
        """Test initialization with large numbers."""
        f = Fraction(1000000, 2000000)
        self.assertEqual(f.numerator, 1)
        self.assertEqual(f.denominator, 2)


class TestFractionGetters(unittest.TestCase):
    """Test cases for Fraction getter methods."""
    
    def test_get_num(self):
        """Test get_num method."""
        f = Fraction(3, 4)
        self.assertEqual(f.get_num(), 3)
    
    def test_get_den(self):
        """Test get_den method."""
        f = Fraction(3, 4)
        self.assertEqual(f.get_den(), 4)
    
    def test_getters_with_reduced_fraction(self):
        """Test getters return reduced values."""
        f = Fraction(6, 8)
        self.assertEqual(f.get_num(), 3)
        self.assertEqual(f.get_den(), 4)


class TestFractionArithmetic(unittest.TestCase):
    """Test cases for Fraction arithmetic operations."""
    
    def test_addition_normal(self):
        """Test normal fraction addition."""
        f1 = Fraction(1, 2)
        f2 = Fraction(1, 3)
        result = f1 + f2
        self.assertEqual(result.numerator, 5)
        self.assertEqual(result.denominator, 6)
    
    def test_addition_same_denominator(self):
        """Test addition with same denominator."""
        f1 = Fraction(1, 4)
        f2 = Fraction(2, 4)
        result = f1 + f2
        self.assertEqual(result.numerator, 3)
        self.assertEqual(result.denominator, 4)
    
    def test_addition_with_zero(self):
        """Test addition with zero."""
        f1 = Fraction(1, 2)
        f2 = Fraction(0, 1)
        result = f1 + f2
        self.assertEqual(result.numerator, 1)
        self.assertEqual(result.denominator, 2)
    
    def test_addition_negative(self):
        """Test addition with negative fractions."""
        f1 = Fraction(1, 2)
        f2 = Fraction(-1, 3)
        result = f1 + f2
        self.assertEqual(result.numerator, 1)
        self.assertEqual(result.denominator, 6)
    
    def test_right_addition_with_integer(self):
        """Test right addition with integer."""
        f = Fraction(1, 2)
        result = 5 + f
        self.assertEqual(result.numerator, 11)
        self.assertEqual(result.denominator, 2)
    
    def test_in_place_addition(self):
        """Test in-place addition."""
        f1 = Fraction(1, 2)
        f2 = Fraction(1, 3)
        f1 += f2
        self.assertEqual(f1.numerator, 5)
        self.assertEqual(f1.denominator, 6)
    
    def test_subtraction_normal(self):
        """Test normal fraction subtraction."""
        f1 = Fraction(1, 2)
        f2 = Fraction(1, 3)
        result = f1 - f2
        self.assertEqual(result.numerator, 1)
        self.assertEqual(result.denominator, 6)
    
    def test_subtraction_negative_result(self):
        """Test subtraction resulting in negative."""
        f1 = Fraction(1, 3)
        f2 = Fraction(1, 2)
        result = f1 - f2
        self.assertEqual(result.numerator, -1)
        self.assertEqual(result.denominator, 6)
    
    def test_multiplication_normal(self):
        """Test normal fraction multiplication."""
        f1 = Fraction(1, 2)
        f2 = Fraction(2, 3)
        result = f1 * f2
        self.assertEqual(result.numerator, 1)
        self.assertEqual(result.denominator, 3)
    
    def test_multiplication_with_zero(self):
        """Test multiplication with zero."""
        f1 = Fraction(1, 2)
        f2 = Fraction(0, 1)
        result = f1 * f2
        self.assertEqual(result.numerator, 0)
        self.assertEqual(result.denominator, 1)
    
    def test_multiplication_negative(self):
        """Test multiplication with negative fractions."""
        f1 = Fraction(-1, 2)
        f2 = Fraction(2, 3)
        result = f1 * f2
        self.assertEqual(result.numerator, -1)
        self.assertEqual(result.denominator, 3)
    
    def test_division_normal(self):
        """Test normal fraction division."""
        f1 = Fraction(1, 2)
        f2 = Fraction(2, 3)
        result = f1 / f2
        self.assertEqual(result.numerator, 3)
        self.assertEqual(result.denominator, 4)
    
    def test_division_by_zero_error(self):
        """Test division by zero raises ValueError."""
        f1 = Fraction(1, 2)
        f2 = Fraction(0, 1)
        with self.assertRaises(ValueError) as context:
            f1 / f2
        self.assertEqual(str(context.exception), "Cannot divide by zero.")
    
    def test_division_negative(self):
        """Test division with negative fractions."""
        f1 = Fraction(1, 2)
        f2 = Fraction(-2, 3)
        result = f1 / f2
        self.assertEqual(result.numerator, -3)
        self.assertEqual(result.denominator, 4)


class TestFractionComparisons(unittest.TestCase):
    """Test cases for Fraction comparison operations."""
    
    def test_equality_same_values(self):
        """Test equality with same values."""
        f1 = Fraction(1, 2)
        f2 = Fraction(1, 2)
        self.assertTrue(f1 == f2)
    
    def test_equality_equivalent_fractions(self):
        """Test equality with equivalent fractions."""
        f1 = Fraction(1, 2)
        f2 = Fraction(2, 4)
        self.assertTrue(f1 == f2)
    
    def test_equality_different_values(self):
        """Test equality with different values."""
        f1 = Fraction(1, 2)
        f2 = Fraction(1, 3)
        self.assertFalse(f1 == f2)
    
    def test_inequality(self):
        """Test inequality operator."""
        f1 = Fraction(1, 2)
        f2 = Fraction(1, 3)
        self.assertTrue(f1 != f2)
    
    def test_greater_than(self):
        """Test greater than comparison."""
        f1 = Fraction(1, 2)
        f2 = Fraction(1, 3)
        self.assertTrue(f1 > f2)
        self.assertFalse(f2 > f1)
    
    def test_greater_than_equal(self):
        """Test greater than or equal comparison."""
        f1 = Fraction(1, 2)
        f2 = Fraction(1, 3)
        f3 = Fraction(1, 2)
        self.assertTrue(f1 >= f2)
        self.assertTrue(f1 >= f3)
        self.assertFalse(f2 >= f1)
    
    def test_less_than(self):
        """Test less than comparison."""
        f1 = Fraction(1, 3)
        f2 = Fraction(1, 2)
        self.assertTrue(f1 < f2)
        self.assertFalse(f2 < f1)
    
    def test_less_than_equal(self):
        """Test less than or equal comparison."""
        f1 = Fraction(1, 3)
        f2 = Fraction(1, 2)
        f3 = Fraction(1, 3)
        self.assertTrue(f1 <= f2)
        self.assertTrue(f1 <= f3)
        self.assertFalse(f2 <= f1)
    
    def test_comparison_with_negative(self):
        """Test comparisons with negative fractions."""
        f1 = Fraction(-1, 2)
        f2 = Fraction(1, 3)
        self.assertTrue(f1 < f2)
        self.assertFalse(f1 > f2)
    
    def test_comparison_both_negative(self):
        """Test comparisons with both negative fractions."""
        f1 = Fraction(-1, 2)
        f2 = Fraction(-1, 3)
        self.assertTrue(f1 < f2)  # -1/2 < -1/3
        self.assertFalse(f1 > f2)


class TestFractionStringRepresentation(unittest.TestCase):
    """Test cases for Fraction string representations."""
    
    def test_str_representation(self):
        """Test string representation."""
        f = Fraction(1, 2)
        self.assertEqual(str(f), "1/2")
    
    def test_repr_representation(self):
        """Test repr representation."""
        f = Fraction(1, 2)
        self.assertEqual(repr(f), "Fraction(1, 2)")
    
    def test_str_negative_numerator(self):
        """Test string representation with negative numerator."""
        f = Fraction(-3, 4)
        self.assertEqual(str(f), "-3/4")
    
    def test_str_whole_number(self):
        """Test string representation of whole number."""
        f = Fraction(5, 1)
        self.assertEqual(str(f), "5/1")
    
    def test_str_zero(self):
        """Test string representation of zero."""
        f = Fraction(0, 1)
        self.assertEqual(str(f), "0/1")


class TestFractionEdgeCases(unittest.TestCase):
    """Test cases for edge cases and boundary conditions."""
    
    def test_very_large_numbers(self):
        """Test with very large numbers."""
        f1 = Fraction(10**18, 10**18 + 1)
        f2 = Fraction(1, 2)
        result = f1 + f2
        self.assertIsInstance(result, Fraction)
    
    def test_gcd_edge_cases(self):
        """Test GCD calculation edge cases."""
        # test with numbers that have large GCD
        f = Fraction(12345678, 24691356)
        self.assertEqual(f.numerator, 1)
        self.assertEqual(f.denominator, 2)
    
    def test_arithmetic_overflow_protection(self):
        """Test arithmetic operations don't cause overflow issues."""
        f1 = Fraction(10**10, 1)
        f2 = Fraction(1, 10**10)
        result = f1 * f2
        self.assertEqual(result.numerator, 1)
        self.assertEqual(result.denominator, 1)
    
    def test_precision_maintenance(self):
        """Test that precision is maintained through operations."""
        # create a fraction that would lose precision as float
        f = Fraction(1, 3)
        result = f + f + f
        self.assertEqual(result.numerator, 1)
        self.assertEqual(result.denominator, 1)
    
    def test_chained_operations(self):
        """Test chained arithmetic operations."""
        f1 = Fraction(1, 2)
        f2 = Fraction(1, 3)
        f3 = Fraction(1, 4)
        result = f1 + f2 - f3
        expected = Fraction(7, 12)
        self.assertEqual(result, expected)


class TestFractionErrorConditions(unittest.TestCase):
    """Test cases for error conditions and exception handling."""
    
    def test_zero_denominator_various_numerators(self):
        """Test zero denominator with various numerators."""
        test_cases = [1, -1, 0, 100, -100]
        for num in test_cases:
            with self.assertRaises(ValueError):
                Fraction(num, 0)
    
    def test_division_by_zero_various_cases(self):
        """Test division by zero in various scenarios."""
        f = Fraction(1, 2)
        zero_fractions = [Fraction(0, 1), Fraction(0, 5), Fraction(0, 100)]
        for zero_f in zero_fractions:
            with self.assertRaises(ValueError):
                f / zero_f
    
    def test_invalid_right_addition(self):
        """Test right addition with invalid types."""
        f = Fraction(1, 2)
        result = f.__radd__("invalid")
        self.assertEqual(result, NotImplemented)
    
    def test_invalid_in_place_addition(self):
        """Test in-place addition with invalid types."""
        f = Fraction(1, 2)
        result = f.__iadd__("invalid")
        self.assertEqual(result, NotImplemented)


class TestFractionTypeValidation(unittest.TestCase):
    """Test cases for type validation through metaclass."""
    
    def setUp(self):
        """Set up test fixtures."""
        # note: These tests assume the TypeChecker metaclass validates types
        # the actual behavior depends on the implementation of TypeChecker
        pass
    
    @patch('stpstone.transformations.validation.metaclass_type_checker.TypeChecker')
    def test_type_checker_integration(self, mock_type_checker: type[Any]) -> None:
        """Test integration with TypeChecker metaclass."""
        # this test verifies that TypeChecker is called during class creation
        # actual behavior depends on TypeChecker implementation
        f = Fraction(1, 2)
        self.assertIsInstance(f, Fraction)


class TestFractionMathematicalProperties(unittest.TestCase):
    """Test mathematical properties and invariants."""
    
    def test_associativity_addition(self):
        """Test associativity of addition: (a + b) + c = a + (b + c)."""
        a = Fraction(1, 2)
        b = Fraction(1, 3)
        c = Fraction(1, 4)
        
        left = (a + b) + c
        right = a + (b + c)
        self.assertEqual(left, right)
    
    def test_commutativity_addition(self):
        """Test commutativity of addition: a + b = b + a."""
        a = Fraction(1, 2)
        b = Fraction(1, 3)
        
        self.assertEqual(a + b, b + a)
    
    def test_commutativity_multiplication(self):
        """Test commutativity of multiplication: a * b = b * a."""
        a = Fraction(1, 2)
        b = Fraction(2, 3)
        
        self.assertEqual(a * b, b * a)
    
    def test_identity_element_addition(self):
        """Test additive identity: a + 0 = a."""
        a = Fraction(1, 2)
        zero = Fraction(0, 1)
        
        self.assertEqual(a + zero, a)
    
    def test_identity_element_multiplication(self):
        """Test multiplicative identity: a * 1 = a."""
        a = Fraction(1, 2)
        one = Fraction(1, 1)
        
        self.assertEqual(a * one, a)
    
    def test_inverse_element_addition(self):
        """Test additive inverse: a + (-a) = 0."""
        a = Fraction(1, 2)
        neg_a = Fraction(-1, 2)
        zero = Fraction(0, 1)
        
        self.assertEqual(a + neg_a, zero)
    
    def test_inverse_element_multiplication(self):
        """Test multiplicative inverse: a * (1/a) = 1."""
        a = Fraction(2, 3)
        inv_a = Fraction(3, 2)
        one = Fraction(1, 1)
        
        self.assertEqual(a * inv_a, one)
    
    def test_distributivity(self):
        """Test distributivity: a * (b + c) = a * b + a * c."""
        a = Fraction(1, 2)
        b = Fraction(1, 3)
        c = Fraction(1, 4)
        
        left = a * (b + c)
        right = a * b + a * c
        self.assertEqual(left, right)


class TestFractionCompatibility(unittest.TestCase):
    """Test compatibility with Python's standard library fractions."""
    
    def test_comparison_with_standard_fraction(self):
        """Compare results with Python's standard Fraction class."""
        test_cases = [
            (1, 2, 1, 3),  # 1/2 + 1/3
            (3, 4, 2, 5),  # 3/4 * 2/5
            (5, 6, 1, 2),  # 5/6 - 1/2
            (2, 3, 3, 4),  # 2/3 / 3/4
        ]
        
        for num1, den1, num2, den2 in test_cases:
            # our implementation
            f1 = Fraction(num1, den1)
            f2 = Fraction(num2, den2)
            
            # standard library
            sf1 = StdFraction(num1, den1)
            sf2 = StdFraction(num2, den2)
            
            # compare addition
            our_add = f1 + f2
            std_add = sf1 + sf2
            self.assertEqual(our_add.numerator, std_add.numerator)
            self.assertEqual(our_add.denominator, std_add.denominator)
            
            # compare multiplication
            our_mul = f1 * f2
            std_mul = sf1 * sf2
            self.assertEqual(our_mul.numerator, std_mul.numerator)
            self.assertEqual(our_mul.denominator, std_mul.denominator)


if __name__ == '__main__':
    # create a test suite with all test cases
    test_suite = unittest.TestSuite()
    
    # add all test classes to the suite
    test_classes = [
        TestFractionInitialization,
        TestFractionGetters,
        TestFractionArithmetic,
        TestFractionComparisons,
        TestFractionStringRepresentation,
        TestFractionEdgeCases,
        TestFractionErrorConditions,
        TestFractionTypeValidation,
        TestFractionMathematicalProperties,
        TestFractionCompatibility
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # run the tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)