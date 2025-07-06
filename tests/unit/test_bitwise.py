"""Unit tests for bitwise operation utilities.

This module provides comprehensive tests for the Bitwise class that implements
basic bitwise operations including AND, OR, XOR, and NOT.
"""

import os
import sys
from unittest import TestCase, main


# Add the project root to the path if needed
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from stpstone.analytics.arithmetic.bitwise import Bitwise


class TestUtilities(TestCase):
    """Test cases for the Bitwise class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.utils = Bitwise()
    
    def test_bitwise_and_basic_operations(self):
        """Test basic bitwise AND operations."""
        self.assertEqual(self.utils.bitwise_and(3, 5), 1)
        self.assertEqual(self.utils.bitwise_and(0b1100, 0b1010), 0b1000)
        self.assertEqual(self.utils.bitwise_and(7, 3), 3)  # 0b111 & 0b011 = 0b011
        self.assertEqual(self.utils.bitwise_and(15, 7), 7)  # 0b1111 & 0b0111 = 0b0111
        # 0b11111111 & 0b01010101 = 0b01010101
        self.assertEqual(self.utils.bitwise_and(255, 85), 85)
    
    def test_bitwise_and_edge_cases(self):
        """Test edge cases for bitwise AND."""
        self.assertEqual(self.utils.bitwise_and(0, 0), 0)
        self.assertEqual(self.utils.bitwise_and(0, 5), 0)
        self.assertEqual(self.utils.bitwise_and(5, 0), 0)
        
        self.assertEqual(self.utils.bitwise_and(5, 5), 5)
        self.assertEqual(self.utils.bitwise_and(255, 255), 255)
        
        self.assertEqual(self.utils.bitwise_and(1024, 512), 0)
        self.assertEqual(self.utils.bitwise_and(2**31 - 1, 2**30 - 1), 2**30 - 1)
    
    def test_bitwise_and_negative_numbers(self):
        """Test bitwise AND with negative numbers."""
        self.assertEqual(self.utils.bitwise_and(-1, 5), 5)  # All 1s & 5 = 5
        self.assertEqual(self.utils.bitwise_and(-2, 7), 6)  # ...11111110 & 00000111 = 00000110
        self.assertEqual(self.utils.bitwise_and(-5, -3), -7)  # Two's complement arithmetic
    
    def test_bitwise_or_basic_operations(self):
        """Test basic bitwise OR operations."""
        self.assertEqual(self.utils.bitwise_or(3, 5), 7)
        self.assertEqual(self.utils.bitwise_or(0b1100, 0b1010), 0b1110)
        
        self.assertEqual(self.utils.bitwise_or(1, 2), 3)  # 0b001 | 0b010 = 0b011
        self.assertEqual(self.utils.bitwise_or(8, 4), 12)  # 0b1000 | 0b0100 = 0b1100
        # 0b00001111 | 0b11110000 = 0b11111111
        self.assertEqual(self.utils.bitwise_or(15, 240), 255)
    
    def test_bitwise_or_edge_cases(self):
        """Test edge cases for bitwise OR."""
        self.assertEqual(self.utils.bitwise_or(0, 0), 0)
        self.assertEqual(self.utils.bitwise_or(0, 5), 5)
        self.assertEqual(self.utils.bitwise_or(5, 0), 5)
        
        self.assertEqual(self.utils.bitwise_or(5, 5), 5)
        self.assertEqual(self.utils.bitwise_or(255, 255), 255)
        
        self.assertEqual(self.utils.bitwise_or(1024, 512), 1536)
        self.assertEqual(self.utils.bitwise_or(2**31 - 1, 2**30), 2**31 - 1)
    
    def test_bitwise_or_negative_numbers(self):
        """Test bitwise OR with negative numbers."""
        self.assertEqual(self.utils.bitwise_or(-1, 5), -1)  # all 1s | anything = all 1s
        self.assertEqual(self.utils.bitwise_or(-8, 3), -5)  # two's complement arithmetic
        self.assertEqual(self.utils.bitwise_or(-5, -3), -1)
    
    def test_bitwise_xor_basic_operations(self):
        """Test basic bitwise XOR operations."""
        self.assertEqual(self.utils.bitwise_xor(3, 5), 6)
        self.assertEqual(self.utils.bitwise_xor(0b1100, 0b1010), 0b0110)
        
        self.assertEqual(self.utils.bitwise_xor(1, 3), 2)  # 0b001 ^ 0b011 = 0b010
        self.assertEqual(self.utils.bitwise_xor(15, 7), 8)  # 0b1111 ^ 0b0111 = 0b1000
        # 0b11111111 ^ 0b10101010 = 0b01010101
        self.assertEqual(self.utils.bitwise_xor(255, 170), 85)
    
    def test_bitwise_xor_edge_cases(self):
        """Test edge cases for bitwise XOR."""
        self.assertEqual(self.utils.bitwise_xor(0, 0), 0)
        self.assertEqual(self.utils.bitwise_xor(0, 5), 5)
        self.assertEqual(self.utils.bitwise_xor(5, 0), 5)
        
        self.assertEqual(self.utils.bitwise_xor(5, 5), 0)
        self.assertEqual(self.utils.bitwise_xor(255, 255), 0)
        self.assertEqual(self.utils.bitwise_xor(1024, 1024), 0)
        
        # xor properties
        # a ^ b ^ b = a
        a, b = 42, 73
        result = self.utils.bitwise_xor(a, b)
        self.assertEqual(self.utils.bitwise_xor(result, b), a)
        
        # large numbers
        self.assertEqual(self.utils.bitwise_xor(2**31 - 1, 2**30), 2**31 - 1 - 2**30)
    
    def test_bitwise_xor_negative_numbers(self):
        """Test bitwise XOR with negative numbers."""
        self.assertEqual(self.utils.bitwise_xor(-1, 5), -6)  # All 1s ^ 5
        self.assertEqual(self.utils.bitwise_xor(-8, 3), -5)  # two's complement arithmetic
        self.assertEqual(self.utils.bitwise_xor(-5, -3), 6)
    
    def test_bitwise_not_basic_operations(self):
        """Test basic bitwise NOT operations."""
        self.assertEqual(self.utils.bitwise_not(3), -4)
        self.assertEqual(self.utils.bitwise_not(0b1010), -11)
        
        self.assertEqual(self.utils.bitwise_not(0), -1)  # ~0 = -1
        self.assertEqual(self.utils.bitwise_not(1), -2)  # ~1 = -2
        self.assertEqual(self.utils.bitwise_not(255), -256)  # ~255 = -256
    
    def test_bitwise_not_edge_cases(self):
        """Test edge cases for bitwise NOT."""
        self.assertEqual(self.utils.bitwise_not(0), -1)
        
        self.assertEqual(self.utils.bitwise_not(1), -2)
        self.assertEqual(self.utils.bitwise_not(2), -3)
        self.assertEqual(self.utils.bitwise_not(4), -5)
        self.assertEqual(self.utils.bitwise_not(8), -9)
        
        self.assertEqual(self.utils.bitwise_not(1024), -1025)
        self.assertEqual(self.utils.bitwise_not(2**31 - 1), -2**31)
        
        # double negation should return original - 1
        # ~(~a) = a for unsigned, but in Python with two's complement: ~(~a) = a
        for num in [0, 1, 5, 42, 255, 1024]:
            self.assertEqual(self.utils.bitwise_not(self.utils.bitwise_not(num)), num)
    
    def test_bitwise_not_negative_numbers(self):
        """Test bitwise NOT with negative numbers."""
        self.assertEqual(self.utils.bitwise_not(-1), 0)  # ~(-1) = 0
        self.assertEqual(self.utils.bitwise_not(-2), 1)  # ~(-2) = 1
        self.assertEqual(self.utils.bitwise_not(-5), 4)  # ~(-5) = 4
        self.assertEqual(self.utils.bitwise_not(-256), 255)  # ~(-256) = 255
    
    def test_type_validation_bitwise_and(self):
        """Test type validation for bitwise_and method."""
        # these should raise TypeError due to metaclass type checking
        with self.assertRaises(TypeError):
            self.utils.bitwise_and("5", 3)
        
        with self.assertRaises(TypeError):
            self.utils.bitwise_and(5, "3")
        
        with self.assertRaises(TypeError):
            self.utils.bitwise_and(5.5, 3)
        
        with self.assertRaises(TypeError):
            self.utils.bitwise_and(5, 3.5)
        
        with self.assertRaises(TypeError):
            self.utils.bitwise_and(None, 3)
        
        with self.assertRaises(TypeError):
            self.utils.bitwise_and(5, None)
    
    def test_type_validation_bitwise_or(self):
        """Test type validation for bitwise_or method."""
        with self.assertRaises(TypeError):
            self.utils.bitwise_or("5", 3)
        
        with self.assertRaises(TypeError):
            self.utils.bitwise_or(5, "3")
        
        with self.assertRaises(TypeError):
            self.utils.bitwise_or(5.5, 3)
        
        with self.assertRaises(TypeError):
            self.utils.bitwise_or([5], 3)
    
    def test_type_validation_bitwise_xor(self):
        """Test type validation for bitwise_xor method."""
        with self.assertRaises(TypeError):
            self.utils.bitwise_xor("5", 3)
        
        with self.assertRaises(TypeError):
            self.utils.bitwise_xor(5, "3")
        
        with self.assertRaises(TypeError):
            self.utils.bitwise_xor(5.5, 3)
        
        with self.assertRaises(TypeError):
            self.utils.bitwise_xor({'a': 5}, 3)
    
    def test_type_validation_bitwise_not(self):
        """Test type validation for bitwise_not method."""
        with self.assertRaises(TypeError):
            self.utils.bitwise_not("5")
        
        with self.assertRaises(TypeError):
            self.utils.bitwise_not(5.5)
        
        with self.assertRaises(TypeError):
            self.utils.bitwise_not(None)
        
        with self.assertRaises(TypeError):
            self.utils.bitwise_not([5])
        
        with self.assertRaises(TypeError):
            self.utils.bitwise_not({'a': 5})
    
    def test_bitwise_properties(self):
        """Test mathematical properties of bitwise operations."""
        a, b, c = 42, 73, 156
        
        # and properties
        # commutative: a & b = b & a # noqa ERA001 - found commented-out code
        self.assertEqual(self.utils.bitwise_and(a, b), self.utils.bitwise_and(b, a))
        
        # associative: (a & b) & c = a & (b & c) # noqa ERA001 - found commented-out code
        left = self.utils.bitwise_and(self.utils.bitwise_and(a, b), c)
        right = self.utils.bitwise_and(a, self.utils.bitwise_and(b, c))
        self.assertEqual(left, right)
        
        # identity: a & 0 = 0, a & -1 = a # noqa ERA001 - found commented-out code
        self.assertEqual(self.utils.bitwise_and(a, 0), 0)
        self.assertEqual(self.utils.bitwise_and(a, -1), a)
        
        # or properties
        # commutative: a | b = b | a # noqa ERA001 - found commented-out code
        self.assertEqual(self.utils.bitwise_or(a, b), self.utils.bitwise_or(b, a))
        
        # associative: (a | b) | c = a | (b | c) # noqa ERA001 - found commented-out code
        left = self.utils.bitwise_or(self.utils.bitwise_or(a, b), c)
        right = self.utils.bitwise_or(a, self.utils.bitwise_or(b, c))
        self.assertEqual(left, right)
        
        # identity: a | 0 = a, a | -1 = -1
        self.assertEqual(self.utils.bitwise_or(a, 0), a)
        self.assertEqual(self.utils.bitwise_or(a, -1), -1)
        
        # xor properties
        # commutative: a ^ b = b ^ a # noqa ERA001 - found commented-out code
        self.assertEqual(self.utils.bitwise_xor(a, b), self.utils.bitwise_xor(b, a))
        
        # associative: (a ^ b) ^ c = a ^ (b ^ c) # noqa ERA001 - found commented-out code
        left = self.utils.bitwise_xor(self.utils.bitwise_xor(a, b), c)
        right = self.utils.bitwise_xor(a, self.utils.bitwise_xor(b, c))
        self.assertEqual(left, right)
        
        # identity: a ^ 0 = a, a ^ a = 0
        self.assertEqual(self.utils.bitwise_xor(a, 0), a)
        self.assertEqual(self.utils.bitwise_xor(a, a), 0)
    
    def test_de_morgan_laws(self):
        """Test De Morgan's laws for bitwise operations."""
        a, b = 42, 73
        
        # ~(a & b) = ~a | ~b
        left = self.utils.bitwise_not(self.utils.bitwise_and(a, b))
        right = self.utils.bitwise_or(self.utils.bitwise_not(a), self.utils.bitwise_not(b))
        self.assertEqual(left, right)
        
        # ~(a | b) = ~a & ~b
        left = self.utils.bitwise_not(self.utils.bitwise_or(a, b))
        right = self.utils.bitwise_and(self.utils.bitwise_not(a), self.utils.bitwise_not(b))
        self.assertEqual(left, right)
    
    def test_docstring_examples(self):
        """Test all examples provided in the docstrings."""
        utils = Bitwise()
        self.assertEqual(utils.bitwise_and(3, 5), 1)
        self.assertEqual(utils.bitwise_or(3, 5), 7)
        
        self.assertEqual(Bitwise().bitwise_and(0b1100, 0b1010), 0b1000)
        self.assertEqual(Bitwise().bitwise_and(3, 5), 1)
        
        self.assertEqual(Bitwise().bitwise_or(0b1100, 0b1010), 0b1110)
        self.assertEqual(Bitwise().bitwise_or(3, 5), 7)
        
        self.assertEqual(Bitwise().bitwise_xor(0b1100, 0b1010), 0b0110)
        self.assertEqual(Bitwise().bitwise_xor(3, 5), 6)
        
        self.assertEqual(Bitwise().bitwise_not(0b1010), -11)
        self.assertEqual(Bitwise().bitwise_not(3), -4)
    
    def test_large_numbers(self):
        """Test operations with very large numbers."""
        large_a = 2**63 - 1  # maximum positive value for 64-bit signed integer
        large_b = 2**62
        
        result_and = self.utils.bitwise_and(large_a, large_b)
        result_or = self.utils.bitwise_or(large_a, large_b)
        result_xor = self.utils.bitwise_xor(large_a, large_b)
        result_not = self.utils.bitwise_not(large_a)
        
        self.assertEqual(result_and, large_b)  # large_a has all bits set including large_b
        self.assertEqual(result_or, large_a)   # large_a already has all bits that large_b has
        self.assertEqual(result_xor, large_a - large_b)  # XOR of overlapping bits
        self.assertEqual(result_not, -large_a - 1)  # Two's complement inversion
    
    def test_multiple_operations_chain(self):
        """Test chaining multiple operations together."""
        a, b, c = 85, 170, 255  # 0b01010101, 0b10101010, 0b11111111
        
        # complex operation: ((a & b) | c) ^ (~a)
        step1 = self.utils.bitwise_and(a, b)       # 0
        step2 = self.utils.bitwise_or(step1, c)    # 255
        step3 = self.utils.bitwise_not(a)          # -86
        result = self.utils.bitwise_xor(step2, step3)
        
        # verify it's computed correctly
        expected = ((a & b) | c) ^ (~a)
        self.assertEqual(result, expected)


if __name__ == "__main__":
    main()