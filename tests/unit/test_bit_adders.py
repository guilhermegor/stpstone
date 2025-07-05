import unittest

from stpstone.analytics.arithmetics.bit_adders import EightBitFullAdder, FullAdder, HalfAdder


class TestHalfAdder(unittest.TestCase):
    """Test cases for HalfAdder class."""

    def test_normal_operations(self):
        """Test normal operations of HalfAdder."""
        # test all possible input combinations
        self.assertEqual(HalfAdder(0, 0).get_sum(), 0)
        self.assertEqual(HalfAdder(0, 0).get_carry(), 0)

        self.assertEqual(HalfAdder(0, 1).get_sum(), 1)
        self.assertEqual(HalfAdder(0, 1).get_carry(), 0)

        self.assertEqual(HalfAdder(1, 0).get_sum(), 1)
        self.assertEqual(HalfAdder(1, 0).get_carry(), 0)

        self.assertEqual(HalfAdder(1, 1).get_sum(), 0)
        self.assertEqual(HalfAdder(1, 1).get_carry(), 1)

    def test_type_validation(self):
        """Test type validation in HalfAdder."""
        # test invalid types
        with self.assertRaises(TypeError):
            HalfAdder("0", 1)
        with self.assertRaises(TypeError):
            HalfAdder(0, "1")
        with self.assertRaises(TypeError):
            HalfAdder(0.5, 1)
        with self.assertRaises(TypeError):
            HalfAdder(0, 1.0)

    def test_value_validation(self):
        """Test value validation in HalfAdder."""
        # test invalid values (non-binary)
        with self.assertRaises(ValueError):
            HalfAdder(2, 1)
        with self.assertRaises(ValueError):
            HalfAdder(0, -1)
        with self.assertRaises(ValueError):
            HalfAdder(3, 4)


class TestFullAdder(unittest.TestCase):
    """Test cases for FullAdder class."""

    def test_normal_operations(self):
        """Test normal operations of FullAdder."""
        # test various combinations
        self.assertEqual(FullAdder(0, 0, 0).get_sum(), 0)
        self.assertEqual(FullAdder(0, 0, 0).get_carry_out(), 0)

        self.assertEqual(FullAdder(0, 1, 0).get_sum(), 1)
        self.assertEqual(FullAdder(0, 1, 0).get_carry_out(), 0)

        self.assertEqual(FullAdder(1, 0, 1).get_sum(), 0)
        self.assertEqual(FullAdder(1, 0, 1).get_carry_out(), 1)

        self.assertEqual(FullAdder(1, 1, 1).get_sum(), 1)
        self.assertEqual(FullAdder(1, 1, 1).get_carry_out(), 1)

    def test_carry_propagation(self):
        """Test carry propagation in FullAdder."""
        # test cases where carry is generated
        self.assertEqual(FullAdder(1, 1, 0).get_carry_out(), 1)
        self.assertEqual(FullAdder(1, 0, 1).get_carry_out(), 1)
        self.assertEqual(FullAdder(0, 1, 1).get_carry_out(), 1)

    def test_type_validation(self):
        """Test type validation in FullAdder."""
        # test invalid types
        with self.assertRaises(TypeError):
            FullAdder("0", 1, 0)
        with self.assertRaises(TypeError):
            FullAdder(0, [1], 0)
        with self.assertRaises(TypeError):
            FullAdder(0, 1, 0.5)

    def test_value_validation(self):
        """Test value validation in FullAdder."""
        # test invalid values (non-binary)
        with self.assertRaises(ValueError):
            FullAdder(2, 1, 0)
        with self.assertRaises(ValueError):
            FullAdder(0, -1, 0)
        with self.assertRaises(ValueError):
            FullAdder(0, 1, 2)


class TestEightBitFullAdder(unittest.TestCase):
    """Test cases for EightBitFullAdder class."""

    def test_normal_operations(self):
        """Test normal operations of EightBitFullAdder."""
        # test basic additions
        adder = EightBitFullAdder(0, 0)
        sum_result, carry = adder.add()
        self.assertEqual(sum_result, 0)
        self.assertEqual(carry, 0)

        adder = EightBitFullAdder(10, 20)
        sum_result, carry = adder.add()
        self.assertEqual(sum_result, 30)
        self.assertEqual(carry, 0)

        adder = EightBitFullAdder(255, 0)
        sum_result, carry = adder.add()
        self.assertEqual(sum_result, 255)
        self.assertEqual(carry, 0)

    def test_carry_operations(self):
        """Test carry operations in EightBitFullAdder."""
        # test cases where carry is generated
        adder = EightBitFullAdder(255, 1)
        sum_result, carry = adder.add()
        self.assertEqual(sum_result, 0)
        self.assertEqual(carry, 1)

        adder = EightBitFullAdder(128, 128)
        sum_result, carry = adder.add()
        self.assertEqual(sum_result, 0)
        self.assertEqual(carry, 1)

    def test_edge_cases(self):
        """Test edge cases of EightBitFullAdder."""
        # test maximum values
        adder = EightBitFullAdder(255, 255)
        sum_result, carry = adder.add()
        self.assertEqual(sum_result, 254)
        self.assertEqual(carry, 1)

        # test minimum values
        adder = EightBitFullAdder(0, 0)
        sum_result, carry = adder.add()
        self.assertEqual(sum_result, 0)
        self.assertEqual(carry, 0)

        # test random values
        adder = EightBitFullAdder(123, 45)
        sum_result, carry = adder.add()
        self.assertEqual(sum_result, 168)
        self.assertEqual(carry, 0)

    def test_type_validation(self):
        """Test type validation in EightBitFullAdder."""
        # test invalid types
        with self.assertRaises(TypeError):
            EightBitFullAdder("255", 0)
        with self.assertRaises(TypeError):
            EightBitFullAdder(0, [1])
        with self.assertRaises(TypeError):
            EightBitFullAdder(0.5, 1)

    def test_value_validation(self):
        """Test value validation in EightBitFullAdder."""
        # test invalid values (out of 8-bit range)
        with self.assertRaises(ValueError):
            EightBitFullAdder(-1, 0)
        with self.assertRaises(ValueError):
            EightBitFullAdder(256, 0)
        with self.assertRaises(ValueError):
            EightBitFullAdder(0, -100)
        with self.assertRaises(ValueError):
            EightBitFullAdder(1000, 1000)

    def test_binary_operations(self):
        """Test binary operations in EightBitFullAdder."""
        # test binary inputs
        adder = EightBitFullAdder(0b11001100, 0b00110011)
        sum_result, carry = adder.add()
        self.assertEqual(sum_result, 0b11111111)
        self.assertEqual(carry, 0)

        adder = EightBitFullAdder(0b11111111, 0b00000001)
        sum_result, carry = adder.add()
        self.assertEqual(sum_result, 0b00000000)
        self.assertEqual(carry, 1)

    def test_docstring_examples(self):
        """Test the examples provided in docstrings."""
        # halfAdder examples
        adder = HalfAdder(0, 1)
        self.assertEqual(adder.get_sum(), 1)
        self.assertEqual(adder.get_carry(), 0)

        # fullAdder examples
        adder = FullAdder(1, 1, 0)
        self.assertEqual(adder.get_sum(), 0)
        self.assertEqual(adder.get_carry_out(), 1)

        # eightBitFullAdder examples
        adder = EightBitFullAdder(10, 20)
        self.assertEqual(adder.add(), (30, 0))
        adder = EightBitFullAdder(255, 1)
        self.assertEqual(adder.add(), (0, 1))


if __name__ == '__main__':
    unittest.main()