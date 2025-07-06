import unittest
from unittest.mock import patch

from stpstone.analytics.arithmetic.bit_adders import (
    ConfigurableHalfAdder,
    EightBitFullAdder,
    FlexibleAdder,
    FullAdder,
    HalfAdder,
)
from stpstone.transformations.validation.metaclass_type_checker import ConfigurableTypeChecker


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


class TestConfigurableHalfAdder(unittest.TestCase):
    """Test cases for ConfigurableHalfAdder class with advanced type checking."""

    def test_normal_operations(self):
        """Test normal operations of ConfigurableHalfAdder."""
        # test all possible input combinations
        adder = ConfigurableHalfAdder(0, 0)
        self.assertEqual(adder.get_sum(), 0)
        self.assertEqual(adder.get_carry(), 0)

        adder = ConfigurableHalfAdder(0, 1)
        self.assertEqual(adder.get_sum(), 1)
        self.assertEqual(adder.get_carry(), 0)

        adder = ConfigurableHalfAdder(1, 0)
        self.assertEqual(adder.get_sum(), 1)
        self.assertEqual(adder.get_carry(), 0)

        adder = ConfigurableHalfAdder(1, 1)
        self.assertEqual(adder.get_sum(), 0)
        self.assertEqual(adder.get_carry(), 1)

    def test_strict_type_checking(self):
        """Test strict type checking in ConfigurableHalfAdder."""
        # test invalid types - should raise TypeError due to strict mode
        with self.assertRaises(TypeError):
            ConfigurableHalfAdder("0", 1)
        with self.assertRaises(TypeError):
            ConfigurableHalfAdder(0, "1")
        with self.assertRaises(TypeError):
            ConfigurableHalfAdder(0.5, 1)
        with self.assertRaises(TypeError):
            ConfigurableHalfAdder(0, 1.0)

    def test_return_type_checking(self):
        """Test return type checking in ConfigurableHalfAdder."""
        # since return type checking is enabled, test that it works
        adder = ConfigurableHalfAdder(1, 0)
        # these should work as they return correct types
        self.assertEqual(adder.get_sum(), 1)
        self.assertEqual(adder.get_carry(), 0)

    def test_excluded_method_type_checking(self):
        """Test that excluded methods don't have type checking."""
        adder = ConfigurableHalfAdder(1, 0)
        # _private_method is excluded from type checking
        # so it should work even with wrong types
        result = adder._private_method("not_an_int")
        self.assertEqual(result, "not_an_int")

    def test_value_validation(self):
        """Test value validation in ConfigurableHalfAdder."""
        # test invalid values (non-binary)
        with self.assertRaises(ValueError):
            ConfigurableHalfAdder(2, 1)
        with self.assertRaises(ValueError):
            ConfigurableHalfAdder(0, -1)
        with self.assertRaises(ValueError):
            ConfigurableHalfAdder(3, 4)

    def test_configuration_behavior(self):
        """Test that the configuration is properly applied."""
        # test that the configuration dictionary is correctly set
        adder = ConfigurableHalfAdder(1, 1)
        self.assertTrue(hasattr(adder.__class__, '_type_check_config'))
        config = adder.__class__._type_check_config
        self.assertTrue(config['strict'])
        self.assertTrue(config['check_returns'])
        self.assertIn('_private_method', config['exclude'])


class TestFlexibleAdder(unittest.TestCase):
    """Test cases for FlexibleAdder class with configurable type checking."""

    def test_normal_operations(self):
        """Test normal operations of FlexibleAdder."""
        adder = FlexibleAdder(0, 1)
        self.assertEqual(adder.a, 0)
        self.assertEqual(adder.b, 1)

        # test add_numbers method
        result = adder.add_numbers(5, 10)
        self.assertEqual(result, 15)

    def test_excluded_method_no_type_checking(self):
        """Test that excluded methods don't have type checking."""
        adder = FlexibleAdder(1, 0)
        # debug_method is excluded from type checking
        # should work with any type without warnings
        with patch('builtins.print') as mock_print:
            adder.debug_method("test data")
            # check that the method was called normally
            mock_print.assert_called_with("Debug: test data")

    def test_private_method_not_checked(self):
        """Test that private methods are not checked by default."""
        adder = FlexibleAdder(1, 0)
        # _private_helper should not be type checked
        result = adder._private_helper("hello")
        self.assertEqual(result, "HELLO")

    def test_constructor_type_checking(self):
        """Test constructor type checking in FlexibleAdder."""
        # constructor should warn but not raise exception due to non-strict mode
        with patch('builtins.print') as mock_print:
            adder = FlexibleAdder("0", 1)
            # should print warning for constructor type mismatch
            mock_print.assert_called()
            warning_message = mock_print.call_args[0][0]
            self.assertIn("Warning in __init__", warning_message)
            # object should still be created
            self.assertEqual(adder.a, "0")
            self.assertEqual(adder.b, 1)

    def test_value_validation(self):
        """Test value validation in FlexibleAdder."""
        # test invalid values (non-binary)
        with self.assertRaises(ValueError):
            FlexibleAdder(2, 1)
        with self.assertRaises(ValueError):
            FlexibleAdder(0, -1)
        with self.assertRaises(ValueError):
            FlexibleAdder(3, 4)

    def test_mixed_valid_invalid_params(self):
        """Test behavior with mixed valid and invalid parameters."""
        adder = FlexibleAdder(1, 0)
        
        with patch('builtins.print') as mock_print:
            # one valid, one invalid parameter
            # this will attempt int + str which will fail
            with self.assertRaises(TypeError):  # The actual operation fails
                adder.add_numbers(5, "10")
            
            # should print one warning before the operation fails
            mock_print.assert_called()
            warning = mock_print.call_args[0][0]
            self.assertIn("Warning in add_numbers", warning)
            self.assertIn("y must be of type int", warning)

    def test_enable_disable_functionality(self):
        """Test that type checking can be disabled."""
        # create a class with type checking disabled
        class DisabledAdder(metaclass=ConfigurableTypeChecker):
            __type_check_config__ = {'enabled': False}
            
            def __init__(self, a: int, b: int) -> None:
                self.a = a
                self.b = b
                
            def add_numbers(self, x: int, y: int) -> int:
                return str(x) + str(y)  # Convert to string to avoid type issues
        
        # should work without any type checking
        adder = DisabledAdder("not_int", 1.5)
        result = adder.add_numbers("5", [10])
        # this would normally cause issues but should work when disabled
        self.assertEqual(result, "5[10]")  # string concatenation


if __name__ == '__main__':
    unittest.main()