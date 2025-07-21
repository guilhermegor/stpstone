"""Unit tests for bit adder classes in stpstone.analytics.arithmetic.bit_adders module."""

from typing import Any
from unittest.mock import patch

import pytest

from stpstone.analytics.arithmetic.bit_adders import (
    ConfigurableHalfAdder,
    EightBitFullAdder,
    FlexibleAdder,
    FullAdder,
    HalfAdder,
)
from stpstone.transformations.validation.metaclass_type_checker import ConfigurableTypeChecker


class TestHalfAdder:
    """Test cases for HalfAdder class."""

    @pytest.mark.parametrize(
        "a, b, expected_sum, expected_carry",
        [
            (0, 0, 0, 0),
            (0, 1, 1, 0),
            (1, 0, 1, 0),
            (1, 1, 0, 1),
        ],
    )
    def test_normal_operations(
        self, a: int, b: int, expected_sum: int, expected_carry: int
    ) -> None:
        """Test normal operations of HalfAdder.
        
        Parameters
        ----------
        a : int
            First bit input (0 or 1)
        b : int
            Second bit input (0 or 1)
        expected_sum : int
            Expected sum output (0 or 1)
        expected_carry : int
            Expected carry output (0 or 1)
        """
        adder: HalfAdder = HalfAdder(a, b)
        assert adder.get_sum() == expected_sum
        assert adder.get_carry() == expected_carry

    @pytest.mark.parametrize(
        "a, b",
        [
            ("0", 1),
            (0, "1"),
            (0.5, 1),
            (0, 1.0),
        ],
    )
    def test_type_validation(self, a: type[Any], b: type[Any]) -> None:
        """Test type validation in HalfAdder.
        
        Parameters
        ----------
        a : type[Any]
            First bit input type
        b : type[Any]
            Second bit input type
        """
        with pytest.raises(TypeError):
            HalfAdder(a, b)

    @pytest.mark.parametrize(
        "a, b",
        [
            (2, 1),
            (0, -1),
            (3, 4),
        ],
    )
    def test_value_validation(self, a: int, b: int) -> None:
        """Test value validation in HalfAdder.
        
        Parameters
        ----------
        a : int
            First bit input (0 or 1)
        b : int
            Second bit input (0 or 1)
        """
        with pytest.raises(ValueError):
            HalfAdder(a, b)


class TestFullAdder:
    """Test cases for FullAdder class."""

    @pytest.mark.parametrize(
        "a, b, carry_in, expected_sum, expected_carry_out",
        [
            (0, 0, 0, 0, 0),
            (0, 1, 0, 1, 0),
            (1, 0, 1, 0, 1),
            (1, 1, 1, 1, 1),
        ],
    )
    def test_normal_operations(
        self,
        a: int,
        b: int,
        carry_in: int,
        expected_sum: int,
        expected_carry_out: int,
    ) -> None:
        """Test normal operations of FullAdder.
        
        Parameters
        ----------
        a : int
            First bit input (0 or 1)
        b : int
            Second bit input (0 or 1)
        carry_in : int
            Carry-in input (0 or 1)
        expected_sum : int
            Expected sum output (0 or 1)
        expected_carry_out : int
            Expected carry-out output (0 or 1)
        """
        adder: FullAdder = FullAdder(a, b, carry_in)
        assert adder.get_sum() == expected_sum
        assert adder.get_carry_out() == expected_carry_out

    @pytest.mark.parametrize(
        "a, b, carry_in",
        [
            (1, 1, 0),
            (1, 0, 1),
            (0, 1, 1),
        ],
    )
    def test_carry_propagation(self, a: int, b: int, carry_in: int) -> None:
        """Test carry propagation in FullAdder.
        
        Parameters
        ----------
        a : int
            First bit input (0 or 1)
        b : int
            Second bit input (0 or 1)
        carry_in : int
            Carry-in input (0 or 1)
        """
        adder: FullAdder = FullAdder(a, b, carry_in)
        assert adder.get_carry_out() == 1

    @pytest.mark.parametrize(
        "a, b, carry_in",
        [
            ("0", 1, 0),
            (0, [1], 0),
            (0, 1, 0.5),
        ],
    )
    def test_type_validation(self, a: type[Any], b: type[Any], carry_in: type[Any]) -> None:
        """Test type validation in FullAdder.
        
        Parameters
        ----------
        a : type[Any]
            First bit input type
        b : type[Any]
            Second bit input type
        carry_in : type[Any]
            Carry-in input type
        """
        with pytest.raises(TypeError):
            FullAdder(a, b, carry_in)

    @pytest.mark.parametrize(
        "a, b, carry_in",
        [
            (2, 1, 0),
            (0, -1, 0),
            (0, 1, 2),
        ],
    )
    def test_value_validation(self, a: int, b: int, carry_in: int) -> None:
        """Test value validation in FullAdder.
        
        Parameters
        ----------
        a : int
            First bit input (0 or 1)
        b : int
            Second bit input (0 or 1)
        carry_in : int
            Carry-in input (0 or 1)
        """
        with pytest.raises(ValueError):
            FullAdder(a, b, carry_in)


class TestEightBitFullAdder:
    """Test cases for EightBitFullAdder class."""

    @pytest.mark.parametrize(
        "a, b, expected_sum, expected_carry",
        [
            (0, 0, 0, 0),
            (10, 20, 30, 0),
            (255, 0, 255, 0),
            (255, 1, 0, 1),
            (128, 128, 0, 1),
            (255, 255, 254, 1),
            (123, 45, 168, 0),
            (0b11001100, 0b00110011, 0b11111111, 0),
            (0b11111111, 0b00000001, 0b00000000, 1),
        ],
    )
    def test_operations(
        self, a: int, b: int, expected_sum: int, expected_carry: int
    ) -> None:
        """Test various operations of EightBitFullAdder.
        
        Parameters
        ----------
        a : int
            First bit input (0 or 1)
        b : int
            Second bit input (0 or 1)
        expected_sum : int
            Expected sum output (0 or 1)
        expected_carry : int
            Expected carry output (0 or 1)
        """
        adder: EightBitFullAdder = EightBitFullAdder(a, b)
        sum_result, carry = adder.add()
        assert sum_result == expected_sum
        assert carry == expected_carry

    @pytest.mark.parametrize(
        "a, b",
        [
            ("255", 0),
            (0, [1]),
            (0.5, 1),
        ],
    )
    def test_type_validation(self, a: type[Any], b: type[Any]) -> None:
        """Test type validation in EightBitFullAdder.
        
        Parameters
        ----------
        a : type[Any]
            First bit input type
        b : type[Any]
            Second bit input type
        """
        with pytest.raises(TypeError):
            EightBitFullAdder(a, b)

    @pytest.mark.parametrize(
        "a, b",
        [
            (-1, 0),
            (256, 0),
            (0, -100),
            (1000, 1000),
        ],
    )
    def test_value_validation(self, a: int, b: int) -> None:
        """Test value validation in EightBitFullAdder.
        
        Parameters
        ----------
        a : int
            First bit input (0 or 1)
        b : int
            Second bit input (0 or 1)
        """
        with pytest.raises(ValueError):
            EightBitFullAdder(a, b)

    def test_docstring_examples(self) -> None:
        """Test the examples provided in docstrings."""
        # HalfAdder examples
        adder: HalfAdder = HalfAdder(0, 1)
        assert adder.get_sum() == 1
        assert adder.get_carry() == 0

        # FullAdder examples
        adder: FullAdder = FullAdder(1, 1, 0)
        assert adder.get_sum() == 0
        assert adder.get_carry_out() == 1

        # EightBitFullAdder examples
        adder: EightBitFullAdder = EightBitFullAdder(10, 20)
        assert adder.add() == (30, 0)
        adder = EightBitFullAdder(255, 1)
        assert adder.add() == (0, 1)


class TestConfigurableHalfAdder:
    """Test cases for ConfigurableHalfAdder class with advanced type checking."""

    @pytest.mark.parametrize(
        "a, b, expected_sum, expected_carry",
        [
            (0, 0, 0, 0),
            (0, 1, 1, 0),
            (1, 0, 1, 0),
            (1, 1, 0, 1),
        ],
    )
    def test_normal_operations(
        self, a: int, b: int, expected_sum: int, expected_carry: int
    ) -> None:
        """Test normal operations of ConfigurableHalfAdder.
        
        Parameters
        ----------
        a : int
            First bit input (0 or 1)
        b : int
            Second bit input (0 or 1)
        expected_sum : int
            Expected sum output (0 or 1)
        expected_carry : int
            Expected carry output (0 or 1)
        """
        adder: ConfigurableHalfAdder = ConfigurableHalfAdder(a, b)
        assert adder.get_sum() == expected_sum
        assert adder.get_carry() == expected_carry

    @pytest.mark.parametrize(
        "a, b",
        [
            ("0", 1),
            (0, "1"),
            (0.5, 1),
            (0, 1.0),
        ],
    )
    def test_strict_type_checking(self, a: type[Any], b: type[Any]) -> None:
        """Test strict type checking in ConfigurableHalfAdder.
        
        Parameters
        ----------
        a : type[Any]
            First bit input type
        b : type[Any]
            Second bit input type
        """
        with pytest.raises(TypeError):
            ConfigurableHalfAdder(a, b)

    def test_return_type_checking(self) -> None:
        """Test return type checking in ConfigurableHalfAdder."""
        adder: ConfigurableHalfAdder = ConfigurableHalfAdder(1, 0)
        assert adder.get_sum() == 1
        assert adder.get_carry() == 0

    def test_excluded_method_type_checking(self) -> None:
        """Test that excluded methods don't have type checking."""
        adder: ConfigurableHalfAdder = ConfigurableHalfAdder(1, 0)
        result: str = adder._private_method("not_an_int")
        assert result == "not_an_int"

    @pytest.mark.parametrize(
        "a, b",
        [
            (2, 1),
            (0, -1),
            (3, 4),
        ],
    )
    def test_value_validation(self, a: int, b: int) -> None:
        """Test value validation in ConfigurableHalfAdder.
        
        Parameters
        ----------
        a : int
            First bit input (0 or 1)
        b : int
            Second bit input (0 or 1)
        """
        with pytest.raises(ValueError):
            ConfigurableHalfAdder(a, b)

    def test_configuration_behavior(self) -> None:
        """Test that the configuration is properly applied."""
        adder: ConfigurableHalfAdder = ConfigurableHalfAdder(1, 1)
        assert hasattr(adder.__class__, "_type_check_config")
        config: dict = adder.__class__._type_check_config
        assert config["strict"]
        assert config["check_returns"]
        assert "_private_method" in config["exclude"]


class TestFlexibleAdder:
    """Test cases for FlexibleAdder class with configurable type checking."""

    def test_normal_operations(self) -> None:
        """Test normal operations of FlexibleAdder."""
        adder: FlexibleAdder = FlexibleAdder(0, 1)
        assert adder.a == 0
        assert adder.b == 1

        result: int = adder.add_numbers(5, 10)
        assert result == 15

    def test_excluded_method_no_type_checking(self) -> None:
        """Test that excluded methods don't have type checking."""
        adder: FlexibleAdder = FlexibleAdder(1, 0)
        with patch("builtins.print") as mock_print:
            adder.debug_method("test data")
            mock_print.assert_called_with("Debug: test data")

    def test_private_method_not_checked(self) -> None:
        """Test that private methods are not checked by default."""
        adder: FlexibleAdder = FlexibleAdder(1, 0)
        result: str = adder._private_helper("hello")
        assert result == "HELLO"

    def test_constructor_type_checking(self) -> None:
        """Test constructor type checking in FlexibleAdder."""
        with patch("builtins.print") as mock_print:
            adder: FlexibleAdder = FlexibleAdder("0", 1)
            mock_print.assert_called()
            warning_message: str = mock_print.call_args[0][0]
            assert "Warning in __init__" in warning_message
            assert adder.a == "0"
            assert adder.b == 1

    @pytest.mark.parametrize(
        "a, b",
        [
            (2, 1),
            (0, -1),
            (3, 4),
        ],
    )
    def test_value_validation(self, a: int, b: int) -> None:
        """Test value validation in FlexibleAdder.
        
        Parameters
        ----------
        a : int
            First bit input (0 or 1)
        b : int
            Second bit input (0 or 1)
        """
        with pytest.raises(ValueError):
            FlexibleAdder(a, b)

    def test_mixed_valid_invalid_params(self) -> None:
        """Test behavior with mixed valid and invalid parameters."""
        adder: FlexibleAdder = FlexibleAdder(1, 0)

        with patch("builtins.print") as mock_print:
            with pytest.raises(TypeError):
                adder.add_numbers(5, "10")

            mock_print.assert_called()
            warning: str = mock_print.call_args[0][0]
            assert "Warning in add_numbers" in warning
            assert "y must be of type int" in warning

    def test_enable_disable_functionality(self) -> None:
        """Test that type checking can be disabled."""

        class DisabledAdder(metaclass=ConfigurableTypeChecker):
            __type_check_config__ = {"enabled": False}

            def __init__(self, a: int, b: int) -> None:
                """Initialize with configurable type checking.
                
                Parameters
                ----------
                a : int
                    First bit input (0 or 1)
                b : int
                    Second bit input (0 or 1)
                """
                self.a = a
                self.b = b

            def add_numbers(self, x: int, y: int) -> int:
                """Add two numbers with type checking.
                
                Parameters
                ----------
                x : int
                    First number
                y : int
                    Second number

                Returns
                -------
                int
                    The sum of x and y
                """
                return str(x) + str(y)

        adder: DisabledAdder = DisabledAdder("not_int", 1.5)
        result: str = adder.add_numbers("5", [10])
        assert result == "5[10]"