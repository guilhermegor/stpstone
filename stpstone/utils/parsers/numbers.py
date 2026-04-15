"""Numerical operations and validation utilities.

This module provides a collection of methods for handling numerical operations including
multiples generation, rounding, conversions, and mathematical computations. It includes
robust input validation and type checking through metaclass.
"""

from fractions import Fraction
import functools
import math
from math import gcd
from numbers import Number
import operator
import re
from typing import Any, Optional, Union

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class NumHandler(metaclass=TypeChecker):
    """Class for handling numerical operations and conversions."""

    def _validate_positive_int(self, value: int, name: str) -> None:
        """Validate that a value is a positive integer.

        Parameters
        ----------
        value : int
            Value to validate
        name : str
            Variable name for error messages

        Raises
        ------
        ValueError
            If value is not a positive integer
        """
        if not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer, got {value}")

    def multiples(self, m: int, closest_ceiling_num: int) -> list[Number]:
        """Generate a list of numerical multiples of a given number m.

        The list includes multiples until reaching or passing closest_ceiling_num.
        The last element will be the closest multiple of m to closest_ceiling_num.

        Parameters
        ----------
        m : int
            The number of which to generate multiples
        closest_ceiling_num : int
            The number after which to stop generating multiples

        Returns
        -------
        list[Number]
            A list of multiples of m, stopping at closest_ceiling_num

        Examples
        --------
        >>> multiples(3, 10)
        [0, 3, 6, 9]
        """
        self._validate_positive_int(m, "m")
        self._validate_positive_int(closest_ceiling_num, "closest_ceiling_num")

        list_numerical_multiples = []
        count = int(closest_ceiling_num / m) + 2
        for i in range(0, count * m, m):
            list_numerical_multiples.append(i)

        if list_numerical_multiples[-1] > closest_ceiling_num:
            list_numerical_multiples[-1] = closest_ceiling_num
        
        return list(set(list_numerical_multiples))

    def nearest_multiple(self, number: Number, multiple: Number) -> int:
        """Return the nearest multiple of a given number.

        Parameters
        ----------
        number : Number
            The number for which to find the nearest multiple
        multiple : Number
            The number of which to find the nearest multiple

        Returns
        -------
        int
            The nearest multiple of multiple to number

        Raises
        ------
        ValueError
            If number or multiple is not a valid number
            If multiple is zero

        Examples
        --------
        >>> nearest_multiple(10.7, 3)
        9
        """
        if not self.is_number(number):
            raise ValueError("number must be a valid number")
        if not self.is_number(multiple) or multiple == 0:
            raise ValueError("multiple must be a non-zero number")

        return multiple * int(number / multiple)

    def round_up(
        self,
        float_number_to_round: float,
        float_base: float,
        float_ceiling: float
    ) -> float:
        """Round a number up to nearest multiple of base, capped at ceiling.

        Parameters
        ----------
        float_number_to_round : float
            The number to round up
        float_base : float
            The base number to round up to
        float_ceiling : float
            The maximum value the new number can take

        Returns
        -------
        float
            The rounded up number, capped at ceiling

        Raises
        ------
        ValueError
            If inputs cannot be converted to float
        """
        try:
            float_number_to_round, float_base, float_ceiling = (
                float(x) for x in [float_number_to_round, float_base, float_ceiling]
            )
        except ValueError as err:
            raise ValueError("All inputs must be convertible to float") from err

        next_multiple = float_base + self.truncate(float_number_to_round / float_base, 0) \
            * float_base
        return float(next_multiple) if next_multiple < float_ceiling else float_ceiling

    def decimal_to_fraction(self, decimal_number: Number) -> Fraction:
        """Convert a decimal number to a fraction.

        Parameters
        ----------
        decimal_number : Number
            The decimal number to convert

        Returns
        -------
        Fraction
            A Fraction object representing the decimal number

        Raises
        ------
        ValueError
            If input cannot be converted to decimal
        """
        try:
            return Fraction(decimal_number)
        except (TypeError, ValueError) as err:
            raise ValueError(f"Could not convert {decimal_number} to fraction") from err

    def greatest_common_divisor(self, int1: int, int2: int) -> int:
        """Calculate the greatest common divisor (GCD) of two integers.

        Parameters
        ----------
        int1 : int
            The first integer
        int2 : int
            The second integer

        Returns
        -------
        int
            The GCD of int1 and int2
        """
        return gcd(int1, int2)

    def truncate(self, number: Union[float, int], digits: int) -> float:
        """Truncate a number to specified decimal places.

        Parameters
        ----------
        number : Union[float, int]
            The number to truncate
        digits : int
            Number of decimal places to truncate to

        Returns
        -------
        float
            Truncated number

        Raises
        ------
        ValueError
            If digits is negative
        """
        if digits < 0:
            raise ValueError("Digits must be non-negative")
        stepper = 10.0 ** digits
        return math.trunc(stepper * number) / stepper

    def sumproduct(self, *lists: list[list[int]]) -> int:
        """Compute sum of products of corresponding elements in lists.

        Parameters
        ----------
        *lists : list[list[int]]
            Lists to compute sumproduct

        Returns
        -------
        int
            Sum of products

        Raises
        ------
        ValueError
            If lists are empty
            If lists are empty
            If lists have different lengths
        """
        if not lists:
            raise ValueError("At least one list required")
        if all(len(lst) == 0 for lst in lists):
            raise ValueError("At least one non-empty list required")
        if len({len(lst) for lst in lists}) > 1:
            raise ValueError("All lists must have same length")

        return sum(functools.reduce(operator.mul, data) for data in zip(*lists))

    def number_sign(
        self,
        number: Union[float, int],
        base_number: Union[float, int] = 1
    ) -> float:
        """Determine sign of number applied to base_number.

        Parameters
        ----------
        number : Union[float, int]
            Number whose sign to use
        base_number : Union[float, int]
            Number to apply sign to (default: 1)

        Returns
        -------
        float
            base_number with sign of number
        """
        return math.copysign(base_number, number)

    def multiply_n_elements(self, *args: Union[int, float]) -> Union[int, float]:
        """Multiply all integer arguments.

        Parameters
        ----------
        *args : Union[int, float]
            Numbers to multiply

        Returns
        -------
        Union[int, float]
            Product of arguments

        Raises
        ------
        ValueError
            If no arguments provided
        """
        if not args:
            raise ValueError("At least one argument required")
        return functools.reduce(operator.mul, args, 1)

    def sum_n_elements(self, *args: Union[int, float]) -> Union[int, float]:
        """Sum all numerical arguments.

        Parameters
        ----------
        *args : Union[int, float]
            Numbers to sum

        Returns
        -------
        Union[int, float]
            Sum of arguments

        Raises
        ------
        ValueError
            If no arguments provided
        """
        if not args:
            raise ValueError("At least one argument required")
        return sum(args)

    def factorial(self, n: int) -> int:
        """Calculate factorial of positive integer.

        Parameters
        ----------
        n : int
            Positive integer

        Returns
        -------
        int
            Factorial of n
        """
        self._validate_positive_int(n, "n")
        return functools.reduce(operator.mul, range(1, n + 1))

    def range_floats(
        self,
        float_epsilon: float,
        float_inf: float,
        float_sup: float,
        float_pace: float
    ) -> list[float]:
        """Generate range of float values with given pace.

        Parameters
        ----------
        float_epsilon : float
            Unit of measurement
        float_inf : float
            Lower bound
        float_sup : float
            Upper bound
        float_pace : float
            Step size

        Returns
        -------
        list[float]
            Generated range

        Raises
        ------
        ValueError
            If bounds are invalid
            If pace is not positive
        """
        if float_inf >= float_sup:
            raise ValueError("Lower bound must be less than upper bound")
        if float_pace <= 0:
            raise ValueError("Pace must be positive")
        if float_epsilon <= 0:
            raise ValueError("Epsilon must be positive")

        num_steps = int((float_sup - float_inf) / float_pace) + 1
        return [float_inf + i * float_pace for i in range(num_steps)]

    def clamp(self, n: float, minn: float, maxn: float) -> float:
        """Clamp number between min and max values.

        Parameters
        ----------
        n : float
            Number to clamp
        minn : float
            Minimum value
        maxn : float
            Maximum value

        Returns
        -------
        float
            Clamped value

        Raises
        ------
        ValueError
            If min > max
        """
        if minn > maxn:
            raise ValueError("minn must be less than or equal to maxn")
        return max(min(maxn, n), minn)

    def is_numeric(self, str_: str) -> bool:
        """Check if string represents a valid number.

        Parameters
        ----------
        str_ : str
            String to check

        Returns
        -------
        bool
            True if string is numeric
        """
        try:
            float(str_)
            return True
        except ValueError:
            return False

    def is_number(
        self, 
        value_: Any # noqa ANN401: typing.Any is not allowed
    ) -> bool:
        """Check if value is a number (excluding bool).

        Parameters
        ----------
        value_ : Any
            Value to check

        Returns
        -------
        bool
            True if value is a number
        """
        return isinstance(value_, Number) and not isinstance(value_, bool)

    def transform_to_float(
        self,
        value_: Union[str, int, float, bool],
        int_precision: Optional[int] = None
    ) -> Union[float, str, bool]:
        """Convert value to float handling various formats.

        Parameters
        ----------
        value_ : Union[str, int, float, bool]
            Value to convert
        int_precision : Optional[int]
            Decimal places to round to

        Returns
        -------
        Union[float, str, bool]
            Converted value or original if conversion fails

        Notes
        -----
        Handles European/American formats, percentages, basis points.
        Preserves boolean values and non-convertible strings.
        """
        if isinstance(value_, bool):
            return value_

        if isinstance(value_, (int, float)):
            return round(value_, int_precision) if int_precision is not None else float(value_)

        original = str(value_).strip()
        s = original

        if re.search(r'[a-zA-Z].*\d|\d.*[a-zA-Z]', s) and not (
            '%' in s or re.search(r'(bp|b\.p\.?)$', s, flags=re.IGNORECASE)):
            return original

        bool_percentage = '%' in s
        bool_bp = re.search(r'(bp|b\.p\.?)$', s, flags=re.IGNORECASE) is not None
        bool_negative = re.search(r'\(.*\)|^-', original.strip()) is not None

        s = re.sub(r'[%\+\(\)]', '', s).strip()
        s = s.replace(')', '')
        s_clean = re.sub(r'(bp|b\.p\.?)$', '', s, flags=re.IGNORECASE).strip() if bool_bp else s
        
        comma_count = s_clean.count(',')
        dot_count = s_clean.count('.')

        if comma_count == 1 and dot_count > 0:
            if s_clean.find(',') > s_clean.find('.'):
                s_clean = s_clean.replace('.', '').replace(',', '.')
            else:
                s_clean = s_clean.replace(',', '')
        elif comma_count > 1:
            s_clean = s_clean.replace(',', '')
        elif dot_count > 1:
            parts = s_clean.split('.')
            if all(len(p) == 3 for p in parts[:-1]):
                s_clean = s_clean.replace('.', '')
            else:
                s_clean = s_clean.replace('.', '')
        elif comma_count == 1 and dot_count == 0:
            s_clean = s_clean.replace(',', '.')

        try:
            num = float(s_clean)
            if bool_negative:
                num = -abs(num)
            if bool_percentage:
                num /= 100
            elif bool_bp:
                num /= 10_000
            if int_precision is not None:
                num = round(num, int_precision)
            return num
        except ValueError:
            return original