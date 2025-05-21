### HANDLING NUMERICAL ISSUES ###

import re
import math
import operator
import functools
from fractions import Fraction
from math import gcd
from numbers import Number
from typing import Any, Union, Dict, List, Optional
from stpstone.utils.parsers.str import StrHandler
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class NumHandler(metaclass=TypeChecker):

    def multiples(self, m: int, closest_ceiling_num: int) -> List[Number]:
        """Generate a list of numerical multiples of a given number m, until it reaches or passes
        a given number closest_ceiling_num. The last element of the list will be the closest
        multiple of m to closest_ceiling_num.

        For example, if m = 3 and closest_ceiling_num = 10, the output will be [0, 3, 6, 9].

        Parameters
        ----------
        m : int
            The number of which to generate multiples
        closest_ceiling_num : int
            The number after which to stop generating multiples

        Returns
        -------
        list
            A list of multiples of m, stopping at closest_ceiling_num
        """
        # appending multiples
        list_numerical_mulptiples = list()
        count = int(closest_ceiling_num / m) + 2
        for i in range(0, count * m, m):
            list_numerical_mulptiples.append(i)
        # replacing last value
        if list_numerical_mulptiples[-1] > closest_ceiling_num:
            list_numerical_mulptiples[-1] = closest_ceiling_num
        # output
        return list_numerical_mulptiples

    def nearest_multiple(self, number: Number, multiple: Number) -> int:
        """Return the nearest multiple of a given number. For example, if number = 10.7 and multiple = 3,
        the output will be 9, which is the nearest multiple of 3 to 10.7.
        
        Parameters
        ----------
        number : int or float
            The number for which to find the nearest multiple
        multiple : int
            The number of which to find the nearest multiple
        
        Returns
        -------
        int
            The nearest multiple of multiple to number
        """
        return multiple * int(number / multiple)

    def round_up(self, float_number_to_round: float, float_base: float, float_ceiling: float) \
        -> float:
        """Round a given number up to the nearest multiple of a given base number.
        The new number is capped at a given ceiling.
        
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
            The rounded up number, capped at the given ceiling
        """
        # correcting variables to float type
        float_number_to_round, float_base, float_ceiling = (float(x) for x in
                                                            [float_number_to_round, float_base, 
                                                             float_ceiling])
        # defining next multiple with a ceiling
        if float(float_base + self.truncate(float_number_to_round / float_base, 0)
                 * float_base) < float_ceiling:
            return float(float_base + self.truncate(float_number_to_round / float_base, 0)
                         * float_base)
        else:
            return float_ceiling

    def decimal_to_fraction(self, decimal_number: Number) -> Fraction:
        """Converts a decimal number to a fraction.

        Parameters
        ----------
        decimal_number : float or str
            The decimal number to convert. Can be a float or a string that 
            represents a decimal number.

        Returns
        -------
        Fraction
            A Fraction object representing the decimal number as a fraction.
        """
        return Fraction(decimal_number)

    def greatest_common_divisor(self, int1: int, int2: int) -> int:
        """Calculate the greatest common divisor (GCD) of two integers.

        Parameters
        ----------
        int1 : int
            The first integer.
        int2 : int
            The second integer.

        Returns
        -------
        int
            The greatest common divisor of int1 and int2.
        """
        return gcd(int1, int2)

    def truncate(self, number: Union[float, int], digits: int) -> float:
        """Truncates a given number to a given number of decimal places.

        Parameters
        ----------
        number : float or int
            The number to truncate.
        digits : int
            The number of decimal places to truncate to.

        Returns
        -------
        float
            The number truncated to the given number of decimal places.
        """
        stepper = 10.0 ** digits
        return math.trunc(stepper * number) / stepper

    def sumproduct(self, *lists: List[List[int]]) -> int:
        """Compute the sum of the products of corresponding elements of a list of lists.
        
        Parameters
        ----------
        *lists : list of lists
            The lists of which to compute the sum of products.
        
        Returns
        -------
        int
            The sum of the products of corresponding elements of the lists.
        """
        return sum(functools.reduce(operator.mul, data) for data in zip(*lists))

    def number_sign(self, number: Union[float, int], base_number: Union[float, int] = 1) -> float:
        """Determine the sign of a number.

        Parameters
        ----------
        number : float or int
            The number whose sign is to be determined.
        base_number : float or int, optional
            The base number whose sign will be returned. Default is 1.

        Returns
        -------
        float
            A float with the magnitude of base_number and the sign of number.
        """
        return math.copysign(base_number, number)

    def multiply_n_elements(self, *args: int) -> int:
        """Multiply the given elements together.

        Parameters
        ----------
        *args : int
            A variable number of integer arguments to be multiplied.

        Returns
        -------
        int
            The product of the given integer arguments.
        """
        product = 1
        for a in args:
            product *= a
        return product

    def sum_n_elements(self, *args: Union[int, float]) -> Union[int, float]:
        """Calculate the sum of a variable number of arguments.

        Parameters
        ----------
        *args : int or float
            A variable number of numerical arguments to be summed.

        Returns
        -------
        int or float
            The sum of the provided arguments.
        """
        sum_ = 0
        for a in args:
            sum_ += a
        return sum_

    def factorial(self, n: int) -> int:
        """Calculate the factorial of a given positive integer n.

        Parameters
        ----------
        n : int
            A positive integer for which the factorial is to be calculated.

        Returns
        -------
        int
            The factorial of the input integer n.
        """
        return functools.reduce(operator.mul, range(1, n + 1))

    def range_floats(self, float_epsilon, float_inf, float_sup, float_pace):
        """Generate a list of float values from float_inf to float_sup, with a step size of
        float_pace. The list is generated by multiplying each of the input parameters by
        float_epsilon, and then dividing each element of the list by float_epsilon. This
        allows the method to be used with any unit of measurement, such as days, hours,
        minutes, etc.

        Parameters
        ----------
        float_epsilon : float
            The unit of measurement for the output list.
        float_inf : float
            The lower bound of the range.
        float_sup : float
            The upper bound of the range.
        float_pace : float
            The step size of the range.

        Returns
        -------
        list
            A list of float values from float_inf to float_sup, with a step size of
            float_pace.
        """
        return [float(x) / float_epsilon for x in range(int(float_inf * float_epsilon),
                                                        int(float_sup * float_epsilon),
                                                        int(float_pace * float_epsilon))]

    def clamp(self, n: float, minn: float, maxn: float) -> float:
        """Clamp a number n to the range [minn, maxn].
        
        Parameters
        ----------
        n : float
            The number to clamp.
        minn : float
            The lower bound of the range.
        maxn : float
            The upper bound of the range.
        
        Returns
        -------
        float
            The clamped number.
        """
        return max(min(maxn, n), minn)

    def is_numeric(self, str_: str) -> bool:
        """Check whether a given string is a valid number.

        Parameters
        ----------
        str_ : str
            The string to check.

        Returns
        -------
        bool
            True if the string is a valid number, False otherwise.
        """
        try:
            float(str_)
            return True
        except ValueError:
            return False

    def is_number(self, value_: Any) -> bool:
        """Check whether a given value is a number (int, float, etc.).

        Parameters
        ----------
        value_ : Any
            The value to check.

        Returns
        -------
        bool
            True if the value is a number, False otherwise.
        """
        return isinstance(value_, (Number)) and not isinstance(value_, bool)

    def transform_to_float(
        self,
        value_: Union[str, int, float, bool], 
        int_precision: Optional[int] = None
    ) -> Union[float, str, bool]:
        """Convert a given value to a float, handling a variety of possible formats and
        edge cases.

        Parameters
        ----------
        value_ : Union[str, int, float, bool]
            The value to convert.
        int_precision : Optional[int]
            The number of decimal places to round to. If None, the original precision is
            preserved.

        Returns
        -------
        Union[float, str, bool]
            The converted float value, or the original value if conversion fails.

        Notes
        -----
        This method is designed to handle a wide range of possible number formats, including
        European and American number formats, percentages, basis points, and others. It also
        handles negative numbers and edge cases like empty strings and non-numeric input.

        Examples
        --------
        >>> transform_to_float('3,132.45%')
        31.3245
        >>> transform_to_float('1.234,56')
        1234.56
        >>> transform_to_float('1,234,567.89')
        1234567.89
        >>> transform_to_float('4.56 bp')
        0.000456
        >>> transform_to_float('(-)912.412.911,231')
        -912412911.231
        >>> transform_to_float(True)
        True
        >>> transform_to_float('Text Tried')
        'Text Tried'
        >>> transform_to_float('-0,10%')
        -0.001
        >>> transform_to_float('0,10%')
        0.001
        >>> transform_to_float('9888 Example')
        '9888 Example'
        """
        # return boolean values as-is
        if isinstance(value_, bool):
            return value_
        # handle non-string cases
        if isinstance(value_, (int, float)):
            return round(value_, int_precision) if int_precision is not None else float(value_)
        original = str(value_).strip()
        s = original
        # first check if this is clearly a mixed number/text case that should remain unchanged
        if re.search(r'[a-zA-Z].*\d|\d.*[a-zA-Z]', s) and not (
            '%' in s or re.search(r'(bp|b\.p\.?)$', s, flags=re.IGNORECASE)):
            return original
        # check for percentage or basis points
        bl_percentage = '%' in s
        bl_bp = re.search(r'(bp|b\.p\.?)$', s, flags=re.IGNORECASE) is not None
        # handle negative numbers (check original string)
        bl_negative = re.search(r'\(.*\)|^-', original.strip()) is not None
        # remove percentage signs, parentheses, and + signs but keep negative signs
        s = re.sub(r'[%\+\(\)]', '', s).strip()
        s = s.replace(')', '')  # remove closing parentheses if any
        # remove only known suffixes (bp, b.p.) but preserve other text
        if bl_bp:
            s_clean = re.sub(r'(bp|b\.p\.?)$', '', s, flags=re.IGNORECASE).strip()
        else:
            s_clean = s
        # count separators to determine format
        comma_count = s_clean.count(',')
        dot_count = s_clean.count('.')
        # handle number formatting
        if comma_count == 1 and dot_count > 0:
            # if there's exactly one comma and it comes after a dot, assume European format
            if s_clean.find(',') > s_clean.find('.'):
                s_clean = s_clean.replace('.', '').replace(',', '.')
            else:
                # otherwise assume American format (1,234.56)
                s_clean = s_clean.replace(',', '')
        elif comma_count > 1:
            # multiple commas - assume American thousands separator (1,234,567.89)
            s_clean = s_clean.replace(',', '')
        elif dot_count > 1:
            # multiple dots - could be European thousands separator (1.234.567,89)
            parts = s_clean.split('.')
            if all(len(p) == 3 for p in parts[:-1]):
                # all parts except last have length 3 → thousands separators
                s_clean = s_clean.replace('.', '')
            else:
                # might be decimal points, but unclear - try removing all dots
                s_clean = s_clean.replace('.', '')
        elif comma_count == 1 and dot_count == 0:
            # single comma - could be European decimal
            # replace comma with decimal point
            s_clean = s_clean.replace(',', '.')
        # try to convert to float
        try:
            num = float(s_clean)
            if bl_negative:
                num = -abs(num)
            # apply percentage or basis point conversion
            if bl_percentage:
                num /= 100
            elif bl_bp:
                num /= 10_000
            if int_precision is not None:
                num = round(num, int_precision)
            return num
        except ValueError:
            return original