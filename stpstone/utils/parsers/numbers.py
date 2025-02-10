### HANDLING NUMERICAL ISSUES ###

import math
import operator
import functools
from fractions import Fraction
from math import gcd
from stpstone.utils.parsers.str import StrHandler


class NumHandler:
    """
    REFERENCES: http://www.hoadley.net/options/develtoolsvolcalc.htm,
        https://introcs.cs.princeton.edu/python/21function/blackscholes.py.html,
        https://aaronschlegel.me/implied-volatility-functions-python.html#:~:text=Implied%20volatility%20σim,can%27t%20be%20directly%20observed.
    """

    def multiples(self, m, closest_ceiling_num):
        """
        DOCSTRING: LIST OF NUMERICAL MULTIPLES FROM A GIVEN NUMBER
        INPUTS: MULTIPLE AND THE CLOSEST CEILING NUMBER (ROUNDED UP)
        OUTPUTS: LIST
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

    def nearest_multiple(self, number, multiple):
        """
        DOCSTRING: RETURN THE NEAREST MULTIPLE OF A GIVEN NUMBER
        INPUTS: NUMBER AND MULTIPLE
        OUTPUTS: INTEGER
        """
        return multiple * int(number / multiple)

    def round_up(self, float_number_to_round, float_base, float_ceiling):
        """
        DOCSTRING: ROUND UP A DIVISION WITH A CEILING
        INPUTS: FLOAT NUMERATOR, FLOAT DENOMINATOR, AND FLOAT CEILING
        OUTPUTS: FLOAT
        """
        # correcting variables to float type
        float_number_to_round, float_base, float_ceiling = (float(x) for x in
                                                            [float_number_to_round, float_base, float_ceiling])
        # defining next multiple with a ceiling
        if float(float_base + self.truncate(float_number_to_round / float_base, 0)
                 * float_base) < float_ceiling:
            return float(float_base + self.truncate(float_number_to_round / float_base, 0)
                         * float_base)
        else:
            return float_ceiling

    def decimal_to_fraction(self, decimal_number):
        """
        DOCSTRING: FRACTION FROM A DECIMAL
        INPUTS: DECIMAL NUMBER
        OUTPUTS: FRACTION OF A NUMBER
        """
        return Fraction(decimal_number)

    def greatest_common_divisor(self, int1, int2):
        """
        DOCSTRING: GREATEST COMMON DIVISOR BETWEEN TWO INTEGERS
        INPUTS: INTEGER 1 AND INTEGER 2
        OUTPUTS: GREATEST COMMON DIVISOR
        """
        return gcd(int1, int2)

    def truncate(self, number, digits):
        """
        DOCSTRING: TRUNCATE A NUMBER IN NTH-DECIMAL
        INPUTS: NUMBER AND DIGITS
        OUTPUTS: FLOAT
        """
        stepper = 10.0 ** digits
        return math.trunc(stepper * number) / stepper

    def sumproduct(self, *lists):
        """
        REFERENCES: https://stackoverflow.com/questions/3849251/sum-of-products-for-multiple-lists-in-python
        DOCSTRING: SUMPRODUCT, OR POSITIONAL MULTIPLIACTION OF N LISTS
        INPUTS: *LISTS
        OUTPUTS: FLOAT
        """
        return sum(functools.reduce(operator.mul, data) for data in zip(*lists))

    def number_sign(self, number, base_number=1):
        """
        DOCSTRING: SIGN OF A GIVEN NUMBER
        INPUTS: NUMBER AND BASE (1 AS DEFAULT)
        OUTPUTS: EITHER 1 OR -1
        """
        return math.copysign(base_number, number)

    def multiply_n_elements(self, *args):
        """
        DOCSTRING: MULTIPLY A GIVEN SET OF ARGUMENTS
        INPUTS: ELEMENTS TO BE MULTIPLIED
        OUTPUTS: A GIVEN SET OF DATA MULTIPLIED, IN THE SAME FORMAT AS THE INPUT
        """
        product = 1
        for a in args:
            product *= a
        return product

    def sum_n_elements(self, *args):
        """
        DOCSTRING: SUM A GIVEN SET OF ARGUMENTS
        INPUTS: ELEMENTS TO BE ADDED
        OUTPUTS: A GIVEN SET OF DATA ADDED, IN THE SAME FORMAT AS THE INPUT
        """
        sum_ = 0
        for a in args:
            sum_ += a
        return sum_

    def factorial(self, n):
        """
        DOCSTRING: FACTORIAL MATHEMATICAL FUNCTION
        INPUTS: INTEGER N
        OUTPUTS: INTEGER
        """
        return functools.reduce(operator.mul, range(1, n + 1))

    def convert_thousands_decimals_separator(self, number_float, precision_decimals=2,
                                             thousands_inputs='.', thousands_outputs=',',
                                             decimals_inputs=',', decimals_outputs='.'):
        """
        DOCSTRING: CONVERTING THOUSANDS AND DECIMALS SEPARATORS
        INPUTS: NUMBER FLOAT AND NUMBER FORMAT
        OUTPUTS: NUMBER FLOAT WITH DESIRED FORMAT
        """
        # defining number format output string
        number_format_output = ':{}{}{}f'.format(thousands_outputs, decimals_outputs,
                                                 precision_decimals)
        number_format_output = '{' + number_format_output + '}'
        # converting to float if number is string
        if type(number_float) == str:
            number_int = ''.join(str(StrHandler().get_string_until_substr(
                number_float, decimals_inputs)).split(thousands_inputs))
            number_decimals = StrHandler().get_string_after_substr(
                number_float, decimals_inputs)
            number_float = float(
                number_int + decimals_outputs + number_decimals)
        # converting to interested format
        return {
            'number_float': number_float,
            'str_number_formated': number_format_output.format(number_float)
        }

    def range_floats(self, float_epsilon, float_inf, float_sup, float_pace):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return [float(x) / float_epsilon for x in range(int(float_inf * float_epsilon), 
                                                        int(float_sup * float_epsilon), 
                                                        int(float_pace * float_epsilon))]

    def clamp(self, n, minn, maxn):
        """
        DOCSTRING: CONSTRICT NUMBER WITHIN RANGE - WINSORIZE
        INPUTS:
        OUTPUTS:
        """
        return max(min(maxn, n), minn)

    def is_numeric(self, str_):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS
        """
        try:
            float(str_)
            return True
        except ValueError:
            return False