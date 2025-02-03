# HANDLING NUMERCIAL SEQUENCES IN PYTHON

from scipy.misc import derivative
import math


class Fibonacci:

    def __init__(self, set_cache={0: 0, 1: 1}):
        self.set_cache = set_cache

    def fibonacci_of_n(self, n):
        """
        REFERENCES: https://realpython.com/fibonacci-sequence-python/
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # base case
        if n in self.set_cache:
            return self.set_cache[n]
        # compute and cache the fibonacci number
        self.set_cache[n] = self.fibonacci_of_n(
            n - 1) + self.fibonacci_of_n(n - 2)
        # recursive case
        return self.set_cache[n]

    def fibonacci(self, n):
        """
        REFERENCES: https://realpython.com/fibonacci-sequence-python/
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return [self.fibonacci_of_n(i) for i in range(n)]


class TaylorSeries:
    """
    REFERENCES: https://www.anthonymorast.com/blog/2021/10/03/taylor-series-in-python/#:~:text=The%20Taylor%20series%20of%20a,and%20using%20higher%20degree%20polynomials.
    DOCSTRING:
    INPUTS:
    OUTPUTS:
    """

    def __init__(self, function, order, center=0):
        self.center = center
        self.f = function
        self.order = order
        self.d_pts = order * 2
        self.coefficients = []
        # number of points (order) for scipy.misc.derivative
        # must be odd and greater than derivative order
        if self.d_pts % 2 == 0:
            self.d_pts += 1
        # find taylor series coefficients
        self.__find_coefficients()

    def __find_coefficients(self):
        """
        DOCSTRING: FIND TAYLOR SERIES COEFFICIENTS
        INPUTS: -
        OUTPUTS: -
        """
        for i in range(0, self.order + 1):
            self.coefficients.append(round(derivative(
                self.f, self.center, n=i, order=self.d_pts) / math.factorial(i), 5))

    def print_equation(self):
        """
        DOCSTRING: PRINT TAYLOR SERIES EQUATION
        INPUTS: -
        OUTPUTS: -
        """
        eqn_string = ''
        for i in range(self.order + 1):
            if self.coefficients[i] != 0:
                eqn_string += str(self.coefficients[i]) + (
                    '(x-{})^{}'.format(self.center, i) if i > 0 else '') + ' + '
        eqn_string = eqn_string[:-
                                3] if eqn_string.endswith(' + ') else eqn_string
        print(eqn_string)

    def print_coefficients(self):
        """
        DOCSTRING: PRINT TAYLOR SERIES' COEFFICIENTS
        INPUTS: -
        OUTPUTS: -
        """
        print(self.coefficients)

    def approximate_value(self, x):
        """
        DOCSTRING: APPROXIMATES THE VALUE OF F(X) USING THE TAYLOR POLYNOMIAL.
            X = POINT TO APPROXIMATE F(X)
        INPUTS: FLOAT
        OUTPUT: FLOAT
        """
        fx = 0
        for i in range(len(self.coefficients)):
            # coefficient * nth term
            fx += self.coefficients[i] * ((x - self.center)**i)
        return fx

    def approximate_derivative(self, x):
        """
        DOCSTRING: ESTIMATES THE DERIVATIVE OF A FUNCTION F(X) FROM ITS TAYLOR SERIES.
            USELESS SINCE WE NEED THE DERIVATIVE OF THE ACTUAL FUNCTION TO FIND THE SERIES
        INPUTS: FLOAT
        OUTPUTS: FLOAT
        """
        value = 0
        # coefficient * nth term
        for i in range(1, len(self.coefficients)):
            #   differentiate each term: x^n => n*x^(n-1)
            value += self.coefficients[i] * i * ((x - self.center)**(i - 1))
        return value

    def approximate_integral(self, x0, x1):
        """
        DOCSTRING: ESTIMATES THE DEFINITE INTEGRAL OF THE FUNCTION USING THE TAYLOR SERIES EXPANSION.
            MORE USEFUL, CONSIDER E^X * SIN(X), EASY TO DIFFERENTIATE BUT DIFFICULT TO INTEGRATE.
            X0 - LOWER LIMIT OF INTEGRATION
            X1 - UPPER LIMIT OF INTEGRATION
        INPUTS: X0, X1
        OUTPUT: FLOAT
        """

        # integrals can be off by a constant since int(f(x)) = F(x) + C
        value = 0
        for i in range(len(self.coefficients)):
            #   integrate each term: x^n => (1/n+1)*x^(n+1)
            value += ((self.coefficients[i] * (1 / (i + 1)) * ((x1 - self.center)**(i + 1))) -
                      (self.coefficients[i] * (1 / (i + 1)) * ((x0 - self.center)**(i + 1))))
        return value

    def get_coefficients(self):
        """
        DOCSTRING: RETURNS THE COEFFICIENTS OF THE TAYLOR SERIES
        INPUTS: -
        OUTPUTS: -
        """
        return self.coefficients
