# ROOT OF A FUNCTION WITH PYTHON

import numpy as np
from scipy.optimize import newton, fsolve


class Root:

    def bisection(self, f, a, b, epsilon):
        """
        REFERENCES: PYTHON PROGRAMMING AND NUMERICAL METHODS, A GUIDE FOR ENGINEERS AND SCIENTS -
            QINGKAI KONG, TIMMY SIAUW, ALEXANDRE M. BAYERN
        DOCSTRING:
        INPUTS: FUNCTION (F), A, B (BOUNDARIES) AND EPSILON (TOLERANCE)
        OUTPUTS: FLOAT
        """
        # approximates a root, R, of f bounded
        # by a and b to within tolerance
        # |f(m)| < epsilon with m being the midpoint
        # between a and b. Recursive implementation
        # check if a and b bound a root
        if np.sign(f(a)) == np.sign(f(b)):
            raise Exception('the scalars a and b do not bound a root')
        # get midpoint
        m = (a + b) / 2
        if np.abs(f(m)) < epsilon:
            #   stopping condition, report m as root
            return m
        elif np.sign(f(a)) == np.sign(f(m)):
            #   case where m is an improvement on a.
            #   make recursive call with a = m
            return self.bisection(f, m, b, epsilon)
        elif np.sign(f(b)) == np.sign(f(m)):
            #   case where m is an improvement on b.
            #   make recursive call with b = m
            return self.bisection(f, a, m, epsilon)

    def newton_raphson(self, f, x0, epsilon):
        """
        DOCSTRING: NEWTHON RAPHSON METHOD TO OPTIMIZE ROOT-FINDING
        INPUTS: F(FUNCTION), X0 (FIRST ATTEMPT) AND EPSILON
        OUTPUTS: FLOAT
        """
        return newton(f, x0, tol=epsilon)

    def fsolve(self, f, x0, epsilon):
        """
        DOCSTRING: FSOLVE METHOD TO OPTIMIZE ROOT-FINDING
        INPUTS: F(FUNCTION), X0 (FIRST ATTEMPT) AND EPSILON
        OUTPUTS: FLOAT
        """
        return fsolve(f, x0, xtol=epsilon)
