# HANDLE CALCULUS ISSUES WITH PYTHON

import random
import sympy as sym
import pandas as np
from scipy.integrate import trapz, cumtrapz, quad
from linear_algebra import distance, add, scalar_multiply


class Calculus:

    def variables(self, str_symbols):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return sym.symbols(str_symbols.split(' '))

    def differentiation(self, f, variable_, nth_derivative=1, *args_symbols):
        """
        DOCSTRING: PERFORM NTH DIFFERENTIATION OF A FUNCTION
        INPUTS: FUNCTION, VARIABLE OF DIFFERENTIATION, NTH DERIVATIVE, *ARGS SYMBOLS
        OUTPUTS: POLYNOM
        """
        return sym.diff(f, variable_, nth_derivative)

    def integration(self, f, variable_, lower_bound=None, upper_bound=None):
        """
        DOCSTRING: PERFORM NTH DIFFERENTIATION OF A FUNCTION
        INPUTS: FUNCTION, VARIABLE, LOWER BOUNDARY, UPPER BOUNDARY
        OUTPUTS: POLYNOM
        """
        if all([x is None for x in [lower_bound, upper_bound]]):
            return sym.integrate(f, variable_)
        else:
            return sym.integrate(f, (variable_, lower_bound, upper_bound))

    def trapz_integration(self, f, variable_):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return trapz(f, variable_)

    def cumtrapz_integration(self, f, variable_):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return cumtrapz(f, variable_)

    def simplify(self, f):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return sym.simplify(f)

    def gradient_step(self, array_indep, gradient, step_size):
        """
        DOCSTRING: MOVES STEP_SIZE IN THE GRADIENT DIRECTION FROM V
        INPUTS: VECTOR, GRADIENT, STEP_SIZE
        OUTPUTS: VECTOR
        """
        array_indep = np.array(array_indep)
        assert len(array_indep) == len(gradient)
        step = scalar_multiply(step_size, gradient)
        return add(array_indep, step)

    def sum_of_squares_gradient(self, array_indep):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return [2 * v_i for v_i in array_indep]

    def least_gradient_vector(self, step_size=-0.01, iter=1000, epsilon=0.001):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # pick a random starting point
        array_indep = [random.uniform(-10, 10) for i in range(3)]
        # loop through
        for epoch in range(iter):
            # compute the gradient at array_indep
            grad = self.sum_of_squares_gradient(array_indep)
            # take a negative gradient step
            array_indep = self.gradient_step(array_indep, grad, step_size)
            print(epoch, array_indep)
        # array_indep should be close to 0
        assert distance(array_indep, [0, 0, 0]) < epsilon
        # returning array with least gradient vector
        return array_indep


# # differentiation
# x, y, z = Calculus().variables('x y z')
# f = x * y + x ** 2 + sym.sin(2 * y)
# df_dx = Calculus().differentiation(f, x, 1, x, y, z)
# print(df_dx)
# # output: 2*x + y

# # integration
# x, y, z = Calculus().variables('x y z')
# f = x * y + x ** 2 + sym.sin(2 * y)
# F = Calculus().integration(f, x, 0, 2)
# print(F)
# # output: 2*y + 2*sin(2*y) + 8/3

# # integration
# x, y, z = Calculus().variables('x y z')
# f = x * y + x ** 2 + sym.sin(2 * y)
# F = Calculus().integration(f, x)
# print(F)
# # output: x**3/3 + x**2*y/2 + x*sin(2*y)
