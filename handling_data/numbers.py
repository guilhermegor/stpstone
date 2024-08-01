### HANDLING NUMERICAL ISSUES ###

import math
import operator
import functools
from fractions import Fraction, gcd
from stpstone.handling_data.str import StrHandler


class NumHandler:
    '''
    REFERENCES: http://www.hoadley.net/options/develtoolsvolcalc.htm,
        https://introcs.cs.princeton.edu/python/21function/blackscholes.py.html,
        https://aaronschlegel.me/implied-volatility-functions-python.html#:~:text=Implied%20volatility%20σim,can%27t%20be%20directly%20observed.
    '''

    def multiples(self, m, closest_ceiling_num):
        '''
        DOCSTRING: LIST OF NUMERICAL MULTIPLES FROM A GIVEN NUMBER
        INPUTS: MULTIPLE AND THE CLOSEST CEILING NUMBER (ROUNDED UP)
        OUTPUTS: LIST
        '''
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
        '''
        DOCSTRING: RETURN THE NEAREST MULTIPLE OF A GIVEN NUMBER
        INPUTS: NUMBER AND MULTIPLE
        OUTPUTS: INTEGER
        '''
        return multiple * int(number / multiple)

    def round_up(self, float_number_to_round, float_base, float_ceiling):
        '''
        DOCSTRING: ROUND UP A DIVISION WITH A CEILING
        INPUTS: FLOAT NUMERATOR, FLOAT DENOMINATOR, AND FLOAT CEILING
        OUTPUTS: FLOAT
        '''
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
        '''
        DOCSTRING: FRACTION FROM A DECIMAL
        INPUTS: DECIMAL NUMBER
        OUTPUTS: FRACTION OF A NUMBER
        '''
        return Fraction(decimal_number)

    def greatest_common_divisor(self, int1, int2):
        '''
        DOCSTRING: GREATEST COMMON DIVISOR BETWEEN TWO INTEGERS
        INPUTS: INTEGER 1 AND INTEGER 2
        OUTPUTS: GREATEST COMMON DIVISOR
        '''
        return gcd(int1, int2)

    def truncate(self, number, digits):
        '''
        DOCSTRING: TRUNCATE A NUMBER IN NTH-DECIMAL
        INPUTS: NUMBER AND DIGITS
        OUTPUTS: FLOAT
        '''
        stepper = 10.0 ** digits
        return math.trunc(stepper * number) / stepper

    def sumproduct(self, *lists):
        '''
        REFERENCES: https://stackoverflow.com/questions/3849251/sum-of-products-for-multiple-lists-in-python
        DOCSTRING: SUMPRODUCT, OR POSITIONAL MULTIPLIACTION OF N LISTS
        INPUTS: *LISTS
        OUTPUTS: FLOAT
        '''
        return sum(functools.reduce(operator.mul, data) for data in zip(*lists))

    def number_sign(self, number, base_number=1):
        '''
        DOCSTRING: SIGN OF A GIVEN NUMBER
        INPUTS: NUMBER AND BASE (1 AS DEFAULT)
        OUTPUTS: EITHER 1 OR -1
        '''
        return math.copysign(base_number, number)

    def multiply_n_elements(self, *args):
        '''
        DOCSTRING: MULTIPLY A GIVEN SET OF ARGUMENTS
        INPUTS: ELEMENTS TO BE MULTIPLIED
        OUTPUTS: A GIVEN SET OF DATA MULTIPLIED, IN THE SAME FORMAT AS THE INPUT
        '''
        product = 1
        for a in args:
            product *= a
        return product

    def sum_n_elements(self, *args):
        '''
        DOCSTRING: SUM A GIVEN SET OF ARGUMENTS
        INPUTS: ELEMENTS TO BE ADDED
        OUTPUTS: A GIVEN SET OF DATA ADDED, IN THE SAME FORMAT AS THE INPUT
        '''
        sum_ = 0
        for a in args:
            sum_ += a
        return sum_

    def factorial(self, n):
        '''
        DOCSTRING: FACTORIAL MATHEMATICAL FUNCTION
        INPUTS: INTEGER N
        OUTPUTS: INTEGER
        '''
        return functools.reduce(operator.mul, range(1, n + 1))

    def convert_thousands_decimals_separator(self, number_float, precision_decimals=2,
                                             thousands_inputs='.', thousands_outputs=',',
                                             decimals_inputs=',', decimals_outputs='.'):
        '''
        DOCSTRING: CONVERTING THOUSANDS AND DECIMALS SEPARATORS
        INPUTS: NUMBER FLOAT AND NUMBER FORMAT
        OUTPUTS: NUMBER FLOAT WITH DESIRED FORMAT
        '''
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
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        return [float(x) / float_epsilon for x in range(int(float_inf * float_epsilon), 
                                                        int(float_sup * float_epsilon), 
                                                        int(float_pace * float_epsilon))]
    
    def clamp(self, n, minn, maxn):
        '''
        DOCSTRING: CONSTRICT NUMBER WITHIN RANGE - WINSORIZE
        INPUTS:
        OUTPUTS:
        '''
        return max(min(maxn, n), minn)

    def is_numeric(self, str_):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS
        '''
        try:
            float(str_)
            return True
        except ValueError:
            return False

# print(StatiscalToolkit().inv_norm(0.99))
# # output: 2.3263478740408408


# a = [[6, 3, 4, 8],
#      [3, 6, 5, 1],
#      [4, 5, 10, 7],
#      [8, 1, 7, 25]]

# l = LinearAlgebra().cholesky_decomposition(a)
# # output: [[ 2.44948974  0.          0.          0.        ]
#  [ 1.22474487  2.12132034  0.          0.        ]
#  [ 1.63299316  1.41421356  2.30940108  0.        ]
#  [ 3.26598632 -1.41421356  1.58771324  3.13249102]]

# print(l)

# u = LinearAlgebra().cholesky_decomposition(a, False)
# # output: [[ 2.44948974  0.          0.          0.        ]
#  [ 1.22474487  2.12132034  0.          0.        ]
#  [ 1.63299316  1.41421356  2.30940108  0.        ]
#  [ 3.26598632 -1.41421356  1.58771324  3.13249102]]
# [[ 2.44948974  1.22474487  1.63299316  3.26598632]
#  [ 0.          2.12132034  1.41421356 -1.41421356]
#  [ 0.          0.          2.30940108  1.58771324]
#  [ 0.          0.          0.          3.13249102]]

# print(u)

# a = [[6, 3, 4, 8],
#      [3, 6, 5, 1],
#      [4, 5, 10, 7],
#      [8, 1, 7, 25]]

# print(LinearAlgebra().eigenvalue_eigenarray(a))

# # output

# (array([31.48923716+0.j, 11.01742013+0.j,  1.25941947+0.j,  3.23392324+0.j]), array([[ 0.34254383,  0.15773425,  0.61341024,  0.6939103 ],
#        [ 0.14709227,  0.62461793, -0.66834122,  0.37621313],
#        [ 0.37453336,  0.62134938,  0.32021262, -0.60919094],
#        [ 0.84897135, -0.44597904, -0.27296843, -0.0764106 ]]))

# a = [[7, 14, -2],
#      [-3, -10, 2],
#      [-12, -28, 5]]

# print(LinearAlgebra().power_matrix(a, 3))

# # output
# [[ 127  182  -14]
#  [ -63 -118   14]
#  [-252 -364   29]]

# a = [[7, 14, -2],
#      [-3, -10, 2],
#      [-12, -28, 5]]

# print(LinearAlgebra().sqrt_matrix(a, 3))

# # output
# [[ 3. +1.05076175e-15j  4. -3.46410162e+00j -0.5+8.66025404e-01j]
#  [-1. -1.05076175e-15j -2. +3.46410162e+00j  0.5-8.66025404e-01j]
#  [-4. -2.10152351e-15j -8. +6.92820323e+00j  2. -1.73205081e+00j]]

# print(NumHandler().decimal_to_fraction(2.25))

# # output
# 9/4

# a = [9, 3, 27]

# print(StatiscalToolkit().statistical_description(a))

# # output
# DescribeResult(nobs=3, minmax=(3, 27), mean=13.0, variance=156.0,
#                skewness=0.5280049792181878, kurtosis=-1.5)

# a = [1, 4, 5, 6, 8, 10, 12, 14, 15]
# b = [28.13, 31.87, 26.28, 21.05, 47.02, 41.87, 44.83, 52.94, 55.11]
# print(StatiscalToolkit().linear_regression(a, b))

# a = [1, 4, 5, 6, 8, 10, 12, 14, 15]
# b = [28.13, 31.87, 26.28, 21.05, 47.02, 41.87, 44.83, 52.94, 55.11]
# print(StatiscalToolkit().linear_regression(a, b).slope)

# a = [1, 2, 5, 4, 8, 9, 12]
# print(StatiscalToolkit().standard_deviation_sample(a))

# # output
# 3.9761191895520196

# a = [[0.6, 0.1, 0],
#      [0.3, 0.7, 0],
#      [0.1, 0.2, 1]]
# print(LinearAlgebra().eigenvalue_eigenarray(a))

# # output
# (array([1.        +0.j, 0.83027756+0.j, 0.46972244+0.j]), array([[ 0.        ,  0.24104363,  0.59880267],
#        [ 0.        ,  0.55506939, -0.78010553],
#        [ 1.        , -0.79611302,  0.18130286]]))

# a = [1, 5, 3, 4]
# b = [103, 8, 6, 3]
# print(StatiscalToolkit().covariance(a, b))

# # outputs
# -72.33333333333333

# a = [1, 5, 3, 4]
# b = [103, 8, 6, 3]
# print(StatiscalToolkit().correlation(a, b))

# # output
# -0.8695142734017307

# a = [907, 926, 506, 741, 789, 889, 874, 510, 529, 420, 679, 872, 924, 607, 452, 729, 794,
#      844, 1010, 621]
# b = [9.43, 11.05, 6.84, 9.21, 9.42, 8.80, 7.07, 6.73, 9.56, 9.20, 7.63, 6.43, 9.46,
#      7.64, 5.32, 8.95, 9.33, 10.23, 9.86, 7.41]
# print(StatiscalToolkit().covariance(a, a))
# print(StatiscalToolkit().covariance(a, b))
# print(np.cov(np.array(a), np.array(b), rowvar=False)[-1, :-1][0])
# print(StatiscalToolkit().covariance(b, b))

# # output
# 32347.502631578944
# 135.5760263157895
# 2.2148976315789475
# 135.57602631578948

# a = [907, 926, 506, 741, 789, 889, 874, 510, 529, 420, 679, 872, 924, 607, 452, 729, 794,
#      844, 1010, 621]
# b = [9.43, 11.05, 6.84, 9.21, 9.42, 8.80, 7.07, 6.73, 9.56, 9.20, 7.63, 6.43, 9.46,
#      7.64, 5.32, 8.95, 9.33, 10.23, 9.86, 7.41]
# print(StatiscalToolkit().correlation(a, b))

# # output
# 0.5065072812924544

# a = [1, 4, 5, 6, 8, 10, 12, 14, 15]
# b = [28.13, 31.87, 26.28, 21.05, 47.02, 41.87, 44.83, 52.94, 55.11]
# print(StatiscalToolkit().linear_regression(a, b))
# alfa = StatiscalToolkit().linear_regression(a, b).intercept
# beta = StatiscalToolkit().linear_regression(a, b).slope
# r2 = pow(StatiscalToolkit().linear_regression(a, b).rvalue, 2)
# print('ALFA: {}, BETA: {}, R2: {}'.format(alfa, beta, r2))

# a = [[7, 14, -2],
#      [-3, -10, 2],
#      [-12, -28, 5]]

# print(LinearAlgebra().eigenvalue_eigenarray(a))

# # output
# (array([ 4.+0.j,  1.+0.j, -3.+0.j]), array([[ 0.43643578,  0.23570226,  0.40824829],
#        [-0.21821789, -0.23570226, -0.40824829],
#        [-0.87287156, -0.94280904, -0.81649658]]))

# a = [1, 4, 5, 6, 8, 10, 12, 14, 15]
# b = [28.13, 31.87, 26.28, 21.05, 47.02, 41.87, 44.83, 52.94, 55.11]
# print(StatiscalToolkit().linear_regression(a, b))
# LinregressResult(slope=2.224029304029304, intercept=20.255311355311356, rvalue=0.8614827909709815, pvalue=0.002836723777375444, stderr=0.49548014711984273)

# y = [94, 63, 29, 82, 36, 50]
# x = [[80, 77],
#      [10, 84],
#      [14, 21],
#      [68, 86],
#      [47, 49],
#      [37, 46]]
# print(StatiscalToolkit().multiple_linear_regression(x, y))

# # output
# {'score': 0.860446198078443, 'coeficients': array([0.37209869, 0.65749468]), 'intercept': 3.3453612194300817, 'predict': array([83.74034665, 62.29590099, 22.36213111, 85.19261444, 53.05123888,
#        47.35776793])}

# y = [94, 63, 29, 82, 36, 50]
# x = [[80, 77],
#      [10, 84],
#      [14, 21],
#      [68, 86],
#      [47, 49],
#      [37, 46]]

# print(StatiscalToolkit().anova(x, y, 'json'))

# less than 8 observations; 6 samples were given.
#   warn("omni_normtest is not valid with less than 8 observations; %i "
#                             OLS Regression Results
# ==============================================================================
# Dep. Variable:                      y   R-squared:                       0.860
# Model:                            OLS   Adj. R-squared:                  0.767
# Method:                 Least Squares   F-statistic:                     9.249
# Date:                Mon, 24 Aug 2020   Prob (F-statistic):             0.0521
# Time:                        22:08:44   Log-Likelihood:                -21.517
# No. Observations:                   6   AIC:                             49.03
# Df Residuals:                       3   BIC:                             48.41
# Df Model:                           2
# Covariance Type:            nonrobust
# ==============================================================================
#                  coef    std err          t      P>|t|      [0.025      0.975]
# ------------------------------------------------------------------------------
# const          3.3454     14.102      0.237      0.828     -41.535      48.225
# x1             0.3721      0.220      1.694      0.189      -0.327       1.071
# x2             0.6575      0.238      2.758      0.070      -0.101       1.416
# ==============================================================================
# Omnibus:                          nan   Durbin-Watson:                   1.754
# Prob(Omnibus):                    nan   Jarque-Bera (JB):                0.820
# Skew:                          -0.900   Prob(JB):                        0.663
# Kurtosis:                       2.798   Cond. No.                         223.
# ==============================================================================

# Warnings:
# [1] Standard Errors assume that the covariance matrix of the errors is correctly specified.

# params for reshape: -1 to get as many rows as needed and 1 to get one column
# x = np.arange(10).reshape(-1, 1)
# y = np.array([0, 0, 0, 0, 1, 1, 1, 1, 1, 1])
# print(StatiscalToolkit().logistic_regrression_logit(x, y))
# {'fit': LogisticRegression(multi_class='ovr', random_state=0, solver='liblinear'), 'classes': array([0, 1]), 'intercept': array([-1.04608067]), 'coeficient': array([[0.51491375]]), 'predict_probability': array([[0.74002157, 0.25997843],
#        [0.62975524, 0.37024476],
#        [0.5040632 , 0.4959368 ],
#        [0.37785549, 0.62214451],
#        [0.26628093, 0.73371907],
#        [0.17821501, 0.82178499],
#        [0.11472079, 0.88527921],
#        [0.07186982, 0.92813018],
#        [0.04422513, 0.95577487],
#        [0.02690569, 0.97309431]]), 'predictions': array([0, 0, 0, 1, 1, 1, 1, 1, 1, 1]), 'score': 0.9, 'confusion_matrix': array([[3, 1],
#        [0, 6]], dtype=int64), 'classification_report': {'0': {'precision': 1.0, 'recall': 0.75, 'f1-score': 0.8571428571428571, 'support': 4}, '1': {'precision': 0.8571428571428571, 'recall': 1.0, 'f1-score': 0.923076923076923, 'support': 6}, 'accuracy': 0.9, 'macro avg': {'precision': 0.9285714285714286, 'recall': 0.875, 'f1-score': 0.8901098901098901, 'support': 10}, 'weighted avg': {'precision': 0.9142857142857143, 'recall': 0.9, 'f1-score': 0.8967032967032967, 'support': 10}}}

# print(StatiscalToolkit().geometric_distribution(0.2, 12))
# # output
# {'mean': array([ 5.        ,  6.25      ,  7.8125    ,  9.765625  , 12.20703125,
#        15.25878906, 19.07348633, 23.84185791, 29.80232239, 37.25290298,
#        46.56612873, 58.20766091]), 'var': array([  20.        ,   32.8125    ,   53.22265625,   85.60180664,
#         136.80458069,  217.57185459,  344.72439438,  544.5923307 ,
#         858.37609731, 1350.5258778 , 2121.83821624, 3329.9241281 ]), 'skew': array([2.01246118, 2.00760459, 2.00469174, 2.00291837, 2.00182659,
#        2.00114872, 2.00072509, 2.00045901, 2.00029123, 2.0001851 ,
#        2.00011782, 2.00007508]), 'kurt': array([6.05      , 6.03047619, 6.01878899, 6.011682  , 6.0073097 ,
#        6.00459618, 6.00290087, 6.00183624, 6.00116499, 6.00074045,
#        6.00047129, 6.00030031]), 'distribution': array([0.2       , 0.16      , 0.128     , 0.1024    , 0.08192   ,
#        0.065536  , 0.0524288 , 0.04194304, 0.03355443, 0.02684355,
#        0.02147484, 0.01717987])}

# print(StatiscalToolkit().binomial_distribution(0.4, 5))

# print(StatiscalToolkit().bernoulli_distribution(0.4, 3))
# # output
# {'mean': array(0.4), 'var': array(0.24), 'skew': array(0.40824829), 'kurt': array(-1.83333333), 'distribution': 1.0}

# print(StatiscalToolkit().geometric_distribution(0.5, 12))
# # output
# {'stats': (array([2.000e+00, 4.000e+00, 8.000e+00, 1.600e+01, 3.200e+01, 6.400e+01,
#        1.280e+02, 2.560e+02, 5.120e+02, 1.024e+03, 2.048e+03, 4.096e+03]), array([2.000000e+00, 1.200000e+01, 5.600000e+01, 2.400000e+02,
#        9.920000e+02, 4.032000e+03, 1.625600e+04, 6.528000e+04,
#        2.616320e+05, 1.047552e+06, 4.192256e+06, 1.677312e+07]), array([2.12132034, 2.02072594, 2.00445931, 2.0010414 , 2.000252  ,
#        2.000062  , 2.00001538, 2.00000383, 2.00000096, 2.00000024,
#        2.00000006, 2.00000001]), array([6.5       , 6.08333333, 6.01785714, 6.00416667, 6.00100806,
#        6.00024802, 6.00006152, 6.00001532, 6.00000382, 6.00000095,
#        6.00000024, 6.00000006])), 'distribution': array([5.00000000e-01, 2.50000000e-01, 1.25000000e-01, 6.25000000e-02,
#        3.12500000e-02, 1.56250000e-02, 7.81250000e-03, 3.90625000e-03,
#        1.95312500e-03, 9.76562500e-04, 4.88281250e-04, 2.44140625e-04])}

# print(StatiscalToolkit().binomial_distribution(0.4, 5))
# # output
# {'mean': array([1.296 , 1.728 , 1.152 , 0.384 , 0.0512]),
#     'var': array([0.9600768 , 1.1308032 , 0.8865792 , 0.3545088 , 0.05067571]),
#     'skew': array([0.49151128, 0.29039142, 0.57265244, 1.42155002, 4.35124341]),
#     'kurt': array([-0.15841666, -0.31567282, -0.07206918,  1.62080445, 18.53331919]),
#     'distribution': array([0.2592 , 0.3456 , 0.2304 , 0.0768 , 0.01024])}

# print(StatiscalToolkit().poisson_distribution(12, 0.6))
# # output
# {'mean': array(0.6), 'var': array(0.6), 'skew': array(1.29099445),
#     'kurt': array(1.66666667),
#     'distribution': array([3.29286982e-01, 9.87860945e-02, 1.97572189e-02, 2.96358283e-03,
#        3.55629940e-04, 3.55629940e-05, 3.04825663e-06, 2.28619247e-07,
#        1.52412832e-08, 9.14476989e-10, 4.98805630e-11, 2.49402815e-12])}

# print(StatiscalToolkit().inv_norm(0.95, 15, 6))
# # output
# 24.869121761708833

# a = [2, 33, 456, 4, 2, 1, 5, 85, 89, 9, 12]
# print('DESCRIÇÃO VETOR A: {}'.format(
#     StatiscalToolkit().statistical_description(a)))
# # output
# DESCRIÇÃO VETOR A: {'nobs': 11, 'minmax': (1, 456), 'mean': 63.45454545454545,
#                     'variance_sample': 18015.472727272725,
#                     'standard_deviation_sample': 134.22172971345856,
#                     'skewness': 2.5621410960814357, 'kurtosis': 5.076384966234089}
# print(StatiscalToolkit().standard_deviation_sample(a))
# output
# 134.22172971345856

# a = [1.42738, 1.52229, 1.69742, 1.90642, 1.98492,
#      1.99568, 2.10288, 2.22488, 2.61826, 3.15435]
# print(StatiscalToolkit().kolmogorov_smirnov_test(a))
# {'dn': array(0.17709753), 'critical_value': 0.4092461395823647, 'reject_h0': False}

# a = [-15, 15, 9, 99, 1037, 2]
# print(StatiscalToolkit().kolmogorov_smirnov_test(a))
# {'dn': array(0.47891788), 'critical_value': 0.5192619542681385, 'reject_h0': False}

# print(StatiscalToolkit().cdf(1.42738, 2.0634, 0.5156))
# # output
# 0.10868473142780599

# print(np.log(StatiscalToolkit().cdf(1.42738, 2.0634, 0.5156)))
# # output
# -2.219303959966715

# print(np.log(1 - StatiscalToolkit().cdf(1.42738, 2.0634, 0.5156)))
# # output
# -0.11505707731543621

# a = [1.42738, 1.52229, 1.69742, 1.90642, 1.98492,
#      1.99568, 2.10288, 2.22488, 2.61826, 3.15435]
# print(StatiscalToolkit().anderson_darling(a))
# # output
# {'alpha': 0.05, 'p-value': 0.41437355423908495, 'reject_h0': False}

# a = [1.42738, 1.52229, 1.69742, 1.90642, 1.98492,
#      1.99568, 2.10288, 2.22488, 2.61826, 3.15435]
# print(StatiscalToolkit().shapiro_wilk(a))

# a = [1.42738, 1.52229, 1.69742, 1.90642, 1.98492,
#      1.99568, 2.10288, 2.22488, 2.61826, 3.15435]
# print(StatiscalToolkit().test_adherence_normal_distribution(a))
# # output
# {'kolmogorov_smirnov': {'dn': array(0.17709753), 'critical_value': 0.4092461395823647, 'reject_h0': False}, 'anderson_darling': {'alpha': 0.05, 'p-value': 0.41437355423908495, 'reject_h0': False}, 'shapiro_wilk': {'alpha': 0.05, 'p-value': 0.41617822647094727, 'reject_h0': False}}

# print(StatiscalToolkit().confidence_interval_normal([25, 30, 28], 0.95))
# {'mean': 27.666666666666668, 'inferior_inteval': 21.415057187055783,
#     'superior_interval': 33.91827614627755}

# a = [56, 37, 23, 22, 11, 16, 12, 8, 11]
# print(StatiscalToolkit().bendford_law(a, True))
# # output
# {'benford_expected_array': array([0.30103   , 0.17609126, 0.12493874, 0.09691001, 0.07918125,
#        0.06694679, 0.05799195, 0.05115252, 0.04575749]), 'real_numbers_observed_array': array([0.28571429, 0.18877551, 0.11734694, 0.1122449 , 0.05612245,
#        0.08163265, 0.06122449, 0.04081633, 0.05612245])}

# print(np.multiply(np.array([2.4, 8.7, 3.15]), np.array([5.7, 9.4, 2.13])))
# output
# [13.68   81.78    6.7095]

# print(np.array([2.4, 8.7, 3.15]) *
#       np.array([5.7, 9.4, 2.13]) / np.array([8.14, 9.37, 10.53]))
# # output
# [1.68058968 8.72785486 0.63717949]

# print(sum([np.array([2.4, 8.7, 3.15]),
#            np.array([5.7, 9.4, 2.13]), np.array([8.14, 9.37, 10.53])]))

# dict_teste = {
#     '1': np.array([2.4, 8.7, 3.15]),
#     '2': np.array([5.7, 9.4, 2.13]),
#     '3': np.array([8.14, 9.37, 10.53])
# }

# print(sum(dict_teste.values()))
