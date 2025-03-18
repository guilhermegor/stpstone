### PROBABILITY DISTRIBUTIONS ###

from __future__ import print_function, division
import numpy as np
import matplotlib.pylab as plt
import seaborn as sns
from scipy.special import gamma, gammaln
from scipy.stats import t, uniform, chi2, f, bernoulli, geom, binom, poisson, sem, norm
from numpy import shape, dot, ones, multiply, pi, sqrt, log


class ProbabilityDistributions:

    def bernoulli_distribution(self, prob, num_trials=1):
        """
        REFERENCES: https://docs.scipy.org/doc/scipy-0.14.0/reference/generated/scipy.stats.bernoulli.html
        DOCSTRING: BERNOULLI DISTRIBUTION TO ANALYZE PERCENTAGE OF ACCOMPLISHMENT AND FAILURE
            FOR EACH EVENT: P(x=1) = P, E(X) = P, V(X) = P * (1-P)
        INPUTS: PROBABILITY AND NUMBER OF TRIALS
        OUTPUTS: ARRAY OF BERNOULLI DISTRIBUTION (MEAN, VAR, SKEW, KURT AND
            CUMULATIVE DISTRIBUTION FUNCTION)
        """
        return {
            'mean': bernoulli.stats(prob, moments='mvsk')[0],
            'var': bernoulli.stats(prob, moments='mvsk')[1],
            'skew': bernoulli.stats(prob, moments='mvsk')[2],
            'kurt': bernoulli.stats(prob, moments='mvsk')[3],
            'distribution': bernoulli.cdf(num_trials, prob)
        }

    def geometric_distribution(self, prob, num_trials):
        """
        REFERENCES: http://biorpy.blogspot.com/2015/02/py19-geometric-distribution-in-python.html
        DOCSTRING: GEOMETRIC DISTRIBUTION TO INDICATE NUMBER OF INDEPENDENT TRIALS TO REACH
            FIRST SUCCESS: P(X=N) = (1-P) ** (N-1) * P, E(X) = 1/P, V(X) = (1-P) / P ** 2
        INPUTS: PROBABILITY (FLOAT) AND NUMBER OF TRIALS
        OUTPUTS: DICT OF GEOMETRIC DISTRIBUTION (MEAN, VAR, SKEW, KURT AND
            CUMULATIVE DISTRIBUTION FUNCTION)
        """
        p = np.zeros(num_trials)
        for k in range(1, num_trials + 1):
            p[k - 1] = geom.pmf(k, prob)
        return {
            'mean': geom.stats(p, moments='mvsk')[0],
            'var': geom.stats(p, moments='mvsk')[1],
            'skew': geom.stats(p, moments='mvsk')[2],
            'kurt': geom.stats(p, moments='mvsk')[3],
            'distribution': p
        }

    def binomial_distribution(self, prob, num_trials):
        """
        REFERENCES: https://docs.scipy.org/doc/scipy-0.14.0/reference/generated/scipy.stats.binom.html
        DOCSTRING: BINOMIAL DISRTIBUTION TO INVESTIGATE K-NUMBER OF SUCCESSES IN N-TRIALS:
            P(X=K) = COMB(N,K) * P ** K * (1 - P) ** (N-K), E(X) = N * P, V (X) = N * P * (1-P)
        INPUTS: PROBABILITY (FLOAT) AND NUMBER OF TRIALS
        OUTPUTS: DICT OF BINOMIAL DISTRIBUTION (MEAN, VAR, SKEW, KURT AND
            CUMULATIVE DISTRIBUTION FUNCTION)
        """
        p = np.zeros(num_trials)
        for k in range(1, num_trials + 1):
            p[k - 1] = binom.pmf(k, num_trials, prob)
        return {
            'mean': binom.stats(num_trials, p, moments='mvsk')[0],
            'var': binom.stats(num_trials, p, moments='mvsk')[1],
            'skew': binom.stats(num_trials, p, moments='mvsk')[2],
            'kurt': binom.stats(num_trials, p, moments='mvsk')[3],
            'distribution': p
        }

    def poisson_distribution(self, num_trials, mu):
        """
        REFERENCES: https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.poisson.html
        DOCSTRING: POISSON DISTRIBUTION TO COUNT OCCURRENCES WITHIN CERTAIN AMOUNT OF TIME, ASSUMING
            A LAMBDA MEAN:
            P(X=K) = EXP(-LAMBDA) * LAMBA ** K / K!, E(x) = V(X) = LAMBDA * RANGE
        INPUTS: NUMBER OF TRIALS AND MU
        OUTPUTS: DICT OF POISSON DISTRIBUTION (MEAN, VAR, SKEW, KURT AND
            CUMULATIVE DISTRIBUTION FUNCTION)
        """
        p = np.zeros(num_trials)
        for k in range(1, num_trials + 1):
            p[k - 1] = poisson.pmf(k, mu)
        return {
            'mean': poisson.stats(mu, moments='mvsk')[0],
            'var': poisson.stats(mu, moments='mvsk')[1],
            'skew': poisson.stats(mu, moments='mvsk')[2],
            'kurt': poisson.stats(mu, moments='mvsk')[3],
            'distribution': p
        }

    def chi_squared(self, p, df, probability_func='ppf', x_axis_inf_range=None,
                    x_axis_sup_range=None, x_axis_pace=None):
        """
        DOCSTRING: CHI SQUARED PROBABILITY POINT FUNCTION (Z-SCORE, OR PPF), PROBABABILITY
            DENSITY FUNCTION (PDF), AND PROBABILITY CUMULATIVE FUNCTION (CDF)
        INPUTS: P (PROBABILITY), DEGREES OF FREEDOM, PROBABILITY FUNCTION (PPF, AS DEFAULT, WHEREAS
            PDF AND CDF ARE POSSIBLE AS WELL)
        OUTPUTS: FLOAT
        """
        # setting x axis range
        arr_ind = np.arange(x_axis_inf_range, x_axis_sup_range, x_axis_pace)
        # getting the statistic
        if probability_func == 'ppf':
            return chi2.ppf(p, df)
        elif probability_func == 'pdf':
            return chi2.pdf(arr_ind, df)
        elif probability_func == 'cdf':
            return chi2.cdf(arr_ind, df)
        else:
            raise Exception('Error defining the probability function of interest. {} '.format(
                probability_func)
                + 'was given, nevertheless was expected ppf, pdf or cdf')

    def t_student(self, p, df, probability_func='ppf', x_axis_inf_range=None,
                  x_axis_sup_range=None, x_axis_pace=None):
        """
        DOCSTRING: T STUDENT PROBABILITY POINT FUNCTION (Z-SCORE, OR PPF), PROBABABILITY
            DENSITY FUNCTION (PDF), AND PROBABILITY CUMULATIVE FUNCTION (CDF)
        INPUTS: P (PROBABILITY), DEGREES OF FREEDOM, PROBABILITY FUNCTION (PPF, AS DEFAULT, WHEREAS
            PDF AND CDF ARE POSSIBLE AS WELL)
        OUTPUTS: FLOAT
        """
        # setting x axis range
        arr_ind = np.arange(x_axis_inf_range, x_axis_sup_range, x_axis_pace)
        # getting the statistic
        if probability_func == 'ppf':
            return t.ppf(p, df)
        elif probability_func == 'pdf':
            return t.pdf(arr_ind, df)
        elif probability_func == 'cdf':
            return t.cdf(arr_ind, df)
        else:
            raise Exception('Error defining the probability function of interest. {} '.format(
                probability_func)
                + 'was given, nevertheless was expected ppf, pdf or cdf')

    def f_fisher_snedecor(self, dfn, dfd, mu, p=None, probability_func='ppf', x_axis_inf_range=None,
                          x_axis_sup_range=None, x_axis_pace=None):
        """
        DOCSTRING: F-SNEDECOR PROBABILITY POINT FUNCTION (Z-SCORE, OR PPF), PROBABABILITY
            DENSITY FUNCTION (PDF), AND PROBABILITY CUMULATIVE FUNCTION (CDF)
        INPUTS: P (PROBABILITY), DEGREES OF FREEDOM, PROBABILITY FUNCTION (PPF, AS DEFAULT, WHEREAS
            PDF AND CDF ARE POSSIBLE AS WELL)
        OUTPUTS: FLOAT
        """
        # checking wheter degrees of freedom numerator is higher than denominator
        assert dfn > dfd
        # defining basic parameters of the distribution
        f_dist = f(dfn, dfd, mu)
        # setting x axis range
        arr_ind = np.arange(x_axis_inf_range, x_axis_sup_range, x_axis_pace)
        # getting the statistic
        if probability_func == 'ppf':
            return f.ppf(p, dfn, dfd)
        elif probability_func == 'pdf':
            return f_dist.pdf(arr_ind)
        elif probability_func == 'cdf':
            return f.cdf(p, dfn, dfd)
        else:
            raise Exception('Error defining the probability function of interest. {} '.format(
                probability_func)
                + 'was given, nevertheless was expected ppf, pdf or cdf')


class NormalDistribution:

    def phi(self, x):
        """
        DOCSTRING: RETURN THE VALUE OF THE GAUSSIAN PROBABILITY FUNCTION WITH MEAN 0.0 AND
            STANDARD DEVIATION 1.0 AT THE GIVEN X VALUE
        INPUTS: X
        OUTPUS: STANDARD NORMAL PROBABILITY
        """
        return np.exp(-x ** 2 / 2.0) / np.sqrt(2.0 * np.pi)

    def pdf(self, x, mu=0.0, sigma=1.0):
        """
        DOCSTRING: RETURN THE VALUE OF THE GAUSSIAN PROBABILITY FUNCTION WITH MEAN MU AND
            STANDARD DEVIATION SIGMA AT THE GIVEN x VALUE
        INPUTS: X, MU (0.0 BY DEFAULT) AND SIGMA (1.0 BY DEFAULT)
        OUTPUTS: VALUE OF THE GAUSSIAN PROBABILITY
        """
        return self.phi((x - mu) / sigma) / sigma

    def cumnulative_phi(self, z):
        """
        DOCSTRING: DENSITY FUNCTION WITH MEAN 0.0 AND STANDARD DEVIATION 1.0 AT THE GIVEN Z VALUE
        INPUTS: Z
        OUTPUTS: PHI
        """
        if z < -8.0:
            return 0.0
        if z > 8.0:
            return 1.0
        total = 0.0
        term = z
        i = 3
        while total != total + term:
            total += term
            term *= z * z / float(i)
            i += 2
        return 0.5 + total * self.phi(z)

    def cdf(self, x, mu=0.0, sigma=1.0):
        """
        DOCSTRING: STANDARD GAUSSIAN CDF WITH MEAN MI AND STDDEV SIGMA, USING TAYLOR
            APPROXIMATION - CUMULATIVE DISTRIBUTION FUNCTION - AREA BELOW GAUSSIAN CURVE -
            NORMAL DISTRIBUTION FORMULA
        INPUTS: X, MU(STANDARD 0.0) AND SIGMA (STANDARD 1.0)
        OUTPUTS: CUMULATIVE DENSITY FUNCTION OF A GAUSSIAN DISTRIBUTION
        """
        return self.cumnulative_phi((x - mu) / sigma)

    def inv_cdf(self, p, mu=0.0, sigma=1.0):
        """
        DOCSTRING: INVERSE OF THE NORMAL CULMULATIVE DISTRIBUTION FOR A SUPPLIED VALUE OF X, OR
            A PROBABILITY, WITH A GIVEN DISTRIBUTION MEAND AND STANDARD DEVIATION
        INPUTS: PROBABILITY, MEAN AND STANDARD DEVIATION
        OUTPUTS: INV.NORM, OR Z-SCORE
        """
        return norm.ppf(p, mu, sigma)

    def confidence_interval_normal(self, data, confidence=0.95):
        """
        REFERENCE: https://stackoverflow.com/questions/15033511/compute-a-confidence-interval-from-sample-data
        DOCSTRING: CONFIDECENCE INTERVAL FOR A NORMAL DISTRIBUTION
        INPUTS: DATA AND CONFIDENCE
        OUTPUTS: DICTIONARY (MEAN, INFERIOR AND SUPERIOR INTERVALS)
        """
        a = 1.0 * np.array(data)
        n = len(a)
        mu, se = np.mean(a), sem(a)
        z = se * t.ppf((1 + confidence) / 2., n - 1)
        return {
            'mean': mu,
            'inferior_inteval': mu - z,
            'superior_interval': mu + z
        }

    def ecdf(self, data):
        """
        REFERENCES: https://campus.datacamp.com/courses/statistical-thinking-in-python-part-1/graphical-exploratory-data-analysis?ex=12
        DOCSTRING: COMPUTE ECDF FOR A ONE-DIMENSIONAL ARRAY OF MEASUREMENTS AN EMPIRICAL
            CUMULATIVE DISTRIBUTION FUNCTION (ECDF)
        INPUTS: DATA
        OUTPUTS: X-AXIS AND Y-AXIS
        """
        # number of data points: n
        n = len(data)
        # x-data for the ECDF: x
        x = np.sort(data)
        # y-data for the ECDF: y
        y = np.arange(1, n + 1) / n
        return x, y


class HansenSkewStudent(object):

    """Skewed Student distribution class - This is the version introduced by Bruce E. Hansen in 1994.
    References: https://www.ssc.wisc.edu/~bhansen/papers/ier_94.pdf
    Attributes
    ----------
    eta : float
        Degrees of freedom. :math:`2 < \eta < \infty`
    lam : float
        Skewness. :math:`-1 < \lambda < 1`
    Methods
    -------
    pdf
        Probability density function (PDF)
    cdf
        Cumulative density function (CDF)
    ppf
        Inverse cumulative density function (ICDF)
    rvs
        Random variates with mean zero and unit variance
    """

    def __init__(self, eta=10., lam=-.1):
        """Initialize the class.
        Parameters
        ----------
        eta : float
            Degrees of freedom. :math:`2 < \eta < \infty`
        lam : float
            Skewness. :math:`-1 < \lambda < 1`
        """
        self.eta = eta
        self.lam = lam

    @property
    def const_a(self):
        """Compute a constant.
        Returns
        -------
        a : float
        """
        return 4 * self.lam * self.const_c() * (self.eta - 2) / (self.eta - 1)

    @property
    def const_b(self):
        """Compute b constant.
        Returns
        -------
        b : float
        """
        return (1 + 3 * self.lam**2 - self.const_a**2)**.5

    def const_c(self):
        """Compute c constant.
        Returns
        -------
        c : float
        """
        return gamma((self.eta + 1) / 2) \
            / ((np.pi * (self.eta - 2))**.5 * gamma(self.eta / 2))

    def pdf(self, arg):
        """Probability density function (PDF).
        Parameters
        ----------
        arg : array
            Grid of point to evaluate PDF at
        Returns
        -------
        array
            PDF values. Same shape as the input.
        """
        c = self.const_c()
        a = self.const_a
        b = self.const_b

        return b * c * (1 + 1 / (self.eta - 2) * ((b * arg + a) / (1 + np.sign(
            arg + a / b) * self.lam))**2)**(-(self.eta + 1) / 2)

    def cdf(self, arg):
        """Cumulative density function (CDF).
        Parameters
        ----------
        arg : array
            Grid of point to evaluate CDF at
        Returns
        -------
        array
            CDF values. Same shape as the input.
        """
        a = self.const_a
        b = self.const_b

        y = (b * arg + a) / (1 + np.sign(arg + a / b) * self.lam) * (
            1 - 2 / self.eta)**(-.5)
        cond = arg < -a / b

        return cond * (1 - self.lam) * t.cdf(y, self.eta) \
            + ~cond * (-self.lam + (1 + self.lam) * t.cdf(y, self.eta))

    def ppf(self, arg):
        """Inverse cumulative density function (ICDF).
        Parameters
        ----------
        arg : array
            Grid of point to evaluate ICDF at. Must belong to (0, 1)
        Returns
        -------
        array
            ICDF values. Same shape as the input.
        """
        arg = np.atleast_1d(arg)

        a = self.const_a
        b = self.const_b

        cond = arg < (1 - self.lam) / 2

        ppf1 = t.ppf(arg / (1 - self.lam), self.eta)
        ppf2 = t.ppf(.5 + (arg - (1 - self.lam) / 2) /
                     (1 + self.lam), self.eta)
        ppf = -999.99 * np.ones_like(arg)
        ppf = np.nan_to_num(ppf1) * cond \
            + np.nan_to_num(ppf2) * np.logical_not(cond)
        ppf = (ppf * (1 + np.sign(arg - (1 - self.lam) / 2) * self.lam) * (
            1 - 2 / self.eta)**.5 - a) / b

        if ppf.shape == (1, ):
            return float(ppf)
        else:
            return ppf

    def rvs(self, size=1):
        """Random variates with mean zero and unit variance.
        Parameters
        ----------
        size : int or tuple
            Size of output array
        Returns
        -------
        array
            Array of random variates
        """
        return self.ppf(uniform.rvs(size=size))

    def plot_pdf(self, arg=np.linspace(-2, 2, 100)):
        """Plot probability density function.
        Parameters
        ----------
        arg : array
            Grid of point to evaluate PDF at
        """
        scale = (self.eta / (self.eta - 2))**.5
        plt.plot(arg, t.pdf(arg, self.eta, scale=1 / scale),
                 label='t distribution')
        plt.plot(arg, self.pdf(arg), label='skew-t distribution')
        plt.legend()
        plt.show()

    def plot_cdf(self, arg=np.linspace(-2, 2, 100)):
        """Plot cumulative density function.
        Parameters
        ----------
        arg : array
            Grid of point to evaluate CDF at
        """
        scale = (self.eta / (self.eta - 2))**.5
        plt.plot(arg, t.cdf(arg, self.eta, scale=1 / scale),
                 label='t distribution')
        plt.plot(arg, self.cdf(arg), label='skew-t distribution')
        plt.legend()
        plt.show()

    def plot_ppf(self, arg=np.linspace(.01, .99, 100)):
        """Plot inverse cumulative density function.
        Parameters
        ----------
        arg : array
            Grid of point to evaluate ICDF at
        """
        scale = (self.eta / (self.eta - 2))**.5
        plt.plot(arg, t.ppf(arg, self.eta, scale=1 / scale),
                 label='t distribution')
        plt.plot(arg, self.ppf(arg), label='skew-t distribution')
        plt.legend()
        plt.show()

    def plot_rvspdf(self, arg=np.linspace(-2, 2, 100), size=1000):
        """Plot kernel density estimate of a random sample.
        Parameters
        ----------
        arg : array
            Grid of point to evaluate ICDF at. Must belong to (0, 1)
        """
        rvs = self.rvs(size=size)
        xrange = [arg.min(), arg.max()]
        sns.kdeplot(rvs, clip=xrange, label='kernel')
        plt.plot(arg, self.pdf(arg), label='true pdf')
        plt.xlim(xrange)
        plt.legend()
        plt.show()

    def loglikelihood(theta=None, x=None):
        nu = theta[0]

        lambda_ = theta[1]

        c = gamma((nu + 1) / 2) / \
            (multiply(sqrt(dot(pi, (nu - 2))), gamma(nu / 2)))

        a = multiply(multiply(dot(4, lambda_), c), ((nu - 2) / (nu - 1)))

        b = sqrt(1 + dot(3, lambda_ ** 2) - a ** 2)

        logc = gammaln((nu + 1) / 2) - gammaln(nu / 2) - \
            dot(0.5, log(dot(pi, (nu - 2))))

        logb = dot(0.5, log(1 + dot(3, lambda_ ** 2) - a ** 2))

        find1 = (x < (- a / b))

        find2 = (x >= (- a / b))

        LL1 = logb + logc - dot((nu + 1) / 2.0, log(1 + multiply(1.0 / (nu - 2), ((
            multiply(b, x) + a) / (1 - lambda_)) ** 2)))

        LL2 = logb + logc - dot((nu + 1) / 2.0, log(1 + multiply(1.0 / (nu - 2), ((
            multiply(b, x) + a) / (1 + lambda_)) ** 2)))

        LL = sum(LL1[find1]) + sum(LL2[find2])

        LL = -LL

        return LL.sum()
