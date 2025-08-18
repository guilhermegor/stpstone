"""Module for probability distribution calculations.

This module provides implementations of various probability distributions including
Bernoulli, Geometric, Binomial, Poisson, Chi-Squared, T-Student, F-Snedecor,
Normal, and Hansen's Skewed Student distributions.
"""

from typing import Literal, Optional, TypedDict, Union

import matplotlib.pylab as plt
import numpy as np
from numpy import dot, log, multiply, pi, sqrt
from numpy.typing import NDArray
from scipy.special import gamma, gammaln
from scipy.stats import (
    bernoulli,
    binom,
    chi2,
    f,
    geom,
    norm,
    poisson,
    sem,
    t,
    uniform,
)
import seaborn as sns
from sklearn.base import BaseEstimator

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


# module-level constants for default plot ranges
DEFAULT_PDF_CDF_RANGE = np.linspace(-2, 2, 100)
DEFAULT_PPF_RANGE = np.linspace(0.01, 0.99, 100)


class ResultProbDistribution(TypedDict, metaclass=TypeChecker):
    """Typing for probability distribution results."""

    mean: float
    var: float
    skew: float
    kurt: float
    distribution: BaseEstimator


class ProbabilityDistributions(metaclass=TypeChecker):
    """Class implementing various probability distributions."""

    def bernoulli_distribution(
        self, 
        float_p: float, 
        int_num_trials: int = 1
    ) -> ResultProbDistribution:
        """Calculate Bernoulli distribution statistics.

        Parameters
        ----------
        float_p : float
            Probability of success (0 <= float_p <= 1)
        int_num_trials : int, optional
            Number of trials, by default 1

        Returns
        -------
        ResultProbDistribution
            Dictionary containing mean, variance, skewness, kurtosis and CDF

        Raises
        ------
        ValueError
            If float_p is not between 0 and 1
            If int_num_trials is less than 0

        References
        ----------
        .. [1] https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bernoulli.html
        """
        if not 0.0 <= float_p <= 1.0:
            raise ValueError(f"Probability must be between 0 and 1, got {float_p}")
        if int_num_trials < 0:
            raise ValueError("Number of trials must be greater than 0")
        
        return {
            "mean": bernoulli.stats(float_p, moments="mvsk")[0],
            "var": bernoulli.stats(float_p, moments="mvsk")[1],
            "skew": bernoulli.stats(float_p, moments="mvsk")[2],
            "kurt": bernoulli.stats(float_p, moments="mvsk")[3],
            "distribution": bernoulli.cdf(int_num_trials, float_p),
        }

    def geometric_distribution(
        self, 
        float_p: float, 
        int_num_trials: int
    ) -> ResultProbDistribution:
        """Calculate Geometric distribution statistics.

        Parameters
        ----------
        float_p : float
            Probability of success (0 <= float_p <= 1)
        int_num_trials : int
            Number of trials

        Returns
        -------
        ResultProbDistribution
            Dictionary containing mean, variance, skewness, kurtosis and PMF

        Raises
        ------
        ValueError
            If float_p is not between 0 and 1
            If int_num_trials is less than 0

        References
        ----------
        .. [1] http://biorpy.blogspot.com/2015/02/py19-geometric-distribution-in-python.html
        """
        if not 0.0 <= float_p <= 1.0:
            raise ValueError(f"Probability must be between 0 and 1, got {float_p}")
        if int_num_trials < 0:
            raise ValueError("Number of trials must be greater than 0")
        
        array_pmf = np.zeros(int_num_trials)
        for k in range(1, int_num_trials + 1):
            array_pmf[k - 1] = geom.pmf(k, float_p)
        return {
            "mean": geom.stats(float_p, moments="mvsk")[0],
            "var": geom.stats(float_p, moments="mvsk")[1],
            "skew": geom.stats(float_p, moments="mvsk")[2],
            "kurt": geom.stats(float_p, moments="mvsk")[3],
            "distribution": array_pmf,
        }

    def binomial_distribution(
        self, 
        float_p: float, 
        int_num_trials: int
    ) -> ResultProbDistribution:
        """Calculate Binomial distribution statistics.

        Parameters
        ----------
        float_p : float
            Probability of success (0 <= float_p <= 1)
        int_num_trials : int
            Number of trials

        Returns
        -------
        ResultProbDistribution
            Dictionary containing mean, variance, skewness, kurtosis and PMF

        Raises
        ------
        ValueError
            If float_p is not between 0 and 1
            If int_num_trials is less than 0

        References
        ----------
        .. [1] https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.binom.html
        """
        if not 0.0 <= float_p <= 1.0:
            raise ValueError(f"Probability must be between 0 and 1, got {float_p}")
        if int_num_trials < 0:
            raise ValueError("Number of trials must be greater than 0")
        
        array_pmf = np.zeros(int_num_trials)
        for k in range(1, int_num_trials + 1):
            array_pmf[k - 1] = binom.pmf(k, int_num_trials, float_p)
        return {
            "mean": binom.stats(int_num_trials, float_p, moments="mvsk")[0],
            "var": binom.stats(int_num_trials, float_p, moments="mvsk")[1],
            "skew": binom.stats(int_num_trials, float_p, moments="mvsk")[2],
            "kurt": binom.stats(int_num_trials, float_p, moments="mvsk")[3],
            "distribution": array_pmf,
        }

    def poisson_distribution(
        self, 
        int_num_trials: int, 
        float_mu: float
    ) -> ResultProbDistribution:
        """Calculate Poisson distribution statistics.

        Parameters
        ----------
        int_num_trials : int
            Number of trials
        float_mu : float
            Expected number of occurrences

        Returns
        -------
        ResultProbDistribution
            Dictionary containing mean, variance, skewness, kurtosis and PMF

        Raises
        ------
        ValueError
            If float_mu is not between 0 and 1
            If int_num_trials is less than 0

        References
        ----------
        .. [1] https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.poisson.html
        """
        if not 0.0 <= float_mu <= 1.0:
            raise ValueError("Mu must be between 0 and 1")
        if int_num_trials < 0:
            raise ValueError("Number of trials must be greater than 0")
        
        float_p = np.zeros(int_num_trials)
        for k in range(1, int_num_trials + 1):
            float_p[k - 1] = poisson.pmf(k, float_mu)
        return {
            "mean": poisson.stats(float_mu, moments="mvsk")[0],
            "var": poisson.stats(float_mu, moments="mvsk")[1],
            "skew": poisson.stats(float_mu, moments="mvsk")[2],
            "kurt": poisson.stats(float_mu, moments="mvsk")[3],
            "distribution": float_p,
        }

    def chi_squared(
        self,
        float_p: float,
        int_df: int,
        probability_func: Literal["ppf", "pdf", "cdf"] = "ppf",
        x_axis_inf_range: Optional[float] = None,
        x_axis_sup_range: Optional[float] = None,
        x_axis_pace: Optional[float] = None,
    ) -> Union[float, NDArray[np.float64]]:
        """Calculate Chi-Squared distribution statistics.

        Parameters
        ----------
        float_p : float
            Probability value
        int_df : int
            Degrees of freedom
        probability_func : Literal['ppf', 'pdf', 'cdf'], optional
            Function type ('ppf', 'pdf', or 'cdf'), by default 'ppf'
        x_axis_inf_range : Optional[float], optional
            Lower bound of x-axis range, by default None
        x_axis_sup_range : Optional[float], optional
            Upper bound of x-axis range, by default None
        x_axis_pace : Optional[float], optional
            Step size for x-axis range, by default None

        Returns
        -------
        Union[float, NDArray[np.float64]]
            Result of the specified probability function

        Raises
        ------
        ValueError
            If float_p is not between 0 and 1
            If int_df is less than 0
            If probability_func is not one of 'ppf', 'pdf', or 'cdf'
            If x_axis_inf_range, x_axis_sup_range, or x_axis_pace is None
        
        Notes
        -----
        ppf: percent point function
            Inverse of the CDF, is a statistical function that returns the value of a random \
variable corresponding to a specified probability level (or percentile).
        pdf: probability density function
            The PDF is the derivative of the CDF.
        cdf: cumulative density function
            The CDF is the probability that a random variable is less than or equal to a given \
value.

        References
        ----------
        .. [1] https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.chi2.html
        """
        if not 0.0 <= float_p <= 1.0:
            raise ValueError(f"Probability must be between 0 and 1, got {float_p}")
        if int_df <= 0:
            raise ValueError("Degrees of freedom must be positive")
        if probability_func not in ("ppf", "pdf", "cdf"):
            raise ValueError("probability_func must be one of 'ppf', 'pdf', or 'cdf'")
        if probability_func in ("pdf", "cdf") and (
            x_axis_inf_range is None or x_axis_sup_range is None or x_axis_pace is None
        ):
            raise ValueError("Range parameters must be provided for pdf/cdf calculations")

        if probability_func in ("pdf", "cdf"):
            arr_ind = np.arange(x_axis_inf_range, x_axis_sup_range, x_axis_pace)
            if probability_func == "pdf":
                return chi2.pdf(arr_ind, int_df)
            return chi2.cdf(arr_ind, int_df)
        elif probability_func == "ppf":
            return chi2.ppf(float_p, int_df)
        raise ValueError("probability_func must be one of 'ppf', 'pdf', or 'cdf'")

    def t_student(
        self,
        float_p: float,
        int_df: int,
        probability_func: Literal["ppf", "pdf", "cdf"] = "ppf",
        x_axis_inf_range: Optional[float] = None,
        x_axis_sup_range: Optional[float] = None,
        x_axis_pace: Optional[float] = None,
    ) -> Union[float, NDArray[np.float64]]:
        """Calculate Student's T distribution statistics.

        Parameters
        ----------
        float_p : float
            Probability value
        int_df : int
            Degrees of freedom
        probability_func : Literal['ppf', 'pdf', 'cdf'], optional
            Function type ('ppf', 'pdf', or 'cdf'), by default 'ppf'
        x_axis_inf_range : Optional[float], optional
            Lower bound of x-axis range, by default None
        x_axis_sup_range : Optional[float], optional
            Upper bound of x-axis range, by default None
        x_axis_pace : Optional[float], optional
            Step size for x-axis range, by default None

        Returns
        -------
        Union[float, NDArray[np.float64]]
            Result of the specified probability function

        Raises
        ------
        ValueError
            If float_p is not between 0 and 1
            If int_df is less than 0
            If probability_func is not one of 'ppf', 'pdf', or 'cdf'
            If x_axis_inf_range, x_axis_sup_range, or x_axis_pace is None
        
        Notes
        -----
        ppf: percent point function
            Inverse of the CDF, is a statistical function that returns the value of a random \
variable corresponding to a specified probability level (or percentile).
        pdf: probability density function
            The PDF is the derivative of the CDF.
        cdf: cumulative density function
            The CDF is the probability that a random variable is less than or equal to a given \
value.
        """
        if not 0.0 <= float_p <= 1.0:
            raise ValueError(f"Probability must be between 0 and 1, got {float_p}")
        if int_df <= 0:
            raise ValueError("Degrees of freedom must be positive")
        if probability_func not in ("ppf", "pdf", "cdf"):
            raise ValueError("probability_func must be one of 'ppf', 'pdf', or 'cdf'")
        if probability_func in ("pdf", "cdf") and (
            x_axis_inf_range is None or x_axis_sup_range is None or x_axis_pace is None
        ):
            raise ValueError("Range parameters must be provided for pdf/cdf calculations")
        
        if probability_func in ("pdf", "cdf"):
            arr_ind = np.arange(x_axis_inf_range, x_axis_sup_range, x_axis_pace)
            if probability_func == "pdf":
                return t.pdf(arr_ind, int_df)
            return t.cdf(arr_ind, int_df)
        elif probability_func == "ppf":
            return t.ppf(float_p, int_df)
        raise ValueError("probability_func must be one of 'ppf', 'pdf', or 'cdf'")

    def f_fisher_snedecor(
        self,
        int_dfn: int,
        int_dfd: int,
        float_mu: float,
        float_p: Optional[float] = None,
        probability_func: Literal["ppf", "pdf", "cdf"] = "ppf",
        x_axis_inf_range: Optional[float] = None,
        x_axis_sup_range: Optional[float] = None,
        x_axis_pace: Optional[float] = None,
    ) -> Union[float, NDArray[np.float64]]:
        """Calculate F-Snedecor distribution statistics.

        Parameters
        ----------
        int_dfn : int
            Degrees of freedom numerator
        int_dfd : int
            Degrees of freedom denominator
        float_mu : float
            Mean value
        float_p : Optional[float], optional
            Probability value, by default None
        probability_func : Literal['ppf', 'pdf', 'cdf'], optional
            Function type ('ppf', 'pdf', or 'cdf'), by default 'ppf'
        x_axis_inf_range : Optional[float], optional
            Lower bound of x-axis range, by default None
        x_axis_sup_range : Optional[float], optional
            Upper bound of x-axis range, by default None
        x_axis_pace : Optional[float], optional
            Step size for x-axis range, by default None

        Returns
        -------
        Union[float, NDArray[np.float64]]
            Result of the specified probability function

        Raises
        ------
        ValueError
            If numerator degrees of freedom <= denominator degrees of freedom
            If degrees of freedom is less than 0
            If probability_func is not one of 'ppf', 'pdf', or 'cdf'
            If x_axis_inf_range, x_axis_sup_range, or x_axis_pace is None
        """
        if int_dfn <= 0 or int_dfd <= 0:
            raise ValueError("Degrees of freedom must be positive")
        if int_dfn <= int_dfd:
            raise ValueError("Numerator df must be greater than denominator df")
        if probability_func not in ("ppf", "pdf", "cdf"):
            raise ValueError("probability_func must be one of 'ppf', 'pdf', or 'cdf'")
        if probability_func == "ppf" and (float_p is None or not 0.0 <= float_p <= 1.0):
            raise ValueError("Probability must be between 0 and 1 for ppf calculation")
        if probability_func in ("pdf", "cdf") and (
            x_axis_inf_range is None or x_axis_sup_range is None or x_axis_pace is None
        ):
            raise ValueError("Range parameters must be provided for pdf/cdf calculations")
        
        f_dist = f(int_dfn, int_dfd, float_mu)

        if probability_func in ("pdf", "cdf"):
            arr_ind = np.arange(x_axis_inf_range, x_axis_sup_range, x_axis_pace)
            if probability_func == "pdf":
                return f_dist.pdf(arr_ind)
            return f.cdf(arr_ind, int_dfn, int_dfd)
        elif probability_func == "ppf":
            return f.ppf(float_p, int_dfn, int_dfd)
        raise ValueError("probability_func must be one of 'ppf', 'pdf', or 'cdf'")


class NormalDistribution(metaclass=TypeChecker):
    """Class implementing Normal distribution calculations."""

    def phi(self, x: float) -> float:
        """Calculate standard normal probability density function.

        Parameters
        ----------
        x : float
            Input value

        Returns
        -------
        float
            Probability density at x
        """
        return np.exp(-(x**2) / 2.0) / np.sqrt(2.0 * np.pi)

    def pdf(self, x: float, float_mu: float = 0.0, float_sigma: float = 1.0) -> float:
        """Calculate normal probability density function.

        Parameters
        ----------
        x : float
            Input value
        float_mu : float, optional
            Mean, by default 0.0
        float_sigma : float, optional
            Standard deviation, by default 1.0

        Returns
        -------
        float
            Probability density at x
        """
        return self.phi((x - float_mu) / float_sigma) / float_sigma

    def cumnulative_phi(self, z: float) -> float:
        """Calculate standard normal cumulative distribution function.

        Parameters
        ----------
        z : float
            Input value

        Returns
        -------
        float
            Cumulative probability at z
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

    def cdf(self, x: float, float_mu: float = 0.0, float_sigma: float = 1.0) -> float:
        """Calculate normal cumulative distribution function.

        Parameters
        ----------
        x : float
            Input value
        float_mu : float, optional
            Mean, by default 0.0
        float_sigma : float, optional
            Standard deviation, by default 1.0

        Returns
        -------
        float
            Cumulative probability at x
        """
        return self.cumnulative_phi((x - float_mu) / float_sigma)

    def inv_cdf(self, float_p: float, float_mu: float = 0.0, float_sigma: float = 1.0) -> float:
        """Calculate inverse normal cumulative distribution function.

        Parameters
        ----------
        float_p : float
            Probability value (0 <= float_p <= 1)
        float_mu : float, optional
            Mean, by default 0.0
        float_sigma : float, optional
            Standard deviation, by default 1.0

        Returns
        -------
        float
            Quantile corresponding to float_p

        Raises
        ------
        ValueError
            If float_p is not between 0 and 1
            If float_sigma is less than or equal to 0
        """
        if not 0.0 <= float_p <= 1.0:
            raise ValueError(f"Probability must be between 0 and 1, got {float_p}")
        if float_sigma <= 0:
            raise ValueError("Standard deviation must be positive")
        return norm.ppf(float_p, float_mu, float_sigma)

    def confidence_interval_normal(
        self, data: NDArray[np.float64], confidence: float = 0.95
    ) -> dict[str, float]:
        """Calculate confidence interval for normal distribution.

        Parameters
        ----------
        data : NDArray[np.float64]
            Input data array
        confidence : float, optional
            Confidence level, by default 0.95

        Returns
        -------
        dict[str, float]
            Dictionary containing mean and confidence interval bounds

        Raises
        ------
        ValueError
            If data array is empty
            If confidence is not between 0 and 1

        References
        ----------
        .. [1] https://stackoverflow.com/questions/15033511/compute-a-confidence-interval-from-sample-data
        """
        if len(data) == 0:
            raise ValueError("Data array must not be empty")
        if not 0.0 <= confidence <= 1.0:
            raise ValueError("Confidence must be between 0 and 1")
        
        a = 1.0 * np.array(data)
        n = len(a)
        float_mu, se = np.mean(a), sem(a)
        z = se * t.ppf((1 + confidence) / 2.0, n - 1)
        return {
            "mean": float_mu,
            "inferior_inteval": float_mu - z,
            "superior_interval": float_mu + z,
        }

    def ecdf(self, data: NDArray[np.float64]) -> tuple[NDArray[np.float64], NDArray[np.float64]]:
        """Calculate empirical cumulative distribution function.

        Parameters
        ----------
        data : NDArray[np.float64]
            Input data array

        Returns
        -------
        tuple[NDArray[np.float64], NDArray[np.float64]]
            Sorted data and corresponding ECDF values

        Raises
        ------
        ValueError
            If data array is empty

        References
        ----------
        .. [1] https://campus.datacamp.com/courses/statistical-thinking-in-python-part-1/
               graphical-exploratory-data-analysis?ex=12
        """
        if len(data) == 0:
            raise ValueError("Data array cannot be empty")

        n = len(data)
        x = np.sort(data)
        y = np.arange(1, n + 1) / n
        return x, y


class HansenSkewStudent(metaclass=TypeChecker):
    """Skewed Student distribution class (Hansen 1994).

    Attributes
    ----------
    eta : float
        Degrees of freedom (2 < eta < ∞)
    lam : float
        Skewness (-1 < lam < 1)

    References
    ----------
    .. [1] https://www.ssc.wisc.edu/~bhansen/papers/ier_94.pdf
    """

    def __init__(self, eta: float = 10.0, lam: float = -0.1) -> None:
        """Initialize Hansen's Skewed Student distribution.

        Parameters
        ----------
        eta : float, optional
            Degrees of freedom, by default 10.0
        lam : float, optional
            Skewness parameter, by default -0.1
        """
        self.eta = eta
        self.lam = lam

    def const_a(self) -> float:
        """Compute constant a.

        Returns
        -------
        float
            Constant a value
        """
        return 4 * self.lam * self.const_c() * (self.eta - 2) / (self.eta - 1)

    def const_b(self) -> float:
        """Compute constant b.

        Returns
        -------
        float
            Constant b value
        """
        return sqrt(1 + 3 * self.lam**2 - self.const_a()**2)

    def const_c(self) -> float:
        """Compute constant c.

        Returns
        -------
        float
            Constant c value
        """
        return gamma((self.eta + 1) / 2) / (sqrt(pi * (self.eta - 2)) * gamma(self.eta / 2))

    def pdf(self, array_data: NDArray[np.float64]) -> NDArray[np.float64]:
        """Calculate probability density function.

        Parameters
        ----------
        array_data : NDArray[np.float64]
            Input values

        Returns
        -------
        NDArray[np.float64]
            PDF values

        Raises
        ------
        ValueError
            If data array is empty
        """
        if len(array_data) == 0:
            raise ValueError("Data array cannot be empty")
        
        c = self.const_c()
        a = self.const_a()
        b = self.const_b()

        return b * c * (1 + 1 / (self.eta - 2) * ((b * array_data + a) / (
            1 + np.sign(array_data + a / b) * self.lam)) ** 2) ** (-(self.eta + 1) / 2)

    def cdf(self, array_data: NDArray[np.float64]) -> NDArray[np.float64]:
        """Calculate cumulative distribution function.

        Parameters
        ----------
        array_data : NDArray[np.float64]
            Input values

        Returns
        -------
        NDArray[np.float64]
            CDF values

        Raises
        ------
        ValueError
            If data array is empty
        """
        if len(array_data) == 0:
            raise ValueError("Data array cannot be empty")
        
        a = self.const_a()
        b = self.const_b()

        y = (b * array_data + a) / (1 + np.sign(array_data + a / b) * self.lam) \
            * sqrt(1 - 2 / self.eta)
        cond = array_data < -a / b

        return cond * (1 - self.lam) * t.cdf(y, self.eta) + ~cond * (
            -self.lam + (1 + self.lam) * t.cdf(y, self.eta))

    def ppf(self, array_data: NDArray[np.float64]) -> NDArray[np.float64]:
        """Calculate inverse cumulative distribution function.

        Parameters
        ----------
        array_data : NDArray[np.float64]
            Probability values (0 < array_data < 1)

        Returns
        -------
        NDArray[np.float64]
            Quantile values

        Raises
        ------
        ValueError
            If data array is empty
        """
        if len(array_data) == 0:
            raise ValueError("Data array cannot be empty")
        
        array_data = np.atleast_1d(array_data)
        a = self.const_a()
        b = self.const_b()

        cond = array_data < (1 - self.lam) / 2

        ppf1 = t.ppf(array_data / (1 - self.lam), self.eta)
        ppf2 = t.ppf(0.5 + (array_data - (1 - self.lam) / 2) / (1 + self.lam), self.eta)
        ppf = -999.99 * np.ones_like(array_data)
        ppf = np.nan_to_num(ppf1) * cond + np.nan_to_num(ppf2) * ~cond
        ppf = (ppf * (1 + np.sign(array_data - (1 - self.lam) / 2) * self.lam) 
               * sqrt(1 - 2 / self.eta) - a) / b

        return float(ppf) if ppf.shape == (1,) else ppf

    def rvs(self, size: Union[int, tuple[int, ...]] = 1) -> NDArray[np.float64]:
        """Generate random variates.

        Parameters
        ----------
        size : Union[int, tuple[int, ...]], optional
            Output shape, by default 1

        Returns
        -------
        NDArray[np.float64]
            Random variates

        Raises
        ------
        ValueError
            If data array is empty
        """
        if size <= 0:
            raise ValueError("Data array cannot be empty")
        
        return self.ppf(uniform.rvs(size=size))

    def plot_pdf(self, array_data: Optional[NDArray[np.float64]] = None) -> None:
        """Plot probability density function.

        Parameters
        ----------
        array_data : Optional[NDArray[np.float64]], optional
            Input values, by default None (uses DEFAULT_PDF_CDF_RANGE)
        """
        if array_data is None:
            array_data = DEFAULT_PDF_CDF_RANGE
        scale = sqrt(self.eta / (self.eta - 2))
        plt.plot(array_data, t.pdf(array_data, self.eta, scale=1 / scale), label="t dist")
        plt.plot(array_data, self.pdf(array_data), label="skew-t dist")
        plt.legend()
        plt.show()

    def plot_cdf(self, array_data: Optional[NDArray[np.float64]] = None) -> None:
        """Plot cumulative distribution function.

        Parameters
        ----------
        array_data : Optional[NDArray[np.float64]], optional
            Input values, by default None (uses DEFAULT_PDF_CDF_RANGE)
        """
        if array_data is None:
            array_data = DEFAULT_PDF_CDF_RANGE
        scale = sqrt(self.eta / (self.eta - 2))
        plt.plot(array_data, t.cdf(array_data, self.eta, scale=1 / scale), label="t dist")
        plt.plot(array_data, self.cdf(array_data), label="skew-t dist")
        plt.legend()
        plt.show()

    def plot_ppf(self, array_data: Optional[NDArray[np.float64]] = None) -> None:
        """Plot inverse cumulative distribution function.

        Parameters
        ----------
        array_data : Optional[NDArray[np.float64]], optional
            Probability values, by default None (uses DEFAULT_PPF_RANGE)
        """
        if array_data is None:
            array_data = DEFAULT_PPF_RANGE
        scale = sqrt(self.eta / (self.eta - 2))
        plt.plot(array_data, t.ppf(array_data, self.eta, scale=1 / scale), label="t dist")
        plt.plot(array_data, self.ppf(array_data), label="skew-t dist")
        plt.legend()
        plt.show()
    
    def plot_rvspdf(
        self,
        array_data: Optional[NDArray[np.float64]] = None,
        size: int = 1000,
    ) -> None:
        """Plot kernel density estimate of random sample.

        Parameters
        ----------
        array_data : Optional[NDArray[np.float64]], optional
            Input values, by default None (uses DEFAULT_PDF_CDF_RANGE)
        size : int, optional
            Sample size, by default 1000

        Raises
        ------
        ValueError
            If data array is empty
        """
        if size <= 0:
            raise ValueError("Data array cannot be empty")
        
        if array_data is None:
            array_data = DEFAULT_PDF_CDF_RANGE
        rvs = self.rvs(size=size)
        xrange = [array_data.min(), array_data.max()]
        sns.kdeplot(rvs, clip=xrange, label="kernel")
        plt.plot(array_data, self.pdf(array_data), label="true pdf")
        plt.xlim(xrange)
        plt.legend()
        plt.show()

    def loglikelihood(
        self, theta: Optional[NDArray[np.float64]] = None, x: Optional[NDArray[np.float64]] = None
    ) -> float:
        """Calculate log-likelihood function.

        Parameters
        ----------
        theta : Optional[NDArray[np.float64]], optional
            Parameters [eta, lambda], by default None
        x : Optional[NDArray[np.float64]], optional
            Input data, by default None

        Returns
        -------
        float
            Log-likelihood value
        
        Raises
        ------
        ValueError
            If theta and x are not provided
        """
        if theta is None or x is None:
            raise ValueError("theta and x must be provided")
        
        nu = theta[0]
        lambda_ = theta[1]

        c = gamma((nu + 1) / 2) / (multiply(sqrt(dot(pi, (nu - 2))), gamma(nu / 2)))
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
