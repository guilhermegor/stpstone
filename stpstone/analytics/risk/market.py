import pandas as pd
import numpy as np
from scipy.stats import norm
from typing import List, Union, Optional, Literal, Dict
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.arrays import Arrays


class RiskStats(metaclass=TypeChecker):

    def __init__(self, array_r: Union[np.ndarray, pd.Series, List[float]]) -> None:
        """
        Initializer for RiskStats class

        Args:
            array_r (Union[np.ndarray, pd.Series, List[float]]): A sequence of returns,
                ordered by dates in descending order, which can be a
                numpy array, pandas Series, list of floats, or a single float.
        """
        self.array_r = Arrays().to_array(array_r)

    def variance_ewma(self, float_lambda: Optional[float] = 0.94) -> float:
        """
        Exponentially Weighted Moving Average(EWMA) is a type of moving average that gives more
        weight to recent observations.

        Formula:
            EWMAt = λ * Rt + (1 - λ) * EWMAt-1

        Args:
            float_lambda (float): The smoothing factor, typically between 0 and 1, defaulting to 0.94

        Returns:
            np.ndarray: The exponentially weighted moving average of the input array.

        Metadata: https://corporatefinanceinstitute.com/resources/career-map/sell-side/capital-markets/exponentially-weighted-moving-average-ewma/
        """
        array_ewma = np.zeros_like(self.array_r)
        array_ewma[0] = self.array_r[0]
        for t in range(1, len(self.array_r)):
            array_ewma[t] = float_lambda * self.array_r[t] + (1 - float_lambda) * array_ewma[t - 1]
        return np.sum(array_ewma)

    def descriptive_stats(self, float_lambda: Optional[float] = 0.94) -> Dict[str, float]:
        return {
            "mu": np.mean(self.array_r),
            "std": np.std(self.array_r),
            "ewma_std": np.sqrt(self.variance_ewma(float_lambda)),
        }


class MarkowitzPortf(metaclass=TypeChecker):

    def __init__(
        self,
        array_r: Union[np.ndarray, pd.DataFrame],
        array_w: Union[np.ndarray, pd.DataFrame],
        float_lambda: Optional[float] = 0.94,
        bl_validate_w: Optional[bool] = True,
        float_atol: Optional[float] = 1e-4
    ) -> None:
        self.float_lambda = float_lambda
        self.array_r = Arrays().to_array(array_r)
        self.array_w = Arrays().to_array(array_w)
        if bl_validate_w == True:
            if self.array_w.ndim == 1:
                if not np.isclose(np.sum(self.array_w), 1.0, atol=float_atol):
                    raise ValueError("Portfolio weights must sum to 1.")
            elif self.array_w.ndim == 2:
                for i, row in enumerate(self.array_w):
                    if not np.isclose(np.sum(row), 1.0, atol=float_atol):
                        raise ValueError(f"Portfolio weights in row {i} must sum to 1.")
            else:
                raise ValueError("Portfolio weights must be either 1D or 2D array.")

    @property
    def mu(self) -> float:
        return np.mean(np.sum(self.array_r * self.array_w, axis=1))

    @property
    def cov(self) -> np.ndarray:
        """
        The covariance matrix of the portfolio. If float_lambda is None, then a regular
        covariance matrix is computed. Otherwise, the exponentially weighted moving
        average (EWMA) of the covariance matrix is computed.

        Formula:
            Covt = λ * Covt-1 + (1 - λ) * Rt * Rt.T

        Metadata:
            https://www.ime.usp.br/~rvicente/Aula2_VaROverviewParte2.pdf, page 27
        """
        array_cov = np.cov(self.array_r, rowvar=False)
        if self.float_lambda is None:
            return array_cov
        else:
            for t in range(1, self.array_r.shape[0]):
                array_r_t = self.array_r[t]
                array_cov = self.float_lambda * array_cov + (1 - self.float_lambda) \
                    * np.dot(array_r_t, array_r_t.T)
            return array_cov

    @property
    def sigma(self) -> float:
        if self.array_w.ndim == 1:
            return np.dot(self.array_w.T, np.dot(self.cov, self.array_w))
        elif self.array_w.ndim == 2:
            array_sigmas = np.zeros(self.array_w.shape[0])
            for i, array_new_w in enumerate(self.array_w):
                array_sigmas[i] = np.dot(array_new_w.T, np.dot(self.cov, array_new_w))
            return float(array_sigmas[0])
        else:
            raise ValueError("Portfolio weights must be either 1D or 2D array.")

    def sharpe_ratio(self, float_rf: float) -> float:
        return (self.mu - float_rf) / self.sigma


class VaR(metaclass=TypeChecker):

    def __init__(
        self,
        float_mu: float,
        float_sigma: float,
        array_r: Optional[Union[np.ndarray, pd.Series, List[float]]] = None,
        float_cl: Optional[float] = 0.95,
        int_t: Optional[int] = 1,
        float_lambda: Optional[float] = 0.94
    ) -> None:
        self.float_mu = float_mu
        self.float_sigma = float_sigma
        self.float_cl = float_cl
        self.int_t = int_t
        self.float_lambda = float_lambda
        self.array_r = Arrays().to_array(array_r)
        self.float_z = norm.ppf(float_cl)

    @property
    def historic_var(self) -> float:
        return np.percentile(self.array_r, (1 - self.float_cl) * 100)

    def historic_var_stress_test(
        self,
        float_shock: float,
        str_shock_type: Literal["absolute", "relative"] = "relative"
    ) -> float:
        if str_shock_type == "relative":
            array_r_shock = self.array_r * (1 + float_shock)
        elif str_shock_type == "absolute":
            array_r_shock = self.array_r + float_shock
        else:
            raise ValueError(f"Invalid shock type {str_shock_type}. Must be 'relative' or 'absolute'")
        return np.percentile(array_r_shock, (1 - self.float_cl) * 100) * self.int_t

    @property
    def parametric_var(self) -> float:
        """
        Calculates the parametric value at risk (VaR) using a normal distribution.

        Args:
            float_cl (float): The confidence level, between 0 and 1, defaulting to 0.95.
            str_std_methd (str): The method to use for calculating the standard deviation,
                either "std" or "ewma".
            float_lambda (float): The smoothing factor for EWMA, between 0 and 1, defaulting to 0.94.

        Returns:
            float: The parametric VaR.
        """
        return self.float_mu * self.int_t - self.float_z * self.float_sigma * np.sqrt(self.int_t)

    @property
    def cvar(self) -> float:
        """
        Calculates the conditional value at risk (CVaR), also known as the Expected Shortfall,
        for a given portfolio.

        Args:
            float_cl (float): The confidence level, between 0 and 1, defaulting to 0.95.

        Returns:
            float: The CVaR.

        Notes:
            The CVaR is the mean loss of the left tail of the distribution below the VaR.
            The VaR is calculated using the percentile method.
        """
        float_var = np.percentile(self.array_r, (1 - self.float_cl) * 100)
        array_cvar = self.array_r[self.array_r <= float_var]
        return np.mean(array_cvar) * self.int_t

    def monte_carlo_var(self, int_simulations: Optional[int] = 10_000,
                        float_portf_nv: Optional[float] = 1_000_000) -> float:
        """
        Calculates the Monte Carlo Value at Risk (VaR) for a given portfolio.

        Args:
            float_cl (float, optional): The confidence level for the VaR calculation,
                with a default value of 0.95.
            str_std_methd (Literal["std", "ewma_std"], optional): The method to use for
                calculating the standard deviation, either "std" or "ewma_std", with a
                default of "std".
            float_lambda (float, optional): The smoothing factor for EWMA, defaulting
                to 0.94.
            int_t (int, optional): The time horizon for the VaR calculation, with a
                default value of 1.
            int_simulations (int, optional): The number of simulations to perform for
                the Monte Carlo analysis, with a default value of 10,000.
            float_portf_nv (float, optional): The notional value of the portfolio,
                defaulting to 1,000,000.

        Returns:
            float: The Monte Carlo VaR of the portfolio, representing the potential
            loss in portfolio value at the specified confidence level.
        """
        array_simulated_r = np.random.normal(
            loc=self.float_mu, scale=self.float_sigma, size=(int_simulations, self.int_t))
        array_portfs_nv = float_portf_nv * np.cumprod(1 + array_simulated_r, axis=1)
        array_portfs_nv = np.sort(array_portfs_nv)
        int_percentile_idx = int((1 - self.float_cl) * int_simulations)
        return float(float_portf_nv - array_portfs_nv[int_percentile_idx].item()) * self.int_t


class RiskMeasures(VaR):

    def __init__(
        self,
        float_mu: float,
        float_sigma: float,
        array_r: Optional[Union[np.ndarray, pd.Series, List[float]]] = None,
        float_cl: Optional[float] = 0.95,
        int_t: Optional[int] = 1,
        float_lambda: Optional[float] = 0.94
    ) -> None:
        self.float_mu = float_mu
        self.float_sigma = float_sigma
        self.float_cl = float_cl
        self.int_t = int_t
        self.float_lambda = float_lambda
        self.array_r = Arrays().to_array(array_r)
        self.float_z = norm.ppf(float_cl)

    @property
    def drawdown(self) -> float:
        array_cum_r = np.cumprod(1 + self.array_r)
        array_cummax_r = np.maximum.accumulate(array_cum_r)
        array_drawdown = (array_cum_r - array_cummax_r) / array_cummax_r
        return np.min(array_drawdown)

    def tracking_error(
        self,
        array_portf_r: Union[np.ndarray, pd.Series, List[float]],
        array_benchmark_r: Union[np.ndarray, pd.Series, List[float]],
        float_ddof: Optional[float] = 1
    ) -> float:
        """
        Calculates the tracking error between a portfolio and a benchmark.

        Args:
            float_ddof (float, optional): The delta degrees of freedom, with a default value of 1,
            representing the N - ddof in the denominator of the standard deviation calculation.
            Use 0 for population standard deviation and 1 for sample standard deviation.

        Returns:
            float: The tracking error, which is the standard deviation of the active returns.
        """
        array_portf_r = Arrays().to_array(array_portf_r)
        array_benchmark_r = Arrays().to_array(array_benchmark_r)
        array_active_r = array_portf_r - array_benchmark_r
        return np.std(array_active_r, ddof=float_ddof)

    def sharpe(self, float_rf: float) -> float:
        return (self.float_mu - float_rf) / self.float_sigma

    def beta(self, array_market_r: Union[np.ndarray, pd.Series, List[float]],
        float_ddof: Optional[float] = 1) -> float:
        array_market_r = Arrays().to_array(array_market_r)
        array_cov = np.cov(self.array_r, array_market_r, ddof=float_ddof)
        return array_cov[0, 1] / array_cov[1, 1]


class QuoteVar(VaR):

    def __init__(
        self,
        array_r: Union[np.ndarray, pd.Series, List[float]],
        str_method_str: Literal["std", "ewma_std"] = "std",
        float_cl: Optional[float] = 0.95,
        int_t: Optional[int] = 1,
        float_lambda: Optional[float] = 0.94
    ) -> None:
        self.dict_desc_stats = RiskStats(array_r).descriptive_stats(float_lambda=float_lambda)
        super().__init__(
            float_mu=self.dict_desc_stats["mu"],
            float_sigma=self.dict_desc_stats[str_method_str],
            array_r=array_r,
            float_cl=float_cl,
            int_t=int_t,
            float_lambda=float_lambda
        )


class PortfVar(VaR):

    def __init__(
        self,
        array_r: Union[np.ndarray, pd.DataFrame],
        array_w: Union[np.ndarray, pd.DataFrame],
        float_cl: Optional[float] = 0.95,
        int_t: Optional[int] = 1,
        float_lambda: Optional[float] = 0.94,
        bl_validate_w: Optional[bool] = True,
        float_atol: Optional[float] = 1e-4
    ) -> None:
        if (array_w.shape[0] > 1) and (array_r.shape != array_w.shape):
            raise ValueError("Return and weight arrays must have the same shape.")
        self.cls_markowitz = MarkowitzPortf(array_r, array_w, float_lambda, bl_validate_w, float_atol)
        super().__init__(
            float_mu=self.cls_markowitz.mu,
            float_sigma=self.cls_markowitz.sigma,
            array_r=array_r,
            float_cl=float_cl,
            int_t=int_t,
            float_lambda=float_lambda
        )
