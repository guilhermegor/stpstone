import pandas as pd
import numpy as np
from scipy.stats import norm
from typing import List, Union, Optional, Literal, Dict
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class RiskStats(metaclass=TypeChecker):

    def __init__(self, array_r: Union[np.ndarray, pd.Series, float, List[float]]):
        """
        Initializer for RiskStats class
        
        Args:
            array_r (Union[np.ndarray, pd.Series, float, List[float]]): A sequence of returns, 
                ordered by dates in descending order, which can be a 
                numpy array, pandas Series, list of floats, or a single float.
        """
        self.array_r = array_r if isinstance(array_r, np.ndarray) else (
            np.array(array_r) if isinstance(array_r, list) 
            else (array_r.to_numpy() if isinstance(array_r, pd.Series) else None)
        )
    
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
            array_ewma[t] = float_lambda * self.array_r[t -1] + (1 - float_lambda) * array_ewma[t - 1]
        return np.sum(array_ewma)
    
    @property
    def skewness(self) -> float:
        return np.sum((self.array_r - np.mean(self.array_r)) ** 3)
    
    @property
    def kurtosis(self) -> float:
        return np.sum((self.array_r - np.mean(self.array_r)) ** 4)

    def descriptive_stats(self, float_lambda: Optional[float] = 0.94) -> Dict[str, float]:
        return {
            "mu": np.mean(self.array_r),
            "std": np.std(self.array_r),
            "ewma_std": np.sqrt(self.variance_ewma(float_lambda)), 
            "skewness": self.skewness,
            "kurtosis": self.kurtosis, 
            "excess_kurtosis": self.kurtosis - 3, 
            "min": np.min(self.array_r),
            "max": np.max(self.array_r)
        }


class VaR(RiskStats):

    def __init__(self, float_mu: float, float_sigma: float, 
                 array_r: Union[np.ndarray, pd.Series, float, List[float]], 
                 float_cl: Optional[float] = 0.95, int_t: Optional[int] = 1) -> None:
        self.float_mu = float_mu
        self.float_sigma = float_sigma
        self.float_cl = float_cl
        self.int_t = int_t
        self.array_r = array_r if isinstance(array_r, np.ndarray) else (
            np.array(array_r) if isinstance(array_r, list) 
            else (array_r.to_numpy() if isinstance(array_r, pd.Series) else None)
        )
        self.z = norm.ppf(float_cl)

    @property
    def historic_var(self) -> float:
        return np.percentile(self.array_r, (1 - self.float_cl) * 100)

    @property
    def parametric_var(self) -> float:
        """
        Calculates the parametric value at risk (VaR) using a normal distribution.

        Args:
            float_cl (float): The confidence level, between 0 and 1, defaulting to 0.95.
            str_std_methd (str): The method to use for calculating the standard deviation,
                either "std" or "ewma".
            float_lambda (float): The smoothing factor for EWMA, between 0 and 1, defaulting to 0.94.
            int_t (int): Default = 1
                - The time horizon for the VaR calculation.

        Returns:
            float: The parametric VaR.
        """
        return (self.float_mu - self.float_z * self.float_sigma) * np.sqrt(self.int_t)
    
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
        return np.mean(array_cvar)

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
        return float_portf_nv - array_portfs_nv[int_percentile_idx]


class QuotePortfolioRiskAssessment(VaR):

    def __init__(
        self, 
        array_portf_r: Union[np.ndarray, pd.Series, float, List[float]], 
        array_benchmark_r: Optional[Union[np.ndarray, pd.Series, float, List[float]]] = None, 
        float_cl: Optional[float] = 0.95, 
        int_t: Optional[int] = 1, 
        float_lambda: Optional[float] = 0.94, 
        str_method_sigmna_calc: Optional[Literal["std", "ewma_std"]] = "ewma_std"
    ) -> None:
        self.array_portf_r = array_portf_r if isinstance(array_portf_r, np.ndarray) else (
            np.array(array_portf_r) if isinstance(array_portf_r, list) 
            else (array_portf_r.to_numpy() if isinstance(array_portf_r, pd.Series) else None)
        )
        self.array_benchmark_r = array_benchmark_r if isinstance(array_benchmark_r, np.ndarray) else (
            np.array(array_benchmark_r) if isinstance(array_benchmark_r, list) 
            else (array_benchmark_r.to_numpy() if isinstance(array_benchmark_r, pd.Series) else None)
        )
        self.array_r = array_portf_r
        self.float_cl = float_cl
        self.int_t = int_t
        self.float_lambda = float_lambda
        self.str_method_sigmna_calc = str_method_sigmna_calc
        self.dict_stats = self.descriptive_stats(self.float_lambda)
        self.float_mu = self.dict_stats["mu"]
        self.float_sigma = self.dict_stats[self.str_method_sigmna_calc]

    @property
    def drawdown(self):
        array_cum_r = np.cumprod(1 + self.array_r)
        array_cummax_r = array_cum_r.cummax()
        array_drawdown = (array_cum_r - array_cummax_r) / array_cummax_r
        return np.min(array_drawdown)
    
    def tracking_error(self, float_ddof: Optional[float] = 1) -> float:
        """
        Calculates the tracking error between a portfolio and a benchmark.

        Args:
            float_ddof (float, optional): The delta degrees of freedom, with a default value of 1, 
            representing the N - ddof in the denominator of the standard deviation calculation.
            Use 0 for population standard deviation and 1 for sample standard deviation.

        Returns:
            float: The tracking error, which is the standard deviation of the active returns.
        """
        array_active_r = self.array_portf_r - self.array_benchmark_r
        return np.std(array_active_r, ddof=float_ddof)


class QuoteBenchmarkRiskAssessment(VaR):

    def __init__(
        self, 
        array_portf_r: Union[np.ndarray, pd.Series, float, List[float]], 
        array_benchmark_r: Optional[Union[np.ndarray, pd.Series, float, List[float]]] = None, 
        float_cl: Optional[float] = 0.95, 
        int_t: Optional[int] = 1, 
        float_lambda: Optional[float] = 0.94, 
        str_method_sigmna_calc: Optional[Literal["std", "ewma_std"]] = "ewma_std", 
        str_method_bvar_calc: Optional[Literal["historic", "parametric", "monte_carlo"]] = "parametric"
    ) -> None:
        self.array_portf_r = array_portf_r if isinstance(array_portf_r, np.ndarray) else (
            np.array(array_portf_r) if isinstance(array_portf_r, list) 
            else (array_portf_r.to_numpy() if isinstance(array_portf_r, pd.Series) else None)
        )
        self.array_benchmark_r = array_benchmark_r if isinstance(array_benchmark_r, np.ndarray) else (
            np.array(array_benchmark_r) if isinstance(array_benchmark_r, list) 
            else (array_benchmark_r.to_numpy() if isinstance(array_benchmark_r, pd.Series) else None)
        )
        self.array_r = array_benchmark_r
        self.float_cl = float_cl
        self.int_t = int_t
        self.float_lambda = float_lambda
        self.str_method_sigmna_calc = str_method_sigmna_calc
        self.str_method_bvar_calc = str_method_bvar_calc
        self.dict_stats = self.descriptive_stats(self.float_lambda)
        self.float_mu = self.dict_stats["mu"]
        self.float_sigma = self.dict_stats[self.str_method_sigmna_calc]

    @property
    def drawdown(self):
        array_cum_r = np.cumprod(1 + self.array_r)
        array_cummax_r = array_cum_r.cummax()
        array_drawdown = (array_cum_r - array_cummax_r) / array_cummax_r
        return np.min(array_drawdown)
    
    def tracking_error(self, float_ddof: Optional[float] = 1) -> float:
        """
        Calculates the tracking error between a portfolio and a benchmark.

        Args:
            float_ddof (float, optional): The delta degrees of freedom, with a default value of 1, 
            representing the N - ddof in the denominator of the standard deviation calculation.
            Use 0 for population standard deviation and 1 for sample standard deviation.

        Returns:
            float: The tracking error, which is the standard deviation of the active returns.
        """
        array_active_r = self.array_portf_r - self.array_benchmark_r
        return np.std(array_active_r, ddof=float_ddof)
    
    @property
    def bvar(self):
        if self.str_method_bvar_calc == "historic":
            return self.historic_var
        elif self.str_method_bvar_calc == "parametric":
            return self.parametric_var
        elif self.str_method_bvar_calc == "monte_carlo":
            return self.monte_carlo_var
        else:
            raise ValueError("Unknown method for calculating bvar. Please choose from "
                             + "'historic', 'parametric', or 'monte_carlo'")
