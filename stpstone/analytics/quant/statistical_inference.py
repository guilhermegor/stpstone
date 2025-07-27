"""Statistical hypothesis testing module.

Provides classes for performing various statistical hypothesis tests, including
multiple regression assumptions, normality, correlation, stationarity, means,
distributions, and independence tests. All methods validate inputs and return
results in structured dictionaries.
"""

from typing import Optional, TypedDict

import numpy as np
from numpy.typing import NDArray
from scipy import stats
from sklearn.linear_model import LinearRegression
import statsmodels.api as sm
import statsmodels.stats.diagnostic as dg
from statsmodels.stats.stattools import durbin_watson

from stpstone.analytics.quant.prob_distributions import NormalDistribution
from stpstone.analytics.quant.statistical_description import StatisticalDescription
from stpstone.transformations.cleaner.eda import ExploratoryDataAnalysis
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class AnovaResults(TypedDict):
    """TypedDict for ANOVA results."""

    summary: str


class LinearityResults(TypedDict):
    """TypedDict for linearity test results."""

    r_squared: dict[str, float]
    h0: str
    bl_reject_h0: dict[str, bool]


class VifResults(TypedDict):
    """TypedDict for VIF results."""

    vif_ivs: dict[str, float]
    r_squared_mc_ivs: dict[str, float]
    h0: str
    bl_reject_h0: dict[str, bool]


class BreuschGodfreyResults(TypedDict):
    """TypedDict for Breusch-Godfrey test results."""

    breush_godfrey_tup: tuple[float, float, int, dict[float, float]]
    h0: str
    bl_reject_h0: bool


class DurbinWatsonResults(TypedDict):
    """TypedDict for Durbin-Watson test results."""

    test_value: float
    h0: str
    bl_reject_h0: bool


class BreuschPaganResults(TypedDict):
    """TypedDict for Breusch-Pagan test results."""

    lagrange_multiplier: float
    p_value: float
    h0: str
    bl_reject_h0: bool


class CooksDistanceResults(TypedDict):
    """TypedDict for Cook's distance results."""

    distance: NDArray[np.float64]
    p_value: NDArray[np.float64]


class KolmogorovSmirnovResults(TypedDict):
    """TypedDict for Kolmogorov-Smirnov test results."""

    dn: float
    critical_value: float
    h0: str
    reject_h0: bool


class NormalityTestResults(TypedDict):
    """TypedDict for normality test results."""

    alpha: float
    p_value: float
    h0: str
    reject_h0: bool


class CorrelationTestResults(TypedDict):
    """TypedDict for correlation test results."""

    alpha: float
    p_value: float
    reject_h0: bool


class StationarityTestResults(TypedDict):
    """TypedDict for stationarity test results."""

    statistic: float
    p_value: float
    lags: int
    criteria: dict[float, float]
    reject_h0: bool


class PearsonChiSquaredResults(TypedDict):
    """TypedDict for Pearson's chi-squared test results."""

    chi_squard_statistic: float
    p_value: float
    degrees_freedom: int
    array_expected_values: NDArray[np.float64]
    significance: float
    h0_hypothesis: str
    reject_h0: bool


class BenfordResults(TypedDict):
    """TypedDict for Benford's Law test results."""
    
    benford_expected_array: NDArray[np.float64]
    real_numbers_observed_array: NDArray[np.float64]


def validate_array(
    array: NDArray[np.float64], 
    name: str, 
    min_samples: int = 1
) -> None:
    """Validate numpy array inputs.

    Parameters
    ----------
    array : NDArray[np.float64]
        Input array to validate.
    name : str
        Name of the array for error messages.
    min_samples : int, optional
        Minimum number of samples required, by default 1.

    Raises
    ------
    ValueError
        If array is empty, contains non-finite values, or has fewer samples than required.
    TypeError
        If array is not a numpy array of float64.
    """
    if not isinstance(array, np.ndarray):
        raise TypeError(f"{name} must be a numpy array, got {type(array)}")
    if array.dtype != np.float64:
        raise TypeError(f"{name} must be of type float64, got {array.dtype}")
    if len(array) < min_samples:
        raise ValueError(f"{name} must have at least {min_samples} samples, got {len(array)}")
    if not np.all(np.isfinite(array)):
        raise ValueError(f"{name} contains NaN or infinite values")


def validate_scalar(
    value: float, 
    name: str, 
    min_val: Optional[float] = None, 
    max_val: Optional[float] = None
) -> None:
    """Validate scalar inputs.

    Parameters
    ----------
    value : float
        Scalar value to validate.
    name : str
        Name of the value for error messages.
    min_val : Optional[float]
        Minimum allowed value, by default None.
    max_val : Optional[float]
        Maximum allowed value, by default None.

    Raises
    ------
    ValueError
        If value is outside specified bounds or non-finite.
    TypeError
        If value is not a float.
    """
    if not isinstance(value, float):
        raise TypeError(f"{name} must be a float, got {type(value)}")
    if not np.isfinite(value):
        raise ValueError(f"{name} must be finite, got {value}")
    if min_val is not None and value < min_val:
        raise ValueError(f"{name} must be at least {min_val}, got {value}")
    if max_val is not None and value > max_val:
        raise ValueError(f"{name} must be at most {max_val}, got {value}")


class MultipleRegressionHT(metaclass=TypeChecker):
    """Class for multiple regression hypothesis tests."""

    def anova(self, array_x: NDArray[np.float64], array_y: NDArray[np.float64]) -> AnovaResults:
        """Perform ANOVA for multiple regression.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Independent variables array, shape (n_samples, n_predictors).
        array_y : NDArray[np.float64]
            Dependent variable array, shape (n_samples,).

        Returns
        -------
        AnovaResults
            Dictionary containing the ANOVA summary.

        Raises
        ------
        ValueError
            If arrays are invalid or have mismatched shapes.

        References
        ----------
        .. [1] https://medium.com/swlh/interpressing-linear-regression-through-statsmodels-summary-4796d359035a
        """
        validate_array(array_x, "array_x", min_samples=2)
        validate_array(array_y, "array_y", min_samples=2)
        if array_x.shape[0] != array_y.shape[0]:
            raise ValueError(
                f"array_x and array_y must have the same number of samples, "
                f"got {array_x.shape[0]} and {array_y.shape[0]}"
            )
        if array_y.ndim != 1:
            raise ValueError(f"array_y must be 1D, got shape {array_y.shape}")
        model_fitted = sm.OLS(array_y, array_x).fit()
        return {"summary": str(model_fitted.summary())}

    def linearity_test(
        self,
        array_x: NDArray[np.float64],
        array_y: NDArray[np.float64],
        list_cols_iv: list[str],
        r_squared_cut: float = 0.8,
    ) -> LinearityResults:
        """Test linearity between dependent and independent variables.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Independent variables array, shape (n_samples, n_predictors).
        array_y : NDArray[np.float64]
            Dependent variable array, shape (n_samples,).
        list_cols_iv : list[str]
            List of independent variable names.
        r_squared_cut : float, optional
            R-squared threshold for linearity, by default 0.8.

        Returns
        -------
        LinearityResults
            Dictionary with R-squared values and hypothesis test results.

        Raises
        ------
        ValueError
            If arrays are invalid, shapes mismatch, or r_squared_cut is not in [0, 1].
        """
        validate_array(array_x, "array_x", min_samples=2)
        validate_array(array_y, "array_y", min_samples=2)
        validate_scalar(r_squared_cut, "r_squared_cut", min_val=0.0, max_val=1.0)
        if array_x.shape[0] != array_y.shape[0]:
            raise ValueError(
                f"array_x and array_y must have the same number of samples, "
                f"got {array_x.shape[0]} and {array_y.shape[0]}"
            )
        if array_y.ndim != 1:
            raise ValueError(f"array_y must be 1D, got shape {array_y.shape}")
        if len(list_cols_iv) != array_x.shape[1]:
            raise ValueError(
                f"list_cols_iv length {len(list_cols_iv)} "
                + f"must match array_x columns {array_x.shape[1]}"
            )
        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)
        linearity_results = []
        for idx in range(array_x.shape[1]):
            lr_model = np.polyfit(array_x[:, idx], array_y, 1)
            r_squared = 1.0 - np.var(array_y - np.polyval(lr_model, array_x[:, idx])) / np.var(
                array_y
            )
            validate_scalar(r_squared, f"R-squared for {list_cols_iv[idx]}", 0.0, 1.0)
            linearity_results.append(r_squared)
        return {
            "r_squared": \
                {list_cols_iv[idx]: linearity_results[idx] for idx in range(len(list_cols_iv))},
            "h0": "linear relationship between X and y",
            "bl_reject_h0": {
                list_cols_iv[idx]: linearity_results[idx] < r_squared_cut
                for idx in range(len(list_cols_iv))
            },
        }

    def vif_iv(
        self, 
        vector_x: NDArray[np.float64], 
        array_y: NDArray[np.float64]
    ) -> tuple[float, float]:
        """Calculate Variance Inflation Factor for a single independent variable.

        Parameters
        ----------
        vector_x : NDArray[np.float64]
            Independent variable vector, shape (n_samples,).
        array_y : NDArray[np.float64]
            Dependent variable array, shape (n_samples,).

        Returns
        -------
        tuple[float, float]
            VIF and R-squared values.

        Raises
        ------
        ValueError
            If arrays are invalid or have mismatched shapes.
        """
        validate_array(vector_x, "vector_x", min_samples=2)
        validate_array(array_y, "array_y", min_samples=2)
        if vector_x.shape[0] != array_y.shape[0]:
            raise ValueError(
                f"vector_x and array_y must have the same number of samples, "
                f"got {vector_x.shape[0]} and {array_y.shape[0]}"
            )
        if vector_x.ndim != 1:
            raise ValueError(f"vector_x must be 1D, got shape {vector_x.shape}")
        lr_model = np.polyfit(vector_x, array_y, 1)
        r_squared = 1.0 - np.var(array_y - np.polyval(lr_model, vector_x)) / np.var(array_y)
        validate_scalar(r_squared, "R-squared", 0.0, 1.0)
        if r_squared == 1.0:
            raise ValueError("Perfect multicollinearity detected, VIF is undefined")
        vif = 1.0 / (1.0 - r_squared)
        validate_scalar(vif, "VIF", min_val=1.0)
        return vif, r_squared

    def calculate_vif_ivs(
        self,
        array_x: NDArray[np.float64],
        array_y: NDArray[np.float64],
        list_cols_iv: list[str],
        float_r_squared_mc_cut: float = 0.8,
    ) -> VifResults:
        """Calculate VIF for all independent variables to test multicollinearity.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Independent variables array, shape (n_samples, n_predictors).
        array_y : NDArray[np.float64]
            Dependent variable array, shape (n_samples,).
        list_cols_iv : list[str]
            List of independent variable names.
        float_r_squared_mc_cut : float, optional
            VIF threshold for multicollinearity, by default 0.8.

        Returns
        -------
        VifResults
            Dictionary with VIF, R-squared, and hypothesis test results.

        Raises
        ------
        ValueError
            If arrays are invalid, shapes mismatch, or threshold is invalid.
        """
        validate_array(array_x, "array_x", min_samples=2)
        validate_array(array_y, "array_y", min_samples=2)
        validate_scalar(float_r_squared_mc_cut, "float_r_squared_mc_cut", min_val=0.0)
        if array_x.shape[0] != array_y.shape[0]:
            raise ValueError(
                f"array_x and array_y must have the same number of samples, "
                f"got {array_x.shape[0]} and {array_y.shape[0]}"
            )
        if len(list_cols_iv) != array_x.shape[1]:
            raise ValueError(
                f"list_cols_iv length {len(list_cols_iv)} "
                + f"must match array_x columns {array_x.shape[1]}"
            )
        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)
        vif_results = [self.vif_iv(array_x[:, idx], array_y) for idx in range(array_x.shape[1])]
        return {
            "vif_ivs": {col: vif_results[idx][0] for idx, col in enumerate(list_cols_iv)},
            "r_squared_mc_ivs": {col: vif_results[idx][1] for idx, col in enumerate(list_cols_iv)},
            "h0": "lack of multicollinearity",
            "bl_reject_h0": {
                col: vif_results[idx][0] > float_r_squared_mc_cut 
                for idx, col in enumerate(list_cols_iv)
            },
        }

    def normality_error_dist_test(
        self, 
        array_x: NDArray[np.float64], 
        array_y: NDArray[np.float64], 
        alpha: float = 0.05
    ) -> NormalityTestResults:
        """Test normality of regression residuals using Anderson-Darling.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Independent variables array, shape (n_samples, n_predictors).
        array_y : NDArray[np.float64]
            Dependent variable array, shape (n_samples,).
        alpha : float, optional
            Significance level, by default 0.05.

        Returns
        -------
        NormalityTestResults
            Dictionary with Anderson-Darling test results.

        Raises
        ------
        ValueError
            If arrays are invalid, shapes mismatch, or alpha is not in [0, 1].
        """
        validate_array(array_x, "array_x", min_samples=2)
        validate_array(array_y, "array_y", min_samples=2)
        validate_scalar(alpha, "alpha", 0.0, 1.0)
        if array_x.shape[0] != array_y.shape[0]:
            raise ValueError(
                f"array_x and array_y must have the same number of samples, "
                f"got {array_x.shape[0]} and {array_y.shape[0]}"
            )
        model_fitted = sm.OLS(array_y, array_x).fit()
        return NormalityHT().anderson_darling(model_fitted.resid, alpha)

    def breusch_godfrey(
        self, 
        array_x: NDArray[np.float64], 
        array_y: NDArray[np.float64], 
        p: Optional[int] = None
    ) -> BreuschGodfreyResults:
        """Perform Breusch-Godfrey test for serial correlation.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Independent variables array, shape (n_samples, n_predictors).
        array_y : NDArray[np.float64]
            Dependent variable array, shape (n_samples,).
        p : Optional[int]
            Number of lags, by default n_predictors + 1.

        Returns
        -------
        BreuschGodfreyResults
            Dictionary with test statistic, p-value, and hypothesis result.

        Raises
        ------
        ValueError
            If arrays are invalid, shapes mismatch, or p is invalid.

        References
        ----------
        .. [1] https://en.wikipedia.org/wiki/Breusch%E2%80%93Godfrey_test
        """
        validate_array(array_x, "array_x", min_samples=2)
        validate_array(array_y, "array_y", min_samples=2)
        if array_x.shape[0] != array_y.shape[0]:
            raise ValueError(
                f"array_x and array_y must have the same number of samples, "
                f"got {array_x.shape[0]} and {array_y.shape[0]}"
            )
        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)
        if p is None:
            p = array_x.shape[1] + 1
        else:
            validate_scalar(p, "p", min_val=1.0)
            if not float(p).is_integer():
                raise ValueError(f"p must be an integer, got {p}")
        model_fitted = sm.OLS(array_y, array_x).fit()
        test_result = dg.acorr_breusch_godfrey(model_fitted, nlags=p)
        return {
            "breush_godfrey_tup": test_result,
            "h0": "no serial correlation of any order up to p",
            "bl_reject_h0": test_result[0] < test_result[1],
        }

    def durbin_watson_test(
        self, 
        array_x: NDArray[np.float64], 
        array_y: NDArray[np.float64]
    ) -> DurbinWatsonResults:
        """Perform Durbin-Watson test for autocorrelation in residuals.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Independent variables array, shape (n_samples, n_predictors).
        array_y : NDArray[np.float64]
            Dependent variable array, shape (n_samples,).

        Returns
        -------
        DurbinWatsonResults
            Dictionary with test statistic and hypothesis result.

        Raises
        ------
        ValueError
            If arrays are invalid or shapes mismatch.
        """
        validate_array(array_x, "array_x", min_samples=2)
        validate_array(array_y, "array_y", min_samples=2)
        if array_x.shape[0] != array_y.shape[0]:
            raise ValueError(
                f"array_x and array_y must have the same number of samples, "
                f"got {array_x.shape[0]} and {array_y.shape[0]}"
            )
        model_fitted = sm.OLS(array_y, array_x).fit()
        test_value = durbin_watson(model_fitted.resid)
        validate_scalar(test_value, "Durbin-Watson statistic", 0.0, 4.0)
        return {
            "test_value": test_value,
            "h0": "no autocorrelation among the residuals",
            "bl_reject_h0": (test_value <= 1.5) or (test_value >= 2.5),
        }

    def breusch_pagan_test(
        self, 
        array_x: NDArray[np.float64], 
        array_y: NDArray[np.float64], 
        alpha: float = 0.05
    ) -> BreuschPaganResults:
        """Perform Breusch-Pagan test for heteroscedasticity.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Independent variables array, shape (n_samples, n_predictors).
        array_y : NDArray[np.float64]
            Dependent variable array, shape (n_samples,).
        alpha : float, optional
            Significance level, by default 0.05.

        Returns
        -------
        BreuschPaganResults
            Dictionary with test statistic, p-value, and hypothesis result.

        Raises
        ------
        ValueError
            If arrays are invalid, shapes mismatch, or alpha is not in [0, 1].

        References
        ----------
        .. [1] https://stackoverflow.com/questions/30061054/ols-breusch-pagan-test-in-python
        """
        validate_array(array_x, "array_x", min_samples=2)
        validate_array(array_y, "array_y", min_samples=2)
        validate_scalar(alpha, "alpha", 0.0, 1.0)
        if array_x.shape[0] != array_y.shape[0]:
            raise ValueError(
                f"array_x and array_y must have the same number of samples, "
                f"got {array_x.shape[0]} and {array_y.shape[0]}"
            )
        if array_y.ndim != 1:
            raise ValueError(f"array_y must be 1D, got shape {array_y.shape}")
        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)
        lm = LinearRegression()
        lm.fit(array_x, array_y)
        err = (array_y - lm.predict(array_x)) ** 2
        lm.fit(array_x, err)
        pred_err = lm.predict(array_x)
        ss_tot = np.sum((err - np.mean(err)) ** 2)
        ss_res = np.sum((err - pred_err) ** 2)
        r_squared = 1.0 - (ss_res / ss_tot)
        validate_scalar(r_squared, "R-squared", 0.0, 1.0)
        lagrange_multiplier = array_x.shape[0] * r_squared
        validate_scalar(lagrange_multiplier, "Lagrange multiplier", min_val=0.0)
        p_value = stats.chi2.sf(lagrange_multiplier, array_x.shape[1])
        validate_scalar(p_value, "p-value", 0.0, 1.0)
        return {
            "lagrange_multiplier": lagrange_multiplier,
            "p_value": p_value,
            "h0": "homoscedasticity",
            "bl_reject_h0": alpha > p_value,
        }

    def cooks_distance(
        self, 
        array_x: NDArray[np.float64], 
        array_y: NDArray[np.float64]
    ) -> CooksDistanceResults:
        """Calculate Cook's distance for detecting influential data points.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Independent variables array, shape (n_samples, n_predictors).
        array_y : NDArray[np.float64]
            Dependent variable array, shape (n_samples,).

        Returns
        -------
        CooksDistanceResults
            Dictionary with Cook's distance and p-values.

        Raises
        ------
        ValueError
            If arrays are invalid or shapes mismatch.
        """
        validate_array(array_x, "array_x", min_samples=2)
        validate_array(array_y, "array_y", min_samples=2)
        if array_x.shape[0] != array_y.shape[0]:
            raise ValueError(
                f"array_x and array_y must have the same number of samples, "
                f"got {array_x.shape[0]} and {array_y.shape[0]}"
            )
        model_fitted = sm.OLS(array_y, array_x).fit()
        influence = model_fitted.get_influence()
        return {
            "distance": influence.cooks_distance[0],
            "p_value": influence.cooks_distance[1],
        }

    def test_joint_coeff(
        self, 
        sse_r: float, 
        sse_unr: float, 
        q: int, 
        n: int, 
        k: int
    ) -> float:
        """Perform F-test for joint coefficients.

        Parameters
        ----------
        sse_r : float
            Sum of squared errors for restricted model.
        sse_unr : float
            Sum of squared errors for unrestricted model.
        q : int
            Number of restrictions.
        n : int
            Number of observations.
        k : int
            Number of independent variables.

        Returns
        -------
        float
            F-statistic for joint coefficients.

        Raises
        ------
        ValueError
            If inputs are invalid or out of range.
        """
        validate_scalar(sse_r, "sse_r", min_val=0.0)
        validate_scalar(sse_unr, "sse_unr", min_val=0.0)
        validate_scalar(q, "q", min_val=1.0)
        validate_scalar(n, "n", min_val=2.0)
        validate_scalar(k, "k", min_val=1.0)
        if not float(q).is_integer():
            raise ValueError(f"q must be an integer, got {q}")
        if not float(n).is_integer():
            raise ValueError(f"n must be an integer, got {n}")
        if not float(k).is_integer():
            raise ValueError(f"k must be an integer, got {k}")
        if n <= k + 1:
            raise ValueError(f"n must be greater than k + 1, got n={n}, k={k}")
        if sse_unr == 0:
            raise ValueError("sse_unr cannot be zero")
        return (sse_r - sse_unr) / q / (sse_unr / (n - k - 1))


class NormalityHT(metaclass=TypeChecker):
    """Class for normality hypothesis tests."""

    def kolmogorov_smirnov_test(
        self,
        array_x: NDArray[np.float64],
        alpha: float = 0.05,
        factor_empirical_func: float = 10.0,
    ) -> KolmogorovSmirnovResults:
        """Perform Kolmogorov-Smirnov test for normality.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Input data array.
        alpha : float, optional
            Significance level, by default 0.05.
        factor_empirical_func : float, optional
            Factor for empirical distribution function, by default 10.0.

        Returns
        -------
        KolmogorovSmirnovResults
            Dictionary with test statistic, critical value, and hypothesis result.
        """
        validate_array(array_x, "array_x", min_samples=2)
        validate_scalar(alpha, "alpha", 0.0, 1.0)
        validate_scalar(factor_empirical_func, "factor_empirical_func", min_val=0.0)
        sorted_x = np.sort(array_x)
        stats_desc = StatisticalDescription().statistical_description(sorted_x)
        mean_val = stats_desc["mean"]
        std_dev = stats_desc["standard_deviation_sample"]
        empirical_func = np.arange(1, len(sorted_x) + 1) / factor_empirical_func
        cdf_vals = np.array(
            [NormalDistribution().cdf(x, mean_val, std_dev) for x in sorted_x]
        )
        test_1 = np.abs(empirical_func - cdf_vals)
        test_2 = np.array([cdf_vals[0]] + [cdf_vals[i] - empirical_func[i - 1] 
                                           for i in range(1, len(sorted_x))])
        dn = float(np.max([np.max(test_1), np.max(test_2)]))
        critical_value = stats.ksone.ppf(1 - alpha / 2, len(sorted_x))
        return {
            "dn": dn,
            "critical_value": critical_value,
            "h0": "normally distributed data",
            "reject_h0": dn > critical_value,
        }

    def anderson_darling(
        self, 
        array_data: NDArray[np.float64], 
        alpha: float = 0.05
    ) -> NormalityTestResults:
        """Perform Anderson-Darling test for normality.

        Parameters
        ----------
        array_data : NDArray[np.float64]
            Input data array.
        alpha : float, optional
            Significance level, by default 0.05.

        Returns
        -------
        NormalityTestResults
            Dictionary with alpha, p-value, and hypothesis result.

        References
        ----------
        .. [1] http://www.uel.br/projetos/experimental/pages/arquivos/Anderson_Darling.html
        """
        validate_array(array_data, "array_data", min_samples=2)
        validate_scalar(alpha, "alpha", 0.0, 1.0)
        sorted_data = np.sort(array_data)
        stats_desc = StatisticalDescription().statistical_description(sorted_data)
        mean_val = stats_desc["mean"]
        std_dev = stats_desc["standard_deviation_sample"]
        d_array = [
            (2 * (i + 1) - 1) * np.log(NormalDistribution().cdf(sorted_data[i], mean_val, std_dev))
            + (2 * (len(sorted_data) - (i + 1)) + 1)
            * np.log(1 - NormalDistribution().cdf(sorted_data[i], mean_val, std_dev))
            for i in range(len(sorted_data))
        ]
        a_2 = -len(sorted_data) - np.sum(d_array) / len(sorted_data)
        a_m_2 = a_2 * (1 + 0.75 / len(sorted_data) + 2.25 / len(sorted_data) ** 2)
        if a_m_2 < 0.2:
            p_value = 1 - np.exp(-13.436 + 101.14 * a_m_2 - 223.73 * a_m_2**2)
        elif a_m_2 < 0.340:
            p_value = 1 - np.exp(-8.318 + 42.796 * a_m_2 - 59.938 * a_m_2**2)
        elif a_m_2 < 0.6:
            p_value = np.exp(0.9177 - 4.279 * a_m_2 - 1.38 * a_m_2**2)
        else:
            p_value = np.exp(1.2937 - 5.709 * a_m_2 + 0.0186 * a_m_2**2)
        validate_scalar(p_value, "p-value", 0.0, 1.0)
        return {
            "alpha": alpha,
            "p_value": p_value,
            "h0": "normally distributed data",
            "reject_h0": alpha > p_value,
        }

    def shapiro_wilk(
        self, 
        array_data: NDArray[np.float64], 
        alpha: float = 0.05
    ) -> NormalityTestResults:
        """Perform Shapiro-Wilk test for normality.

        Parameters
        ----------
        array_data : NDArray[np.float64]
            Input data array.
        alpha : float, optional
            Significance level, by default 0.05.

        Returns
        -------
        NormalityTestResults
            Dictionary with alpha, p-value, and hypothesis result.

        References
        ----------
        .. [1] https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.shapiro.html
        """
        validate_array(array_data, "array_data", min_samples=2)
        validate_scalar(alpha, "alpha", 0.0, 1.0)
        sorted_data = np.sort(array_data)
        p_value = stats.shapiro(sorted_data).pvalue
        validate_scalar(p_value, "p-value", 0.0, 1.0)
        return {
            "alpha": alpha,
            "p_value": p_value,
            "h0": "normally distributed data",
            "reject_h0": alpha > p_value,
        }

    def d_agostinos_k2(
        self, 
        array_data: NDArray[np.float64], 
        alpha: float = 0.05
    ) -> NormalityTestResults:
        """Perform D'Agostino's K² test for normality.

        Parameters
        ----------
        array_data : NDArray[np.float64]
            Input data array.
        alpha : float, optional
            Significance level, by default 0.05.

        Returns
        -------
        NormalityTestResults
            Dictionary with alpha, p-value, and hypothesis result.

        References
        ----------
        .. [1] https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        """
        validate_array(array_data, "array_data", min_samples=2)
        validate_scalar(alpha, "alpha", 0.0, 1.0)
        sorted_data = np.sort(array_data)
        p_value = stats.normaltest(sorted_data).pvalue
        validate_scalar(p_value, "p-value", 0.0, 1.0)
        return {
            "alpha": alpha,
            "p_value": p_value,
            "h0": "normally distributed data",
            "reject_h0": alpha > p_value,
        }


class CorrelationHT(metaclass=TypeChecker):
    """Class for correlation hypothesis tests."""

    def pearsons_correlation_coefficient(
        self, 
        x1_array: NDArray[np.float64], 
        x2_array: NDArray[np.float64], 
        alpha: float = 0.05
    ) -> CorrelationTestResults:
        """Perform Pearson's correlation coefficient test.

        Parameters
        ----------
        x1_array : NDArray[np.float64]
            First input data array.
        x2_array : NDArray[np.float64]
            Second input data array.
        alpha : float, optional
            Significance level, by default 0.05.

        Returns
        -------
        CorrelationTestResults
            Dictionary with alpha, p-value, and hypothesis result.

        Raises
        ------
        ValueError
            If x1_array and x2_array have different shapes

        References
        ----------
        .. [1] https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        """
        validate_array(x1_array, "x1_array", min_samples=2)
        validate_array(x2_array, "x2_array", min_samples=2)
        validate_scalar(alpha, "alpha", 0.0, 1.0)
        if x1_array.shape != x2_array.shape:
            raise ValueError(
                "x1_array and x2_array must have the same shape, "
                + f"got {x1_array.shape} and {x2_array.shape}"
            )
        sorted_x1 = np.sort(x1_array)
        sorted_x2 = np.sort(x2_array)
        p_value = stats.pearsonr(sorted_x1, sorted_x2).pvalue
        validate_scalar(p_value, "p-value", 0.0, 1.0)
        return {
            "alpha": alpha,
            "p_value": p_value,
            "reject_h0": alpha > p_value,
        }

    def spearmans_rank_correlation(
        self, 
        x1_array: NDArray[np.float64], 
        x2_array: NDArray[np.float64], 
        alpha: float = 0.05
    ) -> CorrelationTestResults:
        """Perform Spearman's rank correlation test.

        Parameters
        ----------
        x1_array : NDArray[np.float64]
            First input data array.
        x2_array : NDArray[np.float64]
            Second input data array.
        alpha : float, optional
            Significance level, by default 0.05.

        Returns
        -------
        CorrelationTestResults
            Dictionary with alpha, p-value, and hypothesis result.

        Raises
        ------
        ValueError
            If x1_array and x2_array have different shapes
        
        References
        ----------
        .. [1] https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        """
        validate_array(x1_array, "x1_array", min_samples=2)
        validate_array(x2_array, "x2_array", min_samples=2)
        validate_scalar(alpha, "alpha", 0.0, 1.0)
        if x1_array.shape != x2_array.shape:
            raise ValueError(
                "x1_array and x2_array must have the same shape, "
                + f"got {x1_array.shape} and {x2_array.shape}"
            )
        sorted_x1 = np.sort(x1_array)
        sorted_x2 = np.sort(x2_array)
        p_value = stats.spearmanr(sorted_x1, sorted_x2).pvalue
        validate_scalar(p_value, "p-value", 0.0, 1.0)
        return {
            "alpha": alpha,
            "p_value": p_value,
            "reject_h0": alpha > p_value,
        }

    def kendalls_rank_correlation(
        self, 
        x1_array: NDArray[np.float64], 
        x2_array: NDArray[np.float64], 
        alpha: float = 0.05
    ) -> CorrelationTestResults:
        """Perform Kendall's rank correlation test.

        Parameters
        ----------
        x1_array : NDArray[np.float64]
            First input data array.
        x2_array : NDArray[np.float64]
            Second input data array.
        alpha : float, optional
            Significance level, by default 0.05.

        Returns
        -------
        CorrelationTestResults
            Dictionary with alpha, p-value, and hypothesis result.

        Raises
        ------
        ValueError
            If x1_array and x2_array have different shapes

        References
        ----------
        .. [1] https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        """
        validate_array(x1_array, "x1_array", min_samples=2)
        validate_array(x2_array, "x2_array", min_samples=2)
        validate_scalar(alpha, "alpha", 0.0, 1.0)
        if x1_array.shape != x2_array.shape:
            raise ValueError(
                "x1_array and x2_array must have the same shape, "
                + f"got {x1_array.shape} and {x2_array.shape}"
            )
        sorted_x1 = np.sort(x1_array)
        sorted_x2 = np.sort(x2_array)
        p_value = stats.kendalltau(sorted_x1, sorted_x2).pvalue
        validate_scalar(p_value, "p-value", 0.0, 1.0)
        return {
            "alpha": alpha,
            "p_value": p_value,
            "reject_h0": alpha > p_value,
        }

    def chi_squared_test(
        self, 
        x1_array: NDArray[np.float64], 
        x2_array: NDArray[np.float64], 
        alpha: float = 0.05
    ) -> CorrelationTestResults:
        """Perform chi-squared test for independence of categorical variables.

        Parameters
        ----------
        x1_array : NDArray[np.float64]
            First input data array.
        x2_array : NDArray[np.float64]
            Second input data array.
        alpha : float, optional
            Significance level, by default 0.05.

        Returns
        -------
        CorrelationTestResults
            Dictionary with alpha, p-value, and hypothesis result.

        Raises
        ------
        ValueError
            If x1_array and x2_array have different shapes

        References
        ----------
        .. [1] https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        """
        validate_array(x1_array, "x1_array", min_samples=2)
        validate_array(x2_array, "x2_array", min_samples=2)
        validate_scalar(alpha, "alpha", 0.0, 1.0)
        if x1_array.shape != x2_array.shape:
            raise ValueError(
                "x1_array and x2_array must have the same shape, "
                + f"got {x1_array.shape} and {x2_array.shape}"
            )
        sorted_x1 = np.sort(x1_array)
        sorted_x2 = np.sort(x2_array)
        p_value = stats.chi2_contingency(np.vstack((sorted_x1, sorted_x2)).T).pvalue
        validate_scalar(p_value, "p-value", 0.0, 1.0)
        return {
            "alpha": alpha,
            "p_value": p_value,
            "reject_h0": alpha > p_value,
        }


class StationaryHT(metaclass=TypeChecker):
    """Class for stationarity hypothesis tests."""

    def augmented_dickey_fuller(
        self, 
        array_x: NDArray[np.float64], 
        alpha: float = 0.05
    ) -> StationarityTestResults:
        """Perform Augmented Dickey-Fuller test for stationarity.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Time series data array.
        alpha : float, optional
            Significance level, by default 0.05.

        Returns
        -------
        StationarityTestResults
            Dictionary with test statistic, p-value, lags, and hypothesis result.

        References
        ----------
        .. [1] https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        """
        validate_array(array_x, "array_x", min_samples=2)
        validate_scalar(alpha, "alpha", 0.0, 1.0)
        sorted_x = np.sort(array_x)
        test_result = sm.tsa.stattools.adfuller(sorted_x)
        p_value = test_result[1]
        validate_scalar(p_value, "p-value", 0.0, 1.0)
        return {
            "statistic": test_result[0],
            "p_value": p_value,
            "lags": test_result[2],
            "criteria": test_result[4],
            "reject_h0": alpha > p_value,
        }

    def kwiatkowski_phillips_schmidt_shin(
        self, 
        array_x: NDArray[np.float64], 
        alpha: float = 0.05
    ) -> StationarityTestResults:
        """Perform KPSS test for trend stationarity.

        Parameters
        ----------
        array_x : NDArray[np.float64]
            Time series data array.
        alpha : float, optional
            Significance level, by default 0.05.

        Returns
        -------
        StationarityTestResults
            Dictionary with test statistic, p-value, lags, and hypothesis result.

        References
        ----------
        .. [1] https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        """
        validate_array(array_x, "array_x", min_samples=2)
        validate_scalar(alpha, "alpha", 0.0, 1.0)
        sorted_x = np.sort(array_x)
        test_result = sm.tsa.stattools.kpss(sorted_x)
        p_value = test_result[1]
        validate_scalar(p_value, "p-value", 0.0, 1.0)
        return {
            "statistic": test_result[0],
            "p_value": p_value,
            "lags": test_result[2],
            "criteria": test_result[3],
            "reject_h0": alpha > p_value,
        }


class MeansHT(metaclass=TypeChecker):
    """Class for means hypothesis tests."""

    def student_s_t_test(
        self, 
        x1_array: NDArray[np.float64], 
        x2_array: NDArray[np.float64], 
        alpha: float = 0.05
    ) -> CorrelationTestResults:
        """Perform Student's t-test for independent samples.

        Parameters
        ----------
        x1_array : NDArray[np.float64]
            First input data array.
        x2_array : NDArray[np.float64]
            Second input data array.
        alpha : float, optional
            Significance level, by default 0.05.

        Returns
        -------
        CorrelationTestResults
            Dictionary with alpha, p-value, and hypothesis result.

        Raises
        ------
        ValueError
            If x1_array and x2_array have different shapes.

        References
        ----------
        .. [1] https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        """
        validate_array(x1_array, "x1_array", min_samples=2)
        validate_array(x2_array, "x2_array", min_samples=2)
        validate_scalar(alpha, "alpha", 0.0, 1.0)
        if x1_array.shape != x2_array.shape:
            raise ValueError(
                "x1_array and x2_array must have the same shape, "
                + f"got {x1_array.shape} and {x2_array.shape}"
            )
        sorted_x1 = np.sort(x1_array)
        sorted_x2 = np.sort(x2_array)
        p_value = stats.ttest_ind(sorted_x1, sorted_x2).pvalue
        validate_scalar(p_value, "p-value", 0.0, 1.0)
        return {
            "alpha": alpha,
            "p_value": p_value,
            "reject_h0": alpha > p_value,
        }

    def paired_student_s_t_test(
        self, 
        x1_array: NDArray[np.float64], 
        x2_array: NDArray[np.float64], 
        alpha: float = 0.05
    ) -> CorrelationTestResults:
        """Perform paired Student's t-test.

        Parameters
        ----------
        x1_array : NDArray[np.float64]
            First input data array.
        x2_array : NDArray[np.float64]
            Second input data array.
        alpha : float, optional
            Significance level, by default 0.05.

        Returns
        -------
        CorrelationTestResults
            Dictionary with alpha, p-value, and hypothesis result.

        Raises
        ------
        ValueError
            If x1_array and x2_array have different shapes.

        References
        ----------
        .. [1] https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        """
        validate_array(x1_array, "x1_array", min_samples=2)
        validate_array(x2_array, "x2_array", min_samples=2)
        validate_scalar(alpha, "alpha", 0.0, 1.0)
        if x1_array.shape != x2_array.shape:
            raise ValueError(
                "x1_array and x2_array must have the same shape, "
                + f"got {x1_array.shape} and {x2_array.shape}"
            )
        sorted_x1 = np.sort(x1_array)
        sorted_x2 = np.sort(x2_array)
        p_value = stats.ttest_rel(sorted_x1, sorted_x2).pvalue
        validate_scalar(p_value, "p-value", 0.0, 1.0)
        return {
            "alpha": alpha,
            "p_value": p_value,
            "reject_h0": alpha > p_value,
        }


class StatisticalDistributionsHT(metaclass=TypeChecker):
    """Class for statistical distribution hypothesis tests."""

    def mann_whitney_u_test(
        self, 
        x1_array: NDArray[np.float64], 
        x2_array: NDArray[np.float64], 
        alpha: float = 0.05
    ) -> CorrelationTestResults:
        """Perform Mann-Whitney U test for distribution equality.

        Parameters
        ----------
        x1_array : NDArray[np.float64]
            First input data array.
        x2_array : NDArray[np.float64]
            Second input data array.
        alpha : float, optional
            Significance level, by default 0.05.

        Returns
        -------
        CorrelationTestResults
            Dictionary with alpha, p-value, and hypothesis result.

        Raises
        ------
        ValueError
            If x1_array and x2_array have different shapes.

        References
        ----------
        .. [1] https://machinelearningmastery.com/nonparametric-statistical-significance-tests-in-python/
        """
        validate_array(x1_array, "x1_array", min_samples=2)
        validate_array(x2_array, "x2_array", min_samples=2)
        validate_scalar(alpha, "alpha", 0.0, 1.0)
        if x1_array.shape != x2_array.shape:
            raise ValueError(
                "x1_array and x2_array must have the same shape, "
                + f"got {x1_array.shape} and {x2_array.shape}"
            )
        sorted_x1 = np.sort(x1_array)
        sorted_x2 = np.sort(x2_array)
        p_value = stats.mannwhitneyu(sorted_x1, sorted_x2).pvalue
        validate_scalar(p_value, "p-value", 0.0, 1.0)
        return {
            "alpha": alpha,
            "p_value": p_value,
            "reject_h0": alpha > p_value,
        }

    def wilcoxon_signed_rank_test(
        self, 
        x1_array: NDArray[np.float64], 
        x2_array: NDArray[np.float64], 
        alpha: float = 0.05
    ) -> CorrelationTestResults:
        """Perform Wilcoxon signed-rank test for paired samples.

        Parameters
        ----------
        x1_array : NDArray[np.float64]
            First input data array.
        x2_array : NDArray[np.float64]
            Second input data array.
        alpha : float, optional
            Significance level, by default 0.05.

        Returns
        -------
        CorrelationTestResults
            Dictionary with alpha, p-value, and hypothesis result.

        Raises
        ------
        ValueError
            If x1_array and x2_array have different shapes.

        References
        ----------
        .. [1] https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        """
        validate_array(x1_array, "x1_array", min_samples=2)
        validate_array(x2_array, "x2_array", min_samples=2)
        validate_scalar(alpha, "alpha", 0.0, 1.0)
        if x1_array.shape != x2_array.shape:
            raise ValueError(
                "x1_array and x2_array must have the same shape, "
                + f"got {x1_array.shape} and {x2_array.shape}"
            )
        sorted_x1 = np.sort(x1_array)
        sorted_x2 = np.sort(x2_array)
        p_value = stats.wilcoxon(sorted_x1, sorted_x2).pvalue
        validate_scalar(p_value, "p-value", 0.0, 1.0)
        return {
            "alpha": alpha,
            "p_value": p_value,
            "reject_h0": alpha > p_value,
        }

    def kruskal_wallis_h_test(
        self, 
        x1_array: NDArray[np.float64], 
        x2_array: NDArray[np.float64], 
        alpha: float = 0.05
    ) -> CorrelationTestResults:
        """Perform Kruskal-Wallis H test for distribution equality.

        Parameters
        ----------
        x1_array : NDArray[np.float64]
            First input data array.
        x2_array : NDArray[np.float64]
            Second input data array.
        alpha : float, optional
            Significance level, by default 0.05.

        Returns
        -------
        CorrelationTestResults
            Dictionary with alpha, p-value, and hypothesis result.

        Raises
        ------
        ValueError
            If x1_array and x2_array have different shapes.

        References
        ----------
        .. [1] https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        """
        validate_array(x1_array, "x1_array", min_samples=2)
        validate_array(x2_array, "x2_array", min_samples=2)
        validate_scalar(alpha, "alpha", 0.0, 1.0)
        if x1_array.shape != x2_array.shape:
            raise ValueError(
                "x1_array and x2_array must have the same shape, "
                + f"got {x1_array.shape} and {x2_array.shape}"
            )
        sorted_x1 = np.sort(x1_array)
        sorted_x2 = np.sort(x2_array)
        p_value = stats.kruskal(sorted_x1, sorted_x2).pvalue
        validate_scalar(p_value, "p-value", 0.0, 1.0)
        return {
            "alpha": alpha,
            "p_value": p_value,
            "reject_h0": alpha > p_value,
        }

    def friedman_test(
        self, 
        x1_array: NDArray[np.float64], 
        x2_array: NDArray[np.float64], 
        alpha: float = 0.05
    ) -> CorrelationTestResults:
        """Perform Friedman test for paired samples.

        Parameters
        ----------
        x1_array : NDArray[np.float64]
            First input data array.
        x2_array : NDArray[np.float64]
            Second input data array.
        alpha : float, optional
            Significance level, by default 0.05.

        Returns
        -------
        CorrelationTestResults
            Dictionary with alpha, p-value, and hypothesis result.

        Raises
        ------
        ValueError
            If x1_array and x2_array have different shapes.

        References
        ----------
        .. [1] https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        """
        validate_array(x1_array, "x1_array", min_samples=2)
        validate_array(x2_array, "x2_array", min_samples=2)
        validate_scalar(alpha, "alpha", 0.0, 1.0)
        if x1_array.shape != x2_array.shape:
            raise ValueError(
                "x1_array and x2_array must have the same shape, "
                + f"got {x1_array.shape} and {x2_array.shape}"
            )
        sorted_x1 = np.sort(x1_array)
        sorted_x2 = np.sort(x2_array)
        p_value = stats.friedmanchisquare(sorted_x1, sorted_x2).pvalue
        validate_scalar(p_value, "p-value", 0.0, 1.0)
        return {
            "alpha": alpha,
            "p_value": p_value,
            "reject_h0": alpha > p_value,
        }

    def benford_law(
        self, 
        array_data: NDArray[np.float64], 
        bl_list_number_occurrencies: bool = False
    ) -> BenfordResults:
        """Apply Benford's Law for fraud detection by evaluating first digit occurrences.

        Parameters
        ----------
        array_data : NDArray[np.float64]
            Input data array, either raw numbers or counts of first digits (1-9).
        bl_list_number_occurrencies : bool, optional
            If True, array_data contains counts of first digits; if False, raw numbers, \
                by default False.

        Returns
        -------
        BenfordResults
            Dictionary with expected and observed first digit distributions.

        Raises
        ------
        TypeError
            If bl_list_number_occurrencies is not a boolean.
        ValueError
            If boolean list is not either True or False.

        References
        ----------
        .. [1] https://brilliant.org/wiki/benfords-law/
        """
        if not isinstance(bl_list_number_occurrencies, bool):
            raise TypeError("bl_list_number_occurrencies must be a boolean")
        validate_array(array_data, "array_data", min_samples=1)

        array_benford_expected = np.array([np.log10(i + 2) - np.log10(i + 1) for i in range(9)])
        array_percentual_occurrence = np.zeros(9)

        if not bl_list_number_occurrencies:
            list_first_digits: list[int] = []
            for x in array_data:
                abs_x = abs(x)
                if abs_x == 0:
                    continue
                # normalize to get first digit
                while abs_x >= 10:
                    abs_x /= 10
                while abs_x < 1:
                    abs_x *= 10
                first_digit = int(abs_x)
                if first_digit != 0:
                    list_first_digits.append(first_digit)
            if not list_first_digits:
                raise ValueError("No valid non-zero first digits found in array_data")
            for i in range(1, 10):
                array_percentual_occurrence[i - 1] = list_first_digits.count(i) \
                    / len(list_first_digits)
        else:
            if array_data.sum() == 0:
                raise ValueError(
                    "Sum of array_data cannot be zero when bl_list_number_occurrencies is True")
            for i in range(9):
                array_percentual_occurrence[i] = array_data[i] / array_data.sum()
                validate_scalar(
                    array_percentual_occurrence[i], 
                    f"array_percentual_occurrence[{i+1}]", 
                    min_val=0.0, 
                    max_val=1.0
                )

        return {
            "benford_expected_array": array_benford_expected,
            "real_numbers_observed_array": array_percentual_occurrence,
        }


class IndependenceHT(metaclass=TypeChecker):
    """Class for independence hypothesis tests."""

    def pearson_chi_squared(
        self, array_y: NDArray[np.float64], float_significance: float = 0.05
    ) -> PearsonChiSquaredResults:
        """Perform Pearson's chi-squared test for independence.

        Parameters
        ----------
        array_y : NDArray[np.float64]
            Contingency table array.
        float_significance : float, optional
            Significance level, by default 0.05.

        Returns
        -------
        PearsonChiSquaredResults
            Dictionary with test statistic, p-value, degrees of freedom, expected values, and \
                hypothesis result.
        """
        validate_array(array_y, "array_y", min_samples=2)
        validate_scalar(float_significance, "float_significance", 0.0, 1.0)
        test_result = stats.chi2_contingency(array_y)
        p_value = test_result[1]
        validate_scalar(p_value, "p-value", 0.0, 1.0)
        return {
            "chi_squard_statistic": test_result[0],
            "p_value": p_value,
            "degrees_freedom": test_result[2],
            "array_expected_values": test_result[3],
            "significance": float_significance,
            "h0_hypothesis": "independent",
            "reject_h0": float_significance > p_value,
        }