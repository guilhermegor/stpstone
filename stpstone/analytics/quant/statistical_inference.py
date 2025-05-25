### STATISTICAL HYPOTHESIS TESTS ###

import numpy as np
import statsmodels.api as sm
import statsmodels.stats.diagnostic as dg
from scipy import stats
stats.chisqprob = lambda chisq, df: stats.chi2.sf(chisq, df)
from sklearn.linear_model import LinearRegression
from statsmodels.stats.stattools import durbin_watson
from stpstone.analytics.quant.statistical_inference import StatiscalDescription
from stpstone.analytics.quant.prob_distributions import NormalDistribution
from stpstone.transformations.cleaner.eda import ExploratoryDataAnalysis


class MultipleRegressionHT:

    def anova(self, array_x, array_y):
        """
        REFERENCES: https://medium.com/swlh/interpreting-linear-regression-through-statsmodels-summary-4796d359035a
        DOCSTRING: ANOVA - ANALYSIS OF VARIANCE
        INPUTS:
        OUTPUTS:
        """
        # fitting multiple regression model
        model_fitted = sm.OLS(array_y, array_x).fit()
        # returning stats
        return model_fitted.summary()

    def linearity_test(self, array_x, array_y, list_cols_iv, r_squared_cut=0.8):
        """
        DOCSTRING: LINEARITY TEST - LINEAR RELATIONSHIP BETWEEN DV AND EACH IV
        INPUTS: ARRAY X, ARRAY Y, R-SQUARED CUT
        OUTPUTS: DICTIONARY
        """
        # setting variables
        list_linearity = list()
        # checking wheter the array is unidimensional and reshaping it
        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)
        # looping through each iv
        for idx in range(array_x.shape[1]):
            # fit a simple linear regression model
            lr_model = np.polyfit(array_x[:, idx], array_y, 1)
            # r squared caclulation
            float_r_squared = 1.0 - np.var(array_y - np.polyval(lr_model, array_x[:, idx])) \
                / np.var(array_x[:, idx])
            list_linearity.append(float_r_squared)
        # print(list_linearity)
        return {
            'r_squared': {list_cols_iv[idx]: list_linearity[idx] for idx in
                          range(len(list_linearity))},
            'h0': 'linear relationship between X and y',
            'bl_reject_h0': {list_cols_iv[idx]: list_linearity[idx] < r_squared_cut for idx in
                          range(len(list_linearity))}
        }

    def vif_iv(self, vector_x, array_y):
        """
        DOCSTRING: VARIANCE INFLATION FACTOR - INDEPENDENCE OF INDEPENDENT VARIABLES - VIOLATION:
            MULTICOLLINEARITY; ISSUE: INFLATED STANDARD ERRORS; DETECTION: VARIANCE INFLATION
            FACTOR; CORRECTION: REVISE MODEL, INCREASE SAMPLE SIZE
        INPUTS
        """
        # fit a simple linear regression model
        lr_model = np.polyfit(vector_x, array_y, 1)
        # r squared caclulation
        float_r_squared = 1.0 - np.var(array_y - np.polyval(lr_model, vector_x)) / np.var(vector_x)
        # calculate individual vif
        float_vif = 1.0 / (1.0 - float_r_squared)
        # return vif
        return float_vif, float_r_squared

    def calculate_vif_ivs(self, array_x, array_y, list_cols_iv, float_r_squared_mc_cut=0.8):
        """
        DOCSTRING: VARIANCE INFLATION FACTOR - IF VIF SCORE FOR A VARIABLE X_J IS HIGH, THEN THE
            VARIANCE EXPLAINED BY THAT VARIABLE IS CAPTURED (ALMOST COMPLETELY) BY ALL OTHER
            VARIABLES --> TEST FOR MULTICOLLINEARITY ASSUMPTION OF LINEAR REGRESSION
        INPUTS: ARRAY INDEPENDENT AND DEPENDENT
        OUTPUTS:
        """
        # checking wheter the array is unidimensional and reshaping it
        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)
        # iterating through array iv columns to calculate vif regarding one iv and the dv
        tups_vif_ivs = [self.vif_iv(array_x[:, idx], array_y) for idx in range(array_x.shape[1])]
        # return vif for ivs
        return {
            'vif_ivs': {col_: tups_vif_ivs[idx][0] for idx, col_ in enumerate(list_cols_iv)},
            'r_squared_mc_ivs': {col_: tups_vif_ivs[idx][1] for idx, col_ in
                                 enumerate(list_cols_iv)},
            'h0': 'lack of multicollinearity',
            'bl_reject_h0': {col_: tups_vif_ivs[idx][0] > float_r_squared_mc_cut for idx, col_ in
                                 enumerate(list_cols_iv)}
        }

    def normality_error_dist_test(self, array_x, array_y, alpha=0.05):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # fitting multiple regression model
        model_fitted = sm.OLS(array_y, array_x).fit()
        # anderson darling normality check of error's distribution
        dict_anderson_darling = NormalityHT().anderson_darling(model_fitted.resid, alpha)
        return dict_anderson_darling

    def breusch_godfrey(self, array_x, array_y, p=None):
        """
        REFERENCES: https://en.wikipedia.org/wiki/Breusch%E2%80%93Godfrey_test
        DOCSTRING: BREUSCH GODFREY TEST ASSESS INDEPENDENCE (ABSENSE OF SERIAL CORRELATION, OR
            AUTOCORRELATION); IF REJECTED, IT WOULD MEAN THAT INCORRECT CONCLUSIONS WOULD BE DRAWN
            FROM OTHER TESTS OR THAT SUB-OPTIMAL ESTIMATES OF MODEL PARAMETERS WOULD BE OBTAINED
            - THE REGRESSION MODELS TO WHICH THE TEST CAN BE APPLIED INCLUDE CASES WHERE LAGGED
                VALUESOF THE DEPENDENT VARIABLES ARE USED AS INDEPENDENT VARIABLES IN THE MODEL'S
                REPRESENTATION FOR LATER OBSERVATIONS
            - TEST FOR AUTOCORRELATION IN THE ERRORS OF A REGRESSION MODEL
            - H0: NO SERIAL CORRELATION OF ANY ORDER UP TO P
            - HA: SERIAL CORRELATION OF ANY ORDER UP TO P
        INPUTS: ARRAY INDEPENDENT, DEPENDENT
        OUTPUTS: DICTIONARY WITH BREUSH GODFREY TEST OUTPUT (TUPPLE, BEING THE FIRST VALUE THE TEST
            STATISTIC AND THE SECOND THE P-VALUE, WHICH OUGHT BE LESS THAN THE TEST STATISTIC TO
            ACCEPT THE H0 HYPOTHESIS)
        """
        # checking wheter the array is unidimensional and reshaping it
        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)
        # defining the order p of the test
        if p is None:
            p = array_x.shape[1] + 1
        # fitting multiple regression model
        model_fitted = sm.OLS(array_y, array_x).fit()
        # autocorrelation test for time-series residuals at order p
        tup_dg = dg.acorr_breusch_godfrey(model_fitted, nlags=p)
        # return dictionary
        return {
            'breush_godfrey_tup': tup_dg,
            'h0': 'no serial correlation of any order up to p',
            'bl_reject_h0': tup_dg[0] < tup_dg[1]
        }

    def durbin_watson_test(self, array_x, array_y):
        """
        REFERENCES: https://www.statology.org/durbin-watson-test-python/
        DOCSTRING: DURBIN-WATSON SERIAL CORRELATION: ONE OF THE ASSUMPTIONS OF LINEAR REGRESSION
            IS THAT THERE IS NO CORRELATION BETWEEN THE RESIDUALS (ASSUMED TO BE INDEPENDENT),
            ONE WAY TO DETERMINE WHETER THIS ASSUMPTION IS MET IS TO PERFORM A
            DURBIN-WATSON TEST, WHICH IS USED TO DETECT THE PRESENCE OF AUTOCORRELATION IN
            THE RESIDUALS OF A REGRESSION
            A. H0: NO CORRELATION AMONG THE RESIDUALS
            B. HA: THE RESIDUALS ARE AUTOCORRELATED
            C. VALUES: 0 TO 4, WHERE A TEST STATISTIC OF 2 INDICATES NO SERIAL CORRELATION,
                THE CLOSER THE TEST STATISTIC IS TO 0, THE MORE EVIDENCE OF POSITIVE SERIAL
                CORRELATION, THE CLOSER THE TEST STATISTIC IS TO 4, THE MORE EVIDENCE OF NEGATIVE
                SERIAL CORRELATION
            D. RULE OF THUMB: TEST STATISTIC VALUES BETWEEN 1.5 AND 2.5 ARE CONSIDERED NORMAL,
                HOWEVER, VALUES OUTSIDE OF THIS RANGE COULD INDICATE THAT AUTOCORRELATION IS A
                PROBLEM
            E. DURBIN-WATSON IS A TEST OF SERIAL CORRELATION
        INPUTS: ARRAY INDEPENDENT AND DEPENDENT
        OUTPUTS: DICTIONARY
        """
        # fitting multiple regression model
        model_fitted = sm.OLS(array_y, array_x).fit()
        # durbin watson test value
        float_dw_test_value = durbin_watson(model_fitted.resid)
        # test
        return {
            'test_value': float_dw_test_value,
            'h0': 'no autocorrelation among the residuals',
            'bl_reject_h0': (float_dw_test_value <= 1.5) or (float_dw_test_value >= 2.5)
        }

    def breusch_pagan_test(self, array_x, array_y, alpha=0.05):
        """
        REFERENCES: https://stackoverflow.com/questions/30061054/ols-breusch-pagan-test-in-python
        DOCSTRING: BREUSCH-PAGAN: TEST WHETER HETEROSCEDASTICITY EXISTS IN REGRESSION ANALYSIS, DEFINED
            AS THE UNEQUAL SCATTERING OF RESIDUALS
            A. H0: HOMOSCEDASTICITY - EQUAL VARIANCE AMONG RESIDUALS
            B. HA: HETEROSKEDASTICITY - UNEQUAL VARIANCE AMONG RESIDUALS
        INPUTS:
            X = A NUMPY.NDARRAY CONTAINING THE PREDICTOR VARIABLES. SHAPE = (NSAMPLES, NPREDICTORS).
            Y = A 1D NUMPY.NDARRAY CONTAINING THE RESPONSE VARIABLE. SHAPE = (NSAMPLES, ).
        OUTPUTS: A DICTIONARY CONTAINING THREE ELEMENTS
            1. THE BREUSCH-PAGAN TEST STATISTIC.
            2. THE P-VALUE FOR THE TEST.
            3. THE TEST RESULT.
        """
        # checking wheter the array is unidimensional and reshaping it
        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)
        # checking dv and ivs
        if array_y.ndim != 1:
            raise SystemExit('Error: array_y has more than 1 dimension.')
        if array_x.shape[0] != array_y.shape[0]:
            raise SystemExit('Error: the number of samples differs between array_x and array_y.')
        else:
            n_samples = array_y.shape[0]
        # fit an OLS linear model to array_y using array_x
        lm = LinearRegression()
        lm.fit(array_x, array_y)
        # calculate the squared errors
        err = (array_y - lm.predict(array_x))**2
        # fit an auxiliary regression to the squared errors to estimate the variance
        #   in err explained by array_x
        lm.fit(array_x, err)
        pred_err = lm.predict(array_x)
        del lm
        # calculate the coefficient of determination
        ss_tot = sum((err - np.mean(err))**2)
        ss_res = sum((err - pred_err)**2)
        r2 = 1 - (ss_res / ss_tot)
        del err, pred_err, ss_res, ss_tot
        # calculate the Lagrange multiplier:
        lagrange_multiplier = n_samples * r2
        del r2
        # calculate p-value. degrees of freedom = number of predictors.
        # this is equivalent to (p - 1) parameter restrictions in Wikipedia entry.
        pval = stats.chisqprob(lagrange_multiplier, array_x.shape[1])
        # hipothesis test for heteroskedasticity
        bl_resject_h0 = alpha > pval
        # returning results
        return {
            'lagrange_multiplier': lagrange_multiplier,
            'p_value': pval,
            'h0': 'homoscedasticity',
            'bl_reject_h0': bl_resject_h0
        }

    def cooks_distance(self, array_x, array_y):
        """
        DOCSTRING: COOK'S DISTANCE DETECTS OUTLIERS, WHICH EXCLUSION MAY CAUSE SUBSTANTIAL
            CHANGES IN THE ESTIMATED REGRESSION FUNCTION - METRIC FOR IDENTIFYING INFLUENTIAL
            DATA POINTS
            A. TUPLE OF ARRAYS WITH COOK'S DISTANCE AND P-VALUE
            B. LARGE COOK'S D INFLUENCE:
                B.1. D > 0.5 MAY BE INFLUENTIAL
                B.2. D > 1 LIKELY TO BE INFLUENTIAL
                B.3. D > 2 / SQRT(K / N) - LARGE SAMPLES
        INPUTS:
        OUTPUTS:
        """
        # fitting multiple regression model
        model_fitted = sm.OLS(array_y, array_x).fit()
        # calculate influence and outlier measures
        influence = model_fitted.get_influence()
        # return cook's distance
        return influence.cooks_distance

    def test_joint_coeff(self, sse_r, sse_unr, q, n, k):
        """
        DOCSTRING: F-STAT TEST FOR JOINT COEFFICIENTS
        INPUTS: SSE RESTRICTED, SSE UNRESTRICTED, Q (NUMBER OF RESTRICTIONS),
            N (NUMBER OF OBSERVATIONS), K (NUMBER OF INDEPENDENT VARIABLES)
        OUTPUTS: FLOAT
        """
        return (sse_r - sse_unr) / q / (sse_unr / (n - k - 1))

class NormalityHT:

    def kolmogorov_smirnov_test(self, array_x, alpha=0.05, factor_empirical_func=10):
        """
        REFERENCES: http://www.portalaction.com.br/inferencia/62-teste-de-kolmogorov-smirnov
        DOCSTRING: TEST OF HYPOTHESIS KOLMOGOROV-SMIRNOV TO CHECK WHETER THE NORMAL DISTRIBUTIONS
            FITS A SAMPLE OF DATA
        INPUTS: LIST OF REAL NUMBERS, ALPHA (STANDARD VALUE IS 0.05) AND
            FACTOR FOR EMPIRICAL FUNCTION
        OUTPUTS: DICTIONARY WITH DN, CRITICAL VALUE AND WHETHER TO REJECT OR NOT H0 HYPOTHESIS THAT
            THE DATA IS FITTED BY A NORMAL DISTRIBUTION
        """
        # sort list
        array_x.sort()
        # turning list to numpy array
        array_x = np.array(array_x)
        # empirical function array
        empirical_func_array = [
            array_x / factor_empirical_func for array_x in range(1, len(array_x) + 1)]
        # theoretical function array, with normal function distribution for cumulative values
        mean_array = StatiscalDescription().statistical_description(
            array_x)['mean']
        standard_deviation = StatiscalDescription().statistical_description(array_x)[
            'standard_deviation_sample']
        cdf_array = [NormalDistribution().cdf(array_x, mean_array, standard_deviation)
                     for array_x in np.array(array_x)]
        # test 1 array: |F(array_x(i)) - Fn(array_x(i-1))|
        test_1_array = [abs(empirical_func_array[i] - cdf_array[i])
                        for i in range(len(array_x))]
        # test 2 array: |F(array_x(i))-Fn(array_x(i-1))|
        test_2_array = list()
        for i in range(len(array_x)):
            if i == 0:
                test_2_array.append(cdf_array[i])
            else:
                test_2_array.append(cdf_array[i] - empirical_func_array[i - 1])
        test_2_array = np.array(test_2_array)
        # either reject or not h0 hypothesis
        return {
            'dn': max(np.array(np.max(test_1_array)), np.array(np.max(test_2_array))),
            'critical_value': stats.ksone.ppf(
                1 - alpha / 2, len(array_x)),
            'h0': 'nomally distributed data',
            'reject_h0': max(np.array(np.max(test_1_array)), np.array(np.max(
                test_2_array))) > stats.ksone.ppf(1 - alpha / 2, len(array_x))
        }

    def anderson_darling(self, array_data, alpha=0.05):
        """
        REFERENCES: http://www.portalaction.com.br/inferencia/63-teste-de-anderson-darling,
            http://www.uel.br/projetos/experimental/pages/arquivos/Anderson_Darling.html
        DOCSTRING: TEST OF HYPOTHESIS ANDERSON-DARLING TO CHECK WHETER THE NORMAL DISTRIBUTIONS
            FITS A SAMPLE OF DATA
        INPUTS: LIST OF REAL NUMBERS AND ALPHA (STANDARD VALUE IS 0.05)
        OUTPUTS: DICT WITH ALPHA, P-VALUE AND WHETHER REJECT H0 OR NOT
        """
        # sort list
        array_data.sort()
        # turning list to numpy array
        array_x = np.array(array_data)
        # statistical description
        mean_array = StatiscalDescription().statistical_description(
            array_x)['mean']
        standard_deviation = StatiscalDescription().statistical_description(array_x)[
            'standard_deviation_sample']
        # anderson-darling parameter
        d_array = list()
        for i in range(len(array_x)):
            d_array.append((2 * (i + 1) - 1) * np.log(
                NormalDistribution().cdf(array_x[i], mean_array, standard_deviation))
                + (2 * (len(array_x) - (
                    i + 1)) + 1) * np.log(1 -
                                          NormalDistribution().cdf(
                                              array_x[i], mean_array,
                                              standard_deviation)))
        a_2 = - len(array_x) - 1 / \
            len(array_x) * sum(d_array)
        # anderson-darling modified
        a_m_2 = a_2 * (1 + 0.75 / len(array_x) +
                       2.25 / len(array_x) ** 2)
        # p-value
        if a_m_2 < 0.2:
            p_value = 1 - np.exp(-13.436 + 101.14 *
                                 a_m_2 - 223.73 * a_m_2 ** 2)
        elif a_m_2 < 0.340:
            p_value = 1 - np.exp(-8.318 + 42.796 * a_m_2 - 59.938 * a_m_2 ** 2)
        elif a_m_2 < 0.6:
            p_value = np.exp(0.9177 - 4.279 * a_m_2 - 1.38 * a_m_2 ** 2)
        elif a_m_2 > 0.6:
            p_value = np.exp(1.2937 - 5.709 * a_m_2 + 0.0186 * a_m_2 ** 2)
        return {
            'alpha': alpha,
            'p-value': p_value,
            'h0': 'nomally distributed data',
            'reject_h0': alpha > p_value
        }

    def shapiro_wilk(self, array_data, alpha=0.05):
        """
        REFERENCES: http://www.portalaction.com.br/inferencia/64-teste-de-shapiro-wilk,
            https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.shapiro.html
        DOCSTRING: TEST OF HYPOTHESIS SHAPIRO-WALK TO CHECK WHETER THE NORMAL DISTRIBUTIONS
            FITS A SAMPLE OF DATA
        INPUTS: LIST OF REAL NUMBERS AND ALPHA (STANDARD VALUE IS 0.05)
        OUTPUTS: DICT WITH ALPHA, P-VALUE AND WHETHER REJECT H0 OR NOT
        """
        # sort list
        array_data.sort()
        # turning list to numpy array
        array_x = np.array(array_data)
        # hypothesis test
        return {
            'alpha': alpha,
            'p-value': stats.shapiro(array_x).pvalue,
            'h0': 'nomally distributed data',
            'reject_h0': alpha > stats.shapiro(array_x).pvalue
        }

    def d_agostinos_k2(self, array_data, alpha=0.05):
        """
        REFERENCES: https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        DOCSTRING: TEST OF HYPOTHESIS D'AGOSTINO'S K² TO CHECK WHETER THE NORMAL DISTRIBUTIONS
            FITS A SAMPLE OF DATA
        INPUTS: LIST OF REAL NUMBERS AND ALPHA (STANDARD VALUE IS 0.05)
        OUTPUTS: DICT WITH ALPHA, P-VALUE AND WHETHER REJECT H0 OR NOT
        """
        # sort list
        array_data.sort()
        # turning list to numpy array
        array_x = np.array(array_data)
        # hypothesis test
        return {
            'alpha': alpha,
            'p-value': stats.normaltest(array_x).pvalue,
            'h0': 'nomally distributed data',
            'reject_h0': alpha > stats.normaltest(array_x).pvalue
        }


class CorrelationHT:

    def pearsons_correlation_coefficient(self, x1_array, x2_array, alpha=0.05):
        """
        REFERENCES: https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        DOCSTRING: PEARSON'S CORRELATION COEFFICIENT, TESTING WHETER TWO SAMPLES HAVE A LINEAR
            RELATIONSHIP - ASSUMPTIONS: I) INDEPENDENT AND EQUALLY DISTRIBUTED, II) NORMALLITY,
            III) OBSERVATIONS IN EACH SAMPLE HAVE THE SAME VARIANCE - INTERPRETATION: H0: THE TWO
            SAMPLES ARE INDEPENDENT, H1: THERE IS A DEPENDECY
        INPUTS: LISTS OF REAL NUMBERS AND ALPHA (STANDARD VALUE IS 0.05)
        OUTPUTS: DICT WITH ALPHA, P-VALUE AND WHETHER REJECT H0 OR NOT
        """
        # sort list
        x1_array, x2_array = (x1_array.sort(), x2_array.sort())
        # turning list to numpy array
        x1_array, x2_array = (np.array(x1_array), np.array(x2_array))
        # hypothesis test
        return {
            'alpha': alpha,
            'p-value': stats.pearsonr(x1_array, x2_array).pvalue,
            'reject_h0': alpha > stats.pearsonr(x1_array, x2_array).pvalue
        }

    def spearmans_rank_correlation(self, x1_array, x2_array, alpha=0.05):
        """
        REFERENCES: https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        DOCSTRING: SPEARMAN'S RANK CORRELATION, TESTING WHETER TWO SAMPLES HAVE A MONOTONIC
            RELATIONSHIP, WITH THE ASSUMPTIONS OF OBSERVATIONS IN EACH SAMPLE BEING INDEPENDET AND
            IDENTICALLY DISTRIBUTED (IID), AS WELL, AS RANKABLE - INTERPRETATION: H0: THE TWO
            SAMPLES ARE INDEPENDENT, H1: THERE IS A DEPENDENCY BETWEEN THE SAMPLES
        INPUTS: LISTS OF REAL NUMBERS AND ALPHA (STANDARD VALUE IS 0.05)
        OUTPUTS: DICT WITH ALPHA, P-VALUE AND WHETHER REJECT H0 OR NOT
        """
        # sort list
        x1_array, x2_array = (x1_array.sort(), x2_array.sort())
        # turning list to numpy array
        x1_array, x2_array = (np.array(x1_array), np.array(x2_array))
        # hypothesis test
        return {
            'alpha': alpha,
            'p-value': stats.spearmanr(x1_array, x2_array).pvalue,
            'reject_h0': alpha > stats.spearmanr(x1_array, x2_array).pvalue
        }

    def kendalls_rank_correlation(self, x1_array, x2_array, alpha=0.05):
        """
        REFERENCES: https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        DOCSTRING: KENDALL'S RANK CORRELATION, TESTING WHETER TWO SAMPLES HAVE A MONOTONIC
            RELATIONSHIP, WITH THE ASSUMPTIONS OF OBSERVATIONS IN EACH SAMPLE BEING INDEPENDET AND
            IDENTICALLY DISTRIBUTED (IID), AS WELL, AS RANKABLE - INTERPRETATION: H0: THE TWO
            SAMPLES ARE INDEPENDENT, H1: THERE IS A DEPENDENCY BETWEEN THE SAMPLES
        INPUTS: LISTS OF REAL NUMBERS AND ALPHA (STANDARD VALUE IS 0.05)
        OUTPUTS: DICT WITH ALPHA, P-VALUE AND WHETHER REJECT H0 OR NOT
        """
        # sort list
        x1_array, x2_array = (x1_array.sort(), x2_array.sort())
        # turning list to numpy array
        x1_array, x2_array = (np.array(x1_array), np.array(x2_array))
        # hypothesis test
        return {
            'alpha': alpha,
            'p-value': stats.kendalltau(x1_array, x2_array).pvalue,
            'reject_h0': alpha > stats.kendalltau(x1_array, x2_array).pvalue
        }

    def chi_squared_test(self, x1_array, x2_array, alpha=0.05):
        """
        REFERENCES: https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        DOCSTRING: CHI-SQUARED TEST, TESTING WHETER TWO CATEGORICAL VARIABLES ARE RELATED OR
            INDEPENDENT, WITH THE ASSUMPTIONS OF OBSERVATIONS USED IN THE CALCULATION OF THE
            CONTINGENCY TABLE BEING INDEPENDENT, AND 25 OR MORE EXAMPLES IN EACH CELL OF THE
            CONTINGENCY TABLE - INTERPRETATION: H0: THE TWO SAMPLES ARE INDEPENDET, H1: THERE IS A
            DEPENDENCY BETWEEN THE SAMPLES
        INPUTS: LISTS OF REAL NUMBERS AND ALPHA (STANDARD VALUE IS 0.05)
        OUTPUTS: DICT WITH ALPHA, P-VALUE AND WHETHER REJECT H0 OR NOT
        """
        # sort list
        x1_array, x2_array = (x1_array.sort(), x2_array.sort())
        # turning list to numpy array
        x1_array, x2_array = (np.array(x1_array), np.array(x2_array))
        # hypothesis test
        return {
            'alpha': alpha,
            'p-value': stats.kendalltau(x1_array, x2_array).pvalue,
            'reject_h0': alpha > stats.kendalltau(x1_array, x2_array).pvalue
        }


class StationaryHT:

    def augmented_dickey_fuller(self, array_x, alpha=0.05):
        """
        REFERENCES: https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        DOCSTRING: TESTS WHETER A TIME SERIES HAS A UNIT ROOT, E.G. HAS A TREND OR MORE GENERALLY
            IS AUTOREGRESSIVE - ASSUMPTIONS: OBSERVATIONS ARE TEMPORALLY ORDERED - INTERPRETATION:
            - H0: A UNIT ROOT IS PRESENT (SERIES IS NON-COVARIANCE STATIONARY, SO IS A RANDOM WALK);
            - HA: A UNIT ROOT IS NOT PRESENT (SERIES IS COVARIANCE STATIONARY)
        INPUTS: TIME SERIES DATA (A NUMPY ARRAY, FOR INSTANCE) AND ALPHA (0.05 AS DEFAULT)
        OUTPUTS: DICTIONARY WITH P-VALUE AND OR NOT WE OUGHT REJECT THE NON-STATIONARITY HYPOTHESIS
        """
        # sort list
        array_x = array_x.sort()
        # turning list to numpy array
        array_x = np.array(array_x)
        # hypothesis test
        return {
            'statistic': sm.tsa.stattools.adfuller(array_x)[0],
            'p-value': sm.tsa.stattools.adfuller(array_x)[1],
            'lags': sm.tsa.stattools.adfuller(array_x)[2],
            'criteria': sm.tsa.stattools.adfuller(array_x)[3],
            'reject_h0': alpha > sm.tsa.stattools.adfuller(array_x)[1]
        }

    def kwiatkowski_phillips_schmidt_shin(self, array_x, alpha=0.05):
        """
        REFERENCES: https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        DOCSTRING: TESTS WHETER A TIME SERIES IS TREND STATIONARY OR NOT - ASSUMPTIONS:
            OBSERVATIONS ARE TEMPORALLY ORDERED - INTERPRETATION: H0: THE TIME SERIES IS
            TREND-STATIONARY, H1: THE TIME SERIES IS NOT TREND-STATIONARY
        INPUTS: TIME SERIES DATA (A NUMPY ARRAY, FOR INSTANCE) AND ALPHA (0.05 AS DEFAULT)
        OUTPUTS: DICTIONARY WITH P-VALUE AND OR NOT WE OUGHT REJECT THE NON-STATIONARITY HYPOTHESIS
        """
        # sort list
        array_x = array_x.sort()
        # turning list to numpy array
        array_x = np.array(array_x)
        # hypothesis test
        return {
            'statistic': sm.tsa.stattools.kpss(array_x)[0],
            'p-value': sm.tsa.stattools.kpss(array_x)[1],
            'lags': sm.tsa.stattools.kpss(array_x)[2],
            'criteria': sm.tsa.stattools.kpss(array_x)[3],
            'reject_h0': alpha > sm.tsa.stattools.kpss(array_x)[1]
        }


class MeansHT:

    def student_s_t_test(self, x1_array, x2_array, alpha=0.05):
        """
        REFERENCES: https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        DOCSTRING: TESTS WHETER THE MEANS OF TWO INDEPENDENT SAMPLES ARE SIGNIFICANTLY DIFFERENT -
            ASSUMPTIONS: OBSERVATIONS IN EACH SAMPLE ARE INDEPENDENT AND IDENTICALLY DISTRIBUTED (
            IID), ARE NORMALLY DISTRIBUTED AND HAVE THE SAME VARIANCE - INTERPRETATION: H0: THE
            MEANS OF THE SAMPLES ARE EQUAL, H1: THE MEANS OF THE SAMPLES ARE UNEQUAL
        INPUTS: LISTS OF REAL NUMBERS AND ALPHA (STANDARD VALUE IS 0.05)
        OUTPUTS: DICT WITH ALPHA, P-VALUE AND WHETHER REJECT H0 OR NOT
        """
        # sort list
        x1_array, x2_array = (x1_array.sort(), x2_array.sort())
        # turning list to numpy array
        x1_array, x2_array = (np.array(x1_array), np.array(x2_array))
        # hypothesis test
        return {
            'alpha': alpha,
            'p-value': stats.ttest_ind(x1_array, x2_array).pvalue,
            'reject_h0': alpha > stats.ttest_ind(x1_array, x2_array).pvalue
        }

    def paired_student_s_t_test(self, x1_array, x2_array, alpha=0.05):
        """
        REFERENCES: https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        DOCSTRING: TESTS WHETER THE MEANS OF TWO INDEPENDENT SAMPLES ARE SIGNIFICANTLY DIFFERENT -
            ASSUMPTIONS: OBSERVATIONS IN EACH SAMPLE ARE INDEPENDENT AND IDENTICALLY DISTRIBUTED (
            IID), ARE NORMALLY DISTRIBUTED AND HAVE THE SAME VARIANCE - INTERPRETATION: H0: THE
            MEANS OF THE SAMPLES ARE EQUAL, H1: THE MEANS OF THE SAMPLES ARE UNEQUAL
        INPUTS: LISTS OF REAL NUMBERS AND ALPHA (STANDARD VALUE IS 0.05)
        OUTPUTS: DICT WITH ALPHA, P-VALUE AND WHETHER REJECT H0 OR NOT
        """
        # sort list
        x1_array, x2_array = (x1_array.sort(), x2_array.sort())
        # turning list to numpy array
        x1_array, x2_array = (np.array(x1_array), np.array(x2_array))
        # hypothesis test
        return {
            'alpha': alpha,
            'p-value': stats.ttest_rel(x1_array, x2_array).pvalue,
            'reject_h0': alpha > stats.ttest_rel(x1_array, x2_array).pvalue
        }


class StatisticalDistributionsHT:

    def mann_whitney_u_test(self, x1_array, x2_array, alpha=0.05):
        """
        REFERENCES: https://machinelearningmastery.com/nonparametric-statistical-significance-tests-in-python/
        DOCSTRING: TESTS WHETER THE DISTRIBUTIONS OF TWO PAIRED SAMPLES ARE EQUAL OR NOT -
            ASSUMPTIONS: OBSERVATIONS IN EACH SAMPLE ARE INDEPENDENT AND IDENTICALLY DISTRIBUTED (
            IID), CAN BE RANKED AND OBSERVATIONS ACCROSS EACH SAMPLE ARE PAIRED - INTERPRETATION:
            H0: THE DISTRIBUTIONS OF BOTH SAMPLES ARE EQUAL, H1: THE DISTRIBUTIONS OF BOTH SAMPLES
            ARE NOT EQUAL
        INPUTS: LISTS OF REAL NUMBERS AND ALPHA (STANDARD VALUE IS 0.05)
        OUTPUTS: DICT WITH ALPHA, P-VALUE AND WHETHER REJECT H0 OR NOT
        """
        # sort list
        x1_array, x2_array = (x1_array.sort(), x2_array.sort())
        # turning list to numpy array
        x1_array, x2_array = (np.array(x1_array), np.array(x2_array))
        # hypothesis test
        return {
            'alpha': alpha,
            'p-value': stats.mannwhitneyu(x1_array, x2_array).pvalue,
            'reject_h0': alpha > stats.mannwhitneyu(x1_array, x2_array).pvalue
        }

    def wilcoxon_signed_rank_test(self, x1_array, x2_array, alpha=0.05):
        """
        REFERENCES: https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        DOCSTRING: TESTS WHETER THE DISTRIBUTIONS OF TWO PAIRED SAMPLES ARE EQUAL OR NOT -
            ASSUMPTIONS: OBSERVATIONS IN EACH SAMPLE ARE INDEPENDENT AND IDENTICALLY DISTRIBUTED (
            IID), CAN BE RANKED AND OBSERVATIONS ACCROSS EACH SAMPLE ARE PAIRED - INTERPRETATION:
            H0: THE DISTRIBUTIONS OF BOTH SAMPLES ARE EQUAL, H1: THE DISTRIBUTIONS OF BOTH SAMPLES
            ARE NOT EQUAL
        INPUTS: LISTS OF REAL NUMBERS AND ALPHA (STANDARD VALUE IS 0.05)
        OUTPUTS: DICT WITH ALPHA, P-VALUE AND WHETHER REJECT H0 OR NOT
        """
        # sort list
        x1_array, x2_array = (x1_array.sort(), x2_array.sort())
        # turning list to numpy array
        x1_array, x2_array = (np.array(x1_array), np.array(x2_array))
        # hypothesis test
        return {
            'alpha': alpha,
            'p-value': stats.wilcoxon(x1_array, x2_array).pvalue,
            'reject_h0': alpha > stats.wilcoxon(x1_array, x2_array).pvalue
        }

    def kruskal_wallis_h_test(self, x1_array, x2_array, alpha=0.05):
        """
        REFERENCES: https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        DOCSTRING: TESTS WHETER THE DISTRIBUTIONS OF TWO PAIRED SAMPLES ARE EQUAL OR NOT -
            ASSUMPTIONS: OBSERVATIONS IN EACH SAMPLE ARE INDEPENDENT AND IDENTICALLY DISTRIBUTED (
            IID), CAN BE RANKED AND OBSERVATIONS ACCROSS EACH SAMPLE ARE PAIRED - INTERPRETATION:
            H0: THE DISTRIBUTIONS OF BOTH SAMPLES ARE EQUAL, H1: THE DISTRIBUTIONS OF BOTH SAMPLES
            ARE NOT EQUAL
        INPUTS: LISTS OF REAL NUMBERS AND ALPHA (STANDARD VALUE IS 0.05)
        OUTPUTS: DICT WITH ALPHA, P-VALUE AND WHETHER REJECT H0 OR NOT
        """
        # sort list
        x1_array, x2_array = (x1_array.sort(), x2_array.sort())
        # turning list to numpy array
        x1_array, x2_array = (np.array(x1_array), np.array(x2_array))
        # hypothesis test
        return {
            'alpha': alpha,
            'p-value': stats.kruskal(x1_array, x2_array).pvalue,
            'reject_h0': alpha > stats.kruskal(x1_array, x2_array).pvalue
        }

    def friedman_test(self, x1_array, x2_array, alpha=0.05):
        """
        REFERENCES: https://machinelearningmastery.com/statistical-hypothesis-tests-in-python-cheat-sheet/
        DOCSTRING: TESTS WHETER THE DISTRIBUTIONS OF TWO PAIRED SAMPLES ARE EQUAL OR NOT -
            ASSUMPTIONS: OBSERVATIONS IN EACH SAMPLE ARE INDEPENDENT AND IDENTICALLY DISTRIBUTED (
            IID), CAN BE RANKED AND OBSERVATIONS ACCROSS EACH SAMPLE ARE PAIRED - INTERPRETATION:
            H0: THE DISTRIBUTIONS OF BOTH SAMPLES ARE EQUAL, H1: THE DISTRIBUTIONS OF BOTH SAMPLES
            ARE NOT EQUAL
        INPUTS: LISTS OF REAL NUMBERS AND ALPHA (STANDARD VALUE IS 0.05)
        OUTPUTS: DICT WITH ALPHA, P-VALUE AND WHETHER REJECT H0 OR NOT
        """
        # sort list
        x1_array, x2_array = (x1_array.sort(), x2_array.sort())
        # turning list to numpy array
        x1_array, x2_array = (np.array(x1_array), np.array(x2_array))
        # hypothesis test
        return {
            'alpha': alpha,
            'p-value': stats.friedmanchisquare(x1_array, x2_array).pvalue,
            'reject_h0': alpha > stats.friedmanchisquare(x1_array, x2_array).pvalue
        }


class IndependenceHT:

    def pearson_chi_squared(self, array_y, float_significance=0.05):
        """
        DOCSTRING: PEARSON'S CHI SQUARED TEST OF INDEPENDENCE
        INPUTS: ARRAY DATA
        OUTPUTS: DICTIONARY
        """
        tup_ind_test = stats.chi2_contingency(array_y)
        return {
            'chi_squard_statistic': tup_ind_test[0],
            'p_value': tup_ind_test[1],
            'degrees_freedom': tup_ind_test[2],
            'array_expected_values': tup_ind_test[3],
            'significance': float_significance,
            'h0_hypothesis': 'independent',
            'reject_h0': float_significance > tup_ind_test[1]
        }


# array_data = [[48, 12, 33, 57], [34, 46, 42, 26]]
# print(IndependenceHT().pearson_chi_squared(array_data))
