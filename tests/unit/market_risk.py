import numpy as np
from unittest import TestCase, main
from stpstone.analytics.risk.market import MarkowitzPortf, PortfVar, RiskStats, QuoteVar, VaR, \
    RiskMeasures


array_r = np.array([
    [0.01, 0.02, -0.01, 0.03, -0.02],
    [0.02, -0.01, 0.03, -0.02, 0.01],
    [-0.01, 0.03, -0.02, 0.01, 0.02],
    [0.03, -0.02, 0.01, 0.02, -0.01],
    [-0.02, 0.01, 0.02, -0.01, 0.03],
    [0.01, 0.02, -0.01, 0.03, -0.02]
])

array_w = np.array([
    [0.2, 0.2, 0.2, 0.2, 0.2],
    [0.3, 0.1, 0.2, 0.2, 0.2],
    [0.1, 0.3, 0.2, 0.2, 0.2],
    [0.2, 0.2, 0.3, 0.1, 0.2],
    [0.2, 0.2, 0.2, 0.3, 0.1],
    [0.1, 0.2, 0.3, 0.2, 0.2]
])


class TestRiskStats(TestCase):

    def setUp(self):
        self.risk_stats = RiskStats(array_r)

    def test_variance_ewma(self):
        float_ewma = self.risk_stats.variance_ewma(float_lambda=0.94)
        self.assertIsInstance(float_ewma, float)
        self.assertGreaterEqual(float_ewma, 0)

    def test_descriptive_stats(self):
        dict_stats = self.risk_stats.descriptive_stats(float_lambda=0.94)
        self.assertIsInstance(dict_stats, dict)
        self.assertIn("mu", dict_stats)
        self.assertIn("std", dict_stats)
        self.assertIn("ewma_std", dict_stats)
        self.assertIsInstance(dict_stats["mu"], float)
        self.assertIsInstance(dict_stats["std"], float)
        self.assertIsInstance(dict_stats["ewma_std"], float)


class TestMarkowitzPortf(TestCase):

    def setUp(self):
        self.markowitz = MarkowitzPortf(array_r, array_w, float_lambda=0.94, bl_validate_w=True,
                                        float_atol=1e-4)

    def test_mu(self):
        float_mu = self.markowitz.mu
        self.assertIsInstance(float_mu, float)
        self.assertAlmostEqual(float_mu, 0.006, places=4)

    def test_sigma(self):
        float_sigma = self.markowitz.sigma
        self.assertIsInstance(float_sigma, float)
        self.assertAlmostEqual(float_sigma, 0.0005055, places=4)

    def test_cov(self):
        array_cov = self.markowitz.cov
        self.assertIsInstance(array_cov, np.ndarray)
        np.testing.assert_allclose(array_cov, np.array([
            [0.00076, 0.00032455, 0.0005594, 0.00057897, 0.00030498],
            [0.00032455, 0.00078202, 0.0002903, 0.00060833, 0.00052271],
            [0.0005594 , 0.0002903, 0.00078936, 0.00027073, 0.00061811],
            [0.00057897, 0.00060833, 0.00027073, 0.0008285, 0.00024138],
            [0.00030498, 0.00052271, 0.00061811, 0.00024138, 0.00084073]
        ]), rtol=1e-5, atol=1e-8)
        self.assertEqual(array_cov.shape, (5, 5))

    def test_sharpe_ratio(self):
        float_sharpe = self.markowitz.sharpe_ratio(float_rf=0.02)
        self.assertIsInstance(float_sharpe, float)
        self.assertAlmostEqual(float_sharpe, -27.6908, places=4)


class TestVaR(TestCase):

    def setUp(self):
        self.mu = np.mean(array_r)
        self.sigma = np.std(array_r)
        self.var = VaR(self.mu, self.sigma, array_r, float_cl=0.95)

    def test_historic_var(self):
        float_historic_var = self.var.historic_var
        self.assertIsInstance(float_historic_var, float)
        self.assertAlmostEqual(float_historic_var, -0.02, places=5)

    def test_historic_var_stress_test(self):
        float_stressed_var = self.var.historic_var_stress_test(
            float_shock=0.1, str_shock_type="relative")
        self.assertIsInstance(float_stressed_var, float)
        self.assertAlmostEqual(float_stressed_var, -0.022, places=5)
        float_stressed_var = self.var.historic_var_stress_test(
            float_shock=0.1, str_shock_type="absolute")
        self.assertIsInstance(float_stressed_var, float)
        self.assertAlmostEqual(float_stressed_var, 0.08, places=5)

    def test_parametric_var(self):
        float_parametric_var = self.var.parametric_var
        self.assertIsInstance(float_parametric_var, float)
        self.assertAlmostEqual(self.var.parametric_var, -0.02450749, places=5)

    def test_cvar(self):
        float_cvar = self.var.cvar
        self.assertIsInstance(float_cvar, float)
        self.assertAlmostEqual(self.var.cvar, -0.02, places=5)

    def test_monte_carlo_var(self):
        np.random.seed(42)
        float_mc_var = self.var.monte_carlo_var(int_simulations=1_000, float_portf_nv=1_000_000)
        self.assertIsInstance(float_mc_var, float)
        self.assertAlmostEqual(float_mc_var, -12010.862185, places=4)


class TestRiskMeasures(TestCase):

    def setUp(self):
        self.mu = np.mean(array_r)
        self.sigma = np.std(array_r)
        self.risk_measures = RiskMeasures(self.mu, self.sigma, array_r)

    def test_drawdown(self):
        array_drawdown = self.risk_measures.drawdown
        self.assertIsInstance(array_drawdown, float)
        self.assertAlmostEqual(array_drawdown, -0.029799999, places=4)

    def test_tracking_error(self):
        array_portf_r = array_r
        array_benchmark_r = np.array([
            [0.011, 0.024, -0.015, 0.035, -0.026],
            [0.022, -0.01, 0.034, -0.025, 0.015],
            [-0.014, 0.031, -0.022, 0.012, 0.022],
            [0.035, -0.022, 0.017, 0.022, -0.019],
            [-0.024, 0.012, 0.029, -0.01, 0.038],
            [0.01, 0.021, -0.018, 0.037, -0.028]
        ])
        array_tracking_error = self.risk_measures.tracking_error(array_portf_r, array_benchmark_r)
        self.assertIsInstance(array_tracking_error, float)
        self.assertAlmostEqual(array_tracking_error, 0.004918181, places=4)

    def test_sharpe(self):
        float_sharpe = self.risk_measures.sharpe(float_rf=0.02)
        self.assertIsInstance(float_sharpe, float)
        self.assertAlmostEqual(float_sharpe, -0.7548, places=4)

    def test_beta(self):
        array_market_r = array_r
        beta = self.risk_measures.beta(array_market_r)
        self.assertIsInstance(beta, float)
        self.assertAlmostEqual(beta, -0.744186, places=4)


class TestQuoteVar(TestCase):

    def setUp(self):
        self.quote_var = QuoteVar(array_r, str_method_str="std", float_cl=0.95)

    def test_parametric_var(self):
        float_parametric_var = self.quote_var.parametric_var
        self.assertIsInstance(float_parametric_var, float)
        self.assertAlmostEqual(float_parametric_var, -0.02450, places=4)


class TestPortfVar(TestCase):

    def setUp(self):
        self.portf_var = PortfVar(array_r, array_w, float_cl=0.95)

    def test_parametric_var(self):
        float_parametric_var = self.portf_var.parametric_var
        self.assertIsInstance(float_parametric_var, float)
        self.assertAlmostEqual(float_parametric_var, 0.005168, places=4)


if __name__ == "__main__":
    main()
