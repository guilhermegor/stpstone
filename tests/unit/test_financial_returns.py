import unittest
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from stpstone.analytics.perf_metrics.financial_returns import FinancialReturns


class TestFinancialReturns(unittest.TestCase):
    def setUp(self):
        self.fr = FinancialReturns()

    def test_continuous_return(self):
        # test basic calculation
        self.assertAlmostEqual(
            self.fr.continuous_return(100, 110),
            np.log(110/100)
        )
        # test with string inputs
        self.assertAlmostEqual(
            self.fr.continuous_return("100", "110"),
            np.log(110/100)
        )
        # test zero case (should raise error)
        with self.assertRaises((ValueError, ZeroDivisionError)):
            self.fr.continuous_return(0, 100)

    def test_discrete_return(self):
        # test basic calculation
        self.assertAlmostEqual(self.fr.discrete_return(100, 110), 0.1)
        # test with string inputs
        self.assertAlmostEqual(self.fr.discrete_return("100", "110"), 0.1)
        # test zero case (should raise error)
        with self.assertRaises((ValueError, ZeroDivisionError)):
            self.fr.discrete_return(0, 100)

    def test_calc_returns_from_prices(self):
        prices = [100, 110, 105, 120]

        # test log returns
        log_returns = self.fr.calc_returns_from_prices(prices, "ln_return")
        expected_log = [np.log(110/100), np.log(105/110), np.log(120/105)]
        for actual, expected in zip(log_returns, expected_log):
            self.assertAlmostEqual(actual, expected)

        # test standard returns
        std_returns = self.fr.calc_returns_from_prices(prices, "stnd_return")
        expected_std = [0.1, -0.04545454545454545, 0.14285714285714285]
        for actual, expected in zip(std_returns, expected_std):
            self.assertAlmostEqual(actual, expected)

        # test invalid return type
        with self.assertRaises(ValueError):
            self.fr.calc_returns_from_prices(prices, "invalid_type")

    def test_pandas_returns_from_spot_prices(self):
        # create test dataframe
        df_ = pd.DataFrame({
            "date": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            "price": [100, 110, 105]
        })

        # test log returns
        result = self.fr.pandas_returns_from_spot_prices(
            df_.copy(), "price", "date", type_return="ln_return"
        )
        self.assertIn("returns", result.columns)

        # First row should be 0 (no previous price)
        self.assertEqual(result["returns"].iloc[0], 0)

        # Second row should be log return
        self.assertAlmostEqual(result["returns"].iloc[1], np.float64(0.04652001563489291))

        # Third row should be NaN (no next price)
        self.assertTrue(pd.isna(result["returns"].iloc[2]))

        # test standard returns
        result = self.fr.pandas_returns_from_spot_prices(
            df_.copy(), "price", "date", type_return="stnd_return"
        )
        self.assertIn("returns", result.columns)

        # First row should be 0 (no previous price)
        self.assertEqual(result["returns"].iloc[0], 0)

        # Second row should be simple return
        self.assertAlmostEqual(result["returns"].iloc[1], np.float64(0.04761904761904767))

        # Third row should be NaN (no next price)
        self.assertTrue(pd.isna(result["returns"].iloc[2]))

    @patch("stpstone.analytics.perf_metrics.financial_math.FinancialMath.compound_r")
    def test_short_fee_cost(self, mock_compound: MagicMock):
        # setup mock
        mock_compound.return_value = 1.004074  # approx value for 5% over 30/360 days

        # test basic calculation
        fee = 0.05  # 5%
        days = 30
        price = 100
        qty = 10
        expected_cost = 1004.074

        result = self.fr.short_fee_cost(fee, days, price, qty)
        self.assertAlmostEqual(result, expected_cost, places=4)
        mock_compound.assert_called_once_with(fee, days, 360)

    def test_pricing_strategy(self):
        # test long strategy with log returns
        long_p = 110
        short_p = 100
        leverage = 2
        result = self.fr.pricing_strategy(long_p, short_p, leverage, 0, "ln_return")
        self.assertEqual(result["mtm"], (short_p - long_p) * leverage)
        self.assertAlmostEqual(result["pct_return"], np.log(long_p/short_p))
        self.assertEqual(result["notional"], short_p)

        # test with operational costs
        op_cost = 5
        result = self.fr.pricing_strategy(long_p, short_p, leverage, op_cost, "ln_return")
        self.assertEqual(result["mtm"], (short_p - long_p) * leverage - op_cost)

        # test with standard returns
        result = self.fr.pricing_strategy(long_p, short_p, leverage, 0, "stnd_return")
        self.assertEqual(result["mtm"], (short_p - long_p) * leverage)
        self.assertAlmostEqual(result["pct_return"], long_p/short_p - 1)

        # test invalid return type
        with self.assertRaises(ValueError):
            self.fr.pricing_strategy(long_p, short_p, leverage, 0, "invalid_type")

if __name__ == "__main__":
    unittest.main()
