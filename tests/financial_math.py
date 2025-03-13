import numpy as np
from unittest import TestCase, main
from stpstone.analytics.perf_metrics.financial_math import FinancialMath


class TestFinancialMath(TestCase):
    
    def setUp(self):
        self.cls_fm = FinancialMath()

    def test_compound_r(self):
        self.assertAlmostEqual(self.cls_fm.compound_r(0.05, 10, 1), 0.62889, places=5)

    def test_simple_r(self):
        self.assertAlmostEqual(self.cls_fm.simple_r(0.05, 10, 1), 0.5, places=5)

    def test_pv_compound(self):
        self.assertAlmostEqual(self.cls_fm.pv(0.05, 10, 1000), -613.91, places=2)

    def test_pv_simple(self):
        self.assertAlmostEqual(
            self.cls_fm.pv(0.05, 10, 1000, str_capitalization="simple"), 666.67, places=2)

    def test_fv_compound(self):
        self.assertAlmostEqual(self.cls_fm.fv(0.05, 10, -613.91), 999.99, places=2)

    def test_fv_simple(self):
        self.assertAlmostEqual(
            self.cls_fm.fv(0.05, 10, 666.67, str_capitalization="simple"), 1000, places=2)

    def test_irr(self):
        self.assertAlmostEqual(self.cls_fm.irr([-1000, 200, 300, 500, 200]), 0.07409, places=4)

    def test_npv(self):
        self.assertAlmostEqual(self.cls_fm.npv(0.05, [-1000, 200, 300, 500, 200]), 59.044, places=2)

    def test_pmt(self):
        self.assertAlmostEqual(self.cls_fm.pmt(0.05, 10, -613.91), 79.504, places=3)

    def test_invalid_irr(self):
        with self.assertRaises(ValueError):
            self.cls_fm.irr([100, 200, 300])

    def test_pv_cfs_compound(self):
        list_cfs = [100, 200, 300]
        float_ytm = 0.05
        list_exp_pvs = [-95.24, -181.41, -259.15]
        _, array_discounted_cfs = self.cls_fm.pv_cfs(list_cfs, float_ytm, str_capitalization="compound")
        np.testing.assert_array_almost_equal(array_discounted_cfs, list_exp_pvs, decimal=2)

    def test_pv_cfs_simple(self):
        list_cfs = [100, 200, 300]
        float_ytm = 0.05
        list_exp_pvs = [95.24, 181.82, 260.87]
        _, array_discounted_cfs = self.cls_fm.pv_cfs(list_cfs, float_ytm, str_capitalization="simple")
        np.testing.assert_array_almost_equal(array_discounted_cfs, list_exp_pvs, decimal=2)

if __name__ == "__main__":
    main()
