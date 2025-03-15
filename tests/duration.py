from unittest import TestCase, main
from stpstone.analytics.risk.yield_ import BondDuration


class TestBondDuration(TestCase):

    def setUp(self) -> None:
        self.list_cfs = [100, 100, 100, 1100]
        self.float_ytm = 0.05
        self.float_fv = 1000
        self.float_coupon_rate = 0.10
        self.cls_bond_duration = BondDuration(
            self.list_cfs, self.float_ytm, self.float_fv, self.float_coupon_rate)

    def test_macaulay_duration(self):
        self.assertAlmostEqual(self.cls_bond_duration.macaulay, 3.5339529365236304, places=2)

    def test_modified_duration(self):
        self.assertAlmostEqual(self.cls_bond_duration.modified(0.05, 1), 3.365669463355838, places=2)

    def test_dollar_duration(self):
        self.assertAlmostEqual(self.cls_bond_duration.dollar(0.05, 1), -4004.3738180384144, places=2)

    def test_effective_duration(self):
        self.assertAlmostEqual(self.cls_bond_duration.effective(0.01), -3.3279316177786717, places=2)

    def test_convexity(self):
        self.assertAlmostEqual(self.cls_bond_duration.convexity(0.01), -39805.485, places=3)

    def test_dv_y(self):
        self.assertAlmostEqual(
            self.cls_bond_duration.dv_y(0.05, 1, 0.0001), -0.4004373818038415, places=3)

if __name__ == '__main__':
    main()
