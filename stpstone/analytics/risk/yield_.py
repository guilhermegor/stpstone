import numpy as np
from typing import List, Optional, Literal, Union
from stpstone.analytics.perf_metrics.financial_math import FinancialMath


class BondDuration(FinancialMath):
    """
    Metadata: https://corporatefinanceinstitute.com/resources/fixed-income/duration/
    """

    def __init__(
        self,
        list_cfs: List[float],
        float_ytm: float,
        float_fv: float,
        str_when: Optional[Literal["end", "begin"]] = "end"
    ) -> None:
        self.list_cfs = list_cfs
        self.float_ytm = float_ytm
        self.float_fv = float_fv
        self.str_when = str_when

    @property
    def macaulay(self) -> float:
        array_nper, array_discounted_cfs = self.pv_cfs(self.list_cfs, self.float_ytm,
                                              str_capitalization="simple", str_when="end")
        float_pv = np.sum(array_discounted_cfs)
        return np.sum(array_nper * array_discounted_cfs) / float_pv

    def modified(self, float_y: float, int_n: int) -> float:
        float_macaulay_duration = self.macaulay
        return float_macaulay_duration / (1 + float_y / int_n)

    def dollar(self, float_y: float, int_n: int) -> float:
        _, array_discounted_cfs = self.pv_cfs(self.list_cfs, self.float_ytm,
                                              str_capitalization="simple", str_when="end")
        float_pv0 = np.sum(array_discounted_cfs)
        return - self.modified(float_y, int_n) * float_pv0

    def effective(self, float_delta_y: float) -> float:
        array_nper, array_discounted_cfs = self.pv_cfs(self.list_cfs, self.float_ytm,
                                              str_capitalization="simple", str_when="end")
        float_pv0 = np.sum(array_discounted_cfs)
        float_pv_minus = np.sum([
            self.pv(self.float_ytm - float_delta_y, int_t, float_cf)
            for int_t, float_cf in tuple(zip(array_nper, self.list_cfs))
        ])
        float_pv_plus = np.sum([
            self.pv(self.float_ytm + float_delta_y, int_t, float_cf)
            for int_t, float_cf in tuple(zip(array_nper, self.list_cfs))
        ])
        return (float_pv_minus - float_pv_plus) / (2 * float_delta_y * float_pv0)

    def convexity(self, float_delta_y: float) -> float:
        array_nper, array_discounted_cfs = self.pv_cfs(self.list_cfs, self.float_ytm,
                                              str_capitalization="simple", str_when="end")
        float_pv0 = np.sum(array_discounted_cfs)
        float_pv_minus = np.sum([
            self.pv(self.float_ytm - float_delta_y, int_t, float_cf)
            for int_t, float_cf in tuple(zip(array_nper, self.list_cfs))
        ])
        float_pv_plus = np.sum([
            self.pv(self.float_ytm + float_delta_y, int_t, float_cf)
            for int_t, float_cf in tuple(zip(array_nper, self.list_cfs))
        ])
        return (float_pv_minus + float_pv_plus - 2 * float_pv0) / (float_pv0 * float_delta_y**2)

    def dv_y(self, float_y: float, int_n: int, float_delta_y: float = 0.0001) -> float:
        """
        DV_Y, used to measure the change in a bond's dollar price for a Y change in yield, typically
        called DV01 for a 1 bps change (1E-4)

        Args:
            float_y: float
                Yield discount rate for modified duration
            int_n: int
                Capitalization period for modified duration
            float_delta_y: float (default 1E-4)
                Yield (YTM) change

        Returns:
            Dollar value for a given change in the YTM
        """
        return self.dollar(float_y, int_n) * float_delta_y
