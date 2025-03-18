import numpy as np
import numpy_financial as npf
from typing import List, Tuple
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class FinancialMath(metaclass=TypeChecker):

    def compound_r(self, float_ytm: float, int_nper: int, int_compound_n: int) -> float:
        return float((1.0 + float_ytm) ** (float(int_nper) / float(int_compound_n))) - 1.0

    def simple_r(self, float_ytm: float, int_nper: int, int_compound_n: int) -> float:
        return float_ytm * float(int_nper) / float(int_compound_n)

    def pv(
        self,
        float_ytm: float,
        int_nper: int,
        float_fv: float,
        float_pmt: float = 0,
        str_capitalization: str = "compound",
        str_when: str = "end"
    ) -> float:
        if str_capitalization == "compound":
            return npf.pv(float_ytm, int_nper, float_pmt, float_fv, str_when)
        elif str_capitalization == "simple":
            return float_fv / (1.0 + self.simple_r(float_ytm, int_nper, 1))
        else:
            raise ValueError("str_capitalization must be 'compound' or 'simple'")

    def fv(
        self,
        float_ytm: float,
        int_nper: int,
        float_pv: float,
        float_pmt: float = 0,
        str_capitalization: str = "compound",
        str_when: str = "end"
    ) -> float:
        if str_capitalization == "compound":
            return npf.fv(float_ytm, int_nper, float_pmt, float_pv, str_when)
        elif str_capitalization == "simple":
            return float_pv * (1.0 + self.simple_r(float_ytm, int_nper, 1))
        else:
            raise ValueError("str_capitalization must be 'compound' or 'simple'")

    def irr(self, list_cfs: List[float]) -> float:
        """
        Internal Rate of Return: interest rate at which the net present value of a list of
        cash flows is zero. The IRR is often used to evaluate the performance of an investment or
        project.

        Ags:
            list_cfs: List[float]
                A list of cash flows. The list must have at least one positive and one negative value.

        Returns:
            The internal rate of return of the cash flows.

        Raises:
            ValueError
                - If the list of cash flows does not have at least one positive and one negative
                value.
        """
        if (not any(float_cf < 0 for float_cf in list_cfs)) \
            or (not any(float_cf > 0 for float_cf in list_cfs)):
            raise ValueError(
                "List of cash flows must have at least one positive and one negative value.")
        return npf.irr(list_cfs)

    def npv(self, float_ytm: float, list_cfs: List[float]) -> float:
        """
        Net Present Value: the difference between the present value of the cash inflows and the
        present value of the cash outflows. The NPV is often used to evaluate the performance of an
        investment or project.

        Args:
            float_ytm: float
                The discount rate to be used when calculating the net present value.
            list_cfs: List[float]
                A list of cash flows. The list must have at least one positive and one negative value.

        Returns:
            The net present value of the cash flows.

        Raises:
            ValueError
                - If the list of cash flows does not have at least one positive and one negative value.
        """
        return npf.npv(float_ytm, list_cfs)

    def pmt(
        self,
        float_ytm: float,
        int_nper: int,
        float_pv: float,
        float_fv: float = 0,
        str_when: str = "end"
    ) -> float:
        """
        Calculate the fixed monthly payment based on a constant interest rate and a constant
        payment schedule.

        Args:
            float_ytm: float
                The interest rate per period.
            int_nper: int
                The total number of periods (months) over which the loan is outstanding.
            float_pv: float
                The present value of the loan/annuity. The present value is the lump-sum amount that
                a series of future payments is worth right now.
            float_fv: float, optional
                The future value of the loan/annuity. The future value is the amount of money the
                series of payments will be worth after the last payment is made. If not given, the
                future value is set to 0.
            str_when: str, optional
                When payments are due (either 'begin' or 'end'). If not given, 'end' is used.

        Returns:
            The fixed monthly payment.
        """
        return npf.pmt(float_ytm, int_nper, float_pv, float_fv, str_when)

    def pv_cfs(
        self,
        list_cfs: List[float],
        float_ytm: float,
        str_capitalization: str = "compound",
        str_when: str = "end"
    ) -> Tuple[np.ndarray, np.ndarray]:
        array_nper = np.arange(1, len(list_cfs) + 1)
        array_discounted_cfs = np.array([
            self.pv(float_ytm, int_t, float_cf, 0, str_capitalization, str_when)
            for int_t, float_cf in tuple(zip(array_nper, list_cfs))
        ])
        return array_nper, array_discounted_cfs
