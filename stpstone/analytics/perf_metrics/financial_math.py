"""Financial mathematics calculations including time value of money and cash flow analysis."""

import numpy as np
import numpy_financial as npf

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class FinancialMath(metaclass=TypeChecker):
    """A collection of financial mathematics calculations.
    
    This class provides methods for common financial calculations including:
    - Compound and simple interest rates
    - Present and future value calculations
    - Internal rate of return (IRR)
    - Net present value (NPV)
    - Payment calculations
    - Cash flow analysis
    
    All methods are type-checked at runtime using the TypeChecker metaclass.
    """

    def compound_r(self, float_ytm: float, int_nper: int, int_compound_n: int) -> float:
        """Calculate compound interest rate.
        
        Parameters
        ----------
        float_ytm : float
            Yield to maturity (annual interest rate)
        int_nper : int
            Number of periods
        int_compound_n : int
            Number of compounding periods per year
            
        Returns
        -------
        float
            Compound interest rate
        """
        if np.isnan(float_ytm):
            raise ValueError("Yield cannot be NaN")
        if np.isinf(float_ytm):
            raise ValueError("Yield cannot be infinite")
        return float((1.0 + float_ytm) ** (float(int_nper) / float(int_compound_n))) - 1.0

    def simple_r(self, float_ytm: float, int_nper: int, int_compound_n: int) -> float:
        """Calculate simple interest rate.
        
        Parameters
        ----------
        float_ytm : float
            Yield to maturity (annual interest rate)
        int_nper : int
            Number of periods
        int_compound_n : int
            Number of compounding periods per year
            
        Returns
        -------
        float
            Simple interest rate
        """
        if np.isnan(float_ytm):
            raise ValueError("Yield cannot be NaN")
        if np.isinf(float_ytm):
            raise ValueError("Yield cannot be infinite")
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
        """Calculate present value of a future cash flow.
        
        Parameters
        ----------
        float_ytm : float
            Yield to maturity (annual interest rate)
        int_nper : int
            Number of periods
        float_fv : float
            Future value
        float_pmt : float, optional
            Periodic payment, by default 0
        str_capitalization : str, optional
            Type of capitalization ('compound' or 'simple'), by default "compound"
        str_when : str, optional
            When payments are due ('begin' or 'end'), by default "end"
            
        Returns
        -------
        float
            Present value
            
        Raises
        ------
        ValueError
            If str_capitalization is not 'compound' or 'simple'
        """
        if float_ytm == 0:
            return -float_fv - float_pmt * int_nper
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
        """Calculate future value of a present cash flow.
        
        Parameters
        ----------
        float_ytm : float
            Yield to maturity (annual interest rate)
        int_nper : int
            Number of periods
        float_pv : float
            Present value
        float_pmt : float, optional
            Periodic payment, by default 0
        str_capitalization : str, optional
            Type of capitalization ('compound' or 'simple'), by default "compound"
        str_when : str, optional
            When payments are due ('begin' or 'end'), by default "end"
            
        Returns
        -------
        float
            Future value
            
        Raises
        ------
        ValueError
            If str_capitalization is not 'compound' or 'simple'
        """
        if str_capitalization == "compound":
            return npf.fv(float_ytm, int_nper, float_pmt, float_pv, str_when)
        elif str_capitalization == "simple":
            return float_pv * (1.0 + self.simple_r(float_ytm, int_nper, 1))
        else:
            raise ValueError("str_capitalization must be 'compound' or 'simple'")

    def irr(self, list_cfs: list[float]) -> float:
        """Calculate internal rate of return (IRR) for a series of cash flows.
        
        The IRR is the discount rate that makes the net present value of all cash flows equal 
        to zero.
        
        Parameters
        ----------
        list_cfs : list[float]
            List of cash flows where the first element is the initial investment
            (negative value) and subsequent elements are the periodic cash flows
            
        Returns
        -------
        float
            Internal rate of return
            
        Raises
        ------
        ValueError
            If cash flows don't contain at least one positive and one negative value
        """
        if (not any(float_cf < 0 for float_cf in list_cfs)) \
            or (not any(float_cf > 0 for float_cf in list_cfs)):
            raise ValueError(
                "Cash flows must have at least one positive and one negative value")
        return npf.irr(list_cfs)

    def npv(self, float_ytm: float, list_cfs: list[float]) -> float:
        """Calculate net present value (NPV) of a series of cash flows.
        
        Parameters
        ----------
        float_ytm : float
            Discount rate
        list_cfs : list[float]
            List of cash flows where the first element is the initial investment
            
        Returns
        -------
        float
            Net present value
        """
        if not list_cfs:
            raise ValueError("Cash flows list cannot be empty")
        return npf.npv(float_ytm, list_cfs)

    def pmt(
        self,
        float_ytm: float,
        int_nper: int,
        float_pv: float,
        float_fv: float = 0,
        str_when: str = "end"
    ) -> float:
        """Calculate periodic payment for a loan or annuity.
        
        Parameters
        ----------
        float_ytm : float
            Interest rate per period
        int_nper : int
            Total number of payments
        float_pv : float
            Present value (principal)
        float_fv : float, optional
            Future value (remaining balance), by default 0
        str_when : str, optional
            When payments are due ('begin' or 'end'), by default "end"
            
        Returns
        -------
        float
            Periodic payment amount
        """
        if int_nper <= 0:
            raise ValueError("Number of periods must be positive")
        return npf.pmt(float_ytm, int_nper, float_pv, float_fv, str_when)

    def pv_cfs(
        self,
        list_cfs: list[float],
        float_ytm: float,
        str_capitalization: str = "compound",
        str_when: str = "end"
    ) -> tuple[np.ndarray, np.ndarray]:
        """Calculate present values for a series of cash flows.
        
        Parameters
        ----------
        list_cfs : list[float]
            List of cash flows
        float_ytm : float
            Discount rate
        str_capitalization : str, optional
            Type of capitalization ('compound' or 'simple'), by default "compound"
        str_when : str, optional
            When payments are due ('begin' or 'end'), by default "end"
            
        Returns
        -------
        tuple[np.ndarray, np.ndarray]
            Tuple containing:
            - Array of period numbers
            - Array of discounted cash flows
        """
        array_nper = np.arange(1, len(list_cfs) + 1).astype(int)
        array_discounted_cfs = np.array([
            self.pv(float_ytm, int(int_t), float_cf, 0.0, str_capitalization, str_when)
            for int_t, float_cf in tuple(zip(array_nper, list_cfs))
        ])
        return array_nper, array_discounted_cfs