"""Financial mathematics calculations including time value of money and cash flow analysis."""

from typing import Literal

import numpy as np
from numpy.typing import NDArray
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

        Raises
        ------
        ValueError
            If yield is NaN or infinite
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

        Raises
        ------
        ValueError
            If yield is NaN or infinite
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
        float_pmt: float = 0.0,
        str_capitalization: Literal['compound', 'simple'] = 'compound',
        str_when: Literal['begin', 'end'] = 'end'
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
        float_pmt : float
            Periodic payment (default: 0.0)
        str_capitalization : Literal['compound', 'simple']
            Type of capitalization (default: 'compound')
        str_when : Literal['begin', 'end']
            When payments are due (default: 'end')
            
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
        if str_capitalization == 'compound':
            return npf.pv(float_ytm, int_nper, float_pmt, float_fv, str_when)
        elif str_capitalization == 'simple':
            return float_fv / (1.0 + self.simple_r(float_ytm, int_nper, 1))
        else:
            raise ValueError("str_capitalization must be 'compound' or 'simple'")

    def fv(
        self,
        float_ytm: float,
        int_nper: int,
        float_pv: float,
        float_pmt: float = 0.0,
        str_capitalization: Literal['compound', 'simple'] = 'compound',
        str_when: Literal['begin', 'end'] = 'end'
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
        float_pmt : float
            Periodic payment (default: 0.0)
        str_capitalization : Literal['compound', 'simple']
            Type of capitalization (default: 'compound')
        str_when : Literal['begin', 'end']
            When payments are due (default: 'end')
            
        Returns
        -------
        float
            Future value
            
        Raises
        ------
        ValueError
            If str_capitalization is not 'compound' or 'simple'
        """
        if str_capitalization == 'compound':
            return npf.fv(float_ytm, int_nper, float_pmt, float_pv, str_when)
        elif str_capitalization == 'simple':
            return float_pv * (1.0 + self.simple_r(float_ytm, int_nper, 1))
        else:
            raise ValueError("str_capitalization must be 'compound' or 'simple'")

    def irr(self, array_cfs: NDArray[np.float64]) -> float:
        """Calculate internal rate of return (IRR) for a series of cash flows.
        
        The IRR is the discount rate that makes the net present value of all cash flows equal 
        to zero.
        
        Parameters
        ----------
        array_cfs : NDArray[np.float64]
            Array of cash flows where the first element is the initial investment
            (negative value) and subsequent elements are the periodic cash flows
            
        Returns
        -------
        float
            Internal rate of return
            
        Raises
        ------
        ValueError
            If cash flows are empty
            If cash flows don't contain at least one positive and one negative value
            If input is not a 1D array
        """
        if array_cfs.size == 0:
            raise ValueError("Cash flows cannot be empty")
        if array_cfs.ndim != 1:
            raise ValueError("Cash flows must be a 1D array")
        if (not any(float_cf < 0 for float_cf in array_cfs)) \
            or (not any(float_cf > 0 for float_cf in array_cfs)):
            raise ValueError(
                "Cash flows must have at least one positive and one negative value")
        return npf.irr(array_cfs)

    def npv(self, float_ytm: float, array_cfs: NDArray[np.float64]) -> float:
        """Calculate net present value (NPV) of a series of cash flows.
        
        Parameters
        ----------
        float_ytm : float
            Discount rate
        array_cfs : NDArray[np.float64]
            Array of cash flows where the first element is the initial investment
            
        Returns
        -------
        float
            Net present value
        
        Raises
        ------
        ValueError
            If cash flows array is empty
            If input is not a 1D array
        """
        if array_cfs.ndim != 1:
            raise ValueError("Cash flows must be a 1D array")
        if len(array_cfs) == 0:
            raise ValueError("Cash flows array cannot be empty")
        return npf.npv(float_ytm, array_cfs)

    def pmt(
        self,
        float_ytm: float,
        int_nper: int,
        float_pv: float,
        float_fv: float = 0.0,
        str_when: Literal['begin', 'end'] = 'end'
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
        float_fv : float
            Future value (remaining balance) (default: 0.0)
        str_when : Literal['begin', 'end']
            When payments are due (default: 'end')
            
        Returns
        -------
        float
            Periodic payment amount
        
        Raises
        ------
        ValueError
            If number of periods is not positive
        """
        if int_nper <= 0:
            raise ValueError("Number of periods must be positive")
        return npf.pmt(float_ytm, int_nper, float_pv, float_fv, str_when)

    def pv_cfs(
        self,
        array_cfs: NDArray[np.float64],
        float_ytm: float,
        str_capitalization: Literal['compound', 'simple'] = 'compound',
        str_when: Literal['begin', 'end'] = 'end'
    ) -> tuple[NDArray[np.int64], NDArray[np.float64]]:
        """Calculate present values for a series of cash flows.
        
        Parameters
        ----------
        array_cfs : NDArray[np.float64]
            Array of cash flows
        float_ytm : float
            Discount rate
        str_capitalization : Literal['compound', 'simple']
            Type of capitalization (default: 'compound')
        str_when : Literal['begin', 'end']
            When payments are due (default: 'end')
            
        Returns
        -------
        tuple[NDArray[np.int64], NDArray[np.float64]]
            Tuple containing:
            - Array of period numbers
            - Array of discounted cash flows
            
        Raises
        ------
        ValueError
            If input is not a 1D array
        """
        if array_cfs.ndim != 1:
            raise ValueError("Cash flows must be a 1D array")
        array_nper = np.arange(1, len(array_cfs) + 1).astype(int)
        array_discounted_cfs = np.array([
            self.pv(float_ytm, int(int_t), float_cf, 0.0, str_capitalization, str_when)
            for int_t, float_cf in tuple(zip(array_nper, array_cfs))
        ])
        return array_nper, array_discounted_cfs