
"""Pricing B3 Brazilian Exchange Future Contracts."""

from datetime import datetime
from logging import Logger
import math
from typing import Optional

from nelson_siegel_svensson.calibrate import calibrate_ns_ols
import numpy as np
from scipy.interpolate import CubicSpline

from stpstone.analytics.quant.linear_transformations import LinearAlgebra
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.calendars.calendar_br import DatesBRB3
from stpstone.utils.parsers.lists import ListHandler


class NotionalFromPV(metaclass=TypeChecker):
    """Notional value from present value."""

    def __init__(
        self, 
        bool_persist_cache: bool = True,
        bool_reuse_cache: bool = True,
        logger: Optional[Logger] = None
    ) -> None:
        """Initialize the NotionalFromPV class.
        
        Parameters
        ----------
        bool_persist_cache : bool, optional
            If True, saves cache to disk; if False, uses in-memory cache only (default: True)
        bool_reuse_cache : bool, optional
            If True, caches in-memory; if False, does not cache in-memory (default: True)
        logger : Optional[Logger], optional
            The logger to use (default: None)
        
        Returns
        -------
        None
        """
        self.bool_persist_cache = bool_persist_cache
        self.bool_reuse_cache = bool_reuse_cache
        self.logger = logger
        self.cls_dates_br_b3 = DatesBRB3(
            bool_persist_cache=self.bool_persist_cache,
            bool_reuse_cache=self.bool_reuse_cache, 
            logger=self.logger
        )

    def generic_pricing(
        self, 
        float_pv: float, 
        float_size: float, 
        float_qty: float, 
        float_xcg_rt_1: float, 
        float_xcg_rt_2: float = 1
    ) -> float:
        """Calculate generic notional value of a future contract.
        
        Parameters
        ----------
        float_pv : float
            Present value
        float_size : float
            Size of the contract
        float_qty : float
            Quantity of contracts
        float_xcg_rt_1 : float
            Cross currency rate
        float_xcg_rt_2 : float, optional
            Cross currency rate, by default 1
        
        Returns
        -------
        float
            Notional value
        """
        return float_pv * float_size * float_qty * float_xcg_rt_1 / float_xcg_rt_2

    def dap(
        self, 
        float_pv: float, 
        float_qty: float, 
        float_pmi_idx_mm1: float, 
        float_pmi_ipca_rt_hat: float, 
        dt_pmi_last: datetime, 
        date_ref: datetime,
        dt_pmi_next: datetime,
    ) -> float:
        """Brazilian DI x IPCA (DAP) Contract Pricing.
        
        The DAP contract serves as a hedge against fluctuations in brazilian real interest rates. 
        It is based on the spread between:
        - DI rate: The 1-day interbank deposit rate (brazilian benchmark interest rate)
        - IPCA: brazilian official consumer price index (Broad National Consumer Price Index)
        
        The "cupom de IPCA" (IPCA coupon) represents the real interest rate, calculated as:
        Real Interest Rate = DI Rate - IPCA Inflation
        
        Parameters
        ----------
        float_pv : float
            Present value of the contract in BRL
        float_qty : float
            Number of contracts (quantity)
        float_pmi_idx_mm1 : float
            Previous month's PMI (Purchasing Managers' Index) value
        float_pmi_ipca_rt_hat : float
            Expected IPCA inflation rate used in PMI calculations (%)
        dt_pmi_last : datetime
            Date of the last PMI index release
        date_ref : datetime
            Pricing reference date (valuation date)
        dt_pmi_next : datetime
            Date of the next scheduled PMI index release
            
        Returns
        -------
        float
            Notional value of the DAP contract position in BRL

        Raises
        ------
        ValueError
            If dt_pmi_last is greater than dt_pmi_next
            
        Notes
        -----
        - The DI rate is published daily by CETIP/B3
        - IPCA inflation is calculated monthly by IBGE (Brazilian Institute of Geography 
        and Statistics)
        - PMI values are leading economic indicators that may affect inflation expectations
            
        Example
        -------
        >>> dap_pricing(
        ...     float_pv=1000000,
        ...     float_qty=50,
        ...     float_pmi_idx_mm1=52.3,
        ...     float_pmi_ipca_rt_hat=0.04,
        ...     dt_pmi_last=datetime(2023, 5, 15),
        ...     dt_pmi_next=datetime(2023, 6, 15),
        ...     date_ref=datetime(2023, 5, 20)
        ... )
        12500.00
        """
        float_size: float = 0.00025
        if dt_pmi_last > dt_pmi_next: 
            raise ValueError(
                "Please validate the input of date pmi last and following, the former should be " \
                + "inferior than the last"
            )
        
        # working days from the last pmi release util the following
        int_wddm = self.cls_dates_br_b3.delta_working_days(dt_pmi_last, dt_pmi_next)
        # working days from the last pmi release until the reference date
        int_wddt = self.cls_dates_br_b3.delta_working_days(dt_pmi_last, date_ref)
        # prt - pmi pro-rata tempore
        float_prt = float_pmi_idx_mm1 * float_size * (1.0 + float_pmi_ipca_rt_hat) \
            ** (int_wddt / int_wddm)
        
        return float_pv * float_qty * float_prt


class NotionalFromRate(metaclass=TypeChecker):
    """Notional value from real rate."""

    def __init__(
        self, 
        bool_persist_cache: bool = True,
        bool_reuse_cache: bool = True,
        logger: Optional[Logger] = None
    ) -> None:
        """Initialize the NotionalFromRate class.
        
        Parameters
        ----------
        bool_persist_cache : bool, optional
            If True, saves cache to disk; if False, uses in-memory cache only (default: True)
        bool_reuse_cache : bool, optional
            If True, caches in-memory; if False, does not cache in-memory (default: True)
        logger : Optional[Logger], optional
            The logger to use (default: None)
        
        Returns
        -------
        None
        """
        self.bool_persist_cache = bool_persist_cache
        self.bool_reuse_cache = bool_reuse_cache
        self.logger = logger
        self.cls_dates_br_b3 = DatesBRB3(
            bool_persist_cache=self.bool_persist_cache,
            bool_reuse_cache=self.bool_reuse_cache, 
            logger=self.logger
        )

    def di1(
        self, 
        float_nominal_rate: float, 
        date_ref: datetime,
        date_xpt: datetime, 
    ) -> float:
        """Brazilian 1-Day Interbank Deposit (DI1) Futures Contract Pricing.
        
        The DI1 contract is based on the daily average of Brazilian interbank deposit rates (DI),
        calculated and published by B3. It serves as a hedge against interest rate risk for
        DI-referenced assets/liabilities between trade date (inclusive) and expiration date 
        (exclusive).
        
        Key Features:
        - Underlying: Average daily DI rates (B3 settlement rates)
        - Notional: BRL 100,000 at expiration
        - Pricing: Present value calculated by discounting BRL 100,000 at contracted rate
        - Daily Settlement: Positions are marked-to-market using DI rates through PU adjustment 
        factors
        
        The contract uses Volume-Weighted Average Price (VWAP) methodology for daily settlement:
        - Calculation window: 10 minutes (4:10pm to 4:20pm BRT)
        - Smooths price distortions during volatile periods
        - Simultaneous pricing across all maturities
        
        Parameters
        ----------
        float_nominal_rate : float
            Nominal rate of the contract
        date_ref : datetime
            Pricing reference date
        date_xpt : datetime
            Expiration date of the contract
        
        Returns
        -------
        float
            Present value (PU) of the contract in BRL
        
        Notes
        -----
        - Daily DI rates are published by B3 (Brazilian Exchange)
        - Final settlement uses the average DI rate over contract period
        - Investors receive daily margin adjustments reflecting rate differences
        
        Formula
        -------
        PU = FV / (1 + nominal_rate)^(business_days/252)
        
        Example
        -------
        >>> di1_pricing = DI1()
        >>> di1_pricing.di1(
        ...     float_nominal_rate=0.10,  # 10% annual
        ...     date_xpt=datetime(2023, 12, 1),
        ...     int_wd_bef=2,
        ...     int_wddy=252
        ... )
        97590.23  # Present value for 10% DI1 contract
        """
        float_fv: float = 100_000.0
        int_wddy: int = 252
        int_wddt = self.cls_dates_br_b3.delta_working_days(date_ref, date_xpt)

        float_real_rate = (1.0 + float_nominal_rate) ** (int_wddt / int_wddy) - 1.0
        return float_fv / (1.0 + float_real_rate)


class RtFromPV(metaclass=TypeChecker):
    """Real rate from present value."""

    def __init__(
        self, 
        bool_persist_cache: bool = True,
        bool_reuse_cache: bool = True,
        logger: Optional[Logger] = None
    ) -> None:
        """Initialize the NotionalFromPV class.
        
        Parameters
        ----------
        bool_persist_cache : bool, optional
            If True, saves cache to disk; if False, uses in-memory cache only (default: True)
        bool_reuse_cache : bool, optional
            If True, caches in-memory; if False, does not cache in-memory (default: True)
        logger : Optional[Logger], optional
            The logger to use (default: None)
        
        Returns
        -------
        None
        """
        self.bool_persist_cache = bool_persist_cache
        self.bool_reuse_cache = bool_reuse_cache
        self.logger = logger
        self.cls_dates_br_b3 = DatesBRB3(
            bool_persist_cache=self.bool_persist_cache,
            bool_reuse_cache=self.bool_reuse_cache, 
            logger=self.logger
        )

    def ddi(
        self, 
        float_pv_di: float, 
        float_fut_dol: float, 
        float_ptax_dm1: float,
        date_ref: datetime, 
        date_xpt: datetime
    ) -> float:
        """Brazilian Dollar Interest Rate (DDI) Futures Contract Pricing.
        
        The DDI contract serves as a hedge against fluctuations in Brazil's dollar-referenced 
        interest rate.
        It represents the dollar-denominated yield for foreign investors assuming Brazilian risk, 
        based on:
        - DI Rate: 1-day interbank deposit rate (Brazil's benchmark interest rate)
        - PTAX800: Official exchange rate published by Central Bank of Brazil (BCB)
        
        The "cupom cambial" (FX coupon) is calculated as:
        FX Coupon = DI Rate - Exchange Rate Variation (PTAX)
        
        This contract is particularly useful for institutions wanting exposure to Brazilian 
        interest rates while hedging against FX risk from dollar internalization. 
        It compensates investors for the
        difference between contracted and realized dollar interest rates.
        
        Important: The contract trades on a "dirty" rate basis, ignoring exchange rate variations
        between trade date and the PTAX rate of the previous business day.
        
        Parameters
        ----------
        float_pv_di : float
            Present value of DI rate contract in BRL (Brazilian Reais)
        float_fut_dol : float
            Future dollar value (contract nominal in USD)
        float_ptax_dm1 : float
            PTAX exchange rate from previous business day (BRL/USD)
        date_ref : datetime
            Contract reference date
        date_xpt : datetime
            Contract settlement/expiration date
        
        Returns
        -------
        float
            The calculated real interest rate (annualized)
        
        Notes
        -----
        - DI rates are published daily by CETIP/B3
        - PTAX800 rates are published by Central Bank of Brazil (BCB)
        - The "dirty rate" convention means FX fluctuations between trade date and
        previous business day are not considered
        
        Formula
        -------
        Real Rate = [(PV_DI / FV_DI) / (Fut_Dol / PTAX_DM1) - 1] * (Days_Year / Days_To_Settlement)
        
        Example
        -------
        >>> ddi_pricing = DDI()
        >>> ddi_pricing.ddi(
        ...     float_pv_di=95000.0,
        ...     float_fut_dol=1.0,
        ...     float_ptax_dm1=5.20,
        ...     date_xpt=datetime(2023, 12, 1),
        ...     int_wd_bef=2
        ... )
        0.0652  # 6.52% annualized real rate
        """
        int_cddy: int = 365
        float_fv_di: float = 100_000.0
        
        int_cddt = self.cls_dates_br_b3.delta_calendar_days(date_ref, date_xpt)
        return ((float_pv_di / float_fv_di) / (float_fut_dol / float_ptax_dm1) - 1.0) \
            * int_cddy / int_cddt


class TSIR(metaclass=TypeChecker):
    """Term Structure of Interest Rates (TSIR)."""

    def __init__(self) -> None:
        """Initialize the TSIR class."""
        self.cls_list_handler = ListHandler()

    def flat_forward(
        self, 
        dict_nper_rates: dict[int, float], 
        int_wddy: float = 252
    ) -> dict[int, float]:
        """Calculate flat forward rates from a term structure of spot rates.

        Flat forward rates represent the implied future interest rates between two periods,
        derived from the current spot rate curve. These rates are "flat" because they assume
        a constant forward rate between compounding periods.

        Financial Interpretation:
        - The forward rate between time t₁ and t₂ is the break-even rate that would make an
        investor indifferent between:
        1) Investing for the entire period t₂ at the spot rate r₂
        2) Investing for period t₁ at rate r₁ and reinvesting at the forward rate f₁₂
        - Used in fixed income valuation, interest rate derivatives, and yield curve analysis

        Parameters
        ----------
        dict_nper_rates : dict[int, float]
            Dictionary mapping periods to spot rates in decimal form (e.g., {1: 0.05, 2: 0.055})
            Format: {period_length: spot_rate}
            Example: {30: 0.042, 90: 0.046} for 30-day and 90-day rates
        int_wddy : float, optional
            Business day count convention (default: 252 days)
            Used for converting rates to daily equivalents

        Returns
        -------
        dict[int, float]
            Dictionary of flat forward rates between consecutive periods
            Format: {period: forward_rate}
            Example: {30: 0.042, 60: 0.048} where 0.048 is the forward rate between day 30-90

        Notes
        -----
        - Assumes piecewise constant forward rates between observed points
        - Rates should be in consistent time units (all daily, monthly, etc.)
        - Uses the formula:
        f₁₂ = [(1+r₂)^t₂ / (1+r₁)^t₁]^(1/(t₂-t₁)) - 1

        Example
        -------
        >>> rates = {30: 0.04, 60: 0.045}  # 4% for 30d, 4.5% for 60d
        >>> flat_forward(rates)
        {30: 0.04, 60: 0.05006}  # Implied 30d->60d forward rate ≈5.006%
        """
        # setting variables
        dict_ = dict()
        # store in memory dictionary with rates per nper
        for curr_nper_wrkdays in range(list(dict_nper_rates.keys())[0],
                                       list(dict_nper_rates.keys())[-1] + 1):
            # forward rate - interpolation for two boundaries
            nper_upper_bound = self.cls_list_handler.get_lower_upper_bound(
                list(dict_nper_rates.keys()), curr_nper_wrkdays)['upper_bound']
            nper_lower_bound = self.cls_list_handler.get_lower_upper_bound(
                list(dict_nper_rates.keys()), curr_nper_wrkdays)['lower_bound']
            rate_upper_bound = dict_nper_rates[nper_upper_bound]
            rate_lower_bound = dict_nper_rates[nper_lower_bound]
            forward_rate = math.pow(math.pow(
                1 + rate_upper_bound, nper_upper_bound / int_wddy) /
                math.pow(
                1 + rate_lower_bound, nper_lower_bound / int_wddy),
                (nper_upper_bound - nper_lower_bound) / int_wddy) - 1
            # flat forward rate for a given nper, between two boundaries
            dict_[curr_nper_wrkdays] = \
                ((1 + forward_rate) ** ((curr_nper_wrkdays - nper_lower_bound) / int_wddy) *
                 (1 + rate_lower_bound) ** (nper_lower_bound / int_wddy)) ** \
                (int_wddy / curr_nper_wrkdays) - 1
        # return a term structure of interest rates
        return dict_

    def cubic_spline(self, dict_nper_rates: dict[int, float]) -> dict[int, float]:
        """Construct a cubic spline interpolation of the term structure of interest rates.

        This method generates a smooth yield curve by fitting piecewise cubic polynomials to 
        observed market rates at different maturities, ensuring:
        - Continuous curve (C⁰ continuity)
        - Continuous first derivative (C¹ continuity) 
        - Continuous second derivative (C² continuity)

        Parameters
        ----------
        dict_nper_rates : dict[int, float]
            Dictionary mapping time periods to observed market rates in decimal form.
            Format: {period: rate}
            Example: {30: 0.015, 90: 0.018, 180: 0.020, 360: 0.022}
            Where keys represent days/months and values are corresponding spot rates

        Returns
        -------
        dict[int, float]
            Dictionary with interpolated rates at all input periods using cubic spline.
            Format: {period: interpolated_rate}

        Raises
        ------
        ValueError
            If input has fewer than 3 points or if periods are not in ascending order
        TypeError
            If input types are invalid
        Exception
            For other interpolation errors
        """
        # Input validation
        if len(dict_nper_rates) < 3:
            raise ValueError(
                "At least 3 points are required for cubic spline interpolation. "
                f"Received {len(dict_nper_rates)} points."
            )
        
        if not all(isinstance(k, int) for k in dict_nper_rates):
            raise TypeError("All period keys must be integers")
        
        if not all(isinstance(v, (float, int)) for v in dict_nper_rates.values()):
            raise TypeError("All rate values must be numeric")
        
        periods = sorted(dict_nper_rates.keys())
        if periods != list(dict_nper_rates.keys()):
            raise ValueError("Periods must be in strictly ascending order")
        
        try:
            # convert to numpy arrays for scipy
            array_x = np.array(periods, dtype=np.float64)
            array_y = np.array([dict_nper_rates[p] for p in periods], dtype=np.float64)
            
            # create cubic spline with natural boundary conditions
            cs = CubicSpline(array_x, array_y, bc_type='natural')
            
            # generate all integer periods between min and max
            min_period = min(periods)
            max_period = max(periods)
            x_interp = np.arange(min_period, max_period + 1)
            y_interp = cs(x_interp)
            
            # create output dictionary
            return dict(zip(x_interp.astype(int), y_interp))
        
        except Exception as e:
            raise Exception(
                f"Failed to construct cubic spline: {str(e)}"
            ) from e

    def third_degree_polynomial_cubic_spline(
        self, 
        list_constants_cubic_spline: list[float], 
        int_nper_wd: int, 
        bool_sup_list: bool, 
        int_num_constants_cubic_spline: int = 8
    ) -> float:
        """Evaluate a cubic spline polynomial segment for term structure modeling.

        This method evaluates either the lower or upper segment of a piecewise cubic polynomial
        used in financial curve construction, particularly for yield curve interpolation.

        Mathematical Form:
        - Lower segment (bool_sup_list=False): Σ (a_i * t^i) for i=0 to 3
        - Upper segment (bool_sup_list=True): Σ (b_i * t^(i-4)) for i=4 to 7
        where t = int_nper_wd and coefficients are in list_constants_cubic_spline

        Parameters
        ----------
        list_constants_cubic_spline : list[float]
            List of 8 spline coefficients ordered as [a0, a1, a2, a3, b0, b1, b2, b3]
            where a_i are coefficients for the lower polynomial segment (t <= knot point)
            and b_i are coefficients for the upper polynomial segment (t > knot point)
        int_nper_wd : int
            Time point (in working days) at which to evaluate the spline
        bool_sup_list : bool
            Segment selection flag:
            - False: Evaluate lower polynomial segment (a coefficients)
            - True: Evaluate upper polynomial segment (b coefficients)
        int_num_constants_cubic_spline : int, optional
            Total number of spline coefficients (default: 8)
            Must be even as it's split equally between lower/upper segments

        Returns
        -------
        float
            Evaluated spline value at int_nper_wd

        Raises
        ------
        Exception
            If list_constants_cubic_spline length doesn't match int_num_constants_cubic_spline

        Notes
        -----
        - Part of a piecewise cubic spline implementation for yield curve construction
        - Maintains C² continuity at knot points when used with proper coefficients
        - Working days convention should match the rest of the term structure model

        Example
        -------
        >>> # Coefficients for a sample spline segment
        >>> coeffs = [0.02, 0.001, -0.0001, 0.00001, 0.025, -0.0005, 0.00002, -0.000001]
        >>> # Evaluate lower segment at 30 working days
        >>> third_degree_polynomial_cubic_spline(coeffs, 30, False)
        0.02371
        >>> # Evaluate upper segment at 45 working days
        >>> third_degree_polynomial_cubic_spline(coeffs, 45, True)
        0.02412
        """
        if len(list_constants_cubic_spline) != int_num_constants_cubic_spline:
            raise Exception('Poor defined list of constants for cubic spline, '
                            + f'ought have {int_num_constants_cubic_spline} elements')
        if not bool_sup_list:
            return sum([list_constants_cubic_spline[x] * int_nper_wd ** x 
                        for x in range(0, int(int_num_constants_cubic_spline / 2))])
        else:
            return sum([list_constants_cubic_spline[x] * int_nper_wd \
                        ** (x - int_num_constants_cubic_spline / 2) 
                        for x in range(int(int_num_constants_cubic_spline / 2), 
                                       int_num_constants_cubic_spline)])

    def literal_cubic_spline(
        self, 
        dict_nper_rates: dict[int, float], 
        bool_debug: bool = False
    ) -> dict[int, float]:
        """Construct a natural cubic spline interpolation of the term structure of interest rates.

        This method implements a piecewise cubic spline interpolation for yield curve construction,
        ensuring C² continuity (continuous value, first derivative, and second derivative) at all
        knot points. The spline is constructed using market-observed rates at specific maturities
        and interpolates rates for all intermediate points.

        Methodology:
        1. For each target maturity point, identifies the surrounding knot points (lower, middle, 
        upper)
        2. Sets up a system of equations enforcing:
        - Value matching at knot points
        - First derivative continuity at middle points
        - Second derivative continuity at middle points
        - Natural boundary conditions (second derivative = 0 at endpoints)
        3. Solves for cubic polynomial coefficients in each segment
        4. Evaluates the appropriate polynomial segment for each requested maturity

        Parameters
        ----------
        dict_nper_rates : dict[int, float]
            Dictionary mapping maturity periods (in working days) to observed market rates.
            Format: {maturity_days: rate}
            Example: {30: 0.015, 90: 0.018, 180: 0.020, 360: 0.022}
            Requirements:
            - Keys must be in ascending order
            - Minimum of 3 points required for cubic spline
            - Rates should be in decimal form (e.g., 0.015 for 1.5%)

        bool_debug : bool, optional
            Debug flag to print intermediate calculations, by default False
            When True, prints:
            - Current working day being calculated
            - Interpolated rate for each day

        Returns
        -------
        dict[int, float]
            Dictionary with interpolated rates for all maturities between the first and last
            input points, with keys representing working days and values representing the
            interpolated rates.
            Format: {maturity_days: interpolated_rate}
            Example: {30: 0.015, 31: 0.0151, ..., 360: 0.022}

        Raises
        ------
        Exception
            - If the input dictionary has fewer than 3 points
            - If the bounds identification returns unexpected format
            - If the linear system cannot be solved

        Notes
        -----
        - Uses natural cubic spline boundary conditions (second derivative = 0 at endpoints)
        - Maintains monotonicity between observed market rates
        - Working day convention should be consistent across all inputs
        - The implementation solves for 8 coefficients (4 per polynomial segment)
        - Matrix formulation follows standard cubic spline construction methods

        Example
        -------
        >>> market_rates = {30: 0.015, 90: 0.018, 180: 0.020, 360: 0.022}
        >>> spline_curve = literal_cubic_spline(market_rates)
        >>> spline_curve[60]  # Interpolated 60-day rate
        0.0167
        >>> spline_curve[150]  # Interpolated 150-day rate
        0.0193
        """
        # setting variables
        dict_ = dict()
        # tsir - nper x rate
        for curr_nper_wrkdays in range(list(dict_nper_rates.keys())[0],
                                       list(dict_nper_rates.keys())[-1] + 1):
            # three-bounds-dictionary, nper-wise
            dict_lower_mid_upper_bound_nper = self.cls_list_handler.get_lower_mid_upper_bound(
                list(dict_nper_rates.keys()), curr_nper_wrkdays)
            if len(dict_lower_mid_upper_bound_nper.keys()) == 4:
                # working days for each bound and boolean of whether its the ending element of
                #   original list within or not
                du1, du2, du3, bool_sup_list = dict_lower_mid_upper_bound_nper.values()
                i1, i2, i3 = [dict_nper_rates[v]
                              for k, v in dict_lower_mid_upper_bound_nper.items()
                              if k != 'end_of_list']
            else:
                raise Exception("Dimension-wise the list ought have 4 positions, contemplating: "
                                + "lower bound, middle bound, upper bound and end of list boolean")
            # arrais working days and IRR (YTM)
            array_wd = np.array([
                [1, du1, du1 ** 2, du1 ** 3, 0, 0, 0, 0],
                [1, du2, du2 ** 2, du2 ** 3, 0, 0, 0, 0],
                [0, 0, 0, 0, 1, du2, du2 ** 2, du2 ** 3],
                [0, 0, 0, 0, 1, du3, du3 ** 2, du3 ** 3],
                [0, 1, 2 * du2, 3 * du2 ** 2, 0, -1, -2 * du2, -3 * du2 ** 2],
                [0, 0, 2, 6 * du2, 0, 0, -2, -6 * du2],
                [0, 0, 2, 6 * du1, 0, 0, 0, 0],
                [0, 0, 0, 0, 0, 0, 2, 6 * du3]
            ])
            array_ytm = np.array([
                [i1],
                [i2],
                [i2],
                [i3],
                [0],
                [0],
                [0],
                [0]
            ])
            # constants array
            array_constants_solution = LinearAlgebra().matrix_multiplication(
                LinearAlgebra().transpose_matrix(array_wd), array_ytm
            )
            print(array_constants_solution)
            # rates of return (IRR, ytm) for the current working day nper
            dict_[curr_nper_wrkdays] = self.third_degree_polynomial_cubic_spline(
                array_constants_solution, curr_nper_wrkdays, bool_sup_list,
                len(array_constants_solution)
            )
            if bool_debug:
                print(curr_nper_wrkdays, dict_[curr_nper_wrkdays])
        # output - term structure of interest rates
        return dict_

    def nelson_siegel(
        self, 
        dict_nper_rates: dict[int, float], 
        float_tau_first_assumption: float = 1.0, 
        int_n_smp: int = None
    ) -> dict[int, float]:
        """Nelson Siegel term structure of interest rates.
        
        Parameters
        ----------
        dict_nper_rates : dict[int, float]
            Dictionary of interest rates by nper.
        float_tau_first_assumption : float
            First assumption of tau.
        int_n_smp : int
            Number of samples.

        Returns
        -------
        dict[int, float]
            Term structure of interest rates.
        
        References
        ----------
        https://nelson-siegel-svensson.readthedocs.io/en/latest/readme.html#calibration
        """
        y, _ = calibrate_ns_ols(
            np.array(list(dict_nper_rates.keys())),
            np.array(list(dict_nper_rates.values())), float_tau_first_assumption)
        if int_n_smp is None:
            t = np.linspace(list(dict_nper_rates.keys())[
                            0], list(dict_nper_rates.keys())[-1], int(list(dict_nper_rates.keys())[
                                -1] - list(dict_nper_rates.keys())[0] + 1))
            t_aux = range(list(dict_nper_rates.keys())[
                0], list(dict_nper_rates.keys())[-1])
        else:
            t = np.linspace(list(dict_nper_rates.keys())[
                            0], list(dict_nper_rates.keys())[-1], int_n_smp)
        dict_ = dict(zip(t_aux, y(t)))
        return dict_
