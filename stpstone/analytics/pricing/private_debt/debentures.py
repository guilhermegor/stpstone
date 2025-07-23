"""Module for debenture pricing."""

from datetime import date
from typing import Optional

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class DebenturesPricer(metaclass=TypeChecker):
    """Class for debenture pricing."""

    def __init__(
        self,
        float_fv: float = 1000,
        float_coupon_r: float = 0.1,
        int_coupon_freq: int = 2,
        int_maturity_years: int = 5,
        float_yield: float = 0.12,
        float_tax_rate: float = 0.15
    ) -> None:
        """Initialize the debenture with default or provided parameters.

        Parameters
        ----------
        float_fv : float, optional
            The face value of the debenture.
        float_coupon_r : float, optional
            The coupon rate of the debenture.
        int_coupon_freq : int, optional
            The coupon frequency of the debenture.
        int_maturity_years : int, optional
            The maturity years of the debenture.
        float_yield : float, optional
            The yield of the debenture.
        float_tax_rate : float, optional
            The tax rate on the debenture's coupons.

        Returns
        -------
        None
        """
        self.float_fv = float_fv
        self.float_coupon_r = float_coupon_r
        self.int_coupon_freq = int_coupon_freq
        self.int_maturity_years = int_maturity_years
        self.float_yield = float_yield
        self.float_tax_rate = float_tax_rate
        self._validate_inputs()

    def _validate_inputs(self) -> None:
        """Validate input parameters.
        
        Raises
        ------
        ValueError
            If any input parameter is invalid
        """
        if self.float_fv <= 0:
            raise ValueError("Face value must be positive")
        if not 0 <= self.float_coupon_r <= 1:
            raise ValueError("Coupon rate must be between 0 and 1")
        if self.int_coupon_freq <= 0:
            raise ValueError("Coupon frequency must be positive")
        if self.int_maturity_years <= 0:
            raise ValueError("Maturity years must be positive")
        if self.float_yield <= -1:
            raise ValueError("Yield must be greater than -1")
        if not 0 <= self.float_tax_rate < 1:
            raise ValueError("Tax rate must be between 0 and 1")

    def _calculate_coupon_dates(self, dt_settlement: date) -> tuple[date, date]:
        """Calculate previous and next coupon dates based on settlement date.

        Parameters
        ----------
        dt_settlement : date
            The settlement date for the calculation.

        Returns
        -------
        tuple[date, date]
        """
        # simplified implementation
        int_months_between = 12 // self.int_coupon_freq

        # find most recent coupon date before settlement
        dt_last_coupon = date(dt_settlement.year, 1, 1)
        while dt_last_coupon <= dt_settlement:
            dt_last_coupon = dt_last_coupon.replace(
                month=dt_last_coupon.month + int_months_between)
        dt_last_coupon = dt_last_coupon.replace(month=dt_last_coupon.month - int_months_between)

        # next coupon is int_months_between after last coupon
        dt_next_coupon = dt_last_coupon.replace(month=dt_last_coupon.month + int_months_between)

        return dt_last_coupon, dt_next_coupon

    def _count_days_between(self, dt_start: date, dt_end: date) -> int:
        """Calculate days between dates using 30/360 convention.

        Parameters
        ----------
        dt_start : date
            The start date for the calculation.
        dt_end : date
            The end date for the calculation.

        Returns
        -------
        int
        """
        dt_d1 = min(dt_start.day, 30)
        dt_d2 = min(dt_end.day, 30) if dt_d1 == 30 else dt_end.day
        return (dt_end.year - dt_start.year) * 360 + \
               (dt_end.month - dt_start.month) * 30 + \
               (dt_d2 - dt_d1)

    def _calculate_ytc_price(
        self,
        ytc_rate: float,
        float_call_price: float,
        call_periods: int
    ) -> float:
        """Calculate float_price given a yield-to-call rate.

        Parameters
        ----------
        ytc_rate : float
            The yield-to-call rate.
        float_call_price : float
            The call price.
        call_periods : int
            The number of periods to call.

        Returns
        -------
        float
        """
        float_price = 0.0
        float_coupon_payment = (self.float_fv * self.float_coupon_r /
                         self.int_coupon_freq) * (1 - self.float_tax_rate)
        for t in range(1, call_periods + 1):
            if t == call_periods:
                float_cash_flow = float_coupon_payment + float_call_price
            else:
                float_cash_flow = float_coupon_payment
            float_price += float_cash_flow / ((1 + ytc_rate/self.int_coupon_freq) ** t)
        return float_price

    def calculate_price(self) -> float:
        """Calculate the theoretical float_price of the debenture considering tax on coupons.

        Parameters
        ----------
        None

        Returns
        -------
        float
        """
        float_price: float = 0.0
        int_periods: int = int(self.int_maturity_years * self.int_coupon_freq)
        float_period_rate: float = self.float_yield / self.int_coupon_freq
        float_coupon_payment: float = (self.float_fv * self.float_coupon_r /
                               self.int_coupon_freq) * (1 - self.float_tax_rate)
        for t in range(1, int_periods + 1):
            float_cf: float = float_coupon_payment + self.float_fv if t == int_periods \
                else float_coupon_payment
            float_price += float_cf / ((1 + float_period_rate) ** t)
        return float_price

    def current_yield(self, float_market_price: Optional[float] = None) -> float:
        """Calculate current yield (annual coupon payment / current market float_price).

        Parameters
        ----------
        float_market_price : Optional[float]
            The current market float_price of the debenture.

        Returns
        -------
        float

        Raises
        ------
        ValueError
            If the market price is not positive.
        """
        if float_market_price is not None and float_market_price <= 0:
            raise ValueError("Price must be positive")

        float_price: float = float_market_price if float_market_price is not None \
            else self.calculate_price()
        float_annual_coupon: float = self.float_fv * self.float_coupon_r * (
            1 - self.float_tax_rate)
        return float_annual_coupon / float_price

    def capital_gain_yield(self, float_market_price: Optional[float] = None) -> float:
        """Calculate expected capital gain/loss yield to maturity.

        Parameters
        ----------
        float_market_price : Optional[float]
            The current market float_price of the debenture.

        Returns
        -------
        float

        Raises
        ------
        ValueError
            If the market price is not positive.
        """
        if float_market_price is not None and float_market_price <= 0:
            raise ValueError("Price must be positive")

        float_price: float = float_market_price if float_market_price is not None \
            else self.calculate_price()
        return (self.float_fv - float_price) / float_price

    def total_return(self, float_market_price: Optional[float] = None) -> float:
        """Calculate total expected return to maturity (current yield + capital gain).

        Parameters
        ----------
        float_market_price : Optional[float]
            The current market float_price of the debenture.

        Returns
        -------
        float
        """
        return (self.current_yield(float_market_price) +
                self.capital_gain_yield(float_market_price))

    def accrued_interest(
        self,
        dt_settlement: Optional[date] = None,
        dt_last_coupon: Optional[date] = None,
        dt_next_coupon: Optional[date] = None,
        str_day_count_convention: str = "actual/actual"
    ) -> float:
        """Calculate float_accrued interest since last coupon payment.

        Parameters
        ----------
        dt_settlement : Optional[date]
            The settlement date for the calculation.
        dt_last_coupon : Optional[date]
            The date of the last coupon payment.
        dt_next_coupon : Optional[date]
            The date of the next coupon payment.
        str_day_count_convention : str, optional
            The day count convention for the calculation.

        Returns
        -------
        float

        Raises
        ------
        ValueError
            If the settlement date is not provided.
            If the last or next coupon date is not provided.
            If the last coupon date is after the next coupon date.
        """
        # validate inputs
        if dt_settlement is None:
            dt_settlement = date.today()
        if dt_last_coupon is None or dt_next_coupon is None:
            last_cp, next_cp = self._calculate_coupon_dates(dt_settlement)
            dt_last_coupon = dt_last_coupon or last_cp
            dt_next_coupon = dt_next_coupon or next_cp
        if dt_last_coupon > dt_next_coupon:
            raise ValueError("Last coupon date must be before next coupon date")
        print(f"***** Settlement: {dt_settlement}, Last: {dt_last_coupon}, Next: {dt_next_coupon}, Test: {dt_last_coupon <= dt_settlement <= dt_next_coupon}") # noqa
        if not (dt_last_coupon <= dt_settlement <= dt_next_coupon):
            raise ValueError("Settlement date must be between coupon dates")

        # calculate days according to convention
        if str_day_count_convention.lower() == "actual/actual":
            int_days_accrued = (dt_settlement - dt_last_coupon).days
            int_days_in_period = (dt_next_coupon - dt_last_coupon).days
        elif str_day_count_convention.lower() == "30/360":
            int_days_accrued = self._count_days_between(dt_last_coupon, dt_settlement)
            int_days_in_period = self._count_days_between(dt_last_coupon, dt_next_coupon)
        else:
            raise ValueError(f"Unsupported day count convention: {str_day_count_convention}")
        float_coupon_payment = (self.float_fv * self.float_coupon_r) / self.int_coupon_freq
        float_accrued = float_coupon_payment * (int_days_accrued / int_days_in_period)

        # apply tax if this is a taxable coupon
        if self.float_tax_rate > 0:
            float_accrued *= (1 - self.float_tax_rate)

        return float_accrued

    def dirty_price(
        self,
        float_clean_price: Optional[float] = None,
        dt_settlement: Optional[date] = None,
        dt_last_coupon: Optional[date] = None,
        dt_next_coupon: Optional[date] = None,
        str_day_count_convention: str = "actual/actual"
    ) -> float:
        """Calculate dirty price (clean price + accrued interest).

        Parameters
        ----------
        float_clean_price : Optional[float]
            The clean price of the bond.
        dt_settlement : Optional[date]
            The settlement date for the calculation.
        dt_last_coupon : Optional[date]
            The date of the last coupon payment.
        dt_next_coupon : Optional[date]
            The date of the next coupon payment.
        str_day_count_convention : str, optional
            The day count convention for the calculation.

        Returns
        -------
        float
        """
        clean_price: float = float_clean_price if float_clean_price is not None \
            else self.calculate_price()
        accrued = self.accrued_interest(
            dt_settlement=dt_settlement,
            dt_last_coupon=dt_last_coupon,
            dt_next_coupon=dt_next_coupon,
            str_day_count_convention=str_day_count_convention
        )
        return clean_price + accrued

    def holding_period_return(
        self,
        float_purchase_price: float,
        float_sale_price: float,
        int_holding_period: float
    ) -> float:
        """Calculate annualized holding period return.

        Parameters
        ----------
        float_purchase_price : float
            The purchase price of the bond.
        float_sale_price : float
            The sale price of the bond.
        int_holding_period : float
            The holding period in years.

        Returns
        -------
        float

        Raises
        ------
        ValueError
            If the purchase price is not positive.
            If the holding period is not positive.
        """
        # validate inputs
        if float_purchase_price <= 0:
            raise ValueError("Purchase price must be positive")
        if int_holding_period <= 0:
            raise ValueError("Holding period must be positive")

        # calculate return
        float_total_return: float = (float_sale_price - float_purchase_price) \
            / float_purchase_price

        return (1 + float_total_return) ** (1 / int_holding_period) - 1

    def credit_spread(self, float_risk_free_rate: float) -> float:
        """Calculate credit spread over risk-free rate.

        Parameters
        ----------
        float_risk_free_rate : float
            The risk-free rate.

        Returns
        -------
        float
        """
        return self.float_yield - float_risk_free_rate

    def early_redemption_value(self, float_call_price: float, float_call_date: float) -> float:
        """Calculate early redemption value.

        Parameters
        ----------
        float_call_price : float
            The call price.
        float_call_date : float
            The call date in years.

        Returns
        -------
        float

        Raises
        ------
        ValueError
            If the call date is after maturity.
            If the call date is not positive.
        """
        # validate inputs
        if float_call_date > self.int_maturity_years:
            raise ValueError("Call date cannot be after maturity")
        if float_call_date <= 0:
            raise ValueError("Call date must be positive")

        float_call_value = 0.0
        # value if held to maturity
        float_maturity_value = self.calculate_price()
        # present value if called
        int_call_periods = int(float_call_date * self.int_coupon_freq)
        float_period_rate = self.float_yield / self.int_coupon_freq
        float_coupon_payment = (self.float_fv * self.float_coupon_r /
                        self.int_coupon_freq) * (1 - self.float_tax_rate)
        for t in range(1, int_call_periods + 1):
            if t == int_call_periods:
                # at call date, investor receives call float_price plus final coupon
                float_cf = float_coupon_payment + float_call_price
            else:
                float_cf = float_coupon_payment

            float_call_value += float_cf / ((1 + float_period_rate) ** t)

        return min(float_maturity_value, float_call_value)

    def inflation_adjusted_return(self, float_inflation_rate: float) -> float:
        """Calculate real return adjusted for inflation.

        Parameters
        ----------
        float_inflation_rate : float
            The inflation rate.

        Returns
        -------
        float
        """
        return (1 + self.float_yield) / (1 + float_inflation_rate) - 1

    def yield_to_call(
        self,
        float_call_price: float,
        float_call_date: float,
        float_current_price: float,
        int_max_iterations: int = 100,
        float_precision: float = 1e-6
    ) -> float:
        """Calculate yield to call using Newton-Raphson method.

        Parameters
        ----------
        float_call_price : float
            The call price.
        float_call_date : float
            The call date in years.
        float_current_price : float
            The current float_price.
        int_max_iterations : int, optional
            The maximum number of iterations.
        float_precision : float, optional
            The desired precision.

        Returns
        -------
        float

        Raises
        ------
        ValueError
            If the call date is not positive.
            If the current price is not positive.
            If yield to call calculation does not converge.
        """
        # validate inputs
        if float_call_date <= 0:
            raise ValueError("Call date must be positive")
        if float_current_price <= 0:
            raise ValueError("Current price must be positive")

        # calculate yield
        float_ytc = self.float_coupon_r
        int_periods = int(float_call_date * self.int_coupon_freq)
        float_coupon_payment = (self.float_fv * self.float_coupon_r /
                         self.int_coupon_freq) * (1 - self.float_tax_rate)
        for _ in range(int_max_iterations):
            # calculate float_price and derivative at current guess
            float_price = 0.0
            float_duration = 0.0
            for t in range(1, int_periods + 1):
                if t == int_periods:
                    float_cash_flow = float_coupon_payment + float_call_price
                else:
                    float_cash_flow = float_coupon_payment
                float_discount_rate = (1 + float_ytc/self.int_coupon_freq) ** t
                float_price += float_cash_flow / float_discount_rate
                float_duration += t * float_cash_flow / (float_discount_rate * \
                                                   (1 + float_ytc/self.int_coupon_freq))
            # newton-Raphson update
            price_diff = float_price - float_current_price
            if abs(price_diff) < float_precision:
                return float_ytc
            # modified float_duration
            float_mod_duration = float_duration / float_price
            # update guess
            float_ytc -= price_diff / (-float_mod_duration * float_price)
            # ensure yield stays reasonable - prevent negative yields < -99%
            float_ytc = max(float_ytc, -0.99)
        raise ValueError("Yield to call calculation did not converge after "\
                         + f"{int_max_iterations} iterations")

    def tax_adjusted_yield(self, float_investor_tax_rate: float) -> float:
        """Adjust yield for investor's specific tax rate.

        Parameters
        ----------
        float_investor_tax_rate : float
            The investor's tax rate.

        Returns
        -------
        float
        """
        return self.float_yield * (1 - float_investor_tax_rate)
