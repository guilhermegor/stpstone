from datetime import date
from typing import Optional
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class DebenturesBR(metaclass=TypeChecker):
    """
    A comprehensive class for pricing and analyzing Brazilian debentures,
    excluding duration-related calculations which are already implemented.
    """
    
    def __init__(
        self,
        float_fv: float = 1000,
        float_coupon_r: float = 0.1,
        int_coupon_freq: int = 2,
        int_maturity_years: int = 5,
        float_yield: float = 0.12,
        float_tax_rate: float = 0.15
    ) -> None:
        """
        Initialize the debenture with default or provided parameters.
        
        Args:
            float_fv: Nominal value (valor de face)
            float_coupon_r: Annual coupon rate (decimal)
            int_coupon_freq: Payments per year
            int_maturity_years: Years to maturity
            float_yield: Market yield (decimal)
            float_tax_rate: Tax on interest (decimal)
        """
        self.float_fv = float_fv
        self.float_coupon_r = float_coupon_r
        self.int_coupon_freq = int_coupon_freq
        self.int_maturity_years = int_maturity_years
        self.float_yield = float_yield
        self.float_tax_rate = float_tax_rate
    
    @property
    def calculate_price(self) -> float:
        """
        Calculate the theoretical float_price of the debenture considering tax on coupons.
        
        Returns:
            Present value/float_price of the debenture
        """
        float_price: float = 0.0
        int_periods: int = int(self.int_maturity_years * self.int_coupon_freq)
        float_period_rate: float = self.float_yield / self.int_coupon_freq
        float_float_coupon_payment: float = (self.float_fv * self.float_coupon_r / 
                               self.int_coupon_freq) * (1 - self.float_tax_rate)
        for t in range(1, int_periods + 1):
            float_cf: float = float_float_coupon_payment + self.float_fv if t == int_periods \
                else float_float_coupon_payment
            float_price += float_cf / ((1 + float_period_rate) ** t)
        return float_price
    
    def current_yield(self, float_market_price: Optional[float] = None) -> float:
        """
        Calculate current yield (annual coupon payment / current market float_price).
        
        Args:
            float_market_price: Current market float_price (if None, uses calculated float_price)
            
        Returns:
            Current yield (decimal)
        """
        float_price: float = float_market_price if float_market_price is not None \
            else self.calculate_price()
        float_annual_coupon: float = self.float_fv * self.float_coupon_r * (1 - self.float_tax_rate)
        return float_annual_coupon / float_price
    
    def capital_gain_yield(self, float_market_price: Optional[float] = None) -> float:
        """
        Calculate expected capital gain/loss yield to maturity.
        
        Args:
            float_market_price: Current market float_price (if None, uses calculated float_price)
            
        Returns:
            Capital gain yield (decimal)
        """
        float_price: float = float_market_price if float_market_price is not None \
            else self.calculate_price()
        return (self.float_fv - float_price) / float_price
    
    def float_total_return(self, float_market_price: Optional[float] = None) -> float:
        """
        Calculate total expected return to maturity (current yield + capital gain).
        
        Args:
            float_market_price: Current market float_price (if None, uses calculated float_price)
            
        Returns:
            Total expected return (decimal)
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
        """
        Calculate float_accrued interest since last coupon payment.
        
        Args:
            dt_settlement: Date for which to calculate float_accrued interest (default: today)
            dt_last_coupon: Date of last coupon payment (if not provided, calculated)
            dt_next_coupon: Date of next coupon payment (if not provided, calculated)
            str_day_count_convention: Day count method ("actual/actual", "30/360", etc.)
            
        Returns:
            float: Accrued interest amount
            
        Raises:
            ValueError: If dates are invalid or day count convention not supported
        """
        if dt_settlement is None:
            dt_settlement = date.today()
        if dt_last_coupon is None or dt_next_coupon is None:
            last_cp, next_cp = self._calculate_coupon_dates(dt_settlement)
            dt_last_coupon = dt_last_coupon or last_cp
            dt_next_coupon = dt_next_coupon or next_cp
        if not (dt_last_coupon <= dt_settlement <= dt_next_coupon):
            raise ValueError("Settlement date must be between coupon dates")
        # calculate days according to convention
        if str_day_count_convention.lower() == "actual/actual":
            int_days_accrued = (dt_settlement - dt_last_coupon).days
            int_days_in_period = (dt_next_coupon - dt_last_coupon).days
        elif str_day_count_convention.lower() == "30/360":
            int_days_accrued = self._days_360(dt_last_coupon, dt_settlement)
            int_days_in_period = self._days_360(dt_last_coupon, dt_next_coupon)
        else:
            raise ValueError(f"Unsupported day count convention: {str_day_count_convention}")
        float_coupon_payment = (self.float_fv * self.float_coupon_r) / self.int_coupon_freq
        float_accrued = float_coupon_payment * (int_days_accrued / int_days_in_period)
        # apply tax if this is a taxable coupon
        if self.float_tax_rate > 0:
            float_accrued *= (1 - self.float_tax_rate)
        return float_accrued
    
    def _calculate_coupon_dates(self, dt_settlement: date) -> tuple[date, date]:
        """
        Calculate previous and next coupon dates based on settlement date.
        
        Args:
            dt_settlement: Reference date for calculation
            
        Returns:
            tuple: (dt_last_coupon, dt_next_coupon)
        """
        # this is a simplified implementation - would need actual issue date in real usage
        int_months_between = 12 // self.int_coupon_freq
        # find most recent coupon date before settlement
        dt_last_coupon = date(dt_settlement.year, 1, 1)
        while dt_last_coupon <= dt_settlement:
            dt_last_coupon = dt_last_coupon.replace(month=dt_last_coupon.month + int_months_between)
        dt_last_coupon = dt_last_coupon.replace(month=dt_last_coupon.month - int_months_between)
        # next coupon is int_months_between after last coupon
        dt_next_coupon = dt_last_coupon.replace(month=dt_last_coupon.month + int_months_between)
        return dt_last_coupon, dt_next_coupon
    
    def _days_360(self, dt_start: date, dt_end: date) -> int:
        """
        Calculate days between dates using 30/360 convention.
        
        Args:
            dt_start: Start date
            dt_end: End date
            
        Returns:
            int: Days between dates under 30/360 convention
        """
        dt_d1 = min(dt_start.day, 30)
        dt_d2 = min(dt_end.day, 30) if dt_d1 == 30 else dt_end.day
        return (dt_end.year - dt_start.year) * 360 + \
               (dt_end.month - dt_start.month) * 30 + \
               (dt_d2 - dt_d1)

    def dirty_price(self, float_clean_price: Optional[float] = None) -> float:
        """
        Calculate dirty float_price (clean float_price + float_accrued interest).
        
        Args:
            float_clean_price: Clean float_price (if None, uses calculated float_price)
            
        Returns:
            Dirty float_price
        """
        clean_price: float = float_clean_price if float_clean_price is not None \
            else self.calculate_price()
        return clean_price + self.accrued_interest()
    
    def holding_period_return(
        self,
        float_purchase_price: float,
        float_sale_price: float,
        int_holding_period: float
    ) -> float:
        """
        Calculate annualized holding period return.
        
        Args:
            float_purchase_price: Price when bought
            float_sale_price: Price when sold
            int_holding_period: Holding period in years
            
        Returns:
            Annualized return (decimal)
        """
        float_total_return: float = (float_sale_price - float_purchase_price) / float_purchase_price
        return (1 + float_total_return) ** (1 / int_holding_period) - 1
    
    def credit_spread(self, risk_free_rate: float) -> float:
        """
        Calculate credit spread over risk-free rate.
        
        Args:
            risk_free_rate: Risk-free rate (decimal)
            
        Returns:
            Credit spread (decimal)
        """
        return self.float_yield - risk_free_rate
    
    def early_redemption_value(self, call_price: float, call_date: float) -> float:
        """
        Calculate the minimum between the debenture's present value to maturity and
        its present value if called at the call date, considering all cash flows.
        
        Args:
            call_price: Price at which the debenture can be called (% of face value)
            call_date: Time until call date in years (must be <= maturity)
            
        Returns:
            float: Minimum value between holding to maturity and being called
            
        Raises:
            ValueError: If call_date is after maturity or negative
        """
        float_call_value = 0.0
        if call_date > self.int_maturity_years:
            raise ValueError("Call date cannot be after maturity")
        if call_date <= 0:
            raise ValueError("Call date must be positive")
        # value if held to maturity
        float_maturity_value = self.calculate_price()
        # present value if called
        int_call_periods = int(call_date * self.int_coupon_freq)
        float_period_rate = self.float_yield / self.int_coupon_freq
        float_coupon_payment = (self.float_fv * self.float_coupon_r / 
                        self.int_coupon_freq) * (1 - self.float_tax_rate)
        for t in range(1, int_call_periods + 1):
            if t == int_call_periods:
                # at call date, investor receives call price plus final coupon
                float_cf = float_coupon_payment + call_price
            else:
                float_cf = float_coupon_payment
            
            float_call_value += float_cf / ((1 + float_period_rate) ** t)
        
        return min(float_maturity_value, float_call_value)
    
    def inflation_adjusted_return(self, inflation_rate: float) -> float:
        """
        Calculate real return adjusted for inflation.
        
        Args:
            inflation_rate: Expected inflation rate (decimal)
            
        Returns:
            Real return (decimal)
        """
        nominal_return: float = self.float_yield
        return (1 + nominal_return) / (1 + inflation_rate) - 1
    
    def yield_to_call(
        self,
        call_price: float,
        call_date: float,
        current_price: float,
        max_iterations: int = 100,
        precision: float = 1e-6
    ) -> float:
        """
        Calculate yield to call using Newton-Raphson method.
        
        Args:
            call_price: Price at which debenture can be called (% of face value)
            call_date: Time until call date in years
            current_price: Current market price of the debenture
            max_iterations: Maximum iterations for convergence
            precision: Desired precision level
            
        Returns:
            float: Yield to call (annualized)
            
        Raises:
            ValueError: If call_date is invalid or calculation doesn't converge
        """
        if call_date <= 0:
            raise ValueError("Call date must be positive")
        if call_date > self.int_maturity_y:
            raise ValueError("Call date cannot be after maturity")
        if current_price <= 0:
            raise ValueError("Current price must be positive")
        float_ytc = self.float_coupon_r
        int_periods = int(call_date * self.int_coupon_freq)
        float_coupon_payment = (self.float_fv * self.float_coupon_r / 
                         self.int_coupon_freq) * (1 - self.float_tax_rate)
        for _ in range(max_iterations):
            # calculate price and derivative at current guess
            price = 0.0
            duration = 0.0
            for t in range(1, int_periods + 1):
                if t == int_periods:
                    cash_flow = float_coupon_payment + call_price
                else:
                    cash_flow = float_coupon_payment
                discount = (1 + float_ytc/self.int_coupon_freq) ** t
                price += cash_flow / discount
                duration += t * cash_flow / (discount * (1 + float_ytc/self.int_coupon_freq))
            # newton-Raphson update
            price_diff = price - current_price
            if abs(price_diff) < precision:
                return float_ytc
            # modified duration
            float_mod_duration = duration / price
            # update guess
            float_ytc -= price_diff / (-float_mod_duration * price)
            # ensure yield stays reasonable - prevent negative yields < -99%
            float_ytc = max(float_ytc, -0.99)
        
        raise ValueError(f"Yield to call calculation did not converge after {max_iterations} iterations")

    def _calculate_ytc_price(
        self,
        ytc_rate: float,
        call_price: float,
        call_periods: int
    ) -> float:
        """
        Helper method to calculate price given a yield-to-call rate.
        
        Args:
            ytc_rate: Yield to call rate to test
            call_price: Call price
            call_periods: Number of int_periods until call
            
        Returns:
            float: Calculated price
        """
        price = 0.0
        float_coupon_payment = (self.float_fv * self.float_coupon_r / 
                         self.int_coupon_freq) * (1 - self.float_tax_rate)
        for t in range(1, call_periods + 1):
            if t == call_periods:
                cash_flow = float_coupon_payment + call_price
            else:
                cash_flow = float_coupon_payment
                
            price += cash_flow / ((1 + ytc_rate/self.int_coupon_freq) ** t)
        return price
    
    def tax_adjusted_yield(self, float_investor_tax_rate: float) -> float:
        """
        Adjust yield for investor's specific tax rate.
        
        Args:
            float_investor_tax_rate: Investor's marginal tax rate (decimal)
            
        Returns:
            After-tax yield (decimal)
        """
        return self.float_yield * (1 - float_investor_tax_rate)