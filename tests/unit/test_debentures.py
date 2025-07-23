"""Unit tests for the DebenturesPricer class.

Tests cover normal operations, edge cases, error conditions, and type validation
for Brazilian debentures pricing calculations.
"""

from datetime import date
from typing import Optional

import pytest

from stpstone.analytics.pricing.private_debt.debentures import DebenturesPricer


class TestDebenturesPricer:
    """Test suite for DebenturesPricer class."""

    # --------------------------
    # Fixtures
    # --------------------------
    @pytest.fixture
    def default_debenture(self) -> DebenturesPricer:
        """Fixture providing default debenture with standard parameters."""
        return DebenturesPricer()

    @pytest.fixture
    def tax_free_debenture(self) -> DebenturesPricer:
        """Fixture providing tax-free debenture."""
        return DebenturesPricer(float_tax_rate=0.0)

    @pytest.fixture
    def high_yield_debenture(self) -> DebenturesPricer:
        """Fixture providing high-yield debenture."""
        return DebenturesPricer(float_yield=0.15, float_coupon_r=0.12)

    @pytest.fixture
    def short_term_debenture(self) -> DebenturesPricer:
        """Fixture providing short-term debenture."""
        return DebenturesPricer(int_maturity_years=1, int_coupon_freq=1)

    # --------------------------
    # Test Initialization
    # --------------------------
    def test_init_default_values(self, default_debenture: DebenturesPricer) -> None:
        """Test initialization with default values.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            An instance of DebenturesPricer with default values.
        """
        assert default_debenture.float_fv == 1000.0
        assert default_debenture.float_coupon_r == 0.1
        assert default_debenture.int_coupon_freq == 2
        assert default_debenture.int_maturity_years == 5
        assert default_debenture.float_yield == 0.12
        assert default_debenture.float_tax_rate == 0.15

    @pytest.mark.parametrize(
        "fv,coupon_r,freq,maturity,yield_,tax",
        [
            (500.0, 0.08, 4, 10, 0.10, 0.20),  # Different parameters
            (10000.0, 0.05, 1, 3, 0.08, 0.0),  # Large face value, no tax
            (100.0, 0.15, 12, 1, 0.20, 0.30),  # Monthly coupons
        ],
    )
    def test_init_custom_values(
        self, fv: float, coupon_r: float, freq: int, maturity: int, yield_: float, tax: float
    ) -> None:
        """Test initialization with custom values.
        
        Parameters
        ----------
        fv : float
            The face value of the debenture.
        coupon_r : float
            The coupon rate of the debenture.
        freq : int
            The coupon frequency of the debenture.
        maturity : int
            The maturity years of the debenture.
        yield_ : float
            The yield of the debenture.
        tax : float
            The tax rate on the debenture's coupons.
        """
        deb = DebenturesPricer(fv, coupon_r, freq, maturity, yield_, tax)
        assert deb.float_fv == fv
        assert deb.float_coupon_r == coupon_r
        assert deb.int_coupon_freq == freq
        assert deb.int_maturity_years == maturity
        assert deb.float_yield == yield_
        assert deb.float_tax_rate == tax

    @pytest.mark.parametrize(
        "invalid_fv",
        [0.0, -100.0],
    )
    def test_init_invalid_face_value(self, invalid_fv: float) -> None:
        """Test initialization with invalid face values.
        
        Parameters
        ----------
        invalid_fv : float
            An invalid face value.
        """
        with pytest.raises(ValueError, match="Face value must be positive"):
            DebenturesPricer(float_fv=invalid_fv)

    @pytest.mark.parametrize(
        "invalid_coupon",
        [-0.1, 1.1],
    )
    def test_init_invalid_coupon_rate(self, invalid_coupon: float) -> None:
        """Test initialization with invalid coupon rates.
        
        Parameters
        ----------
        invalid_coupon : float
            An invalid coupon rate.
        """
        with pytest.raises(ValueError, match="Coupon rate must be between 0 and 1"):
            DebenturesPricer(float_coupon_r=invalid_coupon)

    @pytest.mark.parametrize(
        "invalid_freq",
        [0, -1],
    )
    def test_init_invalid_coupon_freq(self, invalid_freq: int) -> None:
        """Test initialization with invalid coupon frequencies.
        
        Parameters
        ----------
        invalid_freq : int
            An invalid coupon frequency.
        """
        with pytest.raises(ValueError, match="Coupon frequency must be positive"):
            DebenturesPricer(int_coupon_freq=invalid_freq)

    # --------------------------
    # Test calculate_price()
    # --------------------------
    @pytest.mark.parametrize(
        "fv,coupon_r,freq,maturity,yield_,tax,expected",
        [
            (1000.0, 0.1, 2, 5, 0.12, 0.15, pytest.approx(871.1984766, abs=1e-2)),  # Default case
            (1000.0, 0.1, 2, 5, 0.12, 0.0, pytest.approx(926.40, abs=1e-2)),  # No tax
            (1000.0, 0.05, 2, 5, 0.08, 0.15, pytest.approx(847.92, abs=1e-2)),  # Lower coupon
            (5000.0, 0.1, 4, 10, 0.10, 0.20, pytest.approx(4372.43, abs=1e-2)),  # Different params
        ],
    )
    def test_calculate_price_normal_cases(
        self,
        fv: float,
        coupon_r: float,
        freq: int,
        maturity: int,
        yield_: float,
        tax: float, expected: float
    ) -> None:
        """Test price calculation with normal parameters.
        
        Parameters
        ----------
        fv : float
            The face value of the debenture.
        coupon_r : float
            The coupon rate of the debenture.
        freq : int
            The coupon frequency of the debenture.
        maturity : int
            The maturity years of the debenture.
        yield_ : float
            The yield of the debenture.
        tax : float
            The tax rate on the debenture's coupons.
        expected : float
            The expected calculated price.
        """
        deb = DebenturesPricer(fv, coupon_r, freq, maturity, yield_, tax)
        assert deb.calculate_price() == expected

    def test_calculate_price_zero_coupon(
        self,
        default_debenture: DebenturesPricer
    ) -> None:
        """Test price calculation with zero coupon rate.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        """
        default_debenture.float_coupon_r = 0.0
        expected = 558.3947769
        assert pytest.approx(default_debenture.calculate_price(), 2) \
            == pytest.approx(expected, abs=1e-2)

    # --------------------------
    # Test current_yield()
    # --------------------------
    @pytest.mark.parametrize(
        "market_price,expected",
        [
            (None, pytest.approx(0.0975667454466, abs=1e-4)),  # Using calculated price
            (900.0, pytest.approx(0.0944, abs=1e-4)),  # Custom price
            (1000.0, pytest.approx(0.085, abs=1e-4)),  # At par
            (800.0, pytest.approx(0.1062, abs=1e-4)),  # Below par
        ],
    )
    def test_current_yield(
        self,
        default_debenture: DebenturesPricer,
        market_price: Optional[float],
        expected: float
    ) -> None:
        """Test current yield calculation.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        market_price : Optional[float]
            The market price of the debenture.
        expected : float
            The expected current yield.
        """
        assert default_debenture.current_yield(market_price) == expected

    def test_current_yield_tax_free(self, tax_free_debenture: DebenturesPricer) -> None:
        """Test current yield without tax.
        
        Parameters
        ----------
        tax_free_debenture : DebenturesPricer
            A debenture with no tax.
        """
        assert tax_free_debenture.current_yield(900.0) == pytest.approx(0.1111, abs=1e-4)

    @pytest.mark.parametrize(
        "invalid_price",
        [0.0, -100.0],
    )
    def test_current_yield_invalid_price(
        self,
        default_debenture: DebenturesPricer,
        invalid_price: float
    ) -> None:
        """Test current yield with invalid prices.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        invalid_price : float
            An invalid price.
        """
        with pytest.raises(ValueError, match="Price must be positive"):
            default_debenture.current_yield(invalid_price)

    # --------------------------
    # Test capital_gain_yield()
    # --------------------------
    @pytest.mark.parametrize(
        "market_price,expected",
        [
            (None, pytest.approx(0.14784406, abs=1e-4)),  # Using calculated price
            (900.0, pytest.approx(0.1111, abs=1e-4)),  # Custom price
            (1000.0, pytest.approx(0.0, abs=1e-4)),  # At par
            (800.0, pytest.approx(0.25, abs=1e-4)),  # Below par
        ],
    )
    def test_capital_gain_yield(
        self, default_debenture: DebenturesPricer, market_price: Optional[float], expected: float
    ) -> None:
        """Test capital gain yield calculation.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        market_price : Optional[float]
            The market price of the debenture.
        expected : float
            The expected capital gain yield.
        """
        assert default_debenture.capital_gain_yield(market_price) == expected

    # --------------------------
    # Test total_return()
    # --------------------------
    def test_total_return(self, default_debenture: DebenturesPricer) -> None:
        """Test total return calculation.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        """
        current = default_debenture.current_yield()
        capital = default_debenture.capital_gain_yield()
        assert default_debenture.total_return() \
            == pytest.approx(current + capital, abs=1e-4)

    # --------------------------
    # Test accrued_interest()
    # --------------------------
    @pytest.mark.parametrize(
        "settlement,last_coupon,next_coupon,convention,expected",
        [
            # Actual/Actual convention
            (
                date(2023, 6, 10),
                date(2023, 3, 15),
                date(2023, 9, 15),
                "actual/actual",
                pytest.approx(20.09510869, abs=1e-2),
            ),
            # 30/360 convention
            (
                date(2023, 6, 10),
                date(2023, 3, 15),
                date(2023, 9, 15),
                "30/360",
                pytest.approx(20.0694444, abs=1e-2),  # Same in this case
            ),
            # Different dates
            (
                date(2023, 7, 1),
                date(2023, 1, 1),
                date(2023, 7, 1),
                "actual/actual",
                pytest.approx(42.5, abs=1e-2),
            ),
        ],
    )
    def test_accrued_interest_normal_cases(
        self,
        default_debenture: DebenturesPricer,
        settlement: date,
        last_coupon: date,
        next_coupon: date,
        convention: str,
        expected: float,
    ) -> None:
        """Test accrued interest calculation with normal cases.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        settlement : date
            The settlement date for the calculation.
        last_coupon : date
            The date of the last coupon payment.
        next_coupon : date
            The date of the next coupon payment.
        convention : str
            The day count convention for the calculation.
        expected : float
            The expected accrued interest.
        """
        result = default_debenture.accrued_interest(
            settlement, last_coupon, next_coupon, convention)
        assert result == expected

    def test_accrued_interest_tax_free(self, tax_free_debenture: DebenturesPricer) -> None:
        """Test accrued interest without tax.
        
        Parameters
        ----------
        tax_free_debenture : DebenturesPricer
            A debenture with tax-free parameters.
        """
        settlement = date(2023, 6, 10)
        last_coupon = date(2023, 3, 15)
        next_coupon = date(2023, 9, 15)
        result = tax_free_debenture.accrued_interest(settlement, last_coupon, next_coupon)
        assert result == pytest.approx(23.6413043, abs=1e-2)

    def test_accrued_interest_auto_dates(self, default_debenture: DebenturesPricer) -> None:
        """Test accrued interest with automatic date calculation.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        """
        # This test depends on the current date, so we need to mock or use fixed dates
        settlement = date(2023, 6, 10)
        result = default_debenture.accrued_interest(settlement)
        # Exact value depends on _calculate_coupon_dates implementation
        assert result > 0

    @pytest.mark.parametrize(
        "invalid_settlement,last_coupon,next_coupon",
        [
            (date(2023, 6, 10), date(2023, 9, 15), date(2023, 3, 15)),  # Last > Next
            (date(2023, 6, 10), date(2023, 9, 15), date(2023, 6, 1)),  # Settlement < Last
            (date(2023, 6, 20), date(2023, 6, 1), date(2023, 6, 10)),  # Settlement > Next
        ],
    )
    def test_accrued_interest_invalid_dates(
        self,
        default_debenture: DebenturesPricer,
        invalid_settlement: date,
        last_coupon: date,
        next_coupon: date,
    ) -> None:
        """Test accrued interest with invalid date ranges.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        invalid_settlement : date
            An invalid settlement date.
        last_coupon : date
            The date of the last coupon payment.
        next_coupon : date
            The date of the next coupon payment.
        """
        if last_coupon > next_coupon:
            expected_msg = "Last coupon date must be before next coupon date"
        else:
            expected_msg = "Settlement date must be between coupon dates"

        with pytest.raises(ValueError, match=expected_msg):
            default_debenture.accrued_interest(invalid_settlement, last_coupon, next_coupon)

    def test_accrued_interest_invalid_convention(
        self,
        default_debenture: DebenturesPricer
    ) -> None:
        """Test accrued interest with invalid day count convention.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        """
        with pytest.raises(ValueError, match="Unsupported day count convention"):
            default_debenture.accrued_interest(
                date(2023, 6, 10),
                date(2023, 3, 15),
                date(2023, 9, 15),
                "invalid_convention",
            )

    # --------------------------
    # Test dirty_price()
    # --------------------------
    def test_dirty_price(self, default_debenture: DebenturesPricer) -> None:
        """Test dirty price calculation.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        """
        clean_price = default_debenture.calculate_price()
        settlement = date(2023, 6, 10)
        last_coupon = date(2023, 3, 15)
        next_coupon = date(2023, 9, 15)
        accrued = default_debenture.accrued_interest(settlement, last_coupon, next_coupon)
        dirty_price = default_debenture.dirty_price(
            clean_price, settlement, last_coupon, next_coupon
        )
        assert dirty_price == pytest.approx(clean_price + accrued, abs=1e-2)

    # --------------------------
    # Test holding_period_return()
    # --------------------------
    @pytest.mark.parametrize(
        "purchase,sale,period,expected",
        [
            (900.0, 1000.0, 1.0, pytest.approx(0.1111, abs=1e-4)),  # 1 year
            (900.0, 1000.0, 0.5, pytest.approx(0.23456790, abs=1e-4)),  # 6 months
            (1000.0, 900.0, 2.0, pytest.approx(-0.0513, abs=1e-4)),  # Loss over 2 years
        ],
    )
    def test_holding_period_return(
        self,
        default_debenture: DebenturesPricer,
        purchase: float,
        sale: float,
        period: float,
        expected: float,
    ) -> None:
        """Test holding period return calculation.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        purchase : float
            The purchase price of the bond.
        sale : float
            The sale price of the bond.
        period : float
            The holding period in years.
        expected : float
            The expected holding period return.
        """
        result = default_debenture.holding_period_return(purchase, sale, period)
        assert result == expected

    @pytest.mark.parametrize(
        "invalid_purchase",
        [0.0, -100.0],
    )
    def test_holding_period_return_invalid_purchase(
        self, default_debenture: DebenturesPricer, invalid_purchase: float
    ) -> None:
        """Test holding period return with invalid purchase price.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        invalid_purchase : float
            An invalid purchase price.
        """
        with pytest.raises(ValueError, match="Purchase price must be positive"):
            default_debenture.holding_period_return(invalid_purchase, 1000.0, 1.0)

    @pytest.mark.parametrize(
        "invalid_period",
        [0.0, -1.0],
    )
    def test_holding_period_return_invalid_period(
        self, default_debenture: DebenturesPricer, invalid_period: float
    ) -> None:
        """Test holding period return with invalid holding period.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        invalid_period : float
            An invalid holding period.
        """
        with pytest.raises(ValueError, match="Holding period must be positive"):
            default_debenture.holding_period_return(900.0, 1000.0, invalid_period)

    # --------------------------
    # Test credit_spread()
    # --------------------------
    def test_credit_spread(self, default_debenture: DebenturesPricer) -> None:
        """Test credit spread calculation.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        """
        risk_free = 0.05
        assert default_debenture.credit_spread(risk_free) == pytest.approx(
            default_debenture.float_yield - risk_free, abs=1e-4
        )

    # --------------------------
    # Test early_redemption_value()
    # --------------------------
    @pytest.mark.parametrize(
        "call_price,call_date,expected",
        [
            (1000.0, 2.5, pytest.approx(871.1984766, abs=1e-2)),  # Call at par
            (1050.0, 2.5, pytest.approx(871.1984766, abs=1e-2)),  # Call premium
            (950.0, 2.5, pytest.approx(871.19847, abs=1e-2)),  # Call discount (use maturity value)
        ],
    )
    def test_early_redemption_value(
        self,
        default_debenture: DebenturesPricer,
        call_price: float,
        call_date: float,
        expected: float
    ) -> None:
        """Test early redemption value calculation.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        call_price : float
            The call price.
        call_date : float
            The call date in years.
        expected : float
            The expected early redemption value.
        """
        result = default_debenture.early_redemption_value(call_price, call_date)
        assert result == expected

    @pytest.mark.parametrize(
        "invalid_call_date",
        [0.0, -1.0, 6.0],
    )
    def test_early_redemption_invalid_dates(
        self, default_debenture: DebenturesPricer, invalid_call_date: float
    ) -> None:
        """Test early redemption with invalid call dates.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        invalid_call_date : float
            An invalid call date.
        """
        with pytest.raises(ValueError):
            default_debenture.early_redemption_value(1000.0, invalid_call_date)

    # --------------------------
    # Test yield_to_call()
    # --------------------------
    @pytest.mark.parametrize(
        "call_price,call_date,current_price,expected",
        [
            (1000.0, 2.5, 900.0, pytest.approx(0.1333442989, abs=1e-4)),  # Below par call
            (1050.0, 2.5, 950.0, pytest.approx(0.12657595146, abs=1e-4)),  # Premium call
        ],
    )
    def test_yield_to_call(
        self,
        default_debenture: DebenturesPricer,
        call_price: float,
        call_date: float,
        current_price: float,
        expected: float,
    ) -> None:
        """Test yield to call calculation.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        call_price : float
            The call price.
        call_date : float
            The call date in years.
        current_price : float
            The current float_price.
        expected : float
            The expected yield to call.
        """
        result = default_debenture.yield_to_call(call_price, call_date, current_price)
        assert result == expected

    @pytest.mark.parametrize(
        "invalid_price",
        [0.0, -100.0],
    )
    def test_yield_to_call_invalid_price(
        self, default_debenture: DebenturesPricer, invalid_price: float
    ) -> None:
        """Test yield to call with invalid prices.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        invalid_price : float
            An invalid price.
        """
        with pytest.raises(ValueError, match="Current price must be positive"):
            default_debenture.yield_to_call(1000.0, 2.5, invalid_price)

    # --------------------------
    # Test inflation_adjusted_return()
    # --------------------------
    @pytest.mark.parametrize(
        "inflation,expected",
        [
            (0.05, pytest.approx(0.0667, abs=1e-4)),  # Moderate inflation
            (0.10, pytest.approx(0.0182, abs=1e-4)),  # High inflation
            (0.0, pytest.approx(0.12, abs=1e-4)),  # No inflation
        ],
    )
    def test_inflation_adjusted_return(
        self, default_debenture: DebenturesPricer, inflation: float, expected: float
    ) -> None:
        """Test inflation-adjusted return calculation.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        inflation : float
            The inflation rate.
        expected : float
            The expected inflation-adjusted return.
        """
        assert default_debenture.inflation_adjusted_return(inflation) == expected

    # --------------------------
    # Test tax_adjusted_yield()
    # --------------------------
    @pytest.mark.parametrize(
        "tax_rate,expected",
        [
            (0.20, pytest.approx(0.096, abs=1e-4)),  # Higher tax
            (0.0, pytest.approx(0.12, abs=1e-4)),  # No tax
            (0.15, pytest.approx(0.102, abs=1e-4)),  # Same as default
        ],
    )
    def test_tax_adjusted_yield(
        self, default_debenture: DebenturesPricer, tax_rate: float, expected: float
    ) -> None:
        """Test tax-adjusted yield calculation.
        
        Parameters
        ----------
        default_debenture : DebenturesPricer
            A debenture with default parameters.
        tax_rate : float
            The tax rate.
        expected : float
            The expected tax-adjusted yield.
        """
        assert default_debenture.tax_adjusted_yield(tax_rate) == expected
