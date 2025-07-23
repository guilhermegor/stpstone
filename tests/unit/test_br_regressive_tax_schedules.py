"""Unit tests for BRRegressiveTaxSchedules class.

Tests cover tax calculations for Brazilian sovereign bonds including:
- IOF tax calculations
- IR tax calculations
- Combined tax calculations
- Rate lookups
- Error conditions
- Type validation
"""

import pytest

from stpstone.analytics.pricing.taxes.br_regressive_tax_schedules import BRRegressiveTaxSchedules


class TestBRRegressiveTaxSchedules:
    """Test suite for BRRegressiveTaxSchedules class."""

    # --------------------------
    # Fixtures
    # --------------------------
    @pytest.fixture
    def tax_calculator(self) -> BRRegressiveTaxSchedules:
        """Fixture providing a fresh tax calculator instance."""
        return BRRegressiveTaxSchedules()

    @pytest.fixture
    def sample_notional(self) -> float:
        """Fixture providing a sample notional amount for testing."""
        return 10000.0

    # --------------------------
    # Test Initialization
    # --------------------------
    def test_initialization(self, tax_calculator: BRRegressiveTaxSchedules) -> None:
        """Test that tax tables are properly initialized.
        
        Parameters
        ----------
        tax_calculator : BRRegressiveTaxSchedules
            The tax calculator instance to test.
        """
        assert hasattr(tax_calculator, "iof_table")
        assert hasattr(tax_calculator, "ir_table")
        assert len(tax_calculator.iof_table) == 30
        assert len(tax_calculator.ir_table) == 4

    # --------------------------
    # Test IOF Tax Calculations
    # --------------------------
    @pytest.mark.parametrize(
        "days,expected_rate,expected_tax",
        [
            (1, 0.96, 96.0),  # first day maximum rate
            (10, 0.66, 66.0),  # mid-range rate
            (30, 0.00, 0.0),  # Last day zero rate
            (31, 0.00, 0.0),  # Beyond table (should cap at 30)
            (15, 0.50, 50.0),  # Exact middle
        ],
    )
    def test_calculate_iof_tax(
        self,
        tax_calculator: BRRegressiveTaxSchedules,
        sample_notional: float,
        days: int,
        expected_rate: float,
        expected_tax: float,
    ) -> None:
        """Test IOF tax calculation with various days since investment.
        
        Parameters
        ----------
        tax_calculator : BRRegressiveTaxSchedules
            The tax calculator instance to test.
        sample_notional : float
            The sample notional amount for testing.
        days : int
            The number of days since the investment.
        expected_rate : float
            The expected IOF float_rate in percentage.
        expected_tax : float
            The expected IOF tax amount.
        """
        tax = tax_calculator.calculate_iof_tax(days, sample_notional)
        assert tax == pytest.approx(expected_tax, abs=0.01)
        assert tax_calculator.get_iof_rate(days) == expected_rate

    def test_calculate_iof_tax_zero_notional(
        self, tax_calculator: BRRegressiveTaxSchedules
    ) -> None:
        """Test IOF tax calculation with zero notional amount.
        
        Parameters
        ----------
        tax_calculator : BRRegressiveTaxSchedules
            The tax calculator instance to test.
        """
        tax = tax_calculator.calculate_iof_tax(5, 0.0)
        assert tax == 0.0

    def test_calculate_iof_tax_negative_days(
        self, tax_calculator: BRRegressiveTaxSchedules
    ) -> None:
        """Test IOF tax calculation with negative days raises ValueError.
        
        Parameters
        ----------
        tax_calculator : BRRegressiveTaxSchedules
            The tax calculator instance to test.
        """
        with pytest.raises(ValueError, match="cannot be negative"):
            tax_calculator.calculate_iof_tax(-1, 10000.0)

    # --------------------------
    # Test IR Tax Calculations
    # --------------------------
    @pytest.mark.parametrize(
        "days,expected_rate,expected_tax",
        [
            (1, 22.5, 2250.0),  # minimum bracket
            (180, 20.0, 2000.0),  # first threshold
            (181, 20.0, 2000.0),  # just above first threshold
            (360, 20.0, 2000.0),  # just below next threshold
            (361, 17.5, 1750.0),  # second threshold
            (720, 17.5, 1750.0),  # just below next threshold
            (721, 15.0, 1500.0),  # final threshold
            (1000, 15.0, 1500.0),  # well beyond final threshold
        ],
    )
    def test_calculate_ir_tax(
        self,
        tax_calculator: BRRegressiveTaxSchedules,
        sample_notional: float,
        days: int,
        expected_rate: float,
        expected_tax: float,
    ) -> None:
        """Test IR tax calculation with various investment durations.
        
        Parameters
        ----------
        tax_calculator : BRRegressiveTaxSchedules
            The tax calculator instance to test.
        sample_notional : float
            The sample notional amount for testing.
        days : int
            The number of days since the investment.
        expected_rate : float
            The expected IR float_rate in percentage.
        expected_tax : float
            The expected IR tax amount.
        """
        tax = tax_calculator.calculate_ir_tax(days, sample_notional)
        assert tax == pytest.approx(expected_tax, abs=0.01)
        assert tax_calculator.get_ir_rate(days) == expected_rate

    def test_calculate_ir_tax_zero_notional(
        self, tax_calculator: BRRegressiveTaxSchedules
    ) -> None:
        """Test IR tax calculation with zero notional amount.
        
        Parameters
        ----------
        tax_calculator : BRRegressiveTaxSchedules
            The tax calculator instance to test.
        """
        tax = tax_calculator.calculate_ir_tax(100, 0.0)
        assert tax == 0.0

    def test_calculate_ir_tax_negative_days(
        self, tax_calculator: BRRegressiveTaxSchedules
    ) -> None:
        """Test IR tax calculation with negative days raises ValueError.
        
        Parameters
        ----------
        tax_calculator : BRRegressiveTaxSchedules
            The tax calculator instance to test.
        """
        with pytest.raises(ValueError, match="Days invested must be positive"):
            tax_calculator.calculate_ir_tax(-1, 10000.0)

    # --------------------------
    # Test Combined Tax Calculations
    # --------------------------
    @pytest.mark.parametrize(
        "cddt,cddr,expected_iof,expected_ir,expected_total",
        [
            (1, 1, 96.0, 2250.0, 2346.0),  # max both taxes
            (30, 30, 0.0, 2250.0, 2250.0),  # no IOF, max IR
            (181, 1, 96.0, 2000.0, 2096.0),  # mid IR bracket
            (721, 30, 0.0, 1500.0, 1500.0),  # min IR, no IOF
            (500, 15, 50.0, 1750.0, 1800.0),  # mixed case
        ],
    )
    def test_calculate_total_taxes(
        self,
        tax_calculator: BRRegressiveTaxSchedules,
        sample_notional: float,
        cddt: int,
        cddr: int,
        expected_iof: float,
        expected_ir: float,
        expected_total: float,
    ) -> None:
        """Test combined tax calculation with various scenarios.
        
        Parameters
        ----------
        tax_calculator : BRRegressiveTaxSchedules
            The tax calculator instance to test.
        sample_notional : float
            The sample notional amount for testing.
        cddt : int
            The total duration of investment in calendar days.
        cddr : int
            Days since investment when redeemed (for IOF calculation).
        expected_iof : float
            The expected IOF tax amount.
        expected_ir : float
            The expected IR tax amount.
        expected_total : float
            The expected total tax amount.
        """
        iof, ir, total = tax_calculator.calculate_total_taxes(
            cddt, cddr, sample_notional
        )
        assert iof == pytest.approx(expected_iof, abs=0.01)
        assert ir == pytest.approx(expected_ir, abs=0.01)
        assert total == pytest.approx(expected_total, abs=0.01)

    # --------------------------
    # Test Type Validation
    # --------------------------
    @pytest.mark.parametrize(
        "invalid_days,invalid_notional",
        [
            ("1", 10000.0),  # string days
            (1.5, 10000.0),  # float days
            (1, "10000"),  # string notional
            (None, 10000.0),  # none days
            (1, None),  # none notional
        ],
    )
    def test_invalid_types(
        self,
        tax_calculator: BRRegressiveTaxSchedules,
        invalid_days: any,
        invalid_notional: any,
    ) -> None:
        """Test that type checker rejects invalid input types.
        
        Parameters
        ----------
        tax_calculator : BRRegressiveTaxSchedules
            The tax calculator instance to test.
        invalid_days : any
            Invalid days input.
        invalid_notional : any
            Invalid notional input.
        """
        with pytest.raises(TypeError):
            tax_calculator.calculate_iof_tax(invalid_days, invalid_notional)
        with pytest.raises(TypeError):
            tax_calculator.calculate_ir_tax(invalid_days, invalid_notional)
        with pytest.raises(TypeError):
            tax_calculator.calculate_total_taxes(invalid_days, invalid_days, invalid_notional)

    # --------------------------
    # Test Edge Cases
    # --------------------------
    def test_very_large_notional(
        self, tax_calculator: BRRegressiveTaxSchedules
    ) -> None:
        """Test with extremely large notional amount.
        
        Parameters
        ----------
        tax_calculator : BRRegressiveTaxSchedules
            The tax calculator instance to test.
        """
        large_notional = 1e12  # 1 trillion
        iof, ir, total = tax_calculator.calculate_total_taxes(100, 10, large_notional)
        assert total == pytest.approx(iof + ir, abs=0.01)
        assert total > 0

    def test_very_large_days(self, tax_calculator: BRRegressiveTaxSchedules) -> None:
        """Test with extremely large days invested.
        
        Parameters
        ----------
        tax_calculator : BRRegressiveTaxSchedules
            The tax calculator instance to test.
        """
        rate = tax_calculator.get_ir_rate(1_000_000)
        assert rate == 15.0  # should cap at minimum rate

    def test_zero_days(self, tax_calculator: BRRegressiveTaxSchedules) -> None:
        """Test with zero days (edge of validity).
        
        Parameters
        ----------
        tax_calculator : BRRegressiveTaxSchedules
            The tax calculator instance to test.
        """
        # zero days invested is technically invalid (raises ValueError)
        with pytest.raises(ValueError, match="Days invested must be positive"):
            tax_calculator.calculate_ir_tax(0, 10000.0)
        
        # zero days since investment is valid (no IOF tax)
        tax = tax_calculator.calculate_iof_tax(0, 10000.0)
        assert tax == 0.0

    # --------------------------
    # Test Docstring Examples
    # --------------------------
    def test_docstring_example(self, tax_calculator: BRRegressiveTaxSchedules) -> None:
        """Test the example provided in the class docstring.
        
        Parameters
        ----------
        tax_calculator : BRRegressiveTaxSchedules
            The tax calculator instance to test.
        """
        iof, ir, total = tax_calculator.calculate_total_taxes(
            int_cddt=200,
            int_cddr=10,
            float_notional=10000.00
        )
        assert iof == pytest.approx(66.0, abs=0.1)  # 0.66% of 10000
        assert ir == pytest.approx(2000.0, abs=0.1)  # 20% of 10000
        assert total == pytest.approx(2066.0, abs=0.1)