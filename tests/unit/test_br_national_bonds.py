"""Unit tests for Brazilian sovereign bond pricing calculations.

Tests cover all methods of BRSovereignPricer class including LTN, NTN-F, NTN-B, 
LFT bonds and custody fee calculations.
"""

from datetime import date

import pytest

from stpstone.analytics.pricing.public_debt.br_national_bonds import BRSovereignPricer


class TestBRSovereignPricer:
    """Test suite for BRSovereignPricer class."""

    @pytest.fixture
    def pricer(self) -> BRSovereignPricer:
        """Fixture providing initialized BRSovereignPricer instance."""
        return BRSovereignPricer()

    # --------------------------------------------------
    # LTN (Zero-coupon bond) tests
    # --------------------------------------------------
    @pytest.mark.parametrize(
        "ytm,wddt,fv,wddy,expected",
        [
            (0.1297, 248, 1000.0, 252, 886.9059241913299),  # Example 1
            (0.1281, 748, 1000.0, 252, 699.2283541093051),  # Example 2
            (0.1350, 252, 1000.0, 252, 881.0572687224669),  # Example 3
            (0.1150, 252, 1000.0, 252, 896.8609865470852),  # Example 4
            (0.10, 126, 1000.0, 252, 953.463094516),  # Semi-annual
            (0.05, 252, 500.0, 252, 476.19047619),  # Different face value
        ],
    )
    def test_ltn_normal_cases(
        self,
        pricer: BRSovereignPricer,
        ytm: float,
        wddt: int,
        fv: float,
        wddy: int,
        expected: float,
    ) -> None:
        """Test LTN pricing with normal inputs.
        
        Parameters
        ----------
        pricer : BRSovereignPricer
            Instance of BRSovereignPricer class
        ytm : float
            Yield to maturity
        wddt : int
            Business days until maturity
        fv : float
            Nominal value
        wddy : int
            Business days in a year
        expected : float
            Expected result
        """
        result = pricer.ltn(ytm, wddt, fv, wddy)
        assert pytest.approx(result, abs=1e-2) == pytest.approx(expected, abs=1e-2)

    @pytest.mark.parametrize(
        "ytm,wddt,fv,wddy",
        [
            (0.0, 252, 1000.0, 252),  # Zero yield
            (0.1297, 0, 1000.0, 252),  # Zero days to maturity
            (0.1297, 252, 0.0, 252),  # Zero face value
            (0.1297, 252, 1000.0, 0),  # Zero days in year
        ],
    )
    def test_ltn_edge_cases(
        self,
        pricer: BRSovereignPricer,
        ytm: float,
        wddt: int,
        fv: float,
        wddy: int,
    ) -> None:
        """Test LTN pricing with edge case inputs.
        
        Parameters
        ----------
        pricer : BRSovereignPricer
            Instance of BRSovereignPricer class
        ytm : float
            Yield to maturity
        wddt : int
            Business days until maturity
        fv : float
            Nominal value
        wddy : int
            Business days in a year
        """
        with pytest.raises(ValueError):
            pricer.ltn(ytm, wddt, fv, wddy)

    @pytest.mark.parametrize(
        "ytm,wddt,fv,wddy",
        [
            ("0.1297", 248, 1000.0, 252),  # String yield
            (0.1297, "248", 1000.0, 252),  # String days
            (0.1297, 248, "1000.0", 252),  # String face value
            (0.1297, 248, 1000.0, "252"),  # String days in year
            (None, 248, 1000.0, 252),  # None yield
        ],
    )
    def test_ltn_type_validation(
        self,
        pricer: BRSovereignPricer,
        ytm: float,
        wddt: int,
        fv: float,
        wddy: int,
    ) -> None:
        """Test LTN pricing type validation.
        
        Parameters
        ----------
        pricer : BRSovereignPricer
            Instance of BRSovereignPricer class
        ytm : float
            Yield to maturity
        wddt : int
            Business days until maturity
        fv : float
            Nominal value
        wddy : int
            Business days in a year
        """
        with pytest.raises(TypeError):
            pricer.ltn(ytm, wddt, fv, wddy)

    # --------------------------------------------------
    # NTN-F (Fixed-rate bond) tests
    # --------------------------------------------------
    @pytest.mark.parametrize(
        "ytm,wddts,fv,cpn_y,expected",
        [
            (0.1298, [120, 248, 372, 499], 1000.0, 0.1, 953.75),  # Example 1
            (0.14, [122, 250, 374, 501, 625, 750, 874, 1000], 1000.0, 0.1, 889.33),  # Example 2
            (0.1298, [127, 251], 1000.0, 0.1, 974.66),  # Example 3
            (0.10, [126, 252], 1000.0, 0.05, 955.08),  # Lower coupon
            (0.05, [90, 180, 270, 360], 500.0, 0.06, 522.95),  # Different face value
        ],
    )
    def test_ntn_f_normal_cases(
        self,
        pricer: BRSovereignPricer,
        ytm: float,
        wddts: list[int],
        fv: float,
        cpn_y: float,
        expected: float,
    ) -> None:
        """Test NTN-F pricing with normal inputs.
        
        Parameters
        ----------
        pricer : BRSovereignPricer
            Instance of BRSovereignPricer class
        ytm : float
            Yield to maturity
        wddts : list[int]
            List of business days until maturity
        fv : float
            Nominal value
        cpn_y : float
            Nominal yield of the coupon
        expected : float
            Expected result
        """
        result = pricer.ntn_f(ytm, wddts, fv, cpn_y)
        assert result == pytest.approx(expected, abs=0.01)

    def test_ntn_f_empty_days_list(self, pricer: BRSovereignPricer) -> None:
        """Test NTN-F with empty days list.
        
        Parameters
        ----------
        pricer : BRSovereignPricer
            Instance of BRSovereignPricer class
        """
        with pytest.raises(ValueError):
            pricer.ntn_f(0.1298, [], 1000.0, 0.1)

    @pytest.mark.parametrize(
        "wddts",
        [
            [120, 248, 372, 499, 120],  # Duplicate days
            [500, 400, 300, 200],  # Not ascending order
            [-120, 248, 372],  # Negative days
        ],
    )
    def test_ntn_f_invalid_days(
        self,
        pricer: BRSovereignPricer,
        wddts: list[int],
    ) -> None:
        """Test NTN-F with invalid days lists.
        
        Parameters
        ----------
        pricer : BRSovereignPricer
            Instance of BRSovereignPricer class
        wddts : list[int]
            List of business days until maturity
        """
        with pytest.raises(ValueError):
            pricer.ntn_f(0.1298, wddts, 1000.0, 0.1)

    # --------------------------------------------------
    # NTN-B (Inflation-linked bond) tests
    # --------------------------------------------------
    @pytest.mark.parametrize(
        "ytm,wddts,vna_last,ipca,pr1,expected",
        [
            (0.061, [127, 250, 374, 500], 2508.949127, 0.0, 1.0, 2506.65),  # Example 1
            (0.061, [127, 250], 2508.949127, 0.0970, 0.9951013, 2749.74),  # Example 2
            (0.05, [90, 180], 3000.0, 0.03, 0.5, 3115.76),  # Custom case
        ],
    )
    def test_ntn_b_normal_cases(
        self,
        pricer: BRSovereignPricer,
        ytm: float,
        wddts: list[int],
        vna_last: float,
        ipca: float,
        pr1: float,
        expected: float,
    ) -> None:
        """Test NTN-B pricing with normal inputs.
        
        Parameters
        ----------
        pricer : BRSovereignPricer
            Instance of BRSovereignPricer class
        ytm : float
            Yield to maturity
        wddts : list[int]
            List of business days until maturity
        vna_last : float
            Last available VNA (truncated at 6th decimal)
        ipca : float
            IPCA inflation rate
        pr1 : float
            pr1 = (number of calendar days between purchase date and the 15th of current month) / 
          (number of calendar days between the 15th of next month and the 15th of current month)
        expected : float
            Expected result
        """
        result = pricer.ntn_b(ytm, wddts, vna_last, ipca, pr1)
        assert result == pytest.approx(expected, abs=0.01)

    @pytest.mark.parametrize(
        "ytm,wddts,vna_last,ipca,pr1",
        [
            (0.061, [127, 250, 374, 500], -2508.949127, 0.0, 1.0),  # Negative VNA
            (0.061, [127, 250], 2508.949127, -0.0970, 0.9951013),  # Negative IPCA
            (0.061, [127, 250], 2508.949127, 0.0970, -0.9951013),  # Negative PR1
        ],
    )
    def test_ntn_b_invalid_values(
        self,
        pricer: BRSovereignPricer,
        ytm: float,
        wddts: list[int],
        vna_last: float,
        ipca: float,
        pr1: float,
    ) -> None:
        """Test NTN-B with invalid values.
        
        Parameters
        ----------
        pricer : BRSovereignPricer
            Instance of BRSovereignPricer class
        ytm : float
            Yield to maturity
        wddts : list[int]
            List of business days until maturity
        vna_last : float
            Last available VNA (truncated at 6th decimal)
        ipca : float
            IPCA inflation rate
        pr1 : float
            pr1 = (number of calendar days between purchase date and the 15th of current month) / 
          (number of calendar days between the 15th of next month and the 15th of current month)
        """
        with pytest.raises(ValueError):
            pricer.ntn_b(ytm, wddts, vna_last, ipca, pr1)

    # --------------------------------------------------
    # NTN-B Principal (Inflation-linked zero-coupon) tests
    # --------------------------------------------------
    @pytest.mark.parametrize(
        "ytm,wddt,vna_last,ipca,pr1,expected",
        [
            (0.0613, 1089, 2494.977146, 0.0079, 22/31, 1940.14),  # Example 1
            (0.05, 837, 2736.989929, 0.005, 22/31, 2335.77),  # Example 2
            (0.03, 365, 2000.0, 0.02, 0.5, 1935.24),  # Custom case
        ],
    )
    def test_ntn_b_principal_normal_cases(
        self,
        pricer: BRSovereignPricer,
        ytm: float,
        wddt: int,
        vna_last: float,
        ipca: float,
        pr1: float,
        expected: float,
    ) -> None:
        """Test NTN-B Principal pricing with normal inputs.
        
        Parameters
        ----------
        pricer : BRSovereignPricer
            Instance of BRSovereignPricer class
        ytm : float
            Yield to maturity
        wddt : int
            Business days until maturity
        vna_last : float
            Last available VNA (truncated at 6th decimal)
        ipca : float
            IPCA inflation rate
        pr1 : float
            pr1 = (number of calendar days between purchase date and the 15th of current month) / 
          (number of calendar days between the 15th of next month and the 15th of current month)
        expected : float
            Expected result
        """
        result = pricer.ntn_b_principal(ytm, wddt, vna_last, ipca, pr1)
        assert result == pytest.approx(expected, abs=0.01)

    # --------------------------------------------------
    # LFT (Floating rate bond) tests
    # --------------------------------------------------
    @pytest.mark.parametrize(
        "ytm,wddt,vna_last,selic,expected",
        [
            (0.0, 543, 6543.016794, 0.1175, 6545.9),  # Example 1
            (0.01, 365, 5000.0, 0.10, 4930.32),  # Custom case 1
            (0.02, 180, 3000.0, 0.08, 2958.76),  # Custom case 2
        ],
    )
    def test_lft_normal_cases(
        self,
        pricer: BRSovereignPricer,
        ytm: float,
        wddt: int,
        vna_last: float,
        selic: float,
        expected: float,
    ) -> None:
        """Test LFT pricing with normal inputs.
        
        Parameters
        ----------
        pricer : BRSovereignPricer
            Instance of BRSovereignPricer class
        ytm : float
            Yield to maturity
        wddt : int
            Business days until maturity
        vna_last : float
            Last available VNA (truncated at 6th decimal)
        selic : float
            Projected annual Selic rate
        expected : float
            Expected result
        """
        result = pricer.lft(ytm, wddt, vna_last, selic)
        assert result == pytest.approx(expected, abs=0.1)

    # --------------------------------------------------
    # Custody Fee tests
    # --------------------------------------------------
    @pytest.mark.parametrize(
        "price,cddt,expected",
        [
            (2780.36, 180, 4.11),  # Example 1
            (1000.0, 365, 3.0),  # Full year
            (5000.0, 30, 1.23),  # One month
            (1500000.0, 180, 2217.49),  # Maximum balance
        ],
    )
    def test_custody_fee_normal_cases(
        self,
        pricer: BRSovereignPricer,
        price: float,
        cddt: int,
        expected: float,
    ) -> None:
        """Test custody fee calculation with normal inputs.

        Parameters
        ----------
        price : float
            Price of the bond
        cddt : int
            Number of calendar days
        expected : float
            Expected result
        pricer : BRSovereignPricer
            Instance of BRSovereignPricer class
        """
        result = pricer.custody_fee_bmfbov(price, cddt)
        assert result == pytest.approx(expected, abs=0.01)

    @pytest.mark.parametrize(
        "price,cddt",
        [
            (-1000.0, 180),  # Negative price
            (1000.0, -180),  # Negative days
            (0.0, 180),  # Zero price
            (1000.0, 0),  # Zero days
        ],
    )
    def test_custody_fee_invalid_values(
        self,
        pricer: BRSovereignPricer,
        price: float,
        cddt: int,
    ) -> None:
        """Test custody fee with invalid values.

        Parameters
        ----------
        price : float
            Price of the bond
        cddt : int
            Number of calendar days
        pricer : BRSovereignPricer
            Instance of BRSovereignPricer class
        """
        with pytest.raises(ValueError):
            pricer.custody_fee_bmfbov(price, cddt)

    # --------------------------------------------------
    # PR1 Calculation tests
    # --------------------------------------------------
    @pytest.mark.parametrize(
        "date_ref,dt_ipca_last,dt_ipca_next,expected",
        [
            # Before 15th cases
            (date(2023, 6, 10), date(2023, 5, 15), date(2023, 6, 15), 0.806451613),
            (date(2023, 1, 10), date(2022, 12, 15), date(2023, 1, 15), 0.8387096774193549),
            # After 15th cases
            (date(2023, 6, 20), date(2023, 6, 15), date(2023, 7, 15), 0.166666666667), 
            (date(2023, 12, 20), date(2023, 12, 15), date(2024, 1, 15), 0.161290323),
            # Exactly on release day
            (date(2023, 2, 15), date(2023, 1, 15), date(2023, 2, 15), 1.0),
            # Month with different day counts
            (date(2023, 4, 10), date(2023, 3, 15), date(2023, 4, 15), 0.8387096774193549),
            (date(2023, 3, 10), date(2023, 2, 15), date(2023, 3, 15), 0.8214285714),
            # Weekend/holiday adjustment cases
            (date(2023, 7, 10), date(2023, 6, 15), date(2023, 7, 17), 0.78125),
            (date(2023, 5, 10), date(2023, 4, 17), date(2023, 5, 15), 0.821428571),
        ],
    )
    def test_pr1_normal_cases(
        self,
        pricer: BRSovereignPricer,
        date_ref: date,
        dt_ipca_last: date,
        dt_ipca_next: date,
        expected: float,
    ) -> None:
        """Test PR1 calculation with normal dates.
        
        Parameters
        ----------
        pricer : BRSovereignPricer
            Instance of BRSovereignPricer class
        date_ref : date
            Reference date
        dt_ipca_last : date
            Last available IPCA date
        dt_ipca_next : date
            Next available IPCA date
        expected : float
            Expected result
        """
        result = pricer.pr1(date_ref, dt_ipca_last, dt_ipca_next)
        assert result == pytest.approx(expected, abs=1e-4)

    @pytest.mark.parametrize(
        "date_ref,dt_ipca_last,dt_ipca_next",
        [
            # Invalid date orders
            (date(2023, 6, 10), date(2023, 6, 15), date(2023, 5, 15)),  # Next before last
            (date(2023, 6, 10), date(2023, 7, 15), date(2023, 6, 15)),   # Last after ref
            (date(2023, 6, 10), date(2023, 6, 15), date(2023, 6, 10)),   # Next before ref
            # Same dates
            (date(2023, 6, 15), date(2023, 6, 15), date(2023, 6, 15)),   # All same
        ],
    )
    def test_pr1_invalid_date_orders(
        self,
        pricer: BRSovereignPricer,
        date_ref: date,
        dt_ipca_last: date,
        dt_ipca_next: date,
    ) -> None:
        """Test PR1 with invalid date orders.
        
        Parameters
        ----------
        pricer : BRSovereignPricer
            Instance of BRSovereignPricer class
        date_ref : date
            Reference date
        dt_ipca_last : date
            Last available IPCA date
        dt_ipca_next : date
            Next available IPCA date
        """
        with pytest.raises(ValueError):
            pricer.pr1(date_ref, dt_ipca_last, dt_ipca_next)

    def test_pr1_weekend_adjustment(
        self,
        pricer: BRSovereignPricer,
    ) -> None:
        """Test PR1 with weekend dates that should be adjusted.
        
        Parameters
        ----------
        pricer : BRSovereignPricer
            Instance of BRSovereignPricer class
        """
        # Saturday reference date should adjust to Friday
        result = pricer.pr1(
            date(2023, 7, 15),  # Saturday
            date(2023, 6, 15),
            date(2023, 7, 17)   # Next IPCA on Monday
        )
        # Should calculate based on Friday July 14
        assert result == pytest.approx(29/32, abs=1e-4)

    # --------------------------------------------------
    # VNA Projection tests
    # --------------------------------------------------
    @pytest.mark.parametrize(
        "vna_last,ipca,pr1,expected",
        [
            (1000.0, 0.01, 0.5, 1004.9875),  # Simple case
            (2494.977146, 0.0079, 22/31, 2508.9491278626583),  # NTN-B example
            (5000.0, 0.0, 0.5, 5000.0),  # Zero inflation
            (1000.0, 0.10, 1.0, 1100.0),  # Full period
        ],
    )
    def test_vna_ntnb_projection(
        self,
        pricer: BRSovereignPricer,
        vna_last: float,
        ipca: float,
        pr1: float,
        expected: float,
    ) -> None:
        """Test VNA projection for NTN-B bonds.
        
        Parameters
        ----------
        pricer : BRSovereignPricer
            Instance of BRSovereignPricer class
        vna_last : float
            Last available VNA (truncated at 6th decimal)
        ipca : float
            Projected next month IPCA inflation rate
        pr1 : float
            pr1 = (number of calendar days between purchase date and the 15th of current month) / 
          (number of calendar days between the 15th of next month and the 15th of current month)
        expected : float
            Expected result
        """
        result = pricer.vna_ntnb_hat(vna_last, ipca, pr1)
        assert result == pytest.approx(expected, abs=0.01)

    @pytest.mark.parametrize(
        "vna_last,selic,wd_cap,wddy,expected",
        [
            (1000.0, 0.10, 1, 252, 1000.3782865315343),  # Simple case
            (6543.016794, 0.1175, 1, 252, 6545.90191489939),  # LFT example
            (5000.0, 0.0, 1, 252, 5000.0),  # Zero rate
            (1000.0, 0.10, 252, 252, 1100.0),  # Daily compounding
        ],
    )
    def test_vna_lft_projection(
        self,
        pricer: BRSovereignPricer,
        vna_last: float,
        selic: float,
        wd_cap: int,
        wddy: int,
        expected: float,
    ) -> None:
        """Test VNA projection for LFT bonds.
        
        Parameters
        ----------
        pricer : BRSovereignPricer
            Instance of BRSovereignPricer class
        vna_last : float
            Last available VNA
        selic : float
            Projected annual Selic rate
        wd_cap : int
            Compounding frequency
        wddy : int
            Business days in a year
        expected : float
            Expected result
        """
        result = pricer.vna_lft_hat(vna_last, selic, wd_cap, wddy)
        assert pytest.approx(result, abs=1e-2) == pytest.approx(expected, abs=1e-2)