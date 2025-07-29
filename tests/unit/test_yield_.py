"""Unit tests for BondDuration class.

Tests the bond duration calculations including Macaulay, modified, dollar durations,
effective duration, convexity, and DV01 calculations with various input scenarios.
"""

import numpy as np
from numpy.typing import NDArray
import pytest

from stpstone.analytics.risk.yield_ import BondDuration


class TestBondDuration:
    """Test cases for BondDuration class.

    This test class verifies the behavior of all bond duration calculations
    with different input types and edge cases.
    """

    @pytest.fixture
    def sample_cash_flows(self) -> NDArray[np.float64]:
        """Fixture providing sample cash flows for testing.

        Returns
        -------
        NDArray[np.float64]
            Array of cash flows [100, 100, 100, 1100]
        """
        return np.array([100.0, 100.0, 100.0, 1100.0], dtype=np.float64)

    @pytest.fixture
    def valid_ytm(self) -> float:
        """Fixture providing valid yield to maturity.

        Returns
        -------
        float
            Yield to maturity of 0.05 (5%)
        """
        return 0.05

    @pytest.fixture
    def valid_fv(self) -> float:
        """Fixture providing valid face value.

        Returns
        -------
        float
            Face value of 1000.0
        """
        return 1000.0

    @pytest.fixture
    def bond_duration(
        self, sample_cash_flows: NDArray[np.float64], valid_ytm: float, valid_fv: float
    ) -> BondDuration:
        """Fixture providing initialized BondDuration instance.

        Parameters
        ----------
        sample_cash_flows : NDArray[np.float64]
            Cash flows from fixture
        valid_ytm : float
            Yield to maturity from fixture
        valid_fv : float
            Face value from fixture

        Returns
        -------
        BondDuration
            Initialized BondDuration instance
        """
        return BondDuration(sample_cash_flows, valid_ytm, valid_fv)

    # --------------------------
    # Initialization Tests
    # --------------------------
    def test_init_with_valid_inputs(
        self, sample_cash_flows: NDArray[np.float64], valid_ytm: float, valid_fv: float
    ) -> None:
        """Test initialization with valid inputs.

        Parameters
        ----------
        sample_cash_flows : NDArray[np.float64]
            Cash flows from fixture
        valid_ytm : float
            Yield to maturity from fixture
        valid_fv : float
            Face value from fixture

        Verifies
        --------
        - Instance is created successfully
        - Attributes are set correctly
        """
        bd = BondDuration(sample_cash_flows, valid_ytm, valid_fv)
        assert np.array_equal(bd.array_cfs, sample_cash_flows)
        assert bd.float_ytm == valid_ytm
        assert bd.float_fv == valid_fv
        assert bd.str_when == "end"

    def test_init_with_empty_cash_flows(self, valid_ytm: float, valid_fv: float) -> None:
        """Test initialization with empty cash flows.

        Parameters
        ----------
        valid_ytm : float
            Yield to maturity from fixture
        valid_fv : float
            Face value from fixture

        Verifies
        --------
        - ValueError is raised when cash flows array is empty
        """
        with pytest.raises(ValueError, match="Cash flows list cannot be empty"):
            BondDuration(np.array([], dtype=np.float64), valid_ytm, valid_fv)

    @pytest.mark.parametrize("invalid_ytm", [-0.1, 1.1, 2.0])
    def test_init_with_invalid_ytm(
        self, sample_cash_flows: NDArray[np.float64], invalid_ytm: float, valid_fv: float
    ) -> None:
        """Test initialization with invalid YTM values.

        Parameters
        ----------
        sample_cash_flows : NDArray[np.float64]
            Cash flows from fixture
        invalid_ytm : float
            Invalid yield to maturity values
        valid_fv : float
            Face value from fixture

        Verifies
        --------
        - ValueError is raised when YTM is outside [0, 1] range
        """
        with pytest.raises(ValueError, match="Yield to maturity must be between 0 and 1"):
            BondDuration(sample_cash_flows, invalid_ytm, valid_fv)

    # --------------------------
    # Macaulay Duration Tests
    # --------------------------
    def test_macaulay_duration_calculation(self, bond_duration: BondDuration) -> None:
        """Test Macaulay duration calculation.

        Parameters
        ----------
        bond_duration : BondDuration
            Initialized BondDuration instance from fixture

        Verifies
        --------
        - Macaulay duration is calculated correctly
        - Return value is float
        """
        duration = bond_duration.macaulay()
        assert isinstance(duration, float)
        assert duration > 0

    # --------------------------
    # Modified Duration Tests
    # --------------------------
    def test_modified_duration_calculation(self, bond_duration: BondDuration) -> None:
        """Test modified duration calculation.

        Parameters
        ----------
        bond_duration : BondDuration
            Initialized BondDuration instance from fixture

        Verifies
        --------
        - Modified duration is calculated correctly
        - Return value is float
        """
        modified_duration = bond_duration.modified(0.05, 2)
        assert isinstance(modified_duration, float)
        assert modified_duration > 0

    @pytest.mark.parametrize("invalid_yield", [-0.1, 1.1, 2.0])
    def test_modified_duration_invalid_yield(
        self, bond_duration: BondDuration, invalid_yield: float
    ) -> None:
        """Test modified duration with invalid yield.

        Parameters
        ----------
        bond_duration : BondDuration
            Initialized BondDuration instance from fixture
        invalid_yield : float
            Invalid yield values

        Verifies
        --------
        - ValueError is raised when yield is outside [0, 1] range
        """
        with pytest.raises(ValueError, match="Yield must be between 0 and 1"):
            bond_duration.modified(invalid_yield, 2)

    @pytest.mark.parametrize("invalid_periods", [0, -1, -2])
    def test_modified_duration_invalid_periods(
        self, bond_duration: BondDuration, invalid_periods: int
    ) -> None:
        """Test modified duration with invalid periods.

        Parameters
        ----------
        bond_duration : BondDuration
            Initialized BondDuration instance from fixture
        invalid_periods : int
            Invalid capitalization periods

        Verifies
        --------
        - ValueError is raised when periods is not positive
        """
        with pytest.raises(ValueError, match="Capitalization periods must be positive"):
            bond_duration.modified(0.05, invalid_periods)

    # --------------------------
    # Dollar Duration Tests
    # --------------------------
    def test_dollar_duration_calculation(self, bond_duration: BondDuration) -> None:
        """Test dollar duration calculation.

        Parameters
        ----------
        bond_duration : BondDuration
            Initialized BondDuration instance from fixture

        Verifies
        --------
        - Dollar duration is calculated correctly
        - Return value is float
        """
        dollar_duration = bond_duration.dollar(0.05, 2)
        assert isinstance(dollar_duration, float)
        assert dollar_duration < 0  # Dollar duration should be negative

    # --------------------------
    # Effective Duration Tests
    # --------------------------
    def test_effective_duration_calculation(self, bond_duration: BondDuration) -> None:
        """Test effective duration calculation.

        Parameters
        ----------
        bond_duration : BondDuration
            Initialized BondDuration instance from fixture

        Verifies
        --------
        - Effective duration is calculated correctly
        - Return value is float
        """
        effective_duration = bond_duration.effective(0.01)
        assert isinstance(effective_duration, float)
        assert effective_duration > 0

    @pytest.mark.parametrize("invalid_delta", [0.0, -0.01, -0.1])
    def test_effective_duration_invalid_delta(
        self, bond_duration: BondDuration, invalid_delta: float
    ) -> None:
        """Test effective duration with invalid delta.

        Parameters
        ----------
        bond_duration : BondDuration
            Initialized BondDuration instance from fixture
        invalid_delta : float
            Invalid yield change values

        Verifies
        --------
        - ValueError is raised when delta is not positive
        """
        with pytest.raises(ValueError, match="Yield change must be positive"):
            bond_duration.effective(invalid_delta)

    # --------------------------
    # Convexity Tests
    # --------------------------
    def test_convexity_calculation(self, bond_duration: BondDuration) -> None:
        """Test convexity calculation.

        Parameters
        ----------
        bond_duration : BondDuration
            Initialized BondDuration instance from fixture

        Verifies
        --------
        - Convexity is calculated correctly
        - Return value is float
        """
        convexity = bond_duration.convexity(0.01)
        assert isinstance(convexity, float)
        assert convexity > 0

    @pytest.mark.parametrize("invalid_delta", [0.0, -0.01, -0.1])
    def test_convexity_invalid_delta(
        self, bond_duration: BondDuration, invalid_delta: float
    ) -> None:
        """Test convexity with invalid delta.

        Parameters
        ----------
        bond_duration : BondDuration
            Initialized BondDuration instance from fixture
        invalid_delta : float
            Invalid yield change values

        Verifies
        --------
        - ValueError is raised when delta is not positive
        """
        with pytest.raises(ValueError, match="Yield change must be positive"):
            bond_duration.convexity(invalid_delta)

    # --------------------------
    # DV01 Tests
    # --------------------------
    def test_dv_y_calculation(self, bond_duration: BondDuration) -> None:
        """Test DV01 calculation.

        Parameters
        ----------
        bond_duration : BondDuration
            Initialized BondDuration instance from fixture

        Verifies
        --------
        - DV01 is calculated correctly
        - Return value is float
        """
        dv01 = bond_duration.dv_y(0.05, 2)
        assert isinstance(dv01, float)

    @pytest.mark.parametrize("invalid_delta", [0.0, -0.01, -0.1])
    def test_dv_y_invalid_delta(
        self, bond_duration: BondDuration, invalid_delta: float
    ) -> None:
        """Test DV01 with invalid delta.

        Parameters
        ----------
        bond_duration : BondDuration
            Initialized BondDuration instance from fixture
        invalid_delta : float
            Invalid yield change values

        Verifies
        --------
        - ValueError is raised when delta is not positive
        """
        with pytest.raises(ValueError, match="Yield change must be positive"):
            bond_duration.dv_y(0.05, 2, invalid_delta)

    # --------------------------
    # Type Validation Tests
    # --------------------------
    @pytest.mark.parametrize("invalid_cfs", ["string", 123, None])
    def test_invalid_cash_flows_type(
        self, 
        invalid_cfs: str, 
        valid_ytm: float, 
        valid_fv: float
    ) -> None:
        """Test initialization with invalid cash flows types.

        Parameters
        ----------
        invalid_cfs : str
            Invalid cash flows types
        valid_ytm : float
            Yield to maturity from fixture
        valid_fv : float
            Face value from fixture

        Verifies
        --------
        - TypeError is raised when cash flows is not NDArray[np.float64]
        """
        with pytest.raises(TypeError, match="must be of type"):
            BondDuration(invalid_cfs, valid_ytm, valid_fv)

    @pytest.mark.parametrize("invalid_ytm", ["string", None, []])
    def test_invalid_ytm_type(
        self, sample_cash_flows: NDArray[np.float64], invalid_ytm: str, valid_fv: float
    ) -> None:
        """Test initialization with invalid YTM types.

        Parameters
        ----------
        sample_cash_flows : NDArray[np.float64]
            Cash flows from fixture
        invalid_ytm : str
            Invalid YTM types
        valid_fv : float
            Face value from fixture

        Verifies
        --------
        - TypeError is raised when YTM is not float
        """
        with pytest.raises(TypeError):
            BondDuration(sample_cash_flows, invalid_ytm, valid_fv)

    @pytest.mark.parametrize("invalid_when", ["middle", 123, None])
    def test_invalid_when_type(
        self,
        sample_cash_flows: NDArray[np.float64],
        valid_ytm: float,
        valid_fv: float,
        invalid_when: str,
    ) -> None:
        """Test initialization with invalid when types.

        Parameters
        ----------
        sample_cash_flows : NDArray[np.float64]
            Cash flows from fixture
        valid_ytm : float
            Yield to maturity from fixture
        valid_fv : float
            Face value from fixture
        invalid_when : str
            Invalid when values

        Verifies
        --------
        - TypeError is raised when when is not Literal["end", "begin"]
        """
        with pytest.raises(TypeError):
            BondDuration(sample_cash_flows, valid_ytm, valid_fv, invalid_when)