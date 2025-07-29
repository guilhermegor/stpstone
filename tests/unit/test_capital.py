"""Unit tests for CR1Calculator class.

This module contains tests for the Capital Requirement 1 (CR1) calculator,
verifying initialization, calculations, and input validation for various
scenarios including normal cases, edge cases, and error conditions.
"""

import importlib
import math
import sys
from typing import TypedDict

import pytest

from stpstone.analytics.risk.capital import CR1Calculator


class ReturnSummary(TypedDict):
    """Dictionary containing CR1 calculation parameters and results.

    Parameters
    ----------
    EAD : float
        Exposure at Default
    PD : float
        Probability of Default
    LGD : float
        Loss Given Default
    K : float
        Capital requirement factor
    CR1 : float
        Capital Requirement 1
    """

    EAD: float
    PD: float
    LGD: float
    K: float
    CR1: float


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def valid_calculator() -> CR1Calculator:
    """Fixture providing CR1Calculator with valid inputs.

    Returns
    -------
    CR1Calculator
        Instance initialized with EAD=1000, PD=0.1, LGD=0.5
    """
    return CR1Calculator(float_ead=1000.0, float_pd=0.1, float_lgd=0.5)


# --------------------------
# Tests
# --------------------------
class TestCR1Calculator:
    """Test cases for CR1Calculator class.

    Verifies initialization, calculations, and input validation for CR1
    calculations, covering normal operations, edge cases, and error conditions.
    """

    def test_init_valid_inputs(self) -> None:
        """Test initialization with valid float inputs.

        Verifies
        --------
        - Instance is created successfully
        - Attributes are correctly set
        - Attribute types are preserved
        """
        calc = CR1Calculator(float_ead=1000.0, float_pd=0.1, float_lgd=0.5)
        assert calc.float_ead == pytest.approx(1000.0, abs=1e-6)
        assert calc.float_pd == pytest.approx(0.1, abs=1e-6)
        assert calc.float_lgd == pytest.approx(0.5, abs=1e-6)
        assert isinstance(calc.float_ead, float)
        assert isinstance(calc.float_pd, float)
        assert isinstance(calc.float_lgd, float)

    def test_init_edge_cases(self) -> None:
        """Test initialization with edge case inputs.

        Verifies
        --------
        - Zero EAD is accepted
        - Boundary values for PD and LGD (0 and 1) are accepted
        """
        calc = CR1Calculator(float_ead=0.0, float_pd=0.0, float_lgd=1.0)
        assert calc.float_ead == 0.0
        assert calc.float_pd == 0.0
        assert calc.float_lgd == 1.0

    def test_invalid_ead_type(self) -> None:
        """Test initialization with non-numeric EAD.

        Verifies
        --------
        - TypeError is raised for non-numeric EAD
        - Error message is correct
        """
        with pytest.raises(TypeError, match="Exposure at Default must be numeric"):
            CR1Calculator(float_ead="1000", float_pd=0.1, float_lgd=0.5)

    def test_invalid_pd_type(self) -> None:
        """Test initialization with non-numeric PD.

        Verifies
        --------
        - TypeError is raised for non-numeric PD
        - Error message is correct
        """
        with pytest.raises(TypeError, match="Probability of Default must be numeric"):
            CR1Calculator(float_ead=1000.0, float_pd="0.1", float_lgd=0.5)

    def test_invalid_lgd_type(self) -> None:
        """Test initialization with non-numeric LGD.

        Verifies
        --------
        - TypeError is raised for non-numeric LGD
        - Error message is correct
        """
        with pytest.raises(TypeError, match="Loss Given Default must be numeric"):
            CR1Calculator(float_ead=1000.0, float_pd=0.1, float_lgd="0.5")

    def test_negative_ead(self) -> None:
        """Test initialization with negative EAD.

        Verifies
        --------
        - ValueError is raised for negative EAD
        - Error message is correct
        """
        with pytest.raises(ValueError, match="Exposure at Default must be non-negative"):
            CR1Calculator(float_ead=-1000.0, float_pd=0.1, float_lgd=0.5)

    def test_pd_out_of_range(self) -> None:
        """Test initialization with PD outside [0, 1].

        Verifies
        --------
        - ValueError is raised for PD < 0 or PD > 1
        - Error message is correct
        """
        with pytest.raises(ValueError, match="Probability of Default must be in \\[0, 1\\]"):
            CR1Calculator(float_ead=1000.0, float_pd=1.1, float_lgd=0.5)
        with pytest.raises(ValueError, match="Probability of Default must be in \\[0, 1\\]"):
            CR1Calculator(float_ead=1000.0, float_pd=-0.1, float_lgd=0.5)

    def test_lgd_out_of_range(self) -> None:
        """Test initialization with LGD outside [0, 1].

        Verifies
        --------
        - ValueError is raised for LGD < 0 or LGD > 1
        - Error message is correct
        """
        with pytest.raises(ValueError, match="Loss Given Default must be in \\[0, 1\\]"):
            CR1Calculator(float_ead=1000.0, float_pd=0.1, float_lgd=1.1)
        with pytest.raises(ValueError, match="Loss Given Default must be in \\[0, 1\\]"):
            CR1Calculator(float_ead=1000.0, float_pd=0.1, float_lgd=-0.5)

    def test_non_finite_inputs(self) -> None:
        """Test initialization with non-finite inputs.

        Verifies
        --------
        - ValueError is raised for NaN or infinite inputs
        - Error message is correct
        """
        with pytest.raises(ValueError, match="Exposure at Default must be finite"):
            CR1Calculator(float_ead=float("inf"), float_pd=0.1, float_lgd=0.5)
        with pytest.raises(ValueError, match="Probability of Default must be in"):
            CR1Calculator(float_ead=1000.0, float_pd=float("nan"), float_lgd=0.5)
        with pytest.raises(ValueError, match="Loss Given Default must be in"):
            CR1Calculator(float_ead=1000.0, float_pd=0.1, float_lgd=float("inf"))

    def test_calculate_k(self, valid_calculator: CR1Calculator) -> None:
        """Test calculate_k method with valid inputs.

        Parameters
        ----------
        valid_calculator : CR1Calculator
            Fixture providing CR1Calculator instance

        Verifies
        --------
        - Correct calculation of K (LGD * PD)
        - Return type is float
        - Result is finite
        """
        k = valid_calculator.calculate_k()
        assert k == pytest.approx(0.05, abs=1e-6)
        assert isinstance(k, float)
        assert math.isfinite(k)

    def test_calculate_k_edge_cases(self) -> None:
        """Test calculate_k with edge case inputs.

        Verifies
        --------
        - Correct calculation for boundary values (PD=0, LGD=0)
        - Correct calculation for boundary values (PD=1, LGD=1)
        """
        calc_zero = CR1Calculator(float_ead=1000.0, float_pd=0.0, float_lgd=0.0)
        assert calc_zero.calculate_k() == 0.0

        calc_one = CR1Calculator(float_ead=1000.0, float_pd=1.0, float_lgd=1.0)
        assert calc_one.calculate_k() == 1.0

    def test_calculate_cr1(self, valid_calculator: CR1Calculator) -> None:
        """Test calculate_cr1 method with valid inputs.

        Parameters
        ----------
        valid_calculator : CR1Calculator
            Fixture providing CR1Calculator instance

        Verifies
        --------
        - Correct calculation of CR1 (12.5 * K * EAD)
        - Return type is float
        - Result is finite
        """
        cr1 = valid_calculator.calculate_cr1()
        assert cr1 == pytest.approx(625.0, abs=1e-6)
        assert isinstance(cr1, float)
        assert math.isfinite(cr1)

    def test_calculate_cr1_edge_cases(self) -> None:
        """Test calculate_cr1 with edge case inputs.

        Verifies
        --------
        - Correct calculation for zero EAD
        - Correct calculation for boundary values (PD=0, LGD=0)
        """
        calc_zero_ead = CR1Calculator(float_ead=0.0, float_pd=0.1, float_lgd=0.5)
        assert calc_zero_ead.calculate_cr1() == 0.0

        calc_zero_k = CR1Calculator(float_ead=1000.0, float_pd=0.0, float_lgd=0.0)
        assert calc_zero_k.calculate_cr1() == 0.0

    def test_calculate_cr1_non_finite(self) -> None:
        """Test calculate_cr1 with potentially non-finite results.

        Verifies
        --------
        - ValueError is raised for non-finite results
        - Error message is correct
        """
        with pytest.raises(ValueError, match="Exposure at Default must be finite"):
            CR1Calculator(float_ead=float("inf"), float_pd=0.1, float_lgd=0.5)

    def test_summary(self, valid_calculator: CR1Calculator) -> None:
        """Test summary method with valid inputs.

        Parameters
        ----------
        valid_calculator : CR1Calculator
            Fixture providing CR1Calculator instance

        Verifies
        --------
        - Correct dictionary structure and values
        - Return type is ReturnSummary
        - All values are finite
        """
        summary = valid_calculator.summary()
        expected = {
            "EAD": 1000.0,
            "PD": 0.1,
            "LGD": 0.5,
            "K": 0.05,
            "CR1": 625.0
        }
        assert isinstance(summary, dict)
        for key, value in expected.items():
            assert summary[key] == pytest.approx(value, abs=1e-6)
            assert math.isfinite(summary[key])

    def test_summary_edge_cases(self) -> None:
        """Test summary with edge case inputs.

        Verifies
        --------
        - Correct dictionary for zero values
        - Correct dictionary for maximum values
        """
        calc_zero = CR1Calculator(float_ead=0.0, float_pd=0.0, float_lgd=0.0)
        summary_zero = calc_zero.summary()
        assert summary_zero == {
            "EAD": 0.0,
            "PD": 0.0,
            "LGD": 0.0,
            "K": 0.0,
            "CR1": 0.0
        }

        calc_max = CR1Calculator(float_ead=1000.0, float_pd=1.0, float_lgd=1.0)
        summary_max = calc_max.summary()
        assert summary_max == {
            "EAD": 1000.0,
            "PD": 1.0,
            "LGD": 1.0,
            "K": 1.0,
            "CR1": 12500.0
        }

    def test_module_reload(self) -> None:
        """Test module reloading behavior.

        Verifies
        --------
        - Module can be reloaded without errors
        - Class functionality remains intact after reload
        """
        calc_before = CR1Calculator(float_ead=1000.0, float_pd=0.1, float_lgd=0.5)
        importlib.reload(sys.modules["stpstone.analytics.risk.capital"])
        calc_after = CR1Calculator(float_ead=1000.0, float_pd=0.1, float_lgd=0.5)
        assert calc_before.calculate_cr1() == calc_after.calculate_cr1()