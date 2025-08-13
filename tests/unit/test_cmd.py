"""Unit tests for BColors class.

Tests the ANSI color code constants and their usage in terminal text formatting.
Includes verification of all color codes and formatting options.
"""

import platform

import pytest


if platform.system() != "Windows":
    pytest.skip("CMD tests require Windows", allow_module_level=True)
else:
    from stpstone.utils.microsoft_apps.cmd import BColors

class TestBColors:
    """Test cases for BColors class.

    This test class verifies the presence and values of all ANSI color code
    constants defined in the BColors class.
    """

    def test_header_constant(self) -> None:
        """Test HEADER constant value and type.

        Verifies
        --------
        - HEADER constant exists
        - Is of type str
        - Has correct ANSI escape sequence value

        Returns
        -------
        None
        """
        assert hasattr(BColors, "HEADER")
        assert isinstance(BColors.HEADER, str)
        assert BColors.HEADER == "\033[95m"

    def test_okblue_constant(self) -> None:
        """Test OKBLUE constant value and type.

        Verifies
        --------
        - OKBLUE constant exists
        - Is of type str
        - Has correct ANSI escape sequence value

        Returns
        -------
        None
        """
        assert hasattr(BColors, "OKBLUE")
        assert isinstance(BColors.OKBLUE, str)
        assert BColors.OKBLUE == "\033[94m"

    def test_okcyan_constant(self) -> None:
        """Test OKCYAN constant value and type.

        Verifies
        --------
        - OKCYAN constant exists
        - Is of type str
        - Has correct ANSI escape sequence value

        Returns
        -------
        None
        """
        assert hasattr(BColors, "OKCYAN")
        assert isinstance(BColors.OKCYAN, str)
        assert BColors.OKCYAN == "\033[96m"

    def test_okgreen_constant(self) -> None:
        """Test OKGREEN constant value and type.

        Verifies
        --------
        - OKGREEN constant exists
        - Is of type str
        - Has correct ANSI escape sequence value

        Returns
        -------
        None
        """
        assert hasattr(BColors, "OKGREEN")
        assert isinstance(BColors.OKGREEN, str)
        assert BColors.OKGREEN == "\033[92m"

    def test_warning_constant(self) -> None:
        """Test WARNING constant value and type.

        Verifies
        --------
        - WARNING constant exists
        - Is of type str
        - Has correct ANSI escape sequence value

        Returns
        -------
        None
        """
        assert hasattr(BColors, "WARNING")
        assert isinstance(BColors.WARNING, str)
        assert BColors.WARNING == "\033[93m"

    def test_fail_constant(self) -> None:
        """Test FAIL constant value and type.

        Verifies
        --------
        - FAIL constant exists
        - Is of type str
        - Has correct ANSI escape sequence value

        Returns
        -------
        None
        """
        assert hasattr(BColors, "FAIL")
        assert isinstance(BColors.FAIL, str)
        assert BColors.FAIL == "\033[91m"

    def test_endc_constant(self) -> None:
        """Test ENDC constant value and type.

        Verifies
        --------
        - ENDC constant exists
        - Is of type str
        - Has correct ANSI escape sequence value

        Returns
        -------
        None
        """
        assert hasattr(BColors, "ENDC")
        assert isinstance(BColors.ENDC, str)
        assert BColors.ENDC == "\033[0m"

    def test_bold_constant(self) -> None:
        """Test BOLD constant value and type.

        Verifies
        --------
        - BOLD constant exists
        - Is of type str
        - Has correct ANSI escape sequence value

        Returns
        -------
        None
        """
        assert hasattr(BColors, "BOLD")
        assert isinstance(BColors.BOLD, str)
        assert BColors.BOLD == "\033[1m"

    def test_underline_constant(self) -> None:
        """Test UNDERLINE constant value and type.

        Verifies
        --------
        - UNDERLINE constant exists
        - Is of type str
        - Has correct ANSI escape sequence value

        Returns
        -------
        None
        """
        assert hasattr(BColors, "UNDERLINE")
        assert isinstance(BColors.UNDERLINE, str)
        assert BColors.UNDERLINE == "\033[4m"

    def test_all_constants_present(self) -> None:
        """Test presence of all expected constants.

        Verifies
        --------
        - All expected color/formatting constants are present
        - No unexpected constants are present

        Returns
        -------
        None
        """
        expected_constants = {
            "HEADER", "OKBLUE", "OKCYAN", "OKGREEN",
            "WARNING", "FAIL", "ENDC", "BOLD", "UNDERLINE"
        }
        actual_constants = {
            name for name in dir(BColors)
            if not name.startswith("_") and isinstance(getattr(BColors, name), str)
        }
        assert actual_constants == expected_constants

    def test_constant_values_are_strings(self) -> None:
        """Test all constants are strings.

        Verifies
        --------
        - All color/formatting constants are of type str

        Returns
        -------
        None
        """
        for name in dir(BColors):
            if not name.startswith("_") and not callable(getattr(BColors, name)):
                assert isinstance(getattr(BColors, name), str), f"{name} is not a string"

    def test_constant_values_format(self) -> None:
        r"""Test ANSI escape sequence format.

        Verifies
        --------
        - All color/formatting constants follow ANSI escape sequence format
        - Start with \033[
        - End with m

        Returns
        -------
        None
        """
        for name in dir(BColors):
            if not name.startswith("_") and not callable(getattr(BColors, name)):
                value = getattr(BColors, name)
                assert value.startswith("\033["), f"{name} does not start with ANSI escape"
                assert value.endswith("m"), f"{name} does not end with 'm'"
                assert len(value) >= 4, f"{name} value is too short"

    @pytest.mark.parametrize("constant_name,expected_value", [
        ("HEADER", "\033[95m"),
        ("OKBLUE", "\033[94m"),
        ("OKCYAN", "\033[96m"),
        ("OKGREEN", "\033[92m"),
        ("WARNING", "\033[93m"),
        ("FAIL", "\033[91m"),
        ("ENDC", "\033[0m"),
        ("BOLD", "\033[1m"),
        ("UNDERLINE", "\033[4m"),
    ])
    def test_constant_values_parametrized(self, constant_name: str, expected_value: str) -> None:
        """Parametrized test for constant values.

        Verifies
        --------
        - Each constant has the exact expected value

        Parameters
        ----------
        constant_name : str
            Name of the constant to test
        expected_value : str
            Expected ANSI escape sequence value

        Returns
        -------
        None
        """
        assert hasattr(BColors, constant_name)
        assert getattr(BColors, constant_name) == expected_value