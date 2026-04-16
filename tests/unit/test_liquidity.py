"""Unit tests for financial ratios calculation module.

This module contains comprehensive tests for liquidity and solvency ratio calculations,
covering normal operations, edge cases, error conditions, and type validations.
"""

import importlib
import sys

import pytest

from stpstone.analytics.risk.liquidity import LiquidityRatios, SolvencyRatios


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def liquidity_ratios() -> LiquidityRatios:
	"""Provide a LiquidityRatios instance.

	Returns
	-------
	LiquidityRatios
		Initialized instance of LiquidityRatios
	"""
	return LiquidityRatios()


@pytest.fixture
def solvency_ratios() -> SolvencyRatios:
	"""Provide a SolvencyRatios instance.

	Returns
	-------
	SolvencyRatios
		Initialized instance of SolvencyRatios
	"""
	return SolvencyRatios()


# --------------------------
# Tests for LiquidityRatios
# --------------------------
class TestLiquidityRatios:
	"""Test cases for LiquidityRatios class methods.

	Verifies all liquidity ratio calculations including validation checks and edge cases.
	"""

	def test_current_ratio_valid_inputs(self, liquidity_ratios: LiquidityRatios) -> None:
		"""Test current_ratio with valid positive inputs.

		Parameters
		----------
		liquidity_ratios : LiquidityRatios
			Instance of LiquidityRatios class

		Verifies
		--------
		Correct calculation of current ratio with valid inputs
		"""
		result = liquidity_ratios.current_ratio(1000.0, 500.0)
		assert result == pytest.approx(2.0, abs=1e-6)

	def test_current_ratio_zero_liabilities(self, liquidity_ratios: LiquidityRatios) -> None:
		"""Test current_ratio with zero current liabilities.

		Parameters
		----------
		liquidity_ratios : LiquidityRatios
			Instance of LiquidityRatios class

		Verifies
		--------
		Raises ValueError for zero current liabilities
		"""
		with pytest.raises(ValueError, match="current_liabilities must be positive"):
			liquidity_ratios.current_ratio(1000.0, 0.0)

	def test_current_ratio_negative_assets(self, liquidity_ratios: LiquidityRatios) -> None:
		"""Test current_ratio with negative current assets.

		Parameters
		----------
		liquidity_ratios : LiquidityRatios
			Instance of LiquidityRatios class

		Verifies
		--------
		Raises ValueError for negative current assets
		"""
		with pytest.raises(ValueError, match="current_assets must be positive"):
			liquidity_ratios.current_ratio(-1000.0, 500.0)

	def test_current_ratio_non_finite(self, liquidity_ratios: LiquidityRatios) -> None:
		"""Test current_ratio with non-finite inputs.

		Parameters
		----------
		liquidity_ratios : LiquidityRatios
			Instance of LiquidityRatios class

		Verifies
		--------
		Raises ValueError for non-finite inputs
		"""
		with pytest.raises(ValueError, match="current_assets must be finite"):
			liquidity_ratios.current_ratio(float("inf"), 500.0)
		with pytest.raises(ValueError, match="current_liabilities must be finite"):
			liquidity_ratios.current_ratio(1000.0, float("nan"))

	def test_quick_ratio_valid_inputs(self, liquidity_ratios: LiquidityRatios) -> None:
		"""Test quick_ratio with valid positive inputs.

		Parameters
		----------
		liquidity_ratios : LiquidityRatios
			Instance of LiquidityRatios class

		Verifies
		--------
		Correct calculation of quick ratio with valid inputs
		"""
		result = liquidity_ratios.quick_ratio(1000.0, 200.0, 500.0)
		assert result == pytest.approx(1.6, abs=1e-6)

	def test_quick_ratio_assets_less_than_inventories(
		self, liquidity_ratios: LiquidityRatios
	) -> None:
		"""Test quick_ratio when current assets < inventories.

		Parameters
		----------
		liquidity_ratios : LiquidityRatios
			Instance of LiquidityRatios class

		Verifies
		--------
		Raises ValueError when current assets are less than inventories
		"""
		with pytest.raises(ValueError, match="current_assets must be >= inventories"):
			liquidity_ratios.quick_ratio(200.0, 300.0, 500.0)

	def test_quick_ratio_zero_liabilities(self, liquidity_ratios: LiquidityRatios) -> None:
		"""Test quick_ratio with zero current liabilities.

		Parameters
		----------
		liquidity_ratios : LiquidityRatios
			Instance of LiquidityRatios class

		Verifies
		--------
		Raises ValueError for zero current liabilities
		"""
		with pytest.raises(ValueError, match="current_liabilities must be positive"):
			liquidity_ratios.quick_ratio(1000.0, 200.0, 0.0)

	def test_quick_ratio_non_finite(self, liquidity_ratios: LiquidityRatios) -> None:
		"""Test quick_ratio with non-finite inputs.

		Parameters
		----------
		liquidity_ratios : LiquidityRatios
			Instance of LiquidityRatios class

		Verifies
		--------
		Raises ValueError for non-finite inputs
		"""
		with pytest.raises(ValueError, match="current_assets must be finite"):
			liquidity_ratios.quick_ratio(float("inf"), 200.0, 500.0)
		with pytest.raises(ValueError, match="inventories must be finite"):
			liquidity_ratios.quick_ratio(1000.0, float("nan"), 500.0)

	def test_dso_valid_inputs(self, liquidity_ratios: LiquidityRatios) -> None:
		"""Test dso with valid inputs and default days.

		Parameters
		----------
		liquidity_ratios : LiquidityRatios
			Instance of LiquidityRatios class

		Verifies
		--------
		Correct calculation of DSO with valid inputs
		"""
		result = liquidity_ratios.dso(1000.0, 10000.0, 365)
		assert result == pytest.approx(36.5, abs=1e-6)

	def test_dso_valid_days(self, liquidity_ratios: LiquidityRatios) -> None:
		"""Test dso with all valid day options.

		Parameters
		----------
		liquidity_ratios : LiquidityRatios
			Instance of LiquidityRatios class

		Verifies
		--------
		Correct calculation for allowed day values (252, 360, 365)
		"""
		for days in [252, 360, 365]:
			result = liquidity_ratios.dso(1000.0, 10000.0, days)
			assert result == pytest.approx(days / 10.0, abs=1e-6)

	def test_dso_invalid_days(self, liquidity_ratios: LiquidityRatios) -> None:
		"""Test dso with invalid days value.

		Parameters
		----------
		liquidity_ratios : LiquidityRatios
			Instance of LiquidityRatios class

		Verifies
		--------
		Raises ValueError for invalid days value
		"""
		with pytest.raises(TypeError, match="must be one of types"):
			liquidity_ratios.dso(1000.0, 10000.0, 100)

	def test_dso_zero_revenue(self, liquidity_ratios: LiquidityRatios) -> None:
		"""Test dso with zero revenue.

		Parameters
		----------
		liquidity_ratios : LiquidityRatios
			Instance of LiquidityRatios class

		Verifies
		--------
		Raises ValueError for zero revenue
		"""
		with pytest.raises(ValueError, match="revenue must be positive"):
			liquidity_ratios.dso(1000.0, 0.0, 365)

	def test_dso_non_finite(self, liquidity_ratios: LiquidityRatios) -> None:
		"""Test dso with non-finite inputs.

		Parameters
		----------
		liquidity_ratios : LiquidityRatios
			Instance of LiquidityRatios class

		Verifies
		--------
		Raises ValueError for non-finite inputs
		"""
		with pytest.raises(ValueError, match="avg_accounts_receivable must be finite"):
			liquidity_ratios.dso(float("inf"), 10000.0, 365)
		with pytest.raises(ValueError, match="revenue must be finite"):
			liquidity_ratios.dso(1000.0, float("nan"), 365)

	def test_cash_ratio_valid_inputs(self, liquidity_ratios: LiquidityRatios) -> None:
		"""Test cash_ratio with valid positive inputs.

		Parameters
		----------
		liquidity_ratios : LiquidityRatios
			Instance of LiquidityRatios class

		Verifies
		--------
		Correct calculation of cash ratio with valid inputs
		"""
		result = liquidity_ratios.cash_ratio(1000.0, 500.0)
		assert result == pytest.approx(2.0, abs=1e-6)

	def test_cash_ratio_zero_liabilities(self, liquidity_ratios: LiquidityRatios) -> None:
		"""Test cash_ratio with zero current liabilities.

		Parameters
		----------
		liquidity_ratios : LiquidityRatios
			Instance of LiquidityRatios class

		Verifies
		--------
		Raises ValueError for zero current liabilities
		"""
		with pytest.raises(ValueError, match="current_liabilities must be positive"):
			liquidity_ratios.cash_ratio(1000.0, 0.0)

	def test_cash_ratio_non_finite(self, liquidity_ratios: LiquidityRatios) -> None:
		"""Test cash_ratio with non-finite inputs.

		Parameters
		----------
		liquidity_ratios : LiquidityRatios
			Instance of LiquidityRatios class

		Verifies
		--------
		Raises ValueError for non-finite inputs
		"""
		with pytest.raises(ValueError, match="cash_equivalents must be finite"):
			liquidity_ratios.cash_ratio(float("inf"), 500.0)


# --------------------------
# Tests for SolvencyRatios
# --------------------------
class TestSolvencyRatios:
	"""Test cases for SolvencyRatios class methods.

	Verifies all solvency ratio calculations including validation checks and edge cases.
	"""

	def test_interest_coverage_ratio_valid_inputs(self, solvency_ratios: SolvencyRatios) -> None:
		"""Test interest_coverage_ratio with valid inputs.

		Parameters
		----------
		solvency_ratios : SolvencyRatios
			Instance of SolvencyRatios class

		Verifies
		--------
		Correct calculation of interest coverage ratio with valid inputs
		"""
		result = solvency_ratios.interest_coverage_ratio(1000.0, 200.0)
		assert result == pytest.approx(5.0, abs=1e-6)

	def test_interest_coverage_ratio_negative_ebit(self, solvency_ratios: SolvencyRatios) -> None:
		"""Test interest_coverage_ratio with negative ebit.

		Parameters
		----------
		solvency_ratios : SolvencyRatios
			Instance of SolvencyRatios class

		Verifies
		--------
		Correct calculation with negative ebit
		"""
		result = solvency_ratios.interest_coverage_ratio(-1000.0, 200.0)
		assert result == pytest.approx(-5.0, abs=1e-6)

	def test_interest_coverage_ratio_zero_expenses(self, solvency_ratios: SolvencyRatios) -> None:
		"""Test interest_coverage_ratio with zero interest expenses.

		Parameters
		----------
		solvency_ratios : SolvencyRatios
			Instance of SolvencyRatios class

		Verifies
		--------
		Raises ValueError for zero interest expenses
		"""
		with pytest.raises(ValueError, match="interest_expenses must be positive"):
			solvency_ratios.interest_coverage_ratio(1000.0, 0.0)

	def test_interest_coverage_ratio_non_finite(self, solvency_ratios: SolvencyRatios) -> None:
		"""Test interest_coverage_ratio with non-finite inputs.

		Parameters
		----------
		solvency_ratios : SolvencyRatios
			Instance of SolvencyRatios class

		Verifies
		--------
		Raises ValueError for non-finite inputs
		"""
		with pytest.raises(ValueError, match="ebit must be finite"):
			solvency_ratios.interest_coverage_ratio(float("nan"), 200.0)
		with pytest.raises(ValueError, match="interest_expenses must be finite"):
			solvency_ratios.interest_coverage_ratio(1000.0, float("inf"))

	def test_debt_to_assets_ratio_valid_inputs(self, solvency_ratios: SolvencyRatios) -> None:
		"""Test debt_to_assets_ratio with valid positive inputs.

		Parameters
		----------
		solvency_ratios : SolvencyRatios
			Instance of SolvencyRatios class

		Verifies
		--------
		Correct calculation of debt to assets ratio
		"""
		result = solvency_ratios.debt_to_assets_ratio(500.0, 1000.0)
		assert result == pytest.approx(0.5, abs=1e-6)

	def test_debt_to_assets_ratio_zero_assets(self, solvency_ratios: SolvencyRatios) -> None:
		"""Test debt_to_assets_ratio with zero assets.

		Parameters
		----------
		solvency_ratios : SolvencyRatios
			Instance of SolvencyRatios class

		Verifies
		--------
		Raises ValueError for zero assets
		"""
		with pytest.raises(ValueError, match="assets must be positive"):
			solvency_ratios.debt_to_assets_ratio(500.0, 0.0)

	def test_debt_to_assets_ratio_non_finite(self, solvency_ratios: SolvencyRatios) -> None:
		"""Test debt_to_assets_ratio with non-finite inputs.

		Parameters
		----------
		solvency_ratios : SolvencyRatios
			Instance of SolvencyRatios class

		Verifies
		--------
		Raises ValueError for non-finite inputs
		"""
		with pytest.raises(ValueError, match="debt must be finite"):
			solvency_ratios.debt_to_assets_ratio(float("inf"), 1000.0)

	def test_equity_ratio_valid_inputs(self, solvency_ratios: SolvencyRatios) -> None:
		"""Test equity_ratio with valid positive inputs.

		Parameters
		----------
		solvency_ratios : SolvencyRatios
			Instance of SolvencyRatios class

		Verifies
		--------
		Correct calculation of equity ratio
		"""
		result = solvency_ratios.equity_ratio(600.0, 1000.0)
		assert result == pytest.approx(0.6, abs=1e-6)

	def test_equity_ratio_zero_assets(self, solvency_ratios: SolvencyRatios) -> None:
		"""Test equity_ratio with zero assets.

		Parameters
		----------
		solvency_ratios : SolvencyRatios
			Instance of SolvencyRatios class

		Verifies
		--------
		Raises ValueError for zero assets
		"""
		with pytest.raises(ValueError, match="assets must be positive"):
			solvency_ratios.equity_ratio(600.0, 0.0)

	def test_equity_ratio_non_finite(self, solvency_ratios: SolvencyRatios) -> None:
		"""Test equity_ratio with non-finite inputs.

		Parameters
		----------
		solvency_ratios : SolvencyRatios
			Instance of SolvencyRatios class

		Verifies
		--------
		Raises ValueError for non-finite inputs
		"""
		with pytest.raises(ValueError, match="total_shareholders_equity must be finite"):
			solvency_ratios.equity_ratio(float("nan"), 1000.0)

	def test_debt_to_equity_ratio_valid_inputs(self, solvency_ratios: SolvencyRatios) -> None:
		"""Test debt_to_equity_ratio with valid positive inputs.

		Parameters
		----------
		solvency_ratios : SolvencyRatios
			Instance of SolvencyRatios class

		Verifies
		--------
		Correct calculation of debt to equity ratio
		"""
		result = solvency_ratios.debt_to_equity_ratio(500.0, 1000.0)
		assert result == pytest.approx(0.5, abs=1e-6)

	def test_debt_to_equity_ratio_zero_equity(self, solvency_ratios: SolvencyRatios) -> None:
		"""Test debt_to_equity_ratio with zero equity.

		Parameters
		----------
		solvency_ratios : SolvencyRatios
			Instance of SolvencyRatios class

		Verifies
		--------
		Raises ValueError for zero equity
		"""
		with pytest.raises(ValueError, match="equity must be positive"):
			solvency_ratios.debt_to_equity_ratio(500.0, 0.0)

	def test_debt_to_equity_ratio_non_finite(self, solvency_ratios: SolvencyRatios) -> None:
		"""Test debt_to_equity_ratio with non-finite inputs.

		Parameters
		----------
		solvency_ratios : SolvencyRatios
			Instance of SolvencyRatios class

		Verifies
		--------
		Raises ValueError for non-finite inputs
		"""
		with pytest.raises(ValueError, match="debt must be finite"):
			solvency_ratios.debt_to_equity_ratio(float("inf"), 1000.0)

	def test_altmans_z_score_valid_inputs(self, solvency_ratios: SolvencyRatios) -> None:
		"""Test altmans_z_score with valid inputs.

		Parameters
		----------
		solvency_ratios : SolvencyRatios
			Instance of SolvencyRatios class

		Verifies
		--------
		Correct calculation of Altman Z-score with valid inputs
		"""
		result = solvency_ratios.altmans_z_score(100.0, 1000.0, 200.0, 300.0, 500.0, 400.0, 600.0)
		expected = (
			1.2 * (100.0 / 1000.0)
			+ 1.4 * (200.0 / 1000.0)
			+ 3.3 * (300.0 / 1000.0)
			+ 0.6 * (500.0 / 400.0)
			+ 1.0 * (600.0 / 1000.0)
		)
		assert result == pytest.approx(expected, abs=1e-6)

	def test_altmans_z_score_negative_inputs(self, solvency_ratios: SolvencyRatios) -> None:
		"""Test altmans_z_score with negative working capital, retained earnings, and ebit.

		Parameters
		----------
		solvency_ratios : SolvencyRatios
			Instance of SolvencyRatios class

		Verifies
		--------
		Correct calculation with negative inputs where allowed
		"""
		result = solvency_ratios.altmans_z_score(
			-100.0, 1000.0, -200.0, -300.0, 500.0, 400.0, 600.0
		)
		expected = (
			1.2 * (-100.0 / 1000.0)
			+ 1.4 * (-200.0 / 1000.0)
			+ 3.3 * (-300.0 / 1000.0)
			+ 0.6 * (500.0 / 400.0)
			+ 1.0 * (600.0 / 1000.0)
		)
		assert result == pytest.approx(expected, abs=1e-6)

	def test_altmans_z_score_zero_inputs(self, solvency_ratios: SolvencyRatios) -> None:
		"""Test altmans_z_score with zero inputs where not allowed.

		Parameters
		----------
		solvency_ratios : SolvencyRatios
			Instance of SolvencyRatios class

		Verifies
		--------
		Raises ValueError for zero inputs in restricted parameters
		"""
		with pytest.raises(ValueError, match="total_assets must be positive"):
			solvency_ratios.altmans_z_score(100.0, 0.0, 200.0, 300.0, 500.0, 400.0, 600.0)
		with pytest.raises(ValueError, match="market_capitalization must be positive"):
			solvency_ratios.altmans_z_score(100.0, 1000.0, 200.0, 300.0, 0.0, 400.0, 600.0)
		with pytest.raises(ValueError, match="total_liabilities must be positive"):
			solvency_ratios.altmans_z_score(100.0, 1000.0, 200.0, 300.0, 500.0, 0.0, 600.0)
		with pytest.raises(ValueError, match="sales must be positive"):
			solvency_ratios.altmans_z_score(100.0, 1000.0, 200.0, 300.0, 500.0, 400.0, 0.0)

	def test_altmans_z_score_non_finite(self, solvency_ratios: SolvencyRatios) -> None:
		"""Test altmans_z_score with non-finite inputs.

		Parameters
		----------
		solvency_ratios : SolvencyRatios
			Instance of SolvencyRatios class

		Verifies
		--------
		Raises ValueError for non-finite inputs
		"""
		with pytest.raises(ValueError, match="working_capital must be finite"):
			solvency_ratios.altmans_z_score(
				float("inf"), 1000.0, 200.0, 300.0, 500.0, 400.0, 600.0
			)
		with pytest.raises(ValueError, match="total_assets must be finite"):
			solvency_ratios.altmans_z_score(100.0, float("nan"), 200.0, 300.0, 500.0, 400.0, 600.0)


# --------------------------
# Module Reload Tests
# --------------------------
def test_module_reload() -> None:
	"""Test module reload behavior.

	Verifies
	--------
	Module can be reloaded without errors and maintains functionality
	"""
	importlib.reload(sys.modules["stpstone.analytics.risk.liquidity"])
	liquidity = LiquidityRatios()
	solvency = SolvencyRatios()
	assert liquidity.current_ratio(1000.0, 500.0) == pytest.approx(2.0, abs=1e-6)
	assert solvency.debt_to_assets_ratio(500.0, 1000.0) == pytest.approx(0.5, abs=1e-6)


# --------------------------
# Validation Function Tests
# --------------------------
def test_validate_positive_float_valid(liquidity_ratios: LiquidityRatios) -> None:
	"""Test _validate_positive_float with valid input.

	Parameters
	----------
	liquidity_ratios : LiquidityRatios
		Instance of LiquidityRatios class

	Verifies
	--------
	No exception raised for valid positive float
	"""
	liquidity_ratios._validate_positive_float(100.0, "test_value")


def test_validate_positive_float_invalid(liquidity_ratios: LiquidityRatios) -> None:
	"""Test _validate_positive_float with invalid inputs.

	Parameters
	----------
	liquidity_ratios : LiquidityRatios
		Instance of LiquidityRatios class

	Verifies
	--------
	Raises ValueError for zero, negative, or non-finite inputs
	"""
	with pytest.raises(ValueError, match="test_value must be positive"):
		liquidity_ratios._validate_positive_float(0.0, "test_value")
	with pytest.raises(ValueError, match="test_value must be positive"):
		liquidity_ratios._validate_positive_float(-1.0, "test_value")
	with pytest.raises(ValueError, match="test_value must be finite"):
		liquidity_ratios._validate_positive_float(float("inf"), "test_value")


def test_validate_float_valid(solvency_ratios: SolvencyRatios) -> None:
	"""Test _validate_float with valid inputs.

	Parameters
	----------
	solvency_ratios : SolvencyRatios
		Instance of SolvencyRatios class

	Verifies
	--------
	No exception raised for finite floats
	"""
	solvency_ratios._validate_float(100.0, "test_value")
	solvency_ratios._validate_float(-100.0, "test_value")


def test_validate_float_non_finite(solvency_ratios: SolvencyRatios) -> None:
	"""Test _validate_float with non-finite inputs.

	Parameters
	----------
	solvency_ratios : SolvencyRatios
		Instance of SolvencyRatios class

	Verifies
	--------
	Raises ValueError for non-finite inputs
	"""
	with pytest.raises(ValueError, match="test_value must be finite"):
		solvency_ratios._validate_float(float("inf"), "test_value")
	with pytest.raises(ValueError, match="test_value must be finite"):
		solvency_ratios._validate_float(float("nan"), "test_value")
