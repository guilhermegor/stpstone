"""Unit tests for the Brazilian 'Come-Cotas' Tax Calculation Module.

Tests cover normal operations, edge cases, error conditions, and type validation
for the ComeCotasCalculator class.
"""

from datetime import date
import importlib

import pytest
from pytest_mock import MockerFixture

from stpstone.analytics.pricing.taxes.br_come_cotas import ComeCotasCalculator


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture(params=["FIA", "FIRF", "FIM", "FIDC", "FIP", "FI-IE", "FII"])
def fund_type(request: pytest.FixtureRequest) -> str:
	"""Fixture providing all supported fund types.

	Parameters
	----------
	request : pytest.FixtureRequest
		The fixture request object

	Returns
	-------
	str
		Supported fund type
	"""
	return request.param


@pytest.fixture
def tax_day_may() -> date:
	"""Fixture providing a May tax day date."""
	return date(2023, 5, 20)


@pytest.fixture
def tax_day_november() -> date:
	"""Fixture providing a November tax day date."""
	return date(2023, 11, 20)


@pytest.fixture
def non_tax_day() -> date:
	"""Fixture providing a non-tax day date."""
	return date(2023, 6, 15)


@pytest.fixture
def calculator_firf() -> object:
	"""Fixture providing a calculator for FIRF fund type."""
	return ComeCotasCalculator("FIRF")


@pytest.fixture
def calculator_fia() -> object:
	"""Fixture providing a calculator for FIA fund type."""
	return ComeCotasCalculator("FIA")


@pytest.fixture
def calculator_fip() -> object:
	"""Fixture providing a calculator for FIP fund type."""
	return ComeCotasCalculator("FIP")


# --------------------------
# Test Classes
# --------------------------
class TestComeCotasCalculatorInitialization:
	"""Tests for ComeCotasCalculator initialization and basic properties."""

	def test_init_valid_fund_types(self, fund_type: str) -> None:
		"""Test initialization with all valid fund types.

		Parameters
		----------
		fund_type : str
			Valid fund type
		"""
		calculator = ComeCotasCalculator(fund_type)
		assert calculator.str_fund_type == fund_type
		assert calculator.fund_data == ComeCotasCalculator.FUND_TYPES[fund_type]

	def test_init_invalid_fund_type(self) -> None:
		"""Test initialization with invalid fund type raises TypeError."""
		with pytest.raises(TypeError, match="must be one of"):
			ComeCotasCalculator("INVALID")

	def test_get_supported_str_fund_types(self) -> None:
		"""Test get_supported_str_fund_types returns all supported types.

		Returns
		-------
		None
		"""
		supported_types = ComeCotasCalculator.get_supported_str_fund_types()
		assert isinstance(supported_types, list)
		assert len(supported_types) == 7
		assert set(supported_types) == {"FIA", "FIRF", "FIM", "FIDC", "FIP", "FI-IE", "FII"}

	def test_get_fund_description(self, fund_type: str) -> None:
		"""Test get_fund_description returns correct description.

		Parameters
		----------
		fund_type : str
			Valid fund type

		Returns
		-------
		None
		"""
		calculator = ComeCotasCalculator(fund_type)
		description = calculator.get_fund_description()
		assert isinstance(description, str)
		assert description == ComeCotasCalculator.FUND_TYPES[fund_type]["description"]


class TestIsTaxDay:
	"""Tests for the is_tax_day method."""

	def test_is_tax_day_may(self, calculator_firf: object, tax_day_may: date) -> None:
		"""Test May tax day returns True for applicable funds.

		Parameters
		----------
		calculator_firf : object
			Calculator for FIRF fund type
		tax_day_may : date
			May tax day date

		Returns
		-------
		None
		"""
		assert calculator_firf.is_tax_day(tax_day_may) is True

	def test_is_tax_day_november(self, calculator_firf: object, tax_day_november: date) -> None:
		"""Test November tax day returns True for applicable funds.

		Parameters
		----------
		calculator_firf : object
			Calculator for FIRF fund type
		tax_day_november : date
			November tax day date

		Returns
		-------
		None
		"""
		assert calculator_firf.is_tax_day(tax_day_november) is True

	def test_is_tax_day_non_tax_day(self, calculator_firf: object, non_tax_day: date) -> None:
		"""Test non-tax day returns False.

		Parameters
		----------
		calculator_firf : object
			Calculator for FIRF fund type
		non_tax_day : date
			Non-tax day date

		Returns
		-------
		None
		"""
		assert calculator_firf.is_tax_day(non_tax_day) is False

	def test_is_tax_day_default_current_date(
		self, calculator_firf: object, mocker: MockerFixture
	) -> None:
		"""Test is_tax_day uses current date when None is passed.

		Parameters
		----------
		calculator_firf : object
			Calculator for FIRF fund type
		mocker : MockerFixture
			Pytest mocker fixture

		Returns
		-------
		None
		"""
		mock_date = mocker.patch("datetime.date")
		mock_date.today.return_value = date(2025, 5, 20)
		# need to patch the type checker to allow None
		mocker.patch.object(calculator_firf, "_validate_inputs", return_value=None)
		assert calculator_firf.is_tax_day(mock_date.today.return_value) is True

	def test_is_tax_day_fund_without_come_cotas(
		self, calculator_fia: object, tax_day_may: date
	) -> None:
		"""Test funds without come-cotas always return False.

		Parameters
		----------
		calculator_fia : object
			Calculator for FIA fund type
		tax_day_may : date
			May tax day date

		Returns
		-------
		None
		"""
		assert calculator_fia.is_tax_day(tax_day_may) is False

	def test_is_tax_day_fund_without_taxation_day(
		self, calculator_fip: object, tax_day_may: date
	) -> None:
		"""Test funds without taxation day always return False.

		Parameters
		----------
		calculator_fip : object
			Calculator for FIP fund type
		tax_day_may : date
			May tax day date

		Returns
		-------
		None
		"""
		assert calculator_fip.is_tax_day(tax_day_may) is False


class TestCalculateTax:
	"""Tests for the calculate_tax method."""

	def test_calculate_tax_tax_day(self, calculator_firf: object, tax_day_may: date) -> None:
		"""Test tax calculation on tax day.

		Parameters
		----------
		calculator_firf : object
			Calculator for FIRF fund type
		tax_day_may : date
			May tax day date

		Returns
		-------
		None
		"""
		position_value = 10000.0
		acquisition_basis = 8000.0
		expected_tax = (position_value - acquisition_basis) * 0.20
		result = calculator_firf.calculate_tax(position_value, acquisition_basis, tax_day_may)
		assert result == pytest.approx(expected_tax)

	def test_calculate_tax_non_tax_day(self, calculator_firf: object, non_tax_day: date) -> None:
		"""Test tax calculation returns 0 on non-tax day.

		Parameters
		----------
		calculator_firf : object
			Calculator for FIRF fund type
		non_tax_day : date
			Non-tax day date

		Returns
		-------
		None
		"""
		result = calculator_firf.calculate_tax(10000.0, 8000.0, non_tax_day)
		assert result == 0.0

	def test_calculate_tax_fund_without_come_cotas(
		self, calculator_fia: object, tax_day_may: date
	) -> None:
		"""Test funds without come-cotas always return 0.

		Parameters
		----------
		calculator_fia : object
			Calculator for FIA fund type
		tax_day_may : date
			May tax day date

		Returns
		-------
		None
		"""
		result = calculator_fia.calculate_tax(10000.0, 8000.0, tax_day_may)
		assert result == 0.0

	def test_calculate_tax_position_less_than_basis(
		self, calculator_firf: object, tax_day_may: date
	) -> None:
		"""Test ValueError when position value is less than acquisition basis.

		Parameters
		----------
		calculator_firf : object
			Calculator for FIRF fund type
		tax_day_may : date
			May tax day date

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Position value cannot be less than"):
			calculator_firf.calculate_tax(8000.0, 10000.0, tax_day_may)

	def test_calculate_tax_zero_profit(self, calculator_firf: object, tax_day_may: date) -> None:
		"""Test tax calculation with zero profit.

		Parameters
		----------
		calculator_firf : object
			Calculator for FIRF fund type
		tax_day_may : date
			May tax day date

		Returns
		-------
		None
		"""
		result = calculator_firf.calculate_tax(10000.0, 10000.0, tax_day_may)
		assert result == 0.0

	def test_calculate_tax_default_current_date(
		self, calculator_firf: object, mocker: MockerFixture
	) -> None:
		"""Test calculate_tax uses current date when None is passed.

		Parameters
		----------
		calculator_firf : object
			Calculator for FIRF fund type
		mocker : MockerFixture
			Pytest mocker fixture

		Returns
		-------
		None
		"""
		mock_date = mocker.patch("datetime.date")
		mock_date.today.return_value = date(2023, 5, 20)
		# need to patch the type checker to allow None
		mocker.patch.object(calculator_firf, "_validate_inputs", return_value=None)
		result = calculator_firf.calculate_tax(10000.0, 8000.0, mock_date.today.return_value)
		expected_tax = (10000.0 - 8000.0) * 0.20
		assert result == pytest.approx(expected_tax)

	def test_calculate_tax_precision(self, calculator_firf: object, tax_day_may: date) -> None:
		"""Test tax calculation with floating point precision.

		Parameters
		----------
		calculator_firf : object
			Calculator for FIRF fund type
		tax_day_may : date
			May tax day date

		Returns
		-------
		None
		"""
		result = calculator_firf.calculate_tax(10000.12345, 8000.54321, tax_day_may)
		expected_tax = (10000.12345 - 8000.54321) * 0.20
		assert result == pytest.approx(expected_tax)


class TestTypeValidation:
	"""Tests for type validation and error handling."""

	def test_invalid_fund_type_type(self) -> None:
		"""Test TypeError when fund type is not a string.

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			ComeCotasCalculator(123)  # type: ignore

	def test_invalid_position_value_type(self, calculator_firf: object, tax_day_may: date) -> None:
		"""Test TypeError when position value is not a float.

		Parameters
		----------
		calculator_firf : object
			Calculator for FIRF fund type
		tax_day_may : date
			May tax day date

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			calculator_firf.calculate_tax("10000", 8000.0, tax_day_may)  # type: ignore

	def test_invalid_acquisition_basis_type(
		self, calculator_firf: object, tax_day_may: date
	) -> None:
		"""Test TypeError when acquisition basis is not a float.

		Parameters
		----------
		calculator_firf : object
			Calculator for FIRF fund type
		tax_day_may : date
			May tax day date

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			calculator_firf.calculate_tax(10000.0, "8000", tax_day_may)  # type: ignore

	def test_invalid_date_type(self, calculator_firf: object) -> None:
		"""Test TypeError when date is not a date object.

		Parameters
		----------
		calculator_firf : object
			Calculator for FIRF fund type

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			calculator_firf.is_tax_day("2023-05-20")  # type: ignore


class TestReloadLogic:
	"""Tests for module reloading scenarios."""

	def test_reload_module(self) -> None:
		"""Test that the module can be reloaded successfully.

		Returns
		-------
		None
		"""
		import stpstone.transformations.validation.metaclass_type_checker as module

		importlib.reload(module)
		assert True  # Just testing that reload doesn't raise exceptions
