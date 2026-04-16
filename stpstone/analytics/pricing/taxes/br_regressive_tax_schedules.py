"""Tax Calculator for Brazilian Sovereign Bonds.

This module provides a class to calculate IOF and IR taxes based on Brazil's
regressive tax tables, following the country's specific financial regulations.
"""

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class BRRegressiveTaxSchedules(metaclass=TypeChecker):
	"""A class to calculate IOF and IR taxes for Brazilian financial operations.

	The calculator implements Brazil's regressive tax tables for:
	- IOF (Tax on Financial Operations)
	- IR (Income Tax on investments)

	Parameters
	----------
	None
		The tax tables are hardcoded based on Brazilian regulations.

	Attributes
	----------
	iof_table : dict[int, float]
		Regressive IOF tax rates by days since investment (0-30 days)
	ir_table : dict[int, float]
		Regressive IR tax rates by investment duration brackets

	Examples
	--------
	>>> calculator = BRRegressiveTaxSchedules()
	>>> iof, ir, total = calculator.calculate_total_taxes(
	...     int_cddt=200,
	...     int_cddr=10,
	...     float_notional=10000.00
	... )
	>>> print(f"Total taxes: R${total:.2f}")
	"""

	def __init__(self) -> None:
		"""Initialize the tax tables for IOF and IR based on Brazilian regulations."""
		# IOF table: days since investment -> tax float_rate (0-100%)  # noqa: ERA001
		self.iof_table: dict[int, float] = {
			1: 0.96,
			2: 0.93,
			3: 0.90,
			4: 0.86,
			5: 0.83,
			6: 0.80,
			7: 0.76,
			8: 0.73,
			9: 0.70,
			10: 0.66,
			11: 0.63,
			12: 0.60,
			13: 0.56,
			14: 0.53,
			15: 0.50,
			16: 0.46,
			17: 0.43,
			18: 0.40,
			19: 0.36,
			20: 0.33,
			21: 0.30,
			22: 0.26,
			23: 0.23,
			24: 0.20,
			25: 0.16,
			26: 0.13,
			27: 0.10,
			28: 0.06,
			29: 0.03,
			30: 0.00,
		}

		# IR table: days invested -> tax float_rate (0-100%)  # noqa: ERA001
		self.ir_table: dict[int, float] = {1: 22.5, 180: 20.0, 361: 17.5, 721: 15.0}

	def calculate_iof_tax(self, int_cddr: int, float_notional: float) -> float:
		"""Calculate IOF tax based on days since investment.

		Parameters
		----------
		int_cddr : int
			Number of days since the investment was made (0-30)
		float_notional : float
			The taxable float_notional in BRL

		Returns
		-------
		float
			The calculated IOF tax float_notional

		Raises
		------
		ValueError
			If int_cddr is negative

		Notes
		-----
		- IOF only applies to redemptions within 30 days
		- The float_rate decreases progressively each day
		"""
		if int_cddr < 0:
			raise ValueError("Days since investment cannot be negative")

		float_rate = self.iof_table.get(min(int_cddr, 30), 0.0)
		return float_notional * float_rate / 100

	def calculate_ir_tax(self, int_cddt: int, float_notional: float) -> float:
		"""Calculate IR tax based on investment duration.

		Parameters
		----------
		int_cddt : int
			Number of days the investment was held
		float_notional : float
			The taxable float_notional in BRL

		Returns
		-------
		float
			The calculated IR tax float_notional

		Raises
		------
		ValueError
			If int_cddt is negative

		Notes
		-----
		- Tax rates follow Brazilian progressive brackets:
			- Up to 180 days: 22.5%
			- 181-360 days: 20%
			- 361-720 days: 17.5%
			- 721+ days: 15%
		"""
		if int_cddt <= 0:
			raise ValueError("Days invested must be positive")

		float_rate = 22.5  # default float_rate (up to 180 days)
		for int_threshold_cd, float_threshold_rate in sorted(self.ir_table.items()):
			if int_cddt >= int_threshold_cd:
				float_rate = float_threshold_rate
			else:
				break

		return float_notional * float_rate / 100

	def calculate_total_taxes(
		self, int_cddt: int, int_cddr: int, float_notional: float
	) -> tuple[float, float, float]:
		"""Calculate both IOF and IR taxes with the combined total.

		Parameters
		----------
		int_cddt : int
			Total duration of investment in calendar days
		int_cddr : int
			Days since investment when redeemed (for IOF calculation)
		float_notional : float
			The taxable float_notional in BRL

		Returns
		-------
		tuple[float, float, float]
			A tuple containing (iof_tax, ir_tax, total_tax)

		Notes
		-----
		- int_cddr is capped at 30 for IOF calculation
		- int_cddt determines the IR tax bracket
		"""
		iof_tax = self.calculate_iof_tax(int_cddr, float_notional)
		ir_tax = self.calculate_ir_tax(int_cddt, float_notional)
		return (iof_tax, ir_tax, iof_tax + ir_tax)

	def get_iof_rate(self, int_cddr: int) -> float:
		"""Get the IOF float_rate for specific days since investment.

		Parameters
		----------
		int_cddr : int
			Days since investment (0-30)

		Returns
		-------
		float
			The applicable IOF float_rate in percentage
		"""
		return self.iof_table.get(min(int_cddr, 30), 0.0)

	def get_ir_rate(self, int_cddt: int) -> float:
		"""Get the IR float_rate for specific investment duration.

		Parameters
		----------
		int_cddt : int
			Total investment duration in days

		Returns
		-------
		float
			The applicable IR float_rate in percentage

		Notes
		-----
		Follows Brazil's progressive tax brackets for investments
		"""
		float_rate = 22.5  # default float_rate
		for int_threshold_cd, float_threshold_rate in sorted(self.ir_table.items()):
			if int_cddt >= int_threshold_cd:
				float_rate = float_threshold_rate
			else:
				break
		return float_rate
