"""Bond duration calculations including Macaulay, modified, and dollar durations.

This module provides various bond duration calculations using cash flows and yield-to-maturity.
It implements duration measures including Macaulay, modified, dollar, effective durations,
and convexity calculations.

References
----------
.. [1] https://corporatefinanceinstitute.com/resources/fixed-income/duration/
"""

from typing import Literal

import numpy as np
from numpy.typing import NDArray

from stpstone.analytics.perf_metrics.financial_math import FinancialMath
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class BondDuration(FinancialMath, metaclass=TypeChecker):
	"""Calculate various bond duration measures.

	Parameters
	----------
	array_cfs : NDArray[np.float64]
		List of cash flows
	float_ytm : float
		Yield to maturity (must be between 0 and 1)
	float_fv : float
		Face value of the bond
	str_when : Literal['end', 'begin']
		When payments are made (default: "end")

	Raises
	------
	ValueError
		If yield to maturity is not between 0 and 1
		If cash flows list is empty
	"""

	def __init__(
		self,
		array_cfs: NDArray[np.float64],
		float_ytm: float,
		float_fv: float,
		str_when: Literal["end", "begin"] = "end",
	) -> None:
		"""Initialize BondDuration object.

		Parameters
		----------
		array_cfs : NDArray[np.float64]
			List of cash flows
		float_ytm : float
			Yield to maturity (must be between 0 and 1)
		float_fv : float
			Face value of the bond
		str_when : Literal['end', 'begin']
			When payments are made (default: "end")
		"""
		self._validate_inputs(array_cfs, float_ytm)
		self.array_cfs = array_cfs
		self.float_ytm = float_ytm
		self.float_fv = float_fv
		self.str_when = str_when

	def _validate_inputs(self, array_cfs: NDArray[np.float64], float_ytm: float) -> None:
		"""Validate input parameters.

		Parameters
		----------
		array_cfs : NDArray[np.float64]
			Cash flows to validate
		float_ytm : float
			Yield to maturity to validate

		Raises
		------
		ValueError
			If cash flows list is empty
			If yield to maturity is not between 0 and 1
		"""
		if array_cfs.size == 0:
			raise ValueError("Cash flows list cannot be empty")
		if not 0 <= float_ytm <= 1:
			raise ValueError(f"Yield to maturity must be between 0 and 1, got {float_ytm}")

	def _validate_yield(self, float_y: float) -> None:
		"""Validate yield parameter.

		Parameters
		----------
		float_y : float
			Yield to validate

		Raises
		------
		ValueError
			If yield is not between 0 and 1
		"""
		if not 0 <= float_y <= 1:
			raise ValueError(f"Yield must be between 0 and 1, got {float_y}")

	def macaulay(self) -> float:
		"""Calculate Macaulay duration.

		Returns
		-------
		float
			Macaulay duration of the bond

		Notes
		-----
		Macaulay duration is the weighted average time to receive cash flows.
		"""
		array_nper, array_discounted_cfs = self.pv_cfs(
			self.array_cfs, self.float_ytm, str_capitalization="simple", str_when="end"
		)
		float_pv = np.sum(array_discounted_cfs)
		return np.sum(array_nper * array_discounted_cfs) / float_pv

	def modified(self, float_y: float, int_n: int) -> float:
		"""Calculate modified duration.

		Parameters
		----------
		float_y : float
			Yield discount rate (must be between 0 and 1)
		int_n : int
			Capitalization periods per year (must be positive)

		Returns
		-------
		float
			Modified duration of the bond

		Raises
		------
		ValueError
			If yield is not between 0 and 1
			If capitalization periods is not positive
		"""
		self._validate_yield(float_y)
		if int_n <= 0:
			raise ValueError(f"Capitalization periods must be positive, got {int_n}")

		float_macaulay_duration = self.macaulay()
		return float_macaulay_duration / (1 + float_y / int_n)

	def dollar(self, float_y: float, int_n: int) -> float:
		"""Calculate dollar duration.

		Parameters
		----------
		float_y : float
			Yield discount rate (must be between 0 and 1)
		int_n : int
			Capitalization periods per year (must be positive)

		Returns
		-------
		float
			Dollar duration of the bond
		"""
		_, array_discounted_cfs = self.pv_cfs(
			self.array_cfs, self.float_ytm, str_capitalization="simple", str_when="end"
		)
		float_pv0 = np.sum(array_discounted_cfs)
		return -self.modified(float_y, int_n) * float_pv0

	def _params_pv(self, float_delta_y: float) -> tuple[float, float, float]:
		"""Parameters of PV for bond duration calculations.

		Parameters
		----------
		float_delta_y : float
			Yield change (must be positive)

		Returns
		-------
		tuple[float, float, float]
			Parameters for bond duration calculations

		Raises
		------
		ValueError
			If yield change is not positive
		"""
		if float_delta_y <= 0:
			raise ValueError(f"Yield change must be positive, got {float_delta_y}")

		array_nper = np.arange(1, len(self.array_cfs) + 1)
		float_pv0 = np.sum(
			[self.pv(self.float_ytm, t, cf) for t, cf in zip(array_nper, self.array_cfs)]
		)

		float_pv_minus = np.sum(
			[
				self.pv(self.float_ytm - float_delta_y, int_t, float_cf)
				for int_t, float_cf in zip(array_nper, self.array_cfs)
			]
		)
		float_pv_plus = np.sum(
			[
				self.pv(self.float_ytm + float_delta_y, int_t, float_cf)
				for int_t, float_cf in zip(array_nper, self.array_cfs)
			]
		)

		return float_pv0, float_pv_minus, float_pv_plus

	def effective(self, float_delta_y: float) -> float:
		"""Calculate effective duration.

		Parameters
		----------
		float_delta_y : float
			Yield change (must be positive)

		Returns
		-------
		float
			Effective duration of the bond
		"""
		float_pv0, float_pv_minus, float_pv_plus = self._params_pv(float_delta_y)
		return (float_pv_minus - float_pv_plus) / (2 * float_delta_y * float_pv0)

	def convexity(self, float_delta_y: float) -> float:
		"""Calculate bond convexity.

		Parameters
		----------
		float_delta_y : float
			Yield change (must be positive)

		Returns
		-------
		float
			Convexity of the bond
		"""
		float_pv0, float_pv_minus, float_pv_plus = self._params_pv(float_delta_y)
		return (float_pv_minus + float_pv_plus - 2 * float_pv0) / (float_pv0 * float_delta_y**2)

	def dv_y(self, float_y: float, int_n: int, float_delta_y: float = 0.0001) -> float:
		"""Calculate dollar value for a yield change (DV01 for 1bps change).

		Parameters
		----------
		float_y : float
			Yield discount rate (must be between 0 and 1)
		int_n : int
			Capitalization periods per year (must be positive)
		float_delta_y : float
			Yield change (default: 1E-4, must be positive)

		Returns
		-------
		float
			Dollar value for the given yield change

		Raises
		------
		ValueError
			If yield change is not positive
		"""
		if float_delta_y <= 0:
			raise ValueError(f"Yield change must be positive, got {float_delta_y}")

		return self.dollar(float_y, int_n) * float_delta_y
