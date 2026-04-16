"""Capital Requirement 1 (CR1) calculation for financial instruments.

This module provides a class to calculate Capital Requirement 1 (CR1) based on
Exposure at Default (EAD), Probability of Default (PD), and Loss Given Default (LGD).
It includes input validation and follows strict numerical bounds checking.
"""

from typing import TypedDict

import numpy as np

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


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


class CR1Calculator(metaclass=TypeChecker):
	"""Calculator for Capital Requirement 1 (CR1) of financial instruments.

	Parameters
	----------
	float_ead : float
		Exposure at Default, must be non-negative
	float_pd : float
		Probability of Default, must be in [0, 1]
	float_lgd : float
		Loss Given Default, must be in [0, 1]

	Attributes
	----------
	float_ead : float
		Exposure at Default value
	float_pd : float
		Probability of Default value
	float_lgd : float
		Loss Given Default value
	"""

	def __init__(self, float_ead: float, float_pd: float, float_lgd: float) -> None:
		"""Initialize CR1Calculator.

		Parameters
		----------
		float_ead : float
			Exposure at Default
		float_pd : float
			Probability of Default
		float_lgd : float
			Loss Given Default

		Returns
		-------
		None
		"""
		self._validate_inputs(float_ead, float_pd, float_lgd)
		self.float_ead = float_ead
		self.float_pd = float_pd
		self.float_lgd = float_lgd

	def _validate_inputs(self, float_ead: float, float_pd: float, float_lgd: float) -> None:
		"""Validate input parameters for CR1 calculation.

		Parameters
		----------
		float_ead : float
			Exposure at Default to validate
		float_pd : float
			Probability of Default to validate
		float_lgd : float
			Loss Given Default to validate

		Raises
		------
		ValueError
			If EAD is negative, or PD/LGD is outside [0, 1]
		TypeError
			If inputs are not numeric
		"""
		if not isinstance(float_ead, (int, float)):
			raise TypeError("Exposure at Default must be numeric")
		if not isinstance(float_pd, (int, float)):
			raise TypeError("Probability of Default must be numeric")
		if not isinstance(float_lgd, (int, float)):
			raise TypeError("Loss Given Default must be numeric")

		if float_ead < 0:
			raise ValueError(f"Exposure at Default must be non-negative, got {float_ead}")
		if not 0 <= float_pd <= 1:
			raise ValueError(f"Probability of Default must be in [0, 1], got {float_pd}")
		if not 0 <= float_lgd <= 1:
			raise ValueError(f"Loss Given Default must be in [0, 1], got {float_lgd}")

		if not np.isfinite(float_ead):
			raise ValueError("Exposure at Default must be finite")
		if not np.isfinite(float_pd):
			raise ValueError("Probability of Default must be finite")
		if not np.isfinite(float_lgd):
			raise ValueError("Loss Given Default must be finite")

	def calculate_k(self) -> float:
		"""Calculate capital requirement factor K.

		Returns
		-------
		float
			Product of Loss Given Default and Probability of Default

		Notes
		-----
		K = LGD * PD
		"""
		return self.float_lgd * self.float_pd

	def calculate_cr1(self) -> float:
		"""Calculate Capital Requirement 1 (CR1).

		Returns
		-------
		float
			Capital Requirement 1 calculated as 12.5 * K * EAD

		Raises
		------
		ValueError
			If CR1 calculation results in non-finite value

		Notes
		-----
		CR1 = 12.5 * K * EAD, where K = LGD * PD
		"""
		k = self.calculate_k()
		result = 12.5 * k * self.float_ead
		if not np.isfinite(result):
			raise ValueError("CR1 calculation resulted in non-finite value")
		return result

	def summary(self) -> ReturnSummary:
		"""Generate summary of CR1 calculation parameters and results.

		Returns
		-------
		ReturnSummary
			Dictionary containing EAD, PD, LGD, K, and CR1 values
		"""
		k = self.calculate_k()
		cr1 = self.calculate_cr1()
		return {
			"EAD": self.float_ead,
			"PD": self.float_pd,
			"LGD": self.float_lgd,
			"K": k,
			"CR1": cr1,
		}
