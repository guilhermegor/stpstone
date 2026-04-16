"""Module for ROE decomposition using DuPont Analysis."""

from typing import TypedDict

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class ResultDupontAnalysis(TypedDict):
	"""Typed dictionary for holding ROE decomposition results."""

	roe_3_step: float
	roe_5_step: float
	intermediate_metrics: dict[str, float]


class ROEDecomposition(metaclass=TypeChecker):
	"""A class to perform ROE decomposition using DuPont Analysis (3-step and 5-step)."""

	def dupont_analysis(
		self,
		float_ni: float,
		float_net_revenue: float,
		float_avg_ta: float,
		float_avg_te: float,
		float_ebt: float,
		float_ebit: float,
	) -> ResultDupontAnalysis:
		"""
		Perform DuPont Analysis (3-step and 5-step) to decompose ROE.

		Parameters
		----------
		float_ni : float
			Net Income
		float_net_revenue : float
			Net Revenue
		float_avg_ta : float
			Average Total Assets
		float_avg_te : float
			Average Total Shareholder's Equity
		float_ebt : float
			Earnings Before Taxes (EBT)
		float_ebit : float
			Earnings Before Interest and Taxes (EBIT)

		Returns
		-------
		ResultDupontAnalysis
			A dictionary containing:
			- 3-step DuPont ROE
			- 5-step DuPont ROE
			- Intermediate metrics (Net Profit Margin, Asset Turnover, etc.)

		Raises
		------
		ValueError
			If any of the inputs are non-positive or zero.
		"""
		# input validation
		if any(
			x <= 0 for x in [float_net_revenue, float_avg_ta, float_avg_te, float_ebt, float_ebit]
		):
			raise ValueError("Inputs must be positive and non-zero.")
		# 3-Step DuPont Analysis
		float_net_profit_margin = float_ni / float_net_revenue
		float_asset_turnover = float_net_revenue / float_avg_ta
		float_equity_multiplier = float_avg_ta / float_avg_te
		float_roe_3_step = float_net_profit_margin * float_asset_turnover * float_equity_multiplier
		# 5-Step DuPont Analysis
		float_tax_burden = float_ni / float_ebt
		float_interest_burden = float_ebt / float_ebit
		float_opeating_margin = float_ebit / float_net_revenue
		roe_5_step = (
			float_tax_burden
			* float_interest_burden
			* float_opeating_margin
			* float_asset_turnover
			* float_equity_multiplier
		)
		# intermediate metrics
		dict_intermediate_metrics = {
			"Net Profit Margin": float_net_profit_margin,
			"Asset Turnover": float_asset_turnover,
			"Equity Multiplier": float_equity_multiplier,
			"Tax Burden": float_tax_burden,
			"Interest Burden": float_interest_burden,
			"Operating Margin": float_opeating_margin,
		}
		return {
			"roe_3_step": float_roe_3_step,
			"roe_5_step": roe_5_step,
			"intermediate_metrics": dict_intermediate_metrics,
		}
