"""Module for auditing earnings manipulation using the Beneish M-Score model.

The Beneish M-Score is a probabilistic model that identifies companies that may be manipulating
their earnings. It uses eight financial ratios to detect manipulation patterns.
"""

import math
from typing import TypedDict

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class ResultInputs(TypedDict):
    """Inputs for the Beneish M-Score model."""

    float_dsr: float
    float_gmi: float
    float_aqi: float
    float_sgi: float
    float_depi: float
    float_sgai: float
    float_tata: float
    float_lvgi: float

class EarningsManipulation(metaclass=TypeChecker):
    """Detect earnings manipulation using the Beneish M-Score model.

    The Beneish model calculates a score that indicates the likelihood of earnings manipulation
    based on eight financial ratios derived from company financial statements.

    References
    ----------
    Beneish, M. D. (1999). The detection of earnings manipulation.
    Financial Analysts Journal, 55(5), 24-36.
    """

    def inputs_beneish_model(
        self,
        float_ar_t: float,
        float_sales_t: float,
        float_ar_tm1: float,
        float_sales_tm1: float,
        float_gp_tm1: float,
        float_gp_t: float,
        float_ppe_t: float,
        float_ca_t: float,
        float_lti_t: float,
        float_lti_tm1: float,
        float_ta_t: float,
        float_ppe_tm1: float,
        float_ca_tm1: float,
        float_ta_tm1: float,
        float_dep_tm1: float,
        float_dep_t: float,
        float_sga_t: float,
        float_sga_tm1: float,
        float_inc_cont_op: float,
        float_cfo_t: float,
        float_tl_t: float,
        float_tl_tm1: float,
    ) -> ResultInputs:
        """Compute the eight financial ratios used in the Beneish M-Score model.

        Parameters
        ----------
        float_ar_t : float
            Accounts receivable in current period
        float_sales_t : float
            Total sales in current period
        float_ar_tm1 : float
            Accounts receivable in previous period
        float_sales_tm1 : float
            Total sales in previous period
        float_gp_tm1 : float
            Gross profit in previous period
        float_gp_t : float
            Gross profit in current period
        float_ppe_t : float
            Property, plant & equipment in current period
        float_ca_t : float
            Current assets in current period
        float_lti_t : float
            Long-term investments in current period
        float_lti_tm1 : float
            Long-term investments in previous period
        float_ta_t : float
            Total assets in current period
        float_ppe_tm1 : float
            Property, plant & equipment in previous period
        float_ca_tm1 : float
            Current assets in previous period
        float_ta_tm1 : float
            Total assets in previous period
        float_dep_tm1 : float
            Depreciation in previous period
        float_dep_t : float
            Depreciation in current period
        float_sga_t : float
            SG&A expenses in current period
        float_sga_tm1 : float
            SG&A expenses in previous period
        float_inc_cont_op : float
            Income from continuing operations
        float_cfo_t : float
            Cash flow from operations
        float_tl_t : float
            Total liabilities in current period
        float_tl_tm1 : float
            Total liabilities in previous period

        Returns
        -------
        ResultInputs
        """
        self._validate_inputs(locals())
        return {
            "float_dsr": (float_ar_t / float_sales_t) / (float_ar_tm1 / float_sales_tm1),
            "float_gmi": (float_gp_tm1 / float_sales_tm1) / (float_gp_t / float_sales_t),
            "float_aqi": (1 - (float_ppe_t + float_ca_t + float_lti_t) / float_ta_t)
            / (1 - (float_ppe_tm1 + float_ca_tm1 + float_lti_tm1) / float_ta_tm1),
            "float_sgi": float_sales_t / float_sales_tm1,
            "float_depi": (float_dep_tm1 / (float_ppe_tm1 + float_dep_tm1))
            / (float_dep_t / (float_ppe_t + float_dep_t)),
            "float_sgai": (float_sga_t / float_sales_t) / (float_sga_tm1 / float_sales_tm1),
            "float_tata": (float_inc_cont_op - float_cfo_t) / float_ta_t,
            "float_lvgi": (float_tl_t / float_ta_t) / (float_tl_tm1 / float_ta_tm1),
        }

    def _validate_inputs(self, params: dict) -> None:
        """Validate all input parameters for the Beneish model.
        
        Parameters
        ----------
        params : dict
            Dictionary of parameter names and values from locals()
            
        Raises
        ------
        ValueError
            If any parameter contains NaN or infinity
        """
        for param_name, param_value in params.items():
            if param_name == 'self':
                continue
                
            if isinstance(param_value, float):
                if math.isnan(param_value):
                    raise ValueError(f"Parameter {param_name} contains NaN value")
                if math.isinf(param_value):
                    raise ValueError(f"Parameter {param_name} contains infinity value")

    def beneish_model(
        self,
        float_dsr: float,
        float_gmi: float,
        float_aqi: float,
        float_sgi: float,
        float_depi: float,
        float_sgai: float,
        float_tata: float,
        float_lvgi: float,
    ) -> float:
        """Calculate the Beneish M-Score to assess earnings manipulation likelihood.

        The M-Score is calculated as a linear combination of eight financial ratios.
        A score > -1.78 suggests likely earnings manipulation.

        Parameters
        ----------
        float_dsr : float
            Days' sales in receivable index
        float_gmi : float
            Gross margin index
        float_aqi : float
            Asset quality index
        float_sgi : float
            Sales growth index
        float_depi : float
            Depreciation index
        float_sgai : float
            SG&A expense index
        float_tata : float
            Total accruals to total assets
        float_lvgi : float
            Leverage index

        Returns
        -------
        float
            The Beneish M-Score. Interpretation:
            - > -1.78: High probability of earnings manipulation
            - ≤ -1.78: Low probability of earnings manipulation

        Notes
        -----
        The coefficients are from the original Beneish (1999) paper:
        M = -4.84 + 0.920*DSR + 0.528*GMI + 0.404*AQI + 0.892*SGI 
            + 0.115*DEPI - 0.172*SGAI + 4.679*TATA - 0.327*LVGI
        """
        return (
            -4.84
            + 0.920 * float_dsr
            + 0.528 * float_gmi
            + 0.404 * float_aqi
            + 0.892 * float_sgi
            + 0.115 * float_depi
            - 0.172 * float_sgai
            + 4.679 * float_tata
            - 0.327 * float_lvgi
        )