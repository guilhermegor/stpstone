### AUDITING EARNINGS MANIPULATION ###

from typing import Dict


class EarningsManipulation:

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
    ) -> Dict[str, float]:
        """
        Computes financial ratios used in the Beneish M-Score model.
        Args:
            float_ar_t (float):Adjusted revenue for the period
            float_sales_t (float):Total sales for the period
            float_ar_tm1 (float):Adjusted revenue for the previous period
            float_sales_tm1 (float):Total sales for the previous period
            float_gp_tm1 (float):Gross profit for the previous period
            float_gp_t (float):Gross profit for the period
            float_ppe_t (float):Property, plant, and equipment for the period
            float_ca_t (float):Current assets for the period
            float_lti_t (float):Long-term investments for the period
            float_lti_tm1 (float):Long-term investments for the previous period
            float_ta_t (float):Total assets for the period
            float_ppe_tm1 (float):Property, plant, and equipment for the previous period
            float_ca_tm1 (float):Current assets for the previous period
            float_ta_tm1 (float):Total assets for the previous period
            float_dep_tm1 (float):Depreciation for the previous period
            float_dep_t (float):Depreciation for the period
            float_sga_t (float):Selling, general, and administrative expenses for the period
            float_sga_tm1 (float):Selling, general, and administrative expenses for the previous period
            float_inc_cont_op (float):Income from continuing operations for the period
            float_cfo_t (float):Cash from operating activities for the period
            float_tl_t (float):Total liabilities for the period
            float_tl_tm1 (float):Total liabilities for the previous period
        Returns:
            Dict[str, float]:Dictionary of financial ratios
        """
        return {
            "float_dsr": (float_ar_t / float_sales_t)
            / (float_ar_tm1 / float_sales_tm1),
            "float_gmi": (float_gp_tm1 / float_sales_tm1)
            / (float_gp_t / float_sales_t),
            "float_aqi": (1.0 - (float_ppe_t + float_ca_t + float_lti_t) / float_ta_t)
            / (1.0 - (float_ppe_tm1 + float_ca_tm1 + float_lti_tm1) / float_ta_tm1),
            "float_sgi": float_sales_t / float_sales_tm1,
            "float_depi": (float_dep_tm1 / (float_ppe_tm1 + float_dep_tm1))
            / (float_dep_t / (float_ppe_t + float_dep_t)),
            "float_sgai": (float_sga_t / float_sales_t)
            / (float_sga_tm1 / float_sales_tm1),
            "float_tata": (float_inc_cont_op - float_cfo_t) / float_ta_t,
            "float_lvgi": (float_tl_t / float_ta_t) / (float_tl_tm1 / float_ta_tm1),
        }

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
        """
        Computes the Beneish M-Score to assess the likelihood of earnings manipulation.
        Args:
            float_dsr (float):Diluted shares outstanding ratio
            float_gmi (float):Gross margin improvement
            float_aqi (float):Asset quality improvement
            float_sgi (float):Sales growth improvement
            float_depi (float):Depreciation improvement
            float_sgai (float):Selling, general, and administrative expenses improvement
            float_tata (float):Total assets to total assets
            float_lvgi (float):Long-term debt to total assets
        Returns:
            float: Beneish M-Score
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
