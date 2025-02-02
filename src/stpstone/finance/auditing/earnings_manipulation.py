### EARNINGS MANIPULATION AUDITING


class EarningsManipulation:
    def inputs_beneish_model(
        self,
        float_ar_t,
        float_sales_t,
        float_ar_tm1,
        float_sales_tm1,
        float_gp_tm1,
        float_gp_t,
        float_ppe_t,
        float_ca_t,
        float_lti_t,
        float_lti_tm1,
        float_ta_t,
        float_ppe_tm1,
        float_ca_tm1,
        float_ta_tm1,
        float_dep_tm1,
        float_dep_t,
        float_sga_t,
        float_sga_tm1,
        float_inc_cont_op,
        float_cfo_t,
        float_tl_t,
        float_tl_tm1,
    ):
        """
        REFERENCES: https://corporatefinanceinstitute.com/resources/financial-modeling/beneish-m-score-calculator/
        DOCSTRING: FINANCIAL INPUTS FOR BENEISH M-SCORE MODEL
        INPUTS: ACCOUNTS RECEIVABLES T, SALES T, ACCOUNTS RECEIVABLES T-1, SALES T-1, GROSS PROFIT
            T-1 (SALES - COGS), GROSS PROFIT T, PPE T, CURRENT ASSETS T, LONG-TERM INVESTMENTS T
            (SECURITIES), LONG-TERM INVESTMENTS T-1, TOTAL ASSETS T, PPE T-1, CURRENT ASSETS T-1,
            TOTAL ASSETS T-1, DEPRECIATION T-1, DEPRECIATION T, SG&A T, SG&A T-1,
            INCOME FROM CONTINUOUS OPERATIONS, CASH FLOW FROM OPERATIONS T
        OUTPUTS: DICTIONARY
        """
        return {
            'float_dsr': (float_ar_t / float_sales_t)
            / (float_ar_tm1 / float_sales_tm1),
            'float_gmi': (float_gp_tm1 / float_sales_tm1)
            / (float_gp_t / float_sales_t),
            'float_aqi': (
                1.0 - (float_ppe_t + float_ca_t + float_lti_t) / float_ta_t
            )
            / (
                1.0
                - (float_ppe_tm1 + float_ca_tm1 + float_lti_tm1) / float_ta_tm1
            ),
            'float_sgi': float_sales_t / float_sales_tm1,
            'float_depi': (float_dep_tm1 / (float_ppe_tm1 + float_dep_tm1))
            / (float_dep_t / (float_ppe_t + float_dep_t)),
            'float_sgai': (float_sga_t / float_sales_t)
            / (float_sga_tm1 / float_sales_tm1),
            'float_tata': (float_inc_cont_op - float_cfo_t) / float_ta_t,
            'float_lvgi': (float_tl_t / float_ta_t)
            / (float_tl_tm1 / float_ta_tm1),
        }

    def beneish_model(
        self,
        float_dsr,
        float_gmi,
        float_aqi,
        float_sgi,
        float_depi,
        float_sgai,
        float_tata,
        float_lvgi,
    ):
        """
        REFERENCES: https://corporatefinanceinstitute.com/resources/financial-modeling/beneish-m-score-calculator/
        DOCSTRING: BENEISH M-SCORE TO CHECK EARNINGS MANIPULATION - Z-SCORE VALUES GREATER THAN
            -1.78 SHOULD RAISE A WARNING SIGN
        INPUTS: DAYS SALES RECEIVABLES INDEX (DSRI), GROSS MARGING INDEX (GMI), ASSET QUALITY
            INDEX (AQI), SALES GROWTH INDEX (SGI), DEPRECIATION INDEX (DEPI), SELLING, GENERAL &
            ADMINISTRATIVE EXPENSES INDEX (SGAI), TOTAL ACCRUALS TO TOTAL ASSETS (TATA),
            LEVERAGE INDEX (LVGI)
        OUTPUTS: FLOAT
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
