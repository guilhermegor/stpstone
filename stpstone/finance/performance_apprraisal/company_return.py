### MODULE TO HANDLE A COMPANY PERFORMACE APPRAISAL

class CompanyReturnAppraisal:

    def dupont_analysis(self, float_ni, float_net_revenue, float_avg_ta, float_avg_te, 
                        float_ebt, float_ebit):
        """
        REFERENCES: https://www.wallstreetprep.com/knowledge/dupont-analysis-template/
        DOCSTRING: DUPONT ANALYSIS IN THREE AND FIVE-STEPS
        INPUTS: NET INCOME, NET REVENUE, AVERAGE TOTAL ASSETS, AVERAGE TOTAL SHAREHOLDER'S EQUITY, 
            EARNS BEFORE TAXES (EBT), EARNS BEFORE EARNS, INTERESTS AND TAXES (EBIT) - IN A 
            5-STEP DUPONT ANALYSIS, THE MULTIPLYING FACTORS ARE:
            1. TAX BRUDEN (NI / EBT) --> THE LOWER RATIO, THE HIGHER THE BURDEN IS
            2. INTEREST BURDEN (EBT / EBIT) --> THE LOWER RATIO, THE HIGHER THE BURDEN IS
            3. OPERATING MARGIN (EBIT / NET REVENUE)
            4. ASSET TURNOVER (NET REVENUE / AVERAGE TOTAL ASSETS)
            5. FINANCIAL LEVERAGE RATIO (AVERAGE TOTAL ASSETS / AVERAGE SHAREHOLDER'S EQUITY)
        OUTPUTS: DICITONARY WITH 3-STEPS AND 5-STEPS DUPONT ROE
        """
        return {
            '3_steps_dupont_roe': float_ni / float_net_revenue * float_net_revenue / float_avg_ta \
                * float_avg_ta / float_avg_te, 
            '5_steps_dupont_roe': float_ni / float_ebt * float_ebt / float_ebit \
                * float_ebit / float_net_revenue * float_net_revenue / float_avg_ta \
                * float_avg_ta / float_avg_te
        }