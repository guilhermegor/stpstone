### LIQUIDITTY RISK LIB

class LiquidityRatios:

    def current_ratio(self, float_curr_assets, float_curr_liabilities):
        '''
        REFERENCES: https://www.investopedia.com/terms/l/liquidityratios.asp
        DOCSTRING: MEASURES A COMPANY'S ABILITY TO PAY OFF ITS CURRENT LIABILITIES 
            (PAYABLE WITHIN ONE YEAR) WITH ITS TOTAL CURRENT ASSETS SUCH AS CASH, 
            ACCOUNTS RECEIVABLE, AND INVENTORIES
        INPUTS: CURRENT ASSETS AND LIABILITIES
        OUTPUTS: FLOAT
        '''
        return float_curr_assets / float_curr_liabilities

    def quick_ratio(self, float_curr_assets, float_inventories, float_curr_liabilities):
        '''
        REFERENCES: https://www.investopedia.com/terms/l/liquidityratios.asp
        DOCSTRING: MEASURES A COMPANY'S ABILITY TO MEET ITS SHORT-TERM OBLIGATIONS WITH ITS 
            MOST LIQUID ASSETS AND THEREFORE EXCLUDES INVENTORIES FROM ITS CURRENT ASSETS. 
            IT IS ALSO KNOWN AS THE ACID-TEST RATIO
        INPUTS:
        OUTPUTS: FLOAT
        '''
        return (float_curr_assets - float_inventories) / float_curr_liabilities
    
    def dso(self, float_avg_accounts_rec, float_rev, days=365):
        '''
        REFERENCES: https://www.investopedia.com/terms/l/liquidityratios.asp
        DOCSTRING: DAYS SALES OUTSTANDING (DSO) REFERS TO THE AVERAGE NUMBER OF DAYS IT TAKES A 
            COMPANY TO COLLECT PAYMENT AFTER IT MAKES A SALE. A HIGH DSO MEANS THAT A COMPANY 
            IS TAKING UNDULY LONG TO COLLECT PAYMENT AND IS TYING UP CAPITAL IN RECEIVABLES. 
            DSOS ARE GENERALLY CALCULATED ON A QUARTERLY OR ANNUAL BASIS
        INPUTS:
        OUTPUTS: FLOAT
        '''
        return float(days) / (float_rev / float_avg_accounts_rec)

    def cash_ratio(self, float_cash_eq, float_curr_liabilities):
        '''
        REFERENCES: https://www.toppr.com/guides/principles-and-practice-of-accounting/accounting-ratios/liquidity-ratios/
        DOCSTRING: MEASURES THE ABSOLUTE LIQUIDITY OF THE FIRM. IT MEASURES WHETHER A FIRM 
            CAN PAY THE CURRENT DEBTS BY USING ONLY THE CASH BALANCES, BANK BALANCES AND 
            MARKETABLE SECURITIES
        INPUTS:
        OUTPUTS: FLOAT
        '''
        return float_cash_eq / float_curr_liabilities


class SolvencyRatios:

    def interest_coverage_ratio(self, float_ebit, float_int_exp):
        '''
        REFERENCES: https://www.investopedia.com/terms/s/solvencyratio.asp,
        DOCSTRING: THE INTEREST COVERAGE RATIO MEASURES HOW MANY TIMES A COMPANY CAN COVER 
            ITS CURRENT INTEREST PAYMENTS WITH ITS AVAILABLE EARNINGS. IN OTHER WORDS, 
            IT MEASURES THE MARGIN OF SAFETY A COMPANY HAS FOR PAYING INTEREST ON ITS DEBT 
            DURING A GIVEN PERIOD. THE HIGHER THE RATIO, THE BETTER. IF THE RATIO FALLS TO 
            1.5 OR BELOW, IT MAY INDICATE THAT A COMPANY WILL HAVE DIFFICULTY MEETING 
            THE INTEREST ON ITS DEBTS.
        INPUTS:
        OUTPUTS: FLOAT 
        '''
        return float_ebit / float_int_exp
    
    def debt_to_assets_ratio(self, float_debt, float_assets):
        '''
        REFERENCES: https://www.investopedia.com/terms/s/solvencyratio.asp
        DOCSTRING: THE DEBT-TO-ASSETS RATIO MEASURES A COMPANY'S TOTAL DEBT TO ITS TOTAL ASSETS. 
            IT MEASURES A COMPANY'S LEVERAGE AND INDICATES HOW MUCH OF THE COMPANY IS FUNDED 
            BY DEBT VERSUS ASSETS, AND THEREFORE, ITS ABILITY TO PAY OFF ITS DEBT WITH 
            ITS AVAILABLE ASSETS. A HIGHER RATIO, ESPECIALLY ABOVE 1.0, INDICATES THAT A COMPANY 
            IS SIGNIFICANTLY FUNDED BY DEBT AND MAY HAVE DIFFICULTY MEETINGS ITS OBLIGATIONS.
        INPUTS:
        OUTPUTS:
        '''
        return float_debt / float_assets
    
    def equiy_ratio(self, float_tse, float_assets):
        '''
        REFERENCES: https://www.investopedia.com/terms/s/solvencyratio.asp
        DOCSTRING: THE EQUITY RATIO, OR EQUITY-TO-ASSETS, SHOWS HOW MUCH OF A COMPANY IS FUNDED 
            BY EQUITY AS OPPOSED TO DEBT. THE HIGHER THE NUMBER, THE HEALTHIER A COMPANY IS. 
            THE LOWER THE NUMBER, THE MORE DEBT A COMPANY HAS ON ITS BOOKS RELATIVE TO EQUITY.
        INPUTS: TOTAL SHAREHOLDER'S EQUITY AND TOTAL ASSETS
        OUTPUTS: FLOAT
        '''
        return float_tse / float_assets
    
    def debt_to_equity_ratio(self, float_debt, float_equity):
        '''
        REFERENCES: https://www.investopedia.com/terms/s/solvencyratio.asp
        DOCSTRING: THE D/E RATIO IS SIMILAR TO THE DEBT-TO-ASSETS RATIO, IN THAT IT INDICATES HOW 
            A COMPANY IS FUNDED, IN THIS CASE, BY DEBT. THE HIGHER THE RATIO, THE MORE DEBT 
            A COMPANY HAS ON ITS BOOKS, MEANING THE LIKELIHOOD OF DEFAULT IS HIGHER. THE RATIO 
            LOOKS AT HOW MUCH OF THE DEBT CAN BE COVERED BY EQUITY IF THE COMPANY NEEDED TO LIQUIDATE.
        INPUTS:
        OUTPUTS:
        '''
        return float_debt / float_equity

    def altmans_z_score(self, float_nwk_cap, float_total_assets, float_ret_earnings, float_ebit, 
                        float_mkt_cap, float_total_liabilities, float_sales):
        '''
        REFERENCES: https://medium.com/quant-factory/calculating-altman-z-score-with-python-3c6697ee7aee
        DOCSTRING: THE ALTMAN Z-SCORE QUANTIFIES THE FINANCIAL STABILITY OF A COMPANY TO PREDICT 
            HOW LIKELY A COMPANY WILL BECOME INSOLVENT - A SCORE BELOW 1.8 MEANS IT’S LIKELY THE 
            COMPANY IS HEADED FOR BANKRUPTCY, WHILE COMPANIES WITH SCORES ABOVE 3 ARE NOT LIKELY 
            TO GO BANKRUPT
        INPUTS: WORKING CAPITAL, TOTAL ASSETS, RETAINED EARNINGS, EBIT, MARKET CAPITALISATION, 
            TOTAL LIABILITIES AND SALES 
        OUTPUTS:
        '''
        return 1.2 * (float_nwk_cap / float_total_assets) \
            + 1.4 * (float_ret_earnings / float_total_assets) \
            + 3.3 * (float_ebit / float_total_assets) \
            + 0.6 * (float_mkt_cap / float_total_liabilities) \
            + 1.0 * (float_sales / float_total_assets)