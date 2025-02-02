### FINANCIAL MATH LIB ###

import numpy_financial as npf


class FinancialMath:
    def compound_interest(
        self, nominal_interest_rate, nominal_nper, real_nper
    ):
        """
        DOCSTRING: COMPOUND INTEREST (NOMINAL FEE)
        INPUTS: NOMINAL FEE, NPER REALTED TO NOMINAL FEE AND NPER REGARDING THE REAL FEE
            FOR THE OBSERVED PERIOD OF TIME
        OUTPUTS: FLOAT
        """
        # correcting parameters:
        if type(nominal_interest_rate) == list:
            nominal_interest_rate = 0.0
        # returning real compunded interest
        return (
            float((1 + nominal_interest_rate) ** (real_nper / nominal_nper))
            - 1.0
        )

    def simple_interest_rate(
        self, nominal_interest_rate, nominal_nper, real_nper
    ):
        """
        DOCSTRING: SIMPLETE INTEREST RATE
        INPUTS: NOMINAL INTEREST RATE, NOMINAL NPER, REAL NPER
        OUTPUTS: FLOAT
        """
        return 1 + nominal_interest_rate * nominal_nper / real_nper

    def present_value(self, rate, nper, pmt, fv=0, when='end'):
        """
        DOCSTRING: PRESENT VALUE
        INPUTS: RATE (FLOAT), NPER (INT), PMT (PAYMENT, INT), FV (FUTURE VALUE, WHETHER INT OR
            FLOAT), WHEN (BEGIN OR END OF PERIOD)
        OUTPUTS: FLOAT OF PV
        """
        return npf.pv(rate, nper, pmt, fv, when)

    def future_value(self, rate, nper, pmt, pv=0, when='end'):
        """
        DOCSTRING: FUTURE VALUE
        INPUTS: RATE (FLOAT), NPER (INT), PMT (PAYMENT, INT), PV (PRESENT VALUE, WHETHER INT OR
            FLOAT), WHEN (BEGIN OR END OF PERIOD)
        OUTPUTS: FLOAT OF PV
        """
        return npf.fv(rate, nper, pmt, pv, when)

    def internal_rate_return(self, list_cash_flow):
        """
        DOCSTRING: INTERNAL RATE OF RETURN
        INPUTS: LIST OF VALUES THAT COMPRISE THE CASH FLOW
        OUTPUTS: FLOAT OF IRR
        """
        return npf.irr(list_cash_flow)

    def net_present_value(self, rate, list_cash_flow):
        """
        DOCSTRING: NET PRESENT VALUE FOR A CASH FLOW DISCOUNTED BY A RATE; ALONG VALUES OUGHT BE
            AT LEAST ONE NEGATIVE AND ONE POSITIVE
        INPUTS: RATE (FLOAT) AND VALUES
        """
        return npf.npv(rate, list_cash_flow)


# print(MathFinance().present_value(0.1, 1, 0, 110))
# # output
# -99.99999999999999

# print(MathFinance().internal_rate_return([-133, 85.3, 12.47, 55.23, 11.47]))
# # output
# 0.11863351188349469

# print(MathFinance().future_value(0.1, 12, 0, -8000))
# # output
# 25107.427013768025

# print(MathFinance().future_value(0.1, 1, 0, -100))
# # output
# 110.00000000000001
