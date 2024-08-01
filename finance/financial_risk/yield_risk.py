# PERFORMANCE APPRAISAL FOR FIXED INCOME BILLS, NOTES, BONDS, AND OTHER ASSETS

import numpy as np
from stpstone.finance.derivatives.futures import FuturesPricingB3


class FixedIncomeAppraisal:

    def __init__(self, list_cfs=None, list_rates=None, present_value=None, ytm=None,
                 no_coupons=None, payments_per_year=2, pv_minus=None, pv_plus=None,
                 delta_yield=None, delta_pv=None,
                 delta_curve=None, annual_modified_duration=None, future_value=None,
                 shift=0.0001, bl_exact_form_macaulay_dur=True, bl_exact_form_money_dur=True,
                 bl_exact_form_convexity=True, bl_exact_form_pv_01=True,
                 bl_consider_convexity_delta_pv=True):
        self.list_cfs = list_cfs
        self.list_rates = list_rates
        self.present_value = present_value
        self.ytm = ytm
        self.no_coupons = no_coupons
        self.payments_per_year = payments_per_year
        self.bl_exact_form_macaulay_dur = bl_exact_form_macaulay_dur
        self.pv_minus = pv_minus
        self.pv_plus = pv_plus
        self.delta_yield = delta_yield
        self.delta_pv = delta_pv
        self.delta_curve = delta_curve
        self.annual_modified_duration = annual_modified_duration
        self.future_value = future_value
        self.shift = shift
        self.bl_exact_form_money_dur = bl_exact_form_money_dur
        self.bl_exact_form_convexity = bl_exact_form_convexity
        self.bl_exact_form_pv_01 = bl_exact_form_pv_01
        self.bl_consider_convexity_delta_pv = bl_consider_convexity_delta_pv

    @property
    def yield_durations(self):
        '''
        REFERENCE: https://quant.stackexchange.com/questions/15549/modified-or-macauley-duration-in-python
        DOCSTRING: YIELD DURATIONS - MACAULAY AND MODIFIED
        INPUTS: CASH FLOWS, list_rates (FLOAT OR LIST OF FLOATS), PRESENT VALUE OF BOND, YTM, NUMBER
            OF CUPONS AND PAYMENTS PER YEAR
        OUTPUTS: DICTIONARY WITH MACAULAY_DURATION AND MODIFIED_DURATION
        '''
        # calculating wheter through exact or approximated form
        if self.bl_exact_form_macaulay_dur == False:
            mac_dur = np.sum([self.list_cfs[i] * ((i + 1) / self.payments_per_year) /
                              np.power(1 + self.list_rates[i], i + 1) for i in range(
                                  len(self.list_cfs))]) / \
                self.present_value
        else:
            mac_dur = (self.pv_minus - self.pv_plus) / \
                (2 * self.delta_yield * self.present_value)
        # modified duration
        mod_dur = mac_dur / (1 + self.ytm / self.no_coupons)
        # return durations
        return {
            'macaulay_duration': mac_dur,
            'modified_duration': mod_dur
        }

    @property
    def curve_durations(self):
        '''
        DOCSTRING: CURVE DURATIONS
        INPUTS: PV MINUS, PV PLUS, PV ZERO AND DELTA CURVE
        OUTPUTS:
        '''
        # effective duration
        eff_dur = (self.pv_minus - self.pv_plus) / \
            (2 * self.delta_curve * self.present_value)
        # returning durations
        return {
            'effective_duration': eff_dur
        }

    @property
    def key_rate_duration(self):
        '''
        DOCSTRING: KEY RATE DURATION IS THE BOND SENSITIVITY TO THE BENCHMARK YIELD CURVE IN A 
            SPECIFIC MATURITY, PROVIDING FURTHER SENSITIVITY TO NON-PARALLEL BENCHMARK YIELD CURVE 
            CHANGES
        INPUTS: PV, DELTA PV, DELTA RATE K
        OUTPUTS: FLOAT
        '''
        return - 1.0 / self.present_value * (self.delta_pv / self.delta_yield)

    @property
    def money_duration(self):
        '''
        DOCSTRING: THE MONEY DURATION OF A BOND IS A MEASURE OF THE PRICE CHANGE IN UNITS OF THE 
            CURRENCY IN WHICH THE BOND IS DENOMINATED
        INPUTS: ANNUAL MODIFIED DURATION (FLOAT), AND PV (FLOAT)
        OUTPUTS: FLOAT
        '''
        # calculating wheter through exact or approximated form
        if self.bl_exact_form_money_dur == True:
            return self.yield_durations['macaulay_duration'] * self.present_value
        else:
            return self.annual_modified_duration * self.present_value

    @property
    def convexity(self):
        '''
        REFERENCE: https://quant.stackexchange.com/questions/15549/modified-or-macauley-duration-in-python
        DOCSTRING: BOND CONVEXITY ADJUSTMENT - THE SECOND ORDER EFFECT
        INPUTS: CASH FLOWS, list_rates (FLOAT OR LIST OF FLOATS), PRESENT VALUE OF BOND, YTM, NUMBER
            OF CUPONS AND PAYMENTS PER YEAR
        OUTPUTS: TUPLE OF FLOATS
        '''
        # calculating wheter through exact or approximated form
        if self.bl_exact_form_convexity == True:
            return np.sum([self.list_cfs[i] * ((i + 1) * (i + 2) / self.payments_per_year) /
                           np.power(1 + self.list_rates[i], i + 3) for i in range(
                               len(self.list_cfs))]) / self.future_value
        else:
            return ((self.pv_minus + self.pv_plus) - (2 * self.present_value)) / (
                self.delta_yield ** 2 / self.present_value)

    def dv01(self, ytm, nper, side='C', contract='DI1'):
        '''
        DOCSTRING: DV01 TO MEASURE A DI1 PNL APPLIED A STRESS OF 1 BPS (0.1%) IN YTM
        INPUTS: YTM, NPER, SIDE
        OUPUTS: FLOAT
        '''
        if contract == 'DI1':
            if side == 'C':
                reverse_side = 'V'
            elif side == 'V':
                reverse_side = 'C'
            else:
                raise Exception(
                    'Poorly defined side, ought be "C" or "V", please revisit the parameter.')
            return FuturesPricingB3().pu_di1(ytm, nper, c_v_taxa=side) \
                + FuturesPricingB3().pu_di1(ytm + self.shift, nper, c_v_taxa=reverse_side)
        else:
            raise Exception('Contract poorly definied')

    @property
    def pv01(self):
        '''
        REFERENCES: https://www.theice.com/publicdocs/futures/Price_Sensitivity.pdf
        DOCSTRING: PV01, OR BASIS POINT VALUE, IS THE PRICE SENSITIVITY TO AN YIELD PERCENTUAL 
            CHANGE
        INPUTS: CASH FLOWS, list_rates (FLOAT OR LIST OF FLOATS), PRESENT VALUE OF BOND, YTM, NUMBER
            OF CUPONS AND PAYMENTS PER YEAR (2 AS DEFAULT) AND SHIFT (0.0001 AS DEFAULT), BL 
            DIRECT FORM, PV MINUS (NONE DEFAULT) AND PV PLUS (NONE DEFAULT)
        OUPUTS: FLOAT
        '''
        # calculating wheter through exact or approximated form
        if self.bl_exact_form_pv_01 == True:
            return self.yield_durations(
                self.list_cfs, self.list_rates, self.present_value, self.ytm, self.no_coupons,
                self.payments_per_year)['modified_duration'] * self.present_value * self.shift
        else:
            return (self.pv_minus - self.pv_plus) / 2.0

    @property
    def approximate_delta_bond_price(self):
        '''
        DOCSTRING: ESTIMAGTE PERCENTAGE PRICE CHANGE IN CURRENCY UNITS
        INPUTS: ANNUAL MODIFIED DURATION, PV, DELTA YIELD
        OUTPUTS: FLOAT
        '''
        # calculating with or without second order bond pricing - convexity
        if self.bl_consider_convexity_delta_pv == False:
            return -self.money_duration(self.annual_modified_duration, self.present_value) \
                * self.delta_yield
        else:
            return (-self.yield_durations(
                self.list_cfs, self.list_rates, self.present_value, self.ytm, self.no_coupons,
                self.payments_per_year)['modified_duration'] * self.delta_yield) \
                + (0.5 * self.convexity(self.list_cfs, self.list_rates, self.future_value,
                                        self.payments_per_year) * self.delta_yield ** 2)
