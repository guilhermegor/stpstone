
### PRICING FUTURE CONTRACTS

import math
import numpy as np
from operator import itemgetter
from datetime import datetime, date
from scipy.interpolate import CubicSpline
from nelson_siegel_svensson.calibrate import calibrate_ns_ols, calibrate_nss_ols
from nelson_siegel_svensson import NelsonSiegelCurve, NelsonSiegelSvenssonCurve
from stpstone.finance.performance_apprraisal.financial_math import FinancialMath
from stpstone.handling_data.handlingstr import StrHandler
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.central.global_slots import MATURITY_WEEK_DAY_PER_CONTRACT
from stpstone.handling_data.handling_numbers import LinearAlgebra
from stpstone.utils.parsers.json_format import JsonFiles
from stpstone.handling_data.handling_lists import ListHandler


class NotionalFromPV:

    def general_pricing(self, float_pv, float_size, float_qty, float_xcg_rt_1,
                        float_xcg_rt_2=1):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return float_pv * float_size * float_qty * float_xcg_rt_1 / float_xcg_rt_2

    def dap(self, float_pv, float_qty, float_pmi_idx_mm1, float_pmi_ipca_rt_hat, dt_pmi_last,
            dt_pmi_follw, wd_bef_ref=0, float_size=0.00025, str_format_dt_input='YYYY-MM-DD'):
        """
        DOCSTRING: PRICING DAP CONTRACT - FUTURE OF BRAZILLIAN PMI MAIN CORE
        INPUTS: PRESENT VALUE OF DAP, QUANTITY, PMI INDEX MONTH MINUS 1, PMI IPCA RATE EXPECTED (
            ACCORING TO ANBIMA), LAST DATE OF PMI IPCA RELEASE, FOLLOWING DATE OF PMI IPCA RELEASE,
            WORKING DAYS BEFORE (REFERENCE DATE), AND SIZE (0.00025 STANDARD)
        OUTPUTS: FLOAT
        """
        # checking wheter the dates are in datetime format
        if DatesBR().check_date_datetime_format(dt_pmi_last) == False:
            dt_pmi_last = DatesBR().str_date_to_datetime(dt_pmi_last, str_format_dt_input)
        if DatesBR().check_date_datetime_format(dt_pmi_follw) == False:
            dt_pmi_follw = DatesBR().str_date_to_datetime(dt_pmi_follw, str_format_dt_input)
        # validate wheter following pmi date is superior than the last
        if dt_pmi_last > dt_pmi_follw: raise Exception(
            'Please validate the input of date pmi last and following, the former should be ' \
            + 'inferior than the last')
        # working days from the last release util the followin
        int_dudm = DatesBR().get_working_days_delta(
            dt_pmi_last, dt_pmi_follw
        )
        # working days from the last release until the reference date
        int_wddt = DatesBR().get_working_days_delta(
            dt_pmi_last,
            DatesBR().sub_working_days(DatesBR().curr_date, wd_bef_ref)
        )
        # prt - pmi pro-rata tempore
        float_prt = float_pmi_idx_mm1 * float_size * (1.0 + float_pmi_ipca_rt_hat) \
            ** (int_wddt / int_dudm)
        # returning notional pricing
        return float_pv * float_qty * float_prt


class NotionalFromRt:

    def di1(self, float_nominal_rt, dt_xpt, int_wd_bef=0, float_fv=100000.0,
            int_wddy=252, int_wd_cap=1, str_format_dt_input='YYYY-MM-DD'):
        """
        DOCSTRING: DI1 CONTRACT PRICING - CONSIDERS DAYTRADE AND SWING TRADE PRICING
        INPUTS: NOMINAL RATE, DATE OF SETTLEMENT, WORKING DAYS BEFORE, FUTURE VALUE, WORKING DAYS
            WITHIN A YEAR, WORKING DAYS OF CAPITALIZATION (1 AS STANDARD), DATE FORMAT INPUT
        OUTPUTS:
        """
        # checking wheter the settlement date is in datetime format
        if DatesBR().check_date_datetime_format(dt_xpt) == False:
            dt_xpt = DatesBR().str_date_to_datetime(dt_xpt, str_format_dt_input)
        # reference date
        dt_ref = DatesBR().sub_working_days(DatesBR().curr_date, int_wd_bef)
        # number of days to settlement of contract
        int_wddt = DatesBR().get_working_days_delta(dt_ref, dt_xpt)
        # real rate
        float_real_rate = FinancialMath().compound_interest(
            float_nominal_rt, int_wddy, int_wd_cap)
        # returning notional - adjust by di over in case of swing trade
        return abs(FinancialMath().present_value(float_real_rate, int_wddt, 0, float_fv))


class RtFromPV:

    def ddi(self, float_pv_di, float_fut_dol, float_ptax_dm1, dt_xpt, int_wd_bef=0, int_cddy=365,
            float_fv_di=100000.0, str_format_dt_input='YYYY-MM-DD'):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # checking wheter the settlement date is in datetime format
        if DatesBR().check_date_datetime_format(dt_xpt) == False:
            dt_xpt = DatesBR().str_date_to_datetime(dt_xpt, str_format_dt_input)
        # reference date
        dt_ref = DatesBR().sub_working_days(DatesBR().curr_date, int_wd_bef)
        # number of days to settlement of contract
        int_cddt = DatesBR().delta_calendar_days(dt_ref, dt_xpt)
        # returning rate
        return (float_pv_di / float_fv_di) / (float_fut_dol / float_ptax_dm1) - 1.0 \
            * int_cddy / int_cddt


class TSIR:

    def flat_forward(self, dict_nper_rates, working_days_year=252):
        """
        DOCSTRING: TERM STRUCTURE OF INTEREST RATES - FLAT FORWARD MODEL
        INPUTS: DICT WITH NPER (KEYS) AND RATES (VALUES), AND WORKING DAYS IN A YEAR (252 AS DEFAULT)
        OUTPUTS: JSON (KEYS AS NPER TO MATURITY AND RESPECTIVELY RATES)
        """
        # setting variables
        dict_ = dict()
        # store in memory dictionary with rates per nper
        for curr_nper_wrkdays in range(list(dict_nper_rates.keys())[0],
                                       list(dict_nper_rates.keys())[-1] + 1):
            # forward rate - interpolation for two boundaries
            nper_upper_bound = ListHandler().get_lower_upper_bound(
                list(dict_nper_rates.keys()), curr_nper_wrkdays)['upper_bound']
            nper_lower_bound = ListHandler().get_lower_upper_bound(
                list(dict_nper_rates.keys()), curr_nper_wrkdays)['lower_bound']
            rate_upper_bound = dict_nper_rates[nper_upper_bound]
            rate_lower_bound = dict_nper_rates[nper_lower_bound]
            forward_rate = math.pow(math.pow(
                1 + rate_upper_bound, nper_upper_bound / working_days_year) /
                math.pow(
                1 + rate_lower_bound, nper_lower_bound / working_days_year),
                (nper_upper_bound - nper_lower_bound) / working_days_year) - 1
            # flat forward rate for a given nper, between two boundaries
            dict_[curr_nper_wrkdays] = \
                ((1 + forward_rate) ** ((curr_nper_wrkdays - nper_lower_bound) / working_days_year) *
                 (1 + rate_lower_bound) ** (nper_lower_bound / working_days_year)) ** \
                (working_days_year / curr_nper_wrkdays) - 1
        # return a term structure of interest rates
        return dict_

    def cubic_spline(self, dict_nper_rates):
        """
        DOCSTRING: TERM STRUCTURE OF INTEREST RATES
        INPUTS:
        OUTPUTS:
        """
        cs = CubicSpline(list(dict_nper_rates.keys()),
                         list(dict_nper_rates.values()))
        dict_ = dict(zip(range(list(dict_nper_rates.keys())[0],
                                   list(dict_nper_rates.keys())[-1]),
                             cs(range(list(dict_nper_rates.keys())[0],
                                      list(dict_nper_rates.keys())[-1]))))
        return dict_

    def third_degree_polynomial_cubic_splice(self, list_constants_cubic_spline, nper_working_days,
                                             bl_sup_list, num_constants_cubic_spline=8):
        """
        DOCSTRING: THIRD DEGREE POLYNOMINAL FOR CUBIC SPLINE, CALCULATING WICH YTM REFEREES TO
            THE CURRENT NPER
        INPUTS: LIST OF CONSTANTS (A 0 X N AND B 0 X N)
        OUTPUTS: FLOAT
        """
        if len(list_constants_cubic_spline) != num_constants_cubic_spline:
            raise Exception('Poor definied list of constants for cubic spline, '
                            + 'ought have {} elements'.format(num_constants_cubic_spline))
        if bl_sup_list == False:
            return sum([list_constants_cubic_spline[x]
                        * nper_working_days ** x for x in
                        range(0, int(num_constants_cubic_spline / 2))])
        else:
            return sum([list_constants_cubic_spline[x]
                        * nper_working_days ** (x - num_constants_cubic_spline / 2) for x in
                        range(int(num_constants_cubic_spline / 2), num_constants_cubic_spline)])

    def literal_cubic_spline(self, dict_nper_rates, bl_debug=False):
        """
        DOCSTRING: TERM STRUCTURE OF INTEREST RATES
        INPUTS:
        OUTPUTS:
        """
        # setting variables
        dict_ = dict()
        # tsir - nper x rate
        for curr_nper_wrkdays in range(list(dict_nper_rates.keys())[0],
                                       list(dict_nper_rates.keys())[-1] + 1):
            # three-bounds-dictionary, nper-wise
            dict_lower_mid_upper_bound_nper = ListHandler().get_lower_mid_upper_bound(
                list(dict_nper_rates.keys()), curr_nper_wrkdays)
            if len(dict_lower_mid_upper_bound_nper.keys()) == 4:
                # working days for each bound and boolean of wheter its the ending element of
                #   original list within or not
                du1, du2, du3, bl_sup_list = dict_lower_mid_upper_bound_nper.values()
                i1, i2, i3 = [dict_nper_rates[v]
                              for k, v in dict_lower_mid_upper_bound_nper.items()
                              if k != 'end_of_list']
                # print(du1, du2, du3, bl_sup_list)
                # print(i1, i2, i3)
            else:
                raise Exception('Dimension-wise the list ought have 4 positions, contemplating: '
                                + 'lower bound, middle bound, upper bound and end of list boolean')
            # arrais working days and IRR (YTM)
            array_wd = np.array([
                [1, du1, du1 ** 2, du1 ** 3, 0, 0, 0, 0],
                [1, du2, du2 ** 2, du2 ** 3, 0, 0, 0, 0],
                [0, 0, 0, 0, 1, du2, du2 ** 2, du2 ** 3],
                [0, 0, 0, 0, 1, du3, du3 ** 2, du3 ** 3],
                [0, 1, 2 * du2, 3 * du2 ** 2, 0, -1, -2 * du2, -3 * du2 ** 2],
                [0, 0, 2, 6 * du2, 0, 0, -2, -6 * du2],
                [0, 0, 2, 6 * du1, 0, 0, 0, 0],
                [0, 0, 0, 0, 0, 0, 2, 6 * du3]
            ])
            # print(array_wd)
            array_ytm = np.array([
                [i1],
                [i2],
                [i2],
                [i3],
                [0],
                [0],
                [0],
                [0]
            ])
            # print(array_ytm)
            # constants array
            array_constants_solution = LinearAlgebra().matrix_multiplication(
                LinearAlgebra().transpose_matrix(array_wd), array_ytm
            )
            print(array_constants_solution)
            # rates of return (IRR, ytm) for the current working day nper
            dict_[curr_nper_wrkdays] = self.third_degree_polynomial_cubic_splice(
                array_constants_solution, curr_nper_wrkdays, bl_sup_list,
                len(array_constants_solution)
            )
            if bl_debug == True:
                print(curr_nper_wrkdays, dict_[curr_nper_wrkdays])
        # output - term structure of interest rates
        return dict_

    def nelson_siegel(self, dict_nper_rates, tau_first_assumption=1.0,
                           number_samples=None):
        """
        REFERENCES: https://nelson-siegel-svensson.readthedocs.io/en/latest/readme.html#calibration
        DOCSTRING: TERM STRUCTURE OF INTEREST RATES
        INPUTS: DICT WITH NPER (AS KEY) AND RATES (AS VALUES), AS WELL AS FIRST ASSUMPTION
            FOR TAU AND NUMBER OF SAMPLES WITHIN THE RANGE
        OUTPUTS: DICTIONARY WITH RATE (Y), PERIOD (T)
        """
        y, status = calibrate_ns_ols(
            np.array(list(dict_nper_rates.keys())),
            np.array(list(dict_nper_rates.values())), tau_first_assumption)
        if number_samples is None:
            t = np.linspace(list(dict_nper_rates.keys())[
                            0], list(dict_nper_rates.keys())[-1], int(list(dict_nper_rates.keys())[
                                -1] - list(dict_nper_rates.keys())[0] + 1))
            t_aux = range(list(dict_nper_rates.keys())[
                0], list(dict_nper_rates.keys())[-1])
        else:
            t = np.linspace(list(dict_nper_rates.keys())[
                            0], list(dict_nper_rates.keys())[-1], number_samples)
        dict_ = dict(zip(t_aux, y(t)))
        return dict_
