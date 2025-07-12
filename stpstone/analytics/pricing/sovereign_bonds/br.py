"""Pricing Brazilian Sovereign Bonds."""

from datetime import date, datetime

from stpstone.analytics.perf_metrics.financial_math import FinancialMath
from stpstone.utils.cals.handling_dates import DatesBR


class BRSovereignPricer:
    """Class for pricing Brazilian sovereign bonds (Tesouro Direto).
    
    References
    ----------
    [1] https://www.tesourodireto.com.br/data/files/1B/A1/EF/35/855FB610FAC28EB6018E28A8/Modulo1_TesouroDireto%20_2017_.pdf
    [2] https://www.tesourodireto.com.br/data/files/99/A1/BD/35/855FB610FAC28EB6018E28A8/Modulo%202_TesouroDireto%20_2017_.pdf
    [3] https://www.tesourodireto.com.br/data/files/18/A1/2B/35/855FB610FAC28EB6018E28A8/Modulo%203_TesouroDireto%20_2017_.pdf
    """

    def ltn(
        self, 
        float_ytm: float, 
        int_wddt: int, 
        float_fv: float = 1000.0, 
        int_wddy: int = 252, 
        int_wd_cap: int = 1
    ) -> float:
        """Price a zero-coupon Brazilian government bond (LTN).

        Parameters
        ----------
        float_ytm : float
            Yield to maturity (market yield)
        int_wddt : int
            Business days until maturity
        float_fv : float, optional
            Nominal value (default is 1000)
        int_wddy : int, optional
            Business days in a year (default is 252)
        int_wd_cap : int, optional
            Compounding frequency (default is 1)

        Returns
        -------
        float
            Present value of the bond
        """
        return FinancialMath().pv(FinancialMath().compound_r(
            float_ytm, int_wddy, int_wd_cap), int_wddt, 0, float_fv)

    def ntn_f(
        self, 
        float_ytm: float, 
        list_wddts: list[int], 
        float_cpn_y_nominal: float = 0.1, 
        float_fv: float = 1000.0,
        int_wddy: int = 252, 
        int_cpn_freq: int = 126, 
        int_wd_cap: int = 1
    ) -> dict:
        """Price a fixed-rate Brazilian government bond (NTN-F) with semiannual coupons.

        Parameters
        ----------
        float_ytm : float
            Annual yield to maturity (decimal form, e.g. 0.10 for 10%)
        list_wddts : list[int]
            List of business days until each cash flow payment date
        float_cpn_y_nominal : float, optional
            Annual nominal coupon rate in decimal form (default: 0.10 = 10%)
        float_fv : float, optional
            Face value of the bond (default: 1000.0)
        int_wddy : int, optional
            Business days count convention per year (default: 252)
        int_cpn_freq : int, optional
            Business days between coupon payments (default: 126 = semiannual)
        int_wd_cap : int, optional
            Compounding frequency in business days (default: 1 = daily)

        Returns
        -------
        tuple[list[float], float]
            A tuple containing:
            - List of present values for each cash flow (coupons + principal)
            - Total present value (sum of all discounted cash flows)

        Notes
        -----
        - The NTN-F pays semiannual coupons and returns principal at maturity
        - All calculations use business day count convention (252 days/year)
        - First returned value is list of PVs in same order as list_wddts
        """
        ytm_real = FinancialMath().compound_r(
            float_ytm, int_wddy, int_wd_cap)
        float_cpn_y_real = float_fv * FinancialMath().compound_r(
            float_cpn_y_nominal, int_wddy, int_cpn_freq)
        list_cfs = [FinancialMath().pv(ytm_real, int_wddt, 0, float_cpn_y_real) 
                    for int_wddt in list_wddts]
        list_cfs.append(FinancialMath().pv(ytm_real, list_wddts[-1],
                                                               0, float_fv))
        return list_cfs, sum(list_cfs)

    def pr1(
        self, 
        dt_ref: datetime, 
        int_day_ipca_release: int = 15
    ) -> float:
        """Calculate the proportion of days between last and next IPCA publication dates.

        Parameters
        ----------
        dt_ref : str, optional
            Trading date in DD/MM/YYYY format (default is next business day)
        int_day_ipca_release : int, optional
            Day of month when IPCA is published (default is 15)

        Returns
        -------
        float
            Proportion between last and next IPCA publication dates
        """
        # check if trading date is before current month's IPCA publication date
        if date(dt_ref.year, dt_ref.month,
                int_day_ipca_release) > dt_ref:
            dt_ipca_next = date(
                dt_ref.year, dt_ref.month, int_day_ipca_release)
            if dt_ref.month == 1:
                dt_ipca_last = date(
                    dt_ref.year - 1, 12, int_day_ipca_release)
            else:
                dt_ipca_last = date(
                    dt_ref.year, dt_ref.month - 1, int_day_ipca_release)
        else:
            if dt_ref.month == 12:
                dt_ipca_next = date(
                    dt_ref.year + 1, 1, int_day_ipca_release)
            else:
                dt_ipca_next = date(
                    dt_ref.year, dt_ref.month + 1, int_day_ipca_release)
            dt_ipca_last = date(
                dt_ref.year, dt_ref.month, int_day_ipca_release)
        # calculate PR1
        return DatesBR().delta_calendar_days(dt_ref, dt_ipca_last) \
            / DatesBR().delta_calendar_days(dt_ipca_next, dt_ipca_last)

    def vna_hat(
        self, 
        float_vna_ntnb_last: float, 
        float_ipca_y_hat: float, 
        float_pr1: float, 
        int_wddy: int = 252, 
        int_wddm: int = 30
    ) -> float:
        """Project the VNA (Nominal Unit Value) for NTN-B bonds.

        Parameters
        ----------
        float_vna_ntnb_last : float
            Last available VNA (truncated at 6th decimal)
        float_ipca_y_hat : float
            Projected annual IPCA inflation rate
        float_pr1 : float
            Proportion from float_pr1 calculation
        int_wddy : int, optional
            Business days in a year (default is 252)
        int_wddm : int, optional
            Calendar days in a month (default is 30)

        Returns
        -------
        float
            Projected VNA
        """
        float_ipca_y_hat_adu = FinancialMath().compound_r(float_ipca_y_hat, int_wddy,
                                                               int_wddm)
        return float_vna_ntnb_last * (1 + float_ipca_y_hat_adu) ** float_pr1

    def ntn_b_principal(
        self, 
        float_ytm: float, 
        int_wddt: int, 
        float_vna_ntnb_last: float, 
        float_ipca_y_hat: float, 
        dt_ref: datetime, 
        int_day_ipca_release: int = 15, 
        int_wddy: int = 252, 
        int_wddm: int = 30, 
        float_fv: float = 100.0
    ) -> float:
        """Price the principal NTN-B bond (inflation-linked with no coupons).

        Parameters
        ----------
        float_ytm : float
            Yield to maturity
        int_wddt : int
            Business days until maturity
        float_vna_ntnb_last : float
            Last available VNA
        float_ipca_y_hat : float
            Projected annual IPCA inflation rate
        dt_ref : str, optional
            Trading date in DD/MM/YYYY format (default is next business day)
        int_day_ipca_release : int, optional
            Day of month when IPCA is published (default is 15)
        int_wddy : int, optional
            Business days in a year (default is 252)
        int_wddm : int, optional
            Calendar days in a month (default is 30)
        float_fv : float, optional
            Nominal value (default is 100)

        Returns
        -------
        float
            Present value of the bond
        """
        # calculate float_pr1
        float_pr1 = BRSovereignPricer().pr1(dt_ref, int_day_ipca_release)
        # project vna
        vna_hat = BRSovereignPricer().vna_hat(
            float_vna_ntnb_last, float_ipca_y_hat, float_pr1, int_wddy, int_wddm)
        # calculate float_price
        float_price = float_fv / (1 + FinancialMath().compound_r(float_ytm, int_wddy, int_wddt)) \
            / float_fv
        return vna_hat * float_price

    def ntn_b(
        self, 
        float_ytm: float, 
        list_wddts: list[int], 
        float_vna_ntnb_last: float, 
        float_ipca_y_hat: float, 
        dt_ref: datetime, 
        int_day_ipca_release: int = 15, 
        int_wddy: int = 252, 
        int_wddm: int = 30,
        taxa_cupom_aa: float = 0.06, 
        int_cpn_freq: int = 126
    ) -> tuple[list[float], float]:
        """Price a coupon-paying NTN-B bond (inflation-linked with semiannual coupons).

        Parameters
        ----------
        float_ytm : float
            Annual yield to maturity (decimal form, e.g. 0.06 for 6%)
        list_wddts : list[int]
            List of business days until each coupon payment date
        float_vna_ntnb_last : float
            Last available VNA (Nominal Unit Value) before projection
        float_ipca_y_hat : float
            Projected annual IPCA inflation rate (decimal form)
        dt_ref : datetime
            Reference date for pricing calculations
        int_day_ipca_release : int, optional
            Day of month when IPCA is published (default: 15)
        int_wddy : int, optional
            Business days in a year (default: 252)
        int_wddm : int, optional
            Calendar days in a month (default: 30)
        taxa_cupom_aa : float, optional
            Annual coupon rate in decimal form (default: 0.06)
        int_cpn_freq : int, optional
            Business days between coupon payments (default: 126 = semiannual)

        Returns
        -------
        tuple[list[float], float]
            A tuple containing:
            - List of present values for each cash flow (coupons + principal)
            - Total present value (float_price) of the bond

        Notes
        -----
        - The NTN-B pays semiannual coupons indexed to inflation + principal at maturity
        - VNA projection accounts for inflation between last publication and ref date
        - All cash flows are discounted using business day convention (252 days/year)
        - Returned present values are in same order as input list_wddts
        - Final element in list represents discounted principal repayment
        """
        # calculate float_pr1
        float_pr1 = BRSovereignPricer().pr1(dt_ref, int_day_ipca_release)
        # project vna
        vna_hat = BRSovereignPricer().vna_hat(
            float_vna_ntnb_last, float_ipca_y_hat, float_pr1, int_wddy, int_wddm)
        # calculate semiannual coupon
        float_cpn_y_real = FinancialMath().compound_r(taxa_cupom_aa, int_wddy, int_cpn_freq)
        # create cash flow list
        list_cfs = list()
        for int_wddt in list_wddts:
            list_cfs.append(float_cpn_y_real / (
                1 + FinancialMath().compound_r(float_ytm, int_wddy, int_wddt)))
        list_cfs.append(1 / (1 + FinancialMath().compound_r(float_ytm, int_wddy, list_wddts[-1])))
        float_price = sum(list_cfs)
        list_cfs = [x * vna_hat for x in list_cfs]
        return list_cfs, vna_hat * float_price

    def vna_lft_hat(
        self, 
        float_vna_lft_last: float, 
        float_selic_y_hat: float, 
        int_wd_cap: int = 1, 
        int_wddy: int = 252
    ) -> float:
        """Project the VNA for LFT bonds (floating rate linked to Selic).

        Parameters
        ----------
        float_vna_lft_last : float
            Last available VNA for LFT
        float_selic_y_hat : float
            Projected annual Selic rate
        int_wd_cap : int, optional
            Compounding frequency (default is 1)
        int_wddy : int, optional
            Business days in a year (default is 252)

        Returns
        -------
        float
            Projected VNA
        """
        return float_vna_lft_last * \
            (1 + FinancialMath().compound_r(float_selic_y_hat, int_wddy, int_wd_cap))

    def lft(
        self, 
        float_ytm: float, 
        float_vna_lft_last: float, 
        float_selic_y_hat: float, 
        int_wd_cap: int = 1, 
        int_wddy: int = 252, 
        float_fv: float = 1
    ) -> float:
        """Price a LFT bond (floating rate linked to Selic).

        Parameters
        ----------
        float_ytm : float
            Yield to maturity
        float_vna_lft_last : float
            Last available VNA for LFT
        float_selic_y_hat : float
            Projected annual Selic rate
        int_wd_cap : int, optional
            Compounding frequency (default is 1)
        int_wddy : int, optional
            Business days in a year (default is 252)
        float_fv : float, optional
            Nominal value (default is 1)

        Returns
        -------
        float
            Present value of the bond
        """
        vna_lft_hat = BRSovereignPricer().vna_lft_hat(float_vna_lft_last,
                                                               float_selic_y_hat,
                                                               int_wd_cap,
                                                               int_wddy)
        float_price = float_fv / (1 + FinancialMath().compound_r(float_ytm, int_wddy,
                                                                         int_wd_cap))
        return vna_lft_hat * float_price

    def wd_until_maturity(
        self, 
        dt_maturity: datetime, 
        dt_ref: datetime
    ) -> int:
        """Calculate business days until maturity, including ANBIMA holidays.

        Parameters
        ----------
        dt_maturity : str or datetime
            Maturity date
        dt_ref : datetime, optional
            Reference date (default is current date)

        Returns
        -------
        int
            Business days until maturity
        """
        list_years = DatesBR().list_years_within_dates(dt_ref, dt_maturity)
        list_last_week_year_day = DatesBR().list_last_days_of_years(list_years)
        return DatesBR().get_working_days_delta(dt_ref, dt_maturity) \
            + DatesBR().add_holidays_not_considered_anbima(dt_ref, dt_maturity,
                                                           list_last_week_year_day)

    def wds_until_coupons(
        self, 
        dt_first_bzday: datetime, 
        str_bond_type: str, 
        dt_maturity: datetime
    ) -> dict:
        """Calculate business days until each coupon payment date for a bond.

        Parameters
        ----------
        dt_first_bzday : str or datetime
            First business day
        str_bond_type : str
            Bond type (e.g., "ntn-f")
        dt_maturity : str or datetime
            Maturity date

        Returns
        -------
        dict
            Dictionary with payment dates as keys and business days until payment as values
        """
        # initialize variables
        dict_cupons = dict()
        # convert string dates to datetime if needed
        if type(dt_first_bzday) == str:
            dt_first_bzday = DatesBR().str_date_to_datetime(dt_first_bzday, "DD/MM/AAAA")
        if type(dt_maturity) == str:
            dt_maturity = DatesBR().str_date_to_datetime(dt_maturity, "DD/MM/AAAA")
        dt_maturity = DatesBR().add_working_days(
            DatesBR().sub_working_days(dt_maturity, 1), 1)
        # get list of years in the period
        list_years = DatesBR().list_years_within_dates(dt_first_bzday, dt_maturity)
        list_last_week_year_day = DatesBR().list_last_days_of_years(list_years)
        
        # set coupon payment months based on bond type
        if str_bond_type == "ntn-f":
            mes_pagamento_cupom_1 = 1
            mes_pagamento_cupom_2 = 7
            dia_pagamento_cupom = 1
        
        # determine first coupon payment date
        if DatesBR().testing_dates(dt_first_bzday, date(dt_first_bzday.year,
                                                       mes_pagamento_cupom_2,
                                                       dia_pagamento_cupom)) == "OK":
            prim_pagamento_cupom = \
                DatesBR().add_working_days(
                    DatesBR().sub_working_days(date(dt_first_bzday.year,
                                                    mes_pagamento_cupom_2,
                                                    dia_pagamento_cupom), 1), 1)
        else:
            prim_pagamento_cupom = \
                DatesBR().add_working_days(
                    DatesBR().sub_working_days(date(dt_first_bzday.year + 1,
                                                    mes_pagamento_cupom_1,
                                                    dia_pagamento_cupom), 1), 1)
        
        # create dictionary of coupon payment dates and business days until payment
        data_corrente = dt_first_bzday
        data_prox_pagamento = prim_pagamento_cupom
        iterador = DatesBR().testing_dates(data_prox_pagamento, dt_maturity)
        
        while iterador == "OK":
            data_corrente = data_prox_pagamento
            if data_prox_pagamento.month == mes_pagamento_cupom_1:
                data_prox_pagamento = \
                    DatesBR().add_working_days(DatesBR().sub_working_days(
                        date(data_prox_pagamento.year,
                             mes_pagamento_cupom_2, dia_pagamento_cupom), 1), 1)
            else:
                data_prox_pagamento = \
                    DatesBR().add_working_days(DatesBR().sub_working_days(
                        date(data_prox_pagamento.year + 1, mes_pagamento_cupom_1,
                             dia_pagamento_cupom), 1), 1)
            
            dict_cupons[data_corrente] = DatesBR().get_working_days_delta(
                dt_first_bzday, data_corrente) \
                + DatesBR().add_holidays_not_considered_anbima(dt_first_bzday, data_corrente,
                                                               list_last_week_year_day)
            iterador = DatesBR().testing_dates(data_prox_pagamento, dt_maturity)
        
        return dict_cupons