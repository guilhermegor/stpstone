"""Pricing Brazilian Sovereign Bonds."""

from datetime import date

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.parsers.numbers import NumHandler


class BRSovereignPricer(metaclass=TypeChecker):
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
        int_wddy: int = 252
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

        Returns
        -------
        float
            Present value of the bond
        """
        # input validation
        if float_ytm <= 0:
            raise ValueError("Yield must be positive")
        if int_wddt <= 0:
            raise ValueError("Days to maturity must be positive")
        if float_fv <= 0:
            raise ValueError("Face value must be positive")
        if int_wddy <= 0:
            raise ValueError("Days in year must be positive")
        # price
        return NumHandler().truncate(float_fv / (1 + float_ytm) ** (int_wddt / int_wddy), 2)

    def ntn_f(
        self, 
        float_ytm: float, 
        list_wddts: list[int], 
        float_fv: float = 1000.0,
        float_cpn_y: float = 0.1,
        int_wddy: int = 252, 
        int_cpn_freq: int = 126,
    ) -> float:
        """Price a fixed-rate Brazilian government bond (NTN-F) with semiannual coupons.

        Parameters
        ----------
        float_ytm : float
            Yield to maturity (market yield)
        list_wddts : list[int]
            List of business days until maturity
        float_fv : float, optional
            Nominal value (default is 1000)
        float_cpn_y : float, optional
            Nominal yield of the coupon (default is 10%)
        int_wddy : int, optional
            Business days in a year (default is 252)
        int_cpn_freq : int, optional
            Number of days between coupons (default is 126)

        Returns
        -------
        float
            Present value of the bond

        Notes
        -----
        - The NTN-F pays semiannual coupons and returns principal at maturity
        - All calculations use business day count convention (252 days/year)
        - First returned value is list of PVs in same order as list_wddts
        """
        # input validation
        if not list_wddts:
            raise ValueError("Days list cannot be empty")
        if any(d < 0 for d in list_wddts):
            raise ValueError("Days cannot be negative")
        if sorted(list_wddts) != list_wddts:
            raise ValueError("Days must be in ascending order")
        if len(list_wddts) != len(set(list_wddts)):
            raise ValueError("Days list contains duplicates")

        # coupon calculation
        float_cpn_y_real = (1 + float_cpn_y) ** (int_cpn_freq / int_wddy) - 1
        float_cpn = float_cpn_y_real * float_fv
        
        # calculate number of payments correctly
        int_n_payments = len(list_wddts)
        list_cfs = [float_cpn] * int_n_payments
        list_cfs[-1] += float_fv
        
        # discount cash flows
        list_pvs = [
            cf / ((1 + float_ytm) ** (wddt / int_wddy))
            for cf, wddt in zip(list_cfs, list_wddts)
        ]
        
        return NumHandler().truncate(sum(list_pvs), 2)

    def pr1(
        self, 
        dt_ref: date, 
        dt_ipca_last: date, 
        dt_ipca_next: date
    ) -> float:
        """Calculate the PR1 (Projective Rate of Inflation) for NTN-B bonds.

        Parameters
        ----------
        dt_ref : date
            Reference date
        dt_ipca_last : date
            Last available IPCA date
        dt_ipca_next : date
            Next available IPCA date

        Returns
        -------
        float
            PR1 (Projective Rate of Inflation)
        """
        # validate date order
        if dt_ipca_next <= dt_ipca_last:
            raise ValueError("Next IPCA date must be after last IPCA date")
        if dt_ref < dt_ipca_last:
            raise ValueError("Reference date must be on or after last IPCA date")
        if dt_ref > dt_ipca_next:
            raise ValueError("Reference date must be on or before next IPCA date")
        
        # handle same dates
        if dt_ipca_next == dt_ipca_last:
            return 0.0
        
        # adjust for business days
        dt_ref = DatesBR().find_working_day(dt_ref, bl_next=False)
        delta_ref = DatesBR().delta_calendar_days(dt_ref, dt_ipca_last)
        delta_total = DatesBR().delta_calendar_days(dt_ipca_next, dt_ipca_last)
        print(f"PR1: {delta_ref} / {delta_total} = {delta_ref / delta_total}")
        return delta_ref / delta_total

    def vna_ntnb_hat(
        self, 
        float_vna_ntnb_last: float, 
        float_ipca_y_m_hat: float, 
        float_pr1: float,
    ) -> float:
        """Project the VNA (Valor Nominal Atualizado - Nominal Value Updated) for NTN-B bonds.

        Parameters
        ----------
        float_vna_ntnb_last : float
            Last available VNA (truncated at 6th decimal)
        float_ipca_y_m_hat : float
            Projected next month IPCA inflation rate
        float_pr1 : float
            pr1 = (number of calendar days between purchase date and the 15th of current month) / 
          (number of calendar days between the 15th of next month and the 15th of current month)

        Returns
        -------
        float
            Projected VNA
        
        Notes
        -----
        - For NTN-B bonds, the base date for VNA updates is 2000-07-15
        - On the base date, the VNA was worth R$ 1,000.00 and has been adjusted by the IPCA 
        (Broad National Consumer Price Index - Brazil's official inflation index) since then
        """
        if float_vna_ntnb_last <= 0:
            raise ValueError("VNA must be positive")
        if float_pr1 < 0 or float_pr1 > 1:
            raise ValueError("PR1 must be between 0 and 1")
        
        return float_vna_ntnb_last * (1 + float_ipca_y_m_hat) ** float_pr1

    def ntn_b_principal(
        self, 
        float_ytm: float, 
        int_wddt: int, 
        float_vna_ntnb_last: float, 
        float_ipca_y_m_hat: float, 
        float_pr1: float,
        int_wddy: int = 252,
        float_fv: float = 100.0
    ) -> float:
        """Price the principal NTN-B bond (inflation-linked with no coupons).

        Parameters
        ----------
        float_ytm : float
            Yield to maturity above the IPCA (decimal form, e.g. 0.06 for 6%)
        int_wddt : int
            Business days until maturity
        float_vna_ntnb_last : float
            Last available VNA
        float_ipca_y_m_hat : float
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
        float_vna_hat = self.vna_ntnb_hat(float_vna_ntnb_last, float_ipca_y_m_hat, float_pr1)
        float_ytm_real = float_fv / ((1 + float_ytm) ** (int_wddt / int_wddy)) / 100.0
        return NumHandler().truncate(float_vna_hat * float_ytm_real, 2)

    def ntn_b(
        self, 
        float_ytm: float, 
        list_wddts: list[int], 
        float_vna_ntnb_last: float, 
        float_ipca_y_m_hat: float, 
        float_pr1: float,
        float_cpn_y: float = 0.06,
        int_wddy: int = 252,
        int_cpn_freq: int = 126,
    ) -> float:
        """Price a coupon-paying NTN-B bond (inflation-linked with semiannual coupons).

        Parameters
        ----------
        float_ytm : float
            Annual yield to maturity (decimal form, e.g. 0.06 for 6%)
        list_wddts : list[int]
            List of business days until each coupon payment date
        float_vna_ntnb_last : float
            Last available VNA (Nominal Unit Value) before projection
        float_ipca_y_m_hat : float
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
        # input validation
        if float_ipca_y_m_hat < 0:
            raise ValueError("IPCA cannot be negative")
        if float_pr1 < 0 or float_pr1 > 1:
            raise ValueError("PR1 must be between 0 and 1")

        # coupon payment
        float_vna_hat = self.vna_ntnb_hat(float_vna_ntnb_last, float_ipca_y_m_hat, float_pr1)
        float_cpn_y_real = (1 + float_cpn_y) ** (int_cpn_freq / int_wddy) - 1

        # rates
        list_rates = [
            float_cpn_y_real / ((1 + float_ytm) ** (int_wddt / int_wddy))
            for int_wddt in list_wddts
        ]
        list_rates.append(1 / ((1 + float_ytm) ** (list_wddts[-1] / int_wddy)))
        return NumHandler().truncate(float_vna_hat * sum(list_rates), 2)

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
        
        Notes
        -----
        - For LFT bonds, the base date for VNA updates is 2000-07-01
        - On the base date, the VNA was worth R$ 1,000.00 and has been adjusted by the daily Selic 
        since then
        """
        if float_vna_lft_last <= 0:
            raise ValueError("VNA must be positive")
        if float_selic_y_hat < 0:
            raise ValueError("SELIC cannot be negative")
        if int_wd_cap <= 0 or int_wddy <= 0:
            raise ValueError("Days counts must be positive")
        
        return float_vna_lft_last * (1 + float_selic_y_hat) ** (int_wd_cap / int_wddy)

    def lft(
        self, 
        float_ytm: float, 
        int_wddt: int, 
        float_vna_lft_last: float, 
        float_selic_y_hat: float, 
        int_wd_cap: int = 1, 
        int_wddy: int = 252,
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
        vna_lft_hat = self.vna_lft_hat(float_vna_lft_last, float_selic_y_hat, int_wd_cap, int_wddy)
        float_price = 1 / ((1 + float_ytm) ** (int_wddt / int_wddy))
        return NumHandler().truncate(vna_lft_hat * float_price, 2)

    def custody_fee_bmfbov(
        self, 
        float_price: float, 
        int_cddt: int, 
        float_fee: float = 0.003, 
        int_cddy: int = 365,
        float_max_fee: float = 1_500_000.0,
    ) -> float:
        """Calculate the custody fee for a Brazilian sovereign bond, paid to BM&FBOVESPA.

        Parameters
        ----------
        float_price : float
            Price of the bond
        int_cddt : int
            Number of calendar days
        float_fee : float, optional
            Custody fee rate, by default 0.003
        int_cddy : int, optional
            Number of calendar days in a year, by default 365

        Returns
        -------
        float
            Custody fee
        
        Notes
        -----
        - Custody fee is calculated based on the price of the bond
        - Related to securities custody services and balance information/transactions
        - Charged upon occurrence of a custody event:
            - Interest payment
            - Bond sale
            - Bond maturity
        - In case of custody fee and Custodian Agent's fee less than R$ 10.00 during the semester, 
        the amount accumulates for charging in the next semester
        - The fee is charged proportionally to the period the investor holds the security, and is 
        calculated up to a custody account balance of R$ 1,500,00.00
        """
        if float_price <= 0:
            raise ValueError("Price must be positive")
        if int_cddt <= 0:
            raise ValueError("Days must be positive")
    
        return min(float_max_fee, NumHandler().truncate(
            float_price * ((1 + float_fee) ** (int_cddt / int_cddy) - 1), 2))