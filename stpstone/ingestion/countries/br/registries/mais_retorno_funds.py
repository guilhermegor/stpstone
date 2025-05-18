import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from stpstone._config.global_slots import YAML_MAIS_RETORNO_FUNDS
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.colors import ColorIdentifier


class MaisRetornoFunds(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        dt_ref: datetime = DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db: Optional[Session] = None,
        logger: Optional[Logger] = None,
        token: Optional[str] = None,
        list_slugs: Optional[List[str]] = None, 
        int_wait_load_seconds: int = 60, 
        int_delay_seconds: int = 30,
        bl_save_html: bool = False, 
        bl_headless: bool = False,
        bl_incognito: bool = False
    ) -> None:
        super().__init__(
            dict_metadata=YAML_MAIS_RETORNO_FUNDS,
            session=session,
            dt_ref=dt_ref,
            cls_db=cls_db,
            logger=logger,
            token=token,
            list_slugs=list_slugs,
            int_wait_load_seconds=int_wait_load_seconds, 
            int_delay_seconds=int_delay_seconds,
        )
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.list_slugs = list_slugs
        self.int_wait_load_seconds = int_wait_load_seconds
        self.int_delay_seconds = int_delay_seconds
        self.bl_save_html = bl_save_html
        self.bl_headless = bl_headless
        self.bl_incognito = bl_incognito

    def td_treatment_avl_funds(self, i: int, scraper: PlaywrightScraper) \
        -> Dict[str, Any]:
        p_cnpj = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["avl_funds"]["xpaths"]["p_cnpj"].format(i),
            selector_type="xpath"
        )
        href_fund = scraper.get_element_attrb(
            YAML_MAIS_RETORNO_FUNDS["avl_funds"]["xpaths"]["href_fund"].format(i),
            str_attribute="href",
            selector_type="xpath"
        )
        a_fund_name = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["avl_funds"]["xpaths"]["a_fund_name"].format(i),
            selector_type="xpath"
        )
        a_category = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["avl_funds"]["xpaths"]["a_category"].format(i),
            selector_type="xpath"
        )
        status_fund = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["avl_funds"]["xpaths"]["status_fund"].format(i),
            selector_type="xpath"
        )
        return {
            "CNPJ": p_cnpj.get("text", None),
            "URL_FUNDO": "https://maisretorno.com/" + href_fund if href_fund is not None \
                else None,
            "FUND_NAME": a_fund_name.get("text", None),
            "CATEGORY": a_category.get("text", None),
            "STATUS_FUND": status_fund.get("text", None),
            "PAGE_POSITION": i
        }
    
    def td_treatment_fund_properties(self, scraper: PlaywrightScraper) -> List[Dict[str, Any]]:
        h1_fund_nickname = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["h1_fund_nickname"],
            selector_type="xpath"
        )
        p_fund_full_name = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_fund_full_name"],
            selector_type="xpath"
        )
        span_status = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["span_status"],
            selector_type="xpath"
        )
        hex_pool_open = scraper.get_element_attrb(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["svg_pool_open"],
            str_attribute="color",
            selector_type="xpath"
        )
        hex_qualified_investor = scraper.get_element_attrb(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["svg_qualified_investor"],
            str_attribute="color",
            selector_type="xpath"
        )
        hex_exclusive_fund = scraper.get_element_attrb(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["svg_exclusive_fund"],
            str_attribute="color",
            selector_type="xpath"
        )
        hex_long_term_taxation = scraper.get_element_attrb(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["svg_long_term_taxation"],
            str_attribute="color",
            selector_type="xpath"
        )
        hex_pension_fund = scraper.get_element_attrb(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["svg_pension_fund"],
            str_attribute="color",
            selector_type="xpath"
        )
        p_cnpj_fund = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_cnpj_fund"],
            selector_type="xpath"
        )
        p_benchmark = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_benchmark"],
            selector_type="xpath"
        )
        p_fund_initial_date = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_fund_initial_date"],
            selector_type="xpath"
        )
        p_fund_type = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_fund_type"],
            selector_type="xpath"
        )
        a_fund_administrator = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["a_fund_administrator"],
            selector_type="xpath"
        )
        a_fund_class = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["a_fund_class"],
            selector_type="xpath"
        )
        a_fund_subclass = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["a_fund_subclass"],
            selector_type="xpath"
        )
        a_fund_manager = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["a_fund_manager"],
            selector_type="xpath"
        )
        p_rentability_ltm = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_rentability_ltm"],
            selector_type="xpath"
        )
        p_aum = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_aum"],
            selector_type="xpath"
        )
        p_average_aum_ltm = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_average_aum_ltm"],
            selector_type="xpath"
        )
        p_volatility_ltm = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_volatility_ltm"],
            selector_type="xpath"
        )
        p_sharpe_ltm = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_sharpe_ltm"],
            selector_type="xpath"
        )
        p_qty_unitholders = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_qty_unitholders"],
            selector_type="xpath"
        )
        h3_manager_full_name = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["h3_manager_full_name"],
            selector_type="xpath"
        )
        p_fund_manager_director = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_fund_manager_director"],
            selector_type="xpath"
        )
        p_fund_manager_email = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_fund_manager_email"],
            selector_type="xpath"
        )
        p_fund_manager_site = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_fund_manager_site"],
            selector_type="xpath"
        )
        p_fund_manager_telephone = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_fund_manager_telephone"],
            selector_type="xpath"
        )
        p_minimum_investment_amount = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_minimum_investment_amount"],
            selector_type="xpath"
        )
        p_minimum_balance_required = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_minimum_balance_required"],
            selector_type="xpath"
        )
        p_administration_fee = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_administration_fee"],
            selector_type="xpath"
        )
        p_administration_fee_max = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_administration_fee_max"],
            selector_type="xpath"
        )
        p_performance_fee = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_performance_fee"],
            selector_type="xpath"
        )
        p_fund_quotation_period = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_fund_quotation_period"],
            selector_type="xpath"
        )
        p_fund_settlement_period = scraper.get_element(
            YAML_MAIS_RETORNO_FUNDS["fund_properties"]["xpaths"]["p_fund_settlement_period"],
            selector_type="xpath"
        )
        return [
            {
                "NICKNAME": h1_fund_nickname.get("text", None),
                "FUND_NAME": p_fund_full_name.get("text", None), 
                "STATUS": span_status.get("text", None),
                "BL_POOL_OPEN": ColorIdentifier(hex_pool_open).bl_green() \
                    if hex_pool_open is not None else False, 
                "BL_QUALIFIED_INVESTOR": ColorIdentifier(hex_qualified_investor).bl_green() \
                    if hex_qualified_investor is not None else False, 
                "BL_EXCLUSIVE_FUND": ColorIdentifier(hex_exclusive_fund).bl_green() \
                    if hex_exclusive_fund is not None else False, 
                "BL_LONG_TERM_TAXATION": ColorIdentifier(hex_long_term_taxation).bl_green() \
                    if hex_long_term_taxation is not None else False, 
                "BL_PENSION_FUND": ColorIdentifier(hex_pension_fund).bl_green() \
                    if hex_pension_fund is not None else False,
                "CNPJ": p_cnpj_fund.get("text", None), 
                "BENCHMARK": p_benchmark.get("text", None), 
                "FUND_INITIAL_DATE": DatesBR().str_date_to_datetime(
                    p_fund_initial_date.get("text", None), "DD/MM/YYYY"), 
                "FUND_TYPE": p_fund_type.get("text", None), 
                "ADMINISTRATOR": a_fund_administrator.get("text", None), 
                "CLASS": a_fund_class.get("text", None), 
                "SUBCLASS": a_fund_subclass.get("text", None), 
                "MANAGER": a_fund_manager.get("text", None) if a_fund_manager is not None \
                    else None, 
                "RENTABILITY_LTM": float(p_rentability_ltm.get("text", None).replace(
                    "%", "").replace(",", ".")) / 100.0 \
                    if p_rentability_ltm is not None and \
                        p_rentability_ltm.get("text", None) is not None and \
                        p_rentability_ltm.get("text", None) != "-" else None,
                "AUM": p_aum.get("text", None), 
                "AVERAGE_AUM_LTM": p_average_aum_ltm.get("text", None), 
                "VOLATILITY_LTM": float(p_volatility_ltm.get("text", None).replace(
                    "%", "").replace(",", ".")) / 100.0 \
                    if p_volatility_ltm.get("text", None) is not None and \
                        p_volatility_ltm.get("text", None) != "-" else None, 
                "SHARPE_LTM": float(p_sharpe_ltm.get("text", None).replace(",", ".")) \
                    if p_sharpe_ltm.get("text", None) is not None and \
                        p_sharpe_ltm.get("text", None) != "-" else None,
                "QTY_UNITHOLDERS": p_qty_unitholders.get("text", None), 
                "MANAGER_FULL_NAME": h3_manager_full_name.get("text", None), 
                "MANAGER_DIRECTOR": p_fund_manager_director.get("text", None), 
                "MANAGER_EMAIL": p_fund_manager_email.get("text", None), 
                "MANAGER_SITE": p_fund_manager_site.get("text", None), 
                "MANAGER_TELEPHONE": p_fund_manager_telephone.get("text", None),
                "MINIMUM_INVESTMENT_AMOUNT": float(p_minimum_investment_amount.get(
                    "text", None).replace(".", "").replace("R$ ", "")) \
                    if p_minimum_investment_amount.get("text", None) is not None \
                    and p_minimum_investment_amount.get("text", None) != "-" \
                    and len(p_minimum_investment_amount.get("text", None)) > 1 else None, 
                "MINIMUM_BALANCE_REQUIRED": float(p_minimum_balance_required.get(
                    "text", None).replace(".", "").replace("R$ ", "")) \
                    if p_minimum_balance_required.get("text", None) is not None \
                    and p_minimum_balance_required.get("text", None) != "-" \
                    and len(p_minimum_balance_required.get("text", None)) > 1 else None, 
                "ADMINISTRATION_FEE": float(p_administration_fee.get("text", None).replace(
                    "%", "").replace(",", ".")) / 100.0 \
                    if p_administration_fee.get("text", None) is not None \
                        and p_administration_fee.get("text", None) != "-" else None,
                "ADMINISTRATION_FEE_MAX": float(p_administration_fee_max.get("text", None).replace(
                    "%", "").replace(",", ".")) / 100.0 \
                    if p_administration_fee_max.get("text", None) is not None \
                        and p_administration_fee.get("text", None) != "-" else None,
                "PERFORMANCE_FEE": float(p_performance_fee.get("text", None).replace(
                    "%", "").replace(",", ".")) / 100.0 \
                    if p_performance_fee.get("text", None) is not None \
                        and p_performance_fee.get("text", None) != "-" else None,
                "FUND_QUOTATION_PERIOD": p_fund_quotation_period.get("text", None) \
                    if p_fund_quotation_period is not None \
                        and p_fund_quotation_period != "-" else None, 
                "FUND_SETTLEMENT_PERIOD": p_fund_settlement_period.get("text", None) \
                    if p_fund_settlement_period is not None \
                        and p_fund_settlement_period != "-" else None
            }
        ]

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        i = 1
        list_ser = list()
        source = self.get_query_params(resp_req.url, "source")
        scraper = PlaywrightScraper(
            bl_headless=self.bl_headless,
            int_default_timeout=self.int_wait_load_seconds * 1_000, 
            bl_incognito=self.bl_incognito
        )
        with scraper.launch():
            if scraper.navigate(resp_req.url):
                if self.bl_save_html:
                    scraper.export_html(
                        scraper.page.content(), 
                        folder_path="data", 
                        filename="html-mais-retorno-avl-funds", 
                        bl_include_timestamp=True
                    )
                if source == "avl_funds":
                    while True:
                        if not scraper.selector_exists(
                            YAML_MAIS_RETORNO_FUNDS[source]["xpaths"]["p_cnpj"].format(i),
                            selector_type="xpath",
                            timeout=self.int_wait_load_seconds * 1_000
                        ): break
                        list_ser.append(self.td_treatment_avl_funds(i, scraper))
                        i += 1
                elif source == "fund_properties":
                    list_ser = self.td_treatment_fund_properties(scraper)
                else:
                    raise Exception(f"Invalid source {source}. Please choose a valid one.")
        return pd.DataFrame(list_ser)