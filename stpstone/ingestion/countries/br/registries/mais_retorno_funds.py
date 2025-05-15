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
        bl_save_html: bool = False
    ) -> None:
        super().__init__(
            dict_metadata=YAML_MAIS_RETORNO_FUNDS,
            session=session,
            dt_ref=dt_ref,
            cls_db=cls_db,
            logger=logger,
            token=token,
            list_slugs=list_slugs,
            int_wait_load_seconds=int_wait_load_seconds
        )
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.list_slugs = list_slugs
        self.int_wait_load_seconds = int_wait_load_seconds
        self.bl_save_html = bl_save_html

    def td_treatment(self, source: str, i: int, scraper: PlaywrightScraper) -> Dict[str, Any]:
        if source == "avl_funds":
            p_cnpj = scraper.get_element(
                YAML_MAIS_RETORNO_FUNDS[source]["xpaths"]["p_cnpj"].format(i),
                selector_type="xpath"
            )
            href_fundo = scraper.get_element_attrb(
                YAML_MAIS_RETORNO_FUNDS[source]["xpaths"]["href_fundo"].format(i),
                str_attribute="href",
                selector_type="xpath"
            )
            a_nome_fundo = scraper.get_element(
                YAML_MAIS_RETORNO_FUNDS[source]["xpaths"]["a_nome_fundo"].format(i),
                selector_type="xpath"
            )
            a_categoria = scraper.get_element(
                YAML_MAIS_RETORNO_FUNDS[source]["xpaths"]["a_categoria"].format(i),
                selector_type="xpath"
            )
            status_fundo = scraper.get_element(
                YAML_MAIS_RETORNO_FUNDS[source]["xpaths"]["status_fundo"].format(i),
                selector_type="xpath"
            )
            return {
                "CNPJ": p_cnpj.get("text", None),
                "URL_FUNDO": "https://maisretorno.com/" + href_fundo if href_fundo is not None \
                    else None,
                "NOME_FUNDO": a_nome_fundo.get("text", None),
                "CATEGORIA": a_categoria.get("text", None),
                "STATUS_FUNDO": status_fundo.get("text", None),
                "PAGE_POSITION": i
            }

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        i = 1
        list_ser = list()
        source = self.get_query_params(resp_req.url, "source")
        scraper = PlaywrightScraper(
            headless=True,
            default_timeout=self.int_wait_load_seconds * 1_000
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
                while True:
                    if not scraper.selector_exists(
                        YAML_MAIS_RETORNO_FUNDS[source]["xpaths"]["p_cnpj"].format(i),
                        selector_type="xpath",
                        timeout=self.int_wait_load_seconds * 1_000
                    ): break
                    list_ser.append(self.td_treatment(source, i, scraper))
                    i += 1
        print("DF MAIS RETORNO FUNDS: \n", pd.DataFrame(list_ser))
        return pd.DataFrame(list_ser)