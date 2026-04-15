"""Implementation of Anbima Funds ingestion instance."""

from contextlib import suppress
from datetime import date
from io import StringIO
from logging import Logger
from random import randint
import re
import time
from typing import Any, Optional, Union

import pandas as pd
from playwright.sync_api import Locator, Page as PlaywrightPage, sync_playwright
from requests import Response, Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.abc.ingestion_abc import (
    ABCIngestionOperations,
    ContentParser,
    CoreIngestion,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


class AnbimaDataFundsAvailable(ABCIngestionOperations):
    """Anbima Funds ingestion class."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        start_page: int = 0,
        end_page: int = 20,
    ) -> None:
        """Initialize the Anbima Funds ingestion class.
        
        Parameters
        ----------
        date_ref : Optional[date]
            The date of reference, by default None.
        logger : Optional[Logger]
            The logger, by default None.
        cls_db : Optional[Session]
            The database session, by default None.
        start_page : int
            Starting page number, by default 0.
        end_page : int
            Ending page number (inclusive), by default 20.
        
        Returns
        -------
        None

        Raises
        ------
        ValueError
            If start_page is less than 0.
            If end_page is less than start_page.

        Notes
        -----
        [1] Metadata: https://data.anbima.com.br/busca/fundos
        """
        super().__init__(cls_db=cls_db)
        CoreIngestion.__init__(self)
        ContentParser.__init__(self)

        self.logger = logger
        self.cls_db = cls_db
        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()
        self.cls_create_log = CreateLog()
        self.cls_dates_br = DatesBRAnbima()
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.base_url = "https://data.anbima.com.br/busca/fundos"
        
        if start_page < 0:
            raise ValueError("start_page must be greater than or equal to 0")
        if end_page < start_page:
            raise ValueError("end_page must be greater or equal than the start_page")
        
        self.start_page = start_page
        self.end_page = end_page
    
    def run(
        self,
        timeout_ms: int = 30_000,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_anbimadata_fundos"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout_ms : int
            The timeout in milliseconds, by default 30_000
        bool_insert_or_ignore : bool
            Whether to insert or ignore the data, by default False
        str_table_name : str
            The name of the table, by default "br_anbimadata_fundos"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        raw_data = self.get_response(timeout_ms=timeout_ms)
        df_ = self.transform_data(raw_data=raw_data)
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.date_ref,
            dict_dtypes={
                "NOME_FUNDO": str,
                "LINK_FUNDO": str,
                "COD_ANBIMA": str,
                "TIPO_FUNDO": str,
                "PUBLICO_ALVO": str,
                "STATUS_FUNDO": str,
                "CNPJ_FUNDO": str,
                "PL": str,
                "APLICACAO_MIN_INICIAL": str,
                "PRAZO_RESGATE": str,
                "RENTABILIDADE_12M": str,
            }, 
            str_fmt_dt="DD/MM/YYYY",
            url=self.base_url,
        )
        
        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db, 
                str_table_name=str_table_name, 
                df_=df_, 
                bool_insert_or_ignore=bool_insert_or_ignore
            )
        else:
            return df_

    def get_response(
        self, 
        timeout_ms: int = 30_000,
    ) -> list:
        """Scrape funds data using Playwright.

        Parameters
        ----------
        timeout_ms : int
            The timeout in milliseconds, by default 30_000
        
        Returns
        -------
        list
            list of scraped funds data.
        """
        list_pages_data: list[dict[str, Union[str, int, float, date]]] = []
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()

            self.cls_create_log.log_message(
                self.logger, 
                f"🚀 Starting scraping from pages {self.start_page} to {self.end_page}...", 
                "info"
            )
            
            first_url = f"{self.base_url}?size=100&page={self.start_page}"
            page.goto(first_url)
            page.wait_for_timeout(timeout_ms)
            
            total_pages_available = self._get_total_pages(page)
            
            actual_end_page = min(self.end_page, total_pages_available - 1)
            
            self.cls_create_log.log_message(
                self.logger, 
                f"📊 Total pages available: {total_pages_available}. "
                f"Will scrape from {self.start_page} to {actual_end_page}", 
                "info"
            )
            
            for page_num in range(self.start_page, actual_end_page + 1):
                self.cls_create_log.log_message(
                    self.logger, 
                    f"📄 Fetching page {page_num}...", 
                    "info"
                )
                
                url = f"{self.base_url}?size=100&page={page_num}"
                
                if page_num != self.start_page:
                    page.goto(url)
                    page.wait_for_timeout(timeout_ms)

                fund_cards = page.locator(
                    'xpath=//article[@class="AccordionFundosCard_container__1vY2P"]'
                ).all()
                
                list_page_data: list[dict[str, Union[str, int, float, date]]] = []
                for idx, card in enumerate(fund_cards):
                    try:
                        fund_data = self._extract_fund_data(page, card, idx)
                        fund_data["pagina"] = page_num
                        list_page_data.append(fund_data)
                    except Exception as e:
                        self.cls_create_log.log_message(
                            self.logger, 
                            f'Error extracting fund {idx} on page {page_num}: {e}', 
                            "warning"
                        )
                            
                self.cls_create_log.log_message(
                    self.logger, 
                    f"✅ Page {page_num}: {len(list_page_data)} items", 
                    "info"
                )
                
                list_pages_data.extend(list_page_data)
                
                if page_num < actual_end_page:
                    time.sleep(randint(2, 10))  # noqa S311
            
            browser.close()
        
        self.cls_create_log.log_message(
            self.logger, 
            f"💾 Scraping finished. Total: {len(list_pages_data)} funds found.", 
            "info"
        )
        
        return list_pages_data
    
    def _get_total_pages(self, page: PlaywrightPage) -> int:
        """Get the total number of pages from pagination.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        
        Returns
        -------
        int
            The total number of pages, defaults to 1 if not found.
        """
        try:
            pagination_element = page.locator(
                'xpath=//*[@id="pagination"]/div[3]/span/span'
            ).first
            
            if pagination_element.is_visible(timeout=5_000):
                text = pagination_element.inner_text().strip()
                match = re.search(r'(\d+)', text)
                if match:
                    total_pages = int(match.group(1))
                    return total_pages
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Could not determine total pages, defaulting to 1: {e}', 
                "warning"
            )
        
        return 1
    
    def _extract_fund_data(
        self, 
        page: PlaywrightPage,
        card_element: Locator,
        idx: int
    ) -> dict:
        """Extract fund data from a fund card element.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        card_element : Locator
            The fund card element.
        idx : int
            The index of the fund card.
        
        Returns
        -------
        dict
            Dictionary containing extracted fund data.
        """
        data = {}
        
        data["NOME_FUNDO"] = self._extract_from_card(
            card_element, 'xpath=.//*[@id="title"]'
        )
        
        data["LINK_FUNDO"] = self._extract_link_from_card(card_element)
        
        tags_data = self._extract_tags(card_element)
        data.update(tags_data)
        
        data["CNPJ_FUNDO"] = self._extract_from_card(
            card_element, 
            'xpath=.//p[@class="AccordionFundosCard_id__ddKzx"]'
        )
        
        cells_data = self._extract_cells(card_element)
        data.update(cells_data)
        
        return data
    
    def _extract_from_card(
        self, 
        card_element: Locator, 
        selector: str
    ) -> Optional[str]:
        """Extract text from an element within the card.
        
        Parameters
        ----------
        card_element : Locator
            The fund card element.
        selector : str
            The selector string.
        
        Returns
        -------
        Optional[str]
            The extracted text or None.
        """
        with suppress(Exception):
            element = card_element.locator(selector).first
            if element.count() > 0:
                text = element.inner_text().strip()
                return text if text else None
        return None
    
    def _extract_link_from_card(self, card_element: Locator) -> Optional[str]:
        """Extract the href attribute from the link in the card.
        
        Parameters
        ----------
        card_element : Locator
            The fund card element.
        
        Returns
        -------
        Optional[str]
            The extracted link or None.
        """
        with suppress(Exception):
            link_element = card_element.locator('xpath=.//a').first
            if link_element.count() > 0:
                href = link_element.get_attribute('href')
                if href:
                    if href.startswith('/'):
                        return f"https://data.anbima.com.br{href}"
                    return href
        return None
    
    def _extract_tags(self, card_element: Locator) -> dict:
        """Extract tag data (TIPO_FUNDO, PUBLICO_ALVO, STATUS_FUNDO).
        
        Parameters
        ----------
        card_element : Locator
            The fund card element.
        
        Returns
        -------
        dict
            Dictionary with tag data.
        """
        data = {
            "TIPO_FUNDO": None,
            "PUBLICO_ALVO": None,
            "STATUS_FUNDO": None
        }
        
        try:
            regular_tags = card_element.locator(
                'xpath=.//div[@class="AccordionFundosCard_tags__oIM0g"]'
                '/p[@class="_tag_1p5q1_1"]'
            ).all()
            
            status_tag = card_element.locator(
                'xpath=.//p[@class="_tag_1p5q1_1 AccordionFundosCard_lastTag__Z7B_i"]'
            ).first
            
            if len(regular_tags) >= 1:
                data["TIPO_FUNDO"] = regular_tags[0].inner_text().strip()
                
            if len(regular_tags) >= 2:
                data["PUBLICO_ALVO"] = regular_tags[1].inner_text().strip()
                
            if status_tag.count() > 0:
                data["STATUS_FUNDO"] = status_tag.inner_text().strip()
                
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Error extracting tags: {e}', 
                "warning"
            )
        
        return data
    
    def _extract_cells(self, card_element: Locator) -> dict:
        """Extract cell data (PL, APLICACAO_MIN_INICIAL, PRAZO_RESGATE, RENTABILIDADE_12M).
        
        Parameters
        ----------
        card_element : Locator
            The fund card element.
        
        Returns
        -------
        dict
            Dictionary with cell data.
        """
        data = {
            "PL": None,
            "APLICACAO_MIN_INICIAL": None,
            "PRAZO_RESGATE": None,
            "RENTABILIDADE_12M": None
        }
        
        try:
            cells = card_element.locator(
                'xpath=.//div[@class="AccordionFundosCard_cell__K1y3k"]/p'
            ).all()
            
            if len(cells) >= 1:
                data["PL"] = cells[0].inner_text().strip()
            if len(cells) >= 2:
                data["APLICACAO_MIN_INICIAL"] = cells[1].inner_text().strip()
            if len(cells) >= 3:
                data["PRAZO_RESGATE"] = cells[2].inner_text().strip()
            if len(cells) >= 4:
                data["RENTABILIDADE_12M"] = cells[3].inner_text().strip()
                
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Error extracting cells: {e}', 
                "warning"
            )
        
        return data
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> StringIO:
        """Parse the raw file content.
        
        This method is kept for compatibility but not used in web scraping.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The parsed content.
        """
        return StringIO()
    
    def transform_data(
        self, 
        raw_data: list
    ) -> pd.DataFrame:
        """Transform scraped data into a DataFrame.
        
        Parameters
        ----------
        raw_data : list
            The scraped data list.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
        
        df_ = pd.DataFrame(raw_data)
        
        if 'LINK_FUNDO' in df_.columns:
            df_['COD_ANBIMA'] = df_['LINK_FUNDO'].apply(self._extract_cod_anbima)
        else:
            df_['COD_ANBIMA'] = None
        
        if 'pagina' in df_.columns:
            df_ = df_.drop('pagina', axis=1)
        
        return df_
    
    def _extract_cod_anbima(self, link: Optional[str]) -> Optional[str]:
        """Extract the COD_ANBIMA from the fund link.
        
        The code is the last part after the final '/' in the URL.
        
        Parameters
        ----------
        link : Optional[str]
            The fund link URL.
        
        Returns
        -------
        Optional[str]
            The extracted ANBIMA code or None if the link is invalid.
        
        Examples
        --------
        >>> self._extract_cod_anbima("https://data.anbima.com.br/fundos/ABC123")
        'ABC123'
        >>> self._extract_cod_anbima("https://data.anbima.com.br/fundos/XYZ-456")
        'XYZ-456'
        >>> self._extract_cod_anbima(None)
        None
        """
        if not link or not isinstance(link, str):
            return None
        
        try:
            link = link.rstrip('/')
            cod_anbima = link.split('/')[-1]
            return cod_anbima if cod_anbima else None
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Error extracting COD_ANBIMA from link {link}: {e}', 
                "warning"
            )
            return None
        

class AnbimaDataFundsAbout(ABCIngestionOperations):
    """Anbima Funds About ingestion class."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        list_fund_codes: Optional[list[str]] = None,
    ) -> None:
        """Initialize the Anbima Funds About ingestion class.
        
        Parameters
        ----------
        date_ref : Optional[date]
            The date of reference, by default None.
        logger : Optional[Logger]
            The logger, by default None.
        cls_db : Optional[Session]
            The database session, by default None.
        list_fund_codes : Optional[list[str]]
            list of fund codes to scrape about information for, by default None.
        
        Returns
        -------
        None
        """
        super().__init__(cls_db=cls_db)
        CoreIngestion.__init__(self)
        ContentParser.__init__(self)

        self.logger = logger
        self.cls_db = cls_db
        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()
        self.cls_create_log = CreateLog()
        self.cls_dates_br = DatesBRAnbima()
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.base_url = "https://data.anbima.com.br/fundos"
        self.list_fund_codes = list_fund_codes or []
    
    def run(
        self,
        timeout_ms: int = 30_000,
        bool_insert_or_ignore: bool = False, 
        str_table_name_characteristics: str = "br_anbimadata_funds_characteristics",
        str_table_name_related: str = "br_anbimadata_funds_related_structure_class_subclass", 
        str_table_name_about: str = "br_anbimadata_funds_about"
    ) -> Optional[dict[str, pd.DataFrame]]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrames are returned.

        Parameters
        ----------
        timeout_ms : int
            The timeout in milliseconds, by default 30_000
        bool_insert_or_ignore : bool
            Whether to insert or ignore the data, by default False
        str_table_name_characteristics : str
            The name of the characteristics table, by default "br_anbimadata_funds_characteristics"
        str_table_name_related : str
            The name of the related structure table
        str_table_name_about : str
            The name of the about table, by default "br_anbimadata_funds_about"

        Returns
        -------
        Optional[dict[str, pd.DataFrame]]
            Dictionary containing the three DataFrames.
        """
        raw_data = self.get_response(timeout_ms=timeout_ms)
        
        df_characteristics = self.transform_characteristics_data(raw_data["characteristics"])
        df_related = self.transform_related_data(raw_data["related"])
        df_about = self.transform_about_data(raw_data["about"])

        self.cls_create_log.log_message(
            self.logger, 
            f"📊 Initial DataFrame shapes - "
            f"Characteristics: {df_characteristics.shape}, "
            f"Related: {df_related.shape}, "
            f"About: {df_about.shape}", 
            "info"
        )
        
        self.cls_create_log.log_message(
            self.logger, 
            "🔄 Starting standardization of characteristics dataframe...", 
            "info"
        )
        df_characteristics = self.standardize_dataframe(
            df_=df_characteristics, 
            date_ref=self.date_ref,
            dict_dtypes={
                "FUND_CODE": str,
                "NOME_FUNDO": str,
                "CLASSE_FUNDO": str,
                "CLASSE_ANBIMA_FUNDO": str,
                "STATUS_FUNDO": str,
                "CNPJ_COD_ANBIMA_FUNDO": str,
                "PL": str,
                "ULTIMA_COTA": str,
                "DATA_ULTIMA_COTA": "date",
                "APLICACAO_MINIMA_INICIAL": str,
                "PRAZO_RESGATE": str,
                "PERIODO_PRAZO_RESGATE": str,
                "NUMERO_COTISTAS": str,
                "RENTABILIDADE_12M": str,
                "FUNDO_CASCA_NOME": str,
                "FUNDO_CASCA_CNPJ": str,
                "TIPO_FUNDO": str,
                "ADMINISTRADOR_FUNDO": str,
                "GESTOR_FUNDO": str,
                "CLASSE_FUNDO_NOME": str,
                "CLASSE_FUNDO_CNPJ": str,
                "IS_ESG": str,
                "BENCHMARK": str,
                "LINK_REGULAMENTO": str,
                "SUBCLASSE_NOME_FUNDO": str,
                "SUBCLASSE_CODIGO_ANBIMA": str,
            }, 
            str_fmt_dt="DD/MM/YYYY",
            url=self.base_url,
        )
        self.cls_create_log.log_message(
            self.logger, 
            f"✅ Characteristics dataframe standardized - Shape: {df_characteristics.shape}, "
            f"Columns: {len(df_characteristics.columns)}, "
            f"Rows: {len(df_characteristics)}", 
            "info"
        )
        
        self.cls_create_log.log_message(
            self.logger, 
            "🔄 Starting standardization of related structure dataframe...", 
            "info"
        )
        df_related = self.standardize_dataframe(
            df_=df_related, 
            date_ref=self.date_ref,
            dict_dtypes={
                "FUND_CODE": str,
                "NOME_FUNDO": str,
                "TIPO_FUNDO": str,
                "STATUS_FUNDO": str,
                "CODIGO_ANBIMA": str,
                "PL": str,
                "APLICACAO_INICIAL_MINIMA": str,
                "PRAZO_RESGATE": str,
                "RENTABILIDADE_12M": str,
                "FUNDO_CASCA_NOME": str,
                "FUNDO_CASCA_CNPJ": str,
            }, 
            str_fmt_dt="DD/MM/YYYY",
            url=self.base_url,
        )
        self.cls_create_log.log_message(
            self.logger, 
            f"✅ Related structure dataframe standardized - Shape: {df_related.shape}, "
            f"Columns: {len(df_related.columns)}, "
            f"Rows: {len(df_related)}", 
            "info"
        )
        
        self.cls_create_log.log_message(
            self.logger, 
            "🔄 Starting standardization of about fund dataframe...", 
            "info"
        )
        df_about = self.standardize_dataframe(
            df_=df_about, 
            date_ref=self.date_ref,
            dict_dtypes={
                "FUND_CODE": str,
                "NOME_FUNDO": str,
                "CNPJ_FUNDO": str,
                "DATA_HORA_ATUALIZACAO_FUNDO_CASCA": str,
                "STATUS_FUNDO": str,
                "IS_ADAPTADO_ICVM175": str,
                "CODIGO_ANBIMA_FUNDO": str,
                "DATA_ENCERRAMENTO_FUNDO": "date",
                "TIPO_FUNDO": str,
                "MOEDA_COTACAO": str,
                "NOME_CASSE": str,
                "CNPJ_CLASSE": str,
                "DATA_HORA_ATUALIZACAO_CLASSE": str,
                "CODIGO_ANBIMA_CLASSE": str,
                "CATEGORIA_ANBIMA": str,
                "TIPO_ANBIMA": str,
                "STATUS_CLASSE": str,
                "DATA_INICIO_ATIVIDADE_CLASSE": "date",
                "DATA_ENCERRAMENTO_ATIVIDADE_CLASSE": "date",
                "CATEGORIA_CVM": str,
                "SUFIXO": str,
                "IS_ESG": str,
                "COMPOSICAO_FUNDO": str,
                "BENCHMARK": str,
                "FOCO_ATUACAO": str,
                "INVESTIMENTO_EXTERIOR": str,
                "PERCENTUAL_PERMITIDO_INVESTIMENTO_EXTERIOR": str,
                "CREDITO_PRIVADO": str,
                "IS_FUNDO_ALAVANCADO": str,
                "FORMA_CONDOMINIO": str,
                "RESPONSABILIDADE_LIMITADA": str,
                "TRIBUTACAO_PERSEGUIDA": str,
                "NOME_SUBCLASSE_FUNDO": str,
                "CNPJ_CODIGO_ANBIMA_SUBCLASSE": str,
                "DATA_ATAULIZACAO_SUBCLASSE": str,
                "CODIGO_ANBIMA_SUBCLASSE": str,
                "CODIGO_B3": str,
                "TIPO_INVESTIDOR": str,
                "STATUS_SUBCLASSE": str,
                "DATA_INICIO_ATIVIDADE_SUBCLASSE": "date",
                "DATA_ENCERRAMENTO_SUBCLASSE": "date",
                "RESTRICAO_INVESTIMENTO": str,
                "PERIODO_CALCULO_COTA": str,
                "APLICACO_AUTOMATICA": str,
                "PLANO_PREVIDENCIA": str,
                "DATA_HORA_ATUALIZACAO_MOVIMENTACAO": str,
                "APLICACO_MINIMA_INICIAL": str,
                "APLICACAO_ADICIONAL_MINIMA": str,
                "PRAZO_RESGATE": str,
                "PERIODO_PRAZO_RESGATE": str,
                "PRAZO_EMISSAO_COTAS": str,
                "PERIODO_PRAZO_EMISSAO_COTAS": str,
                "PRAZO_CONVERSAO_RESGATE": str,
                "PERIODO_CONVERSAO_RESGATE": str,
                "CARENCIA_INICIAL": str,
                "CARENCIA_CICLICA": str,
                "VALOR_MINIMO_RESGATE": str,
                "VALOR_MINIMO_PERMANENCIA": str,
                "DATA_HORA_ATUALIZACAO_TAXA": str,
                "TAXA_GLOBAL": str,
                "DATA_HORA_INICIO_VIGENCIA_TAXA_GLOBAL": str,
                "TAXA_GLOBAL_MAXIMA": str,
                "DATA_HORA_INICIO_VIGENCIA_TAXA_GLOBAL_MAXIMA": str,
                "UNIDADE_TAXA_GLOBAL": str,
                "PERFIL_TAXA_GLOBAL": str,
                "PERFIL_TAXA_PERFORMANCE": str,
                "PERIODICIDADE_TAXA_PERFORMANCE": str,
                "INFORMACOES_ADICIONAIS_TAXA_PERFORMANCE": str,
                "TAXA_ENTRADA": str,
                "TAXA_SAIDA": str,
                "DATA_HORA_ATUALIZACAO_PRESTADORES_FUNDO_CASCA": str,
                "ADMINISTRADOR_NOME": str,
                "ADMINISTRADOR_CNPJ": str,
                "ADMINISTRADOR_LINK_PERFIL_INSTITUICAO_ANBIMA": str,
                "GESTOR_NOME": str,
                "GESTOR_CNPJ": str,
                "DATA_HORA_ATUALIZACAO_PRESTADORES_CLASSE": str,
                "CONTROLADOR_CLASSE_NOME": str,
                "CONTROLADOR_CLASSE_CNPJ": str,
                "CUSTODIANTE_CLASSE_NOME": str,
                "CUSTODIANTE_CLASSE_CNPJ": str,
                "DATA_HORA_ATUALIZACAO_PRESTADORES_SUBCLASSE": str,
                "DISTRIBUIDOR_SUBCLASSE_NOME": str,
                "DISTRIBUIDOR_SUBCLASSE_CNPJ": str,
                "DATA_HORA_ATUALIZACAO_DOCUMENTOS": str,
                "LINK_REGULAMENTO": str,
            }, 
            str_fmt_dt="DD/MM/YYYY",
            url=self.base_url,
        )
        self.cls_create_log.log_message(
            self.logger, 
            f"✅ About fund dataframe standardized - Shape: {df_about.shape}, "
            f"Columns: {len(df_about.columns)}, "
            f"Rows: {len(df_about)}", 
            "info"
        )
        
        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db, 
                str_table_name=str_table_name_characteristics, 
                df_=df_characteristics, 
                bool_insert_or_ignore=bool_insert_or_ignore
            )
            self.insert_table_db(
                cls_db=self.cls_db, 
                str_table_name=str_table_name_related, 
                df_=df_related, 
                bool_insert_or_ignore=bool_insert_or_ignore
            )
            self.insert_table_db(
                cls_db=self.cls_db, 
                str_table_name=str_table_name_about, 
                df_=df_about, 
                bool_insert_or_ignore=bool_insert_or_ignore
            )
        else:
            return {
                "characteristics": df_characteristics,
                "related": df_related, 
                "about": df_about
            }

    def get_response(
        self, 
        timeout_ms: int = 30_000,
    ) -> dict[str, list]:
        """Scrape funds about information using Playwright.

        Parameters
        ----------
        timeout_ms : int
            The timeout in milliseconds, by default 30_000
        
        Returns
        -------
        dict[str, list]
            Dictionary containing three lists: characteristics, related, and about data.
        """
        list_characteristics_data: list[dict[str, Any]] = []
        list_related_data: list[dict[str, Any]] = []
        list_about_data: list[dict[str, Any]] = []
        
        if not self.list_fund_codes:
            self.cls_create_log.log_message(
                self.logger, 
                "⚠️ No fund codes provided. Cannot scrape about information.", 
                "warning"
            )
            return {
                "characteristics": list_characteristics_data,
                "related": list_related_data,
                "about": list_about_data
            }
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()

            self.cls_create_log.log_message(
                self.logger, 
                f"🚀 Starting about scraping for {len(self.list_fund_codes)} funds...", 
                "info"
            )
            
            for fund_code in self.list_fund_codes:
                self.cls_create_log.log_message(
                    self.logger, 
                    f"📊 Fetching about information for: {fund_code}...", 
                    "info"
                )
                
                try:
                    url = f"{self.base_url}/{fund_code}/sobre-o-fundo"
                    page.goto(url)
                    page.wait_for_timeout(timeout_ms)
                    
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"🔍 Extracting characteristics data for {fund_code}...", 
                        "info"
                    )
                    characteristics_data = self._extract_characteristics_data(
                        page, fund_code
                    )
                    list_characteristics_data.append(characteristics_data)
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"✅ Characteristics data extracted for {fund_code} - "
                        f"{len([v for v in characteristics_data.values() if v is not None])} "
                        "fields populated", 
                        "info"
                    )
                    
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"🔍 Extracting related structure data for {fund_code}...", 
                        "info"
                    )
                    related_data = self._extract_related_structure_data(
                        page, fund_code
                    )
                    list_related_data.extend(related_data)
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"✅ Related structure data extracted for {fund_code} - "
                        f"{len(related_data)} related funds/classes found", 
                        "info"
                    )
                    
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"🔍 Extracting about fund data for {fund_code}...", 
                        "info"
                    )
                    about_data = self._extract_about_fund_data(
                        page, fund_code
                    )
                    list_about_data.append(about_data)
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"✅ About fund data extracted for {fund_code} - "
                        f"{len([v for v in about_data.values() if v is not None])} "
                            "fields populated", 
                        "info"
                    )
                    
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"🎯 Successfully extracted all about information for {fund_code} - "
                        f"Characteristics: "
                            f"{len([v for v in characteristics_data.values() if v is not None])}"
                            f"/{len(characteristics_data)} fields, "
                        f"Related: {len(related_data)} items, "
                        f"About: {len([v for v in about_data.values() if v is not None])}"
                            f"/{len(about_data)} fields", 
                        "info"
                    )
                    
                except Exception as e:
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"❌ Error processing {fund_code}: {str(e)}", 
                        "error"
                    )
                
                time.sleep(randint(3, 8))  # noqa S311

            browser.close()

        self.cls_create_log.log_message(
            self.logger, 
            f"💾 About scraping finished. "
            f"Total - Characteristics: {len(list_characteristics_data)} funds, "
            f"Related: {len(list_related_data)} items, "
            f"About: {len(list_about_data)} funds", 
            "info"
        )
        
        return {
            "characteristics": list_characteristics_data,
            "related": list_related_data,
            "about": list_about_data
        }
    
    def _extract_characteristics_data(
        self, 
        page: PlaywrightPage, 
        fund_code: str
    ) -> dict[str, Any]:
        """Extract characteristics data using specific XPaths.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        fund_code : str
            The fund code.
        
        Returns
        -------
        dict[str, Any]
            Dictionary containing extracted characteristics data.
        """
        data = {"FUND_CODE": fund_code}

        xpath_mapping = {
            'NOME_FUNDO': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[1]/div/div[1]/div/div[2]/h2', # noqa E501: line too long
            'CLASSE_FUNDO': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[1]/div/div[1]/div/div[3]/p[1]', # noqa E501: line too long
            'CLASSE_ANBIMA_FUNDO': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[1]/div/div[1]/div/div[3]/p[2]', # noqa E501: line too long
            'STATUS_FUNDO': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[1]/div/div[1]/div/div[3]/p[3]', # noqa E501: line too long
            'CNPJ_COD_ANBIMA_FUNDO': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[1]/div/div[1]/div/div[3]/p[4]', # noqa E501: line too long
            'PL': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[2]/div/article[1]/p',
            'ULTIMA_COTA': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[2]/div/article[2]/p[1]', # noqa E501: line too long
            'DATA_ULTIMA_COTA': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[2]/div/article[2]/p[2]', # noqa E501: line too long
            'APLICACAO_MINIMA_INICIAL': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[2]/div/article[3]/p', # noqa E501: line too long
            'PRAZO_RESGATE': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[2]/div/article[4]/p[1]', # noqa E501: line too long
            'PERIODO_PRAZO_RESGATE': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[2]/div/article[4]/p[2]', # noqa E501: line too long
            'NUMERO_COTISTAS': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[2]/div/article[5]/p', # noqa E501: line too long
            'RENTABILIDADE_12M': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[2]/div/article[6]/p', # noqa E501: line too long
            'FUNDO_CASCA_NOME': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[3]/div/div/div/article[1]/div/article[1]/p[1]', # noqa E501: line too long
            'FUNDO_CASCA_CNPJ': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[3]/div/div/div/article[1]/div/article[1]/p[2]', # noqa E501: line too long
            'TIPO_FUNDO': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[3]/div/div/div/article[1]/div/article[2]/p', # noqa E501: line too long
            'ADMINISTRADOR_FUNDO': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[3]/div/div/div/article[1]/div/article[3]/p', # noqa E501: line too long
            'GESTOR_FUNDO': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[3]/div/div/div/article[1]/div/article[5]/p', # noqa E501: line too long
            'CLASSE_FUNDO_NOME': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[3]/div/div/div/article[2]/div/article[1]/p[1]', # noqa E501: line too long
            'CLASSE_FUNDO_CNPJ': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[3]/div/div/div/article[2]/div/article[1]/p[2]', # noqa E501: line too long
            'IS_ESG': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[3]/div/div/div/article[2]/div/article[1]/p[2]', # noqa E501: line too long
            'BENCHMARK': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[3]/div/div/div/article[2]/div/article[3]/p', # noqa E501: line too long
            'LINK_REGULAMENTO': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[1]/div/div[2]/a/@href', # noqa E501: line too long
            'SUBCLASSE_NOME_FUNDO': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[4]/div/div/div/article[3]/div/article/p[1]', # noqa E501: line too long
            'SUBCLASSE_CODIGO_ANBIMA': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[4]/div/div/div/article[3]/div/article/p[1]', # noqa E501: line too long
        }

        for field_name, xpath in xpath_mapping.items():
            try:
                if xpath.endswith('/@href'):
                    element = page.locator(f"xpath={xpath.replace('/@href', '')}").first
                    if element.count() > 0 and element.is_visible(timeout=5_000):
                        href = element.get_attribute('href')
                        data[field_name] = href if href else None
                    else:
                        data[field_name] = None
                else:
                    element = page.locator(f"xpath={xpath}").first
                    if element.count() > 0 and element.is_visible(timeout=5_000):
                        text = element.inner_text().strip()
                        data[field_name] = text if text else None
                    else:
                        data[field_name] = None
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger, 
                    f'Error extracting {field_name} for fund {fund_code}: {e}', 
                    "warning"
                )
                data[field_name] = None
        
        return data
    
    def _extract_related_structure_data(
        self, 
        page: PlaywrightPage, 
        fund_code: str
    ) -> list[dict[str, Any]]:
        """Extract related structure data by clicking the structure button.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        fund_code : str
            The fund code.
        
        Returns
        -------
        list[dict[str, Any]]
            list of dictionaries containing extracted related structure data.
        """
        list_related_data = []
        try:
            structure_button = page.locator('xpath=//*[@data-cy="estrutura-btn"]/span')
            if structure_button.count() > 0 and structure_button.is_visible(timeout=5_000):
                structure_button.click()
                page.wait_for_timeout(5_000)

                data_ref_elements = page.locator(
                    'xpath=//div[@class="EstruturaDrawer_drawer-content__3y7lB"]//a'
                ).all()

                if not data_ref_elements:
                    self.cls_create_log.log_message(
                        self.logger, 
                        f'No data references found for fund {fund_code}', 
                        "warning"
                    )
                    return list_related_data
                
                for row_idx in range(len(data_ref_elements)):
                    try:
                        list_related_data.append(self._extract_single_related_structure(
                            page, row_idx + 1, fund_code))
                    except Exception as e:
                        self.cls_create_log.log_message(
                            self.logger, 
                            f'Error extracting related structure for fund {fund_code}: {e}', 
                            "warning"
                        )
            else:
                self.cls_create_log.log_message(
                    self.logger, 
                    f'Structure button not found for fund {fund_code}', 
                    "warning"
                )
                
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Error extracting related structure for fund {fund_code}: {e}', 
                "warning"
            )
        
        return list_related_data
    
    def _extract_single_related_structure(
        self, 
        page: PlaywrightPage,
        row_idx: int,
        fund_code: str,
    ) -> dict[str, Any]:
        """Extract data from a single related structure element.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        row_idx : int
            The row index.
        fund_code : str
            The fund code.
        
        Returns
        -------
        dict[str, Any]
            Dictionary containing extracted related structure data.
        """
        data = {"FUND_CODE": fund_code}
        
        xpath_mapping = {
            'NOME_FUNDO': '//div[@class="EstruturaDrawer_drawer-content__3y7lB"]/a[{}]//*[@id="title"]', # noqa E501: line too long
            'TIPO_FUNDO': '//div[@class="EstruturaDrawer_drawer-content__3y7lB"]/a[{}]//div[@class="EstruturaDrawerCard_tags__0_r1w"]/p[1]', # noqa E501: line too long
            'STATUS_FUNDO': '//div[@class="EstruturaDrawer_drawer-content__3y7lB"]/a[{}]//div[@class="EstruturaDrawerCard_tags__0_r1w"]/p[2]', # noqa E501: line too long
            'CODIGO_ANBIMA': '//div[@class="EstruturaDrawer_drawer-content__3y7lB"]/a[{}]//span[@class="EstruturaDrawerCard_code__LEoiD"]', # noqa E501: line too long
            'PL': '//div[@class="EstruturaDrawer_drawer-content__3y7lB"]/a[{}]//div[@class="EstruturaDrawerCard_content__SRfh5"]/div[1]/p[2]', # noqa E501: line too long
            'APLICACAO_INICIAL_MINIMA': '//div[@class="EstruturaDrawer_drawer-content__3y7lB"]/a[{}]//div[@class="EstruturaDrawerCard_content__SRfh5"]/div[2]/p[2]', # noqa E501: line too long
            'PRAZO_RESGATE': '//div[@class="EstruturaDrawer_drawer-content__3y7lB"]/a[{}]//div[@class="EstruturaDrawerCard_content__SRfh5"]/div[1]/p[3]', # noqa E501: line too long
            'RENTABILIDADE_12M': '//div[@class="EstruturaDrawer_drawer-content__3y7lB"]/a[{}]//div[@class="EstruturaDrawerCard_content__SRfh5"]/div[1]/p[4]', # noqa E501: line too long
            'FUNDO_CASCA_NOME': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[3]/div/div/div/article[1]/div/article[1]/p[1]', # noqa E501: line too long
            'FUNDO_CASCA_CNPJ': '//*[@id="detalhes-do-fundo"]/div[1]/div/div/div[3]/div/div/div/article[1]/div/article[1]/p[2]', # noqa E501: line too long
        }

        for field_name, xpath in xpath_mapping.items():
            try:
                if xpath.endswith('/@href'):
                    element = page.locator(
                        f"xpath={xpath.format(row_idx).replace('/@href', '')}").first
                    if element.count() > 0 and element.is_visible(timeout=5_000):
                        href = element.get_attribute('href')
                        data[field_name] = href if href else None
                    else:
                        data[field_name] = None
                elif xpath.endswith('/text()[2]'):
                    element = page.locator(
                        f"xpath={xpath.format(row_idx).replace('/text()[2]', '')}").first
                    if element.count() > 0 and element.is_visible(timeout=5_000):
                        text = element.inner_text().strip()
                        parts = text.split('\n')
                        if len(parts) >= 2:
                            data[field_name] = parts[1].strip() if parts[1].strip() else None
                        else:
                            data[field_name] = text if text else None
                    else:
                        data[field_name] = None
                else:
                    element = page.locator(f"xpath={xpath.format(row_idx)}").first
                    if element.count() > 0 and element.is_visible(timeout=5_000):
                        text = element.inner_text().strip()
                        data[field_name] = text if text else None
                    else:
                        data[field_name] = None
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger, 
                    f'Error extracting {field_name} for fund {fund_code}: {e}', 
                    "warning"
                )
                data[field_name] = None
        
        return data
    
    def _extract_about_fund_data(
        self, 
        page: PlaywrightPage, 
        fund_code: str
    ) -> dict[str, Any]:
        """Extract about fund data using specific XPaths.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        fund_code : str
            The fund code.
        
        Returns
        -------
        dict[str, Any]
            Dictionary containing extracted about fund data.
        """
        data = {"FUND_CODE": fund_code}

        xpath_mapping = {
            'NOME_FUNDO': '//*[@id="fundoRef"]/article/h5',
            'CNPJ_FUNDO': '//*[@id="fundoRef"]/article/p[2]',
            'DATA_HORA_ATUALIZACAO_FUNDO_CASCA': '//*[@id="fundoRef"]/div/p/text()[2]',
            'STATUS_FUNDO': '//*[@id="fundoRef"]/ul/div/div/div/article/div/article[4]/p',
            'IS_ADAPTADO_ICVM175': '//*[@id="fundoRef"]/ul/div/div/div/article/div/article[7]/p',
            'CODIGO_ANBIMA_FUNDO': '//*[@id="fundoRef"]/ul/div/div/div/article/div/article[2]/p',
            'DATA_ENCERRAMENTO_FUNDO': '//*[@id="fundoRef"]/ul/div/div/div/article/div/article[5]/p', # noqa E501: line too long
            'TIPO_FUNDO': '//*[@id="fundoRef"]/ul/div/div/div/article/div/article[3]/p',
            'MOEDA_COTACAO': '//*[@id="fundoRef"]/ul/div/div/div/article/div/article[6]/p',
            'NOME_CASSE': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[1]/p',
            'CNPJ_CLASSE': '//*[@id="classeRef"]/article/p[2]',
            'DATA_HORA_ATUALIZACAO_CLASSE': '//*[@id="classeRef"]/div/p/text()[2]',
            'CODIGO_ANBIMA_CLASSE': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[2]/p', # noqa E501: line too long
            'CATEGORIA_ANBIMA': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[3]/p',
            'TIPO_ANBIMA': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[4]/p',
            'STATUS_CLASSE': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[5]/p',
            'DATA_INICIO_ATIVIDADE_CLASSE': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[6]/p', # noqa E501: line too long
            'DATA_ENCERRAMENTO_ATIVIDADE_CLASSE': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[7]/p', # noqa E501: line too long
            'CATEGORIA_CVM': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[8]/p',
            'SUFIXO': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[9]/p',
            'IS_ESG': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[10]/p',
            'COMPOSICAO_FUNDO': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[11]/p',
            'BENCHMARK': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[12]/p',
            'FOCO_ATUACAO': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[13]/p',
            'INVESTIMENTO_EXTERIOR': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[14]/p', # noqa E501: line too long
            'PERCENTUAL_PERMITIDO_INVESTIMENTO_EXTERIOR': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[15]/p', # noqa E501: line too long
            'CREDITO_PRIVADO': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[16]/p',
            'IS_FUNDO_ALAVANCADO': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[17]/p', # noqa E501: line too long
            'FORMA_CONDOMINIO': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[18]/p',
            'RESPONSABILIDADE_LIMITADA': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[19]/p', # noqa E501: line too long
            'TRIBUTACAO_PERSEGUIDA': '//*[@id="classeRef"]/ul/div/div/div/article/div/article[20]/p', # noqa E501: line too long
            'NOME_SUBCLASSE_FUNDO': '//*[@id="subclasseRef"]/article/h5',
            'CNPJ_CODIGO_ANBIMA_SUBCLASSE': '//*[@id="subclasseRef"]/article/p[2]',
            'DATA_ATAULIZACAO_SUBCLASSE': '//*[@id="subclasseRef"]/div/p/text()[2]',
            'CODIGO_ANBIMA_SUBCLASSE': '//*[@id="subclasseRef"]/ul/div/div/div/article/div/article[1]/p', # noqa E501: line too long
            'CODIGO_B3': '//*[@id="subclasseRef"]/ul/div/div/div/article/div/article[2]/p',
            'TIPO_INVESTIDOR': '//*[@id="subclasseRef"]/ul/div/div/div/article/div/article[3]/p | //*[@id="classeRef"]/ul/div/div/div/article/div/article[22]/p', # noqa E501: line too long
            'STATUS_SUBCLASSE': '//*[@id="subclasseRef"]/ul/div/div/div/article/div/article[4]/p',
            'DATA_INICIO_ATIVIDADE_SUBCLASSE': '//*[@id="subclasseRef"]/ul/div/div/div/article/div/article[5]/p', # noqa E501: line too long
            'DATA_ENCERRAMENTO_SUBCLASSE': '//*[@id="subclasseRef"]/ul/div/div/div/article/div/article[6]/p', # noqa E501: line too long
            'RESTRICAO_INVESTIMENTO': '//*[@id="subclasseRef"]/ul/div/div/div/article/div/article[7]/p | //*[@id="classeRef"]/ul/div/div/div/article/div/article[21]/p', # noqa E501: line too long
            'PERIODO_CALCULO_COTA': '//*[@id="subclasseRef"]/ul/div/div/div/article/div/article[8]/p | //*[@id="classeRef"]/ul/div/div/div/article/div/article[23]/p', # noqa E501: line too long
            'APLICACO_AUTOMATICA': '//*[@id="subclasseRef"]/ul/div/div/div/article/div/article[9]/p | //*[@id="classeRef"]/ul/div/div/div/article/div/article[24]/p', # noqa E501: line too long
            'PLANO_PREVIDENCIA': '//*[@id="subclasseRef"]/ul/div/div/div/article/div/article[10]/p | //*[@id="classeRef"]/ul/div/div/div/article/div/article[25]/p', # noqa E501: line too long
            'DATA_HORA_ATUALIZACAO_MOVIMENTACAO': '//*[@id="movimentacaoRef"]/div[1]/p/text()[2]',
            'APLICACO_MINIMA_INICIAL': '//*[@id="movimentacaoRef"]/div[2]/article[1]/h5',
            'APLICACAO_ADICIONAL_MINIMA': '//*[@id="movimentacaoRef"]/div[2]/article[2]/h5',
            'PRAZO_RESGATE': '//*[@id="movimentacaoRef"]/div[2]/article[3]/h5',
            'PERIODO_PRAZO_RESGATE': '//*[@id="movimentacaoRef"]/div[2]/article[3]/p[2]',
            'PRAZO_EMISSAO_COTAS': '//*[@id="movimentacaoRef"]/ul/div/div/div/article/div/article[1]/p[1]', # noqa E501: line too long
            'PERIODO_PRAZO_EMISSAO_COTAS': '//*[@id="movimentacaoRef"]/ul/div/div/div/article/div/article[1]/p[2]', # noqa E501: line too long
            'PRAZO_CONVERSAO_RESGATE': '//*[@id="movimentacaoRef"]/ul/div/div/div/article/div/article[2]/p[1]', # noqa E501: line too long
            'PERIODO_CONVERSAO_RESGATE': '//*[@id="movimentacaoRef"]/ul/div/div/div/article/div/article[2]/p[2]', # noqa E501: line too long
            'CARENCIA_INICIAL': '//*[@id="movimentacaoRef"]/ul/div/div/div/article/div/article[3]/p', # noqa E501: line too long
            'CARENCIA_CICLICA': '//*[@id="movimentacaoRef"]/ul/div/div/div/article/div/article[4]/p', # noqa E501: line too long
            'VALOR_MINIMO_RESGATE': '//*[@id="movimentacaoRef"]/ul/div/div/div/article/div/article[5]/p', # noqa E501: line too long
            'VALOR_MINIMO_PERMANENCIA': '//*[@id="movimentacaoRef"]/ul/div/div/div/article/div/article[6]/p', # noqa E501: line too long
            'DATA_HORA_ATUALIZACAO_TAXA': '//*[@id="taxasRef"]/div[1]/p/text()[2]',
            'TAXA_GLOBAL': '//*[@id="taxasRef"]/div[2]/article[1]/h5',
            'DATA_HORA_INICIO_VIGENCIA_TAXA_GLOBAL': '//*[@id="taxasRef"]/div[2]/article[1]/p[2]',
            'TAXA_GLOBAL_MAXIMA': '//*[@id="taxasRef"]/div[2]/article[2]/h5',
            'DATA_HORA_INICIO_VIGENCIA_TAXA_GLOBAL_MAXIMA': '//*[@id="taxasRef"]/div[2]/article[2]/p[2]', # noqa E501: line too long
            'UNIDADE_TAXA_GLOBAL': '//*[@id="taxasRef"]/ul/div/div/div/article/div/article[1]/p',
            'PERFIL_TAXA_GLOBAL': '//*[@id="taxasRef"]/ul/div/div/div/article/div/article[2]/p',
            'PERFIL_TAXA_PERFORMANCE': '//*[@id="taxasRef"]/ul/div/div/div/article/div/article[3]/p', # noqa E501: line too long
            'PERIODICIDADE_TAXA_PERFORMANCE': '//*[@id="taxasRef"]/ul/div/div/div/article/div/article[4]/p', # noqa E501: line too long
            'INFORMACOES_ADICIONAIS_TAXA_PERFORMANCE': '//*[@id="taxasRef"]/ul/div/div/div/article/div/article[5]/p', # noqa E501: line too long
            'TAXA_ENTRADA': '//*[@id="taxasRef"]/ul/div/div/div/article/div/article[6]/p',
            'TAXA_SAIDA': '//*[@id="taxasRef"]/ul/div/div/div/article/div/article[7]/p',
            'DATA_HORA_ATUALIZACAO_PRESTADORES_FUNDO_CASCA': '//*[@id="prestadoresRef"]/div[2]/article[1]/header/p', # noqa E501: line too long
            'ADMINISTRADOR_NOME': '//*[@id="prestadoresRef"]/div[2]/article[1]/div[1]/h5',
            'ADMINISTRADOR_CNPJ': '//*[@id="prestadoresRef"]/div[2]/article[1]/div[1]/p[2]',
            'ADMINISTRADOR_LINK_PERFIL_INSTITUICAO_ANBIMA': '//*[@id="prestadoresRef"]/div[2]/article[1]/div[1]/a/@href', # noqa E501: line too long
            'GESTOR_NOME': '//*[@id="prestadoresRef"]/div[2]/article[1]/div[2]/h5',
            'GESTOR_CNPJ': '//*[@id="prestadoresRef"]/div[2]/article[1]/div[2]/p[2]',
            'DATA_HORA_ATUALIZACAO_PRESTADORES_CLASSE': '//*[@id="prestadoresRef"]/div[2]/article[2]/header/p', # noqa E501: line too long
            'CONTROLADOR_CLASSE_NOME': '//*[@id="prestadoresRef"]/div[2]/article[2]/div[1]/h5',
            'CONTROLADOR_CLASSE_CNPJ': '//*[@id="prestadoresRef"]/div[2]/article[2]/div[1]/p[2]',
            'CUSTODIANTE_CLASSE_NOME': '//*[@id="prestadoresRef"]/div[2]/article[2]/div[2]/h5',
            'CUSTODIANTE_CLASSE_CNPJ': '//*[@id="prestadoresRef"]/div[2]/article[2]/div[2]/p[2]',
            'DATA_HORA_ATUALIZACAO_PRESTADORES_SUBCLASSE': '//*[@id="prestadoresRef"]/div[2]/article[3]/header/p', # noqa E501: line too long
            'DISTRIBUIDOR_SUBCLASSE_NOME': '//*[@id="prestadoresRef"]/div[2]/article[3]/div/h5',
            'DISTRIBUIDOR_SUBCLASSE_CNPJ': '//*[@id="prestadoresRef"]/div[2]/article[3]/div/p[2]',
            'DATA_HORA_ATUALIZACAO_DOCUMENTOS': '//*[@id="documentosRef"]/div/p',
            'LINK_REGULAMENTO': '//*[@id="documentosRef"]/a/@href',
        }

        for field_name, xpath in xpath_mapping.items():
            try:
                if xpath.endswith('/@href'):
                    element = page.locator(f"xpath={xpath.replace('/@href', '')}").first
                    if element.count() > 0 and element.is_visible(timeout=5_000):
                        href = element.get_attribute('href')
                        data[field_name] = href if href else None
                    else:
                        data[field_name] = None
                elif xpath.endswith('/text()[2]'):
                    element = page.locator(f"xpath={xpath.replace('/text()[2]', '')}").first
                    if element.count() > 0 and element.is_visible(timeout=5_000):
                        text = element.inner_text().strip()
                        parts = text.split('\n')
                        if len(parts) >= 2:
                            data[field_name] = parts[1].strip() if parts[1].strip() else None
                        else:
                            data[field_name] = text if text else None
                    else:
                        data[field_name] = None
                else:
                    element = page.locator(f"xpath={xpath}").first
                    if element.count() > 0 and element.is_visible(timeout=5_000):
                        text = element.inner_text().strip()
                        data[field_name] = text if text else None
                    else:
                        data[field_name] = None
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger, 
                    f'Error extracting {field_name} for fund {fund_code}: {e}', 
                    "warning"
                )
                data[field_name] = None
        
        if data.get('DATA_ENCERRAMENTO_FUNDO') == '-':
            data['DATA_ENCERRAMENTO_FUNDO'] = '01/01/2100'
        
        return data
    
    def _handle_date_value(self, date_str: Optional[str]) -> str:
        """Handle date values, replacing '-' with '01/01/2100'.
        
        Parameters
        ----------
        date_str : Optional[str]
            The date string to process.
        
        Returns
        -------
        str
            The processed date string.
        """
        if not date_str or date_str == '-':
            return '01/01/2100'
        return date_str
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> StringIO:
        """Parse the raw file content.
        
        This method is kept for compatibility but not used in web scraping.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The parsed content.
        """
        return StringIO()
    
    def transform_data(
        self, 
        file: StringIO
    ) -> pd.DataFrame:
        """Transform a response object into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return pd.DataFrame()
    
    def transform_characteristics_data(
        self, 
        raw_data: list[dict[str, Any]]
    ) -> pd.DataFrame:
        """Transform scraped characteristics data into a DataFrame.
        
        Parameters
        ----------
        raw_data : list[dict[str, Any]]
            The scraped characteristics data list.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
        
        df_ = pd.DataFrame(raw_data)
        
        date_columns = ['DATA_ULTIMA_COTA']
        for col in date_columns:
            if col in df_.columns:
                df_[col] = df_[col].apply(self._handle_date_value)
        
        return df_
    
    def transform_related_data(
        self, 
        raw_data: list[dict[str, Any]]
    ) -> pd.DataFrame:
        """Transform scraped related structure data into a DataFrame.
        
        Parameters
        ----------
        raw_data : list[dict[str, Any]]
            The scraped related structure data list.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
        
        return pd.DataFrame(raw_data)
    
    def transform_about_data(
        self, 
        raw_data: list[dict[str, Any]]
    ) -> pd.DataFrame:
        """Transform scraped about fund data into a DataFrame.
        
        Parameters
        ----------
        raw_data : list[dict[str, Any]]
            The scraped about fund data list.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
        
        df_ = pd.DataFrame(raw_data)
        
        date_columns = [
            'DATA_ENCERRAMENTO_FUNDO',
            'DATA_INICIO_ATIVIDADE_CLASSE',
            'DATA_ENCERRAMENTO_ATIVIDADE_CLASSE',
            'DATA_INICIO_ATIVIDADE_SUBCLASSE',
            'DATA_ENCERRAMENTO_SUBCLASSE'
        ]
        for col in date_columns:
            if col in df_.columns:
                df_[col] = df_[col].apply(self._handle_date_value)
        
        return df_


class AnbimaDataFundsHistoric(ABCIngestionOperations):
    """Anbima Funds Historic ingestion class."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        list_fund_codes: Optional[list[str]] = None,
    ) -> None:
        """Initialize the Anbima Funds Historic ingestion class.
        
        Parameters
        ----------
        date_ref : Optional[date]
            The date of reference, by default None.
        logger : Optional[Logger]
            The logger, by default None.
        cls_db : Optional[Session]
            The database session, by default None.
        list_fund_codes : Optional[list[str]]
            List of fund codes to scrape historic information for, by default None.
        
        Returns
        -------
        None
        
        Notes
        -----
        [1] Metadata: https://data.anbima.com.br/fundos/{fund_code}/historico
        """
        super().__init__(cls_db=cls_db)
        CoreIngestion.__init__(self)
        ContentParser.__init__(self)

        self.logger = logger
        self.cls_db = cls_db
        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()
        self.cls_create_log = CreateLog()
        self.cls_dates_br = DatesBRAnbima()
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.base_url = "https://data.anbima.com.br/fundos"
        self.list_fund_codes = list_fund_codes or []
    
    def run(
        self,
        timeout_ms: int = 30_000,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_anbimadata_funds_historic"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout_ms : int
            The timeout in milliseconds, by default 30_000
        bool_insert_or_ignore : bool
            Whether to insert or ignore the data, by default False
        str_table_name : str
            The name of the table, by default "br_anbimadata_funds_historic"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        raw_data = self.get_response(timeout_ms=timeout_ms)
        df_ = self.transform_data(raw_data=raw_data)
        
        self.cls_create_log.log_message(
            self.logger, 
            f"📊 Initial DataFrame shape - Historic: {df_.shape}", 
            "info"
        )
        
        self.cls_create_log.log_message(
            self.logger, 
            "🔄 Starting standardization of historic dataframe...", 
            "info"
        )
        
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.date_ref,
            dict_dtypes={
                "FUND_CODE": str,
                "DATA_HORA_ATUALIZACAO": str,
                "DATA_COMPETENCIA": "date",
                "PL": str,
                "VALOR_COTA": str,
                "VOLUME_TOTAL_APLICACOES": str,
                "VOLUME_TOTAL_RESGATES": str,
                "NUMERO_COTISTAS": str,
            }, 
            str_fmt_dt="DD/MM/YYYY",
            url=self.base_url,
        )
        
        self.cls_create_log.log_message(
            self.logger, 
            f"✅ Historic dataframe standardized - Shape: {df_.shape}, "
            f"Columns: {len(df_.columns)}, "
            f"Rows: {len(df_)}", 
            "info"
        )
        
        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db, 
                str_table_name=str_table_name, 
                df_=df_, 
                bool_insert_or_ignore=bool_insert_or_ignore
            )
        else:
            return df_

    def get_response(
        self, 
        timeout_ms: int = 30_000,
    ) -> list[dict[str, Any]]:
        """Scrape funds historic information using Playwright.

        Parameters
        ----------
        timeout_ms : int
            The timeout in milliseconds, by default 30_000
        
        Returns
        -------
        list[dict[str, Any]]
            List of dictionaries containing historic data for all funds.
        """
        list_historic_data: list[dict[str, Any]] = []
        
        if not self.list_fund_codes:
            self.cls_create_log.log_message(
                self.logger, 
                "⚠️ No fund codes provided. Cannot scrape historic information.", 
                "warning"
            )
            return list_historic_data
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()

            self.cls_create_log.log_message(
                self.logger, 
                f"🚀 Starting historic scraping for {len(self.list_fund_codes)} funds...", 
                "info"
            )
            
            for fund_code in self.list_fund_codes:
                self.cls_create_log.log_message(
                    self.logger, 
                    f"📊 Fetching historic information for: {fund_code}...", 
                    "info"
                )
                
                try:
                    url = f"{self.base_url}/{fund_code}/dados-periodicos"
                    page.goto(url)
                    page.wait_for_timeout(timeout_ms)
                    
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"🔍 Extracting historic data for {fund_code}...", 
                        "info"
                    )
                    
                    historic_data = self._extract_historic_data(page, fund_code)
                    list_historic_data.extend(historic_data)
                    
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"✅ Historic data extracted for {fund_code} - "
                        f"{len(historic_data)} records found", 
                        "info"
                    )
                    
                except Exception as e:
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"❌ Error processing {fund_code}: {str(e)}", 
                        "error"
                    )
                
                time.sleep(randint(3, 8))  # noqa S311

            browser.close()

        self.cls_create_log.log_message(
            self.logger, 
            f"💾 Historic scraping finished. Total: {len(list_historic_data)} records found.", 
            "info"
        )
        
        return list_historic_data
    
    def _extract_historic_data(
        self, 
        page: PlaywrightPage, 
        fund_code: str
    ) -> list[dict[str, Any]]:
        """Extract historic data from the table.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        fund_code : str
            The fund code.
        
        Returns
        -------
        list[dict[str, Any]]
            List of dictionaries containing historic data records.
        """
        list_records = []
        
        try:
            data_hora_atualizacao = self._extract_update_timestamp(page)
            
            rows = page.locator(
                'xpath=//*[@id="detalhes-do-fundo"]/div[3]/div/div/div/div/section/div/table/tbody/tr' # noqa E501: line too long
            ).all()
            
            self.cls_create_log.log_message(
                self.logger, 
                f"📋 Found {len(rows)} historic records for fund {fund_code}", 
                "info"
            )
            
            for idx, row in enumerate(rows):
                try:
                    record = self._extract_row_data(
                        row, 
                        fund_code, 
                        data_hora_atualizacao,
                        idx
                    )
                    list_records.append(record)
                except Exception as e:
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"⚠️ Error extracting row {idx} for fund {fund_code}: {e}", 
                        "warning"
                    )
            
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f"❌ Error extracting historic data for fund {fund_code}: {e}", 
                "error"
            )
        
        return list_records
    
    def _extract_update_timestamp(self, page: PlaywrightPage) -> Optional[str]:
        """Extract the update timestamp from the page.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        
        Returns
        -------
        Optional[str]
            The update timestamp or None.
        """
        try:
            element = page.locator(
                'xpath=//*[@id="detalhes-do-fundo"]/div[3]/div/div/div/div/div[1]/p'
            ).first
            
            if element.count() > 0 and element.is_visible(timeout=5_000):
                text = element.inner_text().strip()
                parts = text.split('\n')
                if len(parts) >= 2:
                    return parts[1].strip() if parts[1].strip() else None
                return text if text else None
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f"⚠️ Error extracting update timestamp: {e}", 
                "warning"
            )
        
        return None
    
    def _extract_row_data(
        self, 
        row: Locator, 
        fund_code: str, 
        data_hora_atualizacao: Optional[str],
        idx: int
    ) -> dict[str, Any]:
        """Extract data from a single table row.
        
        Parameters
        ----------
        row : Locator
            The table row element.
        fund_code : str
            The fund code.
        data_hora_atualizacao : Optional[str]
            The update timestamp.
        idx : int
            The row index.
        
        Returns
        -------
        dict[str, Any]
            Dictionary containing extracted row data.
        """
        data = {
            "FUND_CODE": fund_code,
            "DATA_HORA_ATUALIZACAO": data_hora_atualizacao
        }
        
        columns = {
            "DATA_COMPETENCIA": 1,
            "PL": 2,
            "VALOR_COTA": 3,
            "VOLUME_TOTAL_APLICACOES": 4,
            "VOLUME_TOTAL_RESGATES": 5,
            "NUMERO_COTISTAS": 6
        }
        
        for field_name, col_index in columns.items():
            try:
                cell = row.locator(f'xpath=./td[{col_index}]').first
                if cell.count() > 0:
                    text = cell.inner_text().strip()
                    data[field_name] = text if text else None
                else:
                    data[field_name] = None
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger, 
                    f"⚠️ Error extracting {field_name} from row {idx}: {e}", 
                    "warning"
                )
                data[field_name] = None
        
        return data
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> StringIO:
        """Parse the raw file content.
        
        This method is kept for compatibility but not used in web scraping.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The parsed content.
        """
        return StringIO()
    
    def transform_data(
        self, 
        raw_data: list[dict[str, Any]]
    ) -> pd.DataFrame:
        """Transform scraped historic data into a DataFrame.
        
        Parameters
        ----------
        raw_data : list[dict[str, Any]]
            The scraped historic data list.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
        
        df_ = pd.DataFrame(raw_data)
        
        date_columns = ['DATA_COMPETENCIA']
        for col in date_columns:
            if col in df_.columns:
                df_[col] = df_[col].apply(self._handle_date_value)
        
        return df_
    
    def _handle_date_value(self, date_str: Optional[str]) -> str:
        """Handle date values, replacing '-' or None with '01/01/2100'.
        
        Parameters
        ----------
        date_str : Optional[str]
            The date string to process.
        
        Returns
        -------
        str
            The processed date string.
        """
        if not date_str or date_str == '-':
            return '01/01/2100'
        return date_str