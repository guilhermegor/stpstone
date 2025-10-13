"""Implementation of Anbima CRI/CRA ingestion instance."""

from contextlib import suppress
from datetime import date
from io import StringIO
from logging import Logger
from random import randint
import re
import time
from typing import Any, Optional, TypedDict, Union
from urllib.parse import urljoin

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


class ResultDocumentRecord(TypedDict):
    """Result Document Record."""

    COD_ATIVO: str
    IS_CRI_CRA: str
    SECAO: str
    SUBSECAO: str
    NOME_DOCUMENTO: str
    DATA_DIVULGACAO_DOCUMENTO: str
    LINK_DOCUMENTO: str


class AnbimaDataCRICRACharacteristics(ABCIngestionOperations):
    """Anbima CRI/CRA characteristics ingestion class."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        start_page: int = 0,
        end_page: Optional[int] = None,
    ) -> None:
        """Initialize the Anbima CRI/CRA characteristics ingestion class.
        
        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.
        start_page : int, optional
            Starting page number, by default 0.
        end_page : Optional[int], optional
            Ending page number (inclusive), by default None.
            If None, will automatically detect the last page number.
        
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
        [1] Metadata: https://data.anbima.com.br/busca/certificado-de-recebiveis
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
        self.base_url = "https://data.anbima.com.br/busca/certificado-de-recebiveis"
        
        if start_page < 0:
            raise ValueError("start_page must be greater than or equal to 0")
        if end_page is not None and end_page < start_page:
            raise ValueError("end_page must be greater than or equal to start_page")
        
        self.start_page = start_page
        self.end_page = end_page
    
    def run(
        self,
        timeout_ms: int = 30_000,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_anbimadata_cri_cra_characteristics"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout_ms : int, optional
            The timeout in milliseconds, by default 30_000
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "br_anbimadata_cri_cra_characteristics"

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
                "CODIGO_EMISSAO": str,
                "NOME_EMISSOR": str,
                "IS_CRI_CRA": str,
                "DEVEDOR": str,
                "SERIE_EMISSAO": str,
                "REMUNERACAO": str,
                "DURATION": str,
                "SECURITIZADORA": str,
                "DATA_EMISSAO": "date",
                "DATA_VENCIMENTO": "date",
                "PU_INDICATIVO": str,
                "LINK_CARACTERISTICAS_INDIVIDUAIS": str,
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
        """Scrape CRI/CRA data using Playwright.

        Parameters
        ----------
        timeout_ms : int, optional
            The timeout in milliseconds, by default 30_000
        
        Returns
        -------
        list
            List of scraped CRI/CRA data.
        """
        list_pages_data: list[dict[str, Union[str, int, float, date]]] = []
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()
            
            if self.end_page is None:
                self.cls_create_log.log_message(
                    self.logger, 
                    "🔍 Detecting total number of pages...", 
                    "info"
                )
                
                url = f"{self.base_url}?size=100&page={self.start_page}"
                page.goto(url)
                page.wait_for_timeout(timeout_ms)
                
                self.end_page = self._get_total_pages(page)
                
                self.cls_create_log.log_message(
                    self.logger, 
                    f"📊 Total pages detected: {self.end_page}", 
                    "info"
                )

            self.cls_create_log.log_message(
                self.logger, 
                f"🚀 Starting CRI/CRA scraping from pages {self.start_page} to {self.end_page}...", 
                "info"
            )
            
            for page_num in range(self.start_page, self.end_page + 1):
                self.cls_create_log.log_message(
                    self.logger, 
                    f"📄 Fetching page {page_num}...", 
                    "info"
                )
                
                url = f"{self.base_url}?size=100&page={page_num}"
                page.goto(url)
                page.wait_for_timeout(timeout_ms)
                
                elementos_titulo = page.query_selector_all('[id^="item-title-"]')
                
                list_page_data: list[dict[str, Union[str, int, float, date]]] = []
                for elemento in elementos_titulo:
                    titulo_texto = elemento.inner_text().strip()
                    if titulo_texto:
                        item_id = elemento.get_attribute("id")
                        if item_id:
                            id_number = item_id.replace("item-title-", "")
                            
                            cri_cra_data = self._extract_cri_cra_data(
                                page, id_number)
                            cri_cra_data["pagina"] = page_num
                            list_page_data.append(cri_cra_data)
                            
                self.cls_create_log.log_message(
                    self.logger, 
                    f"✅ Page {page_num}: {len(list_page_data)} items", 
                    "info"
                )
                
                list_pages_data.extend(list_page_data)
                
                if page_num < self.end_page:
                    time.sleep(randint(2, 10))  # noqa S311
            
            browser.close()
        
        self.cls_create_log.log_message(
            self.logger, 
            f"💾 Scraping finished. Total: {len(list_pages_data)} records found.", 
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
            The total number of pages, defaults to start_page if not found.
        """
        try:
            xpath_total_pages = '//*[@id="pagination"]/div[3]/span/span'
            element = page.locator(f'xpath={xpath_total_pages}').first
            
            if element.count() > 0 and element.is_visible(timeout=5_000):
                total_pages_text = element.inner_text().strip()
                match = re.search(r'(\d+)', total_pages_text)
                if match:
                    total_pages = int(match.group(1))
                    return total_pages - 1
                    
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Could not determine total pages, using start_page as default: {e}', 
                "warning"
            )
        
        return self.start_page
    
    def _extract_cri_cra_data(
        self, 
        page: PlaywrightPage, 
        id_number: str
    ) -> dict:
        """Extract CRI/CRA data using specific XPaths.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        id_number : str
            The ID number of the CRI/CRA item.
        
        Returns
        -------
        dict
            Dictionary containing extracted CRI/CRA data.
        """
        data = {}
        
        titulo_data = self._extract_titulo_info(page, id_number)
        data.update(titulo_data)
        
        tipo_data = self._extract_tipo(page, id_number)
        data.update(tipo_data)
        
        xpath_mapping = {
            'DEVEDOR': f'[id="cri-cra-item-devedor-{id_number}"] dd',
            'SERIE_EMISSAO': f'[id="cri-cra-item-serie-emissao-{id_number}"] dd',
            'REMUNERACAO': f'[id="cri-cra-item-Remuneração-{id_number}"] dd',
            'DURATION': f'[id="cri-cra-item-duration-{id_number}"] dd',
            'SECURITIZADORA': f'[id="cri-cra-item-securitizadora-{id_number}"] dd',
            'DATA_EMISSAO': f'[id="cri-cra-item-data-emissão-{id_number}"] dd',
            'DATA_VENCIMENTO': f'[id="cri-cra-item-data-vencimento-{id_number}"] dd',
        }

        for field_name, selector in xpath_mapping.items():
            try:
                element = page.query_selector(selector)
                if element:
                    text = element.inner_text().strip()
                    data[field_name] = text if text else None
                else:
                    data[field_name] = None
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger, 
                    f'Erro ao extrair {field_name} para ID {id_number}: {e}', 
                    "warning"
                )
                data[field_name] = None
                
        pu_link_data = self._extract_pu_and_link(page, id_number)
        data.update(pu_link_data)
        
        return data
    
    def _extract_titulo_info(
        self, 
        page: PlaywrightPage, 
        id_number: str
    ) -> dict:
        """Extract CODIGO_EMISSAO and NOME_EMISSOR from title.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        id_number : str
            The ID number of the item.
        
        Returns
        -------
        dict
            Dictionary with CODIGO_EMISSAO and NOME_EMISSOR.
        """
        result = {
            "CODIGO_EMISSAO": None,
            "NOME_EMISSOR": None
        }
        
        try:
            xpath_codigo = f'//*[@id="item-title-{id_number}"]/span/span[1]'
            element_codigo = page.locator(f'xpath={xpath_codigo}').first
            
            if element_codigo.count() > 0:
                codigo_texto = element_codigo.inner_text().strip()
                if codigo_texto:
                    result["CODIGO_EMISSAO"] = codigo_texto
                    
            xpath_nome = f'//*[@id="item-title-{id_number}"]/span/span[3]'
            element_nome = page.locator(f'xpath={xpath_nome}').first
            
            if element_nome.count() > 0:
                nome_texto = element_nome.inner_text().strip()
                if nome_texto:
                    result["NOME_EMISSOR"] = nome_texto
                    
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Erro ao extrair título para ID {id_number}: {e}', 
                "warning"
            )
        
        return result
    
    def _extract_tipo(
        self, 
        page: PlaywrightPage, 
        id_number: str
    ) -> dict:
        """Extract IS_CRI_CRA (tipo) from label.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        id_number : str
            The ID number of the item.
        
        Returns
        -------
        dict
            Dictionary with IS_CRI_CRA.
        """
        result = {
            "IS_CRI_CRA": None
        }
        
        try:
            xpath_tipo = f'//*[@id="tipo-{id_number}"]/label'
            element = page.locator(f'xpath={xpath_tipo}').first
            
            if element.count() > 0:
                tipo_texto = element.inner_text().strip()
                if tipo_texto:
                    result["IS_CRI_CRA"] = tipo_texto
                    
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Erro ao extrair tipo para ID {id_number}: {e}', 
                "warning"
            )
        
        return result
    
    def _extract_pu_and_link(
        self, 
        page: PlaywrightPage, 
        id_number: str
    ) -> dict:
        """Extract PU_INDICATIVO and LINK_CARACTERISTICAS_INDIVIDUAIS.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        id_number : str
            The ID number of the item.
        
        Returns
        -------
        dict
            Dictionary with PU_INDICATIVO and LINK_CARACTERISTICAS_INDIVIDUAIS.
        """
        result = {
            "PU_INDICATIVO": None,
            "LINK_CARACTERISTICAS_INDIVIDUAIS": None
        }
        
        try:
            xpath_link = f'//*[@id="cri-cra-item-pu-indicativo-{id_number}"]/dd/span/a'
            element = page.locator(f'xpath={xpath_link}').first
            
            if element.count() > 0:
                pu_text = element.inner_text().strip()
                result["PU_INDICATIVO"] = pu_text if pu_text else None
                
                href = element.get_attribute('href')
                if href:
                    if href.endswith('/precos'):
                        href = href[:-7]
                        
                    if href.startswith('/'):
                        result["LINK_CARACTERISTICAS_INDIVIDUAIS"] = \
                            urljoin('https://data.anbima.com.br', href)
                    else:
                        result["LINK_CARACTERISTICAS_INDIVIDUAIS"] = href
                        
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Erro ao extrair PU e Link para ID {id_number}: {e}', 
                "warning"
            )
        
        return result
    
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
        """Transform scraped CRI/CRA data into a DataFrame.
        
        Parameters
        ----------
        raw_data : list
            The scraped CRI/CRA data list.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
        
        df_ = pd.DataFrame(raw_data)
        
        if 'pagina' in df_.columns:
            df_ = df_.drop('pagina', axis=1)
            
        for col_ in ["DATA_EMISSAO", "DATA_VENCIMENTO"]:
            if col_ in df_.columns:
                df_[col_] = df_[col_].apply(
                    lambda x: x.replace("-", "01/01/2100") 
                        if x and isinstance(x, str) else "01/01/2100"
                )
        
        return df_
    

class AnbimaDataCRICRAPricesWS(ABCIngestionOperations):
    """Anbima CRI/CRA prices ingestion class."""
    

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        start_page: int = 0,
        end_page: Optional[int] = None,
    ) -> None:
        """Initialize the Anbima CRI/CRA prices ingestion class.
        
        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.
        start_page : int, optional
            Starting page number, by default 0.
        end_page : Optional[int], optional
            Ending page number (inclusive), by default None.
            If None, will automatically detect the last page number.
        
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
        [1] Metadata: https://data.anbima.com.br/busca/certificado-de-recebiveis
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
        self.base_url = "https://data.anbima.com.br/busca/certificado-de-recebiveis"
        
        if start_page < 0:
            raise ValueError("start_page must be greater than or equal to 0")
        if end_page is not None and end_page < start_page:
            raise ValueError("end_page must be greater than or equal to start_page")
        
        self.start_page = start_page
        self.end_page = end_page
    
    def run(
        self,
        timeout_ms: int = 30_000,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_anbimadata_cri_cra_prices"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout_ms : int, optional
            The timeout in milliseconds, by default 30_000
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "br_anbimadata_cri_cra_prices"

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
                "DATA_REFERENCIA": "date",
                "CODIGO_EMISSAO": str,
                "NOME_DEVEDOR_ESTENDIDO": str,
                "IS_CRI_CRA": str,
                "NOME_EMISSOR": str,
                "SERIE_EMISSAO": str,
                "REMUNERACAO": str,
                "DURATION": str,
                "PU_INDICATIVO": str,
                "DEVEDOR": str,
                "TAXA_COMPRA": str,
                "TAXA_VENDA": str,
                "PCT_PU_PAR": str,
                "PU_PAR": str,
                "DATA_EMISSAO": "date",
                "DATA_VENCIMENTO": "date",
                "TAXA_INDICATIVA": str,
                "DESVIO_PADRAO": str,
                "PCT_REUNE": str,
                "DATA_REFERENCIA_NTNB": "date",
                "LINK_CARACTERISTICAS_INDIVIDUAIS": str,
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
        """Scrape CRI/CRA prices using Playwright.

        Parameters
        ----------
        timeout_ms : int, optional
            The timeout in milliseconds, by default 30_000
        
        Returns
        -------
        list
            List of scraped CRI/CRA prices data.
        """
        list_prices_data: list[dict[str, Union[str, int, float, date]]] = []
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()
            
            if self.end_page is None:
                self.cls_create_log.log_message(
                    self.logger, 
                    "🔍 Detecting total number of pages...", 
                    "info"
                )
                
                url = f"{self.base_url}?view=precos&page={self.start_page}&q=&size=100"
                page.goto(url)
                page.wait_for_timeout(timeout_ms)
                
                self._click_precos_button(page, timeout_ms)
                
                self.end_page = self._get_total_pages(page)
                
                self.cls_create_log.log_message(
                    self.logger, 
                    f"📊 Total pages detected: {self.end_page}", 
                    "info"
                )

            self.cls_create_log.log_message(
                self.logger, 
                f"🚀 Starting CRI/CRA prices scraping from pages {self.start_page} "
                f"to {self.end_page}...", 
                "info"
            )
            
            for page_num in range(self.start_page, self.end_page + 1):
                self.cls_create_log.log_message(
                    self.logger, 
                    f"📄 Fetching page {page_num}...", 
                    "info"
                )
                
                url = f"{self.base_url}?view=precos&page={page_num}&q=&size=100"
                page.goto(url)
                page.wait_for_timeout(timeout_ms)
                
                self._click_precos_button(page, timeout_ms)
                
                data_referencia = self._extract_data_referencia(page)
                
                price_articles = page.query_selector_all(
                    'xpath=//*[@id="__next"]/main/div/div/div/div/div[3]/article'
                )
                
                list_page_data: list[dict[str, Union[str, int, float, date]]] = []
                for idx, article in enumerate(price_articles):
                    try:
                        price_data = self._extract_price_data(
                            page, idx + 1, data_referencia
                        )
                        price_data["pagina"] = page_num
                        list_page_data.append(price_data)
                    except Exception as e:
                        self.cls_create_log.log_message(
                            self.logger, 
                            f'Error extracting price data for item {idx + 1}: {e}', 
                            "warning"
                        )
                            
                self.cls_create_log.log_message(
                    self.logger, 
                    f"✅ Page {page_num}: {len(list_page_data)} items", 
                    "info"
                )
                
                list_prices_data.extend(list_page_data)
                
                if page_num < self.end_page:
                    time.sleep(randint(2, 10))  # noqa S311
            
            browser.close()
        
        self.cls_create_log.log_message(
            self.logger, 
            f"💾 Prices scraping finished. Total: {len(list_prices_data)} records found.", 
            "info"
        )
        
        return list_prices_data
    
    def _click_precos_button(
        self, 
        page: PlaywrightPage, 
        timeout_ms: int
    ) -> None:
        """Click on the Preços button to activate prices view.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        timeout_ms : int
            Timeout in milliseconds.
        """
        try:
            xpath_button = """
            //*[@id="__next"]/main/div/div/div/div/div[1]/div[2]/div[2]/div/div/button[1]
            """
            button_xpath = page.locator(f'xpath={xpath_button}').first
            
            if button_xpath.count() > 0 and button_xpath.is_visible(timeout=5_000):
                button_xpath.click()
                page.wait_for_timeout(2_000)
                
                self.cls_create_log.log_message(
                    self.logger, 
                    "✅ Clicked on Preços button (method 1: XPath)", 
                    "info"
                )
                return
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Method 1 failed: {e}', 
                "warning"
            )
        
        try:
            button_selector = 'button.anbima-ui-button--secondary'
            button = page.locator(button_selector).filter(has_text="Preços").first
            
            if button.count() > 0 and button.is_visible(timeout=5_000):
                button.click()
                page.wait_for_timeout(2_000)
                
                self.cls_create_log.log_message(
                    self.logger, 
                    "✅ Clicked on Preços button (method 2: CSS selector)", 
                    "info"
                )
                return
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Method 2 failed: {e}', 
                "warning"
            )
        
        try:
            button_text = page.get_by_role("button", name="Preços").first
            
            if button_text.count() > 0 and button_text.is_visible(timeout=5_000):
                button_text.click()
                page.wait_for_timeout(2_000)
                
                self.cls_create_log.log_message(
                    self.logger, 
                    "✅ Clicked on Preços button (method 3: aria role)", 
                    "info"
                )
                return
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Method 3 failed: {e}', 
                "warning"
            )
        
        self.cls_create_log.log_message(
            self.logger, 
            '❌ Could not click Preços button with any method', 
            "error"
        )
    
    def _get_total_pages(self, page: PlaywrightPage) -> int:
        """Get the total number of pages from pagination.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        
        Returns
        -------
        int
            The total number of pages, defaults to start_page if not found.
        """
        try:
            xpath_total_pages = '//*[@id="pagination"]/div[3]/span/span'
            element = page.locator(f'xpath={xpath_total_pages}').first
            
            if element.count() > 0 and element.is_visible(timeout=5_000):
                total_pages_text = element.inner_text().strip()
                match = re.search(r'(\d+)', total_pages_text)
                if match:
                    total_pages = int(match.group(1))
                    return total_pages - 1
                    
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Could not determine total pages, using start_page as default: {e}', 
                "warning"
            )
        
        return self.start_page
    
    def _extract_data_referencia(self, page: PlaywrightPage) -> Optional[str]:
        """Extract DATA_REFERENCIA from the date input field.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        
        Returns
        -------
        Optional[str]
            The reference date or None.
        """
        try:
            xpath_data_ref = (
                '//*[@id="__next"]/main/div/div/div/div/div[1]/div[2]/div[3]'
                '/div/div[2]/div/div/div/div/div/input'
            )
            element = page.locator(f'xpath={xpath_data_ref}').first
            
            if element.count() > 0:
                data_ref = element.get_attribute('value')
                return data_ref if data_ref else None
                
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Error extracting DATA_REFERENCIA: {e}', 
                "warning"
            )
        
        return None
    
    def _extract_price_data(
        self, 
        page: PlaywrightPage, 
        item_index: int,
        data_referencia: Optional[str]
    ) -> dict:
        """Extract price data for a single item.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        item_index : int
            The index of the item (1-based).
        data_referencia : Optional[str]
            The reference date from the page.
        
        Returns
        -------
        dict
            Dictionary containing extracted price data.
        """
        data = {"DATA_REFERENCIA": data_referencia}
        
        base_xpath = (
            f'//*[@id="__next"]/main/div/div/div/div/div[3]/article[{item_index}]'
        )
        
        xpath_mapping = {
            'CODIGO_EMISSAO': f'{base_xpath}/div[1]/div[1]/h2/a/span[1]',
            'NOME_DEVEDOR_ESTENDIDO': f'{base_xpath}/div[1]/div[1]/h2/a/span[2]/span[2]',
            'NOME_EMISSOR': f'{base_xpath}/div[2]/div[1]/article[1]/p',
            'SERIE_EMISSAO': f'{base_xpath}/div[2]/div[2]/article[1]/p',
            'REMUNERACAO': f'{base_xpath}/div[2]/div[2]/article[2]/p',
            'DURATION': f'{base_xpath}/div[2]/div[3]/article[1]/p',
            'DEVEDOR': f'{base_xpath}/div[2]/div[1]/article[2]/p',
            'TAXA_COMPRA': f'{base_xpath}/div[2]/div[2]/article[3]/p',
            'TAXA_VENDA': f'{base_xpath}/div[2]/div[2]/article[4]/p',
            'PCT_PU_PAR': f'{base_xpath}/div[2]/div[3]/article[3]/p',
            'PU_PAR': f'{base_xpath}/div[2]/div[3]/article[4]/p',
            'DATA_EMISSAO': f'{base_xpath}/div[2]/div[1]/article[3]/p',
            'DATA_VENCIMENTO': f'{base_xpath}/div[2]/div[1]/article[4]/p',
            'TAXA_INDICATIVA': f'{base_xpath}/div[2]/div[2]/article[5]/p',
            'DESVIO_PADRAO': f'{base_xpath}/div[2]/div[2]/article[6]/p',
            'PCT_REUNE': f'{base_xpath}/div[2]/div[3]/article[5]/p',
            'DATA_REFERENCIA_NTNB': f'{base_xpath}/div[2]/div[3]/article[6]/p',
        }
        
        for field_name, xpath in xpath_mapping.items():
            try:
                element = page.locator(f'xpath={xpath}').first
                if element.count() > 0:
                    text = element.inner_text().strip()
                    data[field_name] = text if text else None
                else:
                    data[field_name] = None
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger, 
                    f'Error extracting {field_name} for item {item_index}: {e}', 
                    "warning"
                )
                data[field_name] = None
                
        tipo_data = self._extract_tipo_price(page, item_index)
        data.update(tipo_data)
        
        pu_indicativo_data = self._extract_pu_indicativo(page, item_index, base_xpath)
        data.update(pu_indicativo_data)
        
        link_data = self._extract_link(page, item_index, base_xpath)
        data.update(link_data)
        
        return data
    
    def _extract_tipo_price(
        self, 
        page: PlaywrightPage, 
        item_index: int
    ) -> dict:
        """Extract IS_CRI_CRA (tipo) from paragraph in price view.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        item_index : int
            The index of the item (1-based for articles).
        
        Returns
        -------
        dict
            Dictionary with IS_CRI_CRA.
        """
        result = {
            "IS_CRI_CRA": None
        }
        
        try:
            xpath_tipo = (
                f'//*[@id="__next"]/main/div/div/div/div/div[3]'
                f'/article[{item_index}]/div[1]/div[1]/p'
            )
            element = page.locator(f'xpath={xpath_tipo}').first
            
            if element.count() > 0 and element.is_visible(timeout=2_000):
                tipo_texto = element.inner_text().strip()
                if tipo_texto:
                    result["IS_CRI_CRA"] = tipo_texto
                    
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Error extracting IS_CRI_CRA for item {item_index}: {e}', 
                "warning"
            )
        
        return result
    
    def _extract_pu_indicativo(
        self, 
        page: PlaywrightPage, 
        item_index: int,
        base_xpath: str
    ) -> dict:
        """Extract PU_INDICATIVO from link text.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        item_index : int
            The item index.
        base_xpath : str
            The base XPath for the item.
        
        Returns
        -------
        dict
            Dictionary with PU_INDICATIVO.
        """
        result = {"PU_INDICATIVO": None}
        
        try:
            xpath = f'{base_xpath}/div[2]/div[3]/article[2]/p/a'
            element = page.locator(f'xpath={xpath}').first
            
            if element.count() > 0:
                pu_text = element.inner_text().strip()
                result["PU_INDICATIVO"] = pu_text if pu_text else None
                    
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Error extracting PU_INDICATIVO for item {item_index}: {e}', 
                "warning"
            )
        
        return result
    
    def _extract_link(
        self, 
        page: PlaywrightPage, 
        item_index: int,
        base_xpath: str
    ) -> dict:
        """Extract LINK_CARACTERISTICAS_INDIVIDUAIS.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        item_index : int
            The item index.
        base_xpath : str
            The base XPath for the item.
        
        Returns
        -------
        dict
            Dictionary with LINK_CARACTERISTICAS_INDIVIDUAIS.
        """
        result = {"LINK_CARACTERISTICAS_INDIVIDUAIS": None}
        
        try:
            xpath = f'{base_xpath}/div[1]/div[1]/h2/a'
            element = page.locator(f'xpath={xpath}').first
            
            if element.count() > 0:
                href = element.get_attribute('href')
                if href:
                    if href.startswith('/'):
                        result["LINK_CARACTERISTICAS_INDIVIDUAIS"] = \
                            urljoin('https://data.anbima.com.br', href)
                    else:
                        result["LINK_CARACTERISTICAS_INDIVIDUAIS"] = href
                        
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Error extracting link for item {item_index}: {e}', 
                "warning"
            )
        
        return result
    
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
        """Transform scraped CRI/CRA prices data into a DataFrame.
        
        Parameters
        ----------
        raw_data : list
            The scraped prices data list.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
        
        df_ = pd.DataFrame(raw_data)
        
        if 'pagina' in df_.columns:
            df_ = df_.drop('pagina', axis=1)
            
        for col_ in ["DATA_REFERENCIA", "DATA_EMISSAO", "DATA_VENCIMENTO", 
                     "DATA_REFERENCIA_NTNB"]:
            if col_ in df_.columns:
                df_[col_] = df_[col_].apply(
                    lambda x: x.replace("-", "01/01/2100") 
                        if x and isinstance(x, str) else "01/01/2100"
                )
        
        return df_


class AnbimaDataCRICRAPricesFile(ABCIngestionOperations):
    """Anbima CRI/CRA prices file download ingestion class."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the Anbima CRI/CRA prices file download ingestion class.
        
        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.
        
        Returns
        -------
        None

        Notes
        -----
        [1] Download URL: https://www.anbima.com.br/pt_br/anbima/TaxasCriCraExport/downloadExterno
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
        self.download_url = (
            "https://www.anbima.com.br/pt_br/anbima/TaxasCriCraExport/downloadExterno"
        )
    
    def run(
        self,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_anbimadata_cri_cra_prices_file"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "br_anbimadata_cri_cra_prices_file"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        raw_data = self.get_response()
        df_ = self.transform_data(raw_data=raw_data)
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.date_ref,
            dict_dtypes={
                "DATA_REFERENCIA": "date",
                "RISCO_CREDITO": str,
                "EMISSOR": str,
                "SERIE": str,
                "EMISSAO": str,
                "CODIGO": str,
                "VENCIMENTO": "date",
                "INDICE_CORRECAO": str,
                "TAXA_COMPRA": str,
                "TAXA_VENDA": str,
                "TAXA_INDICATIVA": str,
                "DESVIO_PADRAO": str,
                "PU": str,
                "PCT_PU_PAR_PCT_VNE": str,
                "DURATION": str,
                "DATA_REFERENCIA_NTNB": "date",
                "PCT_REUNE": str,
            }, 
            str_fmt_dt="DD/MM/YYYY",
            url=self.download_url,
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

    def get_response(self) -> Response:
        """Download CRI/CRA prices file from Anbima.

        Returns
        -------
        Response
            The response object containing the CSV file.
        """
        self.cls_create_log.log_message(
            self.logger, 
            f"🚀 Starting CRI/CRA prices file download from {self.download_url}...", 
            "info"
        )
        
        try:
            import requests
            
            headers = {
                'accept': 'application/json, text/plain, */*',
                'accept-language': 'en-US,en;q=0.9,pt;q=0.8,es;q=0.7',
                'origin': 'https://data.anbima.com.br',
                'referer': 'https://data.anbima.com.br/',
                'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Linux"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-site',
                'user-agent': (
                    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                    '(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
                ),
            }
            
            response = requests.get(self.download_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            self.cls_create_log.log_message(
                self.logger, 
                "✅ File downloaded successfully", 
                "info"
            )
            
            return response
            
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f"❌ Error downloading file: {str(e)}", 
                "error"
            )
            raise
    
    def parse_raw_file(
        self, 
        resp_req: Response
    ) -> StringIO:
        """Parse the raw CSV file content.
        
        Parameters
        ----------
        resp_req : Response
            The response object containing the CSV file.
        
        Returns
        -------
        StringIO
            The parsed CSV content.
        """
        try:
            content = resp_req.text
            
            self.cls_create_log.log_message(
                self.logger, 
                f"📄 Parsing CSV content ({len(content)} characters)...", 
                "info"
            )
            
            return StringIO(content)
            
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f"❌ Error parsing file: {str(e)}", 
                "error"
            )
            raise
    
    def transform_data(
        self, 
        raw_data: Response
    ) -> pd.DataFrame:
        """Transform downloaded CSV data into a DataFrame.
        
        Parameters
        ----------
        raw_data : Response
            The response object containing the CSV file.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        try:
            csv_content = self.parse_raw_file(raw_data)
            
            df_raw = pd.read_csv(
                csv_content,
                sep=';',
                encoding='latin1',
                skiprows=0,
            )
            
            self.cls_create_log.log_message(
                self.logger, 
                f"📊 Raw DataFrame: {len(df_raw)} rows, columns: {list(df_raw.columns)}", 
                "info"
            )
            
            column_mapping = {}
            
            for col in df_raw.columns:
                col_clean = col.strip()
                
                if 'Data de Refer' in col_clean and 'NTN' not in col_clean:
                    column_mapping[col] = 'DATA_REFERENCIA'
                elif 'Risco de Cr' in col_clean:
                    column_mapping[col] = 'RISCO_CREDITO'
                elif 'Emissor' in col_clean:
                    column_mapping[col] = 'EMISSOR'
                elif 'Série' in col_clean or 'Serie' in col_clean:
                    column_mapping[col] = 'SERIE'
                elif 'Emissão' in col_clean or 'Emiss' in col_clean:
                    column_mapping[col] = 'EMISSAO'
                elif 'Código' in col_clean or 'Codigo' in col_clean:
                    column_mapping[col] = 'CODIGO'
                elif 'Vencimento' in col_clean:
                    column_mapping[col] = 'VENCIMENTO'
                elif ('Índice' in col_clean or 'Indice' in col_clean) and 'Correção' in col_clean:
                    column_mapping[col] = 'INDICE_CORRECAO'
                elif 'Taxa Compra' in col_clean:
                    column_mapping[col] = 'TAXA_COMPRA'
                elif 'Taxa Venda' in col_clean:
                    column_mapping[col] = 'TAXA_VENDA'
                elif 'Taxa Indicativa' in col_clean:
                    column_mapping[col] = 'TAXA_INDICATIVA'
                elif 'Desvio' in col_clean and ('Padrão' in col_clean or 'Padrao' in col_clean):
                    column_mapping[col] = 'DESVIO_PADRAO'
                elif col_clean.strip() in ['PU', ' PU', 'PU ']:
                    column_mapping[col] = 'PU'
                elif '% PU Par' in col_clean and '% VNE' in col_clean:
                    column_mapping[col] = 'PCT_PU_PAR_PCT_VNE'
                elif 'Duration' in col_clean:
                    column_mapping[col] = 'DURATION'
                elif 'NTN' in col_clean or ('Refer' in col_clean and 'NTN' in col_clean):
                    column_mapping[col] = 'DATA_REFERENCIA_NTNB'
                elif 'Reune' in col_clean or '% Reune' in col_clean:
                    column_mapping[col] = 'PCT_REUNE'
            
            self.cls_create_log.log_message(
                self.logger, 
                f"📝 Column mapping: {column_mapping}", 
                "info"
            )
            
            df_ = df_raw.rename(columns=column_mapping).copy()
            expected_columns = [
                'DATA_REFERENCIA', 'RISCO_CREDITO', 'EMISSOR', 'SERIE', 
                'EMISSAO', 'CODIGO', 'VENCIMENTO', 'INDICE_CORRECAO',
                'TAXA_COMPRA', 'TAXA_VENDA', 'TAXA_INDICATIVA', 'DESVIO_PADRAO',
                'PU', 'PCT_PU_PAR_PCT_VNE', 'DURATION', 'DATA_REFERENCIA_NTNB', 'PCT_REUNE'
            ]
            missing_columns = [col for col in expected_columns if col not in df_.columns]
            
            if missing_columns:
                self.cls_create_log.log_message(
                    self.logger, 
                    f"⚠️ Missing columns: {missing_columns}", 
                    "warning"
                )
                
                for col in missing_columns:
                    df_[col] = None
                    
            df_ = df_[expected_columns].copy()
            
            percentage_columns = ['TAXA_COMPRA', 'TAXA_VENDA', 'TAXA_INDICATIVA', 
                                 'DESVIO_PADRAO', 'PCT_REUNE']
            
            for col in percentage_columns:
                if col in df_.columns:
                    df_[col] = df_[col].astype(str).str.replace('%', '').str.strip()
                    
            if 'PCT_PU_PAR_PCT_VNE' in df_.columns:
                df_['PCT_PU_PAR_PCT_VNE'] = df_['PCT_PU_PAR_PCT_VNE'].astype(str).str.strip()
                
            date_columns = ['DATA_REFERENCIA', 'VENCIMENTO', 'DATA_REFERENCIA_NTNB']
            for col in date_columns:
                if col in df_.columns:
                    def clean_date(val):
                        if pd.isna(val):
                            return '01/01/2100'
                        str_val = str(val).strip()
                        if str_val in ['', '-', 'nan', 'None']:
                            return '01/01/2100'
                        return str_val
                    
                    df_[col] = df_[col].apply(clean_date)
            
            self.cls_create_log.log_message(
                self.logger, 
                f"✅ Data transformation complete: {len(df_)} records processed", 
                "info"
            )
            
            return df_
            
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f"❌ Error transforming data: {str(e)}", 
                "error"
            )
            raise


class AnbimaDataCRICRAIndividualCharacteristics(ABCIngestionOperations):
    """Anbima CRI/CRA Individual Characteristics ingestion class."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        list_asset_codes: Optional[list[str]] = None,
    ) -> None:
        """Initialize the Anbima CRI/CRA Individual Characteristics ingestion class.
        
        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.
        list_asset_codes : Optional[list[str]], optional
            List of CRI/CRA asset codes to scrape, by default None.
            Examples: ['18L1085826', '19C0000001', 'CRA019000GT']
        
        Returns
        -------
        None
        
        Notes
        -----
        [1] Metadata: https://data.anbima.com.br/certificado-de-recebiveis/{asset_code}/caracteristicas
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
        self.base_url = "https://data.anbima.com.br/certificado-de-recebiveis"
        self.list_asset_codes = list_asset_codes or []
    
    def run(
        self,
        timeout_ms: int = 30_000,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_anbimadata_cri_cra_characteristics"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout_ms : int, optional
            The timeout in milliseconds, by default 30_000
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "br_anbimadata_cri_cra_characteristics"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        raw_data = self.get_response(timeout_ms=timeout_ms)
        df_ = self.transform_data(raw_data=raw_data)
        
        self.cls_create_log.log_message(
            self.logger, 
            f"📊 Initial DataFrame shape - CRI/CRA Characteristics: {df_.shape}", 
            "info"
        )
        
        self.cls_create_log.log_message(
            self.logger, 
            "🔄 Starting standardization of CRI/CRA characteristics dataframe...", 
            "info"
        )
        
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.date_ref,
            dict_dtypes={
                "COD_ATIVO": str,
                "IS_CRI_CRA": str,
                "ISIN": str,
                "DEVEDOR": str,
                "SECURITIZADORA": str,
                "NOME_OPERACAO": str,
                "SERIE_EMISSAO": str,
                "DATA_EMISSAO": "date",
                "DATA_VENCIMENTO": "date",
                "REMUNERACAO": str,
                "TAXA_INDICATIVA": str,
                "PU_INDICATIVO": str,
                "PU_PAR": str,
                "SERIE_ATIVO": str,
                "REMUNERACAO_2": str,
                "DATA_INICIO_RENTABILIDADE": "date",
                "EXPRESSAO_PAPEL": str,
                "VOLUME_SERIE_DATA_EMISSAO": str,
                "QUANTIDADE_SERIE_DATA_EMISSAO": str,
                "VNE": str,
                "VNA": str,
                "PRAZO_EMISSAO": str,
                "PRAZO_REMANESCENTE": str,
                "RESGATE_ANTECIPADO": str,
                "ISIN_2": str,
                "DATA_PROXIMO_EVENTO_AGENDA": "date",
                "CLASSE_ATIVO": str,
                "CONCENTRACAO": str,
                "CATEGORIA": str,
                "SETOR": str,
                "TIPO_LASTRO": str,
                "NUMERO_EMISSAO": str,
                "DEVEDOR_EMISSAO": str,
                "CEDENTE": str,
                "NOME_SECURITIZADORA": str,
                "CNPJ_SECURITIZADORA": str,
                "QUANTIDADE_EMISSAO": str,
                "VOLUME_EMISSAO": str,
                "TIPO_EMISSAO": str,
                "NOME_COORDENADOR_LIDER": str,
                "CNPJ_COORDENADOR_LIDER": str,
                "NOME_COORDENADORES": str,
                "CNPJ_COORDENADORES": str,
                "NOME_AGENTE_FIDUCIARIO": str,
                "CNPJ_AGENTE_FIDUCIARIO": str,
            }, 
            str_fmt_dt="DD/MM/YYYY",
            url=self.base_url,
        )
        
        self.cls_create_log.log_message(
            self.logger, 
            f"✅ CRI/CRA characteristics dataframe standardized - Shape: {df_.shape}, "
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
        """Scrape CRI/CRA characteristics using Playwright.

        Parameters
        ----------
        timeout_ms : int, optional
            The timeout in milliseconds, by default 30_000
        
        Returns
        -------
        list[dict[str, Any]]
            List of dictionaries containing CRI/CRA characteristics data.
        """
        list_characteristics_data: list[dict[str, Any]] = []
        
        if not self.list_asset_codes:
            self.cls_create_log.log_message(
                self.logger, 
                "⚠️ No asset codes provided. Cannot scrape CRI/CRA characteristics.", 
                "warning"
            )
            return list_characteristics_data
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()

            self.cls_create_log.log_message(
                self.logger, 
                f"🚀 Starting CRI/CRA characteristics scraping for {len(self.list_asset_codes)} assets...", 
                "info"
            )
            
            for asset_code in self.list_asset_codes:
                self.cls_create_log.log_message(
                    self.logger, 
                    f"📊 Fetching characteristics for: {asset_code}...", 
                    "info"
                )
                
                try:
                    url = f"{self.base_url}/{asset_code}/caracteristicas"
                    page.goto(url)
                    page.wait_for_timeout(timeout_ms)
                    
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"🔍 Extracting characteristics data for {asset_code}...", 
                        "info"
                    )
                    
                    characteristics_data = self._extract_characteristics_data(page, asset_code)
                    list_characteristics_data.append(characteristics_data)
                    
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"✅ Characteristics data extracted for {asset_code} - "
                        f"{len([v for v in characteristics_data.values() if v is not None])} "
                        "fields populated", 
                        "info"
                    )
                    
                except Exception as e:
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"❌ Error processing {asset_code}: {str(e)}", 
                        "error"
                    )
                
                time.sleep(randint(3, 8))  # noqa S311

            browser.close()

        self.cls_create_log.log_message(
            self.logger, 
            "💾 CRI/CRA characteristics scraping finished. "
            f"Total: {len(list_characteristics_data)} assets processed.", 
            "info"
        )
        
        return list_characteristics_data
    
    def _extract_characteristics_data(
        self, 
        page: PlaywrightPage, 
        asset_code: str
    ) -> dict[str, Any]:
        """Extract CRI/CRA characteristics data using specific XPaths.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        asset_code : str
            The asset code.
        
        Returns
        -------
        dict[str, Any]
            Dictionary containing extracted characteristics data.
        """
        data = {"COD_ATIVO": asset_code}
        
        xpath_mapping = {
            'IS_CRI_CRA': '//*[@id="root"]/main/div[1]/div/div/h1/span/label',
            'ISIN': '//*[@id="root"]/main/div[1]/div/div/div/dl[1]/dd',
            'DEVEDOR': '//*[@id="root"]/main/div[1]/div/div/div/dl[2]/dd',
            'SECURITIZADORA': '//*[@id="root"]/main/div[1]/div/div/div/dl[3]/dd',
            'NOME_OPERACAO': '//*[@id="root"]/main/div[3]/main/div/div[2]/div/article/article/section/div/ul[1]/li[1]/span[2]',
            'SERIE_EMISSAO': '//*[@id="root"]/main/div[3]/main/div/div[2]/div/article/article/section/div/ul[1]/li[2]/span[2]',
            'DATA_EMISSAO': '//*[@id="root"]/main/div[3]/main/div/div[2]/div/article/article/section/div/ul[1]/li[3]/span[2]',
            'DATA_VENCIMENTO': '//*[@id="root"]/main/div[3]/main/div/div[2]/div/article/article/section/div/ul[1]/li[4]/span[2]',
            'REMUNERACAO': '//*[@id="root"]/main/div[3]/main/div/div[2]/div/article/article/section/div/ul[1]/li[5]/span[2]',
            'TAXA_INDICATIVA': '//*[@id="root"]/main/div[3]/main/div/div[2]/div/article/article/section/div/ul[2]/li[1]/p[2]',
            'PU_INDICATIVO': '//*[@id="root"]/main/div[3]/main/div/div[2]/div/article/article/section/div/ul[2]/li[2]/p[2]',
            'PU_PAR': '//*[@id="root"]/main/div[3]/main/div/div[2]/div/article/article/section/div/ul[2]/li[3]/p[2]',
            'SERIE_ATIVO': '//*[@id="root"]/main/div[3]/main/div/div[1]/article[1]/article/section/div/div[1]/h2',
            'REMUNERACAO_2': '//*[@id="output__container--remuneracao"]/div/span',
            'DATA_INICIO_RENTABILIDADE': '//*[@id="output__container--dataInicioRentabilidade"]/div/span',
            'EXPRESSAO_PAPEL': '//*[@id="output__container--expressaoPapel"]/div/span',
            'VOLUME_SERIE_DATA_EMISSAO': '//*[@id="output__container--volumeSerieEmissao"]/div/span',
            'QUANTIDADE_SERIE_DATA_EMISSAO': '//*[@id="output__container--quantidadeSerieEmissao"]/div/span',
            'VNE': '//*[@id="output__container--vne"]/div/span',
            'VNA': '//*[@id="output__container--vna"]/div/span/a',
            'PRAZO_EMISSAO': '//*[@id="output__container--prazoEmissao"]/div/span',
            'PRAZO_REMANESCENTE': '//*[@id="output__container--prazoRemanescente"]/div/span',
            'RESGATE_ANTECIPADO': '//*[@id="output__container--resgateAntecipado"]/div/span',
            'ISIN_2': '//*[@id="output__container--isin"]/div/span',
            'DATA_PROXIMO_EVENTO_AGENDA': '//*[@id="output__container--dataProximoEventoAgenda"]/div/span/a',
            'CLASSE_ATIVO': '//*[@id="output__container--classe"]/div/span',
            'CONCENTRACAO': '//*[@id="output__container--concentracao"]/div/span',
            'CATEGORIA': '//*[@id="output__container--categoria"]/div/span',
            'SETOR': '//*[@id="output__container--setor"]/div/span',
            'TIPO_LASTRO': '//*[@id="output__container--tipoLastro"]/div/span',
            'NUMERO_EMISSAO': '//*[@id="root"]/main/div[3]/main/div/div[1]/article[2]/article/section/div/div[1]/h2',
            'DEVEDOR_EMISSAO': '//*[@id="output__container--devedor"]/div/span',
            'CEDENTE': '//*[@id="output__container--cedente"]/div/span',
            'NOME_SECURITIZADORA': '//*[@id="output__container--securitizadora"]/div/span/div/span[1]',
            'CNPJ_SECURITIZADORA': '//*[@id="output__container--securitizadora"]/div/span/div/span[2]',
            'QUANTIDADE_EMISSAO': '//*[@id="output__container--quantidadeEmissao"]/div/span',
            'VOLUME_EMISSAO': '//*[@id="output__container--volumeEmissao"]/div/span',
            'TIPO_EMISSAO': '//*[@id="output__container--tipo"]/div/span',
            'NOME_COORDENADOR_LIDER': '//*[@id="output__container--coordenadorLider"]/div/span/div/span[1]',
            'CNPJ_COORDENADOR_LIDER': '//*[@id="output__container--coordenadorLider"]/div/span/div/span[2]',
            'NOME_AGENTE_FIDUCIARIO': '//*[@id="output__container--agenteFiduciario"]/div/span/div/span[1]',
            'CNPJ_AGENTE_FIDUCIARIO': '//*[@id="output__container--agenteFiduciario"]/div/span/div/span[2]',
        }
        
        for field_name, xpath in xpath_mapping.items():
            data[field_name] = self._extract_single_field(page, xpath, field_name, asset_code)
            
        coordinators_data = self._extract_coordinators(page, asset_code)
        data['NOME_COORDENADORES'] = coordinators_data['names']
        data['CNPJ_COORDENADORES'] = coordinators_data['cnpjs']
        
        return data
    
    def _extract_single_field(
        self, 
        page: PlaywrightPage, 
        xpath: str,
        field_name: str,
        asset_code: str
    ) -> Optional[str]:
        """Extract a single field using XPath.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        xpath : str
            The XPath selector.
        field_name : str
            The field name for logging.
        asset_code : str
            The asset code for logging.
        
        Returns
        -------
        Optional[str]
            The extracted text or None.
        """
        try:
            element = page.locator(f"xpath={xpath}").first
            if element.count() > 0 and element.is_visible(timeout=5_000):
                text = element.inner_text().strip()
                return text if text else None
            else:
                return None
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'⚠️ Error extracting {field_name} for asset {asset_code}: {e}', 
                "warning"
            )
            return None
    
    def _extract_coordinators(
        self, 
        page: PlaywrightPage, 
        asset_code: str
    ) -> dict[str, Optional[str]]:
        """Extract all coordinators (names and CNPJs).
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        asset_code : str
            The asset code for logging.
        
        Returns
        -------
        dict[str, Optional[str]]
            Dictionary with 'names' and 'cnpjs' keys containing pipe-separated values.
        """
        result = {'names': None, 'cnpjs': None}
        
        try:
            coordinator_containers = page.locator(
                'xpath=//*[@id="output__container--coordenadores"]/div/span/div'
            ).all()
            
            if not coordinator_containers:
                return result
            
            names = []
            cnpjs = []
            
            for idx, container in enumerate(coordinator_containers, start=1):
                try:
                    name_element = container.locator('xpath=./span[1]').first
                    if name_element.count() > 0 and name_element.is_visible(timeout=2_000):
                        name = name_element.inner_text().strip()
                        if name:
                            names.append(name)
                    
                    cnpj_element = container.locator('xpath=./span[2]').first
                    if cnpj_element.count() > 0 and cnpj_element.is_visible(timeout=2_000):
                        cnpj = cnpj_element.inner_text().strip()
                        if cnpj:
                            cnpjs.append(cnpj)
                            
                except Exception as e:
                    self.cls_create_log.log_message(
                        self.logger, 
                        f'⚠️ Error extracting coordinator {idx} for asset {asset_code}: {e}', 
                        "warning"
                    )
            
            if names:
                result['names'] = ' | '.join(names)
            if cnpjs:
                result['cnpjs'] = ' | '.join(cnpjs)
            
            self.cls_create_log.log_message(
                self.logger, 
                f"📋 Found {len(names)} coordinators for asset {asset_code}", 
                "info"
            )
            
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'❌ Error extracting coordinators for asset {asset_code}: {e}', 
                "error"
            )
        
        return result
    
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
        """Transform scraped CRI/CRA characteristics data into a DataFrame.
        
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
        
        date_columns = [
            'DATA_EMISSAO',
            'DATA_VENCIMENTO',
            'DATA_INICIO_RENTABILIDADE',
            'DATA_PROXIMO_EVENTO_AGENDA'
        ]
        
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


class AnbimaDataCRICRADocuments(ABCIngestionOperations):
    """Anbima CRI/CRA documents ingestion class."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        list_asset_codes: Optional[list[str]] = None,
    ) -> None:
        """Initialize the Anbima CRI/CRA documents ingestion class.
        
        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.
        list_asset_codes : Optional[list[str]], optional
            List of CRI/CRA asset codes to scrape documents for, by default None.
        
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
        self.base_url = "https://data.anbima.com.br/certificado-de-recebiveis"
        self.list_asset_codes = list_asset_codes or []
    
    def run(
        self,
        timeout_ms: int = 30_000,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_anbimadata_cri_cra_documents"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout_ms : int, optional
            The timeout in milliseconds, by default 30_000
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "br_anbimadata_cri_cra_documents"

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
                "COD_ATIVO": str,
                "IS_CRI_CRA": str,
                "SECAO": str,
                "SUBSECAO": str,
                "NOME_DOCUMENTO": str,
                "DATA_DIVULGACAO_DOCUMENTO": "date",
                "LINK_DOCUMENTO": str,
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
        """Scrape CRI/CRA documents using Playwright.

        Parameters
        ----------
        timeout_ms : int, optional
            The timeout in milliseconds, by default 30_000
        
        Returns
        -------
        list
            List of scraped CRI/CRA documents data.
        """
        list_documents_data: list[dict[str, Union[str, int, float, date]]] = []
        
        if not self.list_asset_codes:
            self.cls_create_log.log_message(
                self.logger, 
                "⚠️ No asset codes provided. Cannot scrape documents.", 
                "warning"
            )
            return list_documents_data
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()

            self.cls_create_log.log_message(
                self.logger, 
                f"🚀 Starting documents scraping for {len(self.list_asset_codes)} "
                "CRI/CRA assets...", 
                "info"
            )
            
            for asset_code in self.list_asset_codes:
                self.cls_create_log.log_message(
                    self.logger, 
                    f"📊 Fetching documents for: {asset_code}...", 
                    "info"
                )
                
                try:
                    url = f"{self.base_url}/{asset_code}/documentos"
                    page.goto(url)
                    page.wait_for_timeout(timeout_ms)
                    
                    documents_data = self._extract_documents_data(page, asset_code)
                    list_documents_data.extend(documents_data)
                    
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"✅ Successfully extracted {len(documents_data)} documents "
                        f"for {asset_code}", 
                        "info"
                    )
                    
                except Exception as e:
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"❌ Error processing {asset_code}: {str(e)}", 
                        "error"
                    )
                
                time.sleep(randint(2, 8))  # noqa S311
            
            browser.close()
        
        self.cls_create_log.log_message(
            self.logger, 
            f"Documents scraping finished. Total: {len(list_documents_data)} records processed.", 
            "info"
        )
        
        return list_documents_data
    
    def _extract_documents_data(
        self, 
        page: PlaywrightPage,
        asset_code: str
    ) -> list[dict]:
        """Extract CRI/CRA documents data using specific XPaths.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        asset_code : str
            The asset code.
        
        Returns
        -------
        list[dict]
            List of dictionaries containing extracted document data.
        """
        cod_ativo = self._extract_cod_ativo(page, asset_code)
        is_cri_cra = self._extract_is_cri_cra(page)
        
        # Verificar se há documentos na página
        check_no_docs = self._check_no_documents(page)
        if check_no_docs:
            self.cls_create_log.log_message(
                self.logger, 
                f'No documents found for {asset_code} (page shows no documents message)', 
                "warning"
            )
            return []
        
        # Encontrar todas as seções de documentos (articles)
        document_sections = self._find_document_sections(page, cod_ativo)
        
        if not document_sections:
            return []
        
        list_documents: list[dict[str, Union[str, int, float, date]]] = []
        for idx, section in enumerate(document_sections, start=1):
            documents = self._process_document_section(
                section, idx, cod_ativo, is_cri_cra, page
            )
            list_documents.extend(documents)
        
        return list_documents

    def _extract_cod_ativo(
        self, 
        page: PlaywrightPage, 
        fallback_code: str
    ) -> str:
        """Extract COD_ATIVO from page header.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        fallback_code : str
            Fallback value if extraction fails.
        
        Returns
        -------
        str
            The extracted asset code.
        """
        with suppress(Exception):
            # Tenta extrair apenas o texto do código (sem o label CRI/CRA)
            codigo_element = page.locator(
                'xpath=//*[@id="root"]/main/div[1]/div/div/h1'
            ).first
            if codigo_element.is_visible(timeout=5000):
                full_text = codigo_element.inner_text().strip()
                # Remove o label CRI/CRA se presente
                codigo_text = full_text.split('\n')[0].strip() if '\n' in full_text else full_text
                return codigo_text
        return fallback_code

    def _extract_is_cri_cra(self, page: PlaywrightPage) -> Optional[str]:
        """Extract IS_CRI_CRA label from page.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        
        Returns
        -------
        Optional[str]
            The extracted CRI/CRA type or None.
        """
        with suppress(Exception):
            is_cri_cra_element = page.locator(
                'xpath=//*[@id="root"]/main/div[1]/div/div/h1/span/label'
            ).first
            if is_cri_cra_element.is_visible(timeout=5000):
                return is_cri_cra_element.inner_text().strip()
        return None

    def _check_no_documents(self, page: PlaywrightPage) -> bool:
        """Check if page shows 'no documents' message.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        
        Returns
        -------
        bool
            True if no documents message is found, False otherwise.
        """
        with suppress(Exception):
            no_docs_text = page.locator('text="Não existem documentos"').first
            if no_docs_text.count() > 0 and no_docs_text.is_visible(timeout=2000):
                return True
        return False

    def _find_document_sections(
        self, 
        page: PlaywrightPage, 
        cod_ativo: str,
    ) -> list:
        """Find all document sections on the page.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        cod_ativo : str
            The asset code for logging purposes.
        
        Returns
        -------
        list
            List of document section elements.
        """
        xpath_base = '//div[@id="root"]/main/div[3]/div/div[1]/article'
        document_sections = page.locator(f'xpath={xpath_base}').all()
        
        if len(document_sections) == 0:
            self.cls_create_log.log_message(
                self.logger, 
                f'No document sections found for {cod_ativo}', 
                "warning"
            )
        else:
            self.cls_create_log.log_message(
                self.logger, 
                f'Found {len(document_sections)} document sections for {cod_ativo}', 
                "info"
            )
        
        return document_sections

    def _process_document_section(
        self,
        section: Locator,
        section_idx: int,
        cod_ativo: str,
        is_cri_cra: Optional[str],
        page: PlaywrightPage
    ) -> list[dict]:
        """Process a single document section and extract all documents.
        
        Parameters
        ----------
        section : Locator
            The document section element.
        section_idx : int
            Section index for XPath construction.
        cod_ativo : str
            The asset code.
        is_cri_cra : Optional[str]
            The CRI/CRA type.
        page : PlaywrightPage
            The Playwright page object.
        
        Returns
        -------
        list[dict]
            List of document records extracted from this section.
        """
        try:
            # Extrair nome da seção
            secao = self._extract_section_name(page, section_idx)
            
            # Extrair subseção (se existir)
            subsecao = self._extract_subsection_name(page, section_idx)
            
            # Encontrar todos os itens de documentos (li elements)
            document_items = section.locator(
                'xpath=.//article/section/div[2]/ul/li'
            ).all()
            
            if len(document_items) == 0:
                self.cls_create_log.log_message(
                    self.logger, 
                    f'Section {section_idx} ({secao}): No document items found', 
                    "warning"
                )
                return []
            
            self.cls_create_log.log_message(
                self.logger, 
                f'Section {section_idx} ({secao}): Found {len(document_items)} document items', 
                "info"
            )
            
            list_documents: list[dict] = []
            for item_idx, item in enumerate(document_items, start=1):
                docs = self._process_document_item(
                    item, page, section_idx, item_idx,
                    cod_ativo, is_cri_cra, secao, subsecao
                )
                list_documents.extend(docs)
            
            return list_documents
                
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Error extracting section {section_idx} for {cod_ativo}: {e}', 
                "warning"
            )
            return []

    def _extract_section_name(
        self, 
        page: PlaywrightPage, 
        section_idx: int
    ) -> Optional[str]:
        """Extract section name.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        section_idx : int
            Section index for XPath.
        
        Returns
        -------
        Optional[str]
            The section name or None.
        """
        xpath_secao = (
            f'//*[@id="root"]/main/div[3]/div/div[1]/article[{section_idx}]'
            f'/article/section/div[1]/div/p[1]'
        )
        return self._extract_text_by_xpath(page, xpath_secao)

    def _extract_subsection_name(
        self, 
        page: PlaywrightPage, 
        section_idx: int
    ) -> Optional[str]:
        """Extract subsection name if exists.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        section_idx : int
            Section index for XPath.
        
        Returns
        -------
        Optional[str]
            The subsection name or None.
        """
        xpath_subsecao = (
            f'//*[@id="root"]/main/div[3]/div/div[1]/article[{section_idx}]'
            f'/article/section/div[1]/div/p[2]'
        )
        return self._extract_text_by_xpath(page, xpath_subsecao)

    def _process_document_item(
        self,
        item: Locator,
        page: PlaywrightPage,
        section_idx: int,
        item_idx: int,
        cod_ativo: str,
        is_cri_cra: Optional[str],
        secao: Optional[str],
        subsecao: Optional[str]
    ) -> list[dict]:
        """Process a single document item (li element) which may contain multiple links.
        
        Parameters
        ----------
        item : Locator
            The document item element.
        page : PlaywrightPage
            The Playwright page object.
        section_idx : int
            Section index for XPath.
        item_idx : int
            Item index within the section.
        cod_ativo : str
            The asset code.
        is_cri_cra : Optional[str]
            The CRI/CRA type.
        secao : Optional[str]
            The section name.
        subsecao : Optional[str]
            The subsection name.
        
        Returns
        -------
        list[dict]
            List of document records (one per link).
        """
        try:
            # Extrair nome do documento
            nome_documento = self._extract_document_name(page, section_idx, item_idx)
            
            # Encontrar todos os links dentro deste item
            link_elements = item.locator('xpath=.//ul/li/a').all()
            
            if len(link_elements) == 0:
                self.cls_create_log.log_message(
                    self.logger, 
                    f'Section {section_idx}, Item {item_idx} ({nome_documento}): No links found', 
                    "warning"
                )
                return [self._create_document_record(
                    cod_ativo, is_cri_cra, secao, subsecao,
                    nome_documento, None, None
                )]
            
            self.cls_create_log.log_message(
                self.logger, 
                f'Section {section_idx}, Item {item_idx} ({nome_documento}): '
                f'Found {len(link_elements)} links', 
                "info"
            )
            
            list_documents: list[dict] = []
            for link_idx, link_element in enumerate(link_elements, start=1):
                # Extrair data de divulgação
                data_divulgacao = self._extract_document_date(
                    page, section_idx, item_idx, link_idx
                )
                
                # Extrair link
                link_documento = self._extract_link_url(
                    link_element, page, section_idx, item_idx, link_idx
                )
                
                if link_documento:
                    list_documents.append(self._create_document_record(
                        cod_ativo, is_cri_cra, secao, subsecao,
                        nome_documento, data_divulgacao, link_documento
                    ))
                    time.sleep(1)
            
            return list_documents
            
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Error processing item {item_idx} in section {section_idx}: {e}', 
                "warning"
            )
            return []

    def _extract_document_name(
        self, 
        page: PlaywrightPage, 
        section_idx: int,
        item_idx: int
    ) -> Optional[str]:
        """Extract document name.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        section_idx : int
            Section index for XPath.
        item_idx : int
            Item index for XPath.
        
        Returns
        -------
        Optional[str]
            The document name or None.
        """
        xpath_nome = (
            f'//*[@id="root"]/main/div[3]/div/div[1]/article[{section_idx}]'
            f'/article/section/div[2]/ul/li[{item_idx}]/div/p'
        )
        return self._extract_text_by_xpath(page, xpath_nome)

    def _extract_document_date(
        self, 
        page: PlaywrightPage, 
        section_idx: int,
        item_idx: int,
        link_idx: int
    ) -> Optional[str]:
        """Extract document release date.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        section_idx : int
            Section index for XPath.
        item_idx : int
            Item index for XPath.
        link_idx : int
            Link index for XPath.
        
        Returns
        -------
        Optional[str]
            The document date or None.
        """
        xpath_data = (
            f'//*[@id="root"]/main/div[3]/div/div[1]/article[{section_idx}]'
            f'/article/section/div[2]/ul/li[{item_idx}]/ul/li[{link_idx}]/span'
        )
        return self._extract_text_by_xpath(page, xpath_data)

    def _extract_link_url(
        self,
        link_element: Locator,
        page: PlaywrightPage,
        section_idx: int,
        item_idx: int,
        link_idx: int
    ) -> Optional[str]:
        """Extract URL by clicking a link and capturing the new page URL.
        
        Parameters
        ----------
        link_element : Locator
            The link element to click.
        page : PlaywrightPage
            The Playwright page object.
        section_idx : int
            Section index for logging.
        item_idx : int
            Item index for logging.
        link_idx : int
            Link index for logging.
        
        Returns
        -------
        Optional[str]
            The extracted URL or None.
        """
        try:
            with page.context.expect_page(timeout=15000) as new_page_info:
                link_element.click(timeout=10000)
            
            new_page = new_page_info.value
            new_page.wait_for_load_state('domcontentloaded', timeout=10000)
            link_document = new_page.url
            new_page.close()
            
            self.cls_create_log.log_message(
                self.logger, 
                f'  Section {section_idx}, Item {item_idx}, Link {link_idx}: {link_document}', 
                "info"
            )
            
            return link_document
            
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'  Error extracting link in section {section_idx}, '
                f'item {item_idx}, link {link_idx}: {e}', 
                "warning"
            )
            return None

    def _extract_text_by_xpath(
        self, 
        page: PlaywrightPage, 
        xpath: str
    ) -> Optional[str]:
        """Extract text from an element using XPath.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        xpath : str
            The XPath expression.
        
        Returns
        -------
        Optional[str]
            The extracted text or None.
        """
        with suppress(Exception):
            element = page.locator(f'xpath={xpath}').first
            if element.count() > 0 and element.is_visible(timeout=2000):
                text = element.inner_text().strip()
                return text if text else None
        return None

    def _create_document_record(
        self,
        cod_ativo: str,
        is_cri_cra: Optional[str],
        secao: Optional[str],
        subsecao: Optional[str],
        nome_documento: Optional[str],
        data_divulgacao: Optional[str],
        link_documento: Optional[str]
    ) -> ResultDocumentRecord:
        """Create a document record dictionary.
        
        Parameters
        ----------
        cod_ativo : str
            The asset code.
        is_cri_cra : Optional[str]
            The CRI/CRA type.
        secao : Optional[str]
            The section name.
        subsecao : Optional[str]
            The subsection name.
        nome_documento : Optional[str]
            The document name.
        data_divulgacao : Optional[str]
            The document date.
        link_documento : Optional[str]
            The document link.
        
        Returns
        -------
        ResultDocumentRecord
            The document record.
        """
        return {
            "COD_ATIVO": cod_ativo,
            "IS_CRI_CRA": is_cri_cra,
            "SECAO": secao,
            "SUBSECAO": subsecao,
            "NOME_DOCUMENTO": nome_documento,
            "DATA_DIVULGACAO_DOCUMENTO": data_divulgacao,
            "LINK_DOCUMENTO": link_documento,
        }
    
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
        """Transform scraped documents data into a DataFrame.
        
        Parameters
        ----------
        raw_data : list
            The scraped documents data list.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
        
        df_ = pd.DataFrame(raw_data)
        
        if "DATA_DIVULGACAO_DOCUMENTO" in df_.columns:
            df_["DATA_DIVULGACAO_DOCUMENTO"] = df_["DATA_DIVULGACAO_DOCUMENTO"].apply(
                lambda x: x.replace("-", "01/01/2100") 
                    if x and isinstance(x, str) else "01/01/2100"
            )
        
        return df_


class AnbimaDataCRICRAPUIndicativo(ABCIngestionOperations):
    """Anbima CRI/CRA PU Indicativo prices ingestion class."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        list_asset_codes: Optional[list[str]] = None,
    ) -> None:
        """Initialize the Anbima CRI/CRA PU Indicativo ingestion class.
        
        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.
        list_asset_codes : Optional[list[str]], optional
            List of CRI/CRA asset codes to scrape prices for, by default None.
        
        Returns
        -------
        None
        
        Notes
        -----
        [1] Metadata: https://data.anbima.com.br/certificado-de-recebiveis/{asset_code}/precos
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
        self.base_url = "https://data.anbima.com.br/certificado-de-recebiveis"
        self.list_asset_codes = list_asset_codes or []
    
    def run(
        self,
        timeout_ms: int = 30_000,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_anbimadata_cri_cra_pu_indicativo"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout_ms : int, optional
            The timeout in milliseconds, by default 30_000
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "br_anbimadata_cri_cra_pu_indicativo"

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
                "COD_ATIVO": str,
                "IS_CRI_CRA": str,
                "DATA_REFERENCIA": "date",
                "TAXA_COMPRA": str,
                "TAXA_VENDA": str,
                "TAXA_INDICATIVA": str,
                "DESVIO_PADRAO": str,
                "DURATION_DC": str,
                "PCT_PU_PAR": str,
                "PU_INDICATIVO": str,
                "PCT_REUNE": str,
                "DATA_REFERENCIA_NTNB": "date",
                "URL": str,
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
        """Scrape CRI/CRA PU Indicativo prices using Playwright (single page only).

        Parameters
        ----------
        timeout_ms : int, optional
            The timeout in milliseconds, by default 30_000
        
        Returns
        -------
        list
            List of scraped PU Indicativo prices data.
        """
        list_prices_data: list[dict[str, Union[str, int, float, date]]] = []
        
        if not self.list_asset_codes:
            self.cls_create_log.log_message(
                self.logger, 
                "⚠️ No asset codes provided. Cannot scrape PU Indicativo prices.", 
                "warning"
            )
            return list_prices_data
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()

            self.cls_create_log.log_message(
                self.logger, 
                f"🚀 Starting PU Indicativo prices scraping for {len(self.list_asset_codes)} "
                "CRI/CRA assets...", 
                "info"
            )
            
            for asset_code in self.list_asset_codes:
                self.cls_create_log.log_message(
                    self.logger, 
                    f"📊 Fetching PU Indicativo prices for: {asset_code}...", 
                    "info"
                )
                
                try:
                    url = f"{self.base_url}/{asset_code}/precos"
                    page.goto(url)
                    page.wait_for_timeout(timeout_ms)
                    
                    prices_data = self._extract_pu_indicativo_prices(
                        page, asset_code, url
                    )
                    list_prices_data.extend(prices_data)
                    
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"✅ Extracted {len(prices_data)} PU Indicativo price records "
                        f"for {asset_code}", 
                        "info"
                    )
                    
                except Exception as e:
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"❌ Error processing {asset_code}: {str(e)}", 
                        "error"
                    )
                
                time.sleep(randint(2, 8))  # noqa S311
            
            browser.close()
        
        self.cls_create_log.log_message(
            self.logger, 
            f"💾 PU Indicativo prices scraping finished. "
            f"Total: {len(list_prices_data)} records processed.", 
            "info"
        )
        
        return list_prices_data
    
    def _extract_pu_indicativo_prices(
        self, 
        page: PlaywrightPage, 
        asset_code: str,
        url: str
    ) -> list[dict]:
        """Extract PU Indicativo prices data from the page.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        asset_code : str
            The asset code.
        url : str
            The current page URL.
        
        Returns
        -------
        list[dict]
            List of dictionaries containing extracted price data.
        """
        cod_ativo = self._extract_cod_ativo(page, asset_code)
        is_cri_cra = self._extract_is_cri_cra(page)
        
        price_records = self._extract_price_rows(
            page, cod_ativo, is_cri_cra, url
        )
        
        return price_records
    
    def _extract_cod_ativo(
        self, 
        page: PlaywrightPage, 
        fallback_code: str
    ) -> str:
        """Extract COD_ATIVO from page header.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        fallback_code : str
            Fallback value if extraction fails.
        
        Returns
        -------
        str
            The extracted asset code.
        """
        with suppress(Exception):
            codigo_element = page.locator(
                'xpath=//*[@id="root"]/main/div[1]/div/div/h1'
            ).first
            if codigo_element.is_visible(timeout=5000):
                full_text = codigo_element.inner_text().strip()
                codigo_text = full_text.split('\n')[0].strip() if '\n' in full_text else full_text
                return codigo_text
        return fallback_code
    
    def _extract_is_cri_cra(self, page: PlaywrightPage) -> Optional[str]:
        """Extract IS_CRI_CRA label from page.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        
        Returns
        -------
        Optional[str]
            The extracted CRI/CRA type or None.
        """
        with suppress(Exception):
            is_cri_cra_element = page.locator(
                'xpath=//*[@id="root"]/main/div[1]/div/div/h1/span/label'
            ).first
            if is_cri_cra_element.is_visible(timeout=5000):
                return is_cri_cra_element.inner_text().strip()
        return None
    
    def _extract_price_rows(
        self,
        page: PlaywrightPage,
        cod_ativo: str,
        is_cri_cra: Optional[str],
        url: str
    ) -> list[dict]:
        """Extract all PU Indicativo price rows from the table.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        cod_ativo : str
            The asset code.
        is_cri_cra : Optional[str]
            The CRI/CRA type.
        url : str
            The current page URL.
        
        Returns
        -------
        list[dict]
            List of price records.
        """
        price_records = []
        
        data_ref_elements = page.locator(
            'xpath=//*[contains(@id, "pu-indicativo-tabela-dataReferencia-")]'
        ).all()
        
        if not data_ref_elements:
            self.cls_create_log.log_message(
                self.logger, 
                f'No PU Indicativo price data found for {cod_ativo}', 
                "warning"
            )
            return price_records
        
        self.cls_create_log.log_message(
            self.logger, 
            f'Found {len(data_ref_elements)} PU Indicativo price records for {cod_ativo}', 
            "info"
        )
        
        for idx in range(len(data_ref_elements)):
            try:
                price_record = self._extract_single_price_row(
                    page, idx, cod_ativo, is_cri_cra, url
                )
                if price_record:
                    price_records.append(price_record)
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger, 
                    f'Error extracting price row {idx} for {cod_ativo}: {e}', 
                    "warning"
                )
        
        return price_records
    
    def _extract_single_price_row(
        self,
        page: PlaywrightPage,
        row_idx: int,
        cod_ativo: str,
        is_cri_cra: Optional[str],
        url: str
    ) -> Optional[dict]:
        """Extract a single PU Indicativo price row.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        row_idx : int
            The row index.
        cod_ativo : str
            The asset code.
        is_cri_cra : Optional[str]
            The CRI/CRA type.
        url : str
            The current page URL.
        
        Returns
        -------
        Optional[dict]
            Price record or None if extraction fails.
        """
        try:
            data_ref_xpath = f'//*[@id="pu-indicativo-tabela-dataReferencia-{row_idx}"]'
            data_ref = self._extract_text_by_xpath(page, data_ref_xpath)
            
            taxa_compra_xpath = f'//*[@id="pu-indicativo-tabela-taxaCompra-{row_idx}"]'
            taxa_compra = self._extract_text_by_xpath(page, taxa_compra_xpath)
            
            taxa_venda_xpath = f'//*[@id="pu-indicativo-tabela-taxaVenda-{row_idx}"]'
            taxa_venda = self._extract_text_by_xpath(page, taxa_venda_xpath)
            
            taxa_indicativa_xpath = f'//*[@id="pu-indicativo-tabela-taxaIndicativa-{row_idx}"]'
            taxa_indicativa = self._extract_text_by_xpath(page, taxa_indicativa_xpath)
            
            desvio_padrao_xpath = f'//*[@id="pu-indicativo-tabela-desvioPadrao-{row_idx}"]'
            desvio_padrao = self._extract_text_by_xpath(page, desvio_padrao_xpath)
            
            duration_xpath = f'//*[@id="pu-indicativo-tabela-duration-{row_idx}"]'
            duration_dc = self._extract_text_by_xpath(page, duration_xpath)
            
            pct_pu_par_xpath = f'//*[@id="pu-indicativo-tabela-puPar-{row_idx}"]'
            pct_pu_par = self._extract_text_by_xpath(page, pct_pu_par_xpath)
            
            pu_indicativo_xpath = f'//*[@id="pu-indicativo-tabela-puIndicativo-{row_idx}"]'
            pu_indicativo = self._extract_text_by_xpath(page, pu_indicativo_xpath)
            
            pct_reune_xpath = f'//*[@id="pu-indicativo-tabela-reune-{row_idx}"]'
            pct_reune = self._extract_text_by_xpath(page, pct_reune_xpath)
            
            data_ref_ntnb_xpath = f'//*[@id="pu-indicativo-tabela-referenciaNTNB-{row_idx}"]'
            data_ref_ntnb = self._extract_text_by_xpath(page, data_ref_ntnb_xpath)
            
            return {
                "COD_ATIVO": cod_ativo,
                "IS_CRI_CRA": is_cri_cra,
                "DATA_REFERENCIA": data_ref,
                "TAXA_COMPRA": taxa_compra,
                "TAXA_VENDA": taxa_venda,
                "TAXA_INDICATIVA": taxa_indicativa,
                "DESVIO_PADRAO": desvio_padrao,
                "DURATION_DC": duration_dc,
                "PCT_PU_PAR": pct_pu_par,
                "PU_INDICATIVO": pu_indicativo,
                "PCT_REUNE": pct_reune,
                "DATA_REFERENCIA_NTNB": data_ref_ntnb,
                "URL": url,
            }
            
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Error in _extract_single_price_row for row {row_idx}: {e}', 
                "warning"
            )
            return None
    
    def _extract_text_by_xpath(
        self, page: PlaywrightPage, xpath: str
    ) -> Optional[str]:
        """Extract text from an element using XPath.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        xpath : str
            The XPath expression.
        
        Returns
        -------
        Optional[str]
            The extracted text or None.
        """
        with suppress(Exception):
            element = page.locator(f'xpath={xpath}').first
            if element.is_visible(timeout=5000):
                text = element.inner_text().strip()
                return text if text else None
        return None
    
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
        """Transform scraped PU Indicativo prices data into a DataFrame.
        
        Parameters
        ----------
        raw_data : list
            The scraped prices data list.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
        
        df_ = pd.DataFrame(raw_data)
        
        for col_ in ["DATA_REFERENCIA", "DATA_REFERENCIA_NTNB"]:
            if col_ in df_.columns:
                df_[col_] = df_[col_].apply(
                    lambda x: x.replace("-", "01/01/2100") 
                        if x and isinstance(x, str) else "01/01/2100"
                )
        
        return df_


class AnbimaDataCRICRAPUHistorico(ABCIngestionOperations):
    """Anbima CRI/CRA PU Histórico prices ingestion class."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        list_asset_codes: Optional[list[str]] = None,
    ) -> None:
        """Initialize the Anbima CRI/CRA PU Histórico ingestion class.
        
        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.
        list_asset_codes : Optional[list[str]], optional
            List of CRI/CRA asset codes to scrape prices for, by default None.
        
        Returns
        -------
        None
        
        Notes
        -----
        [1] Metadata: https://data.anbima.com.br/certificado-de-recebiveis/{asset_code}/precos
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
        self.base_url = "https://data.anbima.com.br/certificado-de-recebiveis"
        self.list_asset_codes = list_asset_codes or []
    
    def run(
        self,
        timeout_ms: int = 30_000,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_anbimadata_cri_cra_pu_historico"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout_ms : int, optional
            The timeout in milliseconds, by default 30_000
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "br_anbimadata_cri_cra_pu_historico"

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
                "COD_ATIVO": str,
                "IS_CRI_CRA": str,
                "DATA_REFERENCIA": "date",
                "VNA": str,
                "PU_PAR": str,
                "PU_EVENTO": str,
                "URL": str,
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
        """Scrape CRI/CRA PU Histórico prices using Playwright with pagination.

        Parameters
        ----------
        timeout_ms : int, optional
            The timeout in milliseconds, by default 30_000
        
        Returns
        -------
        list
            List of scraped PU Histórico prices data.
        """
        list_prices_data: list[dict[str, Union[str, int, float, date]]] = []
        
        if not self.list_asset_codes:
            self.cls_create_log.log_message(
                self.logger, 
                "⚠️ No asset codes provided. Cannot scrape PU Histórico prices.", 
                "warning"
            )
            return list_prices_data
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()

            self.cls_create_log.log_message(
                self.logger, 
                f"🚀 Starting PU Histórico prices scraping for {len(self.list_asset_codes)} "
                "CRI/CRA assets...", 
                "info"
            )
            
            for asset_code in self.list_asset_codes:
                self.cls_create_log.log_message(
                    self.logger, 
                    f"📊 Fetching PU Histórico prices for: {asset_code}...", 
                    "info"
                )
                
                try:
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"Fetching historic PV for {asset_code}...", 
                        "info"
                    )
                    
                    current_url = \
                        f"{self.base_url}/{asset_code}/precos?page=1&size=100"
                    
                    prices_data = self._extract_pu_historico_prices(
                        page, asset_code, current_url
                    )
                    list_prices_data.extend(prices_data)
                    
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"✅ Extracted {len(prices_data)} price records from page", 
                        "info"
                    )
                    
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"✅ Successfully extracted all PU Histórico prices for {asset_code}", 
                        "info"
                    )
                    
                except Exception as e:
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"❌ Error processing {asset_code}: {str(e)}", 
                        "error"
                    )
                
                time.sleep(randint(2, 8))  # noqa S311
            
            browser.close()
        
        self.cls_create_log.log_message(
            self.logger, 
            f"💾 PU Histórico prices scraping finished. "
            f"Total: {len(list_prices_data)} records processed.", 
            "info"
        )
        
        return list_prices_data
    
    def _extract_pu_historico_prices(
        self, 
        page: PlaywrightPage, 
        asset_code: str,
        url: str
    ) -> list[dict]:
        """Extract PU Histórico prices data from the page.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        asset_code : str
            The asset code.
        url : str
            The current page URL.
        
        Returns
        -------
        list[dict]
            List of dictionaries containing extracted price data.
        """
        cod_ativo = self._extract_cod_ativo(page, asset_code)
        is_cri_cra = self._extract_is_cri_cra(page)
        
        price_records = self._extract_price_rows(
            page, cod_ativo, is_cri_cra, url
        )
        
        return price_records
    
    def _extract_cod_ativo(
        self, 
        page: PlaywrightPage, 
        fallback_code: str
    ) -> str:
        """Extract COD_ATIVO from page header.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        fallback_code : str
            Fallback value if extraction fails.
        
        Returns
        -------
        str
            The extracted asset code.
        """
        with suppress(Exception):
            codigo_element = page.locator(
                'xpath=//*[@id="root"]/main/div[1]/div/div/h1'
            ).first
            if codigo_element.is_visible(timeout=5000):
                full_text = codigo_element.inner_text().strip()
                codigo_text = full_text.split('\n')[0].strip() if '\n' in full_text else full_text
                return codigo_text
        return fallback_code
    
    def _extract_is_cri_cra(self, page: PlaywrightPage) -> Optional[str]:
        """Extract IS_CRI_CRA label from page.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        
        Returns
        -------
        Optional[str]
            The extracted CRI/CRA type or None.
        """
        with suppress(Exception):
            is_cri_cra_element = page.locator(
                'xpath=//*[@id="root"]/main/div[1]/div/div/h1/span/label'
            ).first
            if is_cri_cra_element.is_visible(timeout=5000):
                return is_cri_cra_element.inner_text().strip()
        return None
    
    def _extract_price_rows(
        self,
        page: PlaywrightPage,
        cod_ativo: str,
        is_cri_cra: Optional[str],
        url: str
    ) -> list[dict]:
        """Extract all PU Histórico price rows from the table.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        cod_ativo : str
            The asset code.
        is_cri_cra : Optional[str]
            The CRI/CRA type.
        url : str
            The current page URL.
        
        Returns
        -------
        list[dict]
            List of price records.
        """
        price_records = []
        
        data_ref_elements = page.locator(
            'xpath=//*[contains(@id, "pu-historico-tabela-dataReferencia-")]'
        ).all()
        
        if not data_ref_elements:
            self.cls_create_log.log_message(
                self.logger, 
                f'No PU Histórico price data found for {cod_ativo}', 
                "warning"
            )
            return price_records
        
        self.cls_create_log.log_message(
            self.logger, 
            f'Found {len(data_ref_elements)} PU Histórico price records for {cod_ativo}', 
            "info"
        )
        
        for idx in range(len(data_ref_elements)):
            try:
                price_record = self._extract_single_price_row(
                    page, idx, cod_ativo, is_cri_cra, url
                )
                if price_record:
                    price_records.append(price_record)
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger, 
                    f'Error extracting price row {idx} for {cod_ativo}: {e}', 
                    "warning"
                )
        
        return price_records
    
    def _extract_single_price_row(
        self,
        page: PlaywrightPage,
        row_idx: int,
        cod_ativo: str,
        is_cri_cra: Optional[str],
        url: str
    ) -> Optional[dict]:
        """Extract a single PU Histórico price row.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        row_idx : int
            The row index.
        cod_ativo : str
            The asset code.
        is_cri_cra : Optional[str]
            The CRI/CRA type.
        url : str
            The current page URL.
        
        Returns
        -------
        Optional[dict]
            Price record or None if extraction fails.
        """
        try:
            data_ref_xpath = f'//*[@id="pu-historico-tabela-dataReferencia-{row_idx}"]'
            data_ref = self._extract_text_by_xpath(page, data_ref_xpath)
            
            vna_xpath = f'//*[@id="pu-historico-tabela-vna-{row_idx}"]'
            vna = self._extract_text_by_xpath(page, vna_xpath)
            
            pu_par_xpath = f'//*[@id="pu-historico-tabela-puPar-{row_idx}"]'
            pu_par = self._extract_text_by_xpath(page, pu_par_xpath)
            
            pu_evento_xpath = f'//*[@id="pu-historico-tabela-puEvento-{row_idx}"]'
            pu_evento = self._extract_text_by_xpath(page, pu_evento_xpath)
            
            return {
                "COD_ATIVO": cod_ativo,
                "IS_CRI_CRA": is_cri_cra,
                "DATA_REFERENCIA": data_ref,
                "VNA": vna,
                "PU_PAR": pu_par,
                "PU_EVENTO": pu_evento,
                "URL": url,
            }
            
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Error in _extract_single_price_row for row {row_idx}: {e}', 
                "warning"
            )
            return None
    
    def _extract_text_by_xpath(
        self, page: PlaywrightPage, xpath: str
    ) -> Optional[str]:
        """Extract text from an element using XPath.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        xpath : str
            The XPath expression.
        
        Returns
        -------
        Optional[str]
            The extracted text or None.
        """
        with suppress(Exception):
            element = page.locator(f'xpath={xpath}').first
            if element.is_visible(timeout=5000):
                text = element.inner_text().strip()
                return text if text else None
        return None
    
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
        """Transform scraped PU Histórico prices data into a DataFrame.
        
        Parameters
        ----------
        raw_data : list
            The scraped prices data list.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
        
        df_ = pd.DataFrame(raw_data)
        
        if "DATA_REFERENCIA" in df_.columns:
            df_["DATA_REFERENCIA"] = df_["DATA_REFERENCIA"].apply(
                lambda x: x.replace("-", "01/01/2100") 
                    if x and isinstance(x, str) else "01/01/2100"
            )
        
        return df_