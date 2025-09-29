"""Implementation of Anbima Debentures ingestion instance."""

from datetime import date
from io import StringIO
from logging import Logger
from random import randint
import time
from typing import Optional, Union

import pandas as pd
from playwright.sync_api import Page as PlaywrightPage, sync_playwright
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


class AnbimaDataDebenturesAvailable(ABCIngestionOperations):
    """Anbima Debentures ingestion class."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        start_page: int = 0,
        end_page: int = 20,
    ) -> None:
        """Initialize the Anbima Debentures ingestion class.
        
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
        end_page : int, optional
            Ending page number (inclusive), by default 20.
        
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
        self.base_url = "https://data.anbima.com.br/busca/debentures"
        
        if start_page < 0:
            raise ValueError("start_page deve ser maior ou igual a 0")
        if end_page < start_page:
            raise ValueError("end_page deve ser maior ou igual a start_page")
        
        self.start_page = start_page
        self.end_page = end_page
    
    def run(
        self,
        timeout_ms: int = 30_000,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_anbimadata_debentures"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "br_anbimadata_debentures"

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
                "NOME": str,
                "EMISSOR": str,
                "REMUNERACAO": str,
                "DATA_VENCIMENTO": "date",
                "DURATION": str,
                "SETOR": str,
                "DATA_EMISSAO": "date",
                "PU_PAR": str,
                "PU_INDICATIVO": str
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
        """Scrape debentures data using Playwright.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Verify the SSL certificate, by default True
        
        Returns
        -------
        list
            List of scraped debentures data.
        """
        list_pages_data: list[list[dict[str, Union[str, int, float, date]]]] = []
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()

            self.cls_create_log.log_message(
                self.logger, 
                f"🚀 Starting scraping from pages {self.start_page} to {self.end_page}...", 
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

                elementos_nome = page.query_selector_all('[id^="item-nome-"]')
                
                list_page_data: list[dict[str, Union[str, int, float, date]]] = []
                for elemento in elementos_nome:
                    nome_texto = elemento.inner_text().strip()
                    if nome_texto:
                        item_id = elemento.get_attribute("id")
                        if item_id:
                            id_number = item_id.replace("item-nome-", "")
                            
                            debenture_data = self._extract_debenture_data(
                                page, id_number, nome_texto)
                            debenture_data["pagina"] = page_num
                            list_page_data.append(debenture_data)
                            
                self.cls_create_log.log_message(
                    self.logger, 
                    f"✅ Page {page_num}: {len(list_page_data)} itens", 
                    "info"
                )
                
                list_pages_data.extend(list_page_data)
                
                if page_num < self.end_page:
                    time.sleep(randint(2, 10))
            
            browser.close()
        
        self.cls_create_log.log_message(
            self.logger, 
            f"💾 Scraping finished. Total: {len(list_pages_data)} pages found.", 
            "info"
        )
        
        return list_pages_data
    
    def _extract_debenture_data(
        self, 
        page: PlaywrightPage, 
        id_number: str, 
        nome: str
    ) -> dict:
        """Extract debenture data using specific XPaths.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        id_number : str
            The ID number of the debenture item.
        nome : str
            The name of the debenture.
        
        Returns
        -------
        dict
            Dictionary containing extracted debenture data.
        """
        data = {"NOME": nome}

        xpath_mapping = {
            'EMISSOR': f'[id="debentures-item-emissor-{id_number}"] dd',
            'REMUNERACAO': f'[id="debentures-item-remuneracao-{id_number}"] dd',
            'DATA_VENCIMENTO': f'[id="debentures-item-data-vencimento-{id_number}"] dd',
            'DURATION': f'[id="debentures-item-duration-{id_number}"] dd',
            'SETOR': f'[id="debentures-item-setor-{id_number}"] dd',
            'DATA_EMISSAO': f'[id="debentures-item-data-emissao-{id_number}"] dd',
            'PU_PAR': f'[id="debentures-item-pu-par-{id_number}"] dd',
            'PU_INDICATIVO': f'[id="debentures-item-pu-indicativo-{id_number}"] dd'
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
        
        if 'pagina' in df_.columns:
            df_ = df_.drop('pagina', axis=1)
        
        return df_


class AnbimaDataDebenturesCharacteristics(ABCIngestionOperations):
    """Anbima Debentures characteristics ingestion class."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        debenture_codes: Optional[list[str]] = None,
    ) -> None:
        """Initialize the Anbima Debentures characteristics ingestion class.
        
        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.
        debenture_codes : Optional[list[str]], optional
            List of debenture codes to scrape characteristics for, by default None.
        
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
        self.base_url = "https://data.anbima.com.br/debentures"
        self.debenture_codes = debenture_codes or []
    
    def run(
        self,
        timeout_ms: int = 30_000,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_anbimadata_debentures_characteristics"
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
            The name of the table, by default "br_anbimadata_debentures_characteristics"

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
                "CODIGO_DEBENTURE": str,
                "NUMERO_SERIE": str,
                "REMUNERACAO": str,
                "DATA_INICIO_RENTABILIDADE": "date",
                "PERIODO_CAPITALIZACAO_PAPEL": str,
                "QUANTIDADE_SERIE_DATA_EMISSAO": str,
                "VOLUME_SERIE_DATA_EMISSAO": str,
                "VNE": str,
                "VNA": str,
                "QUANTIDADE_MERCADO_B3": str,
                "ESTOQUE_MERCADO_B3": str,
                "DATA_EMISSAO": "date",
                "DATA_VENCIMENTO": "date",
                "PROXIMA_REPACTUACAO": "date",
                "PRAZO_EMISSAO": str,
                "PRAZO_REMANESCENTE": str,
                "RESGATE_ANTECIPADO": str,
                "ISIN": str,
                "DATA_PROXIMO_EVENTO_AGENDA": "date",
                "LEI_12_431": str,
                "ARTIGO": str
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
        """Scrape debentures characteristics using Playwright.

        Parameters
        ----------
        timeout_ms : int, optional
            The timeout in milliseconds, by default 30_000
        
        Returns
        -------
        list
            List of scraped debentures characteristics data.
        """
        list_characteristics_data: list[dict[str, Union[str, int, float, date]]] = []
        
        if not self.debenture_codes:
            self.cls_create_log.log_message(
                self.logger, 
                "⚠️ No debenture codes provided. Cannot scrape characteristics.", 
                "warning"
            )
            return list_characteristics_data
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()

            self.cls_create_log.log_message(
                self.logger, 
                f"🚀 Starting characteristics scraping for {len(self.debenture_codes)} debentures...", 
                "info"
            )
            
            for debenture_code in self.debenture_codes:
                self.cls_create_log.log_message(
                    self.logger, 
                    f"📊 Fetching characteristics for: {debenture_code}...", 
                    "info"
                )
                
                try:
                    url = f"{self.base_url}/{debenture_code}/caracteristicas"
                    page.goto(url)
                    page.wait_for_timeout(timeout_ms)

                    characteristics_data = self._extract_debenture_data(
                        page, debenture_code, debenture_code)
                    list_characteristics_data.append(characteristics_data)
                    
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"✅ Successfully extracted characteristics for {debenture_code}", 
                        "info"
                    )
                    
                except Exception as e:
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"❌ Error processing {debenture_code}: {str(e)}", 
                        "error"
                    )
                
                # Random delay between requests
                time.sleep(randint(2, 8))
            
            browser.close()
        
        self.cls_create_log.log_message(
            self.logger, 
            "💾 Characteristics scraping finished. Total: {} records processed.".format(
                len(list_characteristics_data)
            ), 
            "info"
        )
        
        return list_characteristics_data
    
    def _extract_debenture_data(
        self, 
        page: PlaywrightPage, 
        id_number: str, 
        nome: str
    ) -> dict:
        """Extract debenture data using specific XPaths.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        id_number : str
            The ID number of the debenture item.
        nome : str
            The name of the debenture.
        
        Returns
        -------
        dict
            Dictionary containing extracted debenture data.
        """
        data = {"CODIGO_DEBENTURE": nome}

        xpath_mapping = {
            'NUMERO_SERIE': '//*[@id="root"]/main/div[3]/main/div/div[1]/article[1]/article/section/div/div[1]/h2 | //*[@id="root"]/main/div[3]/main/div/div[2]/article[1]/article/section/div/div[1]/h2',
            'REMUNERACAO': '//*[@id="output__container--remuneracao"]/div/span',
            'DATA_INICIO_RENTABILIDADE': '//*[@id="output__container--dataInicioRentabilidade"]/div/span',
            'PERIODO_CAPITALIZACAO_PAPEL': '//*[@id="output__container--expressaoPapel"]/div/span',
            'QUANTIDADE_SERIE_DATA_EMISSAO': '//*[@id="output__container--quantidadeSerieEmissao"]/div/span',
            'VOLUME_SERIE_DATA_EMISSAO': '//*[@id="output__container--volumeSerieEmissao"]/div/span',
            'VNE': '//*[@id="output__container--vne"]/div/span',
            'VNA': '//*[@id="output__container--vna"]/div/span/a',
            'QUANTIDADE_MERCADO_B3': '//*[@id="output__container--quantidadeMercadoB3"]/div/span',
            'ESTOQUE_MERCADO_B3': '//*[@id="output__container--estoqueMercadoB3"]/div/span',
            'DATA_EMISSAO': '//*[@id="output__container--dataEmissao"]/div/span',
            'DATA_VENCIMENTO': '//*[@id="output__container--dataVencimento"]/div/span',
            'PROXIMA_REPACTUACAO': '//*[@id="output__container--dataProximaRepactuacao"]/div/span',
            'PRAZO_EMISSAO': '//*[@id="output__container--prazoEmissao"]/div/span',
            'PRAZO_REMANESCENTE': '//*[@id="output__container--prazoRemanescente"]/div/span',
            'RESGATE_ANTECIPADO': '//*[@id="output__container--resgateAntecipado"]/div/span',
            'ISIN': '//*[@id="output__container--isin"]/div/span',
            'DATA_PROXIMO_EVENTO_AGENDA': '//*[@id="output__container--dataProximoEventoAgenda"]/div/span',
            'LEI_12_431': '//*[@id="output__container--lei12431"]/div/span',
            'ARTIGO': '//*[@id="output__container--artigo"]/div/span'
        }

        for field_name, xpath in xpath_mapping.items():
            try:
                element = page.locator(f"xpath={xpath}").first
                if element.is_visible(timeout=5000):
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
        """Transform scraped characteristics data into a DataFrame.
        
        Parameters
        ----------
        raw_data : list
            The scraped characteristics data list.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
        
        df_ = pd.DataFrame(raw_data)

        for col_ in [
            "DATA_INICIO_RENTABILIDADE", 
            "PROXIMA_REPACTUACAO", 
            "DATA_PROXIMO_EVENTO_AGENDA"
        ]:
            df_[col_] = [x.replace("-", "01/01/2100") for x in df_[col_]]
        
        return df_