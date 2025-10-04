"""Implementation of Anbima Debentures ingestion instance."""

from contextlib import suppress
from datetime import date
from io import StringIO
from logging import Logger
from random import randint
import re
import time
from typing import Optional, TypedDict, Union
from urllib.parse import unquote

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


class ResultDocumentRecord(TypedDict):
    """Result Document Record."""

    CODIGO_DEBENTURE: str
    EMISSOR: str
    SETOR: str
    NOME_DOCUMENTO: str
    DATA_DIVULGACAO_DOCUMENTO: str
    LINK_DOCUMENTO: str


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

        Notes
        -----
        [1] Metadata: https://data.anbima.com.br/busca/debentures
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
                "DATA_PROXIMA_REPACTUACAO": "date",
                "PRAZO_EMISSAO": str,
                "PRAZO_REMANESCENTE": str,
                "RESGATE_ANTECIPADO": str,
                "ISIN": str,
                "DATA_PROXIMO_EVENTO_AGENDA": "date",
                "LEI_12_431": str,
                "ARTIGO": str,
                "EMISSAO": str,
                "EMPRESA": str,
                "SETOR": str,
                "CNPJ": str,
                "VOLUME_EMISSAO": str,
                "QUANTIDADE_EMISSAO": str,
                "COORDENADOR_LIDER_NOME": str,
                "COORDENADOR_LIDER_CNPJ": str,
                "AGENTE_FIDUCIARIO_NOME": str,
                "AGENTE_FIDUCIARIO_CNPJ": str,
                "BANCO_MANDATARIO_NOME": str,
                "GARANTIA": str,
                "PU_PAR": str,
                "PU_INDICATIVO": str,
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
                f"🚀 Starting characteristics scraping for {len(self.debenture_codes)} "
                "debentures...", 
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
            'CODIGO_DEBENTURE': '//*[@id="root"]/main/div[1]/div/div/h1',
            'NUMERO_SERIE': '//*[@id="root"]/main/div[3]/main/div/div[1]/article[1]/article/section/div/div[1]/h2 | //*[@id="root"]/main/div[3]/main/div/div[2]/article[1]/article/section/div/div[1]/h2',
            'REMUNERACAO': '//*[@id="output__container--remuneracao"]/div/span',
            'DATA_INICIO_RENTABILIDADE': '//*[@id="output__container--dataInicioRentabilidade"]/div/span',
            'PERIODO_CAPITALIZACAO_PAPEL': '//*[@id="output__container--expressaoPapel"]/div/span',
            'QUANTIDADE_SERIE_DATA_EMISSAO': '//*[@id="output__container--quantidadeSerieEmissao"]/div/span',
            'VOLUME_SERIE_DATA_EMISSAO': '//*[@id="output__container--volumeSerieEmissao"]/div/span',
            'VNE': '//*[@id="output__container--vne"]/div//span[@class="anbima-ui-output__value"]',
            'VNA': '//*[@id="output__container--vna"]/div//span[@class="anbima-ui-output__value"]',
            'QUANTIDADE_MERCADO_B3': '//*[@id="output__container--quantidadeMercadoB3"]/div/span',
            'ESTOQUE_MERCADO_B3': '//*[@id="output__container--estoqueMercadoB3"]/div/span',
            'DATA_EMISSAO': '//*[@id="output__container--dataEmissao"]/div/span',
            'DATA_VENCIMENTO': '//*[@id="output__container--dataVencimento"]/div/span',
            'DATA_PROXIMA_REPACTUACAO': '//*[@id="output__container--dataProximaRepactuacao"]/div/span',
            'PRAZO_EMISSAO': '//*[@id="output__container--prazoEmissao"]/div/span',
            'PRAZO_REMANESCENTE': '//*[@id="output__container--prazoRemanescente"]/div/span',
            'RESGATE_ANTECIPADO': '//*[@id="output__container--resgateAntecipado"]/div/span',
            'ISIN': '//*[@id="output__container--isin"]/div/span',
            'DATA_PROXIMO_EVENTO_AGENDA': '//*[@id="output__container--dataProximoEventoAgenda"]/div/span',
            'LEI_12_431': '//*[@id="output__container--lei12431"]/div/span',
            'ARTIGO': '//*[@id="output__container--artigo"]/div/span', 
            'EMISSAO': '//div[@id="root"]/main/div[3]/main/div/div[1]/article[2]/article/section/div/div[1]/h2',
            'EMPRESA': '//*[@id="output__container--empresa"]/div/span',
            'SETOR': '//*[@id="output__container--setor"]/div/span', 
            'CNPJ': '//*[@id="output__container--cnpj"]/div/span',
            'VOLUME_EMISSAO': '//*[@id="output__container--volumeEmissao"]/div/span', 
            'QUANTIDADE_EMISSAO': '//*[@id="output__container--quantidadeEmissao"]/div/span',
            'QUANTIDADE_MERCADO_B3': '//*[@id="output__container--quantidadeMercadoB3Emissao"]/div/span',
            'COORDENADOR_LIDER_NOME': '//*[@id="output__container--coordenadorLider"]/div/span/div/span[1]',
            'COORDENADOR_LIDER_CNPJ': '//*[@id="output__container--coordenadorLider"]/div/span/div/span[2]', 
            'AGENTE_FIDUCIARIO_NOME': '//*[@id="output__container--agenteFiduciario"]/div/span/div/span[1]', 
            'AGENTE_FIDUCIARIO_CNPJ': '//*[@id="output__container--agenteFiduciario"]/div/span/div/span[2]',
            'BANCO_MANDATARIO_NOME': '//*[@id="output__container--bancoMandatorio"]/div/span',
            'GARANTIA': '//*[@id="output__container--garantia"]/div/span',
            'PU_PAR': '//*[@id="root"]/main/div[3]/main/div/div[2]/div/article/article/section/div/ul[2]/li[1]/p[2]', 
            'PU_INDICATIVO': '//*[@id="root"]/main/div[3]/main/div/div[2]/div/article/article/section/div/ul[2]/li[2]/p[2]', 
            'PU_PAR': '//*[@id="root"]/main/div[3]/main/div/div[2]/div/article/article/section/div/ul[2]/li[3]/p[2]',
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
            "DATA_EMISSAO",
            "DATA_VENCIMENTO", 
            "DATA_PROXIMA_REPACTUACAO",
            "DATA_PROXIMO_EVENTO_AGENDA",
        ]:
            df_[col_] = [x.replace("-", "01/01/2100") for x in df_[col_]]
        
        return df_


class AnbimaDataDebenturesDocuments(ABCIngestionOperations):
    """Anbima Debentures documents ingestion class."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        debenture_codes: Optional[list[str]] = None,
    ) -> None:
        """Initialize the Anbima Debentures documents ingestion class.
        
        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.
        debenture_codes : Optional[list[str]], optional
            List of debenture codes to scrape documents for, by default None.
        
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
        str_table_name: str = "br_anbimadata_debentures_documents"
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
                "EMISSOR": str,
                "SETOR": str,
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
        """Scrape debentures documents using Playwright.

        Parameters
        ----------
        timeout_ms : int, optional
            The timeout in milliseconds, by default 30_000
        
        Returns
        -------
        list
            List of scraped debentures documents data.
        """
        list_documents_data: list[dict[str, Union[str, int, float, date]]] = []
        
        if not self.debenture_codes:
            self.cls_create_log.log_message(
                self.logger, 
                "⚠️ No debenture codes provided. Cannot scrape documents.", 
                "warning"
            )
            return list_documents_data
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()

            self.cls_create_log.log_message(
                self.logger, 
                f"🚀 Starting documents scraping for {len(self.debenture_codes)} debentures...", 
                "info"
            )
            
            for debenture_code in self.debenture_codes:
                self.cls_create_log.log_message(
                    self.logger, 
                    f"📊 Fetching documents for: {debenture_code}...", 
                    "info"
                )
                
                try:
                    url = f"{self.base_url}/{debenture_code}/documentos"
                    page.goto(url)
                    page.wait_for_timeout(timeout_ms)
                    
                    documents_data = self._extract_debenture_data(page, debenture_code)
                    list_documents_data.extend(documents_data)
                    
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"✅ Successfully extracted {len(documents_data)} documents "
                        f"for {debenture_code}", 
                        "info"
                    )
                    
                except Exception as e:
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"❌ Error processing {debenture_code}: {str(e)}", 
                        "error"
                    )
                
                time.sleep(randint(2, 8))
            
            browser.close()
        
        self.cls_create_log.log_message(
            self.logger, 
            f"Documents scraping finished. Total: {len(list_documents_data)} records processed.", 
            "info"
        )
        
        return list_documents_data
    
    def _extract_debenture_data(
        self, 
        page: PlaywrightPage,
        nome: str
    ) -> list[dict]:
        """Extract debenture documents data using specific XPaths.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        nome : str
            The name/code of the debenture.
        
        Returns
        -------
        list[dict]
            List of dictionaries containing extracted document data.
        """
        codigo_debenture = self._extract_codigo_debenture(page, nome)
        emissor = self._extract_emissor(page)
        setor = self._extract_setor(page)
        
        document_containers = self._find_document_containers(page, codigo_debenture)
        
        if not document_containers:
            return []
        
        list_documents: list[dict[str, Union[str, int, float, date]]] = []
        for idx, container in enumerate(document_containers):
            documents = self._process_document_container(
                container, idx, codigo_debenture, emissor, setor, page
            )
            list_documents.extend(documents)
        
        return list_documents

    def _extract_codigo_debenture(
        self, 
        page: PlaywrightPage, 
        fallback_nome: str
    ) -> str:
        """Extract codigo debenture from page header.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        fallback_nome : str
            Fallback value if extraction fails.
        
        Returns
        -------
        str
            The extracted codigo debenture.
        """
        try:
            codigo_element = page.locator(
                'xpath=//*[@id="root"]/main/div[1]/div/div/h1'
            ).first
            if codigo_element.is_visible(timeout=5000):
                return codigo_element.inner_text().strip()
        except Exception:
            pass
        return fallback_nome

    def _extract_emissor(self, page: PlaywrightPage) -> Optional[str]:
        """Extract emissor from page.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        
        Returns
        -------
        Optional[str]
            The extracted emissor or None.
        """
        try:
            emissor_element = page.locator(
                'xpath=//*[@id="root"]/main/div[1]/div/div/div/dl[1]/dd'
            ).first
            if emissor_element.is_visible(timeout=5000):
                return emissor_element.inner_text().strip()
        except Exception:
            pass
        return None

    def _extract_setor(self, page: PlaywrightPage) -> Optional[str]:
        """Extract setor from page.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        
        Returns
        -------
        Optional[str]
            The extracted setor or None.
        """
        try:
            setor_element = page.locator(
                'xpath=//*[@id="root"]/main/div[1]/div/div/div/dl[2]/dd'
            ).first
            if setor_element.is_visible(timeout=5000):
                return setor_element.inner_text().strip()
        except Exception:
            pass
        return None

    def _find_document_containers(
        self, 
        page: PlaywrightPage, 
        codigo_debenture: str,
    ) -> list:
        """Find all document containers on the page.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        codigo_debenture : str
            The debenture code for logging purposes.
        
        Returns
        -------
        list
            List of document container elements.
        """
        xpath_base = (
            '//div[@class="card-content__container " and '
            '@style="padding: 0px; margin-bottom: 24px;"]'
        )
        document_containers = page.locator(f'xpath={xpath_base}').all()
        
        if len(document_containers) == 0:
            self.cls_create_log.log_message(
                self.logger, 
                f'No document containers found for {codigo_debenture}', 
                "warning"
            )
        else:
            self.cls_create_log.log_message(
                self.logger, 
                f'Found {len(document_containers)} document containers for {codigo_debenture}', 
                "info"
            )
        
        return document_containers

    def _process_document_container(
        self,
        container,
        idx: int,
        codigo_debenture: str,
        emissor: Optional[str],
        setor: Optional[str],
        page: PlaywrightPage
    ) -> list[dict]:
        """Process a single document container and extract all documents.
        
        Parameters
        ----------
        container
            The document container element.
        idx : int
            Container index for logging.
        codigo_debenture : str
            The debenture code.
        emissor : Optional[str]
            The emissor value.
        setor : Optional[str]
            The setor value.
        page : PlaywrightPage
            The Playwright page object.
        
        Returns
        -------
        list[dict]
            List of document records extracted from this container.
        """
        try:
            nome_documento = self._extract_document_name(container)
            data_divulgacao = self._extract_document_date(container)
            link_elements = container.locator('xpath=.//a').all()
            
            if len(link_elements) > 0:
                return self._process_document_links(
                    link_elements, nome_documento, data_divulgacao,
                    codigo_debenture, emissor, setor, page, idx
                )
            else:
                self.cls_create_log.log_message(
                    self.logger, 
                    f'Document {idx + 1} ({nome_documento}): No links found', 
                    "warning"
                )
                return [self._create_document_record(
                    codigo_debenture, emissor, setor, 
                    nome_documento, data_divulgacao, None
                )]
                
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Error extracting document {idx + 1} for {codigo_debenture}: {e}', 
                "warning"
            )
            return []

    def _extract_document_name(self, container) -> Optional[str]:
        """Extract document name from container using multiple strategies.
        
        Parameters
        ----------
        container
            The document container element.
        
        Returns
        -------
        Optional[str]
            The extracted document name or None.
        """
        nome_documento = self._try_extract_by_xpath(
            container, './/p[@class="large-text-bold"]'
        )
        
        if not nome_documento:
            nome_documento = self._try_extract_by_xpath(
                container, './/div[@class="anbima-ui-card__header"]//p'
            )
            
        if not nome_documento:
            nome_documento = self._try_extract_by_xpath(
                container, './ancestor::article[1]//h2[@class="large-text-bold"]'
            )
            
        if not nome_documento:
            nome_documento = self._try_extract_by_xpath(container, './/p')

        if not nome_documento:
            nome_documento = self._extract_document_name_from_link(container)
        
        return nome_documento

    def _try_extract_by_xpath(self, container, xpath: str) -> Optional[str]:
        """Try to extract text from an element using XPath.
        
        Parameters
        ----------
        container
            The container element.
        xpath : str
            The XPath expression.
        
        Returns
        -------
        Optional[str]
            The extracted text or None.
        """
        try:
            element = container.locator(f'xpath={xpath}').first
            if element.count() > 0:
                text = element.inner_text().strip()
                return text if text else None
        except Exception:
            pass
        return None
    
    def _extract_document_name_from_link(self, container) -> Optional[str]:
        """Extract document name from the first link URL in the container.
        
        This method clicks the first link, captures the S3 URL from the new tab,
        and parses the filename to extract the document type.
        For example, from 'AALM11_Ata de AGD_20230411_000.pdf', 
        it extracts 'ATA DE AGD'.
        
        Parameters
        ----------
        container
            The document container element.
        
        Returns
        -------
        Optional[str]
            The extracted document name from URL or None.
        """
        try:
            link_element = container.locator('xpath=.//a').first
            if link_element.count() == 0:
                return None
            
            page = container.page
            
            try:
                with page.context.expect_page(timeout=10000) as new_page_info:
                    link_element.click(timeout=8000)
                
                new_page = new_page_info.value
                new_page.wait_for_load_state('domcontentloaded', timeout=8000)
                url = new_page.url
                new_page.close()
                
                filename = url.split('/')[-1]
                
                filename = unquote(filename)
                
                filename_no_ext = filename.rsplit('.', 1)[0]
                
                parts = filename_no_ext.split('_')
                
                if len(parts) >= 3:
                    document_name_parts = parts[1:-2]
                    
                    if document_name_parts:
                        document_name = ' '.join(document_name_parts).strip()
                        return document_name.upper() if document_name else None
                        
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger, 
                    f'Error extracting document name from URL: {e}', 
                    "warning"
                )
                
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Error extracting document name from link URL: {e}', 
                "warning"
            )
        
        return None

    def _extract_document_date(self, container) -> Optional[str]:
        """Extract document release date from container.
        
        Parameters
        ----------
        container
            The document container element.
        
        Returns
        -------
        Optional[str]
            The extracted date or None.
        """
        return self._try_extract_by_xpath(container, './/span[@class="normal-text"]')

    def _process_document_links(
        self,
        link_elements: list,
        nome_documento: Optional[str],
        data_divulgacao: Optional[str],
        codigo_debenture: str,
        emissor: Optional[str],
        setor: Optional[str],
        page: PlaywrightPage,
        container_idx: int
    ) -> list[dict]:
        """Process all links in a document container.
        
        Parameters
        ----------
        link_elements : list
            List of link elements.
        nome_documento : Optional[str]
            The document name.
        data_divulgacao : Optional[str]
            The document date.
        codigo_debenture : str
            The debenture code.
        emissor : Optional[str]
            The emissor value.
        setor : Optional[str]
            The setor value.
        page : PlaywrightPage
            The Playwright page object.
        container_idx : int
            Container index for logging.
        
        Returns
        -------
        list[dict]
            List of document records.
        """
        self.cls_create_log.log_message(
            self.logger, 
            f'Document {container_idx + 1} ({nome_documento}): '
            f'Found {len(link_elements)} links', 
            "info"
        )
        
        list_documents: list[dict] = []
        for link_idx, link_element in enumerate(link_elements):
            link_document = self._extract_link_url(
                link_element, page, link_idx, len(link_elements)
            )
            
            if link_document:
                list_documents.append(self._create_document_record(
                    codigo_debenture, emissor, setor,
                    nome_documento, data_divulgacao, link_document
                ))
                time.sleep(1)
        
        return list_documents

    def _extract_link_url(
        self,
        link_element,
        page: PlaywrightPage,
        link_idx: int,
        total_links: int
    ) -> Optional[str]:
        """Extract URL by clicking a link and capturing the new page URL.
        
        Parameters
        ----------
        link_element
            The link element to click.
        page : PlaywrightPage
            The Playwright page object.
        link_idx : int
            Link index for logging.
        total_links : int
            Total number of links for logging.
        
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
                f'  Link {link_idx + 1}/{total_links}: {link_document}', 
                "info"
            )
            
            return link_document
            
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'  Error extracting link {link_idx + 1}: {e}', 
                "warning"
            )
            return None

    def _create_document_record(
        self,
        codigo_debenture: str,
        emissor: Optional[str],
        setor: Optional[str],
        nome_documento: Optional[str],
        data_divulgacao: Optional[str],
        link_document: Optional[str]
    ) -> ResultDocumentRecord:
        """Create a document record dictionary.
        
        Parameters
        ----------
        codigo_debenture : str
            The debenture code.
        emissor : Optional[str]
            The emissor value.
        setor : Optional[str]
            The setor value.
        nome_documento : Optional[str]
            The document name.
        data_divulgacao : Optional[str]
            The document date.
        link_document : Optional[str]
            The document link.
        
        Returns
        -------
        dict
            The document record.
        """
        return {
            "CODIGO_DEBENTURE": codigo_debenture,
            "EMISSOR": emissor,
            "SETOR": setor,
            "NOME_DOCUMENTO": nome_documento,
            "DATA_DIVULGACAO_DOCUMENTO": data_divulgacao,
            "LINK_DOCUMENTO": link_document,
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
    

class AnbimaDataDebenturesPrices(ABCIngestionOperations):
    """Anbima Debentures prices ingestion class."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        debenture_codes: Optional[list[str]] = None,
    ) -> None:
        """Initialize the Anbima Debentures prices ingestion class.
        
        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.
        debenture_codes : Optional[list[str]], optional
            List of debenture codes to scrape prices for, by default None.
        
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
        str_table_name: str = "br_anbimadata_debentures_prices"
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
            The name of the table, by default "br_anbimadata_debentures_prices"

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
                "EMISSOR": str,
                "SETOR": str,
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
        """Scrape debentures prices using Playwright with pagination.

        Parameters
        ----------
        timeout_ms : int, optional
            The timeout in milliseconds, by default 30_000
        
        Returns
        -------
        list
            List of scraped debentures prices data.
        """
        list_prices_data: list[dict[str, Union[str, int, float, date]]] = []
        
        if not self.debenture_codes:
            self.cls_create_log.log_message(
                self.logger, 
                "⚠️ No debenture codes provided. Cannot scrape prices.", 
                "warning"
            )
            return list_prices_data
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()

            self.cls_create_log.log_message(
                self.logger, 
                f"🚀 Starting prices scraping for {len(self.debenture_codes)} debentures...", 
                "info"
            )
            
            for debenture_code in self.debenture_codes:
                self.cls_create_log.log_message(
                    self.logger, 
                    f"📊 Fetching prices for: {debenture_code}...", 
                    "info"
                )
                
                try:
                    url = f"{self.base_url}/{debenture_code}/precos?page=1&size=100"
                    page.goto(url)
                    page.wait_for_timeout(timeout_ms)
                    
                    total_pages = self._get_total_pages(page)
                    
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"Found {total_pages} pages for {debenture_code}", 
                        "info"
                    )
                    
                    for page_num in range(1, total_pages + 1):
                        self.cls_create_log.log_message(
                            self.logger, 
                            f"Fetching page {page_num}/{total_pages} for {debenture_code}...", 
                            "info"
                        )
                        
                        current_url = \
                            f"{self.base_url}/{debenture_code}/precos?page={page_num}&size=100"
                        
                        if page_num > 1:
                            page.goto(current_url)
                            page.wait_for_timeout(timeout_ms)
                        
                        prices_data = self._extract_debenture_prices(
                            page, debenture_code, current_url
                        )
                        list_prices_data.extend(prices_data)
                        
                        self.cls_create_log.log_message(
                            self.logger, 
                            f"✅ Extracted {len(prices_data)} price records from page {page_num}", 
                            "info"
                        )
                        
                        if page_num < total_pages:
                            time.sleep(randint(1, 3))
                    
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"✅ Successfully extracted all prices for {debenture_code}", 
                        "info"
                    )
                    
                except Exception as e:
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"❌ Error processing {debenture_code}: {str(e)}", 
                        "error"
                    )
                
                time.sleep(randint(2, 8))
            
            browser.close()
        
        self.cls_create_log.log_message(
            self.logger, 
            f"💾 Prices scraping finished. Total: {len(list_prices_data)} records processed.", 
            "info"
        )
        
        return list_prices_data

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
                'xpath=//*[@id="pagination"]/div[3]/span/a'
            ).last
            
            if pagination_element.is_visible(timeout=5000):
                last_page_text = pagination_element.inner_text().strip()
                total_pages = int(last_page_text)
                return total_pages
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f'Could not determine total pages, defaulting to 1: {e}', 
                "warning"
            )
        
        return 1
    
    def _extract_debenture_prices(
        self, 
        page: PlaywrightPage, 
        debenture_code: str,
        url: str
    ) -> list[dict]:
        """Extract debenture prices data from the page.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        debenture_code : str
            The debenture code.
        url : str
            The current page URL.
        
        Returns
        -------
        list[dict]
            List of dictionaries containing extracted price data.
        """
        codigo_debenture = self._extract_codigo_debenture(page, debenture_code)
        emissor = self._extract_emissor(page)
        setor = self._extract_setor(page)
        
        price_records = self._extract_price_rows(
            page, codigo_debenture, emissor, setor, url
        )
        
        return price_records
    
    def _extract_codigo_debenture(
        self, 
        page: PlaywrightPage, 
        fallback_code: str
    ) -> str:
        """Extract codigo debenture from page header.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        fallback_code : str
            Fallback value if extraction fails.
        
        Returns
        -------
        str
            The extracted codigo debenture.
        """
        try:
            element = page.locator(
                'xpath=//*[@id="root"]/main/div[1]/div/div/h1'
            ).first
            if element.is_visible(timeout=5000):
                return element.inner_text().strip()
        except Exception:
            pass
        return fallback_code
    
    def _extract_emissor(self, page: PlaywrightPage) -> Optional[str]:
        """Extract emissor from page.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        
        Returns
        -------
        Optional[str]
            The extracted emissor or None.
        """
        try:
            element = page.locator(
                'xpath=//*[@id="root"]/main/div[1]/div/div/div/dl[1]/dd'
            ).first
            if element.is_visible(timeout=5000):
                return element.inner_text().strip()
        except Exception:
            pass
        return None
    
    def _extract_setor(self, page: PlaywrightPage) -> Optional[str]:
        """Extract setor from page.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        
        Returns
        -------
        Optional[str]
            The extracted setor or None.
        """
        try:
            element = page.locator(
                'xpath=//*[@id="root"]/main/div[1]/div/div/div/dl[2]/dd'
            ).first
            if element.is_visible(timeout=5000):
                return element.inner_text().strip()
        except Exception:
            pass
        return None
    
    def _extract_price_rows(
        self,
        page: PlaywrightPage,
        codigo_debenture: str,
        emissor: Optional[str],
        setor: Optional[str],
        url: str
    ) -> list[dict]:
        """Extract all price rows from the table.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        codigo_debenture : str
            The debenture code.
        emissor : Optional[str]
            The emissor value.
        setor : Optional[str]
            The setor value.
        url : str
            The current page URL.
        
        Returns
        -------
        list[dict]
            List of price records.
        """
        price_records = []
        
        data_ref_elements = page.locator(
            'xpath=//span[contains(@id, "pu-historico-tabela-dataReferencia-")]'
        ).all()
        
        if not data_ref_elements:
            self.cls_create_log.log_message(
                self.logger, 
                f'No price data found for {codigo_debenture}', 
                "warning"
            )
            return price_records
        
        self.cls_create_log.log_message(
            self.logger, 
            f'Found {len(data_ref_elements)} price records for {codigo_debenture}', 
            "info"
        )
        
        for idx in range(len(data_ref_elements)):
            try:
                price_record = self._extract_single_price_row(
                    page, idx, codigo_debenture, emissor, setor, url
                )
                if price_record:
                    price_records.append(price_record)
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger, 
                    f'Error extracting price row {idx} for {codigo_debenture}: {e}', 
                    "warning"
                )
        
        return price_records
    
    def _extract_single_price_row(
        self,
        page: PlaywrightPage,
        row_idx: int,
        codigo_debenture: str,
        emissor: Optional[str],
        setor: Optional[str],
        url: str
    ) -> Optional[dict]:
        """Extract a single price row.
        
        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        row_idx : int
            The row index.
        codigo_debenture : str
            The debenture code.
        emissor : Optional[str]
            The emissor value.
        setor : Optional[str]
            The setor value.
        url : str
            The current page URL.
        
        Returns
        -------
        Optional[dict]
            Price record or None if extraction fails.
        """
        try:
            data_ref_xpath = f'//span[@id="pu-historico-tabela-dataReferencia-{row_idx}"]'
            data_ref = self._extract_text_by_xpath(page, data_ref_xpath)
            
            vna_xpath = f'//span[@id="pu-historico-tabela-vna-{row_idx}"]'
            vna = self._extract_text_by_xpath(page, vna_xpath)
            
            pu_par_xpath = f'//span[@id="pu-historico-tabela-puPar-{row_idx}"]'
            pu_par = self._extract_text_by_xpath(page, pu_par_xpath)
            
            pu_evento_xpath = f'//span[@id="pu-historico-tabela-puEvento-{row_idx}"]'
            pu_evento = self._extract_text_by_xpath(page, pu_evento_xpath)
            
            return {
                "CODIGO_DEBENTURE": codigo_debenture,
                "EMISSOR": emissor,
                "SETOR": setor,
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
        try:
            element = page.locator(f'xpath={xpath}').first
            if element.is_visible(timeout=5000):
                text = element.inner_text().strip()
                return text if text else None
        except Exception:
            pass
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
        """Transform scraped prices data into a DataFrame.
        
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