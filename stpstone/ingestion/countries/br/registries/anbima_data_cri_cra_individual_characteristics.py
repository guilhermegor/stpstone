"""Implementation of Anbima CRI/CRA individual characteristics ingestion instance."""

from datetime import date
from io import StringIO
from logging import Logger
from random import randint
import time
from typing import Any, Optional, TypedDict, Union

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


class CoordinatorResult(TypedDict):
    """Coordinator Result."""

    names: Optional[str]
    cnpjs: Optional[str]


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
        date_ref : Optional[date]
            The date of reference, by default None.
        logger : Optional[Logger]
            The logger, by default None.
        cls_db : Optional[Session]
            The database session, by default None.
        list_asset_codes : Optional[list[str]]
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
        timeout_ms : int
            The timeout in milliseconds, by default 30_000
        bool_insert_or_ignore : bool
            Whether to insert or ignore the data, by default False
        str_table_name : str
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
        timeout_ms : int
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
                "🚀 Starting CRI/CRA characteristics scraping for "
                f"{len(self.list_asset_codes)} assets...",
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
            'NOME_OPERACAO': '//*[@id="root"]/main/div[3]/main/div/div[2]/div/article/article/section/div/ul[1]/li[1]/span[2]', # noqa E501: line too long
            'SERIE_EMISSAO': '//*[@id="root"]/main/div[3]/main/div/div[2]/div/article/article/section/div/ul[1]/li[2]/span[2]', # noqa E501: line too long
            'DATA_EMISSAO': '//*[@id="root"]/main/div[3]/main/div/div[2]/div/article/article/section/div/ul[1]/li[3]/span[2]', # noqa E501: line too long
            'DATA_VENCIMENTO': '//*[@id="root"]/main/div[3]/main/div/div[2]/div/article/article/section/div/ul[1]/li[4]/span[2]', # noqa E501: line too long
            'REMUNERACAO': '//*[@id="root"]/main/div[3]/main/div/div[2]/div/article/article/section/div/ul[1]/li[5]/span[2]', # noqa E501: line too long
            'TAXA_INDICATIVA': '//*[@id="root"]/main/div[3]/main/div/div[2]/div/article/article/section/div/ul[2]/li[1]/p[2]', # noqa E501: line too long
            'PU_INDICATIVO': '//*[@id="root"]/main/div[3]/main/div/div[2]/div/article/article/section/div/ul[2]/li[2]/p[2]', # noqa E501: line too long
            'PU_PAR': '//*[@id="root"]/main/div[3]/main/div/div[2]/div/article/article/section/div/ul[2]/li[3]/p[2]', # noqa E501: line too long
            'SERIE_ATIVO': '//*[@id="root"]/main/div[3]/main/div/div[1]/article[1]/article/section/div/div[1]/h2', # noqa E501: line too long
            'REMUNERACAO_2': '//*[@id="output__container--remuneracao"]/div/span',
            'DATA_INICIO_RENTABILIDADE': '//*[@id="output__container--dataInicioRentabilidade"]/div/span', # noqa E501: line too long
            'EXPRESSAO_PAPEL': '//*[@id="output__container--expressaoPapel"]/div/span',
            'VOLUME_SERIE_DATA_EMISSAO': '//*[@id="output__container--volumeSerieEmissao"]/div/span', # noqa E501: line too long
            'QUANTIDADE_SERIE_DATA_EMISSAO': '//*[@id="output__container--quantidadeSerieEmissao"]/div/span', # noqa E501: line too long
            'VNE': '//*[@id="output__container--vne"]/div/span',
            'VNA': '//*[@id="output__container--vna"]/div/span/a',
            'PRAZO_EMISSAO': '//*[@id="output__container--prazoEmissao"]/div/span',
            'PRAZO_REMANESCENTE': '//*[@id="output__container--prazoRemanescente"]/div/span',
            'RESGATE_ANTECIPADO': '//*[@id="output__container--resgateAntecipado"]/div/span',
            'ISIN_2': '//*[@id="output__container--isin"]/div/span',
            'DATA_PROXIMO_EVENTO_AGENDA': '//*[@id="output__container--dataProximoEventoAgenda"]/div/span/a', # noqa E501: line too long
            'CLASSE_ATIVO': '//*[@id="output__container--classe"]/div/span',
            'CONCENTRACAO': '//*[@id="output__container--concentracao"]/div/span',
            'CATEGORIA': '//*[@id="output__container--categoria"]/div/span',
            'SETOR': '//*[@id="output__container--setor"]/div/span',
            'TIPO_LASTRO': '//*[@id="output__container--tipoLastro"]/div/span',
            'NUMERO_EMISSAO': '//*[@id="root"]/main/div[3]/main/div/div[1]/article[2]/article/section/div/div[1]/h2', # noqa E501: line too long
            'DEVEDOR_EMISSAO': '//*[@id="output__container--devedor"]/div/span',
            'CEDENTE': '//*[@id="output__container--cedente"]/div/span',
            'NOME_SECURITIZADORA': '//*[@id="output__container--securitizadora"]/div/span/div/span[1]', # noqa E501: line too long
            'CNPJ_SECURITIZADORA': '//*[@id="output__container--securitizadora"]/div/span/div/span[2]', # noqa E501: line too long
            'QUANTIDADE_EMISSAO': '//*[@id="output__container--quantidadeEmissao"]/div/span',
            'VOLUME_EMISSAO': '//*[@id="output__container--volumeEmissao"]/div/span',
            'TIPO_EMISSAO': '//*[@id="output__container--tipo"]/div/span',
            'NOME_COORDENADOR_LIDER': '//*[@id="output__container--coordenadorLider"]/div/span/div/span[1]', # noqa E501: line too long
            'CNPJ_COORDENADOR_LIDER': '//*[@id="output__container--coordenadorLider"]/div/span/div/span[2]', # noqa E501: line too long
            'NOME_AGENTE_FIDUCIARIO': '//*[@id="output__container--agenteFiduciario"]/div/span/div/span[1]', # noqa E501: line too long
            'CNPJ_AGENTE_FIDUCIARIO': '//*[@id="output__container--agenteFiduciario"]/div/span/div/span[2]', # noqa E501: line too long
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
    ) -> CoordinatorResult:
        """Extract all coordinators (names and CNPJs).

        Parameters
        ----------
        page : PlaywrightPage
            The Playwright page object.
        asset_code : str
            The asset code for logging.

        Returns
        -------
        CoordinatorResult
            Dictionary with 'names' and 'cnpjs' keys containing pipe-separated values.
        """
        result: CoordinatorResult = {'names': None, 'cnpjs': None}

        try:
            coordinator_containers = page.locator(
                'xpath=//*[@id="output__container--coordenadores"]/div/span/div'
            ).all()

            if not coordinator_containers:
                return result

            names: list[str] = []
            cnpjs: list[str] = []

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
