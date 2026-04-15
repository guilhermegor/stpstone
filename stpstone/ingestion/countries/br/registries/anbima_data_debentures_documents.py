"""Implementation of Anbima Debentures documents ingestion instance."""

from contextlib import suppress
from datetime import date
from io import StringIO
from logging import Logger
from random import randint
import time
from typing import Optional, TypedDict, Union
from urllib.parse import unquote

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

    CODIGO_DEBENTURE: str
    EMISSOR: str
    SETOR: str
    NOME_DOCUMENTO: str
    DATA_DIVULGACAO_DOCUMENTO: str
    LINK_DOCUMENTO: str


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
        date_ref : Optional[date]
            The date of reference, by default None.
        logger : Optional[Logger]
            The logger, by default None.
        cls_db : Optional[Session]
            The database session, by default None.
        debenture_codes : Optional[list[str]]
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
        timeout_ms : int
            The timeout in milliseconds, by default 30_000
        bool_insert_or_ignore : bool
            Whether to insert or ignore the data, by default False
        str_table_name : str
            The name of the table, by default "br_anbimadata_debentures_documents"

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
        timeout_ms : int
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
                "No debenture codes provided. Cannot scrape documents.",
                "warning"
            )
            return list_documents_data

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()

            self.cls_create_log.log_message(
                self.logger,
                f"Starting documents scraping for {len(self.debenture_codes)} debentures...",
                "info"
            )

            for debenture_code in self.debenture_codes:
                self.cls_create_log.log_message(
                    self.logger,
                    f"Fetching documents for: {debenture_code}...",
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
                        f"Successfully extracted {len(documents_data)} documents "
                        f"for {debenture_code}",
                        "info"
                    )

                except Exception as e:
                    self.cls_create_log.log_message(
                        self.logger,
                        f"Error processing {debenture_code}: {str(e)}",
                        "error"
                    )

                time.sleep(randint(2, 8))  # noqa S311: standard pseudo-random generators are not suitable for cryptographic purposes

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
        with suppress(Exception):
            codigo_element = page.locator(
                'xpath=//*[@id="root"]/main/div[1]/div/div/h1'
            ).first
            if codigo_element.is_visible(timeout=5000):
                return codigo_element.inner_text().strip()
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
        with suppress(Exception):
            emissor_element = page.locator(
                'xpath=//*[@id="root"]/main/div[1]/div/div/div/dl[1]/dd'
            ).first
            if emissor_element.is_visible(timeout=5000):
                return emissor_element.inner_text().strip()
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
        with suppress(Exception):
            setor_element = page.locator(
                'xpath=//*[@id="root"]/main/div[1]/div/div/div/dl[2]/dd'
            ).first
            if setor_element.is_visible(timeout=5000):
                return setor_element.inner_text().strip()
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
        container: Locator,
        idx: int,
        codigo_debenture: str,
        emissor: Optional[str],
        setor: Optional[str],
        page: PlaywrightPage
    ) -> list[dict]:
        """Process a single document container and extract all documents.

        Parameters
        ----------
        container : Locator
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

    def _extract_document_name(
        self,
        container: Locator
    ) -> Optional[str]:
        """Extract document name from container using multiple strategies.

        Parameters
        ----------
        container : Locator
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

    def _try_extract_by_xpath(
        self,
        container: Locator,
        xpath: str
    ) -> Optional[str]:
        """Try to extract text from an element using XPath.

        Parameters
        ----------
        container : Locator
            The container element.
        xpath : str
            The XPath expression.

        Returns
        -------
        Optional[str]
            The extracted text or None.
        """
        with suppress(Exception):
            element = container.locator(f'xpath={xpath}').first
            if element.count() > 0:
                text = element.inner_text().strip()
                return text if text else None
        return None

    def _extract_document_name_from_link(
        self,
        container: Locator
    ) -> Optional[str]:
        """Extract document name from the first link URL in the container.

        This method clicks the first link, captures the S3 URL from the new tab,
        and parses the filename to extract the document type.
        For example, from 'AALM11_Ata de AGD_20230411_000.pdf',
        it extracts 'ATA DE AGD'.

        Parameters
        ----------
        container : Locator
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

    def _extract_document_date(
        self,
        container: Locator
    ) -> Optional[str]:
        """Extract document release date from container.

        Parameters
        ----------
        container : Locator
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
        link_element: Locator,
        page: PlaywrightPage,
        link_idx: int,
        total_links: int
    ) -> Optional[str]:
        """Extract URL by clicking a link and capturing the new page URL.

        Parameters
        ----------
        link_element : Locator
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
        ResultDocumentRecord
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
