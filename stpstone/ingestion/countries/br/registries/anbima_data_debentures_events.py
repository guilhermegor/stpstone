"""Implementation of Anbima Debentures events ingestion instance."""

from contextlib import suppress
from datetime import date
from io import StringIO
from logging import Logger
from random import randint
import re
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


class AnbimaDataDebenturesEvents(ABCIngestionOperations):
    """Anbima Debentures events (agenda) ingestion class."""

    def __init__(
        self,
        date_ref: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        debenture_codes: Optional[list[str]] = None,
    ) -> None:
        """Initialize the Anbima Debentures events ingestion class.

        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.
        debenture_codes : Optional[list[str]], optional
            List of debenture codes to scrape events for, by default None.

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
        str_table_name: str = "br_anbimadata_debentures_events"
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
            The name of the table, by default "br_anbimadata_debentures_events"

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
                "DATA_EVENTO": "date",
                "DATA_LIQUIDACAO": "date",
                "EVENTO": str,
                "PERCENTUAL_TAXA": str,
                "VALOR_PAGO": str,
                "STATUS": str,
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
        """Scrape debentures events using Playwright with pagination.

        Parameters
        ----------
        timeout_ms : int, optional
            The timeout in milliseconds, by default 30_000

        Returns
        -------
        list
            List of scraped debentures events data.
        """
        list_events_data: list[dict[str, Union[str, int, float, date]]] = []

        if not self.debenture_codes:
            self.cls_create_log.log_message(
                self.logger,
                "No debenture codes provided. Cannot scrape events.",
                "warning"
            )
            return list_events_data

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()

            self.cls_create_log.log_message(
                self.logger,
                f"Starting events scraping for {len(self.debenture_codes)} debentures...",
                "info"
            )

            for debenture_code in self.debenture_codes:
                self.cls_create_log.log_message(
                    self.logger,
                    f"Fetching events for: {debenture_code}...",
                    "info"
                )

                try:
                    url = f"{self.base_url}/{debenture_code}/agenda?page=1&size=100"
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
                            f"{self.base_url}/{debenture_code}/agenda?page={page_num}&size=100"

                        if page_num > 1:
                            page.goto(current_url)
                            page.wait_for_timeout(timeout_ms)

                        events_data = self._extract_debenture_events(
                            page, debenture_code, current_url
                        )
                        list_events_data.extend(events_data)

                        self.cls_create_log.log_message(
                            self.logger,
                            f"Extracted {len(events_data)} event records from page {page_num}",
                            "info"
                        )

                        if page_num < total_pages:
                            time.sleep(randint(1, 3))  # noqa S311: standard pseudo-random generator are not cryptographically secure

                    self.cls_create_log.log_message(
                        self.logger,
                        f"Successfully extracted all events for {debenture_code}",
                        "info"
                    )

                except Exception as e:
                    self.cls_create_log.log_message(
                        self.logger,
                        f"Error processing {debenture_code}: {str(e)}",
                        "error"
                    )

                time.sleep(randint(2, 8))  # noqa S311: standard pseudo-random generator are not cryptographically secure

            browser.close()

        self.cls_create_log.log_message(
            self.logger,
            f"Events scraping finished. Total: {len(list_events_data)} records processed.",
            "info"
        )

        return list_events_data

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
            pagination_text = page.locator(
                'xpath=//*[@id="pagination"]/div[3]/span/text()[2]'
            ).first

            if pagination_text.is_visible(timeout=5000):
                text = pagination_text.inner_text().strip()
                match = re.search(r'(\d+)', text)
                if match:
                    total_pages = int(match.group(1))
                    return total_pages
        except Exception:
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

    def _extract_debenture_events(
        self,
        page: PlaywrightPage,
        debenture_code: str,
        url: str
    ) -> list[dict]:
        """Extract debenture events data from the page.

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
            List of dictionaries containing extracted event data.
        """
        codigo_debenture = self._extract_codigo_debenture(page, debenture_code)
        emissor = self._extract_emissor(page)
        setor = self._extract_setor(page)

        event_records = self._extract_event_rows(
            page, codigo_debenture, emissor, setor, url
        )

        return event_records

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
        with suppress(Exception):
            element = page.locator(
                'xpath=//*[@id="root"]/main/div[1]/div/div/h1'
            ).first
            if element.is_visible(timeout=5000):
                return element.inner_text().strip()
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
        with suppress(Exception):
            element = page.locator(
                'xpath=//*[@id="root"]/main/div[1]/div/div/div/dl[1]/dd'
            ).first
            if element.is_visible(timeout=5000):
                return element.inner_text().strip()
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
            element = page.locator(
                'xpath=//*[@id="root"]/main/div[1]/div/div/div/dl[2]/dd'
            ).first
            if element.is_visible(timeout=5000):
                return element.inner_text().strip()
        return None

    def _extract_event_rows(
        self,
        page: PlaywrightPage,
        codigo_debenture: str,
        emissor: Optional[str],
        setor: Optional[str],
        url: str
    ) -> list[dict]:
        """Extract all event rows from the table.

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
            List of event records.
        """
        event_records = []

        data_evento_elements = page.locator(
            'xpath=//*[contains(@id, "agenda-data-evento-")]'
        ).all()

        if not data_evento_elements:
            self.cls_create_log.log_message(
                self.logger,
                f'No event data found for {codigo_debenture}',
                "warning"
            )
            return event_records

        self.cls_create_log.log_message(
            self.logger,
            f'Found {len(data_evento_elements)} event records for {codigo_debenture}',
            "info"
        )

        for idx in range(len(data_evento_elements)):
            try:
                event_record = self._extract_single_event_row(
                    page, idx, codigo_debenture, emissor, setor, url
                )
                if event_record:
                    event_records.append(event_record)
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger,
                    f'Error extracting event row {idx} for {codigo_debenture}: {e}',
                    "warning"
                )

        return event_records

    def _extract_single_event_row(
        self,
        page: PlaywrightPage,
        row_idx: int,
        codigo_debenture: str,
        emissor: Optional[str],
        setor: Optional[str],
        url: str
    ) -> Optional[dict]:
        """Extract a single event row.

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
            Event record or None if extraction fails.
        """
        try:
            data_evento_xpath = f'//*[@id="agenda-data-evento-{row_idx}"]'
            data_evento = self._extract_text_by_xpath(page, data_evento_xpath)

            data_liquidacao_xpath = f'//*[@id="agenda-data-liquidacao-{row_idx}"]'
            data_liquidacao = self._extract_text_by_xpath(page, data_liquidacao_xpath)

            evento_xpath = f'//*[@id="agenda-evento-{row_idx}"]'
            evento = self._extract_text_by_xpath(page, evento_xpath)

            taxa_xpath = f'//*[@id="agenda-taxa-{row_idx}"]'
            percentual_taxa = self._extract_text_by_xpath(page, taxa_xpath)

            valor_xpath = f'//*[@id="agenda-valor-{row_idx}"]'
            valor_pago = self._extract_text_by_xpath(page, valor_xpath)

            status_xpath = f'//*[@id="agenda-status-{row_idx}"]/label'
            status = self._extract_text_by_xpath(page, status_xpath)

            return {
                "CODIGO_DEBENTURE": codigo_debenture,
                "EMISSOR": emissor,
                "SETOR": setor,
                "DATA_EVENTO": data_evento,
                "DATA_LIQUIDACAO": data_liquidacao,
                "EVENTO": evento,
                "PERCENTUAL_TAXA": percentual_taxa,
                "VALOR_PAGO": valor_pago,
                "STATUS": status,
                "URL": url,
            }

        except Exception as e:
            self.cls_create_log.log_message(
                self.logger,
                f'Error in _extract_single_event_row for row {row_idx}: {e}',
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
        """Transform scraped events data into a DataFrame.

        Parameters
        ----------
        raw_data : list
            The scraped events data list.

        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()

        df_ = pd.DataFrame(raw_data)

        for col_ in ["DATA_EVENTO", "DATA_LIQUIDACAO"]:
            if col_ in df_.columns:
                df_[col_] = df_[col_].apply(
                    lambda x: x.replace("-", "01/01/2100")
                        if x and isinstance(x, str) else "01/01/2100"
                )

        return df_
