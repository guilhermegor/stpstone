"""Implementation of Anbima CRI/CRA prices (web scraping) ingestion instance."""

from datetime import date
from io import StringIO
from logging import Logger
from random import randint
import re
import time
from typing import Optional, Union
from urllib.parse import urljoin

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
        date_ref : Optional[date]
            The date of reference, by default None.
        logger : Optional[Logger]
            The logger, by default None.
        cls_db : Optional[Session]
            The database session, by default None.
        start_page : int
            Starting page number, by default 0.
        end_page : Optional[int]
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
        timeout_ms : int
            The timeout in milliseconds, by default 30_000
        bool_insert_or_ignore : bool
            Whether to insert or ignore the data, by default False
        str_table_name : str
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
        timeout_ms : int
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
                for idx, _ in enumerate(price_articles):
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
