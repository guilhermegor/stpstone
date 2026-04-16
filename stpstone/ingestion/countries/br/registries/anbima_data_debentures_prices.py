"""Implementation of Anbima Debentures prices ingestion instance."""

from contextlib import suppress
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
		date_ref : Optional[date]
			The date of reference, by default None.
		logger : Optional[Logger]
			The logger, by default None.
		cls_db : Optional[Session]
			The database session, by default None.
		debenture_codes : Optional[list[str]]
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
		self.date_ref = date_ref or self.cls_dates_br.add_working_days(
			self.cls_dates_current.curr_date(), -1
		)
		self.base_url = "https://data.anbima.com.br/debentures"
		self.debenture_codes = debenture_codes or []

	def run(
		self,
		timeout_ms: int = 30_000,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_anbimadata_debentures_prices",
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
				bool_insert_or_ignore=bool_insert_or_ignore,
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
		timeout_ms : int
			The timeout in milliseconds, by default 30_000

		Returns
		-------
		list
			List of scraped debentures prices data.
		"""
		list_prices_data: list[dict[str, Union[str, int, float, date]]] = []

		if not self.debenture_codes:
			self.cls_create_log.log_message(
				self.logger, "No debenture codes provided. Cannot scrape prices.", "warning"
			)
			return list_prices_data

		with sync_playwright() as p:
			browser = p.chromium.launch(headless=False)
			page = browser.new_page()

			self.cls_create_log.log_message(
				self.logger,
				f"Starting prices scraping for {len(self.debenture_codes)} debentures...",
				"info",
			)

			for debenture_code in self.debenture_codes:
				self.cls_create_log.log_message(
					self.logger, f"Fetching prices for: {debenture_code}...", "info"
				)

				try:
					url = f"{self.base_url}/{debenture_code}/precos?page=1&size=100"
					page.goto(url)
					page.wait_for_timeout(timeout_ms)

					total_pages = self._get_total_pages(page)

					self.cls_create_log.log_message(
						self.logger, f"Found {total_pages} pages for {debenture_code}", "info"
					)

					for page_num in range(1, total_pages + 1):
						self.cls_create_log.log_message(
							self.logger,
							f"Fetching page {page_num}/{total_pages} for {debenture_code}...",
							"info",
						)

						current_url = (
							f"{self.base_url}/{debenture_code}/precos?page={page_num}&size=100"
						)

						if page_num > 1:
							page.goto(current_url)
							page.wait_for_timeout(timeout_ms)

						prices_data = self._extract_debenture_prices(
							page, debenture_code, current_url
						)
						list_prices_data.extend(prices_data)

						self.cls_create_log.log_message(
							self.logger,
							f"Extracted {len(prices_data)} price records from page {page_num}",
							"info",
						)

						if page_num < total_pages:
							time.sleep(randint(1, 3))  # noqa S311: standard pseudo-random generator are not cryptographically secure

					self.cls_create_log.log_message(
						self.logger,
						f"Successfully extracted all prices for {debenture_code}",
						"info",
					)

				except Exception as e:
					self.cls_create_log.log_message(
						self.logger, f"Error processing {debenture_code}: {str(e)}", "error"
					)

				time.sleep(randint(2, 8))  # noqa S311: standard pseudo-random generator are not cryptographically secure

			browser.close()

		self.cls_create_log.log_message(
			self.logger,
			f"Prices scraping finished. Total: {len(list_prices_data)} records processed.",
			"info",
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
			pagination_element = page.locator('xpath=//*[@id="pagination"]/div[3]/span/a').last

			if pagination_element.is_visible(timeout=5000):
				last_page_text = pagination_element.inner_text().strip()
				total_pages = int(last_page_text)
				return total_pages
		except Exception as e:
			self.cls_create_log.log_message(
				self.logger, f"Could not determine total pages, defaulting to 1: {e}", "warning"
			)

		return 1

	def _extract_debenture_prices(
		self, page: PlaywrightPage, debenture_code: str, url: str
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

		price_records = self._extract_price_rows(page, codigo_debenture, emissor, setor, url)

		return price_records

	def _extract_codigo_debenture(self, page: PlaywrightPage, fallback_code: str) -> str:
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
			element = page.locator('xpath=//*[@id="root"]/main/div[1]/div/div/h1').first
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
			element = page.locator('xpath=//*[@id="root"]/main/div[1]/div/div/div/dl[1]/dd').first
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
			element = page.locator('xpath=//*[@id="root"]/main/div[1]/div/div/div/dl[2]/dd').first
			if element.is_visible(timeout=5000):
				return element.inner_text().strip()
		return None

	def _extract_price_rows(
		self,
		page: PlaywrightPage,
		codigo_debenture: str,
		emissor: Optional[str],
		setor: Optional[str],
		url: str,
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
				self.logger, f"No price data found for {codigo_debenture}", "warning"
			)
			return price_records

		self.cls_create_log.log_message(
			self.logger,
			f"Found {len(data_ref_elements)} price records for {codigo_debenture}",
			"info",
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
					f"Error extracting price row {idx} for {codigo_debenture}: {e}",
					"warning",
				)

		return price_records

	def _extract_single_price_row(
		self,
		page: PlaywrightPage,
		row_idx: int,
		codigo_debenture: str,
		emissor: Optional[str],
		setor: Optional[str],
		url: str,
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
				f"Error in _extract_single_price_row for row {row_idx}: {e}",
				"warning",
			)
			return None

	def _extract_text_by_xpath(self, page: PlaywrightPage, xpath: str) -> Optional[str]:
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
			element = page.locator(f"xpath={xpath}").first
			if element.is_visible(timeout=5000):
				text = element.inner_text().strip()
				return text if text else None
		return None

	def parse_raw_file(
		self, resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
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

	def transform_data(self, raw_data: list) -> pd.DataFrame:
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
				if x and isinstance(x, str)
				else "01/01/2100"
			)

		return df_
