"""Implementation of Anbima CRI/CRA PU Historico ingestion instance."""

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
		date_ref : Optional[date]
			The date of reference, by default None.
		logger : Optional[Logger]
			The logger, by default None.
		cls_db : Optional[Session]
			The database session, by default None.
		list_asset_codes : Optional[list[str]]
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
		self.date_ref = date_ref or self.cls_dates_br.add_working_days(
			self.cls_dates_current.curr_date(), -1
		)
		self.base_url = "https://data.anbima.com.br/certificado-de-recebiveis"
		self.list_asset_codes = list_asset_codes or []

	def run(
		self,
		timeout_ms: int = 30_000,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_anbimadata_cri_cra_pu_historico",
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
				bool_insert_or_ignore=bool_insert_or_ignore,
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
		timeout_ms : int
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
				"warning",
			)
			return list_prices_data

		with sync_playwright() as p:
			browser = p.chromium.launch(headless=False)
			page = browser.new_page()

			self.cls_create_log.log_message(
				self.logger,
				f"🚀 Starting PU Histórico prices scraping for {len(self.list_asset_codes)} "
				"CRI/CRA assets...",
				"info",
			)

			for asset_code in self.list_asset_codes:
				self.cls_create_log.log_message(
					self.logger, f"📊 Fetching PU Histórico prices for: {asset_code}...", "info"
				)

				try:
					url = f"{self.base_url}/{asset_code}/precos?page=1&size=100"
					page.goto(url)
					page.wait_for_timeout(timeout_ms)

					total_pages = self._get_total_pages(page)

					self.cls_create_log.log_message(
						self.logger, f"Found {total_pages} pages for {asset_code}", "info"
					)

					for page_num in range(1, total_pages + 1):
						self.cls_create_log.log_message(
							self.logger,
							f"Fetching page {page_num}/{total_pages} for {asset_code}...",
							"info",
						)

						current_url = (
							f"{self.base_url}/{asset_code}/precos?page={page_num}&size=100"
						)

						if page_num > 1:
							page.goto(current_url)
							page.wait_for_timeout(timeout_ms)

						prices_data = self._extract_pu_historico_prices(
							page, asset_code, current_url
						)
						list_prices_data.extend(prices_data)

						self.cls_create_log.log_message(
							self.logger,
							f"✅ Extracted {len(prices_data)} price records from page {page_num}",
							"info",
						)

						if page_num < total_pages:
							time.sleep(randint(1, 3))  # noqa S311

					self.cls_create_log.log_message(
						self.logger,
						f"✅ Successfully extracted all PU Histórico prices for {asset_code}",
						"info",
					)

				except Exception as e:
					self.cls_create_log.log_message(
						self.logger, f"❌ Error processing {asset_code}: {str(e)}", "error"
					)

				time.sleep(randint(2, 8))  # noqa S311

			browser.close()

		self.cls_create_log.log_message(
			self.logger,
			f"💾 PU Histórico prices scraping finished. "
			f"Total: {len(list_prices_data)} records processed.",
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

	def _extract_pu_historico_prices(
		self, page: PlaywrightPage, asset_code: str, url: str
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

		price_records = self._extract_price_rows(page, cod_ativo, is_cri_cra, url)

		return price_records

	def _extract_cod_ativo(self, page: PlaywrightPage, fallback_code: str) -> str:
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
			codigo_element = page.locator('xpath=//*[@id="root"]/main/div[1]/div/div/h1').first
			if codigo_element.is_visible(timeout=5000):
				full_text = codigo_element.inner_text().strip()
				codigo_text = full_text.split("\n")[0].strip() if "\n" in full_text else full_text
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
		self, page: PlaywrightPage, cod_ativo: str, is_cri_cra: Optional[str], url: str
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
				self.logger, f"No PU Histórico price data found for {cod_ativo}", "warning"
			)
			return price_records

		self.cls_create_log.log_message(
			self.logger,
			f"Found {len(data_ref_elements)} PU Histórico price records for {cod_ativo}",
			"info",
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
					f"Error extracting price row {idx} for {cod_ativo}: {e}",
					"warning",
				)

		return price_records

	def _extract_single_price_row(
		self,
		page: PlaywrightPage,
		row_idx: int,
		cod_ativo: str,
		is_cri_cra: Optional[str],
		url: str,
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
				if x and isinstance(x, str)
				else "01/01/2100"
			)

		return df_
