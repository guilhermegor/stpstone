"""Implementation of AnbimaDataFundsAvailable ingestion instance."""

from contextlib import suppress
from datetime import date
from io import StringIO
from logging import Logger
from random import randint
import re
import time
from typing import Optional, Union

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
		self.date_ref = date_ref or self.cls_dates_br.add_working_days(
			self.cls_dates_current.curr_date(), -1
		)
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
		str_table_name: str = "br_anbimadata_fundos",
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
				bool_insert_or_ignore=bool_insert_or_ignore,
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
				"info",
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
				"info",
			)

			for page_num in range(self.start_page, actual_end_page + 1):
				self.cls_create_log.log_message(
					self.logger, f"📄 Fetching page {page_num}...", "info"
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
							f"Error extracting fund {idx} on page {page_num}: {e}",
							"warning",
						)

				self.cls_create_log.log_message(
					self.logger, f"✅ Page {page_num}: {len(list_page_data)} items", "info"
				)

				list_pages_data.extend(list_page_data)

				if page_num < actual_end_page:
					time.sleep(randint(2, 10))  # noqa S311

			browser.close()

		self.cls_create_log.log_message(
			self.logger,
			f"💾 Scraping finished. Total: {len(list_pages_data)} funds found.",
			"info",
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
			pagination_element = page.locator('xpath=//*[@id="pagination"]/div[3]/span/span').first

			if pagination_element.is_visible(timeout=5_000):
				text = pagination_element.inner_text().strip()
				match = re.search(r"(\d+)", text)
				if match:
					total_pages = int(match.group(1))
					return total_pages
		except Exception as e:
			self.cls_create_log.log_message(
				self.logger, f"Could not determine total pages, defaulting to 1: {e}", "warning"
			)

		return 1

	def _extract_fund_data(self, _page: PlaywrightPage, card_element: Locator, _idx: int) -> dict:
		"""Extract fund data from a fund card element.

		Parameters
		----------
		_page : PlaywrightPage
			The Playwright page object (unused, reserved for future use).
		card_element : Locator
			The fund card element.
		_idx : int
			The index of the fund card (unused, reserved for future use).

		Returns
		-------
		dict
			Dictionary containing extracted fund data.
		"""
		data = {}

		data["NOME_FUNDO"] = self._extract_from_card(card_element, 'xpath=.//*[@id="title"]')

		data["LINK_FUNDO"] = self._extract_link_from_card(card_element)

		tags_data = self._extract_tags(card_element)
		data.update(tags_data)

		data["CNPJ_FUNDO"] = self._extract_from_card(
			card_element, 'xpath=.//p[@class="AccordionFundosCard_id__ddKzx"]'
		)

		cells_data = self._extract_cells(card_element)
		data.update(cells_data)

		return data

	def _extract_from_card(self, card_element: Locator, selector: str) -> Optional[str]:
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
			link_element = card_element.locator("xpath=.//a").first
			if link_element.count() > 0:
				href = link_element.get_attribute("href")
				if href:
					if href.startswith("/"):
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
		data = {"TIPO_FUNDO": None, "PUBLICO_ALVO": None, "STATUS_FUNDO": None}

		try:
			regular_tags = card_element.locator(
				'xpath=.//div[@class="AccordionFundosCard_tags__oIM0g"]/p[@class="_tag_1p5q1_1"]'
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
			self.cls_create_log.log_message(self.logger, f"Error extracting tags: {e}", "warning")

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
			"RENTABILIDADE_12M": None,
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
			self.cls_create_log.log_message(self.logger, f"Error extracting cells: {e}", "warning")

		return data

	def parse_raw_file(self, _: Union[Response, PlaywrightPage, SeleniumWebDriver]) -> StringIO:
		"""Parse the raw file content.

		This method is kept for compatibility but not used in web scraping.

		Parameters
		----------
		_ : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response object (unused; web scraping does not produce a raw file).

		Returns
		-------
		StringIO
			The parsed content.
		"""
		return StringIO()

	def transform_data(self, raw_data: list) -> pd.DataFrame:
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

		if "LINK_FUNDO" in df_.columns:
			df_["COD_ANBIMA"] = df_["LINK_FUNDO"].apply(self._extract_cod_anbima)
		else:
			df_["COD_ANBIMA"] = None

		if "pagina" in df_.columns:
			df_ = df_.drop("pagina", axis=1)

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
			link = link.rstrip("/")
			cod_anbima = link.split("/")[-1]
			return cod_anbima if cod_anbima else None
		except Exception as e:
			self.cls_create_log.log_message(
				self.logger, f"Error extracting COD_ANBIMA from link {link}: {e}", "warning"
			)
			return None
