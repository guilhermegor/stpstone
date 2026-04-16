"""Implementation of Anbima CRI/CRA characteristics ingestion instance."""

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
		self.date_ref = date_ref or self.cls_dates_br.add_working_days(
			self.cls_dates_current.curr_date(), -1
		)
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
		str_table_name: str = "br_anbimadata_cri_cra_characteristics",
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
				bool_insert_or_ignore=bool_insert_or_ignore,
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
		timeout_ms : int
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
					self.logger, "🔍 Detecting total number of pages...", "info"
				)

				url = f"{self.base_url}?size=100&page={self.start_page}"
				page.goto(url)
				page.wait_for_timeout(timeout_ms)

				self.end_page = self._get_total_pages(page)

				self.cls_create_log.log_message(
					self.logger, f"📊 Total pages detected: {self.end_page}", "info"
				)

			self.cls_create_log.log_message(
				self.logger,
				f"🚀 Starting CRI/CRA scraping from pages {self.start_page} to {self.end_page}...",
				"info",
			)

			for page_num in range(self.start_page, self.end_page + 1):
				self.cls_create_log.log_message(
					self.logger, f"📄 Fetching page {page_num}...", "info"
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

							cri_cra_data = self._extract_cri_cra_data(page, id_number)
							cri_cra_data["pagina"] = page_num
							list_page_data.append(cri_cra_data)

				self.cls_create_log.log_message(
					self.logger, f"✅ Page {page_num}: {len(list_page_data)} items", "info"
				)

				list_pages_data.extend(list_page_data)

				if page_num < self.end_page:
					time.sleep(randint(2, 10))  # noqa S311

			browser.close()

		self.cls_create_log.log_message(
			self.logger,
			f"💾 Scraping finished. Total: {len(list_pages_data)} records found.",
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
			The total number of pages, defaults to start_page if not found.
		"""
		try:
			xpath_total_pages = '//*[@id="pagination"]/div[3]/span/span'
			element = page.locator(f"xpath={xpath_total_pages}").first

			if element.count() > 0 and element.is_visible(timeout=5_000):
				total_pages_text = element.inner_text().strip()
				match = re.search(r"(\d+)", total_pages_text)
				if match:
					total_pages = int(match.group(1))
					return total_pages - 1

		except Exception as e:
			self.cls_create_log.log_message(
				self.logger,
				f"Could not determine total pages, using start_page as default: {e}",
				"warning",
			)

		return self.start_page

	def _extract_cri_cra_data(self, page: PlaywrightPage, id_number: str) -> dict:
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
			"DEVEDOR": f'[id="cri-cra-item-devedor-{id_number}"] dd',
			"SERIE_EMISSAO": f'[id="cri-cra-item-serie-emissao-{id_number}"] dd',
			"REMUNERACAO": f'[id="cri-cra-item-Remuneração-{id_number}"] dd',
			"DURATION": f'[id="cri-cra-item-duration-{id_number}"] dd',
			"SECURITIZADORA": f'[id="cri-cra-item-securitizadora-{id_number}"] dd',
			"DATA_EMISSAO": f'[id="cri-cra-item-data-emissão-{id_number}"] dd',
			"DATA_VENCIMENTO": f'[id="cri-cra-item-data-vencimento-{id_number}"] dd',
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
					f"Erro ao extrair {field_name} para ID {id_number}: {e}",
					"warning",
				)
				data[field_name] = None

		pu_link_data = self._extract_pu_and_link(page, id_number)
		data.update(pu_link_data)

		return data

	def _extract_titulo_info(self, page: PlaywrightPage, id_number: str) -> dict:
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
		result = {"CODIGO_EMISSAO": None, "NOME_EMISSOR": None}

		try:
			xpath_codigo = f'//*[@id="item-title-{id_number}"]/span/span[1]'
			element_codigo = page.locator(f"xpath={xpath_codigo}").first

			if element_codigo.count() > 0:
				codigo_texto = element_codigo.inner_text().strip()
				if codigo_texto:
					result["CODIGO_EMISSAO"] = codigo_texto

			xpath_nome = f'//*[@id="item-title-{id_number}"]/span/span[3]'
			element_nome = page.locator(f"xpath={xpath_nome}").first

			if element_nome.count() > 0:
				nome_texto = element_nome.inner_text().strip()
				if nome_texto:
					result["NOME_EMISSOR"] = nome_texto

		except Exception as e:
			self.cls_create_log.log_message(
				self.logger, f"Erro ao extrair título para ID {id_number}: {e}", "warning"
			)

		return result

	def _extract_tipo(self, page: PlaywrightPage, id_number: str) -> dict:
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
		result = {"IS_CRI_CRA": None}

		try:
			xpath_tipo = f'//*[@id="tipo-{id_number}"]/label'
			element = page.locator(f"xpath={xpath_tipo}").first

			if element.count() > 0:
				tipo_texto = element.inner_text().strip()
				if tipo_texto:
					result["IS_CRI_CRA"] = tipo_texto

		except Exception as e:
			self.cls_create_log.log_message(
				self.logger, f"Erro ao extrair tipo para ID {id_number}: {e}", "warning"
			)

		return result

	def _extract_pu_and_link(self, page: PlaywrightPage, id_number: str) -> dict:
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
		result = {"PU_INDICATIVO": None, "LINK_CARACTERISTICAS_INDIVIDUAIS": None}

		try:
			xpath_link = f'//*[@id="cri-cra-item-pu-indicativo-{id_number}"]/dd/span/a'
			element = page.locator(f"xpath={xpath_link}").first

			if element.count() > 0:
				pu_text = element.inner_text().strip()
				result["PU_INDICATIVO"] = pu_text if pu_text else None

				href = element.get_attribute("href")
				if href:
					if href.endswith("/precos"):
						href = href[:-7]

					if href.startswith("/"):
						result["LINK_CARACTERISTICAS_INDIVIDUAIS"] = urljoin(
							"https://data.anbima.com.br", href
						)
					else:
						result["LINK_CARACTERISTICAS_INDIVIDUAIS"] = href

		except Exception as e:
			self.cls_create_log.log_message(
				self.logger, f"Erro ao extrair PU e Link para ID {id_number}: {e}", "warning"
			)

		return result

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

		if "pagina" in df_.columns:
			df_ = df_.drop("pagina", axis=1)

		for col_ in ["DATA_EMISSAO", "DATA_VENCIMENTO"]:
			if col_ in df_.columns:
				df_[col_] = df_[col_].apply(
					lambda x: x.replace("-", "01/01/2100")
					if x and isinstance(x, str)
					else "01/01/2100"
				)

		return df_
