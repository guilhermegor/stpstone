"""Implementation of Anbima CRI/CRA documents ingestion instance."""

from contextlib import suppress
from datetime import date
from io import StringIO
from logging import Logger
from random import randint
import time
from typing import Optional, TypedDict, Union

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

	COD_ATIVO: str
	IS_CRI_CRA: str
	SECAO: str
	SUBSECAO: str
	NOME_DOCUMENTO: str
	DATA_DIVULGACAO_DOCUMENTO: str
	LINK_DOCUMENTO: str


class AnbimaDataCRICRADocuments(ABCIngestionOperations):
	"""Anbima CRI/CRA documents ingestion class."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
		list_asset_codes: Optional[list[str]] = None,
	) -> None:
		"""Initialize the Anbima CRI/CRA documents ingestion class.

		Parameters
		----------
		date_ref : Optional[date]
			The date of reference, by default None.
		logger : Optional[Logger]
			The logger, by default None.
		cls_db : Optional[Session]
			The database session, by default None.
		list_asset_codes : Optional[list[str]]
			List of CRI/CRA asset codes to scrape documents for, by default None.

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
		self.base_url = "https://data.anbima.com.br/certificado-de-recebiveis"
		self.list_asset_codes = list_asset_codes or []

	def run(
		self,
		timeout_ms: int = 30_000,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_anbimadata_cri_cra_documents",
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
			The name of the table, by default "br_anbimadata_cri_cra_documents"

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
				"SECAO": str,
				"SUBSECAO": str,
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
				bool_insert_or_ignore=bool_insert_or_ignore,
			)
		else:
			return df_

	def get_response(
		self,
		timeout_ms: int = 30_000,
	) -> list:
		"""Scrape CRI/CRA documents using Playwright.

		Parameters
		----------
		timeout_ms : int
			The timeout in milliseconds, by default 30_000

		Returns
		-------
		list
			List of scraped CRI/CRA documents data.
		"""
		list_documents_data: list[dict[str, Union[str, int, float, date]]] = []

		if not self.list_asset_codes:
			self.cls_create_log.log_message(
				self.logger, "⚠️ No asset codes provided. Cannot scrape documents.", "warning"
			)
			return list_documents_data

		with sync_playwright() as p:
			browser = p.chromium.launch(headless=False)
			page = browser.new_page()

			self.cls_create_log.log_message(
				self.logger,
				f"🚀 Starting documents scraping for {len(self.list_asset_codes)} "
				"CRI/CRA assets...",
				"info",
			)

			for asset_code in self.list_asset_codes:
				self.cls_create_log.log_message(
					self.logger, f"📊 Fetching documents for: {asset_code}...", "info"
				)

				try:
					url = f"{self.base_url}/{asset_code}/documentos"
					page.goto(url)
					page.wait_for_timeout(timeout_ms)

					documents_data = self._extract_documents_data(page, asset_code)
					list_documents_data.extend(documents_data)

					self.cls_create_log.log_message(
						self.logger,
						f"✅ Successfully extracted {len(documents_data)} documents "
						f"for {asset_code}",
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
			f"Documents scraping finished. Total: {len(list_documents_data)} records processed.",
			"info",
		)

		return list_documents_data

	def _extract_documents_data(self, page: PlaywrightPage, asset_code: str) -> list[dict]:
		"""Extract CRI/CRA documents data using specific XPaths.

		Parameters
		----------
		page : PlaywrightPage
			The Playwright page object.
		asset_code : str
			The asset code.

		Returns
		-------
		list[dict]
			List of dictionaries containing extracted document data.
		"""
		cod_ativo = self._extract_cod_ativo(page, asset_code)
		is_cri_cra = self._extract_is_cri_cra(page)

		check_no_docs = self._check_no_documents(page)
		if check_no_docs:
			self.cls_create_log.log_message(
				self.logger,
				f"No documents found for {asset_code} (page shows no documents message)",
				"warning",
			)
			return []

		document_sections = self._find_document_sections(page, cod_ativo)

		if not document_sections:
			return []

		list_documents: list[dict[str, Union[str, int, float, date]]] = []
		for idx, section in enumerate(document_sections, start=1):
			documents = self._process_document_section(section, idx, cod_ativo, is_cri_cra, page)
			list_documents.extend(documents)

		return list_documents

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

	def _check_no_documents(self, page: PlaywrightPage) -> bool:
		"""Check if page shows 'no documents' message.

		Parameters
		----------
		page : PlaywrightPage
			The Playwright page object.

		Returns
		-------
		bool
			True if no documents message is found, False otherwise.
		"""
		with suppress(Exception):
			no_docs_text = page.locator('text="Não existem documentos"').first
			if no_docs_text.count() > 0 and no_docs_text.is_visible(timeout=2000):
				return True
		return False

	def _find_document_sections(
		self,
		page: PlaywrightPage,
		cod_ativo: str,
	) -> list:
		"""Find all document sections on the page.

		Parameters
		----------
		page : PlaywrightPage
			The Playwright page object.
		cod_ativo : str
			The asset code for logging purposes.

		Returns
		-------
		list
			List of document section elements.
		"""
		xpath_base = '//div[@id="root"]/main/div[3]/div/div[1]/article'
		document_sections = page.locator(f"xpath={xpath_base}").all()

		if len(document_sections) == 0:
			self.cls_create_log.log_message(
				self.logger, f"No document sections found for {cod_ativo}", "warning"
			)
		else:
			self.cls_create_log.log_message(
				self.logger,
				f"Found {len(document_sections)} document sections for {cod_ativo}",
				"info",
			)

		return document_sections

	def _process_document_section(
		self,
		section: Locator,
		section_idx: int,
		cod_ativo: str,
		is_cri_cra: Optional[str],
		page: PlaywrightPage,
	) -> list[dict]:
		"""Process a single document section and extract all documents.

		Parameters
		----------
		section : Locator
			The document section element.
		section_idx : int
			Section index for XPath construction.
		cod_ativo : str
			The asset code.
		is_cri_cra : Optional[str]
			The CRI/CRA type.
		page : PlaywrightPage
			The Playwright page object.

		Returns
		-------
		list[dict]
			List of document records extracted from this section.
		"""
		try:
			secao = self._extract_section_name(page, section_idx)
			subsecao = self._extract_subsection_name(page, section_idx)
			document_items = section.locator("xpath=.//article/section/div[2]/ul/li").all()

			if len(document_items) == 0:
				self.cls_create_log.log_message(
					self.logger,
					f"Section {section_idx} ({secao}): No document items found",
					"warning",
				)
				return []

			self.cls_create_log.log_message(
				self.logger,
				f"Section {section_idx} ({secao}): Found {len(document_items)} document items",
				"info",
			)

			list_documents: list[dict] = []
			for item_idx, item in enumerate(document_items, start=1):
				docs = self._process_document_item(
					item, page, section_idx, item_idx, cod_ativo, is_cri_cra, secao, subsecao
				)
				list_documents.extend(docs)

			return list_documents

		except Exception as e:
			self.cls_create_log.log_message(
				self.logger,
				f"Error extracting section {section_idx} for {cod_ativo}: {e}",
				"warning",
			)
			return []

	def _extract_section_name(self, page: PlaywrightPage, section_idx: int) -> Optional[str]:
		"""Extract section name.

		Parameters
		----------
		page : PlaywrightPage
			The Playwright page object.
		section_idx : int
			Section index for XPath.

		Returns
		-------
		Optional[str]
			The section name or None.
		"""
		xpath_secao = (
			f'//*[@id="root"]/main/div[3]/div/div[1]/article[{section_idx}]'
			f"/article/section/div[1]/div/p[1]"
		)
		return self._extract_text_by_xpath(page, xpath_secao)

	def _extract_subsection_name(self, page: PlaywrightPage, section_idx: int) -> Optional[str]:
		"""Extract subsection name if exists.

		Parameters
		----------
		page : PlaywrightPage
			The Playwright page object.
		section_idx : int
			Section index for XPath.

		Returns
		-------
		Optional[str]
			The subsection name or None.
		"""
		xpath_subsecao = (
			f'//*[@id="root"]/main/div[3]/div/div[1]/article[{section_idx}]'
			f"/article/section/div[1]/div/p[2]"
		)
		return self._extract_text_by_xpath(page, xpath_subsecao)

	def _process_document_item(
		self,
		item: Locator,
		page: PlaywrightPage,
		section_idx: int,
		item_idx: int,
		cod_ativo: str,
		is_cri_cra: Optional[str],
		secao: Optional[str],
		subsecao: Optional[str],
	) -> list[dict]:
		"""Process a single document item (li element) which may contain multiple links.

		Parameters
		----------
		item : Locator
			The document item element.
		page : PlaywrightPage
			The Playwright page object.
		section_idx : int
			Section index for XPath.
		item_idx : int
			Item index within the section.
		cod_ativo : str
			The asset code.
		is_cri_cra : Optional[str]
			The CRI/CRA type.
		secao : Optional[str]
			The section name.
		subsecao : Optional[str]
			The subsection name.

		Returns
		-------
		list[dict]
			List of document records (one per link).
		"""
		try:
			nome_documento = self._extract_document_name(page, section_idx, item_idx)
			link_elements = item.locator("xpath=.//ul/li/a").all()

			if len(link_elements) == 0:
				self.cls_create_log.log_message(
					self.logger,
					f"Section {section_idx}, Item {item_idx} ({nome_documento}): No links found",
					"warning",
				)
				return [
					self._create_document_record(
						cod_ativo, is_cri_cra, secao, subsecao, nome_documento, None, None
					)
				]

			self.cls_create_log.log_message(
				self.logger,
				f"Section {section_idx}, Item {item_idx} ({nome_documento}): "
				f"Found {len(link_elements)} links",
				"info",
			)

			list_documents: list[dict] = []
			for link_idx, link_element in enumerate(link_elements, start=1):
				data_divulgacao = self._extract_document_date(
					page, section_idx, item_idx, link_idx
				)
				link_documento = self._extract_link_url(
					link_element, page, section_idx, item_idx, link_idx
				)

				if link_documento:
					list_documents.append(
						self._create_document_record(
							cod_ativo,
							is_cri_cra,
							secao,
							subsecao,
							nome_documento,
							data_divulgacao,
							link_documento,
						)
					)
					time.sleep(1)

			return list_documents

		except Exception as e:
			self.cls_create_log.log_message(
				self.logger,
				f"Error processing item {item_idx} in section {section_idx}: {e}",
				"warning",
			)
			return []

	def _extract_document_name(
		self, page: PlaywrightPage, section_idx: int, item_idx: int
	) -> Optional[str]:
		"""Extract document name.

		Parameters
		----------
		page : PlaywrightPage
			The Playwright page object.
		section_idx : int
			Section index for XPath.
		item_idx : int
			Item index for XPath.

		Returns
		-------
		Optional[str]
			The document name or None.
		"""
		xpath_nome = (
			f'//*[@id="root"]/main/div[3]/div/div[1]/article[{section_idx}]'
			f"/article/section/div[2]/ul/li[{item_idx}]/div/p"
		)
		return self._extract_text_by_xpath(page, xpath_nome)

	def _extract_document_date(
		self, page: PlaywrightPage, section_idx: int, item_idx: int, link_idx: int
	) -> Optional[str]:
		"""Extract document release date.

		Parameters
		----------
		page : PlaywrightPage
			The Playwright page object.
		section_idx : int
			Section index for XPath.
		item_idx : int
			Item index for XPath.
		link_idx : int
			Link index for XPath.

		Returns
		-------
		Optional[str]
			The document date or None.
		"""
		xpath_data = (
			f'//*[@id="root"]/main/div[3]/div/div[1]/article[{section_idx}]'
			f"/article/section/div[2]/ul/li[{item_idx}]/ul/li[{link_idx}]/span"
		)
		return self._extract_text_by_xpath(page, xpath_data)

	def _extract_link_url(
		self,
		link_element: Locator,
		page: PlaywrightPage,
		section_idx: int,
		item_idx: int,
		link_idx: int,
	) -> Optional[str]:
		"""Extract URL by clicking a link and capturing the new page URL.

		Parameters
		----------
		link_element : Locator
			The link element to click.
		page : PlaywrightPage
			The Playwright page object.
		section_idx : int
			Section index for logging.
		item_idx : int
			Item index for logging.
		link_idx : int
			Link index for logging.

		Returns
		-------
		Optional[str]
			The extracted URL or None.
		"""
		try:
			with page.context.expect_page(timeout=15000) as new_page_info:
				link_element.click(timeout=10000)

			new_page = new_page_info.value
			new_page.wait_for_load_state("domcontentloaded", timeout=10000)
			link_document = new_page.url
			new_page.close()

			self.cls_create_log.log_message(
				self.logger,
				f"  Section {section_idx}, Item {item_idx}, Link {link_idx}: {link_document}",
				"info",
			)

			return link_document

		except Exception as e:
			self.cls_create_log.log_message(
				self.logger,
				f"  Error extracting link in section {section_idx}, "
				f"item {item_idx}, link {link_idx}: {e}",
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
			if element.count() > 0 and element.is_visible(timeout=2000):
				text = element.inner_text().strip()
				return text if text else None
		return None

	def _create_document_record(
		self,
		cod_ativo: str,
		is_cri_cra: Optional[str],
		secao: Optional[str],
		subsecao: Optional[str],
		nome_documento: Optional[str],
		data_divulgacao: Optional[str],
		link_documento: Optional[str],
	) -> ResultDocumentRecord:
		"""Create a document record dictionary.

		Parameters
		----------
		cod_ativo : str
			The asset code.
		is_cri_cra : Optional[str]
			The CRI/CRA type.
		secao : Optional[str]
			The section name.
		subsecao : Optional[str]
			The subsection name.
		nome_documento : Optional[str]
			The document name.
		data_divulgacao : Optional[str]
			The document date.
		link_documento : Optional[str]
			The document link.

		Returns
		-------
		ResultDocumentRecord
			The document record.
		"""
		return {
			"COD_ATIVO": cod_ativo,
			"IS_CRI_CRA": is_cri_cra,
			"SECAO": secao,
			"SUBSECAO": subsecao,
			"NOME_DOCUMENTO": nome_documento,
			"DATA_DIVULGACAO_DOCUMENTO": data_divulgacao,
			"LINK_DOCUMENTO": link_documento,
		}

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
				if x and isinstance(x, str)
				else "01/01/2100"
			)

		return df_
