"""Implementation of AnbimaDataFundsHistoric ingestion instance."""

from datetime import date
from io import StringIO
from logging import Logger
from random import randint
import time
from typing import Any, Optional, Union

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


class AnbimaDataFundsHistoric(ABCIngestionOperations):
	"""Anbima Funds Historic ingestion class."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
		list_fund_codes: Optional[list[str]] = None,
	) -> None:
		"""Initialize the Anbima Funds Historic ingestion class.

		Parameters
		----------
		date_ref : Optional[date]
			The date of reference, by default None.
		logger : Optional[Logger]
			The logger, by default None.
		cls_db : Optional[Session]
			The database session, by default None.
		list_fund_codes : Optional[list[str]]
			List of fund codes to scrape historic information for, by default None.

		Returns
		-------
		None

		Notes
		-----
		[1] Metadata: https://data.anbima.com.br/fundos/{fund_code}/historico
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
		self.base_url = "https://data.anbima.com.br/fundos"
		self.list_fund_codes = list_fund_codes or []

	def run(
		self,
		timeout_ms: int = 30_000,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_anbimadata_funds_historic",
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
			The name of the table, by default "br_anbimadata_funds_historic"

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		raw_data = self.get_response(timeout_ms=timeout_ms)
		df_ = self.transform_data(raw_data=raw_data)

		self.cls_create_log.log_message(
			self.logger, f"📊 Initial DataFrame shape - Historic: {df_.shape}", "info"
		)

		self.cls_create_log.log_message(
			self.logger, "🔄 Starting standardization of historic dataframe...", "info"
		)

		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"FUND_CODE": str,
				"DATA_HORA_ATUALIZACAO": str,
				"DATA_COMPETENCIA": "date",
				"PL": str,
				"VALOR_COTA": str,
				"VOLUME_TOTAL_APLICACOES": str,
				"VOLUME_TOTAL_RESGATES": str,
				"NUMERO_COTISTAS": str,
			},
			str_fmt_dt="DD/MM/YYYY",
			url=self.base_url,
		)

		self.cls_create_log.log_message(
			self.logger,
			f"✅ Historic dataframe standardized - Shape: {df_.shape}, "
			f"Columns: {len(df_.columns)}, "
			f"Rows: {len(df_)}",
			"info",
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
	) -> list[dict[str, Any]]:
		"""Scrape funds historic information using Playwright.

		Parameters
		----------
		timeout_ms : int
			The timeout in milliseconds, by default 30_000

		Returns
		-------
		list[dict[str, Any]]
			List of dictionaries containing historic data for all funds.
		"""
		list_historic_data: list[dict[str, Any]] = []

		if not self.list_fund_codes:
			self.cls_create_log.log_message(
				self.logger,
				"⚠️ No fund codes provided. Cannot scrape historic information.",
				"warning",
			)
			return list_historic_data

		with sync_playwright() as p:
			browser = p.chromium.launch(headless=False)
			page = browser.new_page()

			self.cls_create_log.log_message(
				self.logger,
				f"🚀 Starting historic scraping for {len(self.list_fund_codes)} funds...",
				"info",
			)

			for fund_code in self.list_fund_codes:
				self.cls_create_log.log_message(
					self.logger, f"📊 Fetching historic information for: {fund_code}...", "info"
				)

				try:
					url = f"{self.base_url}/{fund_code}/dados-periodicos"
					page.goto(url)
					page.wait_for_timeout(timeout_ms)

					self.cls_create_log.log_message(
						self.logger, f"🔍 Extracting historic data for {fund_code}...", "info"
					)

					historic_data = self._extract_historic_data(page, fund_code)
					list_historic_data.extend(historic_data)

					self.cls_create_log.log_message(
						self.logger,
						f"✅ Historic data extracted for {fund_code} - "
						f"{len(historic_data)} records found",
						"info",
					)

				except Exception as e:
					self.cls_create_log.log_message(
						self.logger, f"❌ Error processing {fund_code}: {str(e)}", "error"
					)

				time.sleep(randint(3, 8))  # noqa S311

			browser.close()

		self.cls_create_log.log_message(
			self.logger,
			f"💾 Historic scraping finished. Total: {len(list_historic_data)} records found.",
			"info",
		)

		return list_historic_data

	def _extract_historic_data(self, page: PlaywrightPage, fund_code: str) -> list[dict[str, Any]]:
		"""Extract historic data from the table.

		Parameters
		----------
		page : PlaywrightPage
			The Playwright page object.
		fund_code : str
			The fund code.

		Returns
		-------
		list[dict[str, Any]]
			List of dictionaries containing historic data records.
		"""
		list_records = []

		try:
			data_hora_atualizacao = self._extract_update_timestamp(page)

			rows = page.locator(
				'xpath=//*[@id="detalhes-do-fundo"]/div[3]/div/div/div/div/section/div/table/tbody/tr'  # noqa E501: line too long
			).all()

			self.cls_create_log.log_message(
				self.logger, f"📋 Found {len(rows)} historic records for fund {fund_code}", "info"
			)

			for idx, row in enumerate(rows):
				try:
					record = self._extract_row_data(row, fund_code, data_hora_atualizacao, idx)
					list_records.append(record)
				except Exception as e:
					self.cls_create_log.log_message(
						self.logger,
						f"⚠️ Error extracting row {idx} for fund {fund_code}: {e}",
						"warning",
					)

		except Exception as e:
			self.cls_create_log.log_message(
				self.logger,
				f"❌ Error extracting historic data for fund {fund_code}: {e}",
				"error",
			)

		return list_records

	def _extract_update_timestamp(self, page: PlaywrightPage) -> Optional[str]:
		"""Extract the update timestamp from the page.

		Parameters
		----------
		page : PlaywrightPage
			The Playwright page object.

		Returns
		-------
		Optional[str]
			The update timestamp or None.
		"""
		try:
			element = page.locator(
				'xpath=//*[@id="detalhes-do-fundo"]/div[3]/div/div/div/div/div[1]/p'
			).first

			if element.count() > 0 and element.is_visible(timeout=5_000):
				text = element.inner_text().strip()
				parts = text.split("\n")
				if len(parts) >= 2:
					return parts[1].strip() if parts[1].strip() else None
				return text if text else None
		except Exception as e:
			self.cls_create_log.log_message(
				self.logger, f"⚠️ Error extracting update timestamp: {e}", "warning"
			)

		return None

	def _extract_row_data(
		self, row: Locator, fund_code: str, data_hora_atualizacao: Optional[str], idx: int
	) -> dict[str, Any]:
		"""Extract data from a single table row.

		Parameters
		----------
		row : Locator
			The table row element.
		fund_code : str
			The fund code.
		data_hora_atualizacao : Optional[str]
			The update timestamp.
		idx : int
			The row index.

		Returns
		-------
		dict[str, Any]
			Dictionary containing extracted row data.
		"""
		data = {"FUND_CODE": fund_code, "DATA_HORA_ATUALIZACAO": data_hora_atualizacao}

		columns = {
			"DATA_COMPETENCIA": 1,
			"PL": 2,
			"VALOR_COTA": 3,
			"VOLUME_TOTAL_APLICACOES": 4,
			"VOLUME_TOTAL_RESGATES": 5,
			"NUMERO_COTISTAS": 6,
		}

		for field_name, col_index in columns.items():
			try:
				cell = row.locator(f"xpath=./td[{col_index}]").first
				if cell.count() > 0:
					text = cell.inner_text().strip()
					data[field_name] = text if text else None
				else:
					data[field_name] = None
			except Exception as e:
				self.cls_create_log.log_message(
					self.logger,
					f"⚠️ Error extracting {field_name} from row {idx}: {e}",
					"warning",
				)
				data[field_name] = None

		return data

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

	def transform_data(self, raw_data: list[dict[str, Any]]) -> pd.DataFrame:
		"""Transform scraped historic data into a DataFrame.

		Parameters
		----------
		raw_data : list[dict[str, Any]]
			The scraped historic data list.

		Returns
		-------
		pd.DataFrame
			The transformed DataFrame.
		"""
		if not raw_data:
			return pd.DataFrame()

		df_ = pd.DataFrame(raw_data)

		date_columns = ["DATA_COMPETENCIA"]
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
		if not date_str or date_str == "-":
			return "01/01/2100"
		return date_str
