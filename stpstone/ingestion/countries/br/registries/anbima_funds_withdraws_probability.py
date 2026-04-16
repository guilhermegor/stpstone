"""Implementation of Anbima Redemption Probability Matrix ingestion instance.

This module provides web scraping functionality for extracting redemption probability matrix data
from ANBIMA's Power BI dashboard using Playwright.
"""

from contextlib import suppress
from datetime import date
import io
from io import StringIO
from logging import Logger
from pathlib import Path
import tempfile
import time
from typing import Optional, Union

import pandas as pd
from playwright.sync_api import Frame, Page as PlaywrightPage, sync_playwright
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


class AnbimaFundsRedemptionProbabilityMatrix(ABCIngestionOperations):
	"""ANBIMA Redemption Probability Matrix ingestion class."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
		headless: bool = True,
	) -> None:
		"""Initialize the ANBIMA Redemption Probability Matrix ingestion class.

		Parameters
		----------
		date_ref : Optional[date]
			Reference date for data retrieval. If None, defaults to 66 working days before current
			date, by default None
		logger : Optional[Logger]
			Logger instance for logging messages, by default None
		cls_db : Optional[Session]
			Database session/connection instance, by default None
		headless : bool
			Whether to run the browser in headless mode, by default True

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
			self.cls_dates_current.curr_date(), -66
		)
		self.base_url = (
			"https://www.anbima.com.br/pt_br/autorregular/matriz-de-probabilidade-de-resgates.htm"
		)
		self.headless = headless

		self._initialize_target_dates()

	def _initialize_target_dates(self) -> None:
		"""Initialize target month and year from date_ref.

		Returns
		-------
		None
		"""
		target_month = self.date_ref.strftime("%B").capitalize()
		target_year = str(self.date_ref.year)

		month_map = {
			"January": "Janeiro",
			"February": "Fevereiro",
			"March": "Março",
			"April": "Abril",
			"May": "Maio",
			"June": "Junho",
			"July": "Julho",
			"August": "Agosto",
			"September": "Setembro",
			"October": "Outubro",
			"November": "Novembro",
			"December": "Dezembro",
		}

		self.target_month_pt = month_map[target_month]
		self.target_year = target_year

	def run(
		self,
		timeout_ms: int = 180000,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_anbima_redemption_probability_matrix",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		Parameters
		----------
		timeout_ms : int
			Timeout for web requests in milliseconds, by default 180000
		bool_insert_or_ignore : bool
			Whether to insert data into the database or return DataFrame, by default False
		str_table_name : str
			Name of the database table for insertion, by default
			"br_anbima_redemption_probability_matrix"

		Returns
		-------
		Optional[pd.DataFrame]
			DataFrame containing the data if bool_insert_or_ignore is False, None otherwise
		"""
		self.cls_create_log.log_message(
			self.logger,
			f"🚀 Starting ANBIMA Redemption Probability Matrix scraping for date: {self.date_ref}",
			"info",
		)

		self.cls_create_log.log_message(
			self.logger,
			f"🎯 Target: {self.date_ref.strftime('%d/%m/%Y')} "
			f"({self.target_month_pt} {self.target_year})",
			"info",
		)

		raw_data = self.get_response(timeout_ms=timeout_ms)

		if raw_data.empty:
			self.cls_create_log.log_message(
				self.logger, "❌ No data retrieved from source", "error"
			)
			return None

		df_ = self.transform_data(raw_data=raw_data)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"data": "date",
				"periodo": str,
				"classe": str,  # codespell:ignore
				"segmento_investidor": str,
				"tipo_metodologia": str,
				"metrica": str,
				"prazo": "int64",
				"valor": "float64",
			},
			str_fmt_dt="YYYY/MM/DD",
			url=self.base_url,
		)

		if self.cls_db:
			self.insert_table_db(
				cls_db=self.cls_db,
				str_table_name=str_table_name,
				df_=df_,
				bool_insert_or_ignore=bool_insert_or_ignore,
			)
			return None
		else:
			return df_

	def get_response(
		self,
		timeout_ms: int = 180000,
	) -> pd.DataFrame:
		"""Scrape ANBIMA redemption probability matrix data using Playwright.

		Parameters
		----------
		timeout_ms : int
			Timeout for web requests in milliseconds, by default 180000

		Returns
		-------
		pd.DataFrame
			DataFrame containing the data
		"""
		with sync_playwright() as p:
			browser = p.chromium.launch(headless=self.headless)

			context = browser.new_context(
				viewport={"width": 1920, "height": 1080},
				user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
				accept_downloads=True,
			)

			page = context.new_page()

			try:
				iframe = self._initialize_browser(page, timeout_ms)
				if not iframe:
					return pd.DataFrame()

				if not self._select_month(iframe):
					self.cls_create_log.log_message(
						self.logger, "⚠ Continuing with current month selection", "warning"
					)

				if not self._select_year(iframe):
					self.cls_create_log.log_message(
						self.logger, "⚠ Continuing with current year selection", "warning"
					)

				self.cls_create_log.log_message(
					self.logger, "⏳ Waiting for Power BI to refresh data...", "info"
				)
				time.sleep(10)

				csv_data = self._download_data(page, iframe, timeout_ms)
				if not csv_data:
					return pd.DataFrame()

				df_ = self._process_csv_data(csv_data)

				return df_

			except Exception as e:
				self.cls_create_log.log_message(
					self.logger, f"❌ Error during scraping process: {e}", "error"
				)
				return pd.DataFrame()
			finally:
				browser.close()

	def _initialize_browser(self, page: PlaywrightPage, timeout_ms: int) -> Optional[Frame]:
		"""Initialize browser and load the target page.

		Parameters
		----------
		page : PlaywrightPage
			Playwright page object
		timeout_ms : int
			Timeout for web requests in milliseconds

		Returns
		-------
		Optional[Frame]
			Power BI iframe context or None if initialization fails
		"""
		self.cls_create_log.log_message(self.logger, "📡 Navigating to ANBIMA...", "info")

		try:
			page.goto(self.base_url, wait_until="load", timeout=60000)
			time.sleep(10)

			iframe_element = page.query_selector('iframe[src*="powerbi.com"]')
			if not iframe_element:
				self.cls_create_log.log_message(
					self.logger, "✗ Power BI iframe not found", "error"
				)
				return None

			iframe = iframe_element.content_frame()
			self.cls_create_log.log_message(self.logger, "✓ Power BI iframe loaded", "info")
			time.sleep(12)

			return iframe

		except Exception as e:
			self.cls_create_log.log_message(
				self.logger, f"✗ Browser initialization failed: {e}", "error"
			)
			return None

	def _select_month(self, iframe: Frame) -> bool:
		"""Select target month in the Power BI interface.

		Parameters
		----------
		iframe : Frame
			Power BI iframe context

		Returns
		-------
		bool
			True if month selection was successful, False otherwise
		"""
		self.cls_create_log.log_message(
			self.logger, f"📅 Selecting month: {self.target_month_pt}", "info"
		)

		try:
			month_dropdown = iframe.wait_for_selector(
				'div.slicer-dropdown-menu[aria-label="ds_mes"]', timeout=15000
			)

			if not month_dropdown:
				self.cls_create_log.log_message(self.logger, "✗ Month dropdown not found", "error")
				return False

			current_month = (
				month_dropdown.query_selector(".slicer-restatement").inner_text().strip()
			)
			self.cls_create_log.log_message(self.logger, f"Current month: {current_month}", "info")

			if current_month == self.target_month_pt:
				self.cls_create_log.log_message(
					self.logger, f"✓ Already on target month: {self.target_month_pt}", "info"
				)
				return True

			self.cls_create_log.log_message(
				self.logger,
				f"Need to change from {current_month} to {self.target_month_pt}",
				"info",
			)

			month_dropdown.click()
			time.sleep(2)

			month_found = self._find_and_click_option(
				iframe,
				self.target_month_pt,
				[
					"Janeiro",
					"Fevereiro",
					"Março",
					"Abril",
					"Maio",
					"Junho",
					"Julho",
					"Agosto",
					"Setembro",
					"Outubro",
					"Novembro",
					"Dezembro",
				],
				"month",
			)

			if month_found:
				new_month = (
					month_dropdown.query_selector(".slicer-restatement").inner_text().strip()
				)
				if new_month == self.target_month_pt:
					self.cls_create_log.log_message(
						self.logger, f"✅ Confirmed: Month is now {self.target_month_pt}", "info"
					)
					return True
				else:
					self.cls_create_log.log_message(
						self.logger,
						f"⚠ Month shows as {new_month} instead of {self.target_month_pt}",
						"warning",
					)
					return False

			return False

		except Exception as e:
			self.cls_create_log.log_message(self.logger, f"⚠ Month selection error: {e}", "error")
			return False

	def _select_year(self, iframe: Frame) -> bool:
		"""Select target year in the Power BI interface.

		Parameters
		----------
		iframe : Frame
			Power BI iframe context

		Returns
		-------
		bool
			True if year selection was successful, False otherwise
		"""
		self.cls_create_log.log_message(
			self.logger, f"📆 Selecting year: {self.target_year}", "info"
		)

		try:
			year_dropdown = iframe.wait_for_selector(
				'div.slicer-dropdown-menu[aria-label="nu_ano"]', timeout=15000
			)

			if not year_dropdown:
				self.cls_create_log.log_message(self.logger, "✗ Year dropdown not found", "error")
				return False

			current_year = year_dropdown.query_selector(".slicer-restatement").inner_text().strip()
			self.cls_create_log.log_message(self.logger, f"Current year: {current_year}", "info")

			if current_year == self.target_year:
				self.cls_create_log.log_message(
					self.logger, f"✓ Already on target year: {self.target_year}", "info"
				)
				return True

			self.cls_create_log.log_message(
				self.logger, f"Need to change from {current_year} to {self.target_year}", "info"
			)

			year_dropdown.click()
			time.sleep(2)

			year_found = self._find_and_click_option(
				iframe, self.target_year, [str(year) for year in range(2000, 2031)], "year"
			)

			if year_found:
				new_year = year_dropdown.query_selector(".slicer-restatement").inner_text().strip()
				if new_year == self.target_year:
					self.cls_create_log.log_message(
						self.logger, f"✅ Confirmed: Year is now {self.target_year}", "info"
					)
					return True
				else:
					self.cls_create_log.log_message(
						self.logger,
						f"⚠ Year shows as {new_year} instead of {self.target_year}",
						"warning",
					)
					return False

			return False

		except Exception as e:
			self.cls_create_log.log_message(self.logger, f"⚠ Year selection error: {e}", "error")
			return False

	def _find_and_click_option(
		self, iframe: Frame, target_value: str, expected_values: list, option_type: str
	) -> bool:
		"""Find and click an option in a dropdown.

		Parameters
		----------
		iframe : Frame
			The iframe context
		target_value : str
			The value to find and click
		expected_values : list
			List of expected values for validation
		option_type : str
			Type of option for logging (e.g., "month", "year")

		Returns
		-------
		bool
			True if option was found and clicked, False otherwise
		"""
		try:
			time.sleep(2)

			all_elements = iframe.query_selector_all("*")
			options_found = []

			for elem in all_elements:
				with suppress(Exception):
					if elem.is_visible():
						text = elem.inner_text().strip()
						if text in expected_values:
							options_found.append((text, elem))

			self.cls_create_log.log_message(
				self.logger, f"Found {len(options_found)} {option_type} elements", "info"
			)

			# method 1: look for our target value  # noqa: ERA001
			for option_text, elem in options_found:
				if option_text == target_value:
					self.cls_create_log.log_message(
						self.logger, f"Found target {option_type}: {option_text}", "info"
					)

					# try multiple click methods
					try:
						elem.click()
						time.sleep(2)
						self.cls_create_log.log_message(
							self.logger, "✓ Regular click successful", "info"
						)
						return True
					except Exception:
						try:
							elem.click(force=True)
							time.sleep(2)
							self.cls_create_log.log_message(
								self.logger, "✓ Force click successful", "info"
							)
							return True
						except Exception as click_error:
							self.cls_create_log.log_message(
								self.logger, f"✗ Click failed: {click_error}", "warning"
							)

			# method 2: If still not found, use coordinate clicking  # noqa: ERA001
			self.cls_create_log.log_message(
				self.logger, f"Trying coordinate-based selection for {option_type}...", "info"
			)
			for option_text, elem in options_found:
				if option_text == target_value:
					try:
						box = elem.bounding_box()
						if box:
							iframe.click(
								"body",
								position={
									"x": box["x"] + box["width"] / 2,
									"y": box["y"] + box["height"] / 2,
								},
							)
							time.sleep(2)
							self.cls_create_log.log_message(
								self.logger, "✓ Coordinate click successful", "info"
							)
							return True
					except Exception as coord_error:
						self.cls_create_log.log_message(
							self.logger, f"✗ Coordinate click failed: {coord_error}", "warning"
						)

			self.cls_create_log.log_message(
				self.logger,
				f"✗ Target {option_type} '{target_value}' not found or could not be clicked",
				"error",
			)
			return False

		except Exception as e:
			self.cls_create_log.log_message(
				self.logger, f"✗ Error finding and clicking {option_type} option: {e}", "error"
			)
			return False

	def _download_data(
		self, page: PlaywrightPage, iframe: Frame, timeout_ms: int
	) -> Optional[bytes]:
		"""Download the data file.

		Parameters
		----------
		page : PlaywrightPage
			Playwright page object
		iframe : Frame
			Power BI iframe context
		timeout_ms : int
			Timeout for web requests in milliseconds

		Returns
		-------
		Optional[bytes]
			CSV data as bytes or None if download fails
		"""
		self.cls_create_log.log_message(self.logger, "💾 Downloading data...", "info")

		download_button = self._find_download_button(iframe)
		if not download_button:
			return None

		self.cls_create_log.log_message(self.logger, "Initiating download...", "info")

		try:
			with page.expect_download(timeout=timeout_ms) as download_info:
				download_button.click(force=True)
				download = download_info.value

			filename = download.suggested_filename
			self.cls_create_log.log_message(self.logger, f"✓ Download started: {filename}", "info")

			download_dir = tempfile.mkdtemp()
			download_path = Path(download_dir) / filename
			download.save_as(download_path)

			with open(download_path, "rb") as f:
				csv_data = f.read()

			self.cls_create_log.log_message(
				self.logger, f"✓ Download completed: {len(csv_data):,} bytes", "info"
			)

			return csv_data

		except Exception as e:
			self.cls_create_log.log_message(self.logger, f"✗ Download failed: {e}", "error")
			return None

	def _find_download_button(self, iframe: Frame) -> Optional[Frame]:
		"""Find the download button in the Power BI interface.

		Parameters
		----------
		iframe : Frame
			Power BI iframe context

		Returns
		-------
		Optional[Frame]
			Download button element or None if not found
		"""
		# method 1: direct text search  # noqa: ERA001
		with suppress(Exception):
			download_button = iframe.query_selector('text="Download base consolidada"')
			if download_button and download_button.is_visible():
				self.cls_create_log.log_message(
					self.logger, "✓ Found download button by text", "info"
				)
				return download_button

		# method 2: search by partial text  # noqa: ERA001
		with suppress(Exception):
			all_elements = iframe.query_selector_all("*")
			for elem in all_elements:
				with suppress(Exception):
					if elem.is_visible():
						text = elem.inner_text().strip()
						if "Download" in text and "consolidada" in text.lower():
							self.cls_create_log.log_message(
								self.logger, "✓ Found download button by partial text", "info"
							)
							return elem

		self.cls_create_log.log_message(self.logger, "✗ Download button not found!", "error")
		return None

	def _process_csv_data(self, csv_data: bytes) -> pd.DataFrame:
		"""Process the downloaded CSV data.

		Parameters
		----------
		csv_data : bytes
			Raw CSV data bytes

		Returns
		-------
		pd.DataFrame
			Processed DataFrame
		"""
		self.cls_create_log.log_message(self.logger, "📊 Processing data...", "info")

		try:
			encodings = ["utf-8", "latin-1", "iso-8859-1", "cp1252"]
			csv_text = None

			for encoding in encodings:
				with suppress(Exception):
					csv_text = csv_data.decode(encoding)
					self.cls_create_log.log_message(
						self.logger, f"✓ Successfully decoded with {encoding}", "info"
					)
					break

			if csv_text is None:
				self.cls_create_log.log_message(
					self.logger, "✗ Could not decode CSV with any encoding", "error"
				)
				return pd.DataFrame()

			try:
				lines = csv_text.strip().split("\n")
				if len(lines) > 0 and len(lines[0].split(";")) == 1:
					self.cls_create_log.log_message(
						self.logger,
						"⚠ CSV appears to have all data in one column, fixing...",
						"warning",
					)
					df_ = pd.DataFrame([line.split(",") for line in lines])
					df_.columns = df_.iloc[0]
					df_ = df_[1:]
					self.cls_create_log.log_message(
						self.logger, "✓ Fixed single-column CSV", "info"
					)
				else:
					df_ = pd.read_csv(io.StringIO(csv_text), sep=";", decimal=",", thousands=".")
					self.cls_create_log.log_message(
						self.logger, "✓ Parsed with semicolon delimiter", "info"
					)
			except Exception as e:
				self.cls_create_log.log_message(
					self.logger, f"⚠ First parse attempt failed: {e}", "warning"
				)

				try:
					df_ = pd.read_csv(io.StringIO(csv_text), sep=",")
					self.cls_create_log.log_message(
						self.logger, "✓ Parsed with comma delimiter", "info"
					)
				except Exception as e2:
					self.cls_create_log.log_message(
						self.logger, f"✗ CSV parsing failed: {e2}", "error"
					)
					return pd.DataFrame()

			self.cls_create_log.log_message(
				self.logger, f"✓ Parsed {len(df_)} rows, {len(df_.columns)} columns", "info"
			)
			self.cls_create_log.log_message(self.logger, f"✓ Columns: {list(df_.columns)}", "info")

			date_cols = [col for col in df_.columns if "data" in col.lower()]
			if date_cols and len(df_) > 0:
				first_date = df_[date_cols[0]].iloc[0]
				self.cls_create_log.log_message(
					self.logger, f"📅 First date in data: {first_date}", "info"
				)

			self.cls_create_log.log_message(
				self.logger, f"✅ SUCCESS! Final shape: {df_.shape}", "info"
			)

			return df_

		except Exception as e:
			self.cls_create_log.log_message(self.logger, f"✗ Data processing error: {e}", "error")
			return pd.DataFrame()

	def parse_raw_file(
		self, resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
	) -> StringIO:
		"""Parse the raw file content.

		This method is kept for compatibility but not used in web scraping.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response or page object from the request.

		Returns
		-------
		StringIO
			The parsed content.
		"""
		return StringIO()

	def transform_data(self, raw_data: pd.DataFrame) -> pd.DataFrame:
		"""Transform scraped data into a standardized DataFrame.

		Parameters
		----------
		raw_data : pd.DataFrame
			Raw DataFrame obtained from scraping

		Returns
		-------
		pd.DataFrame
			Transformed DataFrame
		"""
		if raw_data.empty:
			self.cls_create_log.log_message(self.logger, "⚠ No data to transform", "warning")
			return pd.DataFrame()

		df_ = raw_data.copy()

		if "valor" in df_.columns and pd.api.types.is_string_dtype(df_["valor"]):
			try:
				# handle brazilian number format: 1.000,00 -> 1000.00  # noqa: ERA001
				df_["valor"] = (
					df_["valor"]
					.astype(str)
					.str.replace(".", "", regex=False)
					.str.replace(",", ".", regex=False)
					.astype(float)
				)
				self.cls_create_log.log_message(
					self.logger, "✓ Processed numeric column: valor", "info"
				)
			except Exception as e:
				self.cls_create_log.log_message(
					self.logger, f"⚠ Could not process numeric column: valor - {e}", "warning"
				)

		if "prazo" in df_.columns and pd.api.types.is_string_dtype(df_["prazo"]):
			try:
				df_["prazo"] = pd.to_numeric(df_["prazo"], errors="coerce")
				self.cls_create_log.log_message(
					self.logger, "✓ Processed numeric column: prazo", "info"
				)
			except Exception as e:
				self.cls_create_log.log_message(
					self.logger, f"⚠ Could not process numeric column: prazo - {e}", "warning"
				)

		self.cls_create_log.log_message(
			self.logger, f"✅ Data transformation complete. Final shape: {df_.shape}", "info"
		)

		return df_
