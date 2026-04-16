"""Implementation of Yahii rent indices ingestion."""

from datetime import date
from logging import Logger
from typing import Optional, Union

import backoff
import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import requests
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
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


class YahiiRentIndices(ABCIngestionOperations):
	"""Ingestion concrete class for Yahii rent indices."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the ingestion class.

		Parameters
		----------
		date_ref : Optional[date]
			The date of reference, by default None.
		logger : Optional[Logger]
			The logger, by default None.
		cls_db : Optional[Session]
			The database session, by default None.

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
			self.cls_dates_current.curr_date(), -300
		)

		# Calculate year from reference date
		self.year = self.date_ref.year
		self.url = f"https://www.yahii.com.br/alugueis{str(self.year)[-2:]}.html"

		# Month mapping
		self.months = [
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
		]

		# Index mapping
		self.indices = [
			"INPC/IBGE",
			"IPC/FIPE",
			"IGP(DI)/FGV",
			"IGPM/FGV",
			"IPCA/IBGE",
			"IPC/FGV",
			"IPCR",
		]

		# Time period mapping
		self.time_periods = {
			"Men.": "monthly",
			"Bim.": "bimonthly",
			"Trim.": "quarterly",
			"Quadrim.": "four_monthly",
			"Sem.": "semiannual",
			"Anual": "annual",  # codespell:ignore
		}

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = 30_000,
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_yahii_rents",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default 30_000
		bool_verify : bool
			Whether to verify the SSL certificate, by default True
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False
		str_table_name : str
			The name of the table, by default "br_yahii_rents"

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		scraper = self.get_response(timeout=timeout, bool_verify=bool_verify)
		page_content = self.parse_raw_file(scraper, timeout=timeout)
		df_ = self.transform_data(page_content)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"YEAR": int,
				"MONTH": str,
				"TIME_PERIOD": str,
				"POOL": str,
				"DATA": float,
			},
			str_fmt_dt="YYYY-MM-DD",
			url=self.url,
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

	@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=120)
	def get_response(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = 30_000,
		bool_verify: bool = True,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver, PlaywrightScraper]:
		"""Return a scraper object.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default 30_000
		bool_verify : bool
			Verify the SSL certificate, by default True

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver, PlaywrightScraper]
			A scraper object.
		"""
		return PlaywrightScraper(
			bool_headless=True, int_default_timeout=timeout, logger=self.logger
		)

	def parse_raw_file(
		self,
		scraper: Union[Response, PlaywrightPage, SeleniumWebDriver, PlaywrightScraper],
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = 30_000,
	) -> PlaywrightPage:
		"""Parse the raw file content.

		Parameters
		----------
		scraper : Union[Response, PlaywrightPage, SeleniumWebDriver, PlaywrightScraper]
			The scraper object.
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default 30_000

		Returns
		-------
		PlaywrightPage
			The page content.
		"""
		page = scraper.navigate_to_url(url=self.url, timeout=timeout)
		return page

	def transform_data(self, page: PlaywrightPage) -> pd.DataFrame:
		"""Transform page content into a DataFrame.

		Parameters
		----------
		page : PlaywrightPage
			The page object.

		Returns
		-------
		pd.DataFrame
			The transformed DataFrame.
		"""
		list_records = []

		# Iterate through each month
		for _month_idx, month_name in enumerate(self.months, start=1):
			# Try to find tables for this month
			locator = f'p:has-text("{month_name.upper()}/{self.year}") ~ center table'
			tables = page.locator(locator).all()

			if not tables:
				continue

			# Get the data table (should be the first table after the month header)  # noqa: ERA001
			for table in tables:
				rows = table.locator("tr").all()

				if len(rows) < 2:
					continue

				# Get header row to identify columns
				header_cells = rows[0].locator("td").all()
				headers = [cell.inner_text().strip() for cell in header_cells]

				# Skip if not a data table
				if not headers or headers[0] == "":
					continue

				# Process data rows
				for row in rows[1:]:
					cells = row.locator("td").all()
					if len(cells) < 2:
						continue

					pool_name = cells[0].inner_text().strip()

					# Skip if not an index name
					if pool_name not in self.indices:
						continue

					# Extract data for each time period
					for col_idx, (_header_key, time_period) in enumerate(
						self.time_periods.items(), start=1
					):
						if col_idx < len(cells):
							value_text = cells[col_idx].inner_text().strip()

							# Clean and convert value
							if value_text and value_text != "-":
								# Remove parentheses and (-)  # noqa: ERA001
								value_text = (
									value_text.replace("(-)", "")
									.replace("(", "")
									.replace(")", "")
									.strip()
								)

								try:
									value = float(value_text.replace(",", "."))

									list_records.append(
										{
											"YEAR": self.year,
											"MONTH": month_name,
											"TIME_PERIOD": time_period,
											"POOL": pool_name,
											"DATA": value,
										}
									)
								except ValueError:
									if self.logger:
										self.logger.warning(
											f"Could not convert value '{value_text}' for "
											f"{pool_name} - {month_name} - {time_period}"
										)

		return pd.DataFrame(list_records)
