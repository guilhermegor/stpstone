"""B3 BDI OTC Settlement ingestion."""

from datetime import date
from logging import Logger
import time
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
from stpstone.utils.parsers.str import StrHandler


class B3BdiFixedIncomeSettlements(ABCIngestionOperations):
	"""B3 BDI OTC Settlement ingestion class."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
		int_page_size: int = 1_000,
		int_page_min: int = 1,
		int_page_max: Optional[int] = None,
	) -> None:
		"""Initialize the ingestion class.

		Parameters
		----------
		date_ref : Optional[date], optional
			The date of reference, by default None.
		logger : Optional[Logger], optional
			The logger, by default None.
		cls_db : Optional[Session], optional
			The database session, by default None.
		int_page_size : int, optional
			Number of records per page, by default 1_000.
		int_page_min : int, optional
			First page to fetch (1-based), by default 1.
		int_page_max : Optional[int], optional
			Last page to fetch inclusive; None means fetch until the API
			returns an empty result, by default None.

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
		self.int_page_size = int_page_size
		self.int_page_min = int_page_min
		self.int_page_max = int_page_max
		str_date = self.date_ref.strftime("%Y-%m-%d")
		self.url_tpl = (
			f"https://arquivos.b3.com.br/bdi/table/SaleOff/"
			f"{str_date}/{str_date}/{{page}}/{self.int_page_size}"
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_bdi_fixed_income_settlements",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
			The timeout, by default (12.0, 21.0).
		bool_verify : bool, optional
			Whether to verify the SSL certificate, by default True.
		bool_insert_or_ignore : bool, optional
			Whether to insert or ignore the data, by default False.
		str_table_name : str, optional
			The name of the table, by default "br_b3_bdi_fixed_income_settlements".

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame, or None when writing to database.
		"""
		int_page = self.int_page_min
		list_dfs: list[pd.DataFrame] = []
		while True:
			resp_req = self.get_response(
				int_page=int_page, timeout=timeout, bool_verify=bool_verify
			)
			data = self.parse_raw_file(resp_req)
			df_page = self.transform_data(data)
			if df_page.empty:
				break
			self._log_page_progress(int_page, len(df_page))
			df_page["URL"] = self.url_tpl.format(page=int_page)
			list_dfs.append(df_page)
			if self.int_page_max is not None and int_page >= self.int_page_max:
				break
			int_page += 1
			time.sleep(2)
		if not list_dfs:
			return None
		df_ = pd.concat(list_dfs, ignore_index=True)
		dict_dtypes = {
			"RPT_DT": "date",
			"DT_REF": "date",
			"TCKR_SYMB": str,
			"REF_DT": "date",
			"OPERATION_TYPE": str,
			"SETTLEMENT_METHOD": str,
			"QUANTITY": int,
			"VOLUME": float,
			"URL": str,
		}
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes=dict_dtypes,
			str_fmt_dt="YYYY-MM-DDTHH:MM:SS",
			url=None,
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

	def _log_page_progress(self, int_page: int, int_rows: int) -> None:
		"""Emit a progress message for the current page.

		Parameters
		----------
		int_page : int
			The page number just fetched.
		int_rows : int
			Number of rows returned on that page.

		Returns
		-------
		None
		"""
		self.cls_create_log.log_message(
			logger=self.logger,
			message=(f"B3BdiFixedIncomeSettlements: page {int_page} fetched ({int_rows} rows)"),
			log_level="info",
		)

	@backoff.on_exception(
		backoff.expo,
		(
			requests.exceptions.HTTPError,
			requests.exceptions.Timeout,
			requests.exceptions.ConnectionError,
		),
		max_tries=5,
		factor=2,
		max_time=120,
	)
	def get_response(
		self,
		int_page: int = 1,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Return a response object for the given page.

		Parameters
		----------
		int_page : int, optional
			The page number to fetch, by default 1.
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
			The timeout, by default (12.0, 21.0).
		bool_verify : bool, optional
			Verify the SSL certificate, by default True.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			A response object.
		"""
		url = self.url_tpl.format(page=int_page)
		resp_req = requests.post(url, json={}, timeout=timeout, verify=bool_verify)
		resp_req.raise_for_status()
		return resp_req

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> dict:
		"""Parse the JSON response into the table metadata dict.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response object.

		Returns
		-------
		dict
			The table object containing columns and values from the API response.
		"""
		return resp_req.json()["table"]

	def transform_data(self, data: dict) -> pd.DataFrame:
		"""Build a DataFrame from the API table dict.

		Column names are converted from PascalCase to UPPER_SNAKE_CASE.

		Parameters
		----------
		data : dict
			The table dict with keys 'columns' and 'values'.

		Returns
		-------
		pd.DataFrame
			DataFrame with UPPER_SNAKE_CASE column names, or empty DataFrame
			when values is an empty list.
		"""
		values = data.get("values", [])
		if not values:
			return pd.DataFrame()
		str_handler = StrHandler()
		col_names = [
			str_handler.convert_case(col["name"], "pascal", "upper_constant")
			for col in data.get("columns", [])
		]
		df_ = pd.DataFrame(values)
		rename_map = {i: name for i, name in enumerate(col_names)}
		return df_.rename(columns=rename_map)
