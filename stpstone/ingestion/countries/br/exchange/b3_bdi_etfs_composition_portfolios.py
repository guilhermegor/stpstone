"""B3 BDI index portfolio composition (PreviaQuadrimestral) ingestion."""

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
from stpstone.utils.parsers.str import StrHandler


class B3BdiEtfsCompositionPortfolios(ABCIngestionOperations):
	"""B3 BDI index portfolio composition ingestion class.

	Fetches the composition of index portfolios (e.g. IBOVESPA members) from
	B3's BDI PreviaQuadrimestral endpoint. This endpoint is non-paginated: a
	single POST returns all indices at once, each nested as a child inside the
	top-level ``table`` object.
	"""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
		int_page_size: int = 100,
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
			Number of records requested per call; used in the URL only, by default 100.

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
		str_date = self.date_ref.strftime("%Y-%m-%d")
		self.url = (
			f"https://arquivos.b3.com.br/bdi/table/PreviaQuadrimestral/"
			f"{str_date}/{str_date}/1/{self.int_page_size}"
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_bdi_etfs_composition_portfolios",
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
			The name of the table, by default "br_b3_bdi_etfs_composition_portfolios".

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame, or None when writing to database.
		"""
		resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
		data = self.parse_raw_file(resp_req)
		df_ = self.transform_data(data)
		if df_.empty:
			return None
		int_non_empty = sum(
			1 for child in data if child.get("values")
		)
		self.cls_create_log.log_message(
			logger=self.logger,
			message=(
				f"B3BdiEtfsCompositionPortfolios: fetched {len(df_)} rows"
				f" across {int_non_empty} indices"
			),
			log_level="info",
		)
		df_["URL"] = self.url
		dict_dtypes = {
			"TCKR_SYMB": str,
			"STOCK": str,
			"CODE_TYPE": str,
			"QTY_THEORETICAL": int,
			"STOCK_PARTICIPATION": float,
			"INDEX_NM": str,
			"URL": str,
		}
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes=dict_dtypes,
			str_fmt_dt="YYYY-MM-DD",
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
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Return a response object for the PreviaQuadrimestral endpoint.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
			The timeout, by default (12.0, 21.0).
		bool_verify : bool, optional
			Verify the SSL certificate, by default True.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			A response object.
		"""
		resp_req = requests.post(self.url, json={}, timeout=timeout, verify=bool_verify)
		resp_req.raise_for_status()
		return resp_req

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> list[dict]:
		"""Parse the JSON response into the list of index children.

		The PreviaQuadrimestral endpoint nests each index (IBOVESPA, IBRX, etc.)
		as a child inside ``response["table"]["children"]``. The top-level
		``values`` is always empty; the data lives only in the children.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response object.

		Returns
		-------
		list[dict]
			List of child dicts, one per index.
		"""
		return resp_req.json()["table"]["children"]

	def transform_data(self, data: list[dict]) -> pd.DataFrame:
		"""Build a DataFrame by concatenating rows from all non-empty index children.

		Each child represents one index (e.g. IBOVESPA). Children with empty
		``values`` are silently skipped. Column names are converted from
		PascalCase to UPPER_SNAKE_CASE and an ``INDEX_NM`` column is appended
		with the index's friendly name.

		Parameters
		----------
		data : list[dict]
			List of child dicts returned by ``parse_raw_file``.

		Returns
		-------
		pd.DataFrame
			Concatenated DataFrame for all non-empty indices, or an empty
			DataFrame when no children carry data.
		"""
		if not data:
			return pd.DataFrame()
		str_handler = StrHandler()
		list_dfs: list[pd.DataFrame] = []
		for child in data:
			values = child.get("values", [])
			if not values:
				continue
			col_names = [
				str_handler.convert_case(col["name"], "pascal", "upper_constant")
				for col in child.get("columns", [])
			]
			n_cols = len(col_names)
			df_child = pd.DataFrame(
				[row[:n_cols] for row in values], columns=col_names
			)
			df_child["INDEX_NM"] = child.get("friendlyNameEn") or child.get("name")
			list_dfs.append(df_child)
		if not list_dfs:
			return pd.DataFrame()
		return pd.concat(list_dfs, ignore_index=True)
