"""B3 Consolidated Trades Ingestion.

This module provides an ingestion class for B3 Consolidated Trades data.
It handles the retrieval of data from the B3 website and stores it in a database.
"""

from datetime import date
from io import StringIO
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


class B3ConsolidatedTrades(ABCIngestionOperations):
	"""Ingestion concrete class."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
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
		self.url_token = (
			"https://arquivos.b3.com.br/api/download/requestname?"
			+ "fileName=TradeInformationConsolidatedFile&"
			+ "date={}&recaptchaToken=".format(self.date_ref.strftime("%Y-%m-%d"))
		)
		self.token = self.get_token()
		self.url = f"https://arquivos.b3.com.br/api/download/?token={self.token}#format=.csv"

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_consolidated_trades",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout, by default (12.0, 21.0)
		bool_verify : bool, optional
		    Whether to verify the SSL certificate, by default True
		bool_insert_or_ignore : bool, optional
		    Whether to insert or ignore the data, by default False
		str_table_name : str, optional
		    The name of the table, by default "br_b3_consolidated_trades"

		Returns
		-------
		Optional[pd.DataFrame]
		    The transformed DataFrame.
		"""
		resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
		file = self.parse_raw_file(resp_req)
		df_ = self.transform_data(file=file)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"RPT_DT": "date",
				"TCKR_SYMB": str,
				"ISIN": str,
				"SGMT_NM": "category",
				"MIN_PRIC": float,
				"MAX_PRIC": float,
				"TRAD_AVRG_PRIC": float,
				"LAST_PRIC": float,
				"OSCN_PCTG": float,
				"ADJSTD_QT": float,
				"ADJSTD_QT_TAX": float,
				"REF_PRIC": float,
				"TRAD_QTY": int,
				"FIN_INSTRM_QTY": int,
				"NTL_FIN_VOL": float,
			},
			str_fmt_dt="YYYY-MM-DD",
			url=self.url,
			dict_fillna_strt={
				"TRAD_QTY": "bfill",
			},
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

	def get_token(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
	) -> str:
		"""Get the token.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout, by default (12.0, 21.0)

		Returns
		-------
		str
		    The token.
		"""
		resp_req = requests.get(self.url_token, timeout=timeout)
		resp_req.raise_for_status()
		return resp_req.json()["token"]

	@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=60)
	def get_response(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Return a list of response objects.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout, by default (12.0, 21.0)
		bool_verify : bool, optional
		    Verify the SSL certificate, by default True

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
		    A list of response objects.
		"""
		resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
		resp_req.raise_for_status()
		return resp_req

	def parse_raw_file(
		self, resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
	) -> StringIO:
		"""Parse the raw file content.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
		    The response object.

		Returns
		-------
		StringIO
		    The parsed content.
		"""
		return self.get_file(resp_req=resp_req)

	def transform_data(self, file: StringIO) -> pd.DataFrame:
		"""Transform a list of response objects into a DataFrame.

		Parameters
		----------
		file : StringIO
		    The parsed content.

		Returns
		-------
		pd.DataFrame
		    The transformed DataFrame.
		"""
		df_ = pd.read_csv(
			file,
			sep=";",
			skiprows=2,
			thousands=".",
			decimal=",",
			names=[
				"RPT_DT",
				"TCKR_SYMB",
				"ISIN",
				"SGMT_NM",
				"MIN_PRIC",
				"MAX_PRIC",
				"TRAD_AVRG_PRIC",
				"LAST_PRIC",
				"OSCN_PCTG",
				"ADJSTD_QT",
				"ADJSTD_QT_TAX",
				"REF_PRIC",
				"TRAD_QTY",
				"FIN_INSTRM_QTY",
				"NTL_FIN_VOL",
			],
		)
		df_ = df_.fillna(-1)
		return df_
