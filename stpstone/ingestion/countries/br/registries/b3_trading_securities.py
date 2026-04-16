"""Implementation of B3 Instruments ingestion instance."""

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


class B3Instruments(ABCIngestionOperations):
	"""B3 Instruments ingestion concrete class."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the B3 Instruments ingestion class.

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
			self.cls_dates_current.curr_date(), -1
		)

		self.host = "https://arquivos.b3.com.br/"
		self.token_url = (
			f"{self.host}api/download/requestname?fileName="
			+ f"InstrumentsConsolidatedFile&date={self.date_ref.strftime('%Y-%m-%d')}"
			+ "&recaptchaToken="
		)
		self.data_url_template = f"{self.host}api/download/?token={{token}}#format=.csv"

		self.token = None

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = False,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_tradable_securities_instruments_list",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0)
		bool_verify : bool
			Whether to verify the SSL certificate, by default False
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False
		str_table_name : str
			The name of the table, by default "br_b3_tradable_securities_instruments_list"

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		token_response = self.get_token(timeout=timeout, bool_verify=bool_verify)
		self.token = token_response["token"]

		resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
		file = self.parse_raw_file(resp_req)
		df_ = self.transform_data(file=file)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"RPT_DT": "date",
				"TCKR_SYMB": str,
				"ASST": "category",
				"ASST_DESC": "category",
				"SGMT_NM": "category",
				"MKT_NM": "category",
				"SCTY_CTGY_NM": "category",
				"XPRTN_DT": "date",
				"XPRTN_CD": "category",
				"TRADG_START_DT": "date",
				"TRADG_END_DT": "date",
				"BASE_CD": "category",
				"CONVS_CRIT_NM": "category",
				"MTRTY_DT_TRGT_PT": "category",
				"REQRD_CONVS_IND": "category",
				"ISIN": str,
				"CFICD": "category",
				"DLVRY_NTCE_START_DT": "date",
				"DLVRY_NTCE_END_DT": "date",
				"OPTN_TP": "category",
				"CTRCT_MLTPLR": float,
				"ASST_QTN_QTY": int,
				"ALLCN_RND_LOT": int,
				"TRADG_CCY": "category",
				"DLVRY_TP_NM": "category",
				"WDRWL_DAYS": int,
				"WRKG_DAYS": int,
				"CLNR_DAYS": int,
				"RLVR_BASE_PRIC_NM": "category",
				"OPNG_FUTR_POS_DAY": int,
				"SD_TP_CD1": "category",
				"UNDRLYG_TCKR_SYMB1": str,
				"SD_TP_CD2": "category",
				"UNDRLYG_TCKR_SYMB2": str,
				"PURE_GOLD_WGHT": float,
				"EXRC_PRIC": float,
				"OPTN_STYLE": "category",
				"VAL_TP_NM": "category",
				"PRM_UPFRNT_IND": "category",
				"OPNG_POS_LMT_DT": "date",
				"DSTRBTN_ID": "category",
				"PRIC_FCTR": int,
				"DAYS_TO_STTLM": int,
				"SRS_TP_NM": "category",
				"PRTCN_FLG": "category",
				"AUTOMTC_EXRC_IND": "category",
				"SPCFCTN_CD": "category",
				"CRPN_NM": str,
				"CORP_ACTN_START_DT": "date",
				"CTDY_TRTMNT_TP_NM": "category",
				"MKT_CPTLSTN": float,
				"CORP_GOVN_LVL_NM": "category",
			},
			str_fmt_dt="YYYY-MM-DD",
			url=self.data_url_template.format(token=self.token),
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

	@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=60)
	def get_token(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 12.0),
		bool_verify: bool = False,
	) -> dict:
		"""Get the token for data download.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 12.0)
		bool_verify : bool
			Verify the SSL certificate, by default False

		Returns
		-------
		dict
			The token response containing token and file information.
		"""
		resp_req = requests.get(self.token_url, timeout=timeout, verify=bool_verify)
		resp_req.raise_for_status()
		return resp_req.json()

	@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=60)
	def get_response(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 12.0),
		bool_verify: bool = False,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Return a response object with the instruments data.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 12.0)
		bool_verify : bool
			Verify the SSL certificate, by default False

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			A response object.

		Raises
		------
		ValueError
			If the token is not available.
		"""
		if not self.token:
			raise ValueError("Token not available. Call get_token() first.")

		data_url = self.data_url_template.format(token=self.token)
		resp_req = requests.get(data_url, timeout=timeout, verify=bool_verify)
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
		"""Transform a response object into a DataFrame.

		Parameters
		----------
		file : StringIO
			The parsed content.

		Returns
		-------
		pd.DataFrame
			The transformed DataFrame.
		"""
		column_names = [
			"RPT_DT",
			"TCKR_SYMB",
			"ASST",
			"ASST_DESC",
			"SGMT_NM",
			"MKT_NM",
			"SCTY_CTGY_NM",
			"XPRTN_DT",
			"XPRTN_CD",
			"TRADG_START_DT",
			"TRADG_END_DT",
			"BASE_CD",
			"CONVS_CRIT_NM",
			"MTRTY_DT_TRGT_PT",
			"REQRD_CONVS_IND",
			"ISIN",
			"CFICD",
			"DLVRY_NTCE_START_DT",
			"DLVRY_NTCE_END_DT",
			"OPTN_TP",
			"CTRCT_MLTPLR",
			"ASST_QTN_QTY",
			"ALLCN_RND_LOT",
			"TRADG_CCY",
			"DLVRY_TP_NM",
			"WDRWL_DAYS",
			"WRKG_DAYS",
			"CLNR_DAYS",
			"RLVR_BASE_PRIC_NM",
			"OPNG_FUTR_POS_DAY",
			"SD_TP_CD1",
			"UNDRLYG_TCKR_SYMB1",
			"SD_TP_CD2",
			"UNDRLYG_TCKR_SYMB2",
			"PURE_GOLD_WGHT",
			"EXRC_PRIC",
			"OPTN_STYLE",
			"VAL_TP_NM",
			"PRM_UPFRNT_IND",
			"OPNG_POS_LMT_DT",
			"DSTRBTN_ID",
			"PRIC_FCTR",
			"DAYS_TO_STTLM",
			"SRS_TP_NM",
			"PRTCN_FLG",
			"AUTOMTC_EXRC_IND",
			"SPCFCTN_CD",
			"CRPN_NM",
			"CORP_ACTN_START_DT",
			"CTDY_TRTMNT_TP_NM",
			"MKT_CPTLSTN",
			"CORP_GOVN_LVL_NM",
		]

		return pd.read_csv(
			file, sep=";", skiprows=2, header=None, names=column_names, thousands=".", decimal=","
		)
