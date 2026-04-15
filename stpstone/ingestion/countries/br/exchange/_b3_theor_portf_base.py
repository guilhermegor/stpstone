"""Shared base class for B3 theoretical portfolio ingestion."""

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
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement


class _B3TheoreticalPortfolioBase(ABCIngestionOperations):
	"""Shared base for B3 theoretical portfolio ingestion classes."""

	_URL: str
	_TABLE_NAME: str
	_DTYPES: dict[str, object] = {
		"SEGMENT": str,
		"COD": str,
		"ASSET": str,
		"TYPE": str,
		"PART": float,
		"PART_ACUM": str,
		"THEORICAL_QTY": int,
		"PAGE_NUMBER": int,
		"PAGE_SIZE": int,
		"TOTAL_RECORDS": int,
		"TOTAL_PAGES": int,
		"DATE_HEADER": "date",
		"TEXT_HEADER": str,
		"PART_HEADER": int,
		"PART_ACUM_HEADER": str,
		"TEXT_REDUCTOR_HEADER": str,
		"REDUCTOR_HEADER": str,
		"THEORICAL_QTY_HEADER": int,
	}

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
		self.cls_handling_dicts = HandlingDicts()
		self.date_ref = date_ref or (
			self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
		)
		self.url = self._URL

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout, by default (12.0, 21.0).
		bool_verify : bool
		    Whether to verify the SSL certificate, by default True.
		bool_insert_or_ignore : bool
		    Whether to insert or ignore the data, by default False.
		str_table_name : str
		    The name of the table; falls back to the class default when empty,
		    by default "".

		Returns
		-------
		Optional[pd.DataFrame]
		    The transformed DataFrame.
		"""
		if self.cls_db and not (str_table_name or self._TABLE_NAME):
			raise ValueError("str_table_name cannot be empty")
		resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
		list_ser = self.parse_raw_file(resp_req)
		df_ = self.transform_data(list_ser=list_ser)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes=self._DTYPES,
			str_fmt_dt="DD/MM/YY",
			url=self.url,
			cols_from_case="camel",
			cols_to_case="upper_constant",
		)
		_table_name = str_table_name or self._TABLE_NAME
		if self.cls_db:
			self.insert_table_db(
				cls_db=self.cls_db,
				str_table_name=_table_name,
				df_=df_,
				bool_insert_or_ignore=bool_insert_or_ignore,
			)
			return None
		return df_

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
		    The timeout, by default (12.0, 21.0).
		bool_verify : bool
		    Verify the SSL certificate, by default True.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
		    A list of response objects.
		"""
		resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
		resp_req.raise_for_status()
		return resp_req

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> list[dict[str, Union[str, int, float]]]:
		"""Parse the raw file content.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
		    The response object.

		Returns
		-------
		list[dict[str, Union[str, int, float]]]
		    The parsed content.
		"""
		json_ = resp_req.json()
		list_ser = json_["results"]
		list_ser = self.cls_handling_dicts.add_key_value_to_dicts(
			list_ser=list_ser,
			key=[json_["page"]],
		)
		list_ser = self.cls_handling_dicts.add_key_value_to_dicts(
			list_ser=list_ser,
			key=[{k + "Header": v for k, v in json_["header"].items()}],
		)
		return list_ser

	def transform_data(
		self,
		list_ser: list[dict[str, Union[str, int, float]]],
	) -> pd.DataFrame:
		"""Transform a list of dicts into a DataFrame.

		Parameters
		----------
		list_ser : list[dict[str, Union[str, int, float]]]
		    The parsed content.

		Returns
		-------
		pd.DataFrame
		    The transformed DataFrame.
		"""
		return pd.DataFrame(list_ser)
