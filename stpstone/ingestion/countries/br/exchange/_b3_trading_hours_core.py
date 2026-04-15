"""B3 Trading Hours base class (private)."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import backoff
from lxml.html import HtmlElement
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
from stpstone.utils.parsers.html import HtmlHandler


class B3TradingHoursCore(ABCIngestionOperations):
	"""B3 Trading Hours base class."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
		url: str = "FILL_ME",
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
		url : str
		    The url, by default "FILL_ME".

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
		self.cls_html_handler = HtmlHandler()
		self.cls_dicts_handler = HandlingDicts()
		self.date_ref = date_ref or self.cls_dates_br.add_working_days(
			self.cls_dates_current.curr_date(), -1
		)
		self.url = url

	def run(
		self,
		dict_dtypes: dict[str, Union[str, int, float]],
		str_fmt_dt: str = "YYYY-MM-DD",
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "<COUNTRY>_<SOURCE>_<TABLE_NAME>",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		dict_dtypes : dict[str, Union[str, int, float]]
		    The data types of the columns.
		str_fmt_dt : str
		    The date format string, by default "YYYY-MM-DD".
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout, by default (12.0, 21.0).
		bool_verify : bool
		    Whether to verify the SSL certificate, by default True.
		bool_insert_or_ignore : bool
		    Whether to insert or ignore the data, by default False.
		str_table_name : str
		    The name of the table, by default "<COUNTRY>_<SOURCE>_<TABLE_NAME>".

		Returns
		-------
		Optional[pd.DataFrame]
		    The transformed DataFrame.
		"""
		self._validate_run(
			dict_dtypes=dict_dtypes,
			str_table_name=str_table_name,
			str_fmt_dt=str_fmt_dt,
			timeout=timeout,
			bool_verify=bool_verify,
			bool_insert_or_ignore=bool_insert_or_ignore,
		)
		resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
		html_root = self.parse_raw_file(resp_req)
		df_ = self.transform_data(html_root=html_root)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes=dict_dtypes,
			str_fmt_dt=str_fmt_dt,
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

	def _validate_run(
		self,
		dict_dtypes: dict[str, Union[str, int, float]],
		str_table_name: str,
		str_fmt_dt: str,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]],
		bool_verify: bool,
		bool_insert_or_ignore: bool,
	) -> None:
		"""Validate the parameters of the run method.

		Parameters
		----------
		dict_dtypes : dict[str, Union[str, int, float]]
		    The data types of the columns.
		str_table_name : str
		    The name of the table.
		str_fmt_dt : str
		    The date format string.
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
		    The timeout.
		bool_verify : bool
		    Whether to verify the SSL certificate.
		bool_insert_or_ignore : bool
		    Whether to insert or ignore the data.

		Returns
		-------
		None

		Raises
		------
		TypeError
		    If dict_dtypes is not of type dict.
		    If str_table_name is not of type str or is empty.
		    If str_fmt_dt is not of type str.
		    If timeout is not a tuple of positive numbers.
		    If bool_verify is not of type bool.
		    If bool_insert_or_ignore is not of type bool.
		"""
		if not isinstance(dict_dtypes, dict):
			raise TypeError("dict_dtypes must be of type dict")
		if not isinstance(str_table_name, str) or not str_table_name.strip():
			raise TypeError("str_table_name must be of type str and non-empty")
		if not isinstance(str_fmt_dt, str):
			raise TypeError("str_fmt_dt must be of type str")
		if not isinstance(timeout, tuple) or not all(
			isinstance(t, (int, float)) and t > 0 for t in timeout
		):
			raise TypeError("timeout must be a tuple of positive numbers")
		if not isinstance(bool_verify, bool):
			raise TypeError("bool_verify must be of type bool")
		if not isinstance(bool_insert_or_ignore, bool):
			raise TypeError("bool_insert_or_ignore must be of type bool")

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
		self._validate_get_reponse(timeout=timeout, bool_verify=bool_verify)
		resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
		resp_req.raise_for_status()
		return resp_req

	def _validate_get_reponse(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]],
		bool_verify: bool,
	) -> None:
		"""Validate the parameters of the get_response method.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
		    The timeout.
		bool_verify : bool
		    Verify the SSL certificate.

		Returns
		-------
		None

		Raises
		------
		TypeError
		    If timeout is not a tuple of positive numbers or bool_verify is not of type bool.
		"""
		if not isinstance(timeout, tuple) or not all(
			isinstance(t, (int, float)) and t > 0 for t in timeout
		):
			raise TypeError("timeout must be a tuple of positive numbers")
		if not isinstance(bool_verify, bool):
			raise TypeError("bool_verify must be of type bool")

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
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
		return self.cls_html_handler.lxml_parser(resp_req=resp_req)

	def transform_data(
		self,
		html_root: HtmlElement,
		list_th: list[str],
		xpath_td: str,
		na_values: str = "-",
	) -> pd.DataFrame:
		"""Transform a list of response objects into a DataFrame.

		Parameters
		----------
		html_root : HtmlElement
		    The root element of the HTML document.
		list_th : list[str]
		    The list of table headers.
		xpath_td : str
		    The XPath expression for the table data.
		na_values : str
		    The value to use for missing data, by default "-".

		Returns
		-------
		pd.DataFrame
		    The transformed DataFrame.
		"""
		self._validate_transform_data(
			html_root=html_root, list_th=list_th, xpath_td=xpath_td, na_values=na_values
		)
		list_td = self.cls_html_handler.lxml_xpath(
			html_content=html_root,
			str_xpath=xpath_td,
		)
		list_td = [el.text for el in list_td]
		list_ser = self.cls_dicts_handler.pair_headers_with_data(
			list_headers=list_th,
			list_data=list_td,
		)
		df_ = pd.DataFrame(list_ser)
		df_ = df_.replace(to_replace=na_values, value=None)
		return df_

	def _validate_transform_data(
		self, html_root: HtmlElement, list_th: list[str], xpath_td: str, na_values: str
	) -> None:
		"""Validate the parameters of the transform_data method.

		Parameters
		----------
		html_root : HtmlElement
		    The root element of the HTML document.
		list_th : list[str]
		    The list of table headers.
		xpath_td : str
		    The XPath expression for the table data.
		na_values : str
		    The value to use for missing data.

		Returns
		-------
		None

		Raises
		------
		TypeError
		    If html_root is not of type HtmlElement.
		    If list_th is not a list of strings.
		    If xpath_td is not of type str.
		    If na_values is not of type str.
		"""
		if not isinstance(html_root, HtmlElement):
			raise TypeError("html_root must be of type HtmlElement")
		if not isinstance(list_th, list) or not all(isinstance(th, str) for th in list_th):
			raise TypeError("list_th must be a list of strings")
		if not isinstance(xpath_td, str):
			raise TypeError("xpath_td must be of type str")
		if not isinstance(na_values, str):
			raise TypeError("na_values must be of type str")
