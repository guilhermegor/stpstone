"""B3 Equities Option Reference Premiums ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
from requests import Response, Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.countries.br.exchange._b3_search_by_trading_session_base import (
	ABCB3SearchByTradingSession,
)


class B3EquitiesOptionReferencePremiums(ABCB3SearchByTradingSession):
	"""B3 Equities Option Reference Premiums."""

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
		super().__init__(
			date_ref=date_ref,
			logger=logger,
			cls_db=cls_db,
			url="https://www.b3.com.br/pesquisapregao/download?filelist=PE{}.ex_",
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_fmt_dt: str = "YYYY-MM-DD",
		str_table_name: str = "br_b3_equities_option_reference_premiums",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout value, by default (12.0, 21.0).
		bool_verify : bool
			Whether to verify the data, by default True.
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_fmt_dt : str
			The date format, by default "YYYY-MM-DD".
		str_table_name : str
			The table name, by default "br_b3_equities_option_reference_premiums".

		Returns
		-------
		Optional[pd.DataFrame]
			The DataFrame with the data.
		"""
		return super().run(
			dict_dtypes={
				"TICKER_SYMBOL": str,
				"OPTION_TYPE": str,
				"OPTION_STYLE": str,
				"EXPIRY_DATE": int,
				"EXERCISE_PRICE": float,
				"REFERENCE_PREMIUM": float,
				"IMPLIED_VOLATILITY": float,
				"FILE_NAME": "category",
			},
			timeout=timeout,
			bool_verify=bool_verify,
			bool_insert_or_ignore=bool_insert_or_ignore,
			str_fmt_dt=str_fmt_dt,
			str_table_name=str_table_name,
		)

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
		prefix: str = "b3_option_premiums_",
		file_name: str = "b3_equities_option_reference_premiums_",
	) -> tuple[StringIO, str]:
		"""Parse the raw file content by executing Windows executable with Wine.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response object.
		prefix : str
			The prefix of the file name, by default "b3_option_premiums_".
		file_name : str
			The name of the file, by default "b3_equities_option_reference_premiums_".

		Returns
		-------
		tuple[StringIO, str]
			The parsed content and file name.
		"""
		return self.parse_raw_ex_file(
			resp_req=resp_req,
			prefix=prefix,
			file_name=file_name,
		)

	def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
		"""Transform file content into a DataFrame.

		Parameters
		----------
		file : StringIO
			The file content.
		file_name : str
			The file name.

		Returns
		-------
		pd.DataFrame
			The transformed DataFrame.
		"""
		df_ = pd.read_csv(
			file,
			sep=";",
			skiprows=1,
			names=[
				"TICKER_SYMBOL",
				"OPTION_TYPE",
				"OPTION_STYLE",
				"EXPIRY_DATE",
				"EXERCISE_PRICE",
				"REFERENCE_PREMIUM",
				"IMPLIED_VOLATILITY",
			],
		)
		df_["FILE_NAME"] = file_name
		return df_
