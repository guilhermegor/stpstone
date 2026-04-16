"""BCB Quotes Bulletins ingestion."""

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
from stpstone.ingestion.countries.br.macroeconomics.bcb_currencies_codes_ptax import (
	BCBCurrenciesCodesPTAX,
)
from stpstone.ingestion.countries.br.macroeconomics.bcb_currencies_codes_quotes_bulletins import (
	BCBCurrenciesCodesQuotesBulletins,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.lists import ListHandler


class BCBQuotesBulletins(ABCIngestionOperations):
	"""Ingestion concrete class."""

	def __init__(
		self,
		date_start: Optional[date] = None,
		date_end: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the ingestion class.

		Parameters
		----------
		date_start : Optional[date]
			The start date, by default None.
		date_end : Optional[date]
			The end date, by default None.
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
		self.cls_list_handler = ListHandler()
		self.date_start = date_start or self.cls_dates_br.add_working_days(
			self.cls_dates_current.curr_date(), -6
		)
		self.date_end = date_end or self.cls_dates_br.add_working_days(
			self.cls_dates_current.curr_date(), -1
		)
		self.cls_bcb_currencies_codes_ptax = BCBCurrenciesCodesPTAX(
			date_ref=self.date_end,
			logger=self.logger,
			cls_db=self.cls_db,
		)
		self.cls_bcb_currencies_codes_quotes_bulletins = BCBCurrenciesCodesQuotesBulletins(
			date_ref=self.date_end,
			logger=self.logger,
			cls_db=self.cls_db,
		)
		self.fstr_url = (
			"https://ptax.bcb.gov.br/ptax_internet/consultaBoletim.do"
			"?method=gerarCSVFechamentoMoedaNoPeriodo&ChkMoeda={}&DATAINI={}&DATAFIM={}"
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_bcb_quotes_bulletins",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Whether to verify the SSL certificate, by default True.
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False.
		str_table_name : str
			The name of the table, by default "br_bcb_quotes_bulletins".

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		list_ser: list[dict[str, Union[str, int, float]]] = []

		list_currencies_codes = self._get_currencies_codes(timeout=30_000)

		for currency_code in list_currencies_codes:
			resp_req = self.get_response(
				currency_code=currency_code,
				timeout=timeout,
				bool_verify=bool_verify,
			)
			file = self.parse_raw_file(resp_req)
			df_ = self.transform_data(file=file)
			try:
				df_ = self.standardize_dataframe(
					df_=df_,
					date_ref=self.date_end,
					dict_dtypes={
						"DATA": "date",
						"CODIGO_MOEDA": str,
						"TIPO_MOEDA": str,
						"SIMBOLO_MOEDA": str,
						"TAXA_COMPRA": float,
						"TAXA_VENDA": float,
						"PARIDADE_COMPRA": float,
						"PARIDADE_VENDA": float,
					},
					str_fmt_dt="DMMYYY",
					url=self.fstr_url.format(
						currency_code,
						self.date_start.strftime("%d/%m/%Y"),
						self.date_end.strftime("%d/%m/%Y"),
					),
				)
			except (ValueError, RuntimeError):
				continue
			list_ser.extend(df_.to_dict("records"))

		df_ = pd.DataFrame(list_ser)
		if self.cls_db:
			self.insert_table_db(
				cls_db=self.cls_db,
				str_table_name=str_table_name,
				df_=df_,
				bool_insert_or_ignore=bool_insert_or_ignore,
			)
		else:
			return df_

	def _get_currencies_codes(self, timeout: int = 30_000) -> list[str]:
		"""Get the currencies codes from the BCB website.

		Parameters
		----------
		timeout : int
			The timeout, by default 30_000.

		Returns
		-------
		list[str]
			The currencies codes.
		"""
		df_ptax = self.cls_bcb_currencies_codes_ptax.run(timeout=timeout)
		df_qb = self.cls_bcb_currencies_codes_quotes_bulletins.run(timeout=timeout)
		return self.cls_list_handler.extend_lists(
			df_ptax["CODIGO"].to_list(),
			df_qb["CODIGO_MOEDA"].to_list(),
			bool_remove_duplicates=True,
		)

	@backoff.on_exception(
		backoff.expo,
		requests.exceptions.HTTPError,
		max_time=60,
	)
	def get_response(
		self,
		currency_code: str,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Return a list of response objects.

		Parameters
		----------
		currency_code : str
			The currency code.
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Verify the SSL certificate, by default True.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			A list of response objects.
		"""
		dict_headers = {
			"accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",  # noqa E501: line too long
			"accept-language": "en-US,en;q=0.9,pt;q=0.8,es;q=0.7",
			"priority": "u=0, i",
			"referer": "https://ptax.bcb.gov.br/ptax_internet/consultaBoletim.do?method=consultarBoletim",  # noqa E501: line too long
			"sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
			"sec-ch-ua-mobile": "?0",
			"sec-ch-ua-platform": '"Linux"',
			"sec-fetch-dest": "iframe",
			"sec-fetch-mode": "navigate",
			"sec-fetch-site": "same-origin",
			"sec-fetch-user": "?1",
			"upgrade-insecure-requests": "1",
			"user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",  # noqa E501: line too long
			"Cookie": "bcb-aceitacookiev2=%7Bnecessary%3A%20true%2C%20performance%3A%20false%2C%20marketing%3A%20false%7D; JSESSIONID=0000KiClB2uMguEWEYcKXboFGiv:1d4qiornc; BIGipServer~was_s_as3~was_s~pool_was_443_s=!7WFD6HSDNlfA8wovwy5NpAoojAV4FS2iqM6sVPDz2rmo32d5Yi/ZvTSMhUCo2vkYlGmYSmUvpHbBxA==; TS0129e25e=01b3424a9d71002e0aae7533aac013a1f4857095e1c797920d55a3336983ac691466568dc5833c202443bb2f53b4cf869e1d4a89166d64d166cea342c7e7e9b00f3b86ef681dfeb012bb377ed9c1314b3d0dbd7a3b",  # noqa E501: line too long
		}
		resp_req = requests.get(
			self.fstr_url.format(
				currency_code,
				self.date_start.strftime("%d/%m/%Y"),
				self.date_end.strftime("%d/%m/%Y"),
			),
			timeout=timeout,
			verify=bool_verify,
			headers=dict_headers,
		)
		resp_req.raise_for_status()
		return resp_req

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
		return self.get_file(resp_req=resp_req)

	def transform_data(
		self,
		file: StringIO,
	) -> pd.DataFrame:
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
		return pd.read_csv(
			file,
			sep=";",
			names=[
				"DATA",
				"CODIGO_MOEDA",
				"TIPO_MOEDA",
				"SIMBOLO_MOEDA",
				"TAXA_COMPRA",
				"TAXA_VENDA",
				"PARIDADE_COMPRA",
				"PARIDADE_VENDA",
			],
			header=None,
		)
