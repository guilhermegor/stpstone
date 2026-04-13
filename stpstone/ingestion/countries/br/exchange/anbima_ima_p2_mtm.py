"""Implementation of ingestion instance."""

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


class AnbimaExchangeBRIMAP2MTMs(ABCIngestionOperations):
	"""AnbimaExchangeBRIMAP2MTMs class."""

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
		self.date_ref = date_ref or \
			self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
		self.url = "https://www.anbima.com.br/informacoes/ima-p2/arqs/ima_completo_p2.txt"

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_anbima_ima_p2_mtms",
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
			The name of the table, by default "br_anbima_ima_p2_mtms".

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
		file = self.parse_raw_file(resp_req)
		df_ = self.transform_data(file)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"DATA_REFERENCIA": "date",
				"INDICE": str,
				"NUMERO_INDICE": int,
				"VARIACAO_DIARIA_PCT": float,
				"VARIACAO_MES_PCT": float,
				"VARIACAO_ANUAL_PCT": float,
				"VARIACAO_ULTIMOS_12_MESES": float,
				"VARIACAO_ULTIMOS_24_MESES": float,
				"DURATION_DU": float,
				"CARTEIRA_MERCADO_MTM": float,
				"NUMERO_OPERACOES": int,
				"QTD_NEGOCIADA_1000_TITULOS": int,
				"VALOR_NEGOCIADO_MIL_BRL": float,
				"PMR": float,
			},
			str_fmt_dt="DD/MM/YYYY",
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
		bool_verify : bool, optional
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
	) -> StringIO:
		"""Filter the file content.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The response object.

		Returns
		-------
		StringIO
			The filtered content.
		"""
		file_io: StringIO = self.get_file(resp_req=resp_req)
		list_filtered_lines: list[str] = []

		file_io.seek(0)
		for line in file_io:
			line = line[2:]
			if line.strip() == "":
				break
			line = line.replace("@--@", "@-1@")
			line = line.replace("@--", "@-1")
			list_filtered_lines.append(line.rstrip())

		return StringIO("\n".join(list_filtered_lines))

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
		return pd.read_csv(
			file,
			sep="@",
			skiprows=3,
			engine="python",
			names=[
				"DATA_REFERENCIA",
				"INDICE",
				"NUMERO_INDICE",
				"VARIACAO_DIARIA_PCT",
				"VARIACAO_MES_PCT",
				"VARIACAO_ANUAL_PCT",
				"VARIACAO_ULTIMOS_12_MESES",
				"VARIACAO_ULTIMOS_24_MESES",
				"DURATION_DU",
				"CARTEIRA_MERCADO_MTM",
				"NUMERO_OPERACOES",
				"QTD_NEGOCIADA_1000_TITULOS",
				"VALOR_NEGOCIADO_MIL_BRL",
				"PMR",
			],
			thousands=".",
			decimal=",",
		)
