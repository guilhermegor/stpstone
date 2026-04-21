"""B3 BDI stocks trade-by-trade (tick-level) ingestion."""

from datetime import date
from io import BytesIO, StringIO
from logging import Logger
from typing import Optional, Union
from zipfile import ZipFile

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


_COLUMN_RENAME: dict[str, str] = {
	"DataReferencia": "DATE_REF",
	"CodigoInstrumento": "INSTRUMENT_CODE",
	"AcaoAtualizacao": "UPDATE_ACTION",
	"PrecoNegocio": "TRADE_PRICE",
	"QuantidadeNegociada": "TRADED_QUANTITY",
	"HoraFechamento": "CLOSING_TIME",
	"CodigoIdentificadorNegocio": "TRADE_ID",
	"TipoSessaoPregao": "SESSION_TYPE",
	"DataNegocio": "TRADE_DATE",
	"CodigoParticipanteComprador": "BUYER_CODE",
	"CodigoParticipanteVendedor": "SELLER_CODE",
	"TipoDoCanal": "CHANNEL_TYPE",
}

_DICT_DTYPES: dict[str, object] = {
	"DATE_REF": "date",
	"INSTRUMENT_CODE": str,
	"UPDATE_ACTION": int,
	"TRADE_PRICE": float,
	"TRADED_QUANTITY": int,
	"CLOSING_TIME": str,
	"TRADE_ID": str,
	"SESSION_TYPE": int,
	"TRADE_DATE": "date",
	"BUYER_CODE": str,
	"SELLER_CODE": str,
	"CHANNEL_TYPE": int,
}


class B3BdiStocksTradeByTrade(ABCIngestionOperations):
	"""B3 BDI stocks trade-by-trade (tick-level) ingestion class.

	Fetches a ZIP archive from the B3 RapiNegócios endpoint for a given
	reference date, extracts the semicolon-delimited TXT file inside, and
	returns a DataFrame with one row per individual trade.
	"""

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
			self.cls_dates_current.curr_date(), -1
		)
		str_date = self.date_ref.strftime("%Y-%m-%d")
		self.url = f"https://drp.b3.com.br/rapinegocios/tickercsv/{str_date}?type=2"

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_bdi_stocks_trade_by_trade",
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
			The name of the table, by default "br_b3_bdi_stocks_trade_by_trade".

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame, or None when writing to database.
		"""
		resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
		file = self.parse_raw_file(resp_req)
		df_ = self.transform_data(file=file)
		if df_.empty:
			return None
		self._log_data_progress(len(df_))
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes=_DICT_DTYPES,
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

	def _log_data_progress(self, int_rows: int) -> None:
		"""Emit a progress message for the fetched dataset.

		Parameters
		----------
		int_rows : int
			Number of trade rows returned from the archive.

		Returns
		-------
		None
		"""
		self.cls_create_log.log_message(
			logger=self.logger,
			message=f"B3BdiStocksTradeByTrade: fetched {int_rows} trade rows from {self.url}",
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
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
	) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
		"""Return the raw HTTP response containing the ZIP archive.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
			The timeout, by default (12.0, 21.0).
		bool_verify : bool
			Verify the SSL certificate, by default True.

		Returns
		-------
		Union[Response, PlaywrightPage, SeleniumWebDriver]
			A response object whose content is the ZIP binary payload.
		"""
		resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
		resp_req.raise_for_status()
		return resp_req

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> StringIO:
		"""Extract the TXT file from the ZIP archive and return it as a StringIO.

		The endpoint returns a ZIP archive containing exactly one ``.txt``
		file with semicolon-separated trade records. This method opens the
		archive in-memory, reads the first ``.txt`` entry, and wraps the
		decoded text in a ``StringIO`` ready for ``pd.read_csv``.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
			The HTTP response object whose ``.content`` is the ZIP payload.

		Returns
		-------
		StringIO
			The decoded text content of the first ``.txt`` file found in the archive.

		Raises
		------
		ValueError
			If the ZIP archive contains no ``.txt`` files.
		"""
		with ZipFile(BytesIO(resp_req.content)) as zf:
			txt_names = [n for n in zf.namelist() if n.lower().endswith(".txt")]
			if not txt_names:
				raise ValueError("ZIP archive contains no .txt files.")
			with zf.open(txt_names[0]) as txt_file:
				txt_content = txt_file.read().decode("latin-1")
		return StringIO(txt_content)

	def transform_data(self, file: StringIO) -> pd.DataFrame:
		"""Parse the semicolon-delimited CSV and rename columns to UPPER_SNAKE_CASE.

		Brazilian decimal notation (comma as decimal separator) is handled
		transparently by ``pd.read_csv`` via ``decimal=","``.  The
		``CLOSING_TIME`` and ``TRADE_ID`` columns are read as strings to
		preserve leading zeros and raw numeric identifiers.

		Parameters
		----------
		file : StringIO
			The decoded text stream produced by ``parse_raw_file``.

		Returns
		-------
		pd.DataFrame
			DataFrame with UPPER_SNAKE_CASE column names, or an empty
			DataFrame when the input stream contains no data rows.
		"""
		df_ = pd.read_csv(
			file,
			sep=";",
			decimal=",",
			dtype={
				"HoraFechamento": str,
				"CodigoIdentificadorNegocio": str,
			},
		)
		if df_.empty:
			return pd.DataFrame()
		return df_.rename(columns=_COLUMN_RENAME)
