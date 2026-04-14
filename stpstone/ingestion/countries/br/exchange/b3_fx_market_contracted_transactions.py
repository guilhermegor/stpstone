"""B3 FX Market Contracted Transactions ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import pandas as pd
from requests import Session

from stpstone.ingestion.countries.br.exchange._b3_search_by_trading_session_base import (
	ABCB3SearchByTradingSession,
)


class B3FXMarketContractedTransactions(ABCB3SearchByTradingSession):
	"""B3 FX Market Contracted Transactions.

	Traded rates, opening parameters and contracted transactions.
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
		super().__init__(
			date_ref=date_ref,
			logger=logger,
			cls_db=cls_db,
			url="https://www.b3.com.br/pesquisapregao/download?filelist=CT{}.zip",
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_fmt_dt: str = "YYYYMMDD",
		str_table_name: str = "br_b3_fx_market_contracted_transactions",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout value, by default (12.0, 21.0).
		bool_verify : bool, optional
		    Whether to verify the data, by default True.
		bool_insert_or_ignore : bool, optional
		    Whether to insert or ignore the data, by default False.
		str_fmt_dt : str, optional
		    The date format, by default "YYYYMMDD".
		str_table_name : str, optional
		    The table name, by default "br_b3_fx_market_contracted_transactions".

		Returns
		-------
		Optional[pd.DataFrame]
		    The DataFrame with the data.
		"""
		return super().run(
			dict_dtypes={
				"ID_TRANSACAO": str,
				"DATA_REFERENCIA": "date",
				"DATA_CONTRATACAO_TXS_PRATICADAS": "date",
				"DATA_LIQUIDACAO_TXS_PRATICADAS": "date",
				"TX_PRATICADA_MX": float,
				"TX_PRATICADA_MIN": float,
				"TX_PRATICADA_MEDIA": float,
				"DATA_CONTRATACAO_PARAMETROS_ABERTURA": "date",
				"DATA_LIQUIDACAO_PARAMETROS_ABERTURA": "date",
				"TAXA_ABERTURA": float,
				"PERCENTAUL_GARANTIDO": float,
				"DATA_CONTRATACAO_OPERACOES_CONTRATADAS": "date",
				"DATA_LIQUIDACAO_OPERACOES_CONTRATADAS": "date",
				"NUMERO_OPERACOES_CONTRATADAS": int,
				"VALOR_OPERACOES_CONTRATADAS_DOLAR": float,
				"VALOR_OPERACOES_CONTRATADAS_REAIS": float,
				"FILE_NAME": "category",
			},
			timeout=timeout,
			bool_verify=bool_verify,
			bool_insert_or_ignore=bool_insert_or_ignore,
			str_fmt_dt=str_fmt_dt,
			str_table_name=str_table_name,
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
		colspecs = [
			(0, 8),
			(8, 16),
			(16, 24),
			(24, 32),
			(32, 41),
			(41, 50),
			(50, 59),
			(59, 67),
			(67, 75),
			(75, 84),
			(84, 89),
			(89, 97),
			(97, 105),
			(105, 113),
			(113, 126),
			(126, 139),
		]

		column_names = [
			"ID_TRANSACAO",
			"DATA_REFERENCIA",
			"DATA_CONTRATACAO_TXS_PRATICADAS",
			"DATA_LIQUIDACAO_TXS_PRATICADAS",
			"TX_PRATICADA_MX",
			"TX_PRATICADA_MIN",
			"TX_PRATICADA_MEDIA",
			"DATA_CONTRATACAO_PARAMETROS_ABERTURA",
			"DATA_LIQUIDACAO_PARAMETROS_ABERTURA",
			"TAXA_ABERTURA",
			"PERCENTAUL_GARANTIDO",
			"DATA_CONTRATACAO_OPERACOES_CONTRATADAS",
			"DATA_LIQUIDACAO_OPERACOES_CONTRATADAS",
			"NUMERO_OPERACOES_CONTRATADAS",
			"VALOR_OPERACOES_CONTRATADAS_DOLAR",
			"VALOR_OPERACOES_CONTRATADAS_REAIS",
		]

		df_ = pd.read_fwf(file, colspecs=colspecs, names=column_names, header=None)
		df_["FILE_NAME"] = file_name
		return df_
