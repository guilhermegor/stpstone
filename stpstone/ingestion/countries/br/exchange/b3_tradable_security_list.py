"""B3 Tradable Security List ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import pandas as pd
from requests import Session

from stpstone.ingestion.countries.br.exchange._b3_search_by_trading_session_base import (
	ABCB3SearchByTradingSession,
)


class B3TradableSecurityList(ABCB3SearchByTradingSession):
	"""B3 Tradable Security List."""

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
			url="https://www.b3.com.br/pesquisapregao/download?filelist=SecurityList{}.zip",
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
		str_table_name: str = "br_b3_tradable_security_list",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout, by default (12.0, 21.0).
		bool_verify : bool
		    Whether to verify the data, by default True.
		bool_insert_or_ignore : bool
		    Whether to insert or ignore the data, by default False.
		str_fmt_dt : str
		    The format of the date, by default "YYYY-MM-DD".
		str_table_name : str
		    The name of the table, by default "br_b3_tradable_security_list".

		Returns
		-------
		Optional[pd.DataFrame]
		    The DataFrame with the data.
		"""
		return super().run(
			dict_dtypes={
				"SYMBOL": str,
				"SECURITY_ID": int,
				"SECURITY_ID_SOURCE": "category",
				"SECURITY_EXCHANGE": "category",
				"NO_APPL_IDS": "category",
				"APPL_ID": str,
				"PUT_OR_CALL": "category",
				"PRODUCT": "category",
				"CFI_CODE": "category",
				"SECURITY_GROUP": str,
				"SECURITY_TYPE": "category",
				"SECURITY_SUB_TYPE": "category",
				"MATURITY_MONTH_YEAR": int,
				"MATURITY_DATE": "date",
				"ISSUE_DATE": "date",
				"COUNTRY_OF_ISSUE": "category",
				"STRIKE_PRICE": float,
				"STRIKE_CURRENCY": "category",
				"EXERCISE_STYLE": "category",
				"CONTRACT_MULTIPLIER": float,
				"SECURITY_DESC": str,
				"CONTRACT_SETTL_MONTH": int,
				"DATED_DATE": "date",
				"SETTL_TYPE": "category",
				"SETTL_DATE": "date",
				"PRICE_DIVISOR": int,
				"MIN_PRICE_INCREMENT": float,
				"TICK_SIZE_DENOMINATOR": int,
				"MIN_ORDER_QTY": int,
				"MAX_ORDER_QTY": int,
				"MULTI_LEG_MODEL": int,
				"MULTI_LEG_PRICE_METHOD": int,
				"INDEX_PCT": str,
				"NO_INSTR_ATTRIB": int,
				"INSTR_ATTRIB_TYPE": str,
				"INSTR_ATTRIB_VALUE": str,
				"START_DATE": str,
				"END_DATE": str,
				"NO_UNDERLYINGS": int,
				"UNDERLYING_SYMBOL": str,
				"UNDERLYING_SECURITY_ID": str,
				"UNDERLYING_SECURITY_ID_SOURCE": str,
				"UNDERLYING_SECURITY_EXCHANGE": "category",
				"INDEX_THEORETICAL_QTY": str,
				"CURRENCY": "category",
				"SETTL_CURRENCY": "category",
				"SECURITY_STRATEGY_TYPE": "category",
				"ASSET": "category",
				"NO_SHARES_ISSUED": int,
				"SECURITY_VALIDITY_TIMESTAMP": str,
				"MARKET_SEGMENT_ID": int,
				"GOVERNANCE_INDICATOR": "category",
				"CORPORATE_ACTION_EVENT_ID": int,
				"SECURITY_MATCH_TYPE": int,
				"NO_LEGS": int,
				"LEG_SYMBOL": str,
				"LEG_SECURITY_ID": str,
				"LEG_SECURITY_ID_SOURCE": str,
				"LEG_SECURITY_TYPE": "category",
				"LEG_SECURITY_EXCHANGE": "category",
				"LEG_RATIO_QTY": "category",
				"LEG_SIDE": "category",
				"NO_TICK_RULES": str,
				"NO_LOT_TYPE_RULES": int,
				"LOT_TYPE": "category",
				"MIN_LOT_SIZE": int,
				"IMPLIED_MARKET_INDICATOR": "category",
				"MIN_CROSS_QTY": int,
				"ISIN_NUMBER": str,
				"CLEARING_HOUSE_ID": int,
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
		df_ = pd.read_csv(
			file,
			sep=",",
			skiprows=1,
			names=[
				"SYMBOL",
				"SECURITY_ID",
				"SECURITY_ID_SOURCE",
				"SECURITY_EXCHANGE",
				"NO_APPL_IDS",
				"APPL_ID",
				"PUT_OR_CALL",
				"PRODUCT",
				"CFI_CODE",
				"SECURITY_GROUP",
				"SECURITY_TYPE",
				"SECURITY_SUB_TYPE",
				"MATURITY_MONTH_YEAR",
				"MATURITY_DATE",
				"ISSUE_DATE",
				"COUNTRY_OF_ISSUE",
				"STRIKE_PRICE",
				"STRIKE_CURRENCY",
				"EXERCISE_STYLE",
				"CONTRACT_MULTIPLIER",
				"SECURITY_DESC",
				"CONTRACT_SETTL_MONTH",
				"DATED_DATE",
				"SETTL_TYPE",
				"SETTL_DATE",
				"PRICE_DIVISOR",
				"MIN_PRICE_INCREMENT",
				"TICK_SIZE_DENOMINATOR",
				"MIN_ORDER_QTY",
				"MAX_ORDER_QTY",
				"MULTI_LEG_MODEL",
				"MULTI_LEG_PRICE_METHOD",
				"INDEX_PCT",
				"NO_INSTR_ATTRIB",
				"INSTR_ATTRIB_TYPE",
				"INSTR_ATTRIB_VALUE",
				"START_DATE",
				"END_DATE",
				"NO_UNDERLYINGS",
				"UNDERLYING_SYMBOL",
				"UNDERLYING_SECURITY_ID",
				"UNDERLYING_SECURITY_ID_SOURCE",
				"UNDERLYING_SECURITY_EXCHANGE",
				"INDEX_THEORETICAL_QTY",
				"CURRENCY",
				"SETTL_CURRENCY",
				"SECURITY_STRATEGY_TYPE",
				"ASSET",
				"NO_SHARES_ISSUED",
				"SECURITY_VALIDITY_TIMESTAMP",
				"MARKET_SEGMENT_ID",
				"GOVERNANCE_INDICATOR",
				"CORPORATE_ACTION_EVENT_ID",
				"SECURITY_MATCH_TYPE",
				"NO_LEGS",
				"LEG_SYMBOL",
				"LEG_SECURITY_ID",
				"LEG_SECURITY_ID_SOURCE",
				"LEG_SECURITY_TYPE",
				"LEG_SECURITY_EXCHANGE",
				"LEG_RATIO_QTY",
				"LEG_SIDE",
				"NO_TICK_RULES",
				"NO_LOT_TYPE_RULES",
				"LOT_TYPE",
				"MIN_LOT_SIZE",
				"IMPLIED_MARKET_INDICATOR",
				"MIN_CROSS_QTY",
				"ISIN_NUMBER",
				"CLEARING_HOUSE_ID",
			],
		)
		df_["FILE_NAME"] = file_name
		return df_
