"""B3 Trading Hours for Crypto Futures."""

from datetime import date
from logging import Logger
from typing import Optional, Union

from lxml.html import HtmlElement
import pandas as pd
from requests import Session

from stpstone.ingestion.countries.br.exchange._b3_trading_hours_core import B3TradingHoursCore


class B3TradingHoursCryptoFutures(B3TradingHoursCore):
	"""B3 Trading Hours for Crypto Futures."""

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
			url="https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-members-and-traders/trading-hours/derivatives/cryptoassets/",
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_trading_hours_crypto_futures",
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
			The name of the table, by default "br_b3_trading_hours_crypto_futures".

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		return super().run(
			dict_dtypes={
				"CONTRACT": str,
				"TICKER": str,
				"REGULAR_HOURS_OPENING": str,
				"REGULAR_HOURS_CLOSING": str,
				"REGULAR_HOURS_ORDER_CANCELLATION_OPENING": str,
				"REGULAR_HOURS_ORDER_CANCELLATION_CLOSING": str,
				"ELETRONIC_CALL_OPENING": str,
				"ELETRONIC_CALL_ORDER_CANCELLATION_OPENING": str,
				"ELETRONIC_CALL_ORDER_CANCELLATION_CLOSING": str,
				"EXTENDED_HOURS_T_0_OPENING": str,
				"EXTENDED_HOURS_T_0_CLOSING": str,
			},
			timeout=timeout,
			bool_verify=bool_verify,
			bool_insert_or_ignore=bool_insert_or_ignore,
			str_table_name=str_table_name,
		)

	def transform_data(
		self,
		html_root: HtmlElement,
	) -> pd.DataFrame:
		"""Transform a list of response objects into a DataFrame.

		Parameters
		----------
		html_root : HtmlElement
			The root element of the HTML document.

		Returns
		-------
		pd.DataFrame
			The transformed DataFrame.
		"""
		return super().transform_data(
			html_root=html_root,
			list_th=[
				"CONTRACT",
				"TICKER",
				"REGULAR_HOURS_OPENING",
				"REGULAR_HOURS_CLOSING",
				"REGULAR_HOURS_ORDER_CANCELLATION_OPENING",
				"REGULAR_HOURS_ORDER_CANCELLATION_CLOSING",
				"ELETRONIC_CALL_OPENING",
				"ELETRONIC_CALL_ORDER_CANCELLATION_OPENING",
				"ELETRONIC_CALL_ORDER_CANCELLATION_CLOSING",
				"EXTENDED_HOURS_T_0_OPENING",
				"EXTENDED_HOURS_T_0_CLOSING",
			],
			xpath_td='//*[@id="conteudo-principal"]/div[4]/div/table/tbody/tr/td',
			na_values="-",
		)
