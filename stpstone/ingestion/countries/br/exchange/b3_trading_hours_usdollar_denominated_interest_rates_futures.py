"""B3 Trading Hours for US Dollar Denominated Interest Rates Futures."""

from datetime import date
from logging import Logger
from typing import Optional, Union

from lxml.html import HtmlElement
import pandas as pd
from requests import Session

from stpstone.ingestion.countries.br.exchange._b3_trading_hours_core import B3TradingHoursCore


class B3TradingHoursUSDollarDenominatedInterestRatesFutures(B3TradingHoursCore):
	"""B3 Trading Hours for US Dollar Denominated Interest Rates Futures.

	Dollar Denominated Interest Rates Futures, Options Contracts and Structures Transactions.
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
			url="https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-members-and-traders/trading-hours/derivatives/interest-rates/",
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_trading_hours_usdollar_denominated_interest_rates_futures",
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
		    The name of the table, by default
		    "br_b3_trading_hours_usdollar_denominated_interest_rates_futures".

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
				"ELECTRONIC_CALL_OPENING": str,
				"ELECTRONIC_CALL_ORDER_CANCELLATION_OPENING": str,
				"ELECTRONIC_CALL_ORDER_CANCELLATION_CLOSING": str,
				"EXTENDED_HOURS_T_0_OPENING": str,
				"EXTENDED_HOURS_T_0_CLOSING": str,
				"AFTER_HOURS_T_1_OPENING": str,
				"AFTER_HOURS_T_1_CLOSING": str,
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
				"ELECTRONIC_CALL_OPENING",
				"ELECTRONIC_CALL_ORDER_CANCELLATION_OPENING",
				"ELECTRONIC_CALL_ORDER_CANCELLATION_CLOSING",
				"EXTENDED_HOURS_T_0_OPENING",
				"EXTENDED_HOURS_T_0_CLOSING",
				"AFTER_HOURS_T_1_OPENING",
				"AFTER_HOURS_T_1_CLOSING",
			],
			xpath_td='//*[@id="conteudo-principal"]/div[4]/div/div/table[2]/tbody/tr/td',
			na_values="-",
		)
