"""Protocols for MetaTrader5 trading platform interface."""

from datetime import datetime
from typing import Any, Optional, Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class IMT5(Protocol):
	"""Structural protocol for MetaTrader5 trading platform clients."""

	def package_info(self) -> None:
		"""Log or display MetaTrader5 package information.

		Returns
		-------
		None
		"""
		...

	def initialize(
		self,
		path: str,
		login: int,
		server: str,
		password: str,
	) -> bool:
		"""Initialize the MetaTrader5 connection.

		Parameters
		----------
		path : str
			Path to the MetaTrader5 terminal executable.
		login : int
			Trading account login number.
		server : str
			Name of the trade server.
		password : str
			Password for the trading account.

		Returns
		-------
		bool
			True if initialization succeeded, False otherwise.
		"""
		...

	def shutdown(self) -> None:
		"""Shut down the MetaTrader5 connection.

		Returns
		-------
		None
		"""
		...

	def symbols_get(self) -> tuple:
		"""Return all available market symbols.

		Returns
		-------
		tuple
			Tuple of symbol objects available in the terminal.
		"""
		...

	def symbols_total(self) -> Optional[int]:
		"""Return the total number of available symbols.

		Returns
		-------
		Optional[int]
			Total symbol count, or None if unavailable.
		"""
		...

	def get_symbols_info(self, market_data: bool = True) -> Optional[pd.DataFrame]:
		"""Return symbol information as a DataFrame.

		Parameters
		----------
		market_data : bool
			Whether to include market data fields, by default True.

		Returns
		-------
		Optional[pd.DataFrame]
			DataFrame of symbol info, or None if unavailable.
		"""
		...

	def get_all_info_of_symbols(self, symbols: tuple) -> pd.DataFrame:
		"""Return all available information for the given symbols.

		Parameters
		----------
		symbols : tuple
			Tuple of symbol names to query.

		Returns
		-------
		pd.DataFrame
			DataFrame containing all symbol information.
		"""
		...

	def get_ticks_from(
		self,
		symbol: str,
		date_ref: datetime,
		ticks_qty: int,
		type_ticks: int = ...,
	) -> Optional[pd.DataFrame]:
		"""Return ticks for a symbol starting from a reference datetime.

		Parameters
		----------
		symbol : str
			The market symbol name.
		date_ref : datetime
			Starting datetime for tick retrieval.
		ticks_qty : int
			Number of ticks to retrieve.
		type_ticks : int
			Type of ticks to retrieve.

		Returns
		-------
		Optional[pd.DataFrame]
			DataFrame of ticks, or None if unavailable.
		"""
		...

	def get_ticks_range(
		self,
		symbol: str,
		date_ref: datetime,
		datetime_to: datetime,
		type_ticks: int = ...,
	) -> Optional[pd.DataFrame]:
		"""Return ticks for a symbol within a datetime range.

		Parameters
		----------
		symbol : str
			The market symbol name.
		date_ref : datetime
			Start of the datetime range.
		datetime_to : datetime
			End of the datetime range.
		type_ticks : int
			Type of ticks to retrieve.

		Returns
		-------
		Optional[pd.DataFrame]
			DataFrame of ticks, or None if unavailable.
		"""
		...

	def get_last_tick(self, symbol: str) -> Optional[Any]:
		"""Return the last tick for a symbol.

		Parameters
		----------
		symbol : str
			The market symbol name.

		Returns
		-------
		Optional[Any]
			Last tick data, or None if unavailable.
		"""
		...

	def get_market_depth(self, ticker: str, n_times: int = 10) -> Optional[tuple]:
		"""Return market depth data for a ticker.

		Parameters
		----------
		ticker : str
			The market ticker name.
		n_times : int
			Number of depth levels to retrieve, by default 10.

		Returns
		-------
		Optional[tuple]
			Market depth data as a tuple, or None if unavailable.
		"""
		...

	def enable_display_marketwatch(self, ticker: str) -> None:
		"""Enable display of a ticker in the Market Watch window.

		Parameters
		----------
		ticker : str
			The market ticker name to display.

		Returns
		-------
		None
		"""
		...

	def get_symbol_info_tick(self, ticker: str) -> Optional[dict[str, Any]]:
		"""Return the latest tick info for a symbol.

		Parameters
		----------
		ticker : str
			The market ticker name.

		Returns
		-------
		Optional[dict[str, Any]]
			Tick info as a dict, or None if unavailable.
		"""
		...

	def get_symbol_properties(self, ticker: str) -> Optional[dict[str, Any]]:
		"""Return properties for a symbol.

		Parameters
		----------
		ticker : str
			The market ticker name.

		Returns
		-------
		Optional[dict[str, Any]]
			Symbol properties as a dict, or None if unavailable.
		"""
		...
