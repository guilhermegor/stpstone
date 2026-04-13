"""Protocols for MetaTrader5 trading platform interface."""

from datetime import datetime
from typing import Any, Optional, Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class IMT5(Protocol):
	"""Structural protocol for MetaTrader5 trading platform clients."""

	def package_info(self) -> None: ...

	def initialize(
		self,
		path: str,
		login: int,
		server: str,
		password: str,
	) -> bool: ...

	def shutdown(self) -> None: ...

	def symbols_get(self) -> tuple: ...

	def symbols_total(self) -> Optional[int]: ...

	def get_symbols_info(self, market_data: bool = True) -> Optional[pd.DataFrame]: ...

	def get_all_info_of_symbols(self, symbols: tuple) -> pd.DataFrame: ...

	def get_ticks_from(
		self,
		symbol: str,
		date_ref: datetime,
		ticks_qty: int,
		type_ticks: int = ...,
	) -> Optional[pd.DataFrame]: ...

	def get_ticks_range(
		self,
		symbol: str,
		date_ref: datetime,
		datetime_to: datetime,
		type_ticks: int = ...,
	) -> Optional[pd.DataFrame]: ...

	def get_last_tick(self, symbol: str) -> Optional[Any]: ...

	def get_market_depth(self, ticker: str, n_times: int = 10) -> Optional[tuple]: ...

	def enable_display_marketwatch(self, ticker: str) -> None: ...

	def get_symbol_info_tick(self, ticker: str) -> Optional[dict[str, Any]]: ...

	def get_symbol_properties(self, ticker: str) -> Optional[dict[str, Any]]: ...
