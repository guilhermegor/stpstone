"""MetaTrader5 trading platform interface.

This module provides a Python interface for interacting with the MetaTrader5 trading platform.
It includes functionality for symbol information retrieval, tick data fetching, and market depth 
analysis.
"""

from datetime import datetime
from logging import Logger
from math import ceil
import time
from typing import Any, Optional

import MetaTrader5 as mt5
import pandas as pd

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker, type_checker
from stpstone.utils.loggs.create_logs import CreateLog


class MT5(metaclass=TypeChecker):
    """Interface for MetaTrader5 trading platform operations."""

    def __init__(self, logger: Logger) -> None:
        """Initialize MetaTrader5 interface.
        
        Parameters
        ----------
        logger : Logger
            Logger object for logging messages

        Returns
        -------
        None
        """
        self.logger = logger

    def package_info(self) -> None:
        """Print MetaTrader5 package information.

        Prints the author and version of the MetaTrader5 package being used.

        Returns
        -------
        None
        """
        CreateLog().log_message(
            self.logger, 
            f"MetaTrader5 package author: {mt5.__author__}", "info"
        )
        CreateLog().log_message(
            self.logger, 
            f"MetaTrader5 package version: {mt5.__version__}", "info"
        )

    def _validate_credentials(
        self, 
        path: str, 
        login: int, 
        server: str, 
        password: str
    ) -> None:
        """Validate MT5 connection credentials.

        Parameters
        ----------
        path : str
            Path to MetaTrader5 terminal executable
        login : int
            Account login number
        server : str
            Trading server name
        password : str
            Account password

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If any credential parameter is empty or invalid type
        """
        if not path:
            raise ValueError("Path cannot be empty")
        if not login:
            raise ValueError("Login cannot be empty")
        if not server:
            raise ValueError("Server cannot be empty")
        if not password:
            raise ValueError("Password cannot be empty")

    def initialize(
        self, 
        path: str, 
        login: int, 
        server: str, 
        password: str
    ) -> bool:
        """Initialize connection to MetaTrader5 terminal.

        Parameters
        ----------
        path : str
            Path to MetaTrader5 terminal executable
        login : int
            Account login number
        server : str
            Trading server name
        password : str
            Account password

        Returns
        -------
        bool
            True if initialization succeeded, False otherwise
        """
        self._validate_credentials(path, login, server, password)
        if not mt5.initialize(path=path, login=login, server=server, password=password):
            CreateLog().log_message(
                self.logger, 
                f"initialize() failed, error code ={mt5.last_error()}", 
                "warning"
            )
            mt5.shutdown()
            return False
        return True

    def shutdown(self) -> None:
        """Shutdown connection to MetaTrader5 terminal.
        
        Returns
        -------
        None
        """
        mt5.shutdown()

    def symbols_get(self) -> tuple:
        """Get all available symbols from MetaTrader5.

        Returns
        -------
        tuple
            Tuple of all available symbols
        """
        return mt5.symbols_get()

    def symbols_total(self) -> Optional[int]:
        """Get total number of available symbols.

        Returns
        -------
        Optional[int]
            Total number of symbols if successful, None otherwise
        """
        symbols = mt5.symbols_total()
        if symbols > 0:
            CreateLog().log_message(self.logger, "Total symbols ={symbols}", "info")
            return symbols
        CreateLog().log_message(self.logger, "Symbols not found", "info")
        return None

    def get_symbols_info(self, market_data: bool = True) -> Optional[pd.DataFrame]:
        """Get detailed information for all symbols.

        Parameters
        ----------
        market_data : bool, optional
            Whether to fetch market data (default: True)

        Returns
        -------
        Optional[pd.DataFrame]
            DataFrame containing symbol information if successful, None otherwise
        """
        symbols = mt5.symbols_get()
        dict_tickers: dict[int, dict[str, Any]] = {}

        if market_data:
            multiplier = ceil(len(symbols) / 4900)
            i = 0
            for j in range(multiplier):
                lim_inf = j * 4900
                lim_sup = len(symbols) if j == multiplier - 1 else (j + 1) * 4900
                
                CreateLog().log_message(self.logger, 'Lower bound: {lim_inf}', "info")
                CreateLog().log_message(self.logger, 'Upper bound: {lim_sup}', "info")
                
                for symbol in symbols[lim_inf:lim_sup]:
                    mt5.symbol_select(symbol.name, True)
                
                CreateLog().log_message(self.logger, 'Loading', "info")
                time.sleep(10)
                
                for symbol in symbols[lim_inf:lim_sup]:
                    ticker_info = mt5.symbol_info(symbol.name)
                    if ticker_info is not None:
                        dict_tickers[i] = ticker_info._asdict()
                        i += 1
                
                for symbol in symbols[lim_inf:lim_sup]:
                    mt5.symbol_select(symbol.name, False)
        else:
            for i, symbol in enumerate(symbols):
                dict_tickers[i] = symbol._asdict()

        df_ = pd.DataFrame(dict_tickers).T
        if df_.empty:
            CreateLog().log_message(self.logger, "Symbols not found", "critical")
            return None

        df_.loc[:, 'paths'] = df_.path.str.split('\\')
        df_.loc[:, 'CLASSE1'] = df_.apply(lambda row: row['paths'][0], axis=1)

        @type_checker
        def type_2(row: pd.Series) -> str:
            """Return the second element of the paths column.
            
            Parameters
            ----------
            row : pd.Series
                Row of the DataFrame
            
            Returns
            -------
            str
                Second element of the paths column
            """
            return 'BMF' if len(row.paths) == 2 else row.paths[1]

        @type_checker
        def type_3(row: pd.Series) -> str:
            """Return the third element of the paths column.
            
            Parameters
            ----------
            row : pd.Series
                Row of the DataFrame
            
            Returns
            -------
            str
                Third element of the paths column
            """
            return row.paths[1] if len(row.paths) == 2 else row.paths[2]

        df_.loc[:, 'CLASSE2'] = df_.apply(type_2, axis=1)
        df_.loc[:, 'CLASSE3'] = df_.apply(type_3, axis=1)

        @type_checker
        def exp_time(row: pd.Series) -> datetime:
            """Return expiration time in datetime format.
            
            Parameters
            ----------
            row : pd.Series
                Row of the DataFrame
            
            Returns
            -------
            datetime
                Expiration time in datetime format
            """
            try:
                return pd.to_datetime(row.expiration_time, unit='s')
            except ValueError:
                return pd.to_datetime(row.expiration_time, unit='ms')

        df_.loc[:, 'expiration_time'] = df_.apply(exp_time, axis=1)
        df_.loc[:, 'time'] = pd.to_datetime(df_['time'], unit='s')

        CreateLog().log_message(self.logger, f"Number of symbols = {df_.shape[0]}", "info")
        return df_

    def get_all_info_of_symbols(self, symbols: tuple) -> pd.DataFrame:
        """Get information for specified symbols.

        Parameters
        ----------
        symbols : tuple
            Symbols to get information for

        Returns
        -------
        pd.DataFrame
            DataFrame containing symbol information
        """
        dict_tickers: dict[int, dict[str, Any]] = {}
        for i, symbol in enumerate(symbols):
            dict_tickers[i] = symbol._asdict()

        df_ = pd.DataFrame(dict_tickers).T
        df_.loc[:, 'paths'] = df_.path.str.split('\\')
        df_.loc[:, 'CLASSE1'] = df_.apply(lambda row: row['paths'][0], axis=1)

        @type_checker
        def type_2(row: pd.Series) -> str:
            """Return the second element of the paths column.
            
            If the length of the paths column is 2, otherwise return the first element of the 
            paths column.
            
            Parameters
            ----------
            row : pd.Series
                Row of the DataFrame.
            
            Returns
            -------
            str
                The second element of the paths column if the length of the paths column is 2, 
                otherwise the first element of the paths column.
            """
            return 'BMF' if len(row.paths) == 2 else row.paths[1]

        @type_checker
        def type_3(row: pd.Series) -> str:
            """Return the third element of the paths column.
            
            If the length of the paths column is 2, otherwise return the second element of the 
            paths column.
            
            Parameters
            ----------
            row : pd.Series
                Row of the DataFrame.
            
            Returns
            -------
            str
                The third element of the paths column if the length of the paths column is 2, 
                otherwise the second element of the paths column.
            """
            return row.paths[1] if len(row.paths) == 2 else row.paths[2]

        df_.loc[:, 'CLASSE2'] = df_.apply(type_2, axis=1)
        df_.loc[:, 'CLASSE3'] = df_.apply(type_3, axis=1)
        df_.loc[:, 'expiration_time'] = pd.to_datetime(df_['expiration_time'], unit='s')
        df_.loc[:, 'time'] = pd.to_datetime(df_['time'], unit='s')
        
        CreateLog().log_message(self.logger, f'Total de tickers: {df_.shape[0]}', "info")
        return df_

    def get_ticks_from(
        self,
        symbol: str,
        dt_ref: datetime,
        ticks_qty: int,
        type_ticks: int = mt5.COPY_TICKS_ALL
    ) -> Optional[pd.DataFrame]:
        """Get tick data starting from specified datetime.

        Parameters
        ----------
        symbol : str
            Symbol to get ticks for
        dt_ref : datetime
            Starting datetime for ticks
        ticks_qty : int
            Number of ticks to retrieve
        type_ticks : int, optional
            Type of ticks to retrieve (default: mt5.COPY_TICKS_ALL)

        Returns
        -------
        Optional[pd.DataFrame]
            DataFrame containing ticks if successful, None otherwise
        """
        ticks = mt5.copy_ticks_from(symbol, dt_ref, ticks_qty, type_ticks)
        ticks_frame = pd.DataFrame(ticks)
        
        CreateLog().log_message(self.logger, f"Ticks recebidos: {ticks_frame.shape[0]}", "info")

        if not ticks_frame.empty:
            ticks_frame['time'] = pd.to_datetime(ticks_frame['time'], unit='s')
            ticks_frame['time_msc'] = pd.to_datetime(ticks_frame['time_msc'], unit='ms')

        return ticks_frame

    def get_ticks_range(
        self,
        symbol: str,
        dt_ref: datetime,
        datetime_to: datetime,
        type_ticks: int = mt5.COPY_TICKS_ALL
    ) -> Optional[pd.DataFrame]:
        """Get tick data within specified datetime range.

        Parameters
        ----------
        symbol : str
            Symbol to get ticks for
        dt_ref : datetime
            Starting datetime for ticks
        datetime_to : datetime
            Ending datetime for ticks
        type_ticks : int, optional
            Type of ticks to retrieve (default: mt5.COPY_TICKS_ALL)

        Returns
        -------
        Optional[pd.DataFrame]
            DataFrame containing ticks if successful, None otherwise
        """
        ticks = mt5.copy_ticks_range(symbol, dt_ref, datetime_to, type_ticks)
        ticks_frame = pd.DataFrame(ticks)
        
        CreateLog().log_message(self.logger, f"Ticks recebidos: {ticks_frame.shape[0]}", "info")

        if not ticks_frame.empty:
            ticks_frame['time'] = pd.to_datetime(ticks_frame['time'], unit='s')
            ticks_frame['time_msc'] = pd.to_datetime(ticks_frame['time_msc'], unit='ms')

        return ticks_frame

    def get_last_tick(self, symbol: str) -> Optional[Any]: # noqa ANN401: typing.Any is disallowed
        """Get last tick for specified symbol.

        Parameters
        ----------
        symbol : str
            Symbol to get last tick for

        Returns
        -------
        Optional[Any]
            Last tick information if successful, None otherwise
        """
        last_tick = mt5.symbol_info_tick(symbol)
        if last_tick:
            return last_tick
        
        CreateLog().log_message(
            self.logger, 
            f"mt5.symbol_info_tick({symbol}) failed, error code = {mt5.last_error()}", 
            "error"
        )
        return None

    def get_market_depth(self, ticker: str, n_times: int = 10) -> Optional[tuple]:
        """Get market depth data for specified ticker.

        Parameters
        ----------
        ticker : str
            Ticker to get market depth for
        n_times : int, optional
            Number of times to fetch market depth (default: 10)

        Returns
        -------
        Optional[tuple]
            Market depth data if successful, None otherwise
        """
        if not mt5.market_book_add(ticker):
            CreateLog().log_message(
                self.logger, 
                f"mt5.market_book_add({ticker}) failed, error code = {mt5.last_error()}", 
                "info"
            )
            return None

        items = None
        for _ in range(n_times):
            items = mt5.market_book_get(ticker)
            CreateLog().log_message(self.logger, items, "info")
            
            if items:
                for it in items:
                    CreateLog().log_message(self.logger, it._asdict(), "info")
            
            time.sleep(5)

        mt5.market_book_release(ticker)
        return items

    def enable_display_marketwatch(self, ticker: str) -> None:
        """Enable display of specified ticker in market watch.

        Parameters
        ----------
        ticker : str
            Ticker to display in market watch

        Returns
        -------
        None
        """
        if not mt5.symbol_select(ticker, True):
            CreateLog().log_message(self.logger, f"Failed to select {ticker}", "error")

    def get_symbol_info_tick(self, ticker: str) -> Optional[dict[str, Any]]:
        """Get tick information for specified symbol.

        Parameters
        ----------
        ticker : str
            Ticker to get information for

        Returns
        -------
        Optional[dict[str, Any]]
            Dictionary containing tick information if successful, None otherwise
        """
        lasttick = mt5.symbol_info_tick(ticker)
        if not lasttick:
            CreateLog().log_message(
                self.logger, 
                f"symbol_info_tick({ticker}) failed, error code = {mt5.last_error()}", 
                "error"
            )
            return None

        CreateLog().log_message(self.logger, f"lasttick = {lasttick}", "info")
        CreateLog().log_message(self.logger, f"Show symbol_info_tick({ticker})._asdict():")
        
        symbol_info_tick_dict = lasttick._asdict()
        for prop in symbol_info_tick_dict:
            CreateLog().log_message(self.logger, f"  {prop}={symbol_info_tick_dict[prop]}", "info")

        return symbol_info_tick_dict

    def get_symbol_properties(self, ticker: str) -> Optional[dict[str, Any]]:
        """Get properties for specified symbol.

        Parameters
        ----------
        ticker : str
            Ticker to get properties for

        Returns
        -------
        Optional[dict[str, Any]]
            Dictionary containing symbol properties if successful, None otherwise
        """
        symbol_info = mt5.symbol_info(ticker)
        if symbol_info is None:
            CreateLog().log_message(
                self.logger, 
                f"symbol_info({ticker}) failed, error code = {mt5.last_error()}", 
                "error"
            )
            return None

        CreateLog().log_message(self.logger, symbol_info, "info")
        CreateLog().log_message(
            self.logger, 
            f"{ticker}: spread = {symbol_info.spread} digits = {symbol_info.digits}", 
            "info"
        )
        CreateLog().log_message(self.logger, f"Show symbol_info({ticker})._asdict():", "info")

        symbol_info_dict = symbol_info._asdict()
        for prop in symbol_info_dict:
            CreateLog().log_message(self.logger, f"  {prop}={symbol_info_dict[prop]}", "info")

        return symbol_info_dict