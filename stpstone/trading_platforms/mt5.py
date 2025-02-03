import MetaTrader5 as mt5
import time
import pandas as pd
from datetime import datetime
from math import ceil


class MT5():

    def package_info(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        print("MetaTrader5 package author: ", mt5.__author__)
        print("MetaTrader5 package version: ", mt5.__version__)

    def initialize(self, path, login, server, password):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        if not mt5.initialize(path=path, login=login, server=server, password=password):
            print("initialize() failed, error code =", mt5.last_error())
            # shut down connection to the MetaTrader 5 terminal
            mt5.shutdown()
            # quit()

    def shutdown(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        mt5.shutdown()

    def symbols_get(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return mt5.symbols_get()

    def symbols_total(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        symbols = mt5.symbols_total()
        if symbols > 0:
            print("Total symbols =", symbols)
            return symbols
        else:
            print("Symbols not found")
            return None

    def get_symbols_info(self, market_data=True):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        symbols = mt5.symbols_get()
        dict_tickers = dict()
        if market_data:
            qtd_vezes = ceil(len(symbols) / 4900)
            l = 0
            for j in range(qtd_vezes):
                lim_inf = j * 4900
                if j == qtd_vezes - 1:
                    lim_sup = len(symbols)
                else:
                    lim_sup = (j + 1) * 4900
                print('Limite inferior:', lim_inf)
                print('Limite superior:', lim_sup)
                for symbol in symbols[lim_inf:lim_sup]:
                    mt5.symbol_select(symbol.name, True)
                print('Aguardando')
                time.sleep(10)
                for symbol in symbols[lim_inf:lim_sup]:
                    ticker_info = mt5.symbol_info(symbol.name)
                    if ticker_info != None:
                        dict_tickers[l] = ticker_info._asdict()
                        l += 1
                for symbol in symbols[lim_inf:lim_sup]:
                    mt5.symbol_select(symbol.name, False)
        else:
            l = 0
            for symbol in symbols:
                dict_tickers[l] = symbol._asdict()
                l += 1
        df = pd.DataFrame(dict_tickers).T
        df.loc[:, 'paths'] = df.path.str.split('\\')
        df.loc[:, 'CLASSE1'] = df.apply(lambda row: row['paths'][0], axis=1)

        def tipo2(row):
            if len(row.paths) == 2:
                return 'BMF'
            else:
                return row.paths[1]

        def tipo3(row):
            if len(row.paths) == 2:
                return row.paths[1]
            else:
                return row.paths[2]
        df.loc[:, 'CLASSE2'] = df.apply(tipo2, axis=1)
        df.loc[:, 'CLASSE3'] = df.apply(tipo3, axis=1)

        def exp_time(row):
            try:
                result = pd.to_datetime(row.expiration_time, unit='s')
            except:
                result = pd.to_datetime(row.expiration_time, unit='ms')
            return result
        df.loc[:, 'expiration_time'] = df.apply(exp_time, axis=1)
        df.loc[:, 'time'] = pd.to_datetime(df['time'], unit='s')
        # symbols=mt5.symbols_get()
        if df.shape[0] > 0:
            print("Number of symbols =", df.shape[0])
            return df
        else:
            print("Symbols not found")
            return None

    def get_all_info_of_symbols(self, symbols):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        dict_tickers = dict()
        i = 0
        for symbol in symbols:
            dict_tickers[i] = symbol._asdict()
            i += 1
        df = pd.DataFrame(dict_tickers).T
        df.loc[:, 'paths'] = df.path.str.split('\\')
        df.loc[:, 'CLASSE1'] = df.apply(lambda row: row['paths'][0], axis=1)

        def tipo2(row):
            if len(row.paths) == 2:
                return 'BMF'
            else:
                return row.paths[1]

        def tipo3(row):
            if len(row.paths) == 2:
                return row.paths[1]
            else:
                return row.paths[2]
        df.loc[:, 'CLASSE2'] = df.apply(tipo2, axis=1)
        df.loc[:, 'CLASSE3'] = df.apply(tipo3, axis=1)
        df.loc[:, 'expiration_time'] = pd.to_datetime(
            df['expiration_time'], unit='s')
        df.loc[:, 'time'] = pd.to_datetime(df['time'], unit='s')
        print('Total de tickers: {}'.format(df.shape[0]))
        return df

    def get_ticks_from(self, symbol, datetime_from, ticks_qty,
                       type_ticks=mt5.COPY_TICKS_ALL, print_quantidade=False):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        if not isinstance(datetime_from, datetime):
            print('datetime_from is not a datetime object')
            return None
        ticks = mt5.copy_ticks_from(
            symbol, datetime_from, ticks_qty, type_ticks)
        # a partir dos dados recebidos criamos o DataFrame
        ticks_frame = pd.DataFrame(ticks)
        if print_quantidade:
            print("Ticks recebidos:", ticks_frame.shape[0])
        if not ticks_frame.empty:
            # convertemos o tempo em segundos no formato datetime
            ticks_frame['time'] = pd.to_datetime(ticks_frame['time'], unit='s')
            ticks_frame['time_msc'] = pd.to_datetime(
                ticks_frame['time_msc'], unit='ms')
        return ticks_frame

    def get_ticks_range(self, symbol, datetime_from, datetime_to,
                        type_ticks=mt5.COPY_TICKS_ALL, print_quantidade=False):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        if not isinstance(datetime_from, datetime) or not isinstance(datetime_to, datetime):
            print('datetime_from or datetime_to is not a datetime object')
            return None
        ticks = mt5.copy_ticks_range(
            symbol, datetime_from, datetime_to, type_ticks)
        # a partir dos dados recebidos criamos o DataFrame
        ticks_frame = pd.DataFrame(ticks)
        if print_quantidade:
            print("Ticks recebidos:", ticks_frame.shape[0])
        if not ticks_frame.empty:
            # convertemos o tempo em segundos no formato datetime
            ticks_frame['time'] = pd.to_datetime(ticks_frame['time'], unit='s')
            ticks_frame['time_msc'] = pd.to_datetime(
                ticks_frame['time_msc'], unit='ms')
        return ticks_frame

    def get_last_tick(self, symbol):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        last_tick = mt5.symbol_info_tick(symbol)
        if last_tick:
            return last_tick
        else:
            print("mt5.symbol_info_tick({}) failed, error code =".format(
                symbol), mt5.last_error())
            return None

    def get_market_depth(self, ticker, n_times=10):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        if mt5.market_book_add(ticker):
            # get the market depth data 10 times in a loop
            for i in range(n_times):
                # get the market depth content (Depth of Market)
                items = mt5.market_book_get(ticker)
                # display the entire market depth 'as is' in a single string
                print(items)
                # now display each order separately for more clarity
                if items:
                    for it in items:
                        # order content
                        print(it._asdict())
                # pause for 5 seconds before the next request of the market depth data
                time.sleep(5)
            # cancel the subscription to the market depth updates (Depth of Market)
            mt5.market_book_release(ticker)
        else:
            print("mt5.market_book_add({}) failed, error code =".format(
                ticker), mt5.last_error())
        # shut down connection to the MetaTrader 5 terminal
        return items

    def enable_display_marketwatch(self, ticker):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        selected = mt5.symbol_select(ticker, True)
        if not selected:
            print("Failed to select {}".format(ticker))

    def get_symbol_info_tick(self, ticker):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # display the last GBPUSD tick
        lasttick = mt5.symbol_info_tick(ticker)
        print(lasttick)
        # display tick field values in the form of a list
        print("Show symbol_info_tick({})._asdict():".format(ticker))
        symbol_info_tick_dict = mt5.symbol_info_tick(ticker)._asdict()
        for prop in symbol_info_tick_dict:
            print("  {}={}".format(prop, symbol_info_tick_dict[prop]))
        return symbol_info_tick_dict

    def get_symbol_properties(self, ticker):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # display EURJPY symbol properties
        symbol_info = mt5.symbol_info(ticker)
        if symbol_info != None:
            # display the terminal data 'as is'
            print(symbol_info)
            print("{}: spread = {} digits = {}".format(
                ticker, symbol_info.spread))
            # display symbol properties as a list
            print("Show symbol_info({})._asdict():".format(ticker))
            symbol_info_dict = mt5.symbol_info(ticker)._asdict()
            for prop in symbol_info_dict:
                print("  {}={}".format(prop, symbol_info_dict[prop]))
        return symbol_info_dict
