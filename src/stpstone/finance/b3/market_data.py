### BRAZILLIAN EXCHANGE MARKET DATA

import ast
import json
import math
import os
import sys
from datetime import date, datetime
from typing import List, Optional, Tuple, Union

import investpy
import numpy as np
import pandas as pd
import yfinance as yf
from requests import request
from yahooquery import Ticker

sys.path.append(
    '\\'.join(
        [d for d in os.path.dirname(os.path.realpath(__file__)).split('\\')][
            :-3
        ]
    )
)
from stpstone.cals.handling_dates import DatesBR
from stpstone.document_numbers.br import DocumentsNumbersBR
from stpstone.finance.comdinheiro.api_request import ComDinheiro
from stpstone.handling_data.dicts import HandlingDicts
from stpstone.handling_data.html import HtmlHndler
from stpstone.handling_data.json import JsonFiles
from stpstone.handling_data.lists import HandlingLists
from stpstone.handling_data.numbers import NumHandler
from stpstone.handling_data.object import HandlingObjects
from stpstone.handling_data.str import StrHandler
from stpstone.loggs.db_logs import DBLogs
from stpstone.settings._global_slots import YAML_B3


class TradingVolumeB3:
    def bov_monthly(
        self, int_year: int, int_month: int
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # setting variables
        list_th = list()
        list_td = list()
        # format month and year to string
        str_month = f'{int_month:02}'
        str_year = str(int_year)
        url = YAML_B3['bov_trading_volume']['url'].format(str_month, str_year)
        # request html
        bs_html = HtmlHndler().html_bs_parser(
            url, bl_verify=YAML_B3['bov_trading_volume']['bl_verify']
        )
        # table
        bs_table = bs_html.find_all('table')[11]
        # looping within rows
        for i_tr, tr in enumerate(bs_table.find_all('tr')):
            #   checking if is a header how, otherwise it is a data row
            if i_tr < 2:
                #   getting headers
                list_th.extend(
                    [
                        StrHandler()
                        .remove_diacritics(el.get_text())
                        .replace('\xa0', '')
                        .replace('Totais dos pregoes  Ref: ', '')
                        .replace('(R$)', 'BRL')
                        .replace(' - ', ' ')
                        .replace(' ', '_')
                        .strip()
                        .upper()
                        for el in tr.find_all('td')
                        if len(
                            StrHandler()
                            .remove_diacritics(el.get_text())
                            .replace('\xa0', '')
                        )
                        > 0
                    ]
                )
            else:
                #   getting data
                list_td.extend(
                    [
                        # data numeric
                        float(
                            td.get_text()
                            .strip()
                            .replace('.', '')
                            .replace(',', '.')
                        )
                        if NumHandler().is_numeric(
                            StrHandler()
                            .remove_diacritics(td.get_text())
                            .strip()
                            .replace('.', '')
                            .replace(',', '.')
                        )
                        # data not numeric
                        else StrHandler()
                        .remove_diacritics(td.get_text())
                        .strip()
                        .replace(' de ', ' ')
                        .replace(' do ', ' ')
                        .replace(' a ', ' ')
                        .replace(' e ', ' ')
                        .replace(' - ', ' ')
                        .replace('-', ' ')
                        .replace(' / ', ' ')
                        .replace(' ', '_')
                        .replace('.', '')
                        .replace(',', '.')
                        .replace('(', '')
                        .replace(')', '')
                        .replace('/', '')
                        .upper()
                        for td in tr.find_all('td')
                    ]
                )
        # dealing with header raw data
        list_th = [
            list_th[2],
            list_th[3] + '_' + list_th[0],
            list_th[4] + '_' + list_th[0],
            list_th[3] + '_' + list_th[1],
            list_th[4] + '_' + list_th[1],
        ]
        # pair headers and data within a list
        list_ser = HandlingDicts().pair_headers_with_data(list_th, list_td)
        # turning into dataframe
        df_bov_montlhy = pd.DataFrame(list_ser)
        # defining columns of month and last twelve months
        cols_month = list(df_bov_montlhy.columns)[0:3]
        col_l12m = [list(df_bov_montlhy.columns)[0]]
        col_l12m.extend(list(df_bov_montlhy.columns)[3:])
        # creating separate into two dataframes
        df_bov_mnt = df_bov_montlhy[cols_month]
        df_bov_l12m = df_bov_montlhy[cols_month]
        # renaming columns
        for df_ in [df_bov_mnt, df_bov_l12m]:
            #   list columns
            list_cols = list(df_.columns)
            #   renaming columns of interest
            df_.rename(
                columns={
                    list_cols[1]: YAML_B3['bov_trading_volume']['col_deals'],
                    list_cols[2]: YAML_B3['bov_trading_volume']['col_volume'],
                    list_cols[0]: YAML_B3['bov_trading_volume']['col_market'],
                },
                inplace=True,
            )
            #   adding column of time period
            df_[
                YAML_B3['bov_trading_volume']['col_time_period']
            ] = DatesBR().month_year_string(
                list_cols[1].replace('NEGOCIACOES_', '')
            )
            # adding logging
            df_ = DBLogs().audit_log(
                df_, url, list_cols[1].replace('NEGOCIACOES_', '')
            )
        # returning dataframe
        return df_bov_mnt, df_bov_l12m


class CalendarB3:
    def bl_weekly_option(
        self, ticker_opc: str, int_min_letters: int = 6
    ) -> bool:
        """
        DOCSTRING: BOOLEAN TO WEEKLY EXPIRING OPTIONS
        INPUTS: TICKER
        OUTPUTS: BOOLEAN
        """
        num_let = sum(c.isalpha() for c in ticker_opc)
        num_let += sum(c.isdigit() for c in ticker_opc[:4])
        return num_let >= int_min_letters

    @property
    def options_exercise_dates(self) -> pd.DataFrame:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # setting variables
        list_ser = list()
        # request html
        bs_html = HtmlHndler().html_bs_parser(
            YAML_B3['options_exercise_dates']['url'],
            bl_verify=YAML_B3['options_exercise_dates']['bl_verify'],
        )
        # looping within li tag
        for bs_li in bs_html.find_all('li'):
            #   getting month
            try:
                str_month = bs_li.find('a').get_text()
            except AttributeError:
                continue
            #   getting headers
            list_headers = [x.get_text().upper() for x in bs_li.find_all('th')]
            #   getting data
            list_data = [x.get_text() for x in bs_li.find_all('td')]
            #   checking headers and data list, in case of empty data continue
            if len(list_headers) == 0 or len(list_data) == 0:
                continue
            #   pair headers and data within a list
            list_ser = HandlingDicts().pair_headers_with_data(
                list_headers, list_data
            )
            #   adding month to serialized list
            list_ser = HandlingDicts().add_k_v_serialized_list(
                list_ser,
                YAML_B3['options_exercise_dates']['key_month'].upper(),
                str_month,
            )
            #   exporting to serialized list
            list_ser.extend(list_ser)
        # exporting serialized list to dataframe
        df_opt_xcr_dts = pd.DataFrame(list_ser)
        # adding request date
        df_opt_xcr_dts[
            YAML_B3['options_exercise_dates']['key_request_date'].upper()
        ] = DatesBR().curr_date
        # altering data types
        df_opt_xcr_dts = df_opt_xcr_dts.astype(
            {
                YAML_B3['options_exercise_dates']['key_day'].upper(): int,
                YAML_B3['options_exercise_dates']['key_details'].upper(): str,
                YAML_B3['options_exercise_dates']['key_month'].upper(): str,
            }
        )
        # adding logging
        df_opt_xcr_dts = DBLogs().audit_log(
            df_opt_xcr_dts,
            YAML_B3['options_exercise_dates']['url'],
            DatesBR().utc_log_ts,
        )
        # returning dataframe
        return df_opt_xcr_dts


class TradingHoursB3:
    def futures_generic(self, url: str, int_cols: int) -> list:
        """
        DOCSTRING: TRADING TIMES OF FUTURES REGARDING BRAZILLIAN PMI (IPCA) AND STOCK INDEXES
        INPUTS: -
        OUTPUTS:
        """
        # setting variables
        list_df = list()
        # request html
        bs_html = HtmlHndler().html_bs_parser(
            url, bl_verify=YAML_B3['trading_hours_b3']['futures']['bl_verify']
        )
        # looping within tables
        for _, bs_table in enumerate(bs_html.find_all('table')):
            #   reseting variables
            list_ser = list()
            #   creating headers' list
            #   auxiliary header 1
            list_headers_1 = [
                StrHandler()
                .remove_diacritics(bs_th.get_text())
                .upper()
                .replace('(', '')
                .replace(')', '')
                .replace(' ', '_')
                .replace('+', '')
                .replace('-', '_')
                .replace('_DE_', '_')
                for bs_th in bs_table.find('thead')
                .find_all('tr')[0]
                .find_all('th')
            ]
            #   checking wheter there is more than one column for the header
            try:
                #   auxiliary header 2
                list_headers_2 = [
                    StrHandler().remove_diacritics(bs_td.get_text()).upper()
                    for bs_td in bs_table.find('thead')
                    .find_all('tr')[1]
                    .find_all('td')
                ]
                #   consolidated header
                list_headers = [list_headers_1[0], list_headers_1[1]]
                for aux_ in range(2, int_cols):
                    if aux_ < 4:
                        list_headers.extend(
                            [
                                list_headers_2[2 * aux_ - 4]
                                + '_'
                                + list_headers_1[aux_],
                                list_headers_2[2 * aux_ - 3]
                                + '_'
                                + list_headers_1[aux_],
                            ]
                        )
                    elif aux_ == 4:
                        list_headers.extend(
                            [
                                list_headers_2[2 * aux_ - 4]
                                + '_'
                                + list_headers_1[aux_]
                            ]
                        )
                    else:
                        list_headers.extend(
                            [
                                list_headers_2[2 * aux_ - 5]
                                + '_'
                                + list_headers_1[aux_],
                                list_headers_2[2 * aux_ - 4]
                                + '_'
                                + list_headers_1[aux_],
                            ]
                        )
            except IndexError:
                list_headers = list_headers_1
            #   looping within rows
            for bs_row in bs_table.find('tbody').find_all('tr'):
                #   body
                list_tr_body = [
                    bs_td.get_text()
                    .replace('(1)', '')
                    .replace('(2)', '')
                    .replace('(3)', '')
                    .replace('(4)', '')
                    .replace('(5)', '')
                    .replace('(6)', '')
                    .replace('(7)', '')
                    .replace('\n', '')
                    for bs_td in bs_row.find_all('td')
                ]
                #   appending to serialized list
                list_ser.append(dict(zip(list_headers, list_tr_body)))
            #   creating dataframe
            df_ = pd.DataFrame(list_ser)
            #   appending to list
            list_df.append(df_)
            #   adding logging
            df_ = DBLogs().audit_log(df_, url, DatesBR().utc_log_ts)
        # returning dataframes
        return list_df

    @property
    def futures_pmi_idx(self) -> list:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.futures_generic(
            YAML_B3['trading_hours_b3']['futures']['url_pmi_idx'],
            YAML_B3['trading_hours_b3']['futures']['num_cols_pmi_idx'],
        )

    @property
    def futures_brl_usd_int_rts(self) -> list:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.futures_generic(
            YAML_B3['trading_hours_b3']['futures']['url_int_rts'],
            YAML_B3['trading_hours_b3']['futures']['num_cols_int_rts'],
        )

    @property
    def futures_commodities(self) -> list:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS
        """
        return self.futures_generic(
            YAML_B3['trading_hours_b3']['futures']['url_commodities'],
            YAML_B3['trading_hours_b3']['futures']['num_cols_commodities'],
        )

    @property
    def futures_crypto(self) -> list:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.futures_generic(
            YAML_B3['trading_hours_b3']['futures']['url_crypto'],
            YAML_B3['trading_hours_b3']['futures']['num_cols_crypto'],
        )

    @property
    def futures_forex(self) -> list:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS
        """
        return self.futures_generic(
            YAML_B3['trading_hours_b3']['futures']['url_forex'],
            YAML_B3['trading_hours_b3']['futures']['num_cols_forex'],
        )

    @property
    def futures_otc(self) -> list:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.futures_generic(
            YAML_B3['trading_hours_b3']['futures']['url_otc'],
            YAML_B3['trading_hours_b3']['futures']['num_cols_otc'],
        )

    @property
    def futures_opf_bef_aft_xrc(self) -> list:
        """
        DOCSTRING: OPTIONS ON FUTURES - BEFORE AND AFTER EXERCISE
        INPUTS: -
        OUTPUTS:
        """
        return self.futures_generic(
            YAML_B3['trading_hours_b3']['futures']['url_opf'],
            YAML_B3['trading_hours_b3']['futures']['num_cols_opf'],
        )

    @property
    def stocks(self) -> list:
        """
        DOCSTRING: TRADING TIMES OF BRAZILLIAN STOCKS
        INPUTS: -
        OUTPUTS:
        """
        # setting variables
        list_df = list()
        # request html
        bs_html = HtmlHndler().html_bs_parser(
            YAML_B3['trading_hours_b3']['stocks']['url'],
            bl_verify=YAML_B3['trading_hours_b3']['stocks']['bl_verify'],
        )
        # looping within tables
        for i, bs_table in enumerate(bs_html.find_all('table')):
            #   reseting variables
            list_ser = list()
            #   CREATING HEADRERS' LIST
            #   TABLE 0
            if i == 0:
                #   auxiliary header 1
                list_headers_1 = [
                    StrHandler().remove_numeric_chars(
                        StrHandler()
                        .remove_diacritics(bs_th.get_text())
                        .upper()
                        .replace(' ', '_')
                        .replace('-', '_')
                        .replace('_DE_', '_')
                    )
                    for bs_th in bs_table.find_all('th')
                    if StrHandler().find_substr_str(
                        StrHandler()
                        .remove_diacritics(bs_th.get_text())
                        .upper()
                        .replace(' ', '_')
                        .replace('-', '_')
                        .replace('_DE_', ''),
                        '*AFTER_MARKET*',
                    )
                    == False
                ]
                list_headers_1.remove('AFTER_MARKET')
                list_headers_1[-1] = list_headers_1[-1] + '_AFTER_MARKET'
                list_headers_1[-2] = list_headers_1[-2] + '_AFTER_MARKET'
                #   auxiliary header 2
                list_headers_2 = [
                    StrHandler().remove_numeric_chars(
                        StrHandler()
                        .remove_diacritics(bs_td.get_text())
                        .upper()
                    )
                    for bs_td in bs_table.find('tbody')
                    .find('tr')
                    .find_all('td')
                ]
                #   consolidating headers list
                list_headers = [list_headers_1[0]]
                for aux_ in range(1, 8):
                    list_headers.extend(
                        [
                            list_headers_2[2 * aux_ - 1]
                            + '_'
                            + list_headers_1[aux_],
                            list_headers_2[2 * aux_]
                            + '_'
                            + list_headers_1[aux_],
                        ]
                    )
            #   TABLE 1
            if i == 1:
                #   auxiliary header 1
                list_headers_1 = [
                    StrHandler().remove_numeric_chars(
                        StrHandler()
                        .remove_diacritics(bs_th.get_text())
                        .upper()
                        .replace(' ', '_')
                        .replace('-', '_')
                        .replace('_DO_', '_')
                    )
                    for bs_th in bs_table.find('thead')
                    .find_all('tr')[1]
                    .find_all('th')
                ]
                #   auxiliary header 2
                list_headers_2 = [
                    StrHandler().remove_numeric_chars(
                        StrHandler()
                        .remove_diacritics(bs_th.get_text())
                        .upper()
                        .replace(' ', '_')
                        .replace('-', '_')
                        .replace('_DO_', '_')
                        .replace('_DE_', '_')
                    )
                    for bs_th in bs_table.find('thead')
                    .find_all('tr')[2]
                    .find_all('th')
                ]
                #   auxiliary header 3
                list_headers_3 = [
                    StrHandler()
                    .remove_diacritics(bs_td.get_text())
                    .upper()
                    .replace('\xa0', '')
                    for bs_td in bs_table.find('tbody')
                    .find_all('tr')[0]
                    .find_all('td')
                ]
                #   consolidating headers list
                list_headers = [
                    'MERCADO',
                    list_headers_3[1]
                    + '_'
                    + list_headers_2[0]
                    + '_'
                    + list_headers_1[0],
                    list_headers_3[2]
                    + '_'
                    + list_headers_2[0]
                    + '_'
                    + list_headers_1[0],
                    list_headers_3[3]
                    + '_'
                    + list_headers_2[1]
                    + '_'
                    + list_headers_1[1],
                    list_headers_3[4]
                    + '_'
                    + list_headers_2[1]
                    + '_'
                    + list_headers_1[1],
                    list_headers_2[2] + '_' + list_headers_1[1],
                    list_headers_3[6]
                    + '_'
                    + list_headers_2[3]
                    + '_'
                    + list_headers_1[1],
                    list_headers_3[7]
                    + '_'
                    + list_headers_2[3]
                    + '_'
                    + list_headers_1[1],
                    list_headers_3[8]
                    + '_'
                    + list_headers_2[4]
                    + '_'
                    + list_headers_1[1],
                ]
            else:
                raise Exception(
                    'Table {} not expected, please validate.'.format(i)
                )
            #   looping within rows
            for bs_row in bs_table.find('tbody').find_all('tr')[1:]:
                #   body
                list_tr_body = [
                    bs_td.get_text()
                    .replace(r'\xa', '')
                    .replace('\xa0', '')
                    .replace('\n', '')
                    .replace('\t', '')
                    .replace('(1)', '')
                    .replace('(2)', '')
                    for bs_td in bs_row.find_all('td')
                ]
                #   appending to serialized list
                list_ser.append(dict(zip(list_headers, list_tr_body)))
            #   creating dataframe
            df_ = pd.DataFrame(list_ser)
            #   appending to list
            list_df.append(df_)
            #   adding logging
            df_ = DBLogs().audit_log(
                df_,
                YAML_B3['trading_hours_b3']['stocks']['url'],
                DatesBR().utc_log_ts,
            )
        # creating individual dataframes
        df_tt_bov = list_df[0]
        df_tt_exc_opt = list_df[1]
        # returning dataframes of interest
        return df_tt_bov, df_tt_exc_opt


class TheorPortfB3:
    def generic_req(
        self,
        str_indice: str,
        method: str = 'GET',
        float_pct_factor: float = 100.0,
    ) -> pd.DataFrame:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # requesting data
        resp_req = request(
            method,
            YAML_B3['theor_port_b3']['url_{}'.format(str_indice)],
            verify=YAML_B3['theor_port_b3']['bl_verify'],
        )
        # raising exception in case of status code different from 2xx
        resp_req.raise_for_status()
        # requesting json
        json_ibov = resp_req.json()
        # building serialized list
        list_ser = [
            {
                YAML_B3['theor_port_b3']['col_ticker']: dict_[
                    YAML_B3['theor_port_b3']['key_code']
                ],
                YAML_B3['theor_port_b3']['col_asset']: dict_[
                    YAML_B3['theor_port_b3']['key_asset']
                ],
                YAML_B3['theor_port_b3']['col_type']: dict_[
                    YAML_B3['theor_port_b3']['key_type']
                ],
                YAML_B3['theor_port_b3']['col_pct']: float(
                    dict_[YAML_B3['theor_port_b3']['key_part']].replace(
                        ',', '.'
                    )
                )
                / float_pct_factor,
                YAML_B3['theor_port_b3']['col_theor_qty']: dict_[
                    YAML_B3['theor_port_b3']['key_theor_qty']
                ].replace('.', ''),
            }
            for dict_ in json_ibov['results']
        ]
        # defining dataframe
        df_ = pd.DataFrame(list_ser)
        # changing data types
        df_ = df_.astype(
            {
                YAML_B3['theor_port_b3']['col_ticker']: str,
                YAML_B3['theor_port_b3']['col_asset']: str,
                YAML_B3['theor_port_b3']['col_type']: str,
                YAML_B3['theor_port_b3']['col_pct']: float,
                YAML_B3['theor_port_b3']['col_theor_qty']: float,
            }
        )
        # adding logging
        df_ = DBLogs().audit_log(
            df_,
            YAML_B3['theor_port_b3']['url_{}'.format(str_indice)],
            DatesBR().utc_log_ts,
        )
        # returning dataframe
        return df_

    @property
    def ibov(self) -> pd.DataFrame:
        """'
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.generic_req('ibov')

    @property
    def ibra(self) -> pd.DataFrame:
        """'
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.generic_req('ibra')

    @property
    def ibrx100(self) -> pd.DataFrame:
        """'
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.generic_req('ibrx100')

    @property
    def ibrx50(self) -> pd.DataFrame:
        """'
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.generic_req('ibrx50')


class MDB3:
    @property
    def financial_indicators_b3(
        self,
        method: str = 'GET',
        float_pct_factor: float = 100.0,
        dt_input_fmt: str = 'DD/MM/YYYY',
    ) -> pd.DataFrame:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # requesting financial indicators b3
        resp_req = request(method, YAML_B3['financial_indicators']['url'])
        # raise exception if status code is different from 2xx
        resp_req.raise_for_status()
        # appeding json to dataframe
        df_fin_ind = pd.DataFrame(resp_req.json())
        # changing column types
        df_fin_ind = df_fin_ind.astype(
            {
                YAML_B3['financial_indicators']['col_si_code']: np.int64,
                YAML_B3['financial_indicators']['col_desc']: str,
                YAML_B3['financial_indicators']['col_grp_desc']: str,
                YAML_B3['financial_indicators']['col_value']: str,
                YAML_B3['financial_indicators']['col_rate']: str,
                YAML_B3['financial_indicators']['col_last_up']: str,
            }
        )
        df_fin_ind[YAML_B3['financial_indicators']['col_last_up']] = [
            DatesBR().str_date_to_datetime(x, dt_input_fmt)
            if x is not math.nan
            else 0
            for x in df_fin_ind[YAML_B3['financial_indicators']['col_last_up']]
        ]
        for col_ in [
            YAML_B3['financial_indicators']['col_value'],
            YAML_B3['financial_indicators']['col_rate'],
        ]:
            df_fin_ind[col_] = [
                float(x.replace(',', '.')) if x != '' else 0
                for x in df_fin_ind[col_]
            ]
        df_fin_ind[YAML_B3['financial_indicators']['col_rate']] = [
            #   selic rate
            (
                row[YAML_B3['financial_indicators']['col_rate']]
                + YAML_B3['financial_indicators']['float_pct_dif_cdi_selic']
            )
            / float_pct_factor
            if row[YAML_B3['financial_indicators']['col_si_code']]
            == YAML_B3['financial_indicators']['int_si_code_selic']
            #   other rates
            else row[YAML_B3['financial_indicators']['col_rate']]
            / float_pct_factor
            for _, row in df_fin_ind.iterrows()
        ]
        # adding logging
        df_fin_ind = DBLogs().audit_log(
            df_fin_ind,
            YAML_B3['financial_indicators']['url'],
            DatesBR().utc_from_dt(DatesBR().curr_date),
        )
        # returning dataframe
        return df_fin_ind

    @property
    def securities_volatility(
        self, method: str = 'GET', float_pct_factor: float = 100.0
    ) -> pd.DataFrame:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # setting variables
        list_ser = list()
        # looping through page sets
        for _, dict_ in YAML_B3['securities_volatility_b3']['pages'].items():
            #   looping through hashes
            for str_hash in dict_['hashes']:
                #   request security volatility page
                resp_req = request(method, dict_['url'].format(str_hash))
                #   raise exception for status different from 2xx
                resp_req.raise_for_status()
                #   get json in memory
                json_ = resp_req.json()
                #   reseting json with results
                list_ = json_['results']
                #   creating serialized list
                for k, v in [
                    (
                        YAML_B3['securities_volatility_b3']['cols_names'][
                            'pg_num'
                        ],
                        json_['page']['pageNumber'],
                    ),
                    (
                        YAML_B3['securities_volatility_b3']['cols_names'][
                            'total_pgs'
                        ],
                        json_['page']['totalPages'],
                    ),
                    (
                        YAML_B3['securities_volatility_b3']['cols_names'][
                            'date_ref'
                        ],
                        DatesBR().curr_date,
                    ),
                    (
                        YAML_B3['securities_volatility_b3']['cols_names'][
                            'url'
                        ],
                        dict_['url'],
                    ),
                ]:
                    list_ = HandlingDicts().add_k_v_serialized_list(
                        list_, k, v
                    )
                #   extend to serialized list
                list_ser.extend(list_)
        # adding to pandas dataframe
        df_sec_vol = pd.DataFrame(list_ser)
        # changing column types
        df_sec_vol = df_sec_vol.astype(
            {
                YAML_B3['securities_volatility_b3']['cols_names'][
                    'ticker'
                ]: str,
                YAML_B3['securities_volatility_b3']['cols_names'][
                    'company_name'
                ]: str,
                YAML_B3['securities_volatility_b3']['cols_names'][
                    'serie'
                ]: str,
                YAML_B3['securities_volatility_b3']['cols_names'][
                    'pg_num'
                ]: int,
                YAML_B3['securities_volatility_b3']['cols_names'][
                    'total_pgs'
                ]: int,
            }
        )
        #   looping within periods of security volatility calculed by b3
        for int_per in YAML_B3['securities_volatility_b3']['vols_calc_per']:
            #   loop through standard deviation and annual volatility for the giver period
            for col_ in [
                YAML_B3['securities_volatility_b3']['cols_names'][
                    'std'
                ].format(str(int_per)),
                YAML_B3['securities_volatility_b3']['cols_names'][
                    'ann_vol'
                ].format(str(int_per)),
            ]:
                #   alter dataframe column types
                df_sec_vol[col_] = [
                    float(str(d).replace(',', '.')) / float_pct_factor
                    for d in df_sec_vol[col_]
                ]
        # adding logging
        df_sec_vol = DBLogs().audit_log(df_sec_vol, None, DatesBR().utc_log_ts)
        # returning dataframe
        return df_sec_vol


class MDYFinance:
    def historical_data_securities_yq(
        self,
        dt_inf: datetime,
        dt_sup: datetime,
        list_securities: Optional[list] = None,
        list_indexes: Optional[list] = None,
        bl_verify: bool = False,
        column_ticker: str = 'ticker',
        colum_dt_date: str = 'dt_date',
    ) -> pd.DataFrame:
        """
        DOCSTRING:
        INPTUS:
        OUTPUTS:
        """
        # dealing with securities list_tickers
        if (list_securities == None) or (list_securities == []):
            list_securities = []
        else:
            # list_securities = ['{}.SA'.format(str(x).upper()) for x in list_securities]
            list_securities = [str(x).upper() for x in list_securities]
        # dealing with indexes list_tickers
        if (list_indexes == None) or (list_indexes == []):
            list_indexes = []
        else:
            list_indexes = ['^{}'.format(str(x).upper()) for x in list_indexes]
        # extending stock's list
        list_tickers = HandlingLists().extend_lists(
            list_indexes, list_securities
        )
        # removing duplicates
        list_tickers = HandlingLists().remove_duplicates(list_tickers)
        # getting historical data
        list_yq_data = Ticker(list_tickers, verify=bl_verify)
        df_yq_data = list_yq_data.history(start=dt_inf, end=dt_sup)
        # index to column - ticker
        df_yq_data[column_ticker] = [
            str(x[0]).replace('^', '').replace('.SA', '')
            for x in df_yq_data.index
        ]
        # index to column - date in datetime format
        df_yq_data[colum_dt_date] = [x[1] for x in df_yq_data.index]
        # filling nan values with upper data
        df_yq_data[column_ticker].fillna(method='ffill', inplace=True)
        # retrieving historical data
        return df_yq_data

    def daily_returns(
        self,
        df_yq_data: pd.DataFrame,
        column_symbol: str = 'symbol',
        col_ticker: str = 'ticker',
        col_close: str = 'adjclose',
        col_open: str = 'open',
        col_date: str = 'dt_date',
        col_daily_return: str = 'daily_return',
        str_type_return_calc: str = 'close_close',
    ) -> pd.DataFrame:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # order by ticker/date in ascending order
        df_yq_data.sort_values(
            by=[col_ticker, col_date], ascending=[True, True], inplace=True
        )
        # daily returns grouped by symbol
        if str_type_return_calc == 'close_close':
            df_yq_data[col_daily_return] = (
                df_yq_data.groupby(col_ticker)[col_close]
                .apply(lambda x: np.log(x / x.shift(1)))
                .reset_index(0, drop=True)
            )
        elif str_type_return_calc == 'open_close':
            df_yq_data[col_daily_return] = (
                df_yq_data.groupby(col_ticker)
                .apply(
                    lambda group: np.log(
                        group[col_open] / group[col_close].shift(1)
                    )
                )
                .reset_index(level=0, drop=True)
            )
        else:
            raise Exception(
                'Type of return calculation not supported. '
                + f'TYPE: {str_type_return_calc}'
            )
        # optionally reset nulls when switching symbols for clarity
        df_yq_data.loc[
            df_yq_data.groupby(level=column_symbol).head(1).index,
            col_daily_return,
        ] = None
        # returning dataframe with daily returns
        return df_yq_data


class MDInvestingDotCom:
    def ticker_reference_investing_com(
        self,
        str_ticker: str,
        str_host: str = 'https://tvc4.investing.com/725910b675af9252224ca6069a1e73cc/1631836267/1/1/8/symbols?symbol={}',
        str_method: str = 'GET',
        bl_verify=True,
        key_ticker='ticker',
        dict_headers: dict = {
            'User-Agent': 'Mozilla/5.0 (Windowns NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36'
        },
    ) -> json:
        """
        DOCSTRING: TICKER REFERENCE FROM INVESTING.COM
        INPUTS: TICKER, HOST (DEFAULT), METHOD (DEFAULT), BOOLEAN VERIFY (DEFAULT),
            KEY TICKER (DEFAULT), HEADERS (DEFAULT)
        OUTPUTS: STRING
        """
        # collect content from rest
        req_resp = request(
            str_method,
            str_host.format(str_ticker),
            verify=bl_verify,
            headers=dict_headers,
        )
        req_resp.raise_for_status()
        # turning to desired format - loading json to memopry
        json_ = req_resp.json()
        # return named ticker form investing.com
        return json_[key_ticker]

    def historical_closing_intraday_data_investing_com(
        self,
        str_ticker_ref: str,
        dt_inf: datetime,
        dt_sup: datetime,
        str_type_closing_intraday: str = 'D',
        str_host: str = 'https://tvc4.investing.com/725910b675af9252224ca6069a1e73cc/'
        + '1631836267/1/1/8/history?symbol={}&resolution={}&from={}&to={}',
        str_method: str = 'GET',
        dict_headers: dict = {
            'User-Agent': 'Mozilla/5.0 (Windowns NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36'
        },
    ) -> json:
        """
        DOCSTRING: HISTORICAL CLOSING/INTRADAY TICKS FROM INVESTING.COM
        INPUTS: TICKER REFERENCE FROM INVESTING.COM, FROM DATE (TIMESTAMP), TO DATE (TIMESTAMP),
            TYPE CLOSING/INTRADAY PRICE (D AS DEFAULT FOR CLOSING), HOST (DEFAULT) AND HEADERS
        OUTPUTS: JSON
        """
        # collect content from rest
        req_resp = request(
            str_method,
            str_host.format(
                str_ticker_ref,
                str_type_closing_intraday,
                dt_inf.strftime('%Y-%m-%d'),
                dt_sup.strftime('%Y-%m-%d'),
            ),
            headers=dict_headers,
        )
        req_resp.raise_for_status()
        # turning to desired format - loading json to memory
        json_ = req_resp.json()
        # return named ticker from investing.com
        return json_

    def historical_closing_data_yf(
        self,
        list_tickers: List[str],
        str_period: str = 'max',
        col_date: str = 'Date',
        col_ticker: str = 'Ticker',
    ) -> pd.DataFrame:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS
        """
        # setting variables
        list_ser = list()
        # looping through list_tickers and collecting historical data
        for ticker in list_tickers:
            #   creating yfinance object for the current ticker
            obj_yf = yf.Ticker(ticker)
            #   collecting historical data
            df_hist_closing_data = obj_yf.history(period=str_period)
            #   creating date column, which is the index
            df_hist_closing_data[col_date] = df_hist_closing_data.index
            #   creating column with ticker name
            df_hist_closing_data[col_ticker] = ticker
            #   appending to exporting list of dictionaries, which will be converted into a dataframe
            list_ser.extend(df_hist_closing_data.to_dict(orient='records'))
        # alterando lista de dicionários para dataframe
        df_closing_data = pd.DataFrame(list_ser)
        # alterando tipo de coluna de data para string
        df_closing_data[col_date] = [
            StrHandler().get_string_until_substring(str(x), ' ')
            for x in df_closing_data[col_date]
        ]
        # alterando string para tipo de data
        df_closing_data[col_date] = [
            DatesBR().str_date_to_datetime(x, 'YYYY-MM-DD')
            for x in df_closing_data[col_date]
        ]
        # returning data of interest
        return df_closing_data

    def close_price_hist(
        self,
        list_tickers: List[str],
        dt_inf: datetime,
        dt_sup: datetime,
        str_country: str = 'brazil',
        str_assets_class: str = 'acao',
    ) -> pd.DataFrame:
        """
        DOCSTRING: HISTORIC CLOSING PRICES FROM INVESTING.COM
        INPUTS: TICKERS, DATA INFERIOR, DATA SUPERIOR, PAÍS (BRAZIL COMO DEFAULT) COM
            FORMATO DATA 'DD/MM/YYYY'
        OUTPUTS: JSON
        """
        # setting variables
        dict_close = dict()
        # filling list of available assets
        if str_assets_class == 'acao' and type(str_assets_class) == str:
            for ticker in list_tickers:
                json_ = HandlingObjects().literal_eval_data(
                    investpy.get_stock_historical_data(
                        ticker, str_country, dt_inf, dt_sup, as_json=True
                    )
                )
                dict_close[ticker] = json_
        else:
            if type(str_assets_class) == str:
                str_assets_class = [str_assets_class]
                str_assets_class.extend(
                    (len(list_tickers) - 1) * [str_assets_class]
                )
            dict_ticker_name = dict()
            for i in range(len(list_tickers)):
                if str_assets_class[i] == 'etf':
                    for dict_ativo in investpy.get_etfs_dict(str_country):
                        if dict_ativo['symbol'] == list_tickers[i]:
                            dict_ticker_name[list_tickers[i]] = dict_ativo[
                                'full_name'
                            ]
                            break
                    else:
                        dict_ticker_name[
                            list_tickers[i]
                        ] = 'Ticker não cadastrado no serviço de cotações investing.com'
                    json_ = investpy.get_etf_historical_data(
                        dict_ticker_name[list_tickers[i]],
                        str_country,
                        dt_inf,
                        dt_sup,
                        as_json=True,
                    )
                    dict_close[
                        list_tickers[i]
                    ] = HandlingObjects().literal_eval_data(json_)
                elif str_assets_class[i] == 'acao':
                    json_ = investpy.get_stock_historical_data(
                        list_tickers[i],
                        str_country,
                        dt_inf,
                        dt_sup,
                        as_json=True,
                    )
                    dict_close[
                        list_tickers[i]
                    ] = HandlingObjects().literal_eval_data(json_)
        return pd.DataFrame(dict_close)

    def idx_hist_data(
        self,
        dt_inf: datetime,
        dt_sup: datetime,
        str_idx: str = 'Bovespa',
        str_country: str = 'brazil',
    ) -> pd.DataFrame:
        """
        DOCSTRING: CLOSING PRICE OF A GIVEN INDICE
        INPUTS: DATE INFERIOR, SUPERIOR, INDICE NAME (DEFAULT BOVESPA),
            COUNTRY (DEFAULT BRAZIL), FORMAT OF EXTRACTION (DEFAULT JSON2)
        OUTPUTS: DICT WITH CLOSE PRICE
        """
        return pd.DataFrame(
            [
                {d['date']: d['close']}
                for d in HandlingObjects().literal_eval_data(
                    investpy.indices.get_index_historical_data(
                        str_idx, str_country, dt_inf, dt_sup, as_json=True
                    )
                )['historical']
            ]
        )


class MDComDinheiro:
    def __init__(self, user: str, passw: str) -> None:
        self.user = user
        self.passw = passw

    def bmf_historical_close_data(
        self,
        str_ticker: str,
        str_maturity_code: str,
        dt_inf: datetime,
        dt_sup: datetime,
        str_fmt_extract: str = 'json2',
    ) -> json:
        """
        DOCSTRING: CLOSING PRICE OF BMF CONTRACTS
        INPUTS: USERNAME (COMDINHEIRO), PASSWORD (COMDINHEIRO), CONTRACT CODE, MATURITY CODE,
            DATE INFERIOR (DDMMAAAA, AS A STRING, OR DATETIME FORMAT), DATE SUPERIOR (SAME FORMAT
            AS DATE INFERIOR) AND FORMAT EXTRACTION (JSON AS DEFAULT)
        OUTPUTS: JSON
        """
        # applying date format
        if DatesBR().check_date_datetime_format(dt_inf) == True:
            dt_inf = DatesBR().datetime_to_string(dt_inf, '%d%m%Y')
        if DatesBR().check_date_datetime_format(dt_sup) == True:
            dt_sup = DatesBR().datetime_to_string(dt_sup, '%d%m%Y')
        # str_payload - self.user / password / str_ticker name / inferior date / superior date / maturity
        #   code / format
        str_payload = str(
            f'username={self.user}&password={self.passw}&URL=HistoricoCotacaoBMF'
            + f'-{str_ticker}-{dt_inf}-{dt_sup}-{str_maturity_code}-1&format={str_fmt_extract}'
        )
        # sending rest resp_req
        json_ = ComDinheiro().requests_api_cd(str_payload).text.encode('utf8')
        json_ = (
            HandlingObjects()
            .literal_eval_data(json_, "b'", "'")
            .replace(r'\n', '')
        )
        return json_

    def indice_neg(
        self, list_tickers: List[str], dt_inf: datetime, dt_sup: datetime
    ) -> json:
        """
        DOCSTRING: FUNÇÃO PARA TRAZER O ÍNDICE DE NEGOCIABILIDADE DA B3
        INPUTS: ATIVO, DATA INICIAL E DATA FINAL ('DD/MM/AAAA')
        OUTPUTS: JSON COM O VALOR DO ÍNDICE DE NEGOCIABILIDADE NO PERÍODO
        """
        # definindo variáveis para pool de conexão
        if len(list_tickers) > 1:
            str_list_tickers = '%2B'.join(list_tickers)
        else:
            str_list_tickers = ''.join(list_tickers)
        dt_inf = dt_inf.strftime('%d~%m~%Y')
        dt_sup = dt_sup.strftime('%d~%m~%Y')
        str_payload = (
            'username={}&password={}&URL=ComparaEmpresas001.php%3F%26d'.format(
                self.user, self.passw
            )
            + 'ata_d%3D31129999%26data_a%3D16%2F06%2F2020%26trailing%3D12%26conv%3DMIXED'
            + '%26c_c%3Dconsolidado%2520preferencialmente%26moeda%3DMOEDA_ORIGINAL'
            + '%26m_m%3D1000000000%26n_c%3D2%26f_v%3D1%26'
            + 'papeis%3D'
            + str_list_tickers
            + '%26indic%3DNEGOCIABILIDADE('
            + dt_inf
            + '%2C'
            + dt_sup
            + '%2C%2C%2C2)'
            + '%26enviar_email%3D0%26enviar_email_log%3D0%26transpor%3D0%26op01%3D'
            + 'tabela%26oculta_cabecalho_sup%3D0%26relat_alias_automatico%3'
            + 'DcMDalias_01&format=json2'
        )
        # fetching data
        resp_req = ComDinheiro().requests_api_cd(str_payload)
        json_ = ast.literal_eval(
            StrHandler()
            .find_between(str(resp_req.text.encode('utf8')), "b'", "'")
            .replace(r'\n', '')
        )
        return JsonFiles().send_json(json_)

    def infos_sectors(self, dt_: datetime) -> json:
        """
        DOCSTRING: SECTOR INFORMATION REGARDING COMPANIES
        INPUTS: DATA PREGÃO DE INTERESSE
        OUTPUTS: JSON
        """
        # definindo variável para pool de conexão
        str_payload = (
            'username={}&password={}&URL=StockScreenerFull.php%3F%26'.format(
                self.user, self.passw
            )
            + 'relat%3D%26data_analise%3D{}%2F{}%2F{}%26data_dem%3D31%2F12%2F'.format(
                dt_.strftime('%d'), dt_.strftime('%m'), dt_.strftime('%Y')
            )
            + '9999%26variaveis%3DTICKER%2BNOME_EMPRESA%2BDATA_REGISTRO%2BSEGMENTO%2BSETOR%'
            + '2BSUBSETOR%2BSUBSUBSETOR%2BTIPO_BOVESPA({}~{}~{}%2CTODOS%2C%2C)'.format(
                dt_.strftime('%d'), dt_.strftime('%m'), dt_.strftime('%Y')
            )
            + '%26segmento%3Dtodos%26setor%3Dtodos%26filtro%3D'
            + '%26demonstracao%3Dconsolidado%2520preferencialmente%26tipo_acao%3D'
            + 'Todas%26convencao%3DMIXED%26acumular%3D12%26valores_em%3D1%26num_casas'
            + '%3D2%26salve%3D%26salve_obs%3D%26var_control%3D0%26overwrite%3D0%26'
            + 'setor_bov%3Dtodos%26subsetor_bov%3Dtodos%26subsubsetor_bov%3Dtodos%26'
            + 'group_by%3D%26relat_alias_automatico%3DcMDalias_01%26'
            + 'primeira_coluna_ticker%3D0%26periodos%3D0%26periodicidade%3Danual%26'
            + 'formato_data%3D1&format=json2'
        )
        # fetching data
        resp_req = ComDinheiro().requests_api_cd(str_payload)
        json_ = ast.literal_eval(
            StrHandler()
            .find_between(str(resp_req.text.encode('utf8')), "b'", "'")
            .replace(r'\n', '')
        )
        return JsonFiles().send_json(json_)

    @property
    def corporate_events(self) -> json:
        """
        DOCSTRING: RETORNA INFORMAÇÕES CORPORATIVAS: DIVULGAÇÃO DE RESULTADOS,
        INPUTS: DATA PREGÃO DE INTERESSE
        OUTPUTS: JSON
        """
        # definindo variável para pool de conexão
        str_payload = (
            'username={}&password={}&URL=StockScreenerFull.php'.format(
                self.user, self.passw
            )
            + '%3F%26relat%3D%26data_analise%3D18%2F06%2F2020%26data_dem%3D'
            + '31%2F12%2F9999%26variaveis%3DTICKER%2BDATA_ENTREGA_DEM_PRIM%'
            + '26segmento%3Dtodos%26setor%3Dtodos%26filtro%3D%26demonstracao%3D'
            + 'consolidado%2520preferencialmente%26tipo_acao%3DTodas%26convencao%3D'
            + 'MIXED%26acumular%3D12%26valores_em%3D1%26num_casas%3D2%26salve%3D%26'
            + 'salve_obs%3D%26var_control%3D0%26overwrite%3D0%26setor_bov%3Dtodos%26'
            + 'subsetor_bov%3Dtodos%26subsubsetor_bov%3Dtodos%26group_by%3D%26'
            + 'relat_alias_automatico%3DcMDalias_01%26primeira_coluna_ticker%3D0%26'
            + 'periodos%3D0%26periodicidade%3Danual%26formato_data%3D1&format=json2'
        )
        # fetching data
        resp_req = ComDinheiro().requests_api_cd(str_payload)
        json_ = ast.literal_eval(
            StrHandler()
            .find_between(str(resp_req.text.encode('utf8')), "b'", "'")
            .replace(r'\n', '')
        )
        return JsonFiles().send_json(json_)

    def trading_infos(self, dt_inf: datetime, dt_sup: datetime) -> json:
        """
        DOCSTRING: RETURN INFORMATION ABOUT THE AMOUNT OF TRADES, LIQUIDITY ON THE STOCK EXCHANGE,
            MARKET VALUE, TRADING VOLUME, WEIGHT IN THE IBRX100 INDEX, BTC FOR PAPER,
            PURCHASES AND SALES OF FUNDS, FOR STOCKS IN THE IBOV
        INPUTS: DATES (INFERIOR AND SUPERIOR)
        OUTPUTS: JSON
        """
        # definindo variável para pool de conexão
        str_payload = (
            'username={}&password={}&URL=StockScreenerFull.php'.format(
                self.user, self.passw
            )
            + '%3F%26relat%3D%26data_analise%3D{}%2F{}%2F{}%26data_dem%3D31'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
            )
            + '%2F12%2F9999%26variaveis%3DTICKER%2B'
            + 'VOLUME_MEDIO({}~{}~{}%2C{}~{}~{}%2C)'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '%2BQUANT_NEGOCIOS({}~{}~{}%2C{}~{}~{}%2C%2Cmedia)%2'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + 'BMARKET_VALUE%2B'
            + '%2BLIQUIDEZ_BOLSA({}~{}~{}%2C{}~{}~{}%2C%2C)%2'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + 'BNEGOCIABILIDADE({}~{}~{}%2C{}~{}~{}%2C%2C%2C2)%2'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + 'BPESO_INDICE(participacao%2CIBRX%2C{}~{}~{}%2C%2C)%2B'.format(
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + 'BTC_ALUGUEL_ACOES(TV%2C{}~{}~{}%2C{}~{}~{})%2B'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + 'COMPRAS_VENDAS_FUNDOS(final_valor%2C{}~{}~{}%2C%2C0)'.format(
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '%2BPRECO_AJ({}~{}~{}%2C%2C%2CA%2CC)'.format(
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '%2BDY_12M%26'
            + 'segmento%3Dtodos%26setor%3Dtodos%26filtro%3D%26demonstracao%3D'
            + 'consolidado%2520preferencialmente%26tipo_acao%3DTodas%26convencao%3D'
            + 'MIXED%26acumular%3D12%26valores_em%3D1%26num_casas%3D2%26salve%3D%26'
            + 'salve_obs%3D%26var_control%3D0%26overwrite%3D0%26setor_bov%3Dtodos%26'
            + 'subsetor_bov%3Dtodos%26subsubsetor_bov%3Dtodos%26group_by%3D%26'
            + 'relat_alias_automatico%3DcMDalias_01%26primeira_coluna_ticker%3D'
            + '0%26periodos%3D0%26periodicidade%3Danual%26formato_data%3D1%26'
            + 'formato_data%3D1&format=json2'
        )
        # fetching data
        resp_req = ComDinheiro().requests_api_cd(str_payload)
        json_ = ast.literal_eval(
            StrHandler()
            .find_between(str(resp_req.text.encode('utf8')), "b'", "'")
            .replace(r'\n', '')
        )
        return JsonFiles().send_json(json_)

    def infos_risco(self, dt_inf: datetime, dt_sup: datetime) -> json:
        """
        DOCSTRING: RETURNS INFORMATION ABOUT 60 MONTHS ANNUALIZED VOLATILITY, VOLATILITY
            ANNUALIZED MONTHLY, YTD ANNUALIZED VOLATILITY, PARAMETRIC VAR, EWMA,
            BENCHMARK VAR PARAMETRIC (IN RELATION TO IBOVESPA), MAXIMUM DRAWDOWN
            OF STOCKS IN THE IBOV
        INPUTS: DATES (INFERIOR AND SUPERIOR)
        OUTPUTS: JSON
        """
        # definindo variável para pool de conexão
        str_payload = (
            'username={}&password={}&URL=StockScreenerFull.php%3F%26'.format(
                self.user, self.passw
            )
            + 'relat%3D%26data_analise%3D{}%2F{}%2F{}%26data_dem%3D31%2F12%2F9999'.format(
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '%26variaveis%3DTICKER%2Bvol_ano_60m%2Bvol_ano_mes_atual%2B'
            + 'vol_ano_ano_atual%2BVAR_PAR(d%2C{}~{}~{}%2C{}~{}~{}%2C95%2C%2C1)'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '%2BVAR_PAR(d%2C{}~{}~{}%2C{}~{}~{}%2C99%2C%2C1)'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '%2BEWMA({}~{}~{}%2C{}~{}~{}%2C94%2CB%2C%2C0)%2BBENCHMARK_VAR_PAR('.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + 'd%2C{}~{}~{}%2C{}~{}~{}%2C95%2C%2C1%2CIBOV)%2BMDD'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '(d%2C{}~{}~{}%2C{}~{}~{}%2Cmdd)%26segmento%3Dtodos%26setor%3Dtodos'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '%26filtro%3D%26demonstracao%3Dconsolidado%2520preferencialmente%26tipo_acao'
            + '%3DTodas%26convencao%3DMIXED%26acumular%3D12%26valores_em%3D1%26num_casas%3D2'
            + '%26salve%3D%26salve_obs%3D%26var_control%3D0%26overwrite%3D0%26setor_bov%'
            + '3Dtodos%26subsetor_bov%3Dtodos%26subsubsetor_bov%3Dtodos%26group_by%3D%26'
            + 'relat_alias_automatico%3DcMDalias_01%26primeira_coluna_ticker%3D0%26'
            + 'periodos%3D0%26periodicidade%3Danual%26formato_data%3D1%26'
            + 'formato_data%3D1&format=json2'
        )
        # fetching data
        resp_req = ComDinheiro().requests_api_cd(str_payload)
        json_ = ast.literal_eval(
            StrHandler()
            .find_between(str(resp_req.text.encode('utf8')), "b'", "'")
            .replace(r'\n', '')
        )
        return JsonFiles().send_json(json_)

    def trading_volume_securities(
        self,
        list_securities: str,
        str_benchmark: str,
        dt_inf: datetime,
        dt_sup: datetime,
    ) -> json:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # lista de papéis no formato da consulta do json da comdinheiro
        list_securities = '%2B'.join(list_securities).upper()
        # definindo variável para pool de conexão
        str_payload = (
            'username={}&password={}&URL=ComparaEmpresas001.php'.format(
                self.user, self.passw
            )
            + '%3F%26data_d%3D31129999%26data_a%3D{}%2F{}%2F{}%26trailing%3D12%26'.format(
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + 'conv%3DMIXED%26c_c%3Dconsolidado%2520preferencialmente%26moeda%3D'
            + 'MOEDA_ORIGINAL%26m_m%3D1000000000%26n_c%3D2%26f_v%3D1%26papeis%3D'
            + '{}'.format(list_securities)
            + '%26indic%3DTICKER%2BVOLUME_MEDIO%'
            + '28{}~{}~{}%2C{}~{}~{}%2C%29%2BQUANT_NEGOCIOS%28'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '{}~{}~{}%2C{}~{}~{}%2C%2Csoma%29%2B%2BMARKET_VALUE%2BLIQUIDEZ_BOLSA%28'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '{}~{}~{}%2C{}~{}~{}%2C%2C%29%2BNEGOCIABILIDADE%28'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '{}~{}~{}%2C{}~{}~{}%2C%2C%2C2%29%2BPESO_INDICE%28'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + 'participacao%2C{}%2C{}~{}~{}%2C%2C%29%2BBTC_ALUGUEL_ACOES'.format(
                str_benchmark,
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '%28TA%2C{}~{}~{}%2C{}~{}~{}%29%2BCOMPRAS_VENDAS_FUNDOS%28'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + 'compras_valor%2C{}~{}~{}%2C%2C0%29%2BPRECO_AJ%28'.format(
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '{}~{}~{}%2C%2C%2CA%2CC%29%2BDY_12M%26enviar_email%3D0%26'.format(
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + 'enviar_email_log%3D0%26transpor%3D0%26op01%3Dtabela%26'
            + 'oculta_cabecalho_sup%3D0%26relat_alias_automatico%3DcMDalias_01&format=json2'
        )
        # print('PAYLOAD: {}'.format(str_payload))
        # fetching data
        resp_req = ComDinheiro().requests_api_cd(str_payload)
        # print(resp_req.text)
        json_ = ast.literal_eval(
            StrHandler()
            .find_between(str(resp_req.text.encode('utf8')), "b'", "'")
            .replace(r'\n', '')
        )
        return JsonFiles().send_json(json_)

    def mdtv_list_securities(
        self, list_securities: List[str], dt_inf: datetime, dt_sup: datetime
    ) -> pd.DataFrame:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # list of securities with the respective format, in order to be used in payload
        list_securities = '%2B'.join(list_securities).upper()
        # payload
        str_payload = (
            'username={}&password={}&URL=ComparaEmpresas001.php%3F%26data_d%'.format(
                self.user, self.passw
            )
            + '3D31129999%26data_a%3D{}%2F{}%2F{}%26trailing%3D12'.format(
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '%26conv%3DMIXED%26c_c%3Dconsolidado%2520preferencialmente%26moeda%3D'
            + 'MOEDA_ORIGINAL%26m_m%3D1000000000%26n_c%3D2%26f_v%3D1%26papeis%3D'
            + '{}'.format(list_securities)
            + '%26indic%3DVOLUME_MEDIO%28'
            + '{}~{}~{}%2C{}~{}~{}%'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '2C%29%26enviar_email%3D0%26enviar_email_log%3D0%26transpor%3D0%26op01%3Dtabela'
            + '%26oculta_cabecalho_sup%3D0%26relat_alias_automatico%3DcMDalias_01%26s'
            + 'cript%3D&format=json2'
        )
        # fetching data
        resp_req = ComDinheiro().requests_api_cd(str_payload)
        json_ = ast.literal_eval(
            StrHandler()
            .find_between(
                str(str(resp_req.text.encode('utf8')) + "'"), "b'", "''"
            )
            .replace(r'\n', '')
            .replace(r' ', '')
        )
        # return dataframe
        return pd.DataFrame(json_['resposta']['tab-p0']['linha'])

    def risk_infos_list_securities(
        self,
        list_securities: List[str],
        str_benchmark: str,
        dt_inf: datetime,
        dt_sup: datetime,
    ) -> pd.DataFrame:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # lista de papéis no formato da consulta do json da comdinheiro
        list_securities = '%2B'.join(list_securities).upper()
        # definindo variável para pool de conexão
        str_payload = (
            'username={}&password={}&URL=ComparaEmpresas001.php%3F%26data_d%'.format(
                self.user, self.passw
            )
            + '3D31129999%26data_a%3D{}%2F{}%2F{}%26trailing%3D12'.format(
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '%26conv%3DMIXED%26c_c%3Dconsolidado%2520preferencialmente%26moeda%3D'
            + 'MOEDA_ORIGINAL%26m_m%3D1000000000%26n_c%3D2%26f_v%3D1%26papeis%3D'
            + '{}'.format(list_securities)
            + '%26indic%3DTICKER%2Bvol_ano_60m%2B%2Bvol_ano_mes_atual%2Bvol_ano_ano_atual%2BVAR_PAR'
            + '%28d%2C{}~{}~{}%2C{}~{}~{}%2C95%2C%2C1%29%'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '2BVAR_PAR%28d%2C{}~{}~{}%2C{}~{}~{}%2C99%2C%2C1%29%2BEWMA'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '%28{}~{}~{}%2C{}~{}~{}%2C94%2CB%2C%2C0%29%2BBENCHMARK_VAR_PAR'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '%28d%2C{}~{}~{}%2C{}~{}~{}%2C95%2C%2C1%2C'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '{}%29%2BMDD%28d%2C'.format(str_benchmark)
            + '{}~{}~{}%2C26~01~2021%2Cmdd%29%26enviar_email%3D0'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
            )
            + '%26enviar_email_log%3D0%26transpor%3D0%26op01%3Dtabela'
            '%26oculta_cabecalho_sup%3D0%26relat_alias_automatico%3DcMDalias_01&format=json2'
        )
        # fetching data
        resp_req = ComDinheiro().requests_api_cd(str_payload)
        json_ = ast.literal_eval(
            StrHandler()
            .find_between(str(resp_req.text.encode('utf8')), "b'", "'")
            .replace(r'\n', '')
        )
        return JsonFiles().send_json(json_)

    def stocks_beta(
        self, list_tickers: List[str], dt_inf: datetime, dt_sup: datetime
    ) -> json:
        """
        DOCSTRING: BETA OF PROVIDED STOCKS
        INPUTS:  LIST OF TICKER AND INFERIOR AND SUPERIOR DATES
        OUTPUTS: JSON
        """
        # lista de papéis no formato da consulta do json da comdinheiro
        list_securities = '%2B'.join(list_tickers)
        # applying date format
        dt_inf = DatesBR().datetime_to_string(dt_inf, '%d%m%Y')
        dt_sup = DatesBR().datetime_to_string(dt_sup, '%d%m%Y')
        # payload
        str_payload = (
            'username={}&password={}&URL=HistoricoIndicadoresFundamentalistas001.php'.format(
                self.user, self.passw
            )
            + '%3F%26data_ini%3D{}%26data_fim%3D{}'.format(dt_inf, dt_sup)
            + '%26trailing%3D12%26conv%3DMIXED%26moeda%3D'
            + 'BRL%26c_c%3Dconsolidado%26m_m%3D1000000%26n_c%3D2%26f_v%3D1%26papel%3D{}'.format(
                list_securities
            )
            + '%26indic%3Dret_01d%2Bret_cdi_01d%2Bbeta_06m%2BLC%26periodicidade%3Ddu%26graf_'
            + 'tab%3Dtabela%26desloc_data_analise%3D1%26flag_transpor%3D0%26c_d%3Dd%26enviar_email'
            + '%3D0%26enviar_email_log%3D0%26'
            + 'cabecalho_excel%3Dmodo1%26relat_alias_automatico%3DcMDalias_01&format=json2'
        )
        # fetching data
        json_ = ComDinheiro().requests_api_cd(str_payload).read()
        json_ = ast.literal_eval(
            str(json_.decode('utf8').replace(r'\n', '').replace(r' ', ''))
        )
        return JsonFiles().send_json(json_)

    def tradability_index(
        self, list_tickers: List[str], dt_inf: datetime, dt_sup: datetime
    ) -> json:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # lista de papéis no formato da consulta do json da comdinheiro
        list_securities = '%2B'.join(list_tickers)
        # applying date format
        dt_inf = DatesBR().datetime_to_string(dt_inf, '%d%m%Y')
        dt_sup = DatesBR().datetime_to_string(dt_sup, '%d%m%Y')
        # definindo variável para pool de conexão
        str_payload = (
            'username={}&password={}&URL=ComparaFundos001.php%3F%26datas'.format(
                self.user, self.passw
            )
            + '%3D{}%2F{}%2F{}%26cnpjs'.format(
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '%3D{}'.format(list_securities)
            + '%26indicadores%3DNEGOCIABILIDADE%28{}~{}~{}%2C{}~{}~{}'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '%2C%2C%2C2%29%26num_casas%3D2%26pc%3Dnome_fundo%26flag_transpor%3D0%26enviar_email'
            + '%3D0%26mostrar_da%3D0%26op01%3Dtabela%26oculta_cabecalho_sup%3D0%26r'
            + 'elat_alias_automatico%3DcMDalias_01&format=json2'
        )
        # fetching data
        json_ = (
            ComDinheiro().requests_api_cd(str_payload).read()
        )  # .text.encode('utf8')
        json_ = ast.literal_eval(
            str(json_.decode('utf8').replace(r'\n', '').replace(r' ', ''))
        )
        # returning data
        return JsonFiles().send_json(json_)

    def open_ended_funds_quotes(
        self, list_funds: List[str], dt_inf: datetime, dt_sup: datetime
    ) -> json:
        """
        DOCSTRING: CLOSING PRICE OF FUNDS' SHARE
        INPUTS:  FUNDS CNPJ (list), MATURITY CODE,
            DATE INFERIOR (DDMMAAAsA, AS A STRING, OR DATETIME FORMAT), DATE SUPERIOR (SAME FORMAT
            AS DATE INFERIOR) AND FORMAT EXTRACTION (JSON AS DEFAULT)
        OUTPUTS: JSON
        """
        # list of securities with the format requested to be queried
        list_securities = '%2B'.join(list_funds)
        # applying date format
        if DatesBR().check_date_datetime_format(dt_inf) == True:
            dt_inf = DatesBR().datetime_to_string(dt_inf, '%d%m%Y')
        if DatesBR().check_date_datetime_format(dt_sup) == True:
            dt_sup = DatesBR().datetime_to_string(dt_sup, '%d%m%Y')
        # payload
        str_payload = (
            'username={}&password={}&URL=HistoricoIndicadoresFundos001.php'.format(
                self.user, self.passw
            )
            + '%3F%26cnpjs%3D{}'.format(list_securities)
            + '%26data_ini%3D{}%26data_fim%3D{}'.format(dt_inf, dt_sup)
            + '%26indicadores%3Dvalor_cota%26op01%3Dtabela_h%26num_casas%3D2%26enviar_email%3D0'
            + '%26periodicidade%3Ddiaria%26cabecalho_excel%3Dmodo1%26transpor%3D0%26asc_desc%3Ddesc'
            + '%26tipo_grafico%3Dlinha%26relat_alias_automatico%3DcMDalias_01&format=json2'
        )
        # fetching data
        json_ = ComDinheiro().requests_api_cd(str_payload).read()
        json_ = ast.literal_eval(
            json_.decode('utf8').replace(r'\n', '').replace(r' ', '')
        )
        return JsonFiles().send_json(json_)

    def open_ended_funds_risk_infos(
        self, list_funds: List[str], dt_sup: datetime
    ) -> json:
        """
        DOCSTRING: RISK INFOS REGARDING HISTORICAL VOLATILITY AND REDEMPTION
        INPUTS:  FUNDS CNPJ (list), DATE SUPERIOR
        OUTPUTS: JSON
        """
        # list of securities with the format requested to be queried
        list_securities = '%2B'.join(list_funds)
        # applying date format
        dt_sup = DatesBR().datetime_to_string(dt_sup, '%d%m%Y')
        # payload
        str_payload = (
            'username={}&password={}&URL=HistoricoIndicadoresFundos001.php'.format(
                self.user, self.passw
            )
            + '%3F%26cnpjs%3D{}'.format(list_securities)
            + '%26data_ini%3D{}%26data_fim%3D{}'.format(dt_sup, dt_sup)
            + '%26indicadores%3Dprazo_liq_resg%2B'
            + 'prazo_disp_rec_resgatado%2Bvol_ano_01m%2Bvol_ano_12m%2B'
            + 'vol_ano_36m%2Bvol_ano_60m%2Bvol_ano_ano_atual%2Bvol_ano_mes_atual%2Bresgate_min%2Btaxa_saida%26op01%3Dtabela_h%26'
            + 'num_casas%3D2%26enviar_email%3D0%26periodicidade%3Ddiaria%26cabecalho_excel%3Dmodo1%26transpor%3D0%26asc_desc%3D'
            + 'desc%26tipo_grafico%3Dlinha%26relat_alias_automatico%3DcMDalias_01&format=json2'
        )
        # fetching data
        json_ = ComDinheiro().requests_api_cd(str_payload).read()
        json_ = ast.literal_eval(
            json_.decode('Latin-1').replace(r'\n', '').replace(r' ', '')
        )
        return JsonFiles().send_json(json_)

    def open_ended_funds_sharpe_dd(
        self, list_eins: List[str], dt_inf: datetime, dt_sup: datetime
    ) -> json:
        """
        DOCSTRING: CÁLCULO DE SHARPE E DROWDOWN POR PERÍODO
        INPUTS:
        OUTPUTS:
        """
        # list of securities with the format requested to be queried
        list_eins = '%2B'.join(list_eins)
        # payload
        str_payload = (
            'username={}&password={}&URL=ComparaFundos001.php%3F%26datas'.format(
                self.user, self.passw
            )
            + '%3D{}%2F{}%2F{}%26cnpjs'.format(
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '%3D{}'.format(list_eins)
            + '%26indicadores%3Dnome_fundo%2Bcnpj_fundo%2Bret_12m_aa%2Bpatrimonio~1e6%2B'
            + 'cotistas%2Bcaptacao~1e6%2Bresgate~1e6%2'
            + 'Bvol_ano_24m%2Bsharpe_12m%2BMDD%28d%2C{}~{}~{}'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
            )
            + '%2C{}~{}~{}%2Cmdd%29%'.format(
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '2BMDD%28d%2C{}~{}~{}%'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
            )
            + '2C{}~{}~{}%2'.format(
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + 'Ctempo_rec%29%2Bobjetivo%2Bclasse%26num_casas%3D2%26pc%3Dnome_fundo%26f'
            + 'lag_transpor%3D0%26enviar_email%3D0%26mostrar_da%3D0%26op01%3Dtabela%26'
            + 'oculta_cabecalho_sup%3D0%26relat_alias_automatico%3DcMDalias_01&format=json2'
        )
        # fetching data
        json_ = (
            ComDinheiro().requests_api_cd(str_payload).text.encode('utf8')
        )  # .read()
        json_ = ast.literal_eval(
            str(json_.decode('utf8').replace(r'\n', '').replace(r' ', ''))
        )
        # returning data
        return JsonFiles().send_json(json_)

    def open_ended_funds_infos(
        self,
        list_eins: List[str],
        dt_inf: datetime,
        dt_sup: datetime,
        str_fmt_dt_input: str = 'YYYY-MM-DD',
        int_wd_bef: int = 2,
        col_classe_anbima: str = 'CLASSE_ANBIMA_DO_FUNDO',
        col_nome_fundo: str = 'NOME_FUNDO',
        col_like_max_drawdown: str = 'MDD*',
        col_mdd_final: str = 'MAX_DRAWDOWN',
        col_cnpj_sem_mascara: str = 'CNPJ_SEM_MASCARA',
        col_cnpj: str = 'CNPJ_DO_FUNDO',
    ) -> pd.DataFrame:
        """
        DOCSTRING: FUND'S NAME, CNPJ, CORPORATE NAME, SHARPE WITH A RANGE OF TEMPORAL WINDOWS,
            MAXIMUM DRAWDOWN (GIVEN A PERIOD OF TIME), ANBIMA'S CLASS, CODE
        INPUTS: LIST OF EINS, DATE INFERIOR AND SUPERIOR
        OUTPUTS: DATAFRAME
        """
        # setting variables
        list_ser = list()
        # changing dates format
        dt_inf = DatesBR().str_date_to_datetime(dt_inf, str_fmt_dt_input)
        dt_sup = DatesBR().str_date_to_datetime(dt_sup, str_fmt_dt_input)
        # inferior reference date within comdinheiro
        dt_ref_cf = DatesBR().sub_working_days(dt_sup, int_wd_bef)
        # eins with the format requested to be queried
        list_eins = '%2B'.join(list_eins)
        # payload
        str_payload = (
            'username={}&password={}&URL=ComparaFundos001.php%3F%26'.format(
                self.user, self.passw
            )
            + 'datas%3D{}%2F{}%2F{}%26'.format(
                dt_ref_cf.strftime('%d'),
                dt_ref_cf.strftime('%m'),
                dt_ref_cf.strftime('%Y'),
            )
            + 'cnpjs%3D{}%26'.format(list_eins)
            + 'indicadores%3Dcnpj_fundo%2Bnome_fundo%2Bsharpe_12m%2Bsharpe_24m%2Bsharpe_'
            + '36m%2Bsharpe_36m%2Bsharpe_48m%2Bsharpe_60m'
            + '%2BMDD%28d%2C{}~{}~{}%2C{}~{}~{}%2Cmdd%29'.format(
                dt_inf.strftime('%d'),
                dt_inf.strftime('%m'),
                dt_inf.strftime('%Y'),
                dt_sup.strftime('%d'),
                dt_sup.strftime('%m'),
                dt_sup.strftime('%Y'),
            )
            + '%2Bclasse_anbima%2Bcodigo_anbima%26num_casas%3D2%26pc%3Dnome_fundo%26flag_transpor'
            + '%3D0%26enviar_email%3D0%26mostrar_da%3D0%26op01%3Dtabela%26oculta_cabecalho_sup%3D0'
            + '%26relat_alias_automatico%3DcMDalias_01&format=json3'
        )
        # fetching data
        json_message = ComDinheiro().requests_api_cd(str_payload).json()
        # cols of exportation
        list_cols = [
            StrHandler()
            .remove_diacritics(StrHandler().latin_characters(str(x)))
            .strip()
            .replace(' ', '_')
            .replace(dt_ref_cf.strftime('%d/%m/%Y'), '')
            .upper()
            for x in list(json_message['tables']['tab0']['lin0'].values())
        ]
        # serialized list of dictionaries
        for index, dict_ in json_message['tables']['tab0'].items():
            if index != 'lin0':
                list_ser.append(
                    {
                        col_nome: dict_['col{}'.format(i)]
                        for i, col_nome in enumerate(list_cols)
                    }
                )
        # serialized list to pandas dataframe
        df_infos_fundos_cd = pd.DataFrame(list_ser)
        # replacing null values
        for col_ in list_cols:
            df_infos_fundos_cd[col_] = [
                str(x).replace('', '0') if len(str(x)) <= 1 else str(x)
                for x in df_infos_fundos_cd[col_].tolist()
            ]
            df_infos_fundos_cd[col_] = [
                str(x).replace(',', '.')
                for x in df_infos_fundos_cd[col_].tolist()
            ]
        # changing columns data types
        df_infos_fundos_cd = df_infos_fundos_cd.astype(
            {
                list_cols[0]: str,
                list_cols[1]: str,
                list_cols[2]: str,
                list_cols[3]: float,
                list_cols[4]: float,
                list_cols[5]: float,
                list_cols[6]: float,
                list_cols[7]: float,
                list_cols[8]: float,
                list_cols[9]: float,
                list_cols[10]: str,
                list_cols[11]: int,
            }
        )
        # renaming columns of interest
        col_mdd = [
            x
            for x in list_cols
            if StrHandler().match_string_like(x, col_like_max_drawdown) == True
        ][0]
        df_infos_fundos_cd.rename(
            columns={col_mdd: col_mdd_final}, inplace=True
        )
        # removing diacritics
        for col_ in [col_nome_fundo, col_classe_anbima]:
            df_infos_fundos_cd[col_] = [
                StrHandler()
                .remove_diacritics(StrHandler().latin_characters(str(x)))
                .upper()
                for x in df_infos_fundos_cd[col_].tolist()
            ]
        # ein without mask
        df_infos_fundos_cd[col_cnpj_sem_mascara] = DocumentsNumbersBR(
            df_infos_fundos_cd[col_cnpj].tolist()
        ).unmask_number
        # returning pandas dataframe
        return df_infos_fundos_cd
