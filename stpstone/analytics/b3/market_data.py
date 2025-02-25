### BRAZILLIAN EXCHANGE MARKET DATA

import ast
import os
import sys
import investpy
import math
import json
import yfinance as yf
import pandas as pd
import numpy as np
from requests import request
from datetime import date, datetime
from typing import Tuple, Optional, List, Dict, Any
sys.path.append('\\'.join([d for d in os.path.dirname(
    os.path.realpath(__file__)).split('\\')][:-3]))
from stpstone._config.global_slots import YAML_B3
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.parsers.json import JsonFiles
from stpstone.utils.parsers.str import StrHandler
from stpstone.finance.comdinheiro.api_request import ComDinheiro
from stpstone.handling_data.object import HandlingObjects
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.handling_data.lists import HandlingLists
from stpstone.document_numbers.br import DocumentsNumbersBR
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.handling_data.html import HtmlHndler
from stpstone.handling_data.numbers import NumHandler
from stpstone.utils.loggs.db_logs import DBLogs


class CalendarB3:

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
            bl_verify=YAML_B3['options_exercise_dates']['bl_verify']
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
            if len(list_headers) == 0 or len(list_data) == 0: continue
            #   pair headers and data within a list
            list_ser = HandlingDicts().pair_headers_with_data(
                list_headers, 
                list_data
            )
            #   adding month to serialized list
            list_ser = HandlingDicts().add_k_v_serialized_list(
                list_ser, 
                YAML_B3['options_exercise_dates']['key_month'].upper(), 
                str_month
            )
            #   exporting to serialized list
            list_ser.extend(list_ser)
        # exporting serialized list to dataframe
        df_opt_xcr_dts = pd.DataFrame(list_ser)
        # adding request date
        df_opt_xcr_dts[YAML_B3['options_exercise_dates']['key_request_date'].upper()] = \
            DatesBR().curr_date
        # altering data types
        df_opt_xcr_dts = df_opt_xcr_dts.astype({
            YAML_B3['options_exercise_dates']['key_day'].upper(): int,
            YAML_B3['options_exercise_dates']['key_details'].upper(): str,
            YAML_B3['options_exercise_dates']['key_month'].upper(): str
        })
        # adding logging
        df_opt_xcr_dts = DBLogs().audit_log(
            df_opt_xcr_dts, YAML_B3['options_exercise_dates']['url'], 
            DatesBR().utc_log_ts
        )
        # returning dataframe
        return df_opt_xcr_dts


class TradingHoursB3:

    def futures_generic(self, url:str, int_cols:int) -> list:
        """
        DOCSTRING: TRADING TIMES OF FUTURES REGARDING BRAZILLIAN PMI (IPCA) AND STOCK INDEXES
        INPUTS: -
        OUTPUTS:
        """
        # setting variables
        list_df = list()
        # request html
        bs_html = HtmlHndler().html_bs_parser(
            url, 
            bl_verify=YAML_B3['trading_hours_b3']['futures']['bl_verify']
        )
        # looping within tables
        for _, bs_table in enumerate(bs_html.find_all('table')):
            #   reseting variables
            list_ser = list()
            #   creating headers' list
            #   auxiliary header 1
            list_headers_1 = [
                StrHandler().remove_diacritics(bs_th.get_text())\
                    .upper()\
                    .replace('(', '')\
                    .replace(')', '')\
                    .replace(' ', '_')\
                    .replace('+', '')\
                    .replace('-', '_')\
                    .replace('_DE_', '_')
                for bs_th in bs_table.find('thead').find_all('tr')[0].find_all('th')
            ]
            #   checking wheter there is more than one column for the header
            try:
                #   auxiliary header 2
                list_headers_2 = [
                    StrHandler().remove_diacritics(bs_td.get_text())\
                        .upper()
                    for bs_td in bs_table.find('thead').find_all('tr')[1].find_all('td')
                ]
                #   consolidated header
                list_headers = [
                    list_headers_1[0], 
                    list_headers_1[1]
                ]
                for aux_ in range(2, int_cols):
                    if aux_ < 4:
                        list_headers.extend(
                            [
                                list_headers_2[2*aux_-4] + '_' + list_headers_1[aux_],
                                list_headers_2[2*aux_-3] + '_' + list_headers_1[aux_]
                            ]
                        )
                    elif aux_ == 4:
                        list_headers.extend(
                            [
                                list_headers_2[2*aux_-4] + '_' + list_headers_1[aux_]
                            ]
                        )
                    else:
                        list_headers.extend(
                            [
                                list_headers_2[2*aux_-5] + '_' + list_headers_1[aux_],
                                list_headers_2[2*aux_-4] + '_' + list_headers_1[aux_]
                            ]
                        )
            except IndexError:
                list_headers = list_headers_1
            #   looping within rows
            for bs_row in bs_table.find('tbody').find_all('tr'):
                #   body
                list_tr_body = [
                    bs_td.get_text()\
                        .replace('(1)', '')\
                        .replace('(2)', '')\
                        .replace('(3)', '')\
                        .replace('(4)', '')\
                        .replace('(5)', '')\
                        .replace('(6)', '')\
                        .replace('(7)', '')\
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
            df_ = DBLogs().audit_log(
                df_, url, 
                DatesBR().utc_log_ts
            )
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
            YAML_B3['trading_hours_b3']['futures']['num_cols_pmi_idx']
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
            YAML_B3['trading_hours_b3']['futures']['num_cols_int_rts']
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
            YAML_B3['trading_hours_b3']['futures']['num_cols_commodities']
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
            YAML_B3['trading_hours_b3']['futures']['num_cols_crypto']
        )
    
    @property
    def futures_currency(self) -> list:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS
        """
        return self.futures_generic(
            YAML_B3['trading_hours_b3']['futures']['url_currency'], 
            YAML_B3['trading_hours_b3']['futures']['num_cols_currency']
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
            YAML_B3['trading_hours_b3']['futures']['num_cols_otc']
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
            YAML_B3['trading_hours_b3']['futures']['num_cols_opf']
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
            bl_verify=YAML_B3['trading_hours_b3']['stocks']['bl_verify']
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
                        StrHandler().remove_diacritics(bs_th.get_text())\
                            .upper()\
                            .replace(' ', '_')\
                            .replace('-', '_')\
                            .replace('_DE_', '_')
                    )
                    for bs_th in bs_table.find_all('th')
                    if StrHandler().find_substr_str(
                        StrHandler().remove_diacritics(bs_th.get_text())\
                            .upper()\
                            .replace(' ', '_')\
                            .replace('-', '_')\
                            .replace('_DE_', ''),
                        '*AFTER_MARKET*'
                    ) == False
                ]
                list_headers_1.remove('AFTER_MARKET')
                list_headers_1[-1] = list_headers_1[-1] + '_AFTER_MARKET'
                list_headers_1[-2] = list_headers_1[-2] + '_AFTER_MARKET'
                #   auxiliary header 2
                list_headers_2 = [
                    StrHandler().remove_numeric_chars(
                        StrHandler().remove_diacritics(bs_td.get_text())\
                            .upper()
                    )
                    for bs_td in bs_table.find('tbody').find('tr').find_all('td')
                ]
                #   consolidating headers list
                list_headers = [
                    list_headers_1[0]
                ]
                for aux_ in range(1, 8):
                    list_headers.extend(
                        [
                            list_headers_2[2*aux_-1] + '_' + list_headers_1[aux_],
                            list_headers_2[2*aux_] + '_' + list_headers_1[aux_]
                        ]
                    )
            #   TABLE 1
            if i == 1:
                #   auxiliary header 1
                list_headers_1 = [
                    StrHandler().remove_numeric_chars(
                        StrHandler().remove_diacritics(bs_th.get_text())\
                            .upper()\
                            .replace(' ', '_')\
                            .replace('-', '_')\
                            .replace('_DO_', '_')
                    )
                    for bs_th in bs_table.find('thead').find_all('tr')[1].find_all('th')
                ]
                #   auxiliary header 2
                list_headers_2 = [
                    StrHandler().remove_numeric_chars(
                        StrHandler().remove_diacritics(bs_th.get_text())\
                            .upper()\
                            .replace(' ', '_')\
                            .replace('-', '_')\
                            .replace('_DO_', '_')\
                            .replace('_DE_', '_')
                    )
                    for bs_th in bs_table.find('thead').find_all('tr')[2].find_all('th')
                ]
                #   auxiliary header 3
                list_headers_3 = [
                    StrHandler().remove_diacritics(bs_td.get_text())\
                        .upper()\
                        .replace('\xa0', '')
                    for bs_td in bs_table.find('tbody').find_all('tr')[0].find_all('td')
                ]
                #   consolidating headers list
                list_headers = [
                    'MERCADO', 
                    list_headers_3[1] + '_' + list_headers_2[0] \
                                + '_' + list_headers_1[0],
                    list_headers_3[2] + '_' + list_headers_2[0] \
                        + '_' + list_headers_1[0], 
                    list_headers_3[3] + '_' + list_headers_2[1] \
                                + '_' + list_headers_1[1],
                    list_headers_3[4] + '_' + list_headers_2[1] \
                        + '_' + list_headers_1[1], 
                    list_headers_2[2] + '_' + list_headers_1[1], 
                    list_headers_3[6] + '_' + list_headers_2[3] \
                        + '_' + list_headers_1[1], 
                    list_headers_3[7] + '_' + list_headers_2[3] \
                        + '_' + list_headers_1[1], 
                    list_headers_3[8] + '_' + list_headers_2[4] \
                        + '_' + list_headers_1[1], 
                ]
            else:
                raise Exception('Table {} not expected, please validate.'.format(i))
            #   looping within rows
            for bs_row in bs_table.find('tbody').find_all('tr')[1:]:
                #   body
                list_tr_body = [
                    bs_td.get_text()\
                        .replace(r'\xa', '')\
                        .replace('\xa0','')\
                        .replace('\n', '')\
                        .replace('\t', '')\
                        .replace('(1)', '')\
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
                df_, YAML_B3['trading_hours_b3']['stocks']['url'], 
                DatesBR().utc_log_ts
            )
        # creating individual dataframes
        df_tt_bov = list_df[0]
        df_tt_exc_opt = list_df[1]
        # returning dataframes of interest
        return df_tt_bov, df_tt_exc_opt


class TheorPortfB3:

    def generic_req(self, str_indice:str, method:str='GET', float_pct_factor:float=100.0) \
        -> pd.DataFrame:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # requesting data
        req_resp = request(method, YAML_B3['theor_port_b3']['url_{}'.format(str_indice)], 
                           verify=YAML_B3['theor_port_b3']['bl_verify'])
        # raising exception in case of status code different from 2xx
        req_resp.raise_for_status()
        # requesting json
        json_ibov = req_resp.json()
        # building serialized list
        list_ser = [
            {
                YAML_B3['theor_port_b3']['col_ticker']: dict_[YAML_B3[
                    'theor_port_b3']['key_code']], 
                YAML_B3['theor_port_b3']['col_asset']: dict_[YAML_B3[
                    'theor_port_b3']['key_asset']], 
                YAML_B3['theor_port_b3']['col_type']: dict_[YAML_B3[
                    'theor_port_b3']['key_type']], 
                YAML_B3['theor_port_b3']['col_pct']: float(dict_[YAML_B3[
                    'theor_port_b3']['key_part']].replace(',', '.')) / float_pct_factor, 
                YAML_B3['theor_port_b3']['col_theor_qty']: dict_[YAML_B3[
                    'theor_port_b3']['key_theor_qty']].replace('.', ''), 
            } for dict_ in json_ibov['results']
        ]
        # defining dataframe
        df_ = pd.DataFrame(list_ser)
        # changing data types
        df_ = df_.astype({
            YAML_B3['theor_port_b3']['col_ticker']: str, 
            YAML_B3['theor_port_b3']['col_asset']: str, 
            YAML_B3['theor_port_b3']['col_type']: str, 
            YAML_B3['theor_port_b3']['col_pct']: float, 
            YAML_B3['theor_port_b3']['col_theor_qty']: float
        })
        # adding logging
        df_ = DBLogs().audit_log(
            df_, YAML_B3['theor_port_b3']['url_{}'.format(str_indice)], 
            DatesBR().utc_log_ts
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
    def financial_indicators_b3(self, method:str='GET', float_pct_factor:float=100.0, 
                                dt_input_fmt:str='DD/MM/YYYY') -> pd.DataFrame:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # requesting financial indicators b3
        req_resp = request(method, YAML_B3['financial_indicators']['url'])
        # raise exception if status code is different from 2xx
        req_resp.raise_for_status()
        # appeding json to dataframe
        df_fin_ind = pd.DataFrame(req_resp.json())
        # changing column types
        df_fin_ind = df_fin_ind.astype({
            YAML_B3['financial_indicators']['col_si_code']: np.int64,
            YAML_B3['financial_indicators']['col_desc']: str,
            YAML_B3['financial_indicators']['col_grp_desc']: str,
            YAML_B3['financial_indicators']['col_value']: str,
            YAML_B3['financial_indicators']['col_rate']: str,
            YAML_B3['financial_indicators']['col_last_up']: str,
        })
        df_fin_ind[YAML_B3['financial_indicators']['col_last_up']] = [
            DatesBR().str_date_to_datetime(x, dt_input_fmt) if x is not math.nan else 0 
            for x in df_fin_ind[YAML_B3['financial_indicators']['col_last_up']]
        ]
        for col_ in [
            YAML_B3['financial_indicators']['col_value'],
            YAML_B3['financial_indicators']['col_rate']
        ]:
            df_fin_ind[col_] = [
                float(x.replace(',', '.')) if x != '' else 0 
                for x in df_fin_ind[col_]
            ]
        df_fin_ind[YAML_B3['financial_indicators']['col_rate']] = [
            #   selic rate
            (row[YAML_B3['financial_indicators']['col_rate']] + YAML_B3['financial_indicators'][
                'float_pct_dif_cdi_selic']) / float_pct_factor if row[YAML_B3[
                    'financial_indicators']['col_si_code']] == YAML_B3['financial_indicators'][
                        'int_si_code_selic']
            #   other rates
            else row[YAML_B3['financial_indicators']['col_rate']] / float_pct_factor
            for _, row in df_fin_ind.iterrows()
        ]
        # adding logging
        df_fin_ind = DBLogs().audit_log(
            df_fin_ind, 
            YAML_B3['financial_indicators']['url'],
            DatesBR().utc_from_dt(DatesBR().curr_date)
        )
        # returning dataframe
        return df_fin_ind

    @property
    def securities_volatility(self, method:str='GET', float_pct_factor:float=100.0) -> pd.DataFrame:
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
                req_resp = request(method, dict_['url'].format(str_hash))
                #   raise exception for status different from 2xx
                req_resp.raise_for_status()
                #   get json in memory
                json_ = req_resp.json()
                #   reseting json with results
                list_ = json_['results']
                #   creating serialized list
                for k, v in [
                    (YAML_B3['securities_volatility_b3']['cols_names']['pg_num'], 
                     json_['page']['pageNumber']), 
                    (YAML_B3['securities_volatility_b3']['cols_names']['total_pgs'], 
                     json_['page']['totalPages']), 
                    (YAML_B3['securities_volatility_b3']['cols_names']['date_ref'], 
                     DatesBR().curr_date), 
                     (YAML_B3['securities_volatility_b3']['cols_names']['url'], 
                     dict_['url']), 
                ]:
                    list_ = HandlingDicts().add_k_v_serialized_list(list_, k , v)
                #   extend to serialized list
                list_ser.extend(list_)
        # adding to pandas dataframe
        df_sec_vol = pd.DataFrame(list_ser)
        # changing column types
        df_sec_vol = df_sec_vol.astype({
            YAML_B3['securities_volatility_b3']['cols_names']['ticker']: str,
            YAML_B3['securities_volatility_b3']['cols_names']['company_name']: str,
            YAML_B3['securities_volatility_b3']['cols_names']['serie']: str, 
            YAML_B3['securities_volatility_b3']['cols_names']['pg_num']: int,
            YAML_B3['securities_volatility_b3']['cols_names']['total_pgs']: int
        })
        #   looping within periods of security volatility calculed by b3
        for int_per in YAML_B3['securities_volatility_b3']['vols_calc_per']:
            #   loop through standard deviation and annual volatility for the giver period
            for col_ in [
                YAML_B3['securities_volatility_b3']['cols_names']['std'].format(str(int_per)), 
                YAML_B3['securities_volatility_b3']['cols_names']['ann_vol'].format(str(int_per))
            ]:
                #   alter dataframe column types
                df_sec_vol[col_] = [float(str(d).replace(',', '.')) / float_pct_factor 
                                    for d in df_sec_vol[col_]]
        # adding logging
        df_sec_vol = DBLogs().audit_log(
            df_sec_vol, None, DatesBR().utc_log_ts
        )
        # returning dataframe
        return df_sec_vol