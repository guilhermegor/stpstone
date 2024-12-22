### BRAZILLIAN EXCHANGE MARKET DATA

import ast
import os
import sys
import investpy
import math
import yfinance as yf
import pandas as pd
import numpy as np
from yahooquery import Ticker
from requests import request
from json import loads
from pprint import pprint
from datetime import date
sys.path.append('\\'.join([d for d in os.path.dirname(
    os.path.realpath(__file__)).split('\\')][:-3]))
from stpstone.settings.global_slots import YAML_B3
from stpstone.cals.handling_dates import DatesBR
from stpstone.handling_data.json import JsonFiles
from stpstone.handling_data.str import StrHandler
from stpstone.finance.comdinheiro.api_request import ComDinheiro
from stpstone.handling_data.object import HandlingObjects
from stpstone.cals.handling_dates import DatesBR
from stpstone.handling_data.lists import HandlingLists
from stpstone.document_numbers.br import DocumentsNumbersBR
from stpstone.handling_data.dicts import HandlingDicts
from stpstone.handling_data.html_parser import HtmlHndler
from stpstone.handling_data.numbers import NumHandler
from stpstone.loggs.db_logs import DBLogs


class TradingVolumeB3:

    def bov_monthly(self, int_year, int_month, list_th=list(), list_td=list()):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # format month and year to string
        str_month = f'{int_month:02}'
        str_year = str(int_year)
        url = YAML_B3['bov_trading_volume']['url'].format(str_month, str_year)
        # request html
        bs_html = HtmlHndler().html_bs_parser(
            url, 
            bl_verify=YAML_B3['bov_trading_volume']['bl_verify']
        )
        # table
        bs_table = bs_html.find_all('table')[11]
        # looping within rows
        for i_tr, tr in enumerate(bs_table.find_all('tr')):
            #   checking if is a header how, otherwise it is a data row
            if i_tr < 2:
                #   getting headers
                list_th.extend([
                    StrHandler().remove_diacritics(el.get_text())
                        .replace('\xa0', '')
                        .replace('Totais dos pregoes  Ref: ', '')
                        .replace('(R$)', 'BRL')
                        .replace(' - ', ' ')
                        .replace(' ', '_')
                        .strip()
                        .upper()
                    for el in tr.find_all('td')
                    if len(
                        StrHandler().remove_diacritics(el.get_text())\
                        .replace('\xa0', '')
                    ) > 0
                ])
            else:
                #   getting data
                list_td.extend([
                    # data numeric
                    float(td.get_text()
                        .strip()
                        .replace('.', '')
                        .replace(',', '.'))
                    if NumHandler().is_numeric(
                        StrHandler().remove_diacritics(td.get_text())
                            .strip()
                            .replace('.', '')
                            .replace(',', '.')
                    )
                    # data not numeric
                    else
                        StrHandler().remove_diacritics(td.get_text())
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
                ])
        # dealing with header raw data
        list_th = [
            list_th[2],
            list_th[3] + '_' + list_th[0],
            list_th[4] + '_' + list_th[0],
            list_th[3] + '_' + list_th[1],
            list_th[4] + '_' + list_th[1],
        ]
        # pair headers and data within a list
        list_ser = HandlingDicts().pair_headers_with_data(
            list_th, 
            list_td
        )
        # turning into dataframe
        df_bov_montlhy = pd.DataFrame(list_ser)
        # defining columns of month and last twelve months
        cols_month = list(df_bov_montlhy.columns)[0:3]
        col_l12m = [list(df_bov_montlhy.columns)[0]]
        col_l12m.extend(
            list(df_bov_montlhy.columns)[3:]
        )
        # creating separate into two dataframes
        df_bov_mnt = df_bov_montlhy[cols_month]
        df_bov_l12m = df_bov_montlhy[cols_month]
        # renaming columns
        for df_ in [
            df_bov_mnt,
            df_bov_l12m
        ]:
            #   list columns
            list_cols = list(df_.columns)
            #   renaming columns of interest
            df_.rename(columns={
                list_cols[1]: YAML_B3['bov_trading_volume']['col_deals'],
                list_cols[2]: YAML_B3['bov_trading_volume']['col_volume'],
                list_cols[0]: YAML_B3['bov_trading_volume']['col_market']
            }, inplace=True)
            #   adding column of time period
            df_[YAML_B3['bov_trading_volume']['col_time_period']] = \
                DatesBR().month_year_string(
                    list_cols[1].replace('NEGOCIACOES_', '')
                )
            # adding logging
            df_ = DBLogs().audit_log(
                df_, url, list_cols[1].replace('NEGOCIACOES_', '')
            )
        # returning dataframe
        return df_bov_mnt, df_bov_l12m


class CalendarB3:

    def bl_weekly_option(self, ticker_opc, int_qtd_letras_min=6):
        '''
        DOCSTRING: BOOLEAN TO WEEKLY EXPIRING OPTIONS
        INPUTS: TICKER
        OUTPUTS: BOOLEAN
        '''
        num_let = sum(c.isalpha() for c in ticker_opc)
        num_let += sum(c.isdigit() for c in ticker_opc[:4])
        return num_let >= int_qtd_letras_min

    @property
    def options_exercise_dates(self, list_dicts=list()):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
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
            list_dicts.extend(list_ser)
        # exporting serialized list to dataframe
        df_opt_xcr_dts = pd.DataFrame(list_dicts)
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

    def futures_generic(self, url, int_cols, list_df=list()):
        '''
        DOCSTRING: TRADING TIMES OF FUTURES REGARDING BRAZILLIAN PMI (IPCA) AND STOCK INDEXES
        INPUTS: -
        OUTPUTS:
        '''
        # request html
        bs_html = HtmlHndler().html_bs_parser(
            url, 
            bl_verify=YAML_B3['trading_hours_b3']['futures']['bl_verify']
        )
        # looping within tables
        for _, bs_table in enumerate(bs_html.find_all('table')):
            #   reseting variables
            list_dicts = list()
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
                list_dicts.append(dict(zip(list_headers, list_tr_body)))
            #   creating dataframe
            df_ = pd.DataFrame(list_dicts)
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
    def futures_pmi_idx(self):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        return self.futures_generic(
            YAML_B3['trading_hours_b3']['futures']['url_pmi_idx'], 
            YAML_B3['trading_hours_b3']['futures']['num_cols_pmi_idx']
        )

    @property
    def futures_brl_usd_int_rts(self):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        return self.futures_generic(
            YAML_B3['trading_hours_b3']['futures']['url_int_rts'], 
            YAML_B3['trading_hours_b3']['futures']['num_cols_int_rts']
        )
    
    @property
    def futures_commodities(self):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS
        '''
        return self.futures_generic(
            YAML_B3['trading_hours_b3']['futures']['url_commodities'], 
            YAML_B3['trading_hours_b3']['futures']['num_cols_commodities']
        )
    
    @property
    def futures_crypto(self):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        return self.futures_generic(
            YAML_B3['trading_hours_b3']['futures']['url_crypto'], 
            YAML_B3['trading_hours_b3']['futures']['num_cols_crypto']
        )
    
    @property
    def futures_forex(self):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS
        '''
        return self.futures_generic(
            YAML_B3['trading_hours_b3']['futures']['url_forex'], 
            YAML_B3['trading_hours_b3']['futures']['num_cols_forex']
        )

    @property
    def futures_otc(self):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        return self.futures_generic(
            YAML_B3['trading_hours_b3']['futures']['url_otc'], 
            YAML_B3['trading_hours_b3']['futures']['num_cols_otc']
        )
    
    @property
    def futures_opf_bef_aft_xrc(self):
        '''
        DOCSTRING: OPTIONS ON FUTURES - BEFORE AND AFTER EXERCISE
        INPUTS: -
        OUTPUTS: 
        '''
        return self.futures_generic(
            YAML_B3['trading_hours_b3']['futures']['url_opf'], 
            YAML_B3['trading_hours_b3']['futures']['num_cols_opf']
        )

    @property
    def stocks(self, list_df=list()):
        '''
        DOCSTRING: TRADING TIMES OF BRAZILLIAN STOCKS
        INPUTS: -
        OUTPUTS:
        '''
        # request html
        bs_html = HtmlHndler().html_bs_parser(
            YAML_B3['trading_hours_b3']['stocks']['url'], 
            bl_verify=YAML_B3['trading_hours_b3']['stocks']['bl_verify']
        )
        # looping within tables
        for i, bs_table in enumerate(bs_html.find_all('table')):
            #   reseting variables
            list_dicts = list()
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
                list_dicts.append(dict(zip(list_headers, list_tr_body)))
            #   creating dataframe
            df_ = pd.DataFrame(list_dicts)
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

    def generic_req(self, str_indice, method='GET', float_pct_factor=100.0):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # requesting data
        resp_req = request(method, YAML_B3['theor_port_b3']['url_{}'.format(str_indice)], 
                           verify=YAML_B3['theor_port_b3']['bl_verify'])
        # raising exception in case of status code different from 2xx
        resp_req.raise_for_status()
        # requesting json
        json_ibov = resp_req.json()
        # building serialized list
        list_dicts = [
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
        df_ = pd.DataFrame(list_dicts)
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
    def ibov(self):
        ''''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        return self.generic_req('ibov')
    
    @property
    def ibra(self):
        ''''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        return self.generic_req('ibra')
    
    @property
    def ibrx100(self):
        ''''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        return self.generic_req('ibrx100')
    
    @property
    def ibrx50(self):
        ''''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        return self.generic_req('ibrx50')


class MDB3:

    @property
    def financial_indicators_b3(self, method='GET', float_pct_factor=100.0, 
                                dt_input_fmt='DD/MM/YYYY'):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # requesting financial indicators b3
        resp_req = request(method, YAML_B3['financial_indicators']['url'])
        # raise exception if status code is different from 2xx
        resp_req.raise_for_status()
        # appeding json to dataframe
        df_fin_ind = pd.DataFrame(resp_req.json())
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
    def securities_volatility(self, method='GET', float_pct_factor=100.0, list_dicts=list()):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
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
                list_dicts.extend(list_)
        # adding to pandas dataframe
        df_sec_vol = pd.DataFrame(list_dicts)
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


class MDYFinance:

    def historical_data_securities_yq(self, start_date, end_date, list_securities=None, 
                                      list_indexes=None, 
                                      input_dates_format='YYYY-MM-DD', bl_verify=False, 
                                      column_ticker='ticker', 
                                      colum_dt_date='dt_date'):
        '''
        DOCSTRING:
        INPTUS:
        OUTPUTS:
        '''
        # defining start and end dates
        if input_dates_format != None:
            start_date = DatesBR().str_date_to_datetime(start_date, input_dates_format)
            end_date = DatesBR().str_date_to_datetime(end_date, input_dates_format)
        # dealing with securities tickers
        if (list_securities == None) or (list_securities == []):
            list_securities = []
        else:
            # list_securities = ['{}.SA'.format(str(x).upper()) for x in list_securities]
            list_securities = [str(x).upper() for x in list_securities]
        # dealing with indexes tickers
        if (list_indexes == None) or (list_indexes == []):
            list_indexes = []
        else:
            list_indexes = ['^{}'.format(str(x).upper()) for x in list_indexes]
        # extending stock's list
        list_tickers = HandlingLists().extend_lists(list_indexes, list_securities)
        # removing duplicates
        list_tickers = HandlingLists().remove_duplicates(list_tickers)
        # getting historical data
        list_yq_data = Ticker(list_tickers, verify=bl_verify)
        df_yq_data = list_yq_data.history(start=start_date, end=end_date)
        # index to column - ticker
        df_yq_data[column_ticker] = [str(x[0]).replace('^', '').replace('.SA', '') 
                                     for x in df_yq_data.index]
        # index to column - date in datetime format
        df_yq_data[colum_dt_date] = [x[1] for x in df_yq_data.index]
        # filling nan values with upper data
        df_yq_data[column_ticker].fillna(method='ffill', inplace=True)
        # retrieving historical data
        return df_yq_data

    def daily_returns(self, df_yq_data:pd.DataFrame, column_symbol:str='symbol', 
                      col_ticker:str='ticker', col_close:str='adjclose', 
                      col_open:str='open', col_date:str='dt_date', 
                      col_daily_return:str='daily_return', 
                      str_type_return_calc:str='close_close') -> pd.DataFrame:
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # order by ticker/date in ascending order
        df_yq_data.sort_values(
            by=[col_ticker, col_date], 
            ascending=[True, True], 
            inplace=True
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
                df_yq_data.groupby(col_ticker).apply(
                    lambda group: np.log(group[col_open] / group[col_close].shift(1))
                ).reset_index(level=0, drop=True)
            )
        else:
            raise Exception('Type of return calculation not supported. '
                            + f'TYPE: {str_type_return_calc}')
        # optionally reset nulls when switching symbols for clarity
        df_yq_data.loc[
            df_yq_data.groupby(level=column_symbol).head(1).index, col_daily_return
        ] = None
        # returning dataframe with daily returns
        return df_yq_data


class MDInvestingDotCom:

    def ticker_reference_investing_com(self, ticker, host='https://tvc4.investing.com/'
                                       + '725910b675af9252224ca6069a1e73cc/1631836267/'
                                       + '1/1/8/symbols?symbol={}', method='GET',
                                       bl_verify=True, key_ticker='ticker',
                                       headers={'User-Agent': 'Mozilla/5.0'
                                                + ' (Windowns NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                                + '(KHTML, like Gecko) Chrome/91.0.4472.101 '
                                                + 'Safari/537.36'}):
        '''
        DOCSTRING: TICKER REFERENCE FROM INVESTING.COM
        INPUTS: TICKER, HOST (DEFAULT), METHOD (DEFAULT), BOOLEAN VERIFY (DEFAULT), 
            KEY TICKER (DEFAULT), HEADERS (DEFAULT)
        OUTPUTS: STRING
        '''
        # collect content from rest
        # print('*** URL ***')
        # print(host.format(ticker))
        content_request = request(method, host.format(
            ticker), verify=bl_verify, headers=headers).text
        # turning to desired format - loading json to memopry
        jsonify_message = loads(content_request)
        # return named ticker form investing.com
        return jsonify_message[key_ticker]

    def historical_closing_intraday_data_investing_com(
        self, ticker_reference, from_date_timestamp,
        to_date_timestamp, type_closing_intraday='D',
        host='https://tvc4.investing.com/725910b675af9252224ca6069a1e73cc/'
            + '1631836267/1/1/8/history?symbol={}&resolution={}&from={}&to={}', method='GET',
        headers={'User-Agent': 'Mozilla/5.0'
                 + ' (Windowns NT 10.0; Win64; x64) AppleWebKit/537.36 '
                 + '(KHTML, like Gecko) Chrome/91.0.4472.101 '
                 + 'Safari/537.36'}):
        '''
        DOCSTRING: HISTORICAL CLOSING/INTRADAY TICKS FROM INVESTING.COM
        INPUTS: TICKER REFERENCE FROM INVESTING.COM, FROM DATE (TIMESTAMP), TO DATE (TIMESTAMP), 
            TYPE CLOSING/INTRADAY PRICE (D AS DEFAULT FOR CLOSING), HOST (DEFAULT) AND HEADERS
        OUTPUTS: JSON
        '''
        # collect content from rest
        # print(host.format(ticker_reference, type_closing_intraday,
        #                   from_date_timestamp, to_date_timestamp))
        content_request = request(method, host.format(ticker_reference, type_closing_intraday,
                                                      from_date_timestamp, to_date_timestamp),
                                  headers=headers).text
        # turning to desired format - loading json to memory
        jsonify_message = loads(content_request)
        # return named ticker from investing.com
        return jsonify_message

    def historical_closing_data_yf(self, list_tickers, str_period='max', str_orient='records',
                                   list_dicts=list(), col_date='Date', col_ticker='Ticker'):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS
        '''
        # looping through tickers and collecting historical data
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
            list_dicts.extend(df_hist_closing_data.to_dict(orient=str_orient))
        # alterando lista de dicionários para dataframe
        df_closing_data = pd.DataFrame(list_dicts)
        # alterando tipo de coluna de data para string
        df_closing_data[col_date] = [StrHandler().get_string_until_substring(str(x), ' ') 
                                     for x in df_closing_data[col_date]]
        # alterando string para tipo de data
        df_closing_data[col_date] = [DatesBR().str_date_to_datetime(x, 'YYYY-MM-DD') 
                                     for x in df_closing_data[col_date]]
        # returning data of interest
        return df_closing_data

    def cotacoes_serie_historica(self, tickers, data_inf=DatesBR().sub_working_days(
            DatesBR().curr_date, -2).strftime('%d/%m/%Y'), data_sup=DatesBR().sub_working_days(
            DatesBR().curr_date, -1).strftime('%d/%m/%Y'), pais='brazil', classes_ativos='acao'):
        '''
        DOCSTRING: COTAÇÕES HISTÓRICAS DE ATIVOS NEGOCIADOS EM BOLSAS DO MUNDO INTEIRO
        INPUTS: TICKERS, DATA INFERIOR, DATA SUPERIOR, PAÍS (BRAZIL COMO DEFAULT) COM
            FORMATO DATA 'DD/MM/YYYY'
        OUTPUTS: JSON
        '''
        # alterando ticker para estar dentro de uma lista
        if type(tickers) == str:
            tickers = [tickers]
        # preenchendo lista de ativos elegíveis para cotação
        dict_cotacoes_hist = dict()
        if classes_ativos == 'acao' and type(classes_ativos) == str:
            for ticker in tickers:
                json_cotacoes_hist = HandlingObjects().literal_eval_data(
                    investpy.get_stock_historical_data(ticker, pais, data_inf, data_sup,
                                                       as_json=True))
                dict_cotacoes_hist[ticker] = json_cotacoes_hist
        else:
            if type(classes_ativos) == str:
                classes_ativos = [classes_ativos]
                classes_ativos.extend((len(tickers) - 1) * [classes_ativos])
            dict_depara_ticker_nome = dict()
            for i in range(len(tickers)):
                if classes_ativos[i] == 'etf':
                    for dict_ativo in investpy.get_etfs_dict(pais):
                        if dict_ativo['symbol'] == tickers[i]:
                            dict_depara_ticker_nome[tickers[i]
                                                    ] = dict_ativo['full_name']
                            break
                    else:
                        dict_depara_ticker_nome[tickers[i]] = \
                            'Ticker não cadastrado no serviço de cotações investing.com'
                    json_cotacoes_hist = investpy.get_etf_historical_data(
                        dict_depara_ticker_nome[tickers[i]],
                        pais, data_inf, data_sup, as_json=True)
                    dict_cotacoes_hist[tickers[i]] = HandlingObjects().literal_eval_data(
                        json_cotacoes_hist)
                elif classes_ativos[i] == 'acao':
                    json_cotacoes_hist = investpy.get_stock_historical_data(
                        tickers[i], pais, data_inf, data_sup, as_json=True)
                    dict_cotacoes_hist[tickers[i]] = HandlingObjects().literal_eval_data(
                        json_cotacoes_hist)
        return JsonFiles().send_json(dict_cotacoes_hist)

    def cotacoes_dia(self, tickers, data_interesse=DatesBR().sub_working_days(
            DatesBR().curr_date, -1).strftime('%d/%m/%Y'),
            data_despejo=DatesBR().curr_date.strftime('%d/%m/%Y'), pais='brazil'):
        '''
        DOCSTRING: COTAÇÃO DO DIA DE INTERESSE DE ATIVOS NEGOCIADOS EM BOLSAS DO MUNDO INTEIRO
        INPUTS: TICKERS, DATA INFERIOR, DATA SUPERIOR, PAÍS (BRAZIL COMO DEFAULT) COM
            FORMATO DATA 'DD/MM/YYYY'
        OUTPUTS: JSON
        '''
        if type(tickers) == str:
            tickers = [tickers]
        dict_cotacoes_hist = dict()
        for ticker in tickers:
            json_cotacoes_hist = investpy.get_stock_historical_data(
                ticker, pais, data_interesse, data_despejo, as_json=True)
            dict_cotacoes_hist[ticker] = json_cotacoes_hist['historical'][-1]['close']
        return JsonFiles().send_json(dict_cotacoes_hist)

    def indice_historical_data(self, date_inf, date_sup, indice='Bovespa', conutry='brazil',
                               format_extraction='json2'):
        '''
        DOCSTRING: CLOSING PRICE OF A GIVEN INDICE
        INPUTS: DATE INFERIOR, SUPERIOR, INDICE NAME (DEFAULT BOVESPA), 
            COUNTRY (DEFAULT BRAZIL), FORMAT OF EXTRACTION (DEFAULT JSON2)
        OUTPUTS: DICT WITH CLOSE PRICE
        '''
        return JsonFiles().send_json(
            {d['date']: d['close'] for d in HandlingObjects().literal_eval_data(
                investpy.indices.get_index_historical_data(
                    'Bovespa', 'brazil', date_inf, date_sup, as_json=True))['historical']})


class MDComDinheiro:

    def __init__(self, user, passw):
        self.user = user
        self.passw = passw

    def bmf_historical_close_data(self, contract, maturity_code,
                                  date_inf, date_sup, format_extraction='json2'):
        '''
        DOCSTRING: CLOSING PRICE OF BMF CONTRACTS
        INPUTS: USERNAME (COMDINHEIRO), PASSWORD (COMDINHEIRO), CONTRACT CODE, MATURITY CODE, 
            DATE INFERIOR (DDMMAAAA, AS A STRING, OR DATETIME FORMAT), DATE SUPERIOR (SAME FORMAT 
            AS DATE INFERIOR) AND FORMAT EXTRACTION (JSON AS DEFAULT)
        OUTPUTS: JSON
        '''
        # applying date format
        if DatesBR().check_date_datetime_format(date_inf) == True:
            date_inf = DatesBR().datetime_to_string(date_inf, '%d%m%Y')
        if DatesBR().check_date_datetime_format(date_sup) == True:
            date_sup = DatesBR().datetime_to_string(date_sup, '%d%m%Y')
        # payload - self.user / password / contract name / inferior date / superior date / maturity
        #   code / format
        payload = str('username={}&password={}&URL=HistoricoCotacaoBMF-{}-{}-{}'
                      + '-{}-1&format={}').format(self.user, self.passw, contract, date_inf,
                                                  date_sup, maturity_code, format_extraction)
        # sending rest response
        jsonify_message = ComDinheiro().requests_api_cd(payload).text.encode('utf8')
        jsonify_message = HandlingObjects().literal_eval_data(
            jsonify_message, "b'", "'").replace(r'\n', '')
        return jsonify_message

    def indice_neg(self, list_tickers, data_inic, data_fim):
        '''
        DOCSTRING: FUNÇÃO PARA TRAZER O ÍNDICE DE NEGOCIABILIDADE DA B3
        INPUTS: ATIVO, DATA INICIAL E DATA FINAL ('DD/MM/AAAA')
        OUTPUTS: JSON COM O VALOR DO ÍNDICE DE NEGOCIABILIDADE NO PERÍODO
        '''
        # definindo variáveis para pool de conexão
        if type(list_tickers) == list:
            if len(list_tickers) > 1:
                str_acoes_consulta = '%2B'.join(list_tickers)
            else:
                str_acoes_consulta = ''.join(list_tickers)
        elif type(list_tickers) == str:
            str_acoes_consulta = list_tickers
        data_inic = data_inic.strftime('%d~%m~%Y')
        data_fim = data_fim.strftime('%d~%m~%Y')
        payload = 'username={}&password={}&URL=ComparaEmpresas001.php%3F%26d'.format(
            self.user, self.passw) \
            + 'ata_d%3D31129999%26data_a%3D16%2F06%2F2020%26trailing%3D12%26conv%3DMIXED' \
            + '%26c_c%3Dconsolidado%2520preferencialmente%26moeda%3DMOEDA_ORIGINAL' \
            + '%26m_m%3D1000000000%26n_c%3D2%26f_v%3D1%26' \
            + 'papeis%3D' \
            + str_acoes_consulta \
            + '%26indic%3DNEGOCIABILIDADE(' \
            + data_inic \
            + '%2C' \
            + data_fim \
            + '%2C%2C%2C2)' \
            + '%26enviar_email%3D0%26enviar_email_log%3D0%26transpor%3D0%26op01%3D' \
            + 'tabela%26oculta_cabecalho_sup%3D0%26relat_alias_automatico%3' \
            + 'DcMDalias_01&format=json2'
        # fetching data
        response = ComDinheiro().requests_api_cd(payload)
        jsonify_message = ast.literal_eval(StrHandler().find_between(
            str(response.text.encode('utf8')), "b'", "'").replace(r'\n', ''))
        return JsonFiles().send_json(jsonify_message)

    def infos_setoriais(self, data_pregao):
        '''
        DOCSTRING: RETORNA INFORMAÇÕES SETORIAS: TICKER, NOME DA EMPRESA,
        INPUTS: DATA PREGÃO DE INTERESSE
        OUTPUTS: JSON
        '''
        # definindo variável para pool de conexão
        payload = 'username={}&password={}&URL=StockScreenerFull.php%3F%26'.format(
            self.user, self.passw) \
            + 'relat%3D%26data_analise%3D{}%2F{}%2F{}%26data_dem%3D31%2F12%2F'.format(
                data_pregao.strftime('%d'), data_pregao.strftime('%m'),
                data_pregao.strftime('%Y')) \
            + '9999%26variaveis%3DTICKER%2BNOME_EMPRESA%2BDATA_REGISTRO%2BSEGMENTO%2BSETOR%' \
            + '2BSUBSETOR%2BSUBSUBSETOR%2BTIPO_BOVESPA({}~{}~{}%2CTODOS%2C%2C)'.format(
                data_pregao.strftime('%d'), data_pregao.strftime('%m'),
                data_pregao.strftime('%Y')
            ) \
            + '%26segmento%3Dtodos%26setor%3Dtodos%26filtro%3D' \
            + '%26demonstracao%3Dconsolidado%2520preferencialmente%26tipo_acao%3D' \
            + 'Todas%26convencao%3DMIXED%26acumular%3D12%26valores_em%3D1%26num_casas' \
            + '%3D2%26salve%3D%26salve_obs%3D%26var_control%3D0%26overwrite%3D0%26' \
            + 'setor_bov%3Dtodos%26subsetor_bov%3Dtodos%26subsubsetor_bov%3Dtodos%26' \
            + 'group_by%3D%26relat_alias_automatico%3DcMDalias_01%26' \
            + 'primeira_coluna_ticker%3D0%26periodos%3D0%26periodicidade%3Danual%26' \
            + 'formato_data%3D1&format=json2'
        # fetching data
        response = ComDinheiro().requests_api_cd(payload)
        jsonify_message = ast.literal_eval(StrHandler().find_between(
            str(response.text.encode('utf8')), "b'", "'").replace(r'\n', ''))
        return JsonFiles().send_json(jsonify_message)

    @property
    def infos_eventos_corporativos(self):
        '''
        DOCSTRING: RETORNA INFORMAÇÕES CORPORATIVAS: DIVULGAÇÃO DE RESULTADOS,
        INPUTS: DATA PREGÃO DE INTERESSE
        OUTPUTS: JSON
        '''
        # definindo variável para pool de conexão
        payload = 'username={}&password={}&URL=StockScreenerFull.php'.format(
            self.user, self.passw) \
            + '%3F%26relat%3D%26data_analise%3D18%2F06%2F2020%26data_dem%3D' \
            + '31%2F12%2F9999%26variaveis%3DTICKER%2BDATA_ENTREGA_DEM_PRIM%' \
            + '26segmento%3Dtodos%26setor%3Dtodos%26filtro%3D%26demonstracao%3D' \
            + 'consolidado%2520preferencialmente%26tipo_acao%3DTodas%26convencao%3D' \
            + 'MIXED%26acumular%3D12%26valores_em%3D1%26num_casas%3D2%26salve%3D%26' \
            + 'salve_obs%3D%26var_control%3D0%26overwrite%3D0%26setor_bov%3Dtodos%26' \
            + 'subsetor_bov%3Dtodos%26subsubsetor_bov%3Dtodos%26group_by%3D%26' \
            + 'relat_alias_automatico%3DcMDalias_01%26primeira_coluna_ticker%3D0%26' \
            + 'periodos%3D0%26periodicidade%3Danual%26formato_data%3D1&format=json2'
        # fetching data
        response = ComDinheiro().requests_api_cd(payload)
        jsonify_message = ast.literal_eval(StrHandler().find_between(
            str(response.text.encode('utf8')), "b'", "'").replace(r'\n', ''))
        return JsonFiles().send_json(jsonify_message)

    def infos_negociacao(self, data_inf, data_sup):
        '''
        DOCSTRING: RETORNA INFORMAÇÕES SOBRE QUANTIDADE DE NEGÓCIOS, LIQUIDEZ NA BOLSA,
            VALOR DE MERCADO, NOGICIABILIDADE, PESO NO ÍNDICE IBRX100, BTC PARA O PAPEL,
            COMPRAS E VENDAS DE FUNDOS
        INPUTS: DATA PREGÃO DE INTERESSE (INFERIOR E SUPERIOR)
        OUTPUTS: JSON COM TODAS AS AÇÕES DO IBOVESPA
        '''
        # definindo variável para pool de conexão
        payload = 'username={}&password={}&URL=StockScreenerFull.php'.format(
            self.user, self.passw) \
            + '%3F%26relat%3D%26data_analise%3D{}%2F{}%2F{}%26data_dem%3D31'.format(
                data_inf.strftime('%d'), data_inf.strftime(
                    '%m'), data_inf.strftime('%Y')
            ) \
            + '%2F12%2F9999%26variaveis%3DTICKER%2B' \
            + 'VOLUME_MEDIO({}~{}~{}%2C{}~{}~{}%2C)'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y'), data_sup.strftime('%d'),
                data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + '%2BQUANT_NEGOCIOS({}~{}~{}%2C{}~{}~{}%2C%2Cmedia)%2'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y'), data_sup.strftime('%d'),
                data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + 'BMARKET_VALUE%2B' \
            + '%2BLIQUIDEZ_BOLSA({}~{}~{}%2C{}~{}~{}%2C%2C)%2'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y'), data_sup.strftime('%d'),
                data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + 'BNEGOCIABILIDADE({}~{}~{}%2C{}~{}~{}%2C%2C%2C2)%2'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y'), data_sup.strftime('%d'),
                data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + 'BPESO_INDICE(participacao%2CIBRX%2C{}~{}~{}%2C%2C)%2B'.format(
                data_sup.strftime('%d'), data_sup.strftime('%m'),
                data_sup.strftime('%Y')
            ) \
            + 'BTC_ALUGUEL_ACOES(TV%2C{}~{}~{}%2C{}~{}~{})%2B'.format(
                data_inf.strftime('%d'),
                data_inf.strftime('%m'),
                data_inf.strftime('%Y'),
                data_sup.strftime('%d'), data_sup.strftime('%m'),
                data_sup.strftime('%Y')
            ) \
            + 'COMPRAS_VENDAS_FUNDOS(final_valor%2C{}~{}~{}%2C%2C0)'.format(
                data_sup.strftime('%d'), data_sup.strftime('%m'),
                data_sup.strftime('%Y')
            ) \
            + '%2BPRECO_AJ({}~{}~{}%2C%2C%2CA%2CC)'.format(
                data_sup.strftime('%d'), data_sup.strftime('%m'),
                data_sup.strftime('%Y')
            ) \
            + '%2BDY_12M%26' \
            + 'segmento%3Dtodos%26setor%3Dtodos%26filtro%3D%26demonstracao%3D' \
            + 'consolidado%2520preferencialmente%26tipo_acao%3DTodas%26convencao%3D' \
            + 'MIXED%26acumular%3D12%26valores_em%3D1%26num_casas%3D2%26salve%3D%26' \
            + 'salve_obs%3D%26var_control%3D0%26overwrite%3D0%26setor_bov%3Dtodos%26' \
            + 'subsetor_bov%3Dtodos%26subsubsetor_bov%3Dtodos%26group_by%3D%26' \
            + 'relat_alias_automatico%3DcMDalias_01%26primeira_coluna_ticker%3D' \
            + '0%26periodos%3D0%26periodicidade%3Danual%26formato_data%3D1%26' \
            + 'formato_data%3D1&format=json2'
        # fetching data
        response = ComDinheiro().requests_api_cd(payload)
        jsonify_message = ast.literal_eval(StrHandler().find_between(
            str(response.text.encode('utf8')), "b'", "'").replace(r'\n', ''))
        return JsonFiles().send_json(jsonify_message)

    def infos_risco(self, data_inf, data_sup):
        '''
        DOCSTRING: RETORNA INFORMAÇÕES SOBRE VOLATILIDADE 60 MESES ANUALIZADA, VOLATILIADADE
            MENSAL ANUALIZADA, VOLATILIDADE ANUALIZADA YTD, VAR PARAMÉTRICA, EWMA,
            BENCHMARK VAR PARAMÉTRICA (EM RELAÇÃO AO IBOVESPA), MÁXIMO DRAWDOWN
        INPUTS: DATA PREGÃO DE INTERESSE (INFERIOR E SUPERIOR)
        OUTPUTS: JSON COM TODAS AS AÇÕES DO IBOVESPA
        '''
        # definindo variável para pool de conexão
        payload = 'username={}&password={}&URL=StockScreenerFull.php%3F%26'.format(
            self.user, self.passw) \
            + 'relat%3D%26data_analise%3D{}%2F{}%2F{}%26data_dem%3D31%2F12%2F9999'.format(
                data_sup.strftime('%d'), data_sup.strftime('%m'),
                data_sup.strftime('%Y')
            ) \
            + '%26variaveis%3DTICKER%2Bvol_ano_60m%2Bvol_ano_mes_atual%2B' \
            + 'vol_ano_ano_atual%2BVAR_PAR(d%2C{}~{}~{}%2C{}~{}~{}%2C95%2C%2C1)'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y'), data_sup.strftime('%d'),
                data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + '%2BVAR_PAR(d%2C{}~{}~{}%2C{}~{}~{}%2C99%2C%2C1)'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y'), data_sup.strftime('%d'),
                data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + '%2BEWMA({}~{}~{}%2C{}~{}~{}%2C94%2CB%2C%2C0)%2BBENCHMARK_VAR_PAR('.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y'), data_sup.strftime('%d'),
                data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + 'd%2C{}~{}~{}%2C{}~{}~{}%2C95%2C%2C1%2CIBOV)%2BMDD'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y'), data_sup.strftime('%d'),
                data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + '(d%2C{}~{}~{}%2C{}~{}~{}%2Cmdd)%26segmento%3Dtodos%26setor%3Dtodos'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y'), data_sup.strftime('%d'),
                data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + '%26filtro%3D%26demonstracao%3Dconsolidado%2520preferencialmente%26tipo_acao' \
            + '%3DTodas%26convencao%3DMIXED%26acumular%3D12%26valores_em%3D1%26num_casas%3D2' \
            + '%26salve%3D%26salve_obs%3D%26var_control%3D0%26overwrite%3D0%26setor_bov%' \
            + '3Dtodos%26subsetor_bov%3Dtodos%26subsubsetor_bov%3Dtodos%26group_by%3D%26' \
            + 'relat_alias_automatico%3DcMDalias_01%26primeira_coluna_ticker%3D0%26' \
            + 'periodos%3D0%26periodicidade%3Danual%26formato_data%3D1%26' \
            + 'formato_data%3D1&format=json2'
        # fetching data
        response = ComDinheiro().requests_api_cd(payload)
        jsonify_message = ast.literal_eval(StrHandler().find_between(
            str(response.text.encode('utf8')), "b'", "'").replace(r'\n', ''))
        return JsonFiles().send_json(jsonify_message)

    def infos_negociacao_cesta_papeis(self, list_papeis, benchmark, data_inf, data_sup):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # lista de papéis no formato da consulta do json da comdinheiro
        list_papeis = '%2B'.join(list_papeis).upper()
        # definindo variável para pool de conexão
        payload = 'username={}&password={}&URL=ComparaEmpresas001.php'.format(
            self.user, self.passw) \
            + '%3F%26data_d%3D31129999%26data_a%3D{}%2F{}%2F{}%26trailing%3D12%26'.format(
                data_sup.strftime('%d'), data_sup.strftime('%m'),
                data_sup.strftime('%Y')
            ) \
            + 'conv%3DMIXED%26c_c%3Dconsolidado%2520preferencialmente%26moeda%3D' \
            + 'MOEDA_ORIGINAL%26m_m%3D1000000000%26n_c%3D2%26f_v%3D1%26papeis%3D' \
            + '{}'.format(list_papeis) \
            + '%26indic%3DTICKER%2BVOLUME_MEDIO%' \
            + '28{}~{}~{}%2C{}~{}~{}%2C%29%2BQUANT_NEGOCIOS%28'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y'), data_sup.strftime('%d'),
                data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + '{}~{}~{}%2C{}~{}~{}%2C%2Csoma%29%2B%2BMARKET_VALUE%2BLIQUIDEZ_BOLSA%28'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y'), data_sup.strftime('%d'),
                data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + '{}~{}~{}%2C{}~{}~{}%2C%2C%29%2BNEGOCIABILIDADE%28'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y'), data_sup.strftime('%d'),
                data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + '{}~{}~{}%2C{}~{}~{}%2C%2C%2C2%29%2BPESO_INDICE%28'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y'), data_sup.strftime('%d'),
                data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + 'participacao%2C{}%2C{}~{}~{}%2C%2C%29%2BBTC_ALUGUEL_ACOES'.format(
                benchmark, data_sup.strftime('%d'), data_sup.strftime(
                    '%m'), data_sup.strftime('%Y')
            ) \
            + '%28TA%2C{}~{}~{}%2C{}~{}~{}%29%2BCOMPRAS_VENDAS_FUNDOS%28'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y'), data_sup.strftime('%d'),
                data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + 'compras_valor%2C{}~{}~{}%2C%2C0%29%2BPRECO_AJ%28'.format(
                data_sup.strftime('%d'), data_sup.strftime(
                    '%m'), data_sup.strftime('%Y')
            ) \
            + '{}~{}~{}%2C%2C%2CA%2CC%29%2BDY_12M%26enviar_email%3D0%26'.format(
                data_sup.strftime('%d'), data_sup.strftime(
                    '%m'), data_sup.strftime('%Y')
            ) \
            + 'enviar_email_log%3D0%26transpor%3D0%26op01%3Dtabela%26' \
            + 'oculta_cabecalho_sup%3D0%26relat_alias_automatico%3DcMDalias_01&format=json2'
        # print('PAYLOAD: {}'.format(payload))
        # fetching data
        response = ComDinheiro().requests_api_cd(payload)
        # print(response.text)
        jsonify_message = ast.literal_eval(StrHandler().find_between(
            str(response.text.encode('utf8')), "b'", "'").replace(r'\n', ''))
        return JsonFiles().send_json(jsonify_message)

    def mdtv_cesta_papeis(self, list_papeis, data_inf, data_sup, 
                          bl_retornar_df=False, formato_data_input='YYYY-MM-DD'):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # alterando tipo de datas input
        if type(data_inf) == str:
            data_inf = DatesBR().str_date_to_datetime(data_inf, formato_data_input)
        if type(data_sup) == str:
            data_sup = DatesBR().str_date_to_datetime(data_sup, formato_data_input)
        # lista de papéis no formato da consulta do json da comdinheiro
        list_papeis = '%2B'.join(list_papeis).upper()
        # determinando payload de interesse
        payload = 'username={}&password={}&URL=ComparaEmpresas001.php%3F%26data_d%'.format(
            self.user, self.passw) \
            + '3D31129999%26data_a%3D{}%2F{}%2F{}%26trailing%3D12'.format(
                data_sup.strftime('%d'), data_sup.strftime('%m'),
                data_sup.strftime('%Y')
            ) \
            + '%26conv%3DMIXED%26c_c%3Dconsolidado%2520preferencialmente%26moeda%3D' \
            + 'MOEDA_ORIGINAL%26m_m%3D1000000000%26n_c%3D2%26f_v%3D1%26papeis%3D' \
            + '{}'.format(list_papeis) \
            + '%26indic%3DVOLUME_MEDIO%28' \
            + '{}~{}~{}%2C{}~{}~{}%'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y'), data_sup.strftime('%d'),
                data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + '2C%29%26enviar_email%3D0%26enviar_email_log%3D0%26transpor%3D0%26op01%3Dtabela' \
            + '%26oculta_cabecalho_sup%3D0%26relat_alias_automatico%3DcMDalias_01%26s' \
            + 'cript%3D&format=json2'
        # fetching data
        response = ComDinheiro().requests_api_cd(payload)
        jsonify_message = ast.literal_eval(StrHandler().find_between(
            str(str(response.text.encode('utf8')) + "'"), "b'", "''").replace(
                r'\n', '').replace(r' ', ''))
        # retornar dataframe, caso seja de interesse do usuário
        if bl_retornar_df == False:
            return JsonFiles().send_json(jsonify_message)
        else:
            df_mdtv = pd.DataFrame(jsonify_message['resposta']['tab-p0']['linha'])
            return df_mdtv

    def infos_risco_cesta_papeis(self, list_papeis, benchmark, data_inf, data_sup):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # lista de papéis no formato da consulta do json da comdinheiro
        list_papeis = '%2B'.join(list_papeis).upper()
        # definindo variável para pool de conexão
        payload = 'username={}&password={}&URL=ComparaEmpresas001.php%3F%26data_d%'.format(
            self.user, self.passw) \
            + '3D31129999%26data_a%3D{}%2F{}%2F{}%26trailing%3D12'.format(
                data_sup.strftime('%d'), data_sup.strftime('%m'),
                data_sup.strftime('%Y')
            ) \
            + '%26conv%3DMIXED%26c_c%3Dconsolidado%2520preferencialmente%26moeda%3D' \
            + 'MOEDA_ORIGINAL%26m_m%3D1000000000%26n_c%3D2%26f_v%3D1%26papeis%3D' \
            + '{}'.format(list_papeis) \
            + '%26indic%3DTICKER%2Bvol_ano_60m%2B%2Bvol_ano_mes_atual%2Bvol_ano_ano_atual%2BVAR_PAR' \
            + '%28d%2C{}~{}~{}%2C{}~{}~{}%2C95%2C%2C1%29%'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y'), data_sup.strftime('%d'),
                data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + '2BVAR_PAR%28d%2C{}~{}~{}%2C{}~{}~{}%2C99%2C%2C1%29%2BEWMA'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y'), data_sup.strftime('%d'),
                data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + '%28{}~{}~{}%2C{}~{}~{}%2C94%2CB%2C%2C0%29%2BBENCHMARK_VAR_PAR'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y'), data_sup.strftime('%d'),
                data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + '%28d%2C{}~{}~{}%2C{}~{}~{}%2C95%2C%2C1%2C'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y'), data_sup.strftime('%d'),
                data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + '{}%29%2BMDD%28d%2C'.format(benchmark) \
            + '{}~{}~{}%2C26~01~2021%2Cmdd%29%26enviar_email%3D0'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'),
                data_inf.strftime('%Y')
            ) \
            + '%26enviar_email_log%3D0%26transpor%3D0%26op01%3Dtabela' \
            '%26oculta_cabecalho_sup%3D0%26relat_alias_automatico%3DcMDalias_01&format=json2'
        # fetching data
        response = ComDinheiro().requests_api_cd(payload)
        jsonify_message = ast.literal_eval(StrHandler().find_between(
            str(response.text.encode('utf8')), "b'", "'").replace(r'\n', ''))
        return JsonFiles().send_json(jsonify_message)

    def stocks_beta(self, list_tickers, data_inf, data_sup):
        '''
        DOCSTRING: BETA OF PROVIDED STOCKS
        INPUTS:  LIST OF TICKER AND INFERIOR AND SUPERIOR DATES
        OUTPUTS: JSON
        '''
        # lista de papéis no formato da consulta do json da comdinheiro
        list_papeis = '%2B'.join(list_tickers)
        # applying date format
        if DatesBR().check_date_datetime_format(data_inf) == True:
            data_inf = DatesBR().datetime_to_string(data_inf, '%d%m%Y')
        if DatesBR().check_date_datetime_format(data_sup) == True:
            data_sup = DatesBR().datetime_to_string(data_sup, '%d%m%Y')
        # definindo variável para pool de conexão
        # payload = 'username={}&password={}&URL=HistoricoIndicadoresFundos001.php' \
        #    + '%3F%26cnpjs%3D{}'.format(list_papeis) \
        #    + '%26data_ini%3D{}%26data_fim%3D{}'.format(data_inf,data_sup) \
        #    + '%26indicadores%3Dvalor_cota%26op01%3Dtabela_h%26num_casas%3D2%26enviar_email%3D0' \
        #    + '%26periodicidade%3Ddiaria%26cabecalho_excel%3Dmodo1%26transpor%3D0%26asc_desc%3Ddesc' \
        #    + '%26tipo_grafico%3Dlinha%26relat_alias_automatico%3DcMDalias_01&format=json2'
        payload = 'username={}&password={}&URL=HistoricoIndicadoresFundamentalistas001.php'.format(
            self.user, self.passw) \
            + '%3F%26data_ini%3D{}%26data_fim%3D{}'.format(data_inf, data_sup) \
            + '%26trailing%3D12%26conv%3DMIXED%26moeda%3D' \
            + 'BRL%26c_c%3Dconsolidado%26m_m%3D1000000%26n_c%3D2%26f_v%3D1%26papel%3D{}'.format(
                list_papeis) \
            + '%26indic%3Dret_01d%2Bret_cdi_01d%2Bbeta_06m%2BLC%26periodicidade%3Ddu%26graf_' \
            + 'tab%3Dtabela%26desloc_data_analise%3D1%26flag_transpor%3D0%26c_d%3Dd%26enviar_email' \
            + '%3D0%26enviar_email_log%3D0%26' \
            + 'cabecalho_excel%3Dmodo1%26relat_alias_automatico%3DcMDalias_01&format=json2'
        #payload = "username={}&password={}&URL=HistoricoIndicadoresFundamentalistas001.php%3F%26data_ini%3D{}%26data_fim%3D{}%26trailing%3D12%26conv%3DMIXED%26moeda%3DBRL%26c_c%3Dconsolidado%26m_m%3D1000000%26n_c%3D2%26f_v%3D1%26papel%3D{}%26indic%3Dret_01d%2Bret_cdi_01d%2Bbeta_06m%2BLC%26periodicidade%3Ddu%26graf_tab%3Dtabela%26desloc_data_analise%3D1%26flag_transpor%3D0%26c_d%3Dd%26enviar_email%3D0%26enviar_email_log%3D0%26cabecalho_excel%3Dmodo1%26relat_alias_automatico%3DcMDalias_01&format=json2".format(data_inf,data_sup,list_papeis)
        # fetching data
        jsonify_messageOut = ComDinheiro().requests_api_cd(
            payload).read()  # .text.encode('utf8')
        #response = ComDinheiro().requests_api_cd(payload)
        # string_resp = response.read()#.decode('utf8')
        jsonify_message = ast.literal_eval(str(jsonify_messageOut.decode('utf8').replace(
            r'\n', '').replace(r' ', '')))
        return JsonFiles().send_json(jsonify_message)

    def indice_negociabiliade(self, list_tickers, data_inf, data_sup):
        '''
        DOCSTRING: 
        INPUTS:
        OUTPUTS:
        '''
        # lista de papéis no formato da consulta do json da comdinheiro
        list_papeis = '%2B'.join(list_tickers)
        # applying date format
        if DatesBR().check_date_datetime_format(data_inf) == True:
            data_inf = DatesBR().datetime_to_string(data_inf, '%d%m%Y')
        if DatesBR().check_date_datetime_format(data_sup) == True:
            data_sup = DatesBR().datetime_to_string(data_sup, '%d%m%Y')
        # definindo variável para pool de conexão
        payload = 'username={}&password={}&URL=ComparaFundos001.php%3F%26datas'.format(
            self.user, self.passw) \
            + '%3D{}%2F{}%2F{}%26cnpjs'.format(data_sup.strftime('%d'), data_sup.strftime('%m'), 
                data_sup.strftime('%Y')) \
            + '%3D{}'.format(list_papeis) \
            + '%26indicadores%3DNEGOCIABILIDADE%28{}~{}~{}%2C{}~{}~{}'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'), data_inf.strftime('%Y'), 
                data_sup.strftime('%d'), data_sup.strftime('%m'), data_sup.strftime('%Y')) \
            + '%2C%2C%2C2%29%26num_casas%3D2%26pc%3Dnome_fundo%26flag_transpor%3D0%26enviar_email' \
            + '%3D0%26mostrar_da%3D0%26op01%3Dtabela%26oculta_cabecalho_sup%3D0%26r' \
            + 'elat_alias_automatico%3DcMDalias_01&format=json2'
        # fetching data
        jsonify_messageOut = ComDinheiro().requests_api_cd(payload).read()#.text.encode('utf8')
        jsonify_message = ast.literal_eval(str(jsonify_messageOut.decode('utf8').replace(
            r'\n', '').replace(r' ', '')))
        # returning data
        return JsonFiles().send_json(jsonify_message)
    
    def open_ended_funds_quotes(self, list_funds, data_inf, data_sup):
        '''
        DOCSTRING: CLOSING PRICE OF FUNDS' SHARE
        INPUTS:  FUNDS CNPJ (list), MATURITY CODE,
            DATE INFERIOR (DDMMAAAsA, AS A STRING, OR DATETIME FORMAT), DATE SUPERIOR (SAME FORMAT
            AS DATE INFERIOR) AND FORMAT EXTRACTION (JSON AS DEFAULT)
        OUTPUTS: JSON
        '''
        # lista de papéis no formato da consulta do json da comdinheiro
        list_papeis = '%2B'.join(list_funds)
        # applying date format
        if DatesBR().check_date_datetime_format(data_inf) == True:
            data_inf = DatesBR().datetime_to_string(data_inf, '%d%m%Y')
        if DatesBR().check_date_datetime_format(data_sup) == True:
            data_sup = DatesBR().datetime_to_string(data_sup, '%d%m%Y')
        # definindo variável para pool de conexão
        payload = 'username={}&password={}&URL=HistoricoIndicadoresFundos001.php'.format(
            self.user, self.passw) \
            + '%3F%26cnpjs%3D{}'.format(list_papeis) \
            + '%26data_ini%3D{}%26data_fim%3D{}'.format(data_inf, data_sup) \
            + '%26indicadores%3Dvalor_cota%26op01%3Dtabela_h%26num_casas%3D2%26enviar_email%3D0' \
            + '%26periodicidade%3Ddiaria%26cabecalho_excel%3Dmodo1%26transpor%3D0%26asc_desc%3Ddesc' \
            + '%26tipo_grafico%3Dlinha%26relat_alias_automatico%3DcMDalias_01&format=json2'
        # print('PAYLOAD: {}'.format(payload))
        # fetching data
        jsonify_message = ComDinheiro().requests_api_cd(
            payload).read()  # .text.encode('utf8')
        #response = ComDinheiro().requests_api_cd(payload)
        # string_resp = response.read()#.decode('utf8')
        jsonify_message = ast.literal_eval(jsonify_message.decode('utf8').replace(r'\n', '').replace(
            r' ', ''))
        return JsonFiles().send_json(jsonify_message)

    def open_ended_funds_risk_infos(self, list_funds, data_sup):
        '''
        DOCSTRING: RISK INFOS REGARDING HISTORICAL VOLATILITY AND REDEMPTION
        INPUTS:  FUNDS CNPJ (list), DATE SUPERIOR
        OUTPUTS: JSON
        '''
        # lista de papéis no formato da consulta do json da comdinheiro
        list_papeis = '%2B'.join(list_funds)
        # applying date format
        if DatesBR().check_date_datetime_format(data_sup) == True:
            data_sup = DatesBR().datetime_to_string(data_sup, '%d%m%Y')
        # definindo variável para pool de conexão
        payload = 'username={}&password={}&URL=HistoricoIndicadoresFundos001.php'.format(
            self.user, self.passw) \
            + '%3F%26cnpjs%3D{}'.format(list_papeis) \
            + '%26data_ini%3D{}%26data_fim%3D{}'.format(data_sup, data_sup) \
            + '%26indicadores%3Dprazo_liq_resg%2B' \
            + 'prazo_disp_rec_resgatado%2Bvol_ano_01m%2Bvol_ano_12m%2B' \
            + 'vol_ano_36m%2Bvol_ano_60m%2Bvol_ano_ano_atual%2Bvol_ano_mes_atual%2Bresgate_min%2Btaxa_saida%26op01%3Dtabela_h%26' \
            + 'num_casas%3D2%26enviar_email%3D0%26periodicidade%3Ddiaria%26cabecalho_excel%3Dmodo1%26transpor%3D0%26asc_desc%3D' \
            + 'desc%26tipo_grafico%3Dlinha%26relat_alias_automatico%3DcMDalias_01&format=json2'
        # print('PAYLOAD: {}'.format(payload))
        # fetching data
        jsonify_message = ComDinheiro().requests_api_cd(
            payload).read()  # .text.encode('utf8')
        #response = ComDinheiro().requests_api_cd(payload)
        # string_resp = response.read()#.decode('utf8')
        jsonify_message = ast.literal_eval(jsonify_message.decode('Latin-1').replace(
            r'\n', '').replace(r' ', ''))
        return JsonFiles().send_json(jsonify_message)

    def open_ended_funds_sharpe_dd(self, list_cnpjs, data_inf, data_sup):
        '''
        DOCSTRING: CÁLCULO DE SHARPE E DROWDOWN POR PERÍODO
        INPUTS:
        OUTPUTS:
        '''
        # lista de papéis no formato da consulta do json da comdinheiro
        list_cnpjs = '%2B'.join(list_cnpjs)
        # definindo variável para pool de conexão
        payload = 'username={}&password={}&URL=ComparaFundos001.php%3F%26datas'.format(
            self.user, self.passw) \
            + '%3D{}%2F{}%2F{}%26cnpjs'.format(data_sup.strftime('%d'), data_sup.strftime('%m'), 
                data_sup.strftime('%Y')) \
            + '%3D{}'.format(list_cnpjs) \
            + '%26indicadores%3Dnome_fundo%2Bcnpj_fundo%2Bret_12m_aa%2Bpatrimonio~1e6%2B' \
            + 'cotistas%2Bcaptacao~1e6%2Bresgate~1e6%2' \
            + 'Bvol_ano_24m%2Bsharpe_12m%2BMDD%28d%2C{}~{}~{}'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'), data_inf.strftime('%Y')) \
            + '%2C{}~{}~{}%2Cmdd%29%'.format(
                data_sup.strftime('%d'), data_sup.strftime('%m'), data_sup.strftime('%Y')) \
            + '2BMDD%28d%2C{}~{}~{}%'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'), data_inf.strftime('%Y')) \
            + '2C{}~{}~{}%2'.format(
                data_sup.strftime('%d'), data_sup.strftime('%m'), data_sup.strftime('%Y')) \
            + 'Ctempo_rec%29%2Bobjetivo%2Bclasse%26num_casas%3D2%26pc%3Dnome_fundo%26f' \
            + 'lag_transpor%3D0%26enviar_email%3D0%26mostrar_da%3D0%26op01%3Dtabela%26' \
            + 'oculta_cabecalho_sup%3D0%26relat_alias_automatico%3DcMDalias_01&format=json2'
        # fetching data
        jsonify_messageOut = ComDinheiro().requests_api_cd(payload).text.encode('utf8')#.read()
        jsonify_message = ast.literal_eval(str(jsonify_messageOut.decode('utf8').replace(
            r'\n', '').replace(r' ', '')))
        # returning data
        return JsonFiles().send_json(jsonify_message)
    
    def open_ended_funds_infos(self, list_cnpjs, data_inf, data_sup, 
                               str_formato_data_input='YYYY-MM-DD', 
                               du_ant_cd=2, list_dicts=list(), 
                               col_classe_anbima='CLASSE_ANBIMA_DO_FUNDO', 
                               col_nome_fundo='NOME_FUNDO', 
                               col_like_max_drawdown='MDD*', 
                               col_mdd_final='MAX_DRAWDOWN', col_cnpj_sem_mascara='CNPJ_SEM_MASCARA', 
                               col_cnpj='CNPJ_DO_FUNDO'):
        '''
        DOCSTRING: FUND'S NAME, CNPJ, CORPORATE NAME, SHARPE WITH A RANGE OF TEMPORAL WINDOWS, 
            MAXIMUM DRAWDOWN (GIVEN A PERIOD OF TIME), ANBIMA'S CLASS, CODE
        INPUTS: LIST OF CNOJS, DATE INFERIOR AND SUPERIOR
        OUTPUTS: DATAFRAME
        '''
        # alterando tipo das datas para date
        if type(data_inf) != date:
            data_inf = DatesBR().str_date_to_datetime(data_inf, str_formato_data_input)
        if type(data_sup) != date:
            data_sup = DatesBR().str_date_to_datetime(data_sup, str_formato_data_input)
        # data anterior comdinheiro de referência
        df_ref_cd = DatesBR().sub_working_days(data_sup, du_ant_cd)
        # lista de papéis no formato da consulta do json da comdinheiro
        list_cnpjs = '%2B'.join(list_cnpjs)
        # definindo variável para pool de conexão
        payload = 'username={}&password={}&URL=ComparaFundos001.php%3F%26'.format(
            self.user, self.passw) \
            + 'datas%3D{}%2F{}%2F{}%26'.format(df_ref_cd.strftime('%d'), df_ref_cd.strftime('%m'), 
                df_ref_cd.strftime('%Y')) \
            + 'cnpjs%3D{}%26'.format(list_cnpjs) \
            + 'indicadores%3Dcnpj_fundo%2Bnome_fundo%2Bsharpe_12m%2Bsharpe_24m%2Bsharpe_' \
            + '36m%2Bsharpe_36m%2Bsharpe_48m%2Bsharpe_60m' \
            + '%2BMDD%28d%2C{}~{}~{}%2C{}~{}~{}%2Cmdd%29'.format(
                data_inf.strftime('%d'), data_inf.strftime('%m'), data_inf.strftime('%Y'), 
                data_sup.strftime('%d'), data_sup.strftime('%m'), data_sup.strftime('%Y')
            ) \
            + '%2Bclasse_anbima%2Bcodigo_anbima%26num_casas%3D2%26pc%3Dnome_fundo%26flag_transpor' \
            + '%3D0%26enviar_email%3D0%26mostrar_da%3D0%26op01%3Dtabela%26oculta_cabecalho_sup%3D0' \
            + '%26relat_alias_automatico%3DcMDalias_01&format=json3'
        # fetching data
        json_message = ComDinheiro().requests_api_cd(payload).json()
        # definindo colunas do dataframe de exportação da cd
        list_cols = [StrHandler().remove_diacritics(
            StrHandler().latin_characters(str(x))).strip().replace(' ', '_').replace(
            df_ref_cd.strftime('%d/%m/%Y'), '').upper() for x in list(json_message[
                'tables']['tab0']['lin0'].values())]
        # definindo lista serializada para importação em dataframe
        for index, dict_ in json_message['tables']['tab0'].items():
            if index != 'lin0':
                list_dicts.append({col_nome: dict_['col{}'.format(i)] for i, 
                                   col_nome in enumerate(list_cols)})
        # definindo dataframe à partir de lista de dicionários serializados
        df_infos_fundos_cd = pd.DataFrame(list_dicts)
        # substituindo valores nulos por zero
        for col_ in list_cols:
            df_infos_fundos_cd[col_] = [str(x).replace('', '0') if len(str(x)) <= 1 
                                        else str(x) for x in df_infos_fundos_cd[col_].tolist()]
            df_infos_fundos_cd[col_] = [str(x).replace(',', '.') for x in df_infos_fundos_cd[
                col_].tolist()]
        # alterando tipo de colunas de interesse
        df_infos_fundos_cd = df_infos_fundos_cd.astype({
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
            list_cols[11]: int
        })
        # renomeando colunas de interesse
        col_mdd = [x for x in list_cols if StrHandler().match_string_like(
            x, col_like_max_drawdown) == True][0]
        df_infos_fundos_cd.rename(columns={
            col_mdd: col_mdd_final
        }, inplace=True)
        # retirando caractéres latinos do campo de interesse e mantendo texto em caixa alta
        for col_ in [col_nome_fundo, col_classe_anbima]:
            df_infos_fundos_cd[col_] = [StrHandler().remove_diacritics(
                StrHandler().latin_characters(str(x))).upper() 
                for x in df_infos_fundos_cd[col_].tolist()]
        # criando coluna com cnpj sem máscara
        df_infos_fundos_cd[col_cnpj_sem_mascara] = DocumentsNumbersBR(
            df_infos_fundos_cd[col_cnpj].tolist()).unmask_number
        # retornando dataframe de interesse
        return df_infos_fundos_cd

# print(YFinance().indice_neg(['WEGE3', 'MDIA3', 'BBAS3', 'ITSA4'], DatesBR().sub_working_days(
#     DatesBR().curr_date, -52), DatesBR().curr_date))
# print(YFinance().acoes_ibrx100())
# print(YFinance().infos_setoriais(DatesBR().sub_working_days(
#     DatesBR().curr_date, -1)))
# print(YFinance().infos_eventos_corporativos())
# print(YFinance().infos_negociacao(DatesBR().sub_working_days(
#     DatesBR().curr_date, -53), DatesBR().sub_working_days(
#     DatesBR().curr_date, -1)))
# print(YFinance().infos_risco(DatesBR().sub_working_days(
#     DatesBR().curr_date, -53), DatesBR().sub_working_days(
#     DatesBR().curr_date, -1)))
# print(YFinance().ativos_bov())
# pprint(YFinance().cotacoes(['PETR4', 'VALE3']))
# print(YFinance().cotacoes_serie_historica('PETR4', '25/08/2020', '28/08/2020'))
# print(YFinance().cotacoes_serie_historica(
#     ['PETR4', 'VALE3'], '25/08/2020', '28/08/2020'))

# pprint(YFinance().cotacoes_serie_historica(['GOOG', 'AMZN', 'TSLA'], pais='United States'))
# {'AMZN': {'historical': [{'close': 3294.62,
#                           'currency': 'USD',
#                           'date': '03/09/2020',
#                           'high': 3381.5,
#                           'low': 3111.13,
#                           'open': 3318.0,
#                           'volume': 8781754},
#                          {'close': 3149.84,
#                           'currency': 'USD',
#                           'date': '07/09/2020',
#                           'high': 3250.0,
#                           'low': 3131.0,
#                           'open': 3139.59,
#                           'volume': 6094205}],
#           'name': 'Amazon.com'},
#  'GOOG': {'historical': [{'close': 1591.04,
#                           'currency': 'USD',
#                           'date': '03/09/2020',
#                           'high': 1645.11,
#                           'low': 1547.61,
#                           'open': 1624.26,
#                           'volume': 2608568},
#                          {'close': 1532.39,
#                           'currency': 'USD',
#                           'date': '07/09/2020',
#                           'high': 1562.51,
#                           'low': 1528.39,
#                           'open': 1533.01,
#                           'volume': 2610884}],
#           'name': 'Alphabet C'},
#  'TSLA': {'historical': [{'close': 418.32,
#                           'currency': 'USD',
#                           'date': '03/09/2020',
#                           'high': 428.0,
#                           'low': 372.02,
#                           'open': 402.81,
#                           'volume': 110321904},
#                          {'close': 330.21,
#                           'currency': 'USD',
#                           'date': '07/09/2020',
#                           'high': 368.59,
#                           'low': 330.01,
#                           'open': 355.75,
#                           'volume': 115465688}],
#           'name': 'Tesla'}}

# pprint(YFinance().cotacoes_serie_historica(
#     ['GOGL34', 'TSLA34'], classes_ativos=['acao', 'acao']))
# print(YFinance().cotacoes_serie_historica('BOVA11', classes_ativos='etf'))
# print(investpy.get_etf_historical_data(
#     'BOVA11', 'brazil', '08/09/2020', '11/09/2020', as_json=True))
# print(investpy.get_etf_recent_data('SPY', 'United States', as_json=True))
# pprint(investpy.get_etf_recent_data('EWZ', 'united states', as_json=True))
# pprint(investpy.get_etfs_list(country='brazil'))
# pprint(investpy.get_etfs_overview(country='brazil'))
# pprint(investpy.get_etfs_dict(country='brazil', as_json=True))
# pprint(investpy.get_etf_recent_data(
#     'Ishares Ibovespa', country='brazil', as_json=True))
# pprint(YFinance().cotacoes_serie_historica(
#     ['ABEV3', 'QUAL3'], classes_ativos=['acao', 'acao']))
# print(investpy.get_etf_historical_data(
#     'Ishares Ibovespa', 'brazil', '08/09/2020', '11/09/2020', as_json=True))
# pprint(investpy.get_etfs_dict('brazil'))

# pprint(YFinance().cotacoes_serie_historica(['BOVA11', 'ABEV3', 'PETR4'], '08/09/2020',
#                                          '11/09/2020', pais='brazil',
#                                          classes_ativos=['etf', 'acao', 'acao']))
# print(YFinance().cotacoes_serie_historica(['AALR3', 'VALE3']))
# output
# {'AALR3': {'name': 'Centro de Imagem Diagnosticos', 'historical': [{'date': '23/11/2020', 'open': 10.86, 'high': 11.05, 'low': 10.67, 'close': 11.03, 'volume': 376200, 'currency': 'BRL'}, {'date': '24/11/2020', 'open': 10.97, 'high': 11.0, 'low': 10.72, 'close': 10.73, 'volume': 477700, 'currency': 'BRL'}]}, 'VALE3': {'name': 'VALE ON', 'historical': [{'date': '23/11/2020', 'open': 68.86, 'high': 71.32, 'low': 68.55, 'close': 71.29, 'volume': 38620700, 'currency': 'BRL'}, {'date': '24/11/2020', 'open': 71.09, 'high': 74.89, 'low': 70.18, 'close': 74.8, 'volume': 49910300, 'currency': 'BRL'}]}}
# pprint([d['close'] for d in HandlingObjects().literal_eval_data(investpy.get_stock_historical_data(
#     'AALR3', 'brazil', DatesBR().sub_working_days(
#         DatesBR().curr_date, 15).strftime('%d/%m/%Y'),
#     DatesBR().curr_date.strftime('%d/%m/%Y'), as_json=True))['historical']])

# list_papeis = ['GOGL34', 'TSLA34', 'IVVB11', 'XPML11']
# # IFIX, BDRX, IBRX
# benchmark = 'IFIX'
# data_inf = DatesBR().sub_working_days(DatesBR().curr_date, -53)
# data_sup = DatesBR().sub_working_days(DatesBR().curr_date, -1)
# print(YFinance().infos_risco_cesta_papeis(list_papeis, benchmark, data_inf, data_sup))

# print(YFinance().bmf_historical_close_data('{}', '{}', 'DOL'))

# print(YFinance().indice_historical_data('04/03/2021', '05/03/2021'))
# # output
# {'04/03/2021': 112690.17, '05/03/2021': 115202.23}

# print(investpy.economic_calendar(countries=['brazil'],
#                                  from_date='01/01/2021', to_date='12/03/2021'))

# print(investpy.stocks.get_stock_information(
#     'PETR4', country='brazil', as_json=True))

# ticker = YFinance().ticker_reference_investing_com('ENEV3')
# from_date_timestamp = '0'
# to_date_timestamp = '1631836346'
# print(YFinance().historical_closing_intraday_data_investing_com(ticker, from_date_timestamp,
#                                                               to_date_timestamp))
# list_tickers = ['PETR4.SA', 'MGLU3.SA']
# print(pd.DataFrame(YFinance().historical_closing_data_yf(list_tickers)))
