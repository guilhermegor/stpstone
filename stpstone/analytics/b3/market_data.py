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