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
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.handling_data.numbers import NumHandler
from stpstone.utils.loggs.db_logs import DBLogs


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
