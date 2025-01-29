### INOA SYSTEMS ###

import json
import backoff
import pandas as pd
from requests import request, exceptions
from typing import List
from datetime import datetime
from stpstone.settings._global_slots import YAML_INOA
from stpstone.cals.handling_dates import DatesBR


class AlphaTools:

    def __init__(self, str_user:str, str_passw:str, str_host:str, str_instance:str, 
        dt_start:datetime, dt_end:datetime, str_fmt_date_output:str='YYYY-MM-DD', 
        bl_debug_mode:bool=False):
        '''
        DOCSTRING: CONNECTION TO INOA SERVICE ALPHATOOLS
        INPUTS: USER, PASSWORD, HOST, INSTANCE, LIST OF EINS, START AND END DATES
        OUTPUTS: -
        '''
        self.str_user = str_user
        self.str_passw = str_passw
        self.str_host = str_host
        self.str_instance = str_instance
        self.dt_start = dt_start
        self.dt_end = dt_end
        self.str_fmt_date_output = str_fmt_date_output
        self.bl_debug_mode = bl_debug_mode if bl_debug_mode is not None else True

    @backoff.on_exception(
        backoff.constant,
        exceptions.RequestException,
        interval=10,
        max_tries=20,
    )
    def generic_req(self, str_method:str, str_app:str, dict_params:dict) -> json:
        '''
        DOCSTRING: GENERIC REQUEST TO ALPHA TOOLS API
        INPUTS: METHOD, APP, PARAMETERS
        OUTPUTS: JSON
        '''
        if self.bl_debug_mode == True:
            print(
                f'METHOD: {str_method}',
                f'HOST: {self.str_host}',
                f'APP: {str_app}',
                f'DICT PARAMS: {dict_params}',
                f'USER: {self.str_user}',
                f'PASSW:{self.str_passw}'
            )
        resp_req = request(str_method, url=self.str_host+str_app, json=dict_params, 
            auth=(self.str_user, self.str_passw))
        resp_req.raise_for_status()
        return resp_req.json()

    @property
    def funds(self) -> pd.DataFrame:
        '''
        DOCSTRING: FUNDS INFOS, WITHIN A GIVEN SET
        INPUTS: - 
        OUTPUTS: DATAFRAME
        '''
        # setting initial variables
        dict_params = {
            YAML_INOA['alpha_tools']['funds']['key_values']: [
                YAML_INOA['alpha_tools']['funds']['col_id'], 
                YAML_INOA['alpha_tools']['funds']['col_name'], 
                YAML_INOA['alpha_tools']['funds']['col_legal_id']
            ],
            YAML_INOA['alpha_tools']['funds']['key_is_active']: None
        }
        # requesting infos
        json_req = self.generic_req(
            YAML_INOA['alpha_tools']['funds']['method'],
            YAML_INOA['alpha_tools']['funds']['app'], 
            dict_params
        )
        # retrieving to dataframe in memory
        df_funds = pd.DataFrame.from_dict(json_req, orient='index')
        if self.bl_debug_mode == True:
            print(f'DF_FUNDS: \n{df_funds}')
        # changing columns types
        df_funds = df_funds.astype({
            YAML_INOA['alpha_tools']['funds']['col_id']: int,
            YAML_INOA['alpha_tools']['funds']['col_name']: str,
            YAML_INOA['alpha_tools']['funds']['col_legal_id']: str
        })
        # including columns of interest
        df_funds[YAML_INOA['alpha_tools']['funds']['col_origin']] = self.str_instance
        # changing columns case
        df_funds.columns = [x.upper() for x in df_funds.columns]
        # returning dataframe of funds
        return df_funds

    def quotes(self, list_ids:List[int]) -> pd.DataFrame:
        '''
        DOCSTRING: FUNDS' QUOTES
        INPUTS: -
        OUTPUTS: DATAFRAME
        '''
        # setting initial variables
        dict_params = {
            YAML_INOA['alpha_tools']['quotes']['key_funds_ids']: list_ids,
            YAML_INOA['alpha_tools']['quotes']['key_start_dt']: self.dt_start.strftime(
                '%Y-%m-%d'),
            YAML_INOA['alpha_tools']['quotes']['key_end_dt']: self.dt_end.strftime(
                '%Y-%m-%d'),
        }
        # requesting infos
        json_req = self.generic_req(
            YAML_INOA['alpha_tools']['quotes']['method'],
            YAML_INOA['alpha_tools']['quotes']['app'], 
            dict_params
        )
        # print(f'JSON_QUOTES_AT: {json_req}')
        # retrieving to dataframe in memory
        df_quotes = pd.DataFrame(json_req[
            YAML_INOA['alpha_tools']['quotes']['key_items']])
        # changing columns types
        if self.bl_debug_mode == True:
            print(f'DF_QUOTES: \n{df_quotes}')
        df_quotes = df_quotes.astype({
            YAML_INOA['alpha_tools']['quotes']['col_fund_id']: int,
            YAML_INOA['alpha_tools']['quotes']['col_date']: str,
            YAML_INOA['alpha_tools']['quotes']['col_status_display']: str
        })
        df_quotes[YAML_INOA['alpha_tools']['quotes']['col_date']] = [
            DatesBR().str_date_to_datetime(d, self.str_fmt_date_output) 
            for d in df_quotes[YAML_INOA['alpha_tools']['quotes']['col_date']]
        ]
        # changing columns case
        df_quotes.columns = [x.upper() for x in df_quotes.columns]
        # returning dataframe
        return df_quotes