### INOA SYSTEMS ###

# pypi.org libs
import json
import backoff
import pandas as pd
from requests import request, exceptions
from typing import List, Dict, Any
from datetime import datetime
# local libs
from stpstone._config.global_slots import YAML_INOA
from stpstone.utils.cals.handling_dates import DatesBR


class AlphaTools:

    def __init__(self, str_user:str, str_passw:str, str_host:str, str_instance:str, 
        dt_inf:datetime, dt_sup:datetime, str_fmt_date_output:str='YYYY-MM-DD', 
        bl_debug_mode:bool=False):
        """
        Connection to INOA Alpha Tools API
        Args:
            - str_user (str): username
            - str_passw (str): password
            - str_host (str): host
            - str_instance (str): instance
            - dt_inf (datetime): start date
            - dt_sup (datetime): end date
            - str_fmt_date_output (str): format date output
            - bl_debug_mode (bool): debug mode
        Returns:
            None
        """
        self.str_user = str_user
        self.str_passw = str_passw
        self.str_host = str_host
        self.str_instance = str_instance
        self.dt_inf = dt_inf
        self.dt_sup = dt_sup
        self.str_fmt_date_output = str_fmt_date_output
        self.bl_debug_mode = bl_debug_mode if bl_debug_mode is not None else True

    @backoff.on_exception(
        backoff.constant,
        exceptions.RequestException,
        interval=10,
        max_tries=20,
    )
    def generic_req(self, str_method:str, str_app:str, dict_params:dict) -> List[Dict[str, Any]]:
        if self.bl_debug_mode == True:
            print(
                f'METHOD: {str_method}',
                f'HOST: {self.str_host}',
                f'APP: {str_app}',
                f'DICT PARAMS: {dict_params}',
                f'USER: {self.str_user}',
                f'PASSW:{self.str_passw}'
            )
        req_resp = request(str_method, url=self.str_host+str_app, json=dict_params, 
            auth=(self.str_user, self.str_passw))
        req_resp.raise_for_status()
        return req_resp.json()

    @property
    def funds(self) -> pd.DataFrame:
        dict_params = {
            YAML_INOA['alpha_tools']['funds']['key_values']: [
                YAML_INOA['alpha_tools']['funds']['col_id'], 
                YAML_INOA['alpha_tools']['funds']['col_name'], 
                YAML_INOA['alpha_tools']['funds']['col_legal_id']
            ],
            YAML_INOA['alpha_tools']['funds']['key_is_active']: None
        }
        json_req = self.generic_req(
            YAML_INOA['alpha_tools']['funds']['method'],
            YAML_INOA['alpha_tools']['funds']['app'], 
            dict_params
        )
        df_funds = pd.DataFrame.from_dict(json_req, orient='index')
        if self.bl_debug_mode == True:
            print(f'DF_FUNDS: \n{df_funds}')
        df_funds = df_funds.astype({
            YAML_INOA['alpha_tools']['funds']['col_id']: int,
            YAML_INOA['alpha_tools']['funds']['col_name']: str,
            YAML_INOA['alpha_tools']['funds']['col_legal_id']: str
        })
        df_funds[YAML_INOA['alpha_tools']['funds']['col_origin']] = self.str_instance
        df_funds.columns = [x.upper() for x in df_funds.columns]
        return df_funds

    def quotes(self, list_ids:List[int]) -> pd.DataFrame:
        dict_params = {
            YAML_INOA['alpha_tools']['quotes']['key_funds_ids']: list_ids,
            YAML_INOA['alpha_tools']['quotes']['key_start_dt']: self.dt_inf.strftime(
                '%Y-%m-%d'),
            YAML_INOA['alpha_tools']['quotes']['key_sup_dt']: self.dt_sup.strftime(
                '%Y-%m-%d'),
        }
        json_req = self.generic_req(
            YAML_INOA['alpha_tools']['quotes']['method'],
            YAML_INOA['alpha_tools']['quotes']['app'], 
            dict_params
        )
        df_quotes = pd.DataFrame(json_req[
            YAML_INOA['alpha_tools']['quotes']['key_items']])
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
        df_quotes.columns = [x.upper() for x in df_quotes.columns]
        return df_quotes