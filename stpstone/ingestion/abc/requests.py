### ABSTRACT BASE CLASS - REQUESTS ###

# pypi.org libs
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime
from requests import exceptions
from typing import Tuple, List, Dict, Any, Optional
from sqlalchemy.orm import Session
from logging import Logger
# project modules
from stpstone.parsers.folders import DirFilesManagement
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.parsers.dicts import HandlingDicts
from stpstone.utils.loggs.db_logs import DBLogs
from stpstone.connections.netops.session import ReqSession
from stpstone.transformations.standardization.dataframe import DFStandardization
from stpstone.utils.loggs.create_logs import CreateLog


class ABCRequests(ABC):

    def __init__(
        self,
        dict_metadata:Dict[str, Any], 
        bl_create_session:bool=False,
        bl_new_proxy:bool=False,
        bl_verify:bool=False,
        str_host:Optional[str]=None,
        dt_ref:datetime=DatesBR().curr_date,
        dict_headers:Optional[Dict[str, str]]=None,
        dict_payload:Optional[Dict[str, str]]=None, 
        session:Optional[ReqSession]=None,
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None
    ) -> None:
        self.dict_metadata = dict_metadata
        self.bl_create_session = bl_create_session
        self.bl_new_proxy = bl_new_proxy
        self.bl_verify = bl_verify
        self.str_host = str_host
        self.dt_ref = dt_ref
        self.dict_headers = dict_headers
        self.dict_payload = dict_payload
        self.session = session if session is not None else self.session_config
        self.cls_db = cls_db
        self.logger = logger

    @property
    def session_config(self):
        if self.bl_create_session == True:
            session = ReqSession(bl_new_proxy=self.bl_new_proxy).session
        else:
            session = None
        return session

    def generic_req(
        self, 
        app:str, dict_dtypes:Dict[str, Any], list_cols_dt:List[str],
        bl_io_interpreting:bool=False, 
        tup_timeout:Tuple[float, float]=(12.0, 12.0)
    ) -> pd.DataFrame:
        url = self.str_host + app
        if self.logger is not None:
            CreateLog().infos(self.logger, 'Requesting data from: {}'.format(url))
        print(f'URL: {url}')
        print('DICT DTYPES: {}'.format(dict_dtypes))
        # requesting for data
        if url.endswith('.zip') == True:
            file = DirFilesManagement().get_zip_from_web_in_memory(
                url, 
                bl_verify=self.bl_verify, 
                bl_io_interpreting=bl_io_interpreting, 
                timeout=tup_timeout,
                dict_headers=self.dict_headers,
                session=self.session
            )
            reader = pd.read_csv(file, sep=';', header=None, names=list(dict_dtypes.keys()), 
                decimal=',', thousands='.')
            df_ = pd.DataFrame(reader)
            df_['FILE_NAME'] = file.name
            df_['REF_DATE'] = str(DatesBR().year_number(self.dt_ref)) \
                + str(DatesBR().month_number(self.dt_ref, bl_month_mm=True))
        else:
            req_resp = self.session.get(url, verify=self.bl_verify, timeout=tup_timeout, 
                                    headers=self.dict_headers, payload=self.dict_payload)
            req_resp.raise_for_status()
            json_ = req_resp.json()
            reader = pd.DataFrame(json_)
            df_ = pd.DataFrame(reader)
        # standardizing
        df_ = DFStandardization()._pipeline(
            df_, 
            HandlingDicts().merge_n_dicts(dict_dtypes, self.dict_metadata['generic']['dtypes']), 
            list_cols_dt
        )
        if self.cls_db is not None:
            df_ = DBLogs().audit_log(
                df_, 
                url, 
                DatesBR().build_date(
                    DatesBR().year_number(self.dt_ref), 
                    DatesBR().month_number(self.dt_ref, bl_month_mm=False), 
                    1
                )
            )
        return df_

    def non_iteratively_get_raw_data(self, str_resource:str) -> pd.DataFrame:
        """
        Non-iteratively get raw data
        Args:
            - str_resource (str): resource name
        Return:
            pd.DataFrame
        """
        return self.generic_req(
            self.dict_metadata[str_resource]['app'], 
            self.dict_metadata[str_resource]['dtypes'],
            self.dict_metadata[str_resource]['list_cols_dt'],
            bl_io_interpreting=self.dict_metadata[str_resource]['bl_io_interpreting']
        )

    def iteratively_get_raw_data(self, str_resource:str, i:int=0) -> pd.DataFrame:
        """
        Iteratively get raw data
        Args:
            - str_resource (str): resource name
        Return:
            pd.DataFrame
        """
        list_ser = list()
        while True:
            try:
                list_ser.extend(
                    self.generic_req(
                        self.dict_metadata[str_resource]['app_fstr'].format(i), 
                        self.dict_metadata[str_resource]['dtypes'],
                        self.dict_metadata[str_resource].get('list_cols_dt', []) \
                            if self.dict_metadata[str_resource].get('list_cols_dt', []) \
                            is not None else [],   
                        bl_io_interpreting=self.dict_metadata[str_resource]['bl_io_interpreting']
                    ).to_dict(orient='records')
                )
                i += 1
            except exceptions.HTTPError:
                break
        return pd.DataFrame(list_ser)

    def get_raw_data(self, str_resource:str) -> pd.DataFrame:
        """
        Get raw data from brazillian federal revenue office (RFB for Brazillian acronym)
        Args:
            - str_resource (str): resource name
        Return:
            pd.DataFrame
        """
        if 'app_fstr' in self.dict_metadata[str_resource]:
            return self.iteratively_get_raw_data(str_resource)
        else:
            return self.non_iteratively_get_raw_data(str_resource)

    def insert_table(self, str_resource:str, bl_insert_or_ignore:bool=False, 
        bl_schema:bool=True) -> None:
        """
        Insert data into data table
        Args:
            - str_resource (str): resource name
            - bl_insert_or_ignore(bool): False as default
            - bl_schema(bool): some databases, like SQLite, doesn't have schemas - True as default
        Return:
            pd.DataFrame
        """
        if self.cls_db == None: 
            raise Exception('data insertion failed due to lack of database definition, '
                + 'please revisit this parameter')
        if str_resource == 'appendix':
            for _, dict_ in self.dict_metadata[str_resource].items():
                if bl_schema == False:
                    str_table_name = '{}_{}'.format(
                        dict_['schema'], 
                        dict_['table_name']
                    )
                self.cls_db._insert(
                    dict_['data'], 
                    str_table_name=str_table_name,
                    bl_insert_or_ignore=True
                )
        else:
            if bl_schema == False:
                str_table_name = '{}_{}'.format(
                    self.dict_metadata[str_resource]['schema'], 
                    self.dict_metadata[str_resource]['table_name']
                )
            self.cls_db._insert(
                self.get_raw_data(str_resource), 
                str_table_name=str_table_name,
                bl_insert_or_ignore=bl_insert_or_ignore
            )

    def insert_tables(self, bl_insert_or_ignore:bool=False, bl_schema:bool=True) -> None:
        """
        Insert data into data tables
        Args:
            - str_resource (str): resource name
            - bl_insert_or_ignore(bool): False as default
            - bl_schema(bool): some databases, like SQLite, doesn't have schemas - True as default
        Return:
            pd.DataFrame
        """
        for key in list(self.dict_metadata.keys()):
            if key != 'generic':
                self.insert_table(key, bl_insert_or_ignore, bl_schema)