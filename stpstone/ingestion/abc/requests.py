### ABSTRACT BASE CLASS - REQUESTS ###

# pypi.org libs
import backoff
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime
from requests import exceptions, request, Response
from typing import Tuple, List, Dict, Any, Optional
from sqlalchemy.orm import Session
from logging import Logger
from zipfile import BadZipFile
# project modules
from stpstone.parsers.folders import DirFilesManagement
from stpstone.parsers.dicts import HandlingDicts
from stpstone.parsers.str import StrHandler
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.loggs.db_logs import DBLogs
from stpstone.connections.netops.session import ReqSession
from stpstone.transformations.standardization.dataframe import DFStandardization
from stpstone.utils.loggs.create_logs import CreateLog


class ABCRequests(ABC):

    def __init__(
        self,
        dict_metadata:Dict[str, Any],
        session:Optional[ReqSession]=None,
        dt_ref:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None
    ) -> None:
        self.dict_metadata = dict_metadata
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.token = self.access_token

    @abstractmethod
    def req_trt_injection(self, req_resp:Response) -> Optional[pd.DataFrame]:
        return None

    @property
    def access_token(self):
        url_token = StrHandler().fill_fstr_from_globals(
            self.dict_metadata['credentials']['get_token']['host_token']
        )
        req_resp = self.generic_req(
            self.dict_metadata['credentials']['get_token']['req_method'], 
            url_token, 
            self.dict_metadata['credentials']['get_token']['bl_verify'], 
            self.dict_metadata['credentials']['get_token']['timeout']
        )
        req_resp.raise_for_status()
        return req_resp.json()[self.dict_metadata['credentials']['keys']['token']]

    def generic_req_w_session(
        self, 
        req_method:str,
        url:str,
        bl_verify:bool,
        tup_timeout:Tuple[float, float]=(12.0, 12.0), 
        dict_headers:Optional[Dict[str, str]]=None,
        dict_payload:Optional[Dict[str, str]]=None
    ) -> Response:
        if req_method == 'GET':
            req_resp = self.session.get(url, verify=bl_verify, timeout=tup_timeout, 
                                        headers=dict_headers, payload=dict_payload)
        elif req_method == 'POST':
            req_resp = self.session.post(url, verify=bl_verify, timeout=tup_timeout, 
                                         headers=dict_headers, payload=dict_payload)
        req_resp.raise_for_status()
        return req_resp

    @backoff.on_exception(
        backoff.constant,
        exceptions.RequestException,
        interval=10,
        max_tries=20,
    )
    def generic_req_wo_session(
        self,
        req_method:str,
        url:str,
        bl_verify:bool,
        tup_timeout:Tuple[float, float]=(12.0, 12.0), 
        dict_headers:Optional[Dict[str, str]]=None,
        dict_payload:Optional[Dict[str, str]]=None
    ) -> Response:
        req_resp = request(req_method, url, verify=bl_verify, 
                           timeout=tup_timeout, headers=dict_headers, payload=dict_payload)
        req_resp.raise_for_status()
        return req_resp

    def generic_req(
        self,
        req_method:str,
        url:str,
        bl_verify:bool,
        tup_timeout:Tuple[float, float]=(12.0, 12.0), 
        dict_headers:Optional[Dict[str, str]]=None,
        dict_payload:Optional[Dict[str, str]]=None
    ) -> Response:
        """
        Check wheter the request method is valid and do the request with distinctions for session and
            local proxy-based requests
        Args:
            - req_method (str): request method
            - url (str): request url
            - bl_verify (bool): verify request
            - tup_timeout (Tuple[float, float]): request timeout
        Returns:
            Tuple[Response, str]
        """
        # checking wheter request method is valid
        if req_method not in ['GET', 'POST']:
            if self.logger is not None:
                CreateLog().warnings(self.logger, f'Invalid request method: {req_method}')
            raise Exception(f'Invalid request method: {req_method}')
        # request content
        if self.session is None:
            func_generic_req = self.generic_req_w_session
        else:
            func_generic_req = self.generic_req_wo_session
        req_resp = func_generic_req(
            req_method, url, bl_verify, tup_timeout, dict_headers, dict_payload
        )
        return req_resp

    def trt_req(
        self, 
        req_method:str, host:str, dict_dtypes:Dict[str, Any], 
        dict_headers:Optional[Dict[str, str]]=None,
        dict_payload:Optional[Dict[str, str]]=None, app:Optional[str]=None, 
        bl_verify:bool=False, bl_io_interpreting:bool=False, 
        tup_timeout:Tuple[float, float]=(12.0, 12.0), cols_from_case:Optional[str]=None, 
        cols_to_case:Optional[str]=None, list_cols_drop_dupl:List[str]=None, 
        str_fmt_dt:str='YYYY-MM-DD', type_error_action:str='raise', 
        strt_keep_when_duplicated:str='first'
    ) -> pd.DataFrame:
        # building url and requesting
        url = host + app
        if self.logger is not None:
            CreateLog().infos(self.logger, 'Requesting data from: {}'.format(url))
        else:
            print('{} - Requesting data from: {}'.format(DatesBR().current_timestamp_string, url))
        req_resp = self.generic_req(
            req_method, url, bl_verify, tup_timeout, dict_headers, dict_payload
        )
        # dealing with request content type
        if self.req_trt_injection(req_resp) is not None:
            df_ = self.req_trt_injection(req_resp)
        elif url.endswith('.zip') == True:
            obj_ = DirFilesManagement().get_zip_from_web_in_memory(
                req_resp, 
                bl_io_interpreting=bl_io_interpreting
            )
            print('OBJ_ZIP_REQ: {}'.format(obj_))
            print('TYPE OBJ_ZIP_REQ: {}'.format(type(obj_)))
            if isinstance(obj_, list) == True:
                list_ = list()
                for name_xlsx in obj_:
                    reader = pd.read_excel(name_xlsx, encoding='utf-8')
                    df_ = pd.DataFrame(reader)
                    df_['FILE_NAME'] = name_xlsx
                    list_.extend(df_.to_dict(orient='records'))
                df_ = pd.DataFrame(list_)
                df_.drop_duplicates(inplace=True)
            else:
                reader = pd.read_csv(obj_, sep=';', header=None, names=list(dict_dtypes.keys()), 
                    decimal=',', thousands='.')
                df_ = pd.DataFrame(reader)
                df_['FILE_NAME'] = obj_.name
        elif url.endswith('.csv') == True:
            reader = pd.read_csv(req_resp.content, sep=',')
            df_ = pd.DataFrame(reader)
        else:
            json_ = req_resp.json()
            reader = pd.DataFrame(json_)
            df_ = pd.DataFrame(reader)
        # standardizing
        cls_dt_stddz = DFStandardization(
            HandlingDicts().merge_n_dicts(
                dict_dtypes, 
                {
                    k: v 
                    for k, v in self.dict_metadata['logs']['dtypes'].items() 
                    if k in list(df_.columns)
                }
            ), 
            cols_from_case,
            cols_to_case,
            list_cols_drop_dupl,
            str_fmt_dt, 
            type_error_action, 
            strt_keep_when_duplicated,
        )
        df_ = cls_dt_stddz._pipeline(df_)
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

    def insert_table(self, str_resource:str, list_ser:Optional[List[Dict[str, Any]]]=None, 
                      bl_insert_or_ignore:bool=False, bl_schema:bool=True) -> None:
        """
        Insert data into data table
        Args:
            - str_resource (str): resource name
            - bl_insert_or_ignore(bool): False as default
            - bl_schema(bool): some databases, like SQLite, doesn't have schemas - True as default
        Returns:
            pd.DataFrame
        """
        if self.cls_db == None: 
            raise Exception('data insertion failed due to lack of database definition, '
                + 'please revisit this parameter')
        if str_resource == 'general':
            return None
        elif str_resource == 'metadata':
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
            if list_ser is None:
                raise Exception('data insertion failed due to lack of data, '
                    + 'please revisit this parameter')
            if bl_schema == False:
                str_table_name = '{}_{}'.format(
                    self.dict_metadata[str_resource]['schema'], 
                    self.dict_metadata[str_resource]['table_name']
                )
            self.cls_db._insert(
                list_ser, 
                str_table_name=str_table_name,
                bl_insert_or_ignore=bl_insert_or_ignore
            )

    def non_iteratively_get_data(self, str_resource:str, bl_fetch:bool=False) -> pd.DataFrame:
        """
        Non-iteratively get raw data
        Args:
            - str_resource (str): resource name
        Returns:
            pd.DataFrame
        """
        app_ = self.dict_metadata[str_resource]['app'] if 'app' in self.dict_metadata[str_resource] \
            else self.dict_metadata[str_resource]['app']
        host_ = self.dict_metadata[str_resource]['host'] \
            if self.dict_metadata[str_resource].get('host', None) is not None \
            else self.dict_metadata['credentials']['host']
        host_ = StrHandler().fill_fstr_from_globals(host_)
        dict_headers = self.dict_metadata[str_resource]['dict_headers'] \
            if self.dict_metadata[str_resource].get('dict_headers', None) is not None \
            else self.dict_metadata['credentials'].get('dict_headers', None)
        dict_payload = self.dict_metadata[str_resource]['dict_payload'] \
            if self.dict_metadata[str_resource].get('dict_payload', None) is not None \
            else self.dict_metadata['credentials'].get('dict_payload', None)
        print('HOST NAME: {}'.format(host_))
        df_ = self.trt_req(
            self.dict_metadata[str_resource]['req_method'],
            host_,
            self.dict_metadata[str_resource]['dtypes'], 
            dict_headers,
            dict_payload,
            app_, 
            self.dict_metadata[str_resource]['bl_verify'],
            self.dict_metadata[str_resource]['bl_io_interpreting'],
            self.dict_metadata[str_resource]['timeout'],
            self.dict_metadata[str_resource]['cols_from_case'],
            self.dict_metadata[str_resource]['cols_to_case'],
            self.dict_metadata[str_resource]['list_cols_drop_dupl'],
            self.dict_metadata[str_resource]['str_fmt_dt']
        )
        if self.cls_db is not None:
            self.insert_table(
                str_resource, 
                df_.to_dict(orient='records'), 
                self.dict_metadata[str_resource]['bl_insert_or_ignore'], 
                self.dict_metadata[str_resource]['bl_schema']
            )
        if bl_fetch == True:
            return df_
        else:
            return None

    def iteratively_get_data(self, str_resource:str, bl_fetch:bool=False) -> Optional[pd.DataFrame]:
        """
        Iteratively get raw data
        Args:
            - str_resource (str): resource name
        Returns:
            Optional[pd.DataFrame]
        """
        list_ser = list()
        i = 0
        app_ = self.dict_metadata[str_resource]['app'] if 'app' in self.dict_metadata[str_resource] \
            else self.dict_metadata[str_resource]['app']
        host_ = self.dict_metadata[str_resource]['host'] \
            if self.dict_metadata[str_resource].get('host', None) is not None \
            else self.dict_metadata['credentials']['host']
        host_ = StrHandler().fill_fstr_from_globals(host_)
        dict_headers = self.dict_metadata[str_resource]['dict_headers'] \
            if self.dict_metadata[str_resource].get('dict_headers', None) is not None \
            else self.dict_metadata['credentials'].get('dict_headers', None)
        dict_payload = self.dict_metadata[str_resource]['dict_payload'] \
            if self.dict_metadata[str_resource].get('dict_payload', None) is not None \
            else self.dict_metadata['credentials'].get('dict_payload', None)
        print('HOST NAME: {}'.format(host_))
        while True:
            try:
                list_ser.extend(
                    self.trt_req(
                        self.dict_metadata[str_resource]['req_method'],
                        host_,
                        self.dict_metadata[str_resource]['dtypes'], 
                        dict_headers,
                        dict_payload,
                        app_.format(i), 
                        self.dict_metadata[str_resource]['bl_verify'],
                        self.dict_metadata[str_resource]['bl_io_interpreting'],
                        self.dict_metadata[str_resource]['timeout'],
                        self.dict_metadata[str_resource]['cols_from_case'],
                        self.dict_metadata[str_resource]['cols_to_case'],
                        self.dict_metadata[str_resource]['list_cols_drop_dupl'],
                        self.dict_metadata[str_resource]['str_fmt_dt']
                    ).to_dict(orient='records')
                )
                if self.cls_db is not None:
                    self.insert_table(
                        str_resource, 
                        list_ser, 
                        self.dict_metadata[str_resource]['bl_insert_or_ignore'], 
                        self.dict_metadata[str_resource]['bl_schema']
                    )
                    if bl_fetch == False: list_ser = list()
                i += 1
            except (exceptions.HTTPError, BadZipFile) as e:
                if self.logger is not None:
                    CreateLog().warnings(self.logger, f'#{i} Iteration failed due to: {e}')
                else:
                    print(f'#{i} Iteration failed due to: {e}')
                break
        if len(list_ser) > 0:
            return pd.DataFrame(list_ser)
        else:
            return None

    def _source(self, str_resource:str, bl_fetch:bool=False) -> Optional[pd.DataFrame]:
        """
        Get/load raw data - if a class database is passed, the data collected will be inserted into 
            the respective table; please make sure this key is filled within the metadata
        Args:
            - str_resource (str): resource name
        Returns:
            Optional[pd.DataFrame]
        """
        if StrHandler().match_string_like(self.dict_metadata[str_resource], '{*}') == True:
            self.dict_metadata[str_resource]['app'] = \
                StrHandler().fill_fstr_from_globals(self.dict_metadata[str_resource]['app'])
        if StrHandler().match_string_like(self.dict_metadata[str_resource], '{i}') == True:
            return self.iteratively_get_data(str_resource, bl_fetch)
        return self.non_iteratively_get_data(str_resource, bl_fetch)
    
    def _sources(self, bl_fetch:bool=False) -> Optional[pd.DataFrame]:
        """
        Get/load raw data
        Args:
            - str_resource (str): resource name
            - bl_insert_or_ignore(bool): False as default
            - bl_schema(bool): some databases, like SQLite, doesn't have schemas - True as default
        Return:
            pd.DataFrame
        """
        list_ser = list()
        for str_resource in list(self.dict_metadata.keys()):
            if str_resource not in \
                ['credentials', 'logs', 'metadata']:
                if bl_fetch == True:
                    list_ser.extend(
                        self._source(str_resource, bl_fetch).to_dict(orient='records')
                    )
                else:
                    self._source(str_resource, bl_fetch)
        if len(list_ser) > 0:
            return pd.DataFrame(list_ser)
        else:
            return None