### ABSTRACT BASE CLASS - REQUESTS ###

# pypi.org libs
import fitz
import re
import os
import backoff
import unicodedata
import pandas as pd
from getpass import getuser
from abc import ABC, abstractmethod
from datetime import datetime
from requests import exceptions, request, Response
from typing import Tuple, List, Dict, Any, Optional, Union
from sqlalchemy.orm import Session
from logging import Logger
from io import TextIOWrapper, BufferedReader, BytesIO
from zipfile import ZipFile, BadZipFile
# project modules
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.loggs.db_logs import DBLogs
from stpstone.utils.connections.netops.session import ReqSession
from stpstone.transformations.standardization.dataframe import DFStandardization
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.xml import XMLFiles
from stpstone.utils.parsers.json import JsonFiles
from stpstone.utils.parsers.folders import DirFilesManagement


class HandleReqResponses(ABC):

    def remove_diacritics(self, str_):
        str_ = str_.lower()
        str_ = str_.replace('\n', ' ')
        return ''.join(c for c in unicodedata.normalize('NFKD', str_) if not unicodedata.combining(c))

    def _handle_response(
        self, 
        req_resp:Response, 
        dict_dtypes:Dict[str, Any], 
        dict_regex_patterns:Optional[Dict[str, Dict[str, str]]]=None, 
        dict_df_read_params:Optional[Dict[str, Any]]=None, 
        bl_debug:bool=False
    ) -> pd.DataFrame:
        if self.req_trt_injection(req_resp) is not None:
            return self.req_trt_injection(req_resp)
        elif req_resp.url.endswith('.zip'):
            return self.handle_zip_response(req_resp, dict_dtypes)
        elif \
            (req_resp.url.endswith('.csv')) \
            or (req_resp.url.endswith('.txt')) \
            or (req_resp.url.endswith('.asp')):
            return self.handle_csv_response(req_resp, dict_df_read_params)
        elif req_resp.url.endswith('.xlsx'):
            return self.handle_excel_response(req_resp, dict_df_read_params)
        elif req_resp.url.endswith('.xml'):
            return self.handle_xml_response(req_resp)
        elif req_resp.url.endswith('.json'):
            return self.handle_xml_response(req_resp)
        elif DirFilesManagement().get_file_extension(req_resp.url) in \
            ['pdf', 'docx', 'doc', 'docm', 'dot', 'dotm']:
            return self.handle_pdf_doc_response(req_resp, dict_regex_patterns, bl_debug)
        else:
            json_ = req_resp.json()
            return pd.DataFrame(json_)

    @abstractmethod
    def req_trt_injection(self, req_resp:Response) -> Optional[pd.DataFrame]:
        return None

    def handle_zip_response(self, req_resp: Response, dict_dtypes: Dict[str, Any], 
                             dict_xml_keys:Optional[Dict[str, Any]]=None, 
                             key_attrb_xml:Optional[str]=None, 
                             dict_regex_patterns:Optional[Dict[str, Dict[str, str]]]=None, 
                             dict_df_read_params:Optional[Dict[str, Any]]=None) \
                            -> Union[pd.DataFrame, List[pd.DataFrame]]:
        zipfile = ZipFile(BytesIO(req_resp.content))
        list_ = []
        for file_name in zipfile.namelist():
            with zipfile.open(file_name) as file:
                if file_name.endswith('.zip'):
                    nested_zip_response = Response()
                    nested_zip_response._content = file.read()
                    nested_df = self.handle_zip_response(
                        nested_zip_response, dict_dtypes, dict_xml_keys, key_attrb_xml)
                    if isinstance(nested_df, pd.DataFrame):
                        nested_df['FILE_NAME'] = file_name
                        list_.extend(nested_df.to_dict(orient='records'))
                    elif isinstance(nested_df, list):
                        for df_ in nested_df:
                            df_['FILE_NAME'] = file_name
                        list_.extend(nested_df)
                elif \
                    (file_name.endswith('.csv')) \
                    or (file_name.endswith('.txt')):
                    df_ = self.handle_csv_response(file, dict_df_read_params)
                    df_['FILE_NAME'] = file_name
                    list_.extend(df_.to_dict(orient='records'))
                elif file_name.endswith('.xlsx'):
                    df_ = self.handle_excel_response(file. dict_df_read_params)
                    df_['FILE_NAME'] = file_name
                    list_.extend(df_.to_dict(orient='records'))
                elif file_name.endswith('.xml'):
                    df_ = self.handle_xml_response(file)
                    df_['FILE_NAME'] = file_name
                    list_.extend(df_.to_dict(orient='records'))
                elif file_name.endswith('.json'):
                    df_ = self.handle_json_response(file)
                    df_['FILE_NAME'] = file_name
                    list_.extend(df_.to_dict(orient='records'))
                elif file_name.endswith('.pdf'):
                    df_ = self.handle_pdf_doc_response(file, dict_regex_patterns)
                    df_['FILE_NAME'] = file_name
                    list_.extend(df_.to_dict(orient='records'))
                else:
                    if self.logger is not None:
                        CreateLog().warnings(self.logger, f'Unsupported file type: {req_resp.url}')
                    raise ValueError(f'Unsupported file type: {req_resp.url}')
        if list_:
            return list_
        else:
            return pd.DataFrame()

    def handle_csv_response(
        self, 
        file:Union[BytesIO, TextIOWrapper], 
        dict_df_read_params:Optional[Dict[str, Any]]
    ) -> pd.DataFrame:
        if isinstance(file, BytesIO):
            file.seek(0)
        return pd.read_csv(file, **dict_df_read_params)

    def handle_excel_response(
        self, 
        file:Union[BytesIO, TextIOWrapper], 
        dict_df_read_params:Optional[Dict[str, Any]]
    ) -> pd.DataFrame:
        if isinstance(file, BytesIO):
            file.seek(0)
        return pd.read_excel(file, **dict_df_read_params)

    def handle_xml_response(self, file:Union[BytesIO, TextIOWrapper], 
                             dict_xml_keys:Dict[str, Any], key_attrb_xml:Optional[str]=None) \
                                -> pd.DataFrame:
        list_ser = list()
        if isinstance(file, BytesIO):
            file.seek(0)
        soup_xml = XMLFiles().memory_parser(file)
        for key, list_tags in dict_xml_keys['tags'].items():
            for soup_content in soup_xml.find_all(key):
                dict_ = dict()
                for tag in list_tags:
                    try:
                        tag_ = soup_content.find(tag)
                        print(tag_)
                        dict_[tag] = tag_.get_text()
                        if \
                            (dict_xml_keys['attributes'][key_attrb_xml] is not None) and \
                            (dict_xml_keys['attributes'][key_attrb_xml] in tag_.attrs):
                            dict_[dict_xml_keys['attributes'][key_attrb_xml]] = tag_.attrs[
                                dict_xml_keys['attributes'][key_attrb_xml]]
                    except AttributeError:
                        continue
                list_ser.append(dict_)
        return pd.DataFrame(list_ser)

    def handle_json_response(self, file:Union[BytesIO, TextIOWrapper]) -> pd.DataFrame:
        if isinstance(file, BytesIO):
            file.seek(0)
        json_file = file.read()
        list_ser = JsonFiles().loads_message_like(json_file)
        df_ = pd.DataFrame(list_ser)
        return df_
    
    def handle_pdf_doc_response(self, req_resp:Response, dict_regex_patterns:Dict[str, Dict[str, str]], 
                                bl_debug:bool=False) \
        -> pd.DataFrame:
        # setting variables
        list_pages = list()
        list_matches = list()
        dict_count_matches = dict()
        dict_actions_per_event = {
            evnt: len(dict_l2) 
            for evnt, dict_l1 in dict_regex_patterns.items() 
            for _, dict_l2 in dict_l1.items()
        }
        # reading pdf/doc
        bytes_pdf = BytesIO(req_resp.content)
        doc_pdf = fitz.open(
            stream=bytes_pdf, 
            filetype=DirFilesManagement().get_file_extension(req_resp.url)
        )
        # join 2 pages at a time, in order to find infos that are separated due to page break
        for i in range(0, len(doc_pdf), 2):
            text1 = doc_pdf[i].get_text('text') if i < len(doc_pdf) else ''
            text2 = doc_pdf[i+1].get_text('text') if i+1 < len(doc_pdf) else ''
            list_pages.append(text1 + '\n' + text2)
        # loop through each page looking for regex pattern, where event is the info that is targeted, 
        #   condition is a case of match found previously, and action is the subclass of action with 
        #   an associated regex
        for i, str_page in enumerate(list_pages):
            str_page = StrHandler().remove_diacritics_nfkd(str_page, bl_lower_case=True)
            if bl_debug == True:
                print('PG {}/{}'.format(i+1, len(list_pages)))
                print('DICT ACTIONS PER EVENT: ', dict_actions_per_event)
                print('DICT COUNT MATCHES: ', dict_count_matches)
                print('LEN COUNT MATHCES: ', len(dict_count_matches))
                if i == 10:
                    print(str_page)
                    root_path = os.path.abspath(os.path.join(os.path.dirname(
                        os.path.realpath(__file__)), "..", "..", ".."))
                    path_json = os.path.join(root_path, 'data/test-str-page_{}_{}_{}.json'.format(
                        getuser(), 
                        DatesBR().curr_date.strftime('%Y%m%d'),
                        DatesBR().curr_time.strftime('%H%M%S')
                    ))
                    JsonFiles().dump_message(str_page, path_json)
            #   if, for every event, all the actions have at least one match, then break the loop
            if \
                (len(dict_count_matches) > 0) \
                and (
                    all([
                        len([_ for _, count in dict_count_matches[evnt].items() if count > 0]) \
                            >= num_actions 
                        for evnt, num_actions in dict_actions_per_event.items() 
                        if evnt in dict_count_matches
                    ]) == True
                ):
                break
            #   looping within pages to find regex matches for every event/action
            for str_event, dict_l1 in dict_regex_patterns.items():
                if bl_debug == True:
                    print(f'EVENT: {str_event}')
                for str_condition, dict_l2 in dict_l1.items():
                    if bl_debug == True:
                        print(f'CONDITION: {str_condition}')
                    for str_action, pattern_regex in dict_l2.items():
                        #   checking wheter should stop at first match
                        if str_event not in dict_count_matches:
                            dict_count_matches[str_event] = dict()
                        if str_action not in dict_count_matches[str_event]:
                            dict_count_matches[str_event][str_action] = 0
                        if (all([dict_count_matches[str_event][k] >= 1 
                                 for k in list(dict_count_matches[str_event].keys())]) == True): break
                        pattern_regex = StrHandler().remove_diacritics_nfkd(
                            pattern_regex, bl_lower_case=False)
                        if bl_debug == True:
                            print(str_action, pattern_regex)
                        regex_match = re.search(
                            pattern_regex, 
                            str_page, 
                            # flags=re.DOTALL | re.MULTILINE
                        )
                        if regex_match is not None:
                            dict_count_matches[str_event][str_action] += 1
                            dict_ = {
                                'EVENT': str_event.upper(),
                                'CONDITION': str_condition.upper(),
                                'ACTION': str_action.upper(),
                                'PATTERN_REGEX': pattern_regex
                            }
                            for i_match in range(0, len(regex_match.groups()) + 1):
                                regex_group = regex_match.group(i_match).replace('\n', ' ')
                                #   remove double spaces and trim the string
                                regex_group = re.sub(r'\s+', ' ', regex_group).strip()
                                dict_[f'REGEX_GROUP_{i_match}'] = regex_group
                            list_matches.append(dict_)
                        else:
                            list_matches.append({
                                'EVENT': str_event.upper(),
                                'CONDITION': 'N/A',
                                'ACTION': str_action.upper(),
                                'PATTERN_REGEX': 'zzN/A',
                                'REGEX_GROUP_0': 'N/A'
                            })
        df_ = pd.DataFrame(list_matches)
        df_.drop_duplicates(inplace=True)
        return df_


class ABCRequests(HandleReqResponses):

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
        self.token = self.access_token \
            if self.dict_metadata['credentials']['token']['host'] is not None else None

    @property
    def access_token(self):
        url_token = StrHandler().fill_fstr_from_globals(
            self.dict_metadata['credentials']['token']['host']
        )
        req_resp = self.generic_req(
            self.dict_metadata['credentials']['token']['get']['req_method'], 
            url_token, 
            self.dict_metadata['credentials']['token']['get']['bl_verify'], 
            self.dict_metadata['credentials']['token']['get']['timeout']
        )
        req_resp.raise_for_status()
        return req_resp.json()[self.dict_metadata['credentials']['token']['keys']['token']]

    # ! TODO: implement timeout
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
            req_resp = self.session.get(url, verify=bl_verify, 
                                        headers=dict_headers, params=dict_payload)
        elif req_method == 'POST':
            req_resp = self.session.post(url, verify=bl_verify, 
                                         headers=dict_headers, data=dict_payload)
        req_resp.raise_for_status()
        return req_resp

    # ! TODO: implement timeout
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
        # print(f'URL: {url}')
        if req_method == 'GET':
            req_resp = request(req_method, url, verify=bl_verify, 
                            headers=dict_headers, params=dict_payload)
        elif req_method == 'POST':
            req_resp = request(req_method, url, verify=bl_verify, 
                            headers=dict_headers, data=dict_payload)
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
        if self.session is not None:
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
        bl_verify:bool=False, tup_timeout:Tuple[float, float]=(12.0, 12.0), 
        cols_from_case:Optional[str]=None, cols_to_case:Optional[str]=None, 
        list_cols_drop_dupl:List[str]=None, str_fmt_dt:str='YYYY-MM-DD', 
        type_error_action:str='raise', strt_keep_when_duplicated:str='first', 
        dict_regex_patterns:Optional[Dict[str, Dict[str, str]]]=None, 
        dict_df_read_params:Optional[Dict[str, Any]]=None, bl_debug:bool=False
    ) -> pd.DataFrame:
        # building url and requesting
        url = host + app if app is not None else host
        if self.logger is not None:
            CreateLog().infos(self.logger, f'Starting request to: {url}')
        else:
            print(f'Starting request to: {url}')
        req_resp = self.generic_req(
            req_method, url, bl_verify, tup_timeout, dict_headers, dict_payload
        )
        # dealing with request content type
        df_ = self._handle_response(
            req_resp, dict_dtypes, dict_regex_patterns, dict_df_read_params, bl_debug
        )
        df_ = DBLogs().audit_log(
            df_, 
            url, 
            DatesBR().build_date(
                DatesBR().year_number(self.dt_ref), 
                DatesBR().month_number(self.dt_ref, bl_month_mm=False), 
                1
            )
        )
        if self.logger is not None:
            CreateLog().infos(self.logger, f'Request completed successfully: {url}')
        # standardizing
        cls_df_stdz = DFStandardization(
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
        df_ = cls_df_stdz._pipeline(df_)
        return df_

    def insert_table(self, str_resource: str, list_ser: Optional[List[Dict[str, Any]]] = None, 
                 bl_insert_or_ignore: bool = False, bl_schema: bool = True) -> None:
        """
        Insert data into data table
        Args:
            - str_resource (str): resource name
            - list_ser (List[Dict[str, Any]]): data to insert
            - bl_insert_or_ignore (bool): False as default
            - bl_schema (bool): some databases, like SQLite, don't have schemas - True as default
        Raises:
            Exception: If database or data is not defined
        """
        if self.cls_db is None:
            raise Exception('Data insertion failed due to lack of database definition. '
                        'Please revisit this parameter.')
        if list_ser is None:
            raise Exception('Data insertion failed due to lack of data. '
                        'Please revisit this parameter.')
        if str_resource == 'general':
            return None
        elif str_resource == 'metadata':
            for _, dict_ in self.dict_metadata[str_resource].items():
                str_table_name = dict_['table_name']
                if bl_schema:
                    str_table_name = f"{dict_['schema']}_{str_table_name}"
                self.cls_db._insert(
                    dict_['data'], 
                    str_table_name=str_table_name,
                    bl_insert_or_ignore=True
                )
        else:
            str_table_name = self.dict_metadata[str_resource]['table_name']
            if bl_schema:
                str_table_name = f"{self.dict_metadata[str_resource]['schema']}_{str_table_name}"
            self.cls_db._insert(
                list_ser, 
                str_table_name=str_table_name,
                bl_insert_or_ignore=bl_insert_or_ignore
            )

    def non_iteratively_get_data(self, str_resource:str, host:Optional[str]=None, bl_fetch:bool=False, 
                                 bl_debug:bool=False) -> Optional[pd.DataFrame]:
        """
        Non-iteratively get raw data
        Args:
            - str_resource (str): resource name
            - host (Optional[str]): host name
            - bl_fetch (bool): False as default
        Returns:
            pd.DataFrame
        """
        app_ = self.dict_metadata[str_resource]['app'] if 'app' in self.dict_metadata[str_resource] \
            else self.dict_metadata[str_resource]['app']
        host_ = host if host is not None else (
            self.dict_metadata[str_resource].get('host', None) \
            if self.dict_metadata[str_resource].get('host', None) is not None \
            else self.dict_metadata['credentials']['host']
        )
        host_ = StrHandler().fill_fstr_from_globals(host_)
        dict_headers = self.dict_metadata[str_resource]['dict_headers'] \
            if self.dict_metadata[str_resource].get('dict_headers', None) is not None \
            else self.dict_metadata['credentials'].get('dict_headers', None)
        dict_payload = self.dict_metadata[str_resource]['dict_payload'] \
            if self.dict_metadata[str_resource].get('dict_payload', None) is not None \
            else self.dict_metadata['credentials'].get('dict_payload', None)
        df_ = self.trt_req(
            self.dict_metadata[str_resource]['req_method'],
            host_,
            self.dict_metadata[str_resource]['dtypes'], 
            dict_headers,
            dict_payload,
            app_, 
            self.dict_metadata[str_resource]['bl_verify'],
            self.dict_metadata[str_resource]['timeout'],
            self.dict_metadata[str_resource]['cols_from_case'],
            self.dict_metadata[str_resource]['cols_to_case'],
            self.dict_metadata[str_resource]['list_cols_drop_dupl'],
            self.dict_metadata[str_resource]['str_fmt_dt'],
            self.dict_metadata[str_resource]['type_error_action'],
            self.dict_metadata[str_resource]['strt_keeping_when_duplicated'],
            self.dict_metadata[str_resource]['regex_patterns'],
            self.dict_metadata[str_resource]['df_read_params'],
            bl_debug
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

    def iteratively_get_data(self, str_resource:str, host:Optional[str]=None, bl_fetch:bool=False, 
                             bl_debug:bool=False) \
        -> Optional[pd.DataFrame]:
        """
        Iteratively get raw data
        Args:
            - str_resource (str): resource name
            - host (Optional[str]): host name
            - bl_fetch (bool): False as default
        Returns:
            Optional[pd.DataFrame]
        """
        list_ser = list()
        i = 0
        app_ = self.dict_metadata[str_resource]['app'] if 'app' in self.dict_metadata[str_resource] \
            else self.dict_metadata[str_resource]['app']
        host_ = host if host is not None else (
            self.dict_metadata[str_resource].get('host', None) \
            if self.dict_metadata[str_resource].get('host', None) is not None \
            else self.dict_metadata['credentials']['host']
        )
        host_ = StrHandler().fill_fstr_from_globals(host_)
        dict_headers = self.dict_metadata[str_resource]['dict_headers'] \
            if self.dict_metadata[str_resource].get('dict_headers', None) is not None \
            else self.dict_metadata['credentials'].get('dict_headers', None)
        dict_payload = self.dict_metadata[str_resource]['dict_payload'] \
            if self.dict_metadata[str_resource].get('dict_payload', None) is not None \
            else self.dict_metadata['credentials'].get('dict_payload', None)
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
                        self.dict_metadata[str_resource]['timeout'],
                        self.dict_metadata[str_resource]['cols_from_case'],
                        self.dict_metadata[str_resource]['cols_to_case'],
                        self.dict_metadata[str_resource]['list_cols_drop_dupl'],
                        self.dict_metadata[str_resource]['str_fmt_dt'],
                        self.dict_metadata[str_resource]['type_error_action'],
                        self.dict_metadata[str_resource]['strt_keeping_when_duplicated'],
                        self.dict_metadata[str_resource]['regex_patterns'],
                        self.dict_metadata[str_resource]['df_read_params'],
                        bl_debug
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

    def _source(self, str_resource:str, host:Optional[str]=None, bl_fetch:bool=False, 
                bl_debug:bool=False) \
        -> Optional[pd.DataFrame]:
        """
        Get/load raw data - if a class database is passed, the data collected will be inserted into 
            the respective table; please make sure this key is filled within the metadata
        Args:
            - str_resource (str): resource name
            - host (Optional[str]): host name
            - bl_fetch (bool): False as default
        Returns:
            Optional[pd.DataFrame]
        """
        if \
            (self.dict_metadata[str_resource]['app'] is not None) \
            and (StrHandler().match_string_like(
                self.dict_metadata[str_resource]['app'], '{*}') == True):
            self.dict_metadata[str_resource]['app'] = \
                StrHandler().fill_fstr_from_globals(self.dict_metadata[str_resource]['app'])
        if \
            (self.dict_metadata[str_resource]['app'] is not None) \
            and (StrHandler().match_string_like(
                self.dict_metadata[str_resource]['app'], '{i}') == True):
            return self.iteratively_get_data(str_resource, host, bl_fetch, bl_debug)
        return self.non_iteratively_get_data(str_resource, host, bl_fetch, bl_debug)
    
    def _sources(self, host:Optional[str]=None, bl_fetch:bool=False, bl_debug:bool=False) \
        -> Optional[pd.DataFrame]:
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
                ['credentials', 'logs', 'metadata', 'downstream_processes']:
                if bl_fetch == True:
                    list_ser.extend(
                        self._source(str_resource, bl_fetch).to_dict(orient='records')
                    )
                else:
                    self._source(str_resource, host, bl_fetch, bl_debug)
        if len(list_ser) > 0:
            return pd.DataFrame(list_ser)
        else:
            return None