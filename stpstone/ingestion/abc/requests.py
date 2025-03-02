### ABSTRACT BASE CLASS - REQUESTS ###

# pypi.org libs
import fitz
import re
import os
import backoff
import pandas as pd
from getpass import getuser
from abc import ABC, abstractmethod
from datetime import datetime
from requests import exceptions, request, Response, Session, Request
from typing import Tuple, List, Dict, Any, Optional, Union
from logging import Logger
from io import TextIOWrapper, BytesIO, StringIO
from zipfile import ZipFile
# project modules
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.lists import HandlingLists
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

    def handle_response(
        self, 
        req_resp:Response,
        dict_xml_keys:Optional[Dict[str, Any]]=None,
        dict_regex_patterns:Optional[Dict[str, Dict[str, str]]]=None, 
        dict_df_read_params:Optional[Dict[str, Any]]=None, 
        bl_debug:bool=False
    ) -> pd.DataFrame:
        str_file_extension = DirFilesManagement().get_file_extension(req_resp.url)
        if self.req_trt_injection(req_resp) is not None:
            return self.req_trt_injection(req_resp)
        elif str_file_extension == 'zip':
            return self.handle_zip_response(req_resp, dict_xml_keys, 
                                            dict_regex_patterns, dict_df_read_params, bl_debug)
        elif str_file_extension in ['csv', 'txt', 'asp', 'do']:
            return self.handle_csv_response(req_resp, dict_df_read_params)
        elif str_file_extension == 'xlsx':
            return self.handle_excel_response(req_resp, dict_df_read_params)
        elif str_file_extension == 'xml':
            return self.handle_xml_response(req_resp, dict_xml_keys)
        elif str_file_extension == 'json':
            return self.handle_json_response(req_resp)
        elif str_file_extension in ['pdf', 'docx', 'doc', 'docm', 'dot', 'dotm']:
            return self.handle_pdf_doc_response(req_resp, dict_regex_patterns, bl_debug)
        else:
            json_ = req_resp.json()
            if isinstance(json_, dict) == True:
                return pd.DataFrame([json_])
            return pd.DataFrame(json_)

    @abstractmethod
    def req_trt_injection(self, req_resp:Response) -> Optional[pd.DataFrame]:
        return None

    def handle_zip_response(
            self, 
            req_resp:Response,
            dict_xml_keys:Optional[Dict[str, Any]]=None,
            dict_regex_patterns:Optional[Dict[str, Dict[str, str]]]=None, 
            dict_df_read_params:Optional[Dict[str, Any]]=None, 
            bl_debug:bool=False
        ) -> Union[pd.DataFrame, List[pd.DataFrame]]:
        zipfile = ZipFile(BytesIO(req_resp.content))
        list_ = []
        for file_name in zipfile.namelist():
            with zipfile.open(file_name) as file:
                str_file_extension = DirFilesManagement().get_file_extension(file_name)
                if str_file_extension == 'zip':
                    nested_zip_response = Response()
                    nested_zip_response._content = file.read()
                    nested_df = self.handle_zip_response(nested_zip_response, dict_xml_keys, 
                                                         dict_regex_patterns, 
                                                         dict_df_read_params, bl_debug)
                    if isinstance(nested_df, pd.DataFrame):
                        nested_df['FILE_NAME'] = file_name
                        list_.extend(nested_df.to_dict(orient='records'))
                    elif isinstance(nested_df, list):
                        for df_ in nested_df:
                            df_['FILE_NAME'] = file_name
                        list_.extend(nested_df)
                elif \
                    (str_file_extension == 'csv') \
                    or (str_file_extension == 'txt'):
                    df_ = self.handle_csv_response(file, dict_df_read_params)
                    df_['FILE_NAME'] = file_name
                    list_.extend(df_.to_dict(orient='records'))
                elif str_file_extension == 'xlsx':
                    df_ = self.handle_excel_response(file, dict_df_read_params)
                    df_['FILE_NAME'] = file_name
                    list_.extend(df_.to_dict(orient='records'))
                elif str_file_extension == '.xml':
                    df_ = self.handle_xml_response(file, dict_xml_keys)
                    df_['FILE_NAME'] = file_name
                    list_.extend(df_.to_dict(orient='records'))
                elif str_file_extension == 'json':
                    df_ = self.handle_json_response(file)
                    df_['FILE_NAME'] = file_name
                    list_.extend(df_.to_dict(orient='records'))
                elif str_file_extension == 'pdf':
                    df_ = self.handle_pdf_doc_response(file, dict_regex_patterns, bl_debug)
                    df_['FILE_NAME'] = file_name
                    list_.extend(df_.to_dict(orient='records'))
                else:
                    if self.logger is not None:
                        CreateLog().warnings(self.logger, f'Unsupported file type: {req_resp.url}')
                    raise ValueError(f'Unsupported file type: {req_resp.url}')
        if list_:
            return pd.DataFrame(list_)
        else:
            return pd.DataFrame()

    def handle_csv_response(
        self, 
        file:Union[BytesIO, TextIOWrapper, Response], 
        dict_df_read_params:Optional[Dict[str, Any]]
    ) -> pd.DataFrame:
        if isinstance(file, BytesIO):
            file.seek(0)
        elif isinstance(file, Response):
            file = StringIO(file.text)
        return pd.read_csv(file, **dict_df_read_params)

    def handle_excel_response(
        self, 
        file:Union[BytesIO, TextIOWrapper, Response], 
        dict_df_read_params:Optional[Dict[str, Any]]
    ) -> pd.DataFrame:
        if isinstance(file, BytesIO):
            file.seek(0)
        elif isinstance(file, Response):
            file = StringIO(file.text)
        return pd.read_excel(file, **dict_df_read_params)

    def handle_xml_response(
        self, 
        file:Union[BytesIO, TextIOWrapper], 
        dict_xml_keys:Dict[str, Any]
    ) -> pd.DataFrame:
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
                        for key_attrb_xml in dict_xml_keys['attributes']:
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
    
    def handle_pdf_doc_response(
        self, 
        req_resp:Response, 
        dict_regex_patterns:Dict[str, Dict[str, str]], 
        bl_debug:bool=False
    ) -> pd.DataFrame:
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
        logger:Optional[Logger]=None, 
        token:Optional[str]=None, 
        list_slugs:Optional[List[str]]=None
    ) -> None:
        self.dict_metadata = dict_metadata
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.token = token \
            if token is not None \
            else (
                self.access_token \
                if self.dict_metadata['credentials']['token']['host'] is not None 
                else None
            )
        self.list_slugs = list_slugs
        self.pattern_special_http_chars = r'["<>#%{}|\\^~\[\]` ]'

    @property
    def access_token(self):
        dict_instance_vars = self.get_instance_variables
        url_token = StrHandler().fill_placeholders(
            self.dict_metadata['credentials']['token']['host'], 
            dict_instance_vars
        )
        req_resp = self.generic_req(
            self.dict_metadata['credentials']['token']['get']['req_method'], 
            url_token, 
            self.dict_metadata['credentials']['token']['get']['bl_verify'], 
            self.dict_metadata['credentials']['token']['get']['timeout']
        )
        req_resp.raise_for_status()
        return req_resp.json()[self.dict_metadata['credentials']['token']['keys']['token']]

    @property
    def get_instance_variables(self) -> Dict[str, Any]:
        """
        Gather all instance variables (self.*) into a dictionary.
        """
        return {key: value for key, value in self.__dict__.items() if not key.startswith('__')}

    # ! TODO: implement timeout
    def generic_req_w_session(
        self, 
        req_method:str,
        url:str,
        bl_verify:bool,
        tup_timeout:Tuple[float, float]=(12.0, 12.0), 
        dict_headers:Optional[Dict[str, str]]=None,
        payload:Optional[Union[str, Dict[str, str]]]=None
    ) -> Response:
        if re.search(self.pattern_special_http_chars, url):
            #   prepare the request manually to preserve special characters
            req_resp = Request(req_method, url, headers=dict_headers, params=payload)
            req_preppped = self.session.prepare_request(req_resp)
            req_preppped.url = url
            req_resp = self.session.send(req_preppped, verify=bl_verify)
        else:
            if req_method == 'GET':
                req_resp = self.session.get(url, verify=bl_verify, headers=dict_headers, params=payload)
            elif req_method == 'POST':
                req_resp = self.session.post(url, verify=bl_verify, headers=dict_headers, data=payload)
        req_resp.raise_for_status()
        return req_resp

    # ! TODO: implement timeout
    @backoff.on_exception(
        backoff.constant,
        (exceptions.RequestException, exceptions.HTTPError),
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
        payload:Optional[Dict[str, str]]=None
    ) -> Response:
        if re.search(self.pattern_special_http_chars, url):
            #   prepare the request manually to preserve special characters
            req = Request(req_method, url, headers=dict_headers, params=payload)
            req_preppped = req.prepare()
            with Session() as session:
                req_resp = session.send(req_preppped, verify=bl_verify)
        else:
            if req_method == 'GET':
                req_resp = request(req_method, url, verify=bl_verify, headers=dict_headers, params=payload)
            elif req_method == 'POST':
                req_resp = request(req_method, url, verify=bl_verify, headers=dict_headers, data=payload)
        req_resp.raise_for_status()
        return req_resp

    def generic_req(
        self,
        req_method:str,
        url:str,
        bl_verify:bool,
        tup_timeout:Tuple[float, float]=(12.0, 12.0), 
        dict_headers:Optional[Dict[str, str]]=None,
        payload:Optional[Dict[str, str]]=None
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
            req_method, url, bl_verify, tup_timeout, dict_headers, payload
        )
        return req_resp

    def trt_req(
        self, 
        req_method:str, host:str, dict_dtypes:Dict[str, Any], 
        dict_headers:Optional[Dict[str, str]]=None,
        payload:Optional[Dict[str, str]]=None, app:Optional[str]=None, 
        bl_verify:bool=False, tup_timeout:Tuple[float, float]=(12.0, 12.0), 
        cols_from_case:Optional[str]=None, cols_to_case:Optional[str]=None, 
        list_cols_drop_dupl:List[str]=None, str_fmt_dt:str='YYYY-MM-DD', 
        type_error_action:str='raise', strt_keep_when_duplicated:str='first', 
        dict_regex_patterns:Optional[Dict[str, Dict[str, str]]]=None, 
        dict_df_read_params:Optional[Dict[str, Any]]=None, 
        dict_xml_keys:Optional[Dict[str, Any]]=None, 
        bl_debug:bool=False
    ) -> pd.DataFrame:
        # building url and requesting
        url = host + app if app is not None else host
        if self.logger is not None:
            CreateLog().infos(self.logger, f'Starting request to: {url}')
        else:
            print(f'Starting request to: {url}')
        req_resp = self.generic_req(
            req_method, url, bl_verify, tup_timeout, dict_headers, payload
        )
        if bl_debug == True:
            print(f'*** DF READ PARAMS: {dict_df_read_params}')
        # dealing with request content type
        df_ = self.handle_response(
            req_resp, dict_xml_keys, dict_regex_patterns, dict_df_read_params, bl_debug
        )
        if bl_debug == True:
            print(f'*** TRT REQ BEFORE STANDARDIZATION: \n{df_}')
        # standardizing
        cls_df_stdz = DFStandardization(
            dict_dtypes, 
            cols_from_case,
            cols_to_case,
            list_cols_drop_dupl,
            str_fmt_dt, 
            type_error_action, 
            strt_keep_when_duplicated,
        )
        df_ = cls_df_stdz.pipeline(df_)
        # audit logging
        if bl_debug == True:
            print(f'*** TRT REQ: \n{df_}')
        df_ = DBLogs().audit_log(
            df_, 
            url, 
            DatesBR().build_date(
                DatesBR().year_number(self.dt_ref), 
                DatesBR().month_number(self.dt_ref, bl_month_mm=False), 
                1
            )
        )
        if bl_debug == True:
            print(f'*** TRT REQ - AUDIT LOG: \n{df_}')
            print(f'*** COLS NAMES: {list(df_.columns)}')
        if self.logger is not None:
            CreateLog().infos(self.logger, f'Request completed successfully: {url}')
        if bl_debug == True:
            print(f'*** TRT REQ - STANDARDIZATION: \n{df_}')
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
                self.cls_db.insert(
                    dict_['data'], 
                    str_table_name=str_table_name,
                    bl_insert_or_ignore=True
                )
        else:
            str_table_name = self.dict_metadata[str_resource]['table_name']
            if bl_schema:
                str_table_name = f"{self.dict_metadata[str_resource]['schema']}_{str_table_name}"
            self.cls_db.insert(
                list_ser, 
                str_table_name=str_table_name,
                bl_insert_or_ignore=bl_insert_or_ignore
            )

    def non_iteratively_get_data(
        self, 
        str_resource:str, 
        host:Optional[str]=None, 
        bl_fetch:bool=False, 
        bl_debug:bool=False
    ) -> Optional[pd.DataFrame]:
        """
        Non-iteratively get raw data
        Args:
            - str_resource (str): resource name
            - host (Optional[str]): host name
            - bl_fetch (bool): False as default
            - bl_debug (bool): False as default
        Returns:
            pd.DataFrame
        """
        # setting variables
        dict_instance_vars = self.get_instance_variables
        app_ = self.dict_metadata[str_resource].get('app', None)
        app_ = StrHandler().fill_placeholders(app_, dict_instance_vars)
        host_ = host if host is not None else (
            self.dict_metadata[str_resource].get('host', None) \
            if self.dict_metadata[str_resource].get('host', None) is not None \
            else self.dict_metadata['credentials']['host']
        )
        host_ = StrHandler().fill_placeholders(host_, dict_instance_vars)
        dict_headers = self.dict_metadata[str_resource]['headers'] \
            if self.dict_metadata[str_resource].get('headers', None) is not None \
            else self.dict_metadata['credentials'].get('headers', None)
        if dict_headers is not None:
            dict_headers = HandlingDicts().fill_placeholders(dict_headers, dict_instance_vars)
        payload = self.dict_metadata[str_resource]['payload'] \
            if self.dict_metadata[str_resource].get('payload', None) is not None \
            else self.dict_metadata['credentials'].get('payload', None)
        if payload is not None:
            if isinstance(payload, dict):
                payload = HandlingDicts().fill_placeholders(payload, dict_instance_vars)
            elif isinstance(payload, str):
                payload = StrHandler().fill_placeholders(payload, dict_instance_vars)
            else:
                raise Exception('Payload must be either dict or str.')
        list_ignorable_exceptions = self.dict_metadata[str_resource].get(
            'list_ignorable_exceptions', list()
        ) if self.dict_metadata[str_resource].get(
            'list_ignorable_exceptions', list()
        ) is not None else list()
        list_ignorable_exceptions = [
            eval(exception) if isinstance(exception, str) else exception
            for exception in list_ignorable_exceptions
        ]
        # requiring data
        try:
            df_ = self.trt_req(
                self.dict_metadata[str_resource]['req_method'],
                host_,
                self.dict_metadata[str_resource]['dtypes'], 
                dict_headers,
                payload,
                app_, 
                self.dict_metadata[str_resource]['bl_verify'],
                self.dict_metadata[str_resource]['timeout'],
                self.dict_metadata[str_resource]['cols_from_case'],
                self.dict_metadata[str_resource]['cols_to_case'],
                self.dict_metadata[str_resource]['list_cols_drop_dupl'],
                self.dict_metadata[str_resource]['str_fmt_dt'],
                self.dict_metadata[str_resource]['type_error_action'],
                self.dict_metadata[str_resource]['strt_keep_when_duplicated'],
                self.dict_metadata[str_resource].get('regex_patterns', None),
                self.dict_metadata[str_resource].get('df_read_params', None),
                self.dict_metadata[str_resource].get('xml_keys', None),
                bl_debug
            )
        except tuple(list_ignorable_exceptions) as e:
            if self.logger is not None:
                CreateLog().warnings(
                    self.logger, 
                    f'Non-iterable method encountered ignorable error: {str(e)}. Continuing...'
                )
            else:
                print(f'Non-iterable method encountered ignorable error: {str(e)}. Continuing...')
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

    def iteratively_get_data(
        self, 
        str_resource:str, 
        host:Optional[str]=None, 
        bl_fetch:bool=False, 
        bl_debug:bool=False
    ) -> Optional[pd.DataFrame]:
        """
        Iteratively get raw data
        Args:
            - str_resource (str): resource name
            - host (Optional[str]): host name
            - bl_fetch (bool): False as default
            - bl_debug (bool): False as default
        Returns:
            Optional[pd.DataFrame]
        """
        # setting variables
        list_ser = list()
        i = 0
        dict_instance_vars = self.get_instance_variables
        app_ = self.dict_metadata[str_resource].get('app', None)
        app_ = StrHandler().fill_placeholders(app_, dict_instance_vars)
        host_ = host if host is not None else (
            self.dict_metadata[str_resource].get('host', None) \
            if self.dict_metadata[str_resource].get('host', None) is not None \
            else self.dict_metadata['credentials']['host']
        )
        host_ = StrHandler().fill_placeholders(host_, dict_instance_vars)
        dict_headers = self.dict_metadata[str_resource]['headers'] \
            if self.dict_metadata[str_resource].get('headers', None) is not None \
            else self.dict_metadata['credentials'].get('headers', None)
        if dict_headers is not None:
            dict_headers = HandlingDicts().fill_placeholders(dict_headers, dict_instance_vars)
        payload = self.dict_metadata[str_resource]['payload'] \
            if self.dict_metadata[str_resource].get('payload', None) is not None \
            else self.dict_metadata['credentials'].get('payload', None)
        if payload is not None:
            if isinstance(payload, dict):
                payload = HandlingDicts().fill_placeholders(payload, dict_instance_vars)
            elif isinstance(payload, str):
                payload = StrHandler().fill_placeholders(payload, dict_instance_vars)
            else:
                raise Exception('Payload must be either dict or str.')
        list_slugs = self.list_slugs if self.list_slugs is not None \
            else self.dict_metadata[str_resource].get('slugs', None)
        list_ignorable_exceptions = self.dict_metadata[str_resource].get(
            'list_ignorable_exceptions', list()
        ) if self.dict_metadata[str_resource].get(
            'list_ignorable_exceptions', list()
        ) is not None else list()
        list_ignorable_exceptions = [
            eval(exception) if isinstance(exception, str) else exception
            for exception in list_ignorable_exceptions
        ]
        # iterating through slugs/number of pages
        if list_slugs is not None:
            str_extract_from_braces = StrHandler().extract_info_between_braces(app_)[0] \
                if StrHandler().extract_info_between_braces(app_) is not None else ''
            if str_extract_from_braces == 'slug':
                for str_slug in list_slugs:
                    try:
                        df_ = self.trt_req(
                            self.dict_metadata[str_resource]['req_method'],
                            host_,
                            self.dict_metadata[str_resource]['dtypes'], 
                            dict_headers,
                            payload,
                            StrHandler().fill_placeholders(app_, {'slug': str_slug}), 
                            self.dict_metadata[str_resource]['bl_verify'],
                            self.dict_metadata[str_resource]['timeout'],
                            self.dict_metadata[str_resource]['cols_from_case'],
                            self.dict_metadata[str_resource]['cols_to_case'],
                            self.dict_metadata[str_resource]['list_cols_drop_dupl'],
                            self.dict_metadata[str_resource]['str_fmt_dt'],
                            self.dict_metadata[str_resource]['type_error_action'],
                            self.dict_metadata[str_resource]['strt_keep_when_duplicated'],
                            self.dict_metadata[str_resource].get('regex_patterns', None),
                            self.dict_metadata[str_resource].get('df_read_params', None),
                            self.dict_metadata[str_resource].get('xml_keys', None),
                            bl_debug
                        )
                        df_['SLUG_URL'] = str_slug
                        list_ser.extend(df_.to_dict(orient='records'))
                        if self.cls_db is not None:
                            self.insert_table(
                                str_resource, 
                                list_ser, 
                                self.dict_metadata[str_resource]['bl_insert_or_ignore'], 
                                self.dict_metadata[str_resource]['bl_schema']
                            )
                            if bl_fetch == False: list_ser = list()
                    except tuple(list_ignorable_exceptions) as e:
                        if self.logger is not None:
                            CreateLog().warnings(
                                self.logger, 
                                'Iteration encountered an ignorable exception ' 
                                + f'{e.__class__.__name__}: {e}. Continuing...'
                            )
                        else:
                            print('Iteration encountered an ignorable exception ' 
                                + f'{e.__class__.__name__}: {e}. Continuing...')
                    except Exception as e:
                        if self.logger is not None:
                            CreateLog().critical(
                                self.logger, f'Iteration failed due to {e.__class__.__name__}: {e}'
                            )
                        else:
                            raise Exception(f'Iteration failed due to {e.__class__.__name__}: {e}')
            elif str_extract_from_braces == 'chunk_slugs':
                list_chunks_slugs = HandlingLists().chunk_list(
                    list_to_chunk=self.list_slugs, 
                    str_character_divides_clients=',',
                    int_chunk=self.dict_metadata[str_resource].get('int_chunk_slugs', 10)
                )
                for str_chunk_slugs in list_chunks_slugs:
                    try:
                        df_ = self.trt_req(
                            self.dict_metadata[str_resource]['req_method'],
                            host_,
                            self.dict_metadata[str_resource]['dtypes'], 
                            dict_headers,
                            payload,
                            StrHandler().fill_placeholders(
                                app_, {'chunk_slugs': str_chunk_slugs}), 
                            self.dict_metadata[str_resource]['bl_verify'],
                            self.dict_metadata[str_resource]['timeout'],
                            self.dict_metadata[str_resource]['cols_from_case'],
                            self.dict_metadata[str_resource]['cols_to_case'],
                            self.dict_metadata[str_resource]['list_cols_drop_dupl'],
                            self.dict_metadata[str_resource]['str_fmt_dt'],
                            self.dict_metadata[str_resource]['type_error_action'],
                            self.dict_metadata[str_resource]['strt_keep_when_duplicated'],
                            self.dict_metadata[str_resource].get('regex_patterns', None),
                            self.dict_metadata[str_resource].get('df_read_params', None),
                            self.dict_metadata[str_resource].get('xml_keys', None),
                            bl_debug
                        )
                        df_['SLUG_URL'] = str_chunk_slugs
                        list_ser.extend(df_.to_dict(orient='records'))
                        if self.cls_db is not None:
                            self.insert_table(
                                str_resource, 
                                list_ser, 
                                self.dict_metadata[str_resource]['bl_insert_or_ignore'], 
                                self.dict_metadata[str_resource]['bl_schema']
                            )
                            if bl_fetch == False: list_ser = list()
                    except tuple(list_ignorable_exceptions) as e:
                        if self.logger is not None:
                            CreateLog().warnings(
                                self.logger, 
                                'Iteration encountered an ignorable exception ' 
                                + f'{e.__class__.__name__}: {e}. Continuing...'
                            )
                        else:
                            print('Iteration encountered an ignorable exception ' 
                                + f'{e.__class__.__name__}: {e}. Continuing...')
                        continue
                    except Exception as e:
                        if self.logger is not None:
                            CreateLog().critical(
                                self.logger, f'Iteration failed due to {e.__class__.__name__}: {e}'
                            )
                        else:
                            raise Exception(f'Iteration failed due to {e.__class__.__name__}: {e}')
            else:
                raise Exception('Neither {{slug}} or {{chunk_slugs}} are found in the app ' 
                                + 'parameter, please revisit it.')
        else:
            while True:
                try:
                    list_ser.extend(
                        self.trt_req(
                            self.dict_metadata[str_resource]['req_method'],
                            host_,
                            self.dict_metadata[str_resource]['dtypes'], 
                            dict_headers,
                            payload,
                            StrHandler().fill_placeholders(app_, {'i': i}),
                            self.dict_metadata[str_resource]['bl_verify'],
                            self.dict_metadata[str_resource]['timeout'],
                            self.dict_metadata[str_resource]['cols_from_case'],
                            self.dict_metadata[str_resource]['cols_to_case'],
                            self.dict_metadata[str_resource]['list_cols_drop_dupl'],
                            self.dict_metadata[str_resource]['str_fmt_dt'],
                            self.dict_metadata[str_resource]['type_error_action'],
                            self.dict_metadata[str_resource]['strt_keep_when_duplicated'],
                            self.dict_metadata[str_resource].get('regex_patterns', None),
                            self.dict_metadata[str_resource].get('df_read_params', None),
                            self.dict_metadata[str_resource].get('xml_keys', None),
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
                except tuple(list_ignorable_exceptions) as e:
                    if self.logger is not None:
                        CreateLog().warnings(
                            self.logger, 
                            'Iteration encountered an ignorable exception ' 
                            + f'{e.__class__.__name__}: {e}. Continuing...'
                        )
                    else:
                        print('Iteration encountered an ignorable exception ' 
                                + f'{e.__class__.__name__}: {e}. Continuing...')
                    continue
                except Exception as e:
                    if self.logger is not None:
                        CreateLog().critical(
                            self.logger, f'Iteration failed due to {e.__class__.__name__}: {e}'
                        )
                    else:
                        raise Exception(f'Iteration failed due to {e.__class__.__name__}: {e}')
        if len(list_ser) > 0:
            return pd.DataFrame(list_ser)
        else:
            return None

    def source(self, str_resource:str, host:Optional[str]=None, bl_fetch:bool=False, 
                bl_debug:bool=False) -> Optional[pd.DataFrame]:
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
        dict_instance_vars = self.get_instance_variables
        app_ = self.dict_metadata[str_resource].get('app', None)
        if \
            (app_ is not None) \
            and (StrHandler().match_string_like(app_, '*{{*}}*') == True):
            app_ = StrHandler().fill_placeholders(app_, dict_instance_vars)
        if \
            (app_ is not None) \
            and (
                (StrHandler().match_string_like(app_, '*{{i}}*') == True) 
                or (StrHandler().match_string_like(app_, '*{{slug}}*') == True)
                or (StrHandler().match_string_like(app_, '*{{chunk_slugs}}*') == True)
            ):
            func_get_data = self.iteratively_get_data
        else:
            func_get_data = self.non_iteratively_get_data
        return func_get_data(str_resource, host, bl_fetch, bl_debug)