import backoff
import chardet
import fitz
import pdfplumber
import urllib3
import os
import re
import subprocess
import tempfile
import pandas as pd
from time import sleep
from abc import ABC, abstractmethod
from datetime import datetime
from io import BytesIO, StringIO, TextIOWrapper
from logging import Logger
from typing import Any, Dict, List, Optional, Tuple, Type, Union, Literal
from urllib.parse import parse_qs, urlparse
from zipfile import ZipExtFile, ZipFile
from selenium.webdriver.remote.webdriver import WebDriver
from sqlalchemy.orm import Session
from requests import Request, Response, Session as ReqSession, request
from requests.exceptions import (ReadTimeout, ConnectTimeout, ChunkedEncodingError,
                                 RequestException, HTTPError, JSONDecodeError)
from stpstone.transformations.standardization.dataframe import DFStandardization
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.loggs.db_logs import DBLogs
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement, RemoteFiles
from stpstone.utils.parsers.json import JsonFiles
from stpstone.utils.parsers.lists import ListHandler
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.parsers.xml import XMLFiles
from stpstone.utils.webdriver_tools.selenium_wd import SeleniumWD


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class UtilsRequests(ABC):

    def mock_response(
        self, file: Union[BytesIO, TextIOWrapper, StringIO, ZipExtFile], file_name: str
    ) -> Response:
        mock_resp = Response()
        mock_resp._content = file.read()
        mock_resp.url = file_name
        mock_resp.status_code = 200
        return mock_resp

    def columns_length(self, list_lines: List[Any]) -> int:
        dict_columns_count = {}
        for line in list_lines:
            num_columns = len(line.split(";"))
            dict_columns_count[num_columns] = dict_columns_count.get(num_columns, 0) + 1
        return max(dict_columns_count, key=dict_columns_count.get)

    def xml_find(
        self,
        soup_content: Type[XMLFiles],
        tag: str,
        tag_name: str,
        dict_xml_keys: Dict[str, Any],
    ) -> Dict[str, Union[str, float, int]]:
        dict_ = dict()
        soup_tag = soup_content.find(tag)
        if soup_tag is None:
            dict_[tag_name] = None
        else:
            dict_[tag_name] = soup_tag.get_text()
        if "attributes" in dict_xml_keys:
            for key_attrb_xml in dict_xml_keys["attributes"]:
                if (
                    (soup_tag is not None)
                    and (dict_xml_keys["attributes"][key_attrb_xml] is not None)
                    and (dict_xml_keys["attributes"][key_attrb_xml] in soup_tag.attrs)
                ):
                    dict_[dict_xml_keys["attributes"][key_attrb_xml]] = soup_tag.attrs[
                        dict_xml_keys["attributes"][key_attrb_xml]
                    ]
        return dict_

    def read_csv_with_error(
        self, file: Union[BytesIO, TextIOWrapper, StringIO, ZipExtFile]
    ) -> Union[BytesIO, TextIOWrapper, StringIO]:
        """
        Reads a file (BytesIO, TextIOWrapper, StringIO, ZipExtFile), corrects problematic lines,
        and returns the corrected content in the same file type as the input

        Args:
            file (Union[BytesIO, TextIOWrapper, StringIO, ZipExtFile]): The file to read.

        Returns:
            Union[BytesIO, TextIOWrapper, StringIO]: The corrected content in the same file type.
        """
        # reset the pointer to the beginning
        file.seek(0)
        if isinstance(file, BytesIO):
            content = file.read().decode("utf-8")
            list_lines = content.splitlines()
        elif isinstance(file, (TextIOWrapper, StringIO)):
            list_lines = file.readlines()
        elif isinstance(file, ZipExtFile):
            list_lines = [line.decode("utf-8") for line in file.readlines()]
        else:
            raise ValueError(
                "Unsupported file type. Expected BytesIO or TextIOWrapper."
            )
        list_corrected_lines = [
            line
            for line in list_lines
            if len(line.split(";")) == self.columns_length(list_lines)
        ]
        corrected_content = "\n".join(list_corrected_lines)
        if isinstance(file, BytesIO):
            return BytesIO(corrected_content.encode("utf-8"))
        elif isinstance(file, TextIOWrapper):
            return TextIOWrapper(BytesIO(corrected_content.encode("utf-8")))
        elif isinstance(file, StringIO):
            return StringIO(corrected_content)
        elif isinstance(file, ZipExtFile):
            return BytesIO(corrected_content.encode("utf-8"))

    def pdf_doc_tables_response(self, bytes_pdf: BytesIO) -> pd.DataFrame:
        list_ser = list()
        with pdfplumber.open(bytes_pdf) as pdf:
            for page in pdf.pages:
                list_ = page.extract_tables()
                list_ser.extend(
                    HandlingDicts().pair_keys_with_values(list_[0][0], list_[0][1:])
                )
        return pd.DataFrame(list_ser)

    def pdf_doc_regex(
        self,
        url: str,
        bytes_pdf: BytesIO,
        dict_regex_patterns: Dict[str, Dict[str, Any]],
        int_pgs_join: int = 2,
    ):
        list_pages = list()
        list_matches = list()
        dict_count_matches = dict()
        doc_pdf = fitz.open(
            stream=bytes_pdf,
            filetype=DirFilesManagement().get_last_file_extension(url),
        )
        str_ = ""
        for i in range(0, len(doc_pdf)):
            str_ = str_ + "\n" + doc_pdf[i].get_text("text")
            if (i % int_pgs_join == 0) \
                or (i == len(doc_pdf) - 1):
                list_pages.append(str_)
                str_ = ""
        for i, str_page in enumerate(list_pages):
            str_page = StrHandler().remove_diacritics_nfkd(str_page, bl_lower_case=True)
            if (len(dict_count_matches) > 0) \
                and (all(count > 0 for count in dict_count_matches.values())): break
            for str_event, dict_l1 in dict_regex_patterns.items():
                for str_condition, regex_pattern in dict_l1.items():
                    if str_event not in dict_count_matches:
                        dict_count_matches[str_event] = 0
                    if dict_count_matches[str_event] > 0:
                        break
                    regex_pattern = StrHandler().remove_diacritics_nfkd(
                        regex_pattern, bl_lower_case=False
                    )
                    regex_match = re.search(
                        regex_pattern,
                        str_page,
                        # flags=re.DOTALL | re.MULTILINE
                    )
                    if (regex_match is not None) \
                        and (regex_match.group(0) is not None) \
                        and (len(regex_match.group(0)) > 0):
                        dict_count_matches[str_event] += 1
                        dict_ = {
                            "EVENT": str_event.upper(),
                            "MATCH_PATTERN": str_condition.upper(),
                            "PATTERN_REGEX": regex_pattern,
                        }
                        for i_match in range(0, len(regex_match.groups()) + 1):
                            regex_group = regex_match.group(i_match).replace("\n", " ")
                            regex_group = re.sub(r"\s+", " ", regex_group).strip()
                            dict_[f"REGEX_GROUP_{i_match}"] = regex_group.upper()
                        list_matches.append(dict_)
                if dict_count_matches[str_event] == 0:
                    list_matches.append({
                        "EVENT": str_event.upper(),
                        "MATCH_PATTERN": "zzN/A",
                        "PATTERN_REGEX": "zzN/A",
                    })
        df_ = pd.DataFrame(list_matches)
        df_.drop_duplicates(inplace=True)
        df_.sort_values(by=["EVENT", "MATCH_PATTERN"], ascending=[True, True], inplace=True)
        df_.drop_duplicates(subset=["EVENT"], inplace=True)
        return df_
    
    def pivot_event_data(self, df_: pd.DataFrame) -> pd.DataFrame:
        """
        Pivots event data from long format to wide format with events as columns.
        
        Args:
            df_ (pd.DataFrame): Input DataFrame containing EVENT, REGEX_GROUP_1 columns
            
        Returns:
            pd.DataFrame: Pivoted DataFrame with one row per country and events as columns
        """
        df_['country_group'] = (df_['EVENT'] == 'COUNTRY_NAME').cumsum()
        df_pivoted = df_.pivot_table(
            index='country_group',
            columns='EVENT',
            values='REGEX_GROUP_1',
            aggfunc='first'
        ).reset_index(drop=True)
        df_pivoted.columns.name = None
        return df_pivoted

    def html_regex(
        self,
        html_content: str,
        dict_regex_patterns: Dict[str, Dict[str, Any]],
    ):
        dict_count_matches = dict()
        list_matches = list()
        with open("data/html_content.html", 'w', encoding='utf-8') as f:
            f.write(html_content)
        for str_event, dict_l1 in dict_regex_patterns.items():
            for str_condition, regex_pattern in dict_l1.items():
                if str_event not in dict_count_matches:
                    dict_count_matches[str_event] = 0
                regex_pattern = StrHandler().remove_diacritics_nfkd(
                    regex_pattern, bl_lower_case=False
                )
                regex_matches = list(re.finditer(regex_pattern, html_content, re.MULTILINE))
                if regex_matches:
                    for int_regex, match in enumerate(regex_matches):
                        dict_ = {
                            "INT_REGEX": int_regex,
                            "EVENT": str_event.upper(),
                            "MATCH_PATTERN": str_condition.upper(),
                            "PATTERN_REGEX": regex_pattern,
                            "REGEX_GROUP_0": match.group(0)
                        }
                        for i, group in enumerate(match.groups(), start=1):
                            dict_[f"REGEX_GROUP_{i}"] = group
                        list_matches.append(dict_)
                        dict_count_matches[str_event] += 1
            if dict_count_matches[str_event] == 0:
                list_matches.append({
                    "INT_REGEX": -1,
                    "EVENT": str_event.upper(),
                    "MATCH_PATTERN": "zzN/A",
                    "PATTERN_REGEX": "zzN/A",
                })
        df_ = pd.DataFrame(list_matches)
        df_.drop_duplicates(inplace=True)
        df_.sort_values(by=["INT_REGEX", "EVENT", "MATCH_PATTERN"], 
                        ascending=[True, True, True], inplace=True)
        return self.pivot_event_data(df_)

    def get_query_params(self, url: str, param: str) -> Union[int, bool, str, float, None]:
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.fragment)
        param_value = query_params.get(param, [None])[0]
        if param_value is not None:
            if isinstance(param_value, bool):
                return param_value
            elif param_value.lower() == 'true':
                return True
            elif param_value.lower() == 'false':
                return False
            try:
                return int(param_value)
            except ValueError:
                return param_value
        return None


class HandleReqResponses(UtilsRequests):

    def handle_response(
        self,
        resp_req: Union[Response, WebDriver],
        dict_xml_keys: Optional[Dict[str, Any]] = None,
        dict_regex_patterns: Optional[Dict[str, Dict[str, str]]] = None,
        dict_df_read_params: Optional[Dict[str, Any]] = None,
        dict_xpaths: Optional[Dict[str, str]] = None,
        list_ignored_file_extensions: Optional[List[str]] = [],
        list_ser_fixed_width_layout: Optional[List[Dict[str, Any]]] = [{}],
        selenium_wd: SeleniumWD = None
    ) -> pd.DataFrame:
        if isinstance(resp_req, WebDriver):
            url = resp_req.current_url
        else:
            url = resp_req.url
        str_file_extension = DirFilesManagement().get_last_file_extension(url)
        file_name = os.path.basename(url) if hasattr(resp_req, "url") else ""
        if self.req_trt_injection(resp_req) is not None:
            df_ = self.req_trt_injection(resp_req)
        elif str_file_extension == "xl_url":
            df_ = self._handle_pandas_excel_url(resp_req.url, dict_df_read_params)
        elif str_file_extension == "csv_url":
            df_ = self._handle_pandas_csv_url(resp_req.url, dict_df_read_params)
        elif isinstance(resp_req, WebDriver):
            df_ = self._handle_web_driver_html(url, resp_req, dict_xpaths, selenium_wd)
        elif str_file_extension == "zip":
            df_ = self._handle_zip_response(
                resp_req,
                dict_xml_keys,
                dict_regex_patterns,
                dict_df_read_params,
                list_ignored_file_extensions,
                list_ser_fixed_width_layout,
                selenium_wd
            )
        elif str_file_extension == "ex_":
            df_ = self._handle_ex_response(
                resp_req,
                dict_xml_keys,
                dict_regex_patterns,
                dict_df_read_params,
                list_ignored_file_extensions,
                list_ser_fixed_width_layout,
                selenium_wd
            )
        elif str_file_extension in ["csv", "txt", "asp", "do", "tex"]:
            if StrHandler().match_string_like(url, "*bl_separator_consistency_check=*") == True:
                bl_separator_consistency_check = (
                    self.get_query_params(url, "bl_separator_consistency_check")
                    if self.get_query_params(url, "bl_separator_consistency_check") is not None
                    else True
                )
            else:
                bl_separator_consistency_check = True
            if dict_df_read_params.get("encoding") is not None:
                resp_req.encoding = dict_df_read_params.get("encoding")
            if (bl_separator_consistency_check == True) \
                and (RemoteFiles().check_separator_consistency(
                    resp_req.content,
                    dict_df_read_params.get("skiprows", 0),
                    dict_df_read_params.get("skipfooter", 0)
                ) == False):
                df_ = self._handle_fwf_response(resp_req, list_ser_fixed_width_layout)
            else:
                df_ = self._handle_csv_response(resp_req, dict_df_read_params)
        elif str_file_extension in ["xlsx", "xls"]:
            df_ = self._handle_excel_response(resp_req, dict_df_read_params)
        elif str_file_extension == "xml":
            df_ = self._handle_xml_response(resp_req, dict_xml_keys)
        elif str_file_extension == "json":
            df_ = self._handle_json_response(resp_req)
        elif str_file_extension in ["pdf", "docx", "doc", "docm", "dot", "dotm"]:
            df_ = self._handle_pdf_doc_response(url, resp_req, dict_regex_patterns)
        elif str_file_extension in ["fwf", "dat"]:
            df_ = self._handle_fwf_response(resp_req, list_ser_fixed_width_layout)
        elif str_file_extension == "html_regex":
            df_ = self.html_regex(resp_req.text, dict_regex_patterns)
        else:
            try:
                json_ = resp_req.json()
            except JSONDecodeError:
                raise Exception(
                    "File extension not expected in the handle response method: "
                    + f"{str_file_extension}, please revisit the if-statement"
                )
            if isinstance(json_, dict) == True:
                df_ = pd.DataFrame([json_])
            else:
                df_ = pd.DataFrame(json_)
            return pd.DataFrame(json_)
        if (file_name is not None) \
            and ("FILE_NAME" not in df_.columns) \
            and (df_.empty == False):
            df_["FILE_NAME"] = file_name
        return df_

    @abstractmethod
    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        return None

    @backoff.on_exception(
        backoff.expo,
        (RequestException, HTTPError, ReadTimeout, ConnectTimeout, ChunkedEncodingError),
        max_tries=20,
        base=2,
        factor=2,
        max_value=1200
    )
    def _handle_web_driver_html(
        self,
        url: str,
        resp_req: WebDriver,
        dict_xpaths: Dict[str, str],
        selenium_wd: SeleniumWD
    ) -> pd.DataFrame:
        try:
            if any(StrHandler().match_string_like(value, "*delay_next_s=*") == True
                for value in dict_xpaths.values()):
                int_delay_next_s = (
                    self.get_query_params(url, "delay_next_s")
                    if self.get_query_params(url, "delay_next_s") is not None
                    else None
                )
            if any(StrHandler().match_string_like(value, "*{{iter}}*") == True
                for value in dict_xpaths.values()):
                int_iter_min = (
                    self.get_query_params(url, "iter_min")
                    if self.get_query_params(url, "iter_min") is not None
                    else 0
                )
                int_iter_max = (
                    self.get_query_params(url, "iter_max")
                    if self.get_query_params(url, "iter_max") is not None
                    else 100
                )
                list_ser = [{
                    key: selenium_wd.find_element(resp_req,
                    StrHandler().fill_placeholders(str_xpath, {"iter": i})).text
                    for key, str_xpath in dict_xpaths.items()
                    for i in range(int_iter_min, int_iter_max)
                }]
            else:
                list_ser = [{
                    key: selenium_wd.find_element(resp_req, str_xpath).text
                    for key, str_xpath in dict_xpaths.items()
                }]
        finally:
            resp_req.quit()
            if int_delay_next_s is not None: sleep(int_delay_next_s)
        return pd.DataFrame(list_ser)

    def _handle_zip_response(
        self,
        resp_req: Response,
        dict_xml_keys: Optional[Dict[str, Any]] = None,
        dict_regex_patterns: Optional[Dict[str, Dict[str, str]]] = None,
        dict_df_read_params: Optional[Dict[str, Any]] = None,
        list_ignored_file_extensions: Optional[List[str]] = [],
        list_ser_fixed_width_layout: Optional[List[Dict[str, Any]]] = [{}],
        selenium_wd: SeleniumWD = None
    ) -> Union[pd.DataFrame, List[pd.DataFrame]]:
        zipfile = ZipFile(BytesIO(resp_req.content))
        list_ = []
        for file_name in zipfile.namelist():
            with zipfile.open(file_name) as file:
                str_file_extension = DirFilesManagement().get_last_file_extension(
                    file_name
                )
                if str_file_extension in list_ignored_file_extensions:
                    continue
                mock_resp = self.mock_response(file, file_name)
                df_ = self.handle_response(
                    mock_resp,
                    dict_xml_keys,
                    dict_regex_patterns,
                    dict_df_read_params,
                    list_ignored_file_extensions,
                    list_ser_fixed_width_layout,
                    selenium_wd
                )
                if df_.empty == True:
                    continue
                elif isinstance(df_, pd.DataFrame):
                    list_.extend(df_.to_dict(orient="records"))
                elif isinstance(df_, list):
                    for df_nested in df_:
                        list_.extend(df_nested.to_dict(orient="records"))
        if list_:
            return pd.DataFrame(list_)
        else:
            return pd.DataFrame()

    def _handle_csv_response(
        self,
        file: Union[BytesIO, TextIOWrapper, Response],
        dict_df_read_params: Optional[Dict[str, Any]],
    ) -> pd.DataFrame:
        if isinstance(file, BytesIO):
            file.seek(0)
        elif isinstance(file, Response):
            file = StringIO(file.text)
        try:
            return pd.read_csv(file, **dict_df_read_params)
        except pd.errors.ParserError:
            file = self.read_csv_with_error(file)
            if dict_df_read_params is not None:
                dict_df_read_corrected_params = dict_df_read_params.copy()
                dict_df_read_corrected_params.pop("skiprows", None)
            else:
                dict_df_read_corrected_params = dict()
            return pd.read_csv(file, **dict_df_read_corrected_params)

    def _handle_pandas_excel_url(
        self,
        url: Union[str],
        dict_df_read_params: Optional[Dict[str, Any]],
    ) -> pd.DataFrame:
        dict_df_read_params = dict_df_read_params if dict_df_read_params is not None else {}
        return pd.read_excel(url, **dict_df_read_params)

    def _handle_pandas_csv_url(
        self,
        url: Union[str],
        dict_df_read_params: Optional[Dict[str, Any]],
    ) -> pd.DataFrame:
        dict_df_read_params = dict_df_read_params if dict_df_read_params is not None else {}
        return pd.read_csv(url, **dict_df_read_params)

    def _handle_excel_response(
        self,
        file: Union[BytesIO, TextIOWrapper, Response],
        dict_df_read_params: Optional[Dict[str, Any]],
    ) -> pd.DataFrame:
        if isinstance(file, BytesIO):
            file.seek(0)
        elif isinstance(file, Response):
            file = StringIO(file.text)
        dict_df_read_params = dict_df_read_params if dict_df_read_params is not None else {}
        return pd.read_excel(file, **dict_df_read_params)

    def _handle_xml_response(
        self,
        file: Union[BytesIO, TextIOWrapper, Response],
        dict_xml_keys: Dict[str, Any],
    ) -> pd.DataFrame:
        list_ser = list()
        if isinstance(file, BytesIO):
            file.seek(0)
        if isinstance(file, Response):
            file = StringIO(file.text)
        soup_xml = XMLFiles().memory_parser(file)
        for key, list_tags in dict_xml_keys["tags"].items():
            for soup_content in soup_xml.find_all(key):
                for tag in list_tags:
                    if isinstance(tag, str):
                        dict_l1 = self.xml_find(soup_content, tag, tag, dict_xml_keys)
                        if ("dict_" in locals()) and (isinstance(dict_, dict)):
                            dict_ = HandlingDicts().merge_n_dicts(dict_, dict_l1)
                        else:
                            dict_ = dict_l1
                    elif isinstance(tag, dict):
                        key_ = list(tag.keys())[0]
                        list_values = list(tag.values())[0]
                        for soup_l2 in soup_content.find_all(key_):
                            for tag_l2 in list_values:
                                dict_l2 = self.xml_find(
                                    soup_l2, tag_l2, f"{key_}{tag_l2}", dict_xml_keys
                                )
                                if ("dict_" in locals()) and (isinstance(dict_, dict)):
                                    dict_ = HandlingDicts().merge_n_dicts(
                                        dict_, dict_l2
                                    )
                                else:
                                    dict_ = dict_l2
            dict_ = HandlingDicts().add_key_value_to_dicts(dict_, "ROOT_TAG", key)
            list_ser.append(dict_)
        return pd.DataFrame(list_ser)

    def _handle_json_response(
        self, file: Union[BytesIO, TextIOWrapper]
    ) -> pd.DataFrame:
        if isinstance(file, BytesIO):
            file.seek(0)
        json_file = file.read()
        list_ser = JsonFiles().loads_message_like(json_file)
        df_ = pd.DataFrame(list_ser)
        return df_

    def _handle_pdf_doc_response(
        self,
        url: str,
        resp_req: Response,
        dict_regex_patterns: Dict[str, Dict[str, str]],
        default_int_pgs_join: int = 2
    ) -> pd.DataFrame:
        bytes_pdf = BytesIO(resp_req.content)
        # checking wheter to read tables or use regex
        if StrHandler().match_string_like(url, "*feat=read_tables*") == True:
            return self.pdf_doc_tables_response(bytes_pdf)
        else:
            if StrHandler().match_string_like(url, "*int_pgs_join=*") == True:
                int_pgs_join = (
                    self.get_query_params(url, "int_pgs_join")
                    if self.get_query_params(url, "int_pgs_join") is not None
                    else default_int_pgs_join
                )
            else:
                int_pgs_join = default_int_pgs_join
            return self.pdf_doc_regex(
                url, bytes_pdf, dict_regex_patterns, int_pgs_join
            )

    def _handle_ex_response(
        self,
        resp_req: Response,
        dict_xml_keys: Optional[Dict[str, Any]] = None,
        dict_regex_patterns: Optional[Dict[str, Dict[str, str]]] = None,
        dict_df_read_params: Optional[Dict[str, Any]] = None,
        list_ignored_file_extensions: Optional[List[str]] = [],
        list_ser_fixed_width_layout: Optional[List[Dict[str, Any]]] = [{}],
        selenium_wd: SeleniumWD = None
    ) -> pd.DataFrame:
        list_ser = list()
        with tempfile.TemporaryDirectory() as temp_dir_path:
            ex_file_path = RemoteFiles().get_file_from_zip(
                resp_req, temp_dir_path, (".ex_")
            )
            os.chmod(ex_file_path, 0o755)
            subprocess.run([ex_file_path], cwd=temp_dir_path, check=True)
            list_files = [
                f
                for f in os.listdir(temp_dir_path)
                if f != os.path.basename(ex_file_path) and not f.endswith(".zip")
            ]
            for file_name in list_files:
                file_path = os.path.join(temp_dir_path, file_name)
                with open(file_path, "rb") as file:
                    mock_resp = self.mock_response(file, file_name)
                    df_ = self.handle_response(
                        mock_resp,
                        dict_xml_keys,
                        dict_regex_patterns,
                        dict_df_read_params,
                        list_ignored_file_extensions,
                        list_ser_fixed_width_layout,
                        selenium_wd
                    )
                    list_ser.extend(df_.to_dict(orient="records"))
            return pd.DataFrame(list_ser)

    def _handle_fwf_response(
        self, resp_req: Response, list_ser_fixed_width_layout: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        list_colspecs = [
            (dict_["start"], dict_["end"]) for dict_ in list_ser_fixed_width_layout
        ]
        list_colnames = [dict_["field"] for dict_ in list_ser_fixed_width_layout]
        if (
            (resp_req.headers is not None)
            and ("application/zip" in resp_req.headers.get("Content-Type", ""))
            and (".zip" in resp_req.headers.get("Content-Disposition"))
        ):
            with tempfile.TemporaryDirectory() as temp_dir_path:
                file_path = RemoteFiles().get_file_from_zip(
                    resp_req, temp_dir_path, (".fwf", ".dat", ".txt", "")
                )
                return pd.read_fwf(
                    file_path, colspecs=list_colspecs, names=list_colnames
                )
        else:
            try:
                encoding = chardet.detect(resp_req.content)["encoding"]
            except:
                encoding = "latin-1"
            decoded_content = resp_req.content.decode(encoding)
            return pd.read_fwf(
                StringIO(decoded_content), colspecs=list_colspecs, names=list_colnames
            )


class ABCRequests(HandleReqResponses):

    def __init__(
        self,
        dict_metadata: Dict[str, Any],
        session: Optional[ReqSession] = None,
        dt_ref: datetime = DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db: Optional[Session] = None,
        logger: Optional[Logger] = None,
        token: Optional[str] = None,
        list_slugs: Optional[List[str]] = None,
        path_webdriver: Optional[str] = None,
        int_port: Optional[int] = None,
        str_user_agent: Optional[str] = None,
        int_wait_load_seconds: int = 10,
        int_delay_seconds: int = 10,
        bl_headless: bool = False,
        bl_incognito: bool = False,
        bl_ts_log_str: bool = True
    ) -> None:
        self.dict_metadata = dict_metadata
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.list_slugs = list_slugs
        self.path_webdriver = path_webdriver
        self.int_port = int_port
        self.str_user_agent = str_user_agent
        self.int_wait_load_seconds = int_wait_load_seconds
        self.int_delay_seconds = int_delay_seconds
        self.bl_headless = bl_headless
        self.bl_incognito = bl_incognito
        self.bl_ts_log_str = bl_ts_log_str
        self.list_options_wd = None \
            if self.dict_metadata["credentials"].get("web_driver", None) is None \
            else self.dict_metadata["credentials"]["web_driver"]["options"]
        self.pattern_special_http_chars = r'["<>#%{}|\\^~\[\]` ]'
        self.token = (
            token
            if token is not None
            else (
                self.access_token
                if self.dict_metadata["credentials"]["token"]["host"] is not None
                else None
            )
        )
        self.create_log = CreateLog()

    @property
    def access_token(self):
        dict_instance_vars = self.get_instance_variables
        url_token = (
            self.dict_metadata["credentials"]["token"]["host"]
            + self.dict_metadata["credentials"]["token"]["app"]
            if self.dict_metadata["credentials"]["token"]["app"] is not None
            else self.dict_metadata["credentials"]["token"]["host"]
        )
        url_token = StrHandler().fill_placeholders(url_token, dict_instance_vars)
        resp_req = self.generic_req(
            self.dict_metadata["credentials"]["token"]["get"]["req_method"],
            url_token,
            self.dict_metadata["credentials"]["token"]["get"]["bl_verify"],
            self.dict_metadata["credentials"]["token"]["get"]["timeout"],
            self.dict_metadata["credentials"]["token"].get("headers", None),
        )
        return resp_req.json()[
            self.dict_metadata["credentials"]["token"]["keys"]["token"]
        ]

    @property
    def get_instance_variables(self) -> Dict[str, Any]:
        """
        Gather all instance variables (self.*) into a dictionary.
        """
        return {
            key: value
            for key, value in self.__dict__.items()
            if not key.startswith("__")
        }

    # ! TODO: implement timeout
    def generic_req_w_session(
        self,
        req_method: str,
        url: str,
        bl_verify: bool,
        tup_timeout: Tuple[float, float] = (12.0, 12.0),
        dict_headers: Optional[Dict[str, str]] = None,
        payload: Optional[Union[str, Dict[str, str]]] = None,
        cookies: Optional[Dict[str, str]] = None
    ) -> Response:
        if re.search(self.pattern_special_http_chars, url):
            #   prepare the request manually to preserve special characters
            resp_req = Request(req_method, url, headers=dict_headers, params=payload, cookies=cookies)
            req_preppped = self.session.prepare_request(resp_req)
            req_preppped.url = url
            resp_req = self.session.send(req_preppped, verify=bl_verify)
        else:
            if req_method == "GET":
                resp_req = self.session.get(
                    url, verify=bl_verify, headers=dict_headers, params=payload, cookies=cookies
                )
            elif req_method == "POST":
                resp_req = self.session.post(
                    url, verify=bl_verify, headers=dict_headers, data=payload, cookies=cookies
                )
        resp_req.raise_for_status()
        return resp_req

    # ! TODO: implement timeout
    @backoff.on_exception(
        backoff.expo,
        (RequestException, HTTPError, ReadTimeout, ConnectTimeout, ChunkedEncodingError),
        max_tries=20,
        base=2,
        factor=2,
        max_value=1200
    )
    def generic_req_wo_session(
        self,
        req_method: str,
        url: str,
        bl_verify: bool,
        tup_timeout: Tuple[float, float] = (12.0, 12.0),
        dict_headers: Optional[Dict[str, str]] = None,
        payload: Optional[Dict[str, str]] = None,
        cookies: Optional[Dict[str, str]] = None
    ) -> Response:
        if re.search(self.pattern_special_http_chars, url):
            #   prepare the request manually to preserve special characters
            req = Request(req_method, url, headers=dict_headers, params=payload, cookies=cookies)
            req_preppped = req.prepare()
            with ReqSession() as session:
                resp_req = session.send(req_preppped, verify=bl_verify)
        else:
            return request(
                req_method,
                url,
                verify=bl_verify,
                headers=dict_headers,
                params=payload if req_method == "GET" else None,
                data=payload if req_method == "POST" else None,
                cookies=cookies,
                # timeout=tup_timeout
            )
        resp_req.raise_for_status()
        return resp_req

    def generic_req(
        self,
        req_method: Literal["GET", "POST"],
        url: str,
        bl_verify: bool,
        tup_timeout: Tuple[float, float] = (12.0, 12.0),
        dict_headers: Optional[Dict[str, str]] = None,
        payload: Optional[Dict[str, str]] = None,
        cookies: Optional[Dict[str, str]] = None
    ) -> Response:
        """
        Check wheter the request method is valid and do the request with distinctions for session and
            local proxy-based requests
        Args:
            req_method (str): request method
            url (str): request url
            bl_verify (bool): verify request
            tup_timeout (Tuple[float, float]): request timeout
        Returns:
            Tuple[Response, str]
        """
        if self.session is not None:
            func_generic_req = self.generic_req_w_session
        else:
            func_generic_req = self.generic_req_wo_session
        resp_req = func_generic_req(
            req_method, url, bl_verify, tup_timeout, dict_headers, payload, cookies
        )
        return resp_req

    def trt_req(
        self,
        req_method: str,
        host: str,
        dict_dtypes: Dict[str, Any],
        dict_headers: Optional[Dict[str, str]] = None,
        payload: Optional[Dict[str, str]] = None,
        app: Optional[str] = None,
        bl_verify: bool = False,
        tup_timeout: Tuple[float, float] = (12.0, 12.0),
        cookies: Optional[Dict[str, str]] = None,
        cols_from_case: Optional[str] = None,
        cols_to_case: Optional[str] = None,
        list_cols_drop_dupl: List[str] = None,
        str_fmt_dt: str = "YYYY-MM-DD",
        type_error_action: str = "raise",
        strategy_keep_when_dupl: str = "first",
        dict_regex_patterns: Optional[Dict[str, Dict[str, str]]] = None,
        dict_df_read_params: Optional[Dict[str, Any]] = None,
        list_ignored_file_extensions: Optional[List[str]] = [],
        list_ser_fixed_width_layout: Optional[List[Dict[str, Any]]] = [{}],
        dict_xml_keys: Optional[Dict[str, Any]] = None,
        dict_fillna_strt: Optional[Dict[str, str]] = {},
        xpath_el_wait_until_loaded: Optional[str] = None,
        dict_xpaths: Optional[Dict[str, str]] = None
    ) -> pd.DataFrame:
        if payload is not None: payload = JsonFiles().dict_to_json(payload)
        url = host + app if app is not None else host
        self.create_log.log_message(self.logger, f"Starting request to: {url}", "info")
        if self.dict_metadata["credentials"].get("web_driver", None) is not None:
            dict_instance_vars = self.get_instance_variables
            list_options_wd = [
                StrHandler().fill_placeholders(x, dict_instance_vars) for x in self.list_options_wd
            ]
            if self.session is not None:
                str_proxy = self.session.proxies
            else:
                str_proxy = None
            selenium_wd = SeleniumWD(
                url, self.path_webdriver, self.int_port, self.str_user_agent, self.int_wait_load_seconds,
                self.int_delay_seconds, str_proxy, self.bl_headless, self.bl_incognito,
                list_options_wd
            )
            if xpath_el_wait_until_loaded is not None:
                selenium_wd.wait_until_el_loaded(xpath_el_wait_until_loaded)
            resp_req = selenium_wd.web_driver
        else:
            selenium_wd = None
            resp_req = self.generic_req(
                req_method, url, bl_verify, tup_timeout, dict_headers, payload, cookies
            )
        df_ = self.handle_response(
            resp_req,
            dict_xml_keys,
            dict_regex_patterns,
            dict_df_read_params,
            dict_xpaths,
            list_ignored_file_extensions,
            list_ser_fixed_width_layout,
            selenium_wd
        )
        cls_df_stdz = DFStandardization(
            dict_dtypes=dict_dtypes,
            cols_from_case=cols_from_case,
            cols_to_case=cols_to_case,
            list_cols_drop_dupl=list_cols_drop_dupl,
            dict_fillna_strt=dict_fillna_strt,
            str_fmt_dt=str_fmt_dt,
            type_error_action=type_error_action,
            strategy_keep_when_dupl=strategy_keep_when_dupl,
            encoding=(
                dict_df_read_params.get("encoding", "latin-1")
                if dict_df_read_params is not None
                else "latin-1"
            ),
            logger=self.logger,
        )
        df_ = cls_df_stdz.pipeline(df_)
        df_ = DBLogs().audit_log(df_, url, self.dt_ref, self.bl_ts_log_str)
        return df_

    def insert_table(
        self,
        str_resource: str,
        list_ser: Optional[List[Dict[str, Any]]] = None,
        bl_insert_or_ignore: bool = False,
        bl_schema: bool = True,
    ) -> None:
        """
        Insert data into data table
        Args:
            str_resource (str): resource name
            list_ser (List[Dict[str, Any]]): data to insert
            bl_insert_or_ignore (bool): False as default
            bl_schema (bool): some databases, like SQLite, don't have schemas - True as default
        Raises:
            Exception: If database or data is not defined
        """
        if self.cls_db is None:
            raise Exception(
                "Data insertion failed due to lack of database definition. "
                "Please revisit this parameter."
            )
        if list_ser is None:
            raise Exception(
                "Data insertion failed due to lack of data. "
                "Please revisit this parameter."
            )
        if str_resource == "general":
            return None
        elif str_resource == "metadata":
            for _, dict_ in self.dict_metadata[str_resource].items():
                str_table_name = dict_["table_name"]
                if bl_schema == False:
                    str_table_name = f"{dict_['schema']}_{str_table_name}"
                self.cls_db.insert(
                    dict_["data"],
                    str_table_name=str_table_name,
                    bl_insert_or_ignore=True,
                )
        else:
            str_table_name = self.dict_metadata[str_resource]["table_name"]
            if bl_schema == False:
                str_table_name = f"{self.dict_metadata[str_resource]['schema']}_{str_table_name}"
            self.cls_db.insert(
                list_ser,
                str_table_name=str_table_name,
                bl_insert_or_ignore=bl_insert_or_ignore,
            )

    def non_iteratively_get_data(
        self,
        str_resource: str,
        host: Optional[str] = None,
        bl_fetch: bool = False,
    ) -> Optional[pd.DataFrame]:
        """
        Non-iteratively get raw data
        Args:
            str_resource (str): resource name
            host (Optional[str]): host name
            bl_fetch (bool): False as default
        Returns:
            pd.DataFrame
        """
        dict_instance_vars = self.get_instance_variables
        app_ = self.dict_metadata[str_resource].get("app", None)
        app_ = StrHandler().fill_placeholders(app_, dict_instance_vars)
        host_ = (
            host
            if host is not None
            else (
                self.dict_metadata[str_resource].get("host", None)
                if self.dict_metadata[str_resource].get("host", None) is not None
                else self.dict_metadata["credentials"]["host"]
            )
        )
        host_ = StrHandler().fill_placeholders(host_, dict_instance_vars)
        dict_headers = (
            self.dict_metadata[str_resource]["headers"]
            if self.dict_metadata[str_resource].get("headers", None) is not None
            else self.dict_metadata["credentials"].get("headers", None)
        )
        if dict_headers is not None:
            dict_headers = HandlingDicts().fill_placeholders(
                dict_headers, dict_instance_vars
            )
        payload = (
            self.dict_metadata[str_resource]["payload"]
            if self.dict_metadata[str_resource].get("payload", None) is not None
            else self.dict_metadata["credentials"].get("payload", None)
        )
        if payload is not None:
            if isinstance(payload, dict):
                payload = HandlingDicts().fill_placeholders(payload, dict_instance_vars)
            elif isinstance(payload, str):
                payload = StrHandler().fill_placeholders(payload, dict_instance_vars)
            else:
                raise Exception("Payload must be either dict or str.")
        list_ignorable_exceptions = (
            self.dict_metadata[str_resource].get("list_ignorable_exceptions", list())
            if self.dict_metadata[str_resource].get("list_ignorable_exceptions", list())
            is not None
            else list()
        )
        list_ignorable_exceptions = [
            eval(exception) if isinstance(exception, str) else exception
            for exception in list_ignorable_exceptions
        ]
        # requiring data
        try:
            df_ = self.trt_req(
                self.dict_metadata[str_resource]["req_method"],
                host_,
                self.dict_metadata[str_resource]["dtypes"],
                dict_headers,
                payload,
                app_,
                self.dict_metadata[str_resource]["bl_verify"],
                self.dict_metadata[str_resource]["timeout"],
                self.dict_metadata[str_resource].get("cookies", None),
                self.dict_metadata[str_resource]["cols_from_case"],
                self.dict_metadata[str_resource]["cols_to_case"],
                self.dict_metadata[str_resource]["list_cols_drop_dupl"],
                self.dict_metadata[str_resource]["str_fmt_dt"],
                self.dict_metadata[str_resource]["type_error_action"],
                self.dict_metadata[str_resource]["strategy_keep_when_dupl"],
                self.dict_metadata[str_resource].get("regex_patterns", None),
                self.dict_metadata[str_resource].get("df_read_params", None),
                self.dict_metadata[str_resource].get("ignored_file_extensions", []),
                self.dict_metadata[str_resource].get("fixed_width_layout", [{}]),
                self.dict_metadata[str_resource].get("xml_keys", None),
                self.dict_metadata[str_resource].get("fillna_strt", {}),
                self.dict_metadata[str_resource]["web_driver"].get("xpath_el_wait_until_loaded", {}),
                self.dict_metadata[str_resource].get("xpaths", [{}]),
            )
        except tuple(list_ignorable_exceptions) as e:
            self.create_log.log_message(
                self.logger,
                "Iteration encountered an ignorable exception "
                + f"{e.__class__.__name__}: {e}. Continuing...",
                "warning"
            )
        except Exception as e:
            self.create_log.log_message(
                self.logger,
                f"Iteration failed due to {e.__class__.__name__}: {e}",
                "critical"
            )
            raise Exception(
                f"Iteration failed due to {e.__class__.__name__}: {e}"
            )
        if self.cls_db is not None:
            self.insert_table(
                str_resource,
                df_.to_dict(orient="records"),
                self.dict_metadata[str_resource]["bl_insert_or_ignore"],
                self.dict_metadata[str_resource]["bl_schema"],
            )
        if bl_fetch == True:
            return df_
        else:
            return None

    def iteratively_get_data(
        self,
        str_resource: str,
        host: Optional[str] = None,
        bl_fetch: bool = False,
    ) -> Optional[pd.DataFrame]:
        """
        Iteratively get raw data
        Args:
            str_resource (str): resource name
            host (Optional[str]): host name
            bl_fetch (bool): False as default
        Returns:
            Optional[pd.DataFrame]
        """
        list_ser = list()
        i = 0
        dict_instance_vars = self.get_instance_variables
        app_ = self.dict_metadata[str_resource].get("app", None)
        app_ = StrHandler().fill_placeholders(app_, dict_instance_vars)
        host_ = (
            host
            if host is not None
            else (
                self.dict_metadata[str_resource].get("host", None)
                if self.dict_metadata[str_resource].get("host", None) is not None
                else self.dict_metadata["credentials"]["host"]
            )
        )
        host_ = StrHandler().fill_placeholders(host_, dict_instance_vars)
        dict_headers = (
            self.dict_metadata[str_resource]["headers"]
            if self.dict_metadata[str_resource].get("headers", None) is not None
            else self.dict_metadata["credentials"].get("headers", None)
        )
        if dict_headers is not None:
            dict_headers = HandlingDicts().fill_placeholders(
                dict_headers, dict_instance_vars
            )
        payload = (
            self.dict_metadata[str_resource]["payload"]
            if self.dict_metadata[str_resource].get("payload", None) is not None
            else self.dict_metadata["credentials"].get("payload", None)
        )
        if payload is not None:
            if isinstance(payload, dict):
                payload = HandlingDicts().fill_placeholders(payload, dict_instance_vars)
            elif isinstance(payload, str):
                payload = StrHandler().fill_placeholders(payload, dict_instance_vars)
            else:
                raise Exception("Payload must be either dict or str.")
        list_slugs = (
            self.list_slugs
            if self.list_slugs is not None
            else self.dict_metadata[str_resource].get("slugs", None)
        )
        list_ignorable_exceptions = (
            self.dict_metadata[str_resource].get("list_ignorable_exceptions", list())
            if self.dict_metadata[str_resource].get("list_ignorable_exceptions", list())
            is not None
            else list()
        )
        list_ignorable_exceptions = [
            eval(exception) if isinstance(exception, str) else exception
            for exception in list_ignorable_exceptions
        ]
        # iterating through slugs/number of pages
        if list_slugs is not None:
            str_extract_from_braces = (
                StrHandler().extract_info_between_braces(app_)[0]
                if StrHandler().extract_info_between_braces(app_) is not None
                else ""
            )
            if str_extract_from_braces == "slug":
                for str_slug in list_slugs:
                    try:
                        df_ = self.trt_req(
                            self.dict_metadata[str_resource]["req_method"],
                            host_,
                            self.dict_metadata[str_resource]["dtypes"],
                            dict_headers,
                            payload,
                            StrHandler().fill_placeholders(app_, {"slug": str_slug}),
                            self.dict_metadata[str_resource]["bl_verify"],
                            self.dict_metadata[str_resource]["timeout"],
                            self.dict_metadata[str_resource].get("cookies", None),
                            self.dict_metadata[str_resource]["cols_from_case"],
                            self.dict_metadata[str_resource]["cols_to_case"],
                            self.dict_metadata[str_resource]["list_cols_drop_dupl"],
                            self.dict_metadata[str_resource]["str_fmt_dt"],
                            self.dict_metadata[str_resource]["type_error_action"],
                            self.dict_metadata[str_resource]["strategy_keep_when_dupl"],
                            self.dict_metadata[str_resource].get("regex_patterns", None),
                            self.dict_metadata[str_resource].get("df_read_params", None),
                            self.dict_metadata[str_resource].get("ignored_file_extensions", []),
                            self.dict_metadata[str_resource].get("fixed_width_layout", [{}]),
                            self.dict_metadata[str_resource].get("xml_keys", None),
                            self.dict_metadata[str_resource].get("fillna_strt", {}),
                            self.dict_metadata[str_resource].get("web_driver", {}).get(
                                "xpath_el_wait_until_loaded", {}),
                            self.dict_metadata[str_resource].get("xpaths", [{}]),
                        )
                        df_["SLUG_URL"] = str_slug
                        if df_.empty == True: break
                        list_ser.extend(df_.to_dict(orient="records"))
                        if self.cls_db is not None:
                            self.insert_table(
                                str_resource,
                                list_ser,
                                self.dict_metadata[str_resource]["bl_insert_or_ignore"],
                                self.dict_metadata[str_resource]["bl_schema"],
                            )
                            if bl_fetch == False:
                                list_ser = list()
                    except tuple(list_ignorable_exceptions) as e:
                        self.create_log.log_message(
                            self.logger,
                            "Iteration encountered an ignorable exception "
                            + f"{e.__class__.__name__}: {e}. Continuing...",
                            "warning"
                        )
                    except Exception as e:
                        self.create_log.log_message(
                            self.logger,
                            f"Iteration failed due to {e.__class__.__name__}: {e}",
                            "critical"
                        )
                        raise Exception(
                            f"Iteration failed due to {e.__class__.__name__}: {e}"
                        )
                    sleep(self.int_delay_seconds)
            elif str_extract_from_braces == "chunk_slugs":
                list_chunks_slugs = ListHandler().chunk_list(
                    list_to_chunk=self.list_slugs,
                    str_character_divides_clients=",",
                    int_chunk=self.dict_metadata[str_resource].get(
                        "int_chunk_slugs", 10
                    ),
                )
                for str_chunk_slugs in list_chunks_slugs:
                    try:
                        df_ = self.trt_req(
                            self.dict_metadata[str_resource]["req_method"],
                            host_,
                            self.dict_metadata[str_resource]["dtypes"],
                            dict_headers,
                            payload,
                            StrHandler().fill_placeholders(
                                app_, {"chunk_slugs": str_chunk_slugs}
                            ),
                            self.dict_metadata[str_resource]["bl_verify"],
                            self.dict_metadata[str_resource]["timeout"],
                            self.dict_metadata[str_resource].get("cookies", None),
                            self.dict_metadata[str_resource]["cols_from_case"],
                            self.dict_metadata[str_resource]["cols_to_case"],
                            self.dict_metadata[str_resource]["list_cols_drop_dupl"],
                            self.dict_metadata[str_resource]["str_fmt_dt"],
                            self.dict_metadata[str_resource]["type_error_action"],
                            self.dict_metadata[str_resource]["strategy_keep_when_dupl"],
                            self.dict_metadata[str_resource].get("regex_patterns", None),
                            self.dict_metadata[str_resource].get("df_read_params", None),
                            self.dict_metadata[str_resource].get("ignored_file_extensions", []),
                            self.dict_metadata[str_resource].get("fixed_width_layout", [{}]),
                            self.dict_metadata[str_resource].get("xml_keys", None),
                            self.dict_metadata[str_resource].get("fillna_strt", {}),
                            self.dict_metadata[str_resource]["web_driver"].get(
                                "xpath_el_wait_until_loaded", {}),
                            self.dict_metadata[str_resource].get("xpaths", [{}]),
                        )
                        df_["SLUG_URL"] = str_chunk_slugs
                        list_ser.extend(df_.to_dict(orient="records"))
                        if self.cls_db is not None:
                            self.insert_table(
                                str_resource,
                                list_ser,
                                self.dict_metadata[str_resource]["bl_insert_or_ignore"],
                                self.dict_metadata[str_resource]["bl_schema"],
                            )
                            if bl_fetch == False:
                                list_ser = list()
                    except tuple(list_ignorable_exceptions) as e:
                        self.create_log.log_message(
                            self.logger,
                            "Iteration encountered an ignorable exception "
                            + f"{e.__class__.__name__}: {e}. Continuing...",
                            "warning"
                        )
                    except Exception as e:
                        self.create_log.log_message(
                            self.logger,
                            f"Iteration failed due to {e.__class__.__name__}: {e}",
                            "critical"
                        )
                        raise Exception(
                            f"Iteration failed due to {e.__class__.__name__}: {e}"
                        )
                    sleep(self.int_delay_seconds)
            else:
                raise Exception(
                    "Neither {{slug}} or {{chunk_slugs}} are found in the app "
                    + "parameter, please revisit it."
                )
        else:
            while True:
                try:
                    list_ser.extend(
                        self.trt_req(
                            self.dict_metadata[str_resource]["req_method"],
                            host_,
                            self.dict_metadata[str_resource]["dtypes"],
                            dict_headers,
                            payload,
                            StrHandler().fill_placeholders(app_, {"i": i}),
                            self.dict_metadata[str_resource]["bl_verify"],
                            self.dict_metadata[str_resource]["timeout"],
                            self.dict_metadata[str_resource].get("cookies", None),
                            self.dict_metadata[str_resource]["cols_from_case"],
                            self.dict_metadata[str_resource]["cols_to_case"],
                            self.dict_metadata[str_resource]["list_cols_drop_dupl"],
                            self.dict_metadata[str_resource]["str_fmt_dt"],
                            self.dict_metadata[str_resource]["type_error_action"],
                            self.dict_metadata[str_resource]["strategy_keep_when_dupl"],
                            self.dict_metadata[str_resource].get("regex_patterns", None),
                            self.dict_metadata[str_resource].get("df_read_params", None),
                            self.dict_metadata[str_resource].get("ignored_file_extensions", []),
                            self.dict_metadata[str_resource].get("fixed_width_layout", [{}]),
                            self.dict_metadata[str_resource].get("xml_keys", None),
                            self.dict_metadata[str_resource].get("fillna_strt", {}),
                            self.dict_metadata[str_resource]["web_driver"].get(
                                "xpath_el_wait_until_loaded", {}),
                            self.dict_metadata[str_resource].get("xpaths", [{}]),
                        ).to_dict(orient="records")
                    )
                    if self.cls_db is not None:
                        self.insert_table(
                            str_resource,
                            list_ser,
                            self.dict_metadata[str_resource]["bl_insert_or_ignore"],
                            self.dict_metadata[str_resource]["bl_schema"],
                        )
                        if bl_fetch == False:
                            list_ser = list()
                    i += 1
                except tuple(list_ignorable_exceptions) as e:
                    self.create_log.log_message(
                        self.logger,
                        "Iteration encountered an ignorable exception "
                        + f"{e.__class__.__name__}: {e}. Continuing...",
                        "warning"
                    )
                except Exception as e:
                    self.create_log.log_message(
                        self.logger,
                        f"Iteration failed due to {e.__class__.__name__}: {e}",
                        "critical"
                    )
                    raise Exception(
                        f"Iteration failed due to {e.__class__.__name__}: {e}"
                    )
                sleep(self.int_delay_seconds)
        if len(list_ser) > 0:
            return pd.DataFrame(list_ser)
        else:
            return None

    def source(
        self,
        str_resource: str,
        host: Optional[str] = None,
        bl_fetch: bool = False,
    ) -> Optional[pd.DataFrame]:
        """
        Get/load raw data - if a class database is passed, the data collected will be inserted into
            the respective table; please make sure this key is filled within the metadata
        Args:
            str_resource (str): resource name
            host (Optional[str]): host name
            bl_fetch (bool): False as default
        Returns:
            Optional[pd.DataFrame]
        """
        dict_instance_vars = self.get_instance_variables
        app_ = self.dict_metadata[str_resource].get("app", None)
        if (app_ is not None) and (
            StrHandler().match_string_like(app_, "*{{*}}*") == True
        ):
            app_ = StrHandler().fill_placeholders(app_, dict_instance_vars)
        if (app_ is not None) and (
            (StrHandler().match_string_like(app_, "*{{i}}*") == True)
            or (StrHandler().match_string_like(app_, "*{{slug}}*") == True)
            or (StrHandler().match_string_like(app_, "*{{chunk_slugs}}*") == True)
        ):
            func_get_data = self.iteratively_get_data
        else:
            func_get_data = self.non_iteratively_get_data
        return func_get_data(str_resource, host, bl_fetch)
