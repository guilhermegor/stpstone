"""Implementation of ingestion instance."""

from abc import abstractmethod
from contextlib import suppress
from datetime import date
from io import BytesIO, StringIO
from logging import Logger
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Optional, Union

import backoff
from bs4 import BeautifulSoup, Tag
import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import requests
from requests import Response, Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.abc.ingestion_abc import (
    ABCIngestionOperations,
    ContentParser,
    CoreIngestion,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.str import TypeCaseFrom, TypeCaseTo
from stpstone.utils.parsers.xml import XMLFiles


class ABCB3SearchByTradingSession(ABCIngestionOperations):
    """Ingestion concrete class."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        url: str = "FILL_ME"
    ) -> None:
        """Initialize the ingestion class.
        
        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.
        
        Returns
        -------
        None
        """
        super().__init__(cls_db=cls_db)
        CoreIngestion.__init__(self)
        ContentParser.__init__(self)

        self.logger = logger
        self.cls_db = cls_db
        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()
        self.cls_create_log = CreateLog()
        self.cls_dates_br = DatesBRAnbima()
        self.cls_xml_handler = XMLFiles()
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.url = url.format(self.date_ref.strftime("%y%m%d"))
    
    def run(
        self,
        dict_dtypes: dict[str, Union[str, int, float]],
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYY-MM-DD",
        cols_from_case: Optional[TypeCaseFrom] = None,
        cols_to_case: Optional[TypeCaseTo] = None,
        str_table_name: str = "<COUNTRY>_<SOURCE>_<TABLE_NAME>"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        dict_dtypes : dict[str, Union[str, int, float]]
            The dictionary of data types.
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "<COUNTRY>_<SOURCE>_<TABLE_NAME>"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        file, file_name = self.parse_raw_file(resp_req)
        df_ = self.transform_data(file=file, file_name=file_name)
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.date_ref,
            dict_dtypes=dict_dtypes, 
            str_fmt_dt=str_fmt_dt,
            url=self.url,
            cols_from_case=cols_from_case, 
            cols_to_case=cols_to_case,
        )
        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db, 
                str_table_name=str_table_name, 
                df_=df_, 
                bool_insert_or_ignore=bool_insert_or_ignore
            )
        else:
            return df_

    @backoff.on_exception(
        backoff.expo, 
        requests.exceptions.HTTPError, 
        max_time=60
    )
    def get_response(
        self, 
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0), 
        bool_verify: bool = True
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Return a list of response objects.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Verify the SSL certificate, by default True
        
        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            A list of response objects.
        """
        resp_req = requests.get(
            self.url, 
            timeout=timeout, 
            verify=bool_verify,
        )
        resp_req.raise_for_status()
        return resp_req
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> tuple[StringIO, str]:
        """Parse the raw file content.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        tuple[StringIO, str]
            The parsed content and the name of the file.

        Raises
        ------
        ValueError
            If no files found in the downloaded content
        """
        files_list = self.cls_dir_files_management.recursive_unzip_in_memory(
        BytesIO(resp_req.content))
        
        if files_list:
            file_content, filename = files_list[0]
            
            if isinstance(file_content, BytesIO) \
                and filename.lower().endswith(('.txt', '.csv', '.dat', '.xml')):
                try:
                    content_str = file_content.getvalue().decode('utf-8')
                    file_content = StringIO(content_str)
                except UnicodeDecodeError:
                    with suppress(UnicodeDecodeError):
                        content_str = file_content.getvalue().decode('latin-1')
                        file_content = StringIO(content_str)
            
            return file_content, filename
        else:
            raise ValueError("No files found in the downloaded content")
    
    def parse_raw_ex_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver], 
        prefix: str, 
        file_name: str,
    ) -> StringIO:
        """Parse the raw file content.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        prefix : str
            The prefix for the temporary directory.
        file_name : str
            The name of the file.
        
        Returns
        -------
        StringIO
            The parsed content.
        """
        # get the list of files from the zip
        files_list = self.cls_dir_files_management.recursive_unzip_in_memory(
            BytesIO(resp_req.content))
        
        # find the .ex_ file (should be the first and likely only file)
        ex_file_content = None
        ex_file_name = None
        for file_content, filename in files_list:
            if filename.lower().endswith('.ex_'):
                ex_file_content = file_content
                ex_file_name = filename
                break
        
        if ex_file_content is None:
            raise ValueError("No .ex_ file found in the downloaded ZIP")
        
        temp_dir = Path(tempfile.mkdtemp(prefix=prefix))
        
        try:
            self.cls_create_log.log_message(
                self.logger, 
                f"Created temporary directory: {temp_dir}", 
                "info"
            )
            
            exe_filename = f"{file_name}_{self.date_ref.strftime('%y%m%d')}.exe"
            exe_path = temp_dir / exe_filename
            
            # get the content from the file object
            if hasattr(ex_file_content, 'getvalue'):
                ex_content = ex_file_content.getvalue()
            else:
                # if it's already bytes
                ex_content = ex_file_content
                
            if isinstance(ex_content, str):
                ex_content = ex_content.encode('latin-1')
                
            with open(exe_path, 'wb') as f:
                f.write(ex_content)
                
            self.cls_create_log.log_message(
                self.logger, 
                f"Saved executable to: {exe_path}", 
                "info"
            )
            
            os.chmod(exe_path, 0o755)
            
            original_cwd = os.getcwd()
            os.chdir(temp_dir)
            
            try:
                self.cls_create_log.log_message(
                    self.logger, 
                    f"Executing Wine command: wine {exe_filename}", 
                    "info"
                )
                
                result = subprocess.run(
                    ['wine', exe_filename], 
                    capture_output=True, 
                    text=True, 
                    timeout=300,
                    check=False
                )
                
                if result.returncode != 0:
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"Wine execution failed with return code {result.returncode}: " \
                            + f"{result.stderr}", 
                        "warning"
                    )
                
                self.cls_create_log.log_message(
                    self.logger, 
                    f"Wine execution completed. Stdout: {result.stdout[:200]}...", 
                    "info"
                )
                
            finally:
                os.chdir(original_cwd)
            
            output_patterns = [
                "*.txt", "*.csv", "*.dat", "*.out", "*.xlsx", "*.xls",
                f"*{self.date_ref.strftime('%y%m%d')}*",
                "premiums*", "option*", "PE*"
            ]
            
            output_files = []
            for pattern in output_patterns:
                output_files.extend(temp_dir.glob(pattern))
            
            output_files = [f for f in output_files if f != exe_path]
            
            if not output_files:
                all_files = list(temp_dir.iterdir())
                self.cls_create_log.log_message(
                    self.logger, 
                    f"No output files found. All files in temp dir: {[f.name for f in all_files]}", 
                    "error"
                )
                raise RuntimeError(
                    f"No output file generated after Wine execution. "
                    f"Wine stderr: {result.stderr if 'result' in locals() else 'N/A'}"
                )
            
            if len(output_files) > 1:
                output_file = max(output_files, key=lambda f: f.stat().st_size)
                self.cls_create_log.log_message(
                    self.logger, 
                    f"Multiple output files found, using largest: {output_file.name}", 
                    "info"
                )
            else:
                output_file = output_files[0]
            
            self.cls_create_log.log_message(
                self.logger, 
                f"Reading output file: {output_file}", 
                "info"
            )
            
            try:
                with open(output_file, 'r', encoding='utf-8') as f:
                    content = f.read()
            except UnicodeDecodeError:
                with open(output_file, 'r', encoding='latin-1') as f:
                    content = f.read()
            
            return StringIO(content)
            
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, 
                f"Error in parse_raw_ex_file: {str(e)}", 
                "error"
            )
            raise
            
        finally:
            try:
                if temp_dir.exists():
                    shutil.rmtree(temp_dir)
                    self.cls_create_log.log_message(
                        self.logger, 
                        f"Cleaned up temporary directory: {temp_dir}", 
                        "info"
                    )
            except Exception as cleanup_error:
                self.cls_create_log.log_message(
                    self.logger, 
                    f"Failed to cleanup temp directory: {cleanup_error}", 
                    "warning"
                )
    
    @abstractmethod
    def transform_data(
        self, 
        file: StringIO, 
        file_name: str
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The parsed content.
        file_name : str
            The name of the file.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.

        Raises
        ------
        NotImplementedError
            If the method is not implemented in a subclass.
        """
        return pd.read_csv(file, sep=";")


class B3StandardizedInstrumentGroups(ABCB3SearchByTradingSession):
    """B3 Standardized Instrument Groups."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=AI{}.zip"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_standardized_instrument_groups"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "TIPO_REGISTRO": str,
                "ID_GRUPO_INSTRUMENTOS": str, 
                "ID_CAMARA": str, 
                "ID_INSTRUMENTO": str, 
                "ORIGEM_INSTRUMENTO": str, 
                "FILE_NAME": str,
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore,
            str_fmt_dt=str_fmt_dt,
            str_table_name=str_table_name
    )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        file_name : str
            The name of the file.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        df_ = pd.read_csv(file, sep=";", skiprows=1, names=[
            "TIPO_REGISTRO", 
            "ID_GRUPO_INSTRUMENTOS", 
            "ID_CAMARA", 
            "ID_INSTRUMENTO", 
            "ORIGEM_INSTRUMENTO",
        ])
        df_["FILE_NAME"] = file_name
        return df_


class B3IndexReport(ABCB3SearchByTradingSession):
    """B3 Index Report BVBG.087.01 IndexReport."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=IR{}.zip"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYY-MM-DD",
        cols_from_case: str = "pascal",
        cols_to_case: str = "upper_constant",
        str_table_name: str = "br_b3_index_report"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "TCKR_SYMB": str,
                "ID": str, 
                "PRTRY": str, 
                "MKT_IDR_CD": str, 
                "OPNG_PRIC": float, 
                "MIN_PRIC": float,
                "MAX_PRIC": float,
                "TRAD_AVRG_PRIC": float,
                "PRVS_DAY_CLSG_PRIC": float,
                "CLSG_PRIC": float,
                "INDX_VAL": float,
                "OSCN_VAL": float,
                "ASST_DESC": str,
                "STTLM_VAL": str,
                "STTLM_VAL_CCY": str,
                "RSNG_SHRS_NB": int,
                "FLNG_SHRS_NB": int,
                "STBL_SHRS_NB": int, 
                "FILE_NAME": str,
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            cols_from_case=cols_from_case, 
            cols_to_case=cols_to_case,
            str_table_name=str_table_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        file_name : str
            The name of the file.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        soup_xml = self.cls_xml_handler.memory_parser(file)
        soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag="IndxInf")
        list_ser: list[dict[str, Union[str, int, float]]] = []

        for soup_parent in soup_node:
            dict_ = {}
            for tag in [
                "TckrSymb",
                "Id",
                "Prtry",
                "MktIdrCd",
                "OpngPric",
                "MinPric",
                "MaxPric",
                "TradAvrgPric",
                "PrvsDayClsgPric",
                "ClsgPric",
                "IndxVal",
                "OscnVal",
                "AsstDesc",
                "SttlmVal",
                "RsngShrsNb",
                "FlngShrsNb",
                "StblShrsNb",
            ]:
                soup_content_tag = soup_parent.find(tag)
                dict_[tag] = soup_content_tag.getText() if soup_content_tag else None
            for tag, attribute in [
                ("SttlmVal", "Ccy"),
            ]:
                soup_content_tag = soup_parent.find(tag)
                dict_[tag + attribute] = soup_content_tag.get(attribute) if soup_content_tag \
                    else None
            list_ser.append(dict_)

        df_ = pd.DataFrame(list_ser)
        df_["FILE_NAME"] = file_name
        return df_


class B3PriceReport(ABCB3SearchByTradingSession):
    """B3 Index Report BVBG.086.01 PriceReport."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=PR{}.zip"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYY-MM-DD",
        cols_from_case: str = "pascal",
        cols_to_case: str = "upper_constant",
        str_table_name: str = "br_b3_standardized_instrument_groups"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "DT": "date",
                "TCKR_SYMB": str,
                "ID": str, 
                "PRTRY": str, 
                "MKT_IDR_CD": "category",
                "TRAD_QTY": int, 
                "OPN_INTRST": float, 
                "FIN_INSTRM_QTY": int, 
                "OSCN_PCTG": float, 
                "NTL_FIN_VOL": float, 
                "INTL_FIN_VOL": float, 
                "BEST_BID_PRIC": float, 
                "BEST_ASK_PRIC": float, 
                "FRST_PRIC": float, 
                "MIN_PRIC": float, 
                "MAX_PRIC": float, 
                "TRAD_AVRG_PRIC": float, 
                "LAST_PRIC": float, 
                "VARTN_PTS": float, 
                "MAX_TRAD_LMT": float, 
                "MIN_TRAD_LMT": float,
                "NTL_FIN_VOL_CCY": str,
                "INTL_FIN_VOL_CCY": str,
                "BEST_BID_PRIC_CCY": str,
                "BEST_ASK_PRIC_CCY": str,
                "FRST_PRIC_CCY": str,
                "MIN_PRIC_CCY": str,
                "MAX_PRIC_CCY": str,
                "TRAD_AVRG_PRIC_CCY": str,
                "LAST_PRIC_CCY": str,
                "NTL_RGLR_VOL_CCY": str,
                "INTL_RGLR_VOL_CCY": str,
                "FILE_NAME": str,
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            cols_from_case=cols_from_case, 
            cols_to_case=cols_to_case,
            str_table_name=str_table_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        file_name : str
            The name of the file.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        soup_xml = self.cls_xml_handler.memory_parser(file)
        soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag="PricRpt")
        list_ser: list[dict[str, Union[str, int, float]]] = []

        for soup_parent in soup_node:
            dict_ = {}
            for tag in [
                "Dt",
                "TckrSymb",
                "Id",
                "Prtry",
                "MktIdrCd",
                "TradQty",
                "OpnIntrst",
                "FinInstrmQty",
                "OscnPctg",
                "NtlFinVol",
                "IntlFinVol",
                "BestBidPric",
                "BestAskPric",
                "FrstPric",
                "MinPric",
                "MaxPric",
                "TradAvrgPric",
                "LastPric",
                "VartnPts",
                "MaxTradLmt",
                "MinTradLmt",
            ]:
                soup_content_tag = soup_parent.find(tag)
                dict_[tag] = soup_content_tag.getText() if soup_content_tag else None
            for tag, attribute in [
                ("NtlFinVol", "Ccy"),
                ("IntlFinVol", "Ccy"),
                ("BestBidPric", "Ccy"),
                ("BestAskPric", "Ccy"),
                ("FrstPric", "Ccy"),
                ("MinPric", "Ccy"),
                ("MaxPric", "Ccy"),
                ("TradAvrgPric", "Ccy"),
                ("LastPric", "Ccy"),
                ("NtlRglrVol", "Ccy"),
                ("IntlRglrVol", "Ccy"),
            ]:
                soup_content_tag = soup_parent.find(tag)
                dict_[tag + attribute] = soup_content_tag.get(attribute) if soup_content_tag \
                    else None
            list_ser.append(dict_)

        df_ = pd.DataFrame(list_ser)
        df_["FILE_NAME"] = file_name
        return df_


class B3InstrumentsFile(ABCB3SearchByTradingSession):
    """B3 Instruments File BVBG.028.02 InstrumentsFile with temporary caching."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=IN{}.zip"
        )
        
        self.temp_dir = Path(tempfile.mkdtemp(prefix="b3_instruments_"))
        self.cls_create_log.log_message(
            self.logger, 
            f"Created temporary directory: {self.temp_dir}", 
            "info"
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {},
        str_fmt_dt: str = "YYYY-MM-DD",
        cols_from_case: str = "pascal",
        cols_to_case: str = "upper_constant",
        str_table_name: str = "<COUNTRY>_<SOURCE>_<TABLE_NAME>"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process with caching.
        
        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        dict_dtypes : dict[str, Union[str, int, float]], optional
            Data types mapping, by default {}
        str_fmt_dt : str, optional
            Date format string, by default "YYYY-MM-DD"
        cols_from_case : str, optional
            Source column case format, by default "pascal"
        cols_to_case : str, optional
            Target column case format, by default "upper_constant"
        str_table_name : str, optional
            The name of the table, by default "<COUNTRY>_<SOURCE>_<TABLE_NAME>"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        # Use cached file or fetch from server
        file, file_name = self.get_cached_or_fetch(timeout=timeout, bool_verify=bool_verify)
        df_ = self.transform_data(file=file, file_name=file_name)
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.date_ref,
            dict_dtypes=dict_dtypes, 
            str_fmt_dt=str_fmt_dt,
            url=self.url,
            cols_from_case=cols_from_case, 
            cols_to_case=cols_to_case,
        )
        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db, 
                str_table_name=str_table_name, 
                df_=df_, 
                bool_insert_or_ignore=bool_insert_or_ignore
            )
        else:
            return df_

    def get_cached_or_fetch(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True
    ) -> StringIO:
        """Get XML content from cache or fetch from server.
        
        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Verify the SSL certificate, by default True
        
        Returns
        -------
        StringIO
            The XML content
        """
        try:
            cached_content = self._load_from_cache()
            return cached_content
        except ValueError:
            self.cls_create_log.log_message(
                self.logger, 
                f"Cache miss, fetching from server: {self.url}", 
                "warning"
            )
            
            resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
            return self.parse_raw_file(resp_req)

    def _load_from_cache(self) -> Optional[StringIO]:
        """Load XML content from cache if available.
        
        Returns
        -------
        Optional[StringIO]
            Cached XML content or None if not found

        Raises
        ------
        ValueError
            If cache file is not found
            If failing to load from cache
        """
        cache_path = self._get_cached_file_path()
        if cache_path.exists():
            self.cls_create_log.log_message(
                self.logger, 
                f"Loading XML from cache: {cache_path}", 
                "info"
            )
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                return StringIO(content)
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger, 
                    f"Failed to load from cache: {e}", 
                    "error"
                )
                raise ValueError(f"Failed to load from cache. Erro: {e}")
        raise ValueError("Cache file not found.")

    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> tuple[StringIO, str]:
        """Parse the raw file content and cache it.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        tuple[StringIO, str]
            The parsed content and file name.
        """
        file_io, file_name = self.cls_dir_files_management.recursive_unzip_in_memory(
            BytesIO(resp_req.content))[0]
        
        xml_content = file_io.getvalue()
        try:
            xml_content_str = xml_content.decode("utf-8")
        except UnicodeDecodeError:
            xml_content_str = xml_content.decode("latin-1")
        
        self._save_to_cache(xml_content_str)
        
        # reset file pointer
        file_io.seek(0)
        string_io = StringIO(xml_content_str)
        return string_io, file_name

    def _save_to_cache(self, xml_content: str) -> None:
        """Save XML content to cache.
        
        Parameters
        ----------
        xml_content : str
            XML content to save
        """
        cache_path = self._get_cached_file_path()
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(cache_path, 'w', encoding='utf-8') as f:
                f.write(xml_content)
                
            self.cls_create_log.log_message(
                self.logger, 
                f"Saved XML to cache: {cache_path}", 
                "info"
            )
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, f"Failed to save to cache: {e}", 
                "warning"
            )

    def _get_cached_file_path(self) -> Path:
        """Get the cached file path for the current date.
        
        Returns
        -------
        Path
            Path to the cached XML file
        """
        filename = f"instruments_{self.date_ref.strftime('%y%m%d')}.xml"
        return self.temp_dir / filename

    def transform_data(
        self, 
        file: StringIO, 
        file_name: str,
        tag_parent: str, 
        list_tags_children: list[str],
        list_tups_attributes: list[tuple[str, str]],
    ) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        soup_xml = self.cls_xml_handler.memory_parser(file)
        list_ser = self._get_node_info(
            soup_xml=soup_xml, 
            tag_parent=tag_parent, 
            list_tags_children=list_tags_children, 
            list_tups_attributes=list_tups_attributes
        )
        df_ = pd.DataFrame(list_ser)
        df_["FILE_NAME"] = file_name
        return df_
    
    def _get_node_info(
        self, 
        soup_xml: BeautifulSoup, 
        tag_parent: str, 
        list_tags_children: list[str], 
        list_tups_attributes: list[tuple[str, str]]
    ) -> str:
        """Get node information from BeautifulSoup XML.
        
        Parameters
        ----------
        soup_xml : BeautifulSoup
            Parsed XML document
        tag_parent : str
            Parent tag name
        list_tags_children : list[str]
            List of child tags
        list_tups_attributes : list[tuple[str, str]]
            List of tuples containing tag name and attribute name
        
        Returns
        -------
        list[dict[str, Union[str, int, float]]]
            List of dictionaries containing node information
        """
        soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag=tag_parent)
        list_ser: list[dict[str, Union[str, int, float]]] = []

        for soup_parent in soup_node:
            dict_ = {}
            for tag in list_tags_children:
                soup_content_tag = soup_parent.find(tag)
                dict_[tag] = soup_content_tag.getText() if soup_content_tag else None
            if list_tups_attributes is not None and len(list_tups_attributes) > 0:
                for tag, attribute in list_tups_attributes:
                    soup_content_tag = soup_parent.find(tag)
                    dict_[tag + attribute] = soup_content_tag.get(attribute) if soup_content_tag \
                        else None
            list_ser.append(dict_)

        return list_ser

    def __del__(self) -> None:
        """Cleanup on destruction."""
        with suppress(Exception):
            self.cleanup_cache()

    def cleanup_cache(self) -> None:
        """Clean up the temporary directory and all cached files.
        
        Raises
        ------
        ValueError
            If failing to cleanup cache
        """
        try:
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
                self.cls_create_log.log_message(
                    self.logger, 
                    f"Cleaned up temporary directory: {self.temp_dir}", 
                    "info"
                )
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger, f"Failed to cleanup cache: {e}", 
                "warning"
            )


class B3InstrumentsFileEqty(B3InstrumentsFile):
    """B3 Instruments File BVBG.028.02 InstrumentsFile (EqtyInf)."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db,
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_b3_instruments_file_eqty"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            dict_dtypes={
                "SCTY_CTGY": str,
                "ISIN": str,
                "DSTRBTN_ID": str,
                "CFICD": str,
                "SPCFCTN_CD": str,
                "CRPN_NM": str,
                "TCKR_SYMB": str,
                "PMT_TP": str,
                "ALLCN_RND_LOT": int,
                "PRIC_FCTR": float,
                "TRADG_START_DT": str,
                "TRADG_END_DT": str,
                "CORP_ACTN_START_DT": str,
                "EXDSTRBTN_NB": int,
                "CTDY_TRTMNT_TP": str,
                "TRADG_CCY": str,
                "MKT_CPTLSTN": str,
                "LAST_PRIC": float,
                "FRST_PRIC": float,
                "DAYS_TO_STTLM": int,
                "RGHTS_ISSE_PRIC": float,
                "ASST_SUB_TP": str,
                "AUCTN_TP": str, 
                "MKT_CPTLSTN_CCY": str,
                "LAST_PRIC_CCY": str,
                "FRST_PRIC_CCY": str,
                "RGHTS_ISSE_PRIC_CCY": str,
                "FILE_NAME": str,
            }, 
            str_fmt_dt="YYYY-MM-DD",
            cols_from_case="pascal", 
            cols_to_case="upper_constant",
            str_table_name=str_table_name,
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return super().transform_data(
            file=file, 
            file_name=file_name,
            tag_parent="EqtyInf", 
            list_tags_children=[
                "SctyCtgy", 
                "ISIN", 
                "DstrbtnId", 
                "CFICd",
                "SpcfctnCd", 
                "CrpnNm", 
                "TckrSymb", 
                "PmtTp", 
                "AllcnRndLot", 
                "PricFctr", 
                "TradgStartDt", 
                "TradgEndDt", 
                "CorpActnStartDt", 
                "EXDstrbtnNb",
                "CtdyTrtmntTp", 
                "TradgCcy", 
                "MktCptlstn", 
                "LastPric", 
                "FrstPric", 
                "DaysToSttlm", 
                "RghtsIssePric", 
                "AsstSubTp", 
                "AuctnTp"
            ], 
            list_tups_attributes=[
                ("MktCptlstn", "Ccy"),
                ("LastPric", "Ccy"),
                ("FrstPric", "Ccy"),
                ("RghtsIssePric", "Ccy"),
            ]
        )


class B3InstrumentsFileOptnOnEqts(B3InstrumentsFile):
    """B3 Instruments File BVBG.028.02 InstrumentsFile (OptnOnEqts)."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db,
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_b3_instruments_file_optn_on_eqts"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            timeout=timeout, 
            bool_verify=bool_verify,
            bool_insert_or_ignore=bool_insert_or_ignore, 
            dict_dtypes={
                "SCTY_CTGY": str,
                "ISIN": str,
                "DSTRBTN_ID": str,
                "CFICD": str,
                "TCKR_SYMB": str,
                "EXRC_PRIC": float,
                "OPTN_STYLE": str,
                "XPRTN_DT": "date",
                "OPTN_TP": str,
                "SRS_TP": str,
                "PRTCN_FLG": str,
                "PRM_UPFRNT_IND": str,
                "TRADG_START_DT": "date",
                "TRADG_END_DT": "date",
                "PMT_TP": str,
                "ALLCN_RND_LOT": int,
                "PRIC_FCTR": int,
                "TRADG_CCY": str,
                "DAYS_TO_STTLM": int,
                "DLVRY_TP": str,
                "AUTOMTC_EXRC_IND": str,
                "EXRC_PRIC_CCY": str,
                "FILE_NAME": str,
            }, 
            str_fmt_dt="YYYY-MM-DD",
            cols_from_case="pascal", 
            str_table_name=str_table_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return super().transform_data(
            file=file, 
            file_name=file_name,
            tag_parent="OptnOnEqtsInf", 
            list_tags_children=[
                "SctyCtgy", 
                "ISIN", 
                "DstrbtnId", 
                "CFICd",
                "TckrSymb",
                "ExrcPric",
                "OptnStyle",
                "XprtnDt",
                "OptnTp",
                "SrsTp",
                "PrtcnFlg",
                "PrmUpfrntInd",
                "TradgStartDt",
                "TradgEndDt",
                "PmtTp",
                "AllcnRndLot",
                "PricFctr", 
                "TradgCcy", 
                "DaysToSttlm",
                "DlvryTp",
                "AutomtcExrcInd",
            ], 
            list_tups_attributes=[
                ("ExrcPric", "Ccy"),
            ]
        )
    

class B3InstrumentsFileOptnOnSpotAndFutures(B3InstrumentsFile):
    """B3 Instruments File BVBG.028.02 InstrumentsFile (OptnOnSpotAndFutrs)."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db,
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_b3_instruments_file_optn_on_spot_and_futures"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            timeout=timeout, 
            bool_verify=bool_verify,
            bool_insert_or_ignore=bool_insert_or_ignore, 
            dict_dtypes={
                "ISIN": str,
                "TCKR_SYMB": str,
                "EXRC_PRIC": float,
                "EXRC_STYLE": str,
                "XPRTN_DT": "date",
                "XPRTN_CD": str,
                "OPTN_TP": str,
                "CTRCT_MLTPLR": float,
                "ASST_QTN_QTY": int,
                "PMT_TP": str,
                "ALLCN_RND_LOT": int,
                "CFICD": str,
                "PRM_UPFRNT_IND": str,
                "TRADG_START_DT": "date",
                "TRADG_END_DT": "date",
                "OPNG_POS_LMT_DT": "date",
                "TRADG_CCY": str,
                "WDRWL_DAYS": int,
                "WRKG_DAYS": int,
                "CLNR_DAYS": int,
                "EXRC_PRIC_CCY": str,
                "FILE_NAME": str,
            }, 
            str_fmt_dt="YYYY-MM-DD",
            cols_from_case="pascal", 
            str_table_name=str_table_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return super().transform_data(
            file=file, 
            file_name=file_name,
            tag_parent="OptnOnSpotAndFutrsInf", 
            list_tags_children=[
                "ISIN", 
                "TckrSymb", 
                "ExrcPric",
                "ExrcStyle", 
                "XprtnCd", 
                "OptnTp",
                "CtrctMltplr",
                "AsstQtnQty",
                "PmtTp",
                "AllcnRndLot",
                "CFICd",
                "PrmUpfrntInd",
                "TradgStartDt",
                "TradgEndDt",
                "OpngPosLmtDt",
                "TradgCcy",
                "WdrwlDays",
                "WrkgDays",
                "ClnrDays",
            ], 
            list_tups_attributes=[
                ("ExrcPric", "Ccy"),
            ]
        )
    

class B3InstrumentsFileExrcEqts(B3InstrumentsFile):
    """B3 Instruments File BVBG.028.02 InstrumentsFile (ExrcEqts)."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db,
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_b3_instruments_file_exrc_eqts"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            timeout=timeout, 
            bool_verify=bool_verify,
            bool_insert_or_ignore=bool_insert_or_ignore, 
            dict_dtypes={
                "SCTY_CTGY": str,
                "TCKR_SYMB": str,
                "ISIN": str,
                "TRADG_CCY": str,
                "TRADG_START_DT": "date",
                "TRADG_END_DT": "date",
                "DLVRY_TP": str,
                "FILE_NAME": str,
            }, 
            str_fmt_dt="YYYY-MM-DD",
            cols_from_case="pascal", 
            str_table_name=str_table_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return super().transform_data(
            file=file, 
            file_name=file_name,
            tag_parent="ExrcEqtsInf", 
            list_tags_children=[
                "SctyCtgy",
                "TckrSymb",
                "ISIN",
                "TradgCcy",
                "TradgStartDt",
                "TradgEndDt",
                "DlvryTp",
            ], 
            list_tups_attributes=[]
        )


class B3InstrumentsFileEqtyFwd(B3InstrumentsFile):
    """B3 Instruments File BVBG.028.02 InstrumentsFile (EqtyFwd)."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db,
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_b3_instruments_file_eqty_fwd"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            timeout=timeout, 
            bool_verify=bool_verify,
            bool_insert_or_ignore=bool_insert_or_ignore, 
            dict_dtypes={
                "SCTY_CTGY": str,
                "TCKR_SYMB": str,
                "ISIN": str,
                "DSTRBTN_ID": str,
                "CFICD": str,
                "PMT_TP": str,
                "ALLCN_RND_LOT": int,
                "PRIC_FCTR": int,
                "TRADG_START_DT": "date",
                "TRADG_END_DT": "date",
                "CTDY_TRTMNT_TP": str,
                "TRADG_CCY": str, 
                "FILE_NAME": str,
            }, 
            str_fmt_dt="YYYY-MM-DD",
            cols_from_case="pascal", 
            str_table_name=str_table_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return super().transform_data(
            file=file, 
            file_name=file_name,
            tag_parent="EqtyFwdInf", 
            list_tags_children=[
                "SctyCtgy",
                "TckrSymb",
                "ISIN",
                "DstrbtnId",
                "CFICd",
                "PmtTp",
                "AllcnRndLot",
                "PricFctr",
                "TradgStartDt",
                "TradgEndDt", 
                "CtdyTrtmntTp",
                "TradgCcy",
            ], 
            list_tups_attributes=[]
        )
    

class B3InstrumentsFileBTC(B3InstrumentsFile):
    """B3 Instruments File BVBG.028.02 InstrumentsFile (BTC)."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db,
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_b3_instruments_file_btc"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            timeout=timeout, 
            bool_verify=bool_verify,
            bool_insert_or_ignore=bool_insert_or_ignore, 
            dict_dtypes={
                "SCTY_CTGY": str,
                "TCKR_SYMB": str,
                "FNGB_IND": str,
                "PMT_TP": str, 
                "FILE_NAME": str,
            }, 
            str_fmt_dt="YYYY-MM-DD",
            cols_from_case="pascal", 
            str_table_name=str_table_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return super().transform_data(
            file=file, 
            file_name=file_name,
            tag_parent="BTCInf", 
            list_tags_children=[
                "SctyCtgy",
                "TckrSymb",
                "FngbINd", 
                "PmtTp",
            ], 
            list_tups_attributes=[]
        )


class B3InstrumentsFileFxdIncm(B3InstrumentsFile):
    """B3 Instruments File BVBG.028.02 InstrumentsFile (FxdIncm)."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db,
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_b3_instruments_file_fxd_incm"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            timeout=timeout, 
            bool_verify=bool_verify,
            bool_insert_or_ignore=bool_insert_or_ignore, 
            dict_dtypes={
                "SCTY_CTGY": str,
                "ISIN": str,
                "TCKR_SYMB": str,
                "TRADG_START_DT": "date",
                "TRADG_END_DT": "date",
                "TRADG_CCY": str,
                "PMT_TP": str,
                "DAYS_TO_STTLM": int,
                "ALLCN_RND_LOT": int,
                "PRIC_FCTR": int, 
                "FILE_NAME": str,
            }, 
            str_fmt_dt="YYYY-MM-DD",
            cols_from_case="pascal", 
            str_table_name=str_table_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return super().transform_data(
            file=file, 
            file_name=file_name,
            tag_parent="FxdIncmInf", 
            list_tags_children=[
                "SctyCtgy",
                "ISIN",
                "TckrSymb",
                "TradgStartDt",
                "TradgEndDt",
                "TradgCcy",
                "PmtTp",
                "DaysToSttlm", 
                "AllcnRndLot",
                "PricFctr",
            ], 
            list_tups_attributes=[]
        )


class B3InstrumentsFileADR(B3InstrumentsFile):
    """B3 Instruments File BVBG.028.02 InstrumentsFile (ADRInf)."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db,
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_b3_instruments_file_adr"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            timeout=timeout, 
            bool_verify=bool_verify,
            bool_insert_or_ignore=bool_insert_or_ignore, 
            dict_dtypes={
                "SCTY_CTGY": str,
                "TCKR_SYMB": str,
                "ISIN": str,
                "CFICD": str,
                "CUSIP": str,
                "PRGM_LVL": str,
                "PPSN": str,
                "TRADG_CCY": str,
                "FILE_NAME": str,
            }, 
            str_fmt_dt="YYYY-MM-DD",
            cols_from_case="pascal", 
            str_table_name=str_table_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return super().transform_data(
            file=file, 
            file_name=file_name,
            tag_parent="FxdIncmInf", 
            list_tags_children=[
                "SctyCtgy",
                "TckrSymb",
                "ISIN",
                "CFICd",
                "CUSIP",
                "PrgmLvl",
                "Ppsn",
                "TradgCcy",
            ], 
            list_tups_attributes=[
                ("Ppsn", "Ccy"),
            ]
        )


class B3InstrumentsFileIndicators(ABCB3SearchByTradingSession):
    """B3 Instruments File Indicators BVBG.029.02 InstrumentsFile Indicator."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=II{}.zip"
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "ACTVTY_IND": str,
            "FRQCY": str,
            "NET_POS_ID": str,
            "DT": "date",
            "ID": str, 
            "PRTRY": str, 
            "MKT_IDR_CD": str, 
            "INSTRM_NM": str, 
            "DESC": str, 
            "SGMT": str, 
            "MKT": str, 
            "ASST": str, 
            "SCTY_CTGY": str, 
            "TP_CD": str, 
            "ECNC_IND_DESC": str, 
            "BASE_CD": str, 
            "VAL_TP_CD": str, 
            "NTRY_REF_CD": str, 
            "DCML_PRCSN": str, 
            "MTRTY": str, 
            "FILE_NAME": str,
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        cols_from_case: str = "pascal",
        cols_to_case: str = "upper_constant",
        str_table_name: str = "br_b3_instruments_file_indicator"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           cols_from_case=cols_from_case, cols_to_case=cols_to_case,
                           str_table_name=str_table_name)

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        soup_xml = self.cls_xml_handler.memory_parser(file)
        soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag="Inds")
        list_ser: list[dict[str, Union[str, int, float]]] = []

        for soup_parent in soup_node:
            list_ser.append(self._instrument_indicator_node(soup_parent))

        df_ = pd.DataFrame(list_ser)
        df_["FILE_NAME"] = file_name
        return df_
    
    def _instrument_indicator_node(self, soup_parent: Tag) -> dict[str, Union[str, int, float]]:
        """Get node information from BeautifulSoup XML.
        
        Parameters
        ----------
        soup_parent : Tag
            Parsed XML document
        
        Returns
        -------
        dict[str, Union[str, int, float]]
            Dictionary containing node information
        """
        dict_: dict[str, Union[str, int, float]] = {}

        dict_["ActvtyInd"] = soup_parent.find("RptParams").find("ActvtyInd").text
        dict_["Frqcy"] = soup_parent.find("RptParams").find("Frqcy").text
        dict_["NetPosId"] = soup_parent.find("RptParams").find("NetPosId").text
        dict_["Dt"] = soup_parent.find("RptParams").find("RptDtAndTm").find("Dt").text
        dict_["Id"] = soup_parent.find("InstrmId").find("OthrId").find("Id").text
        dict_["Prtry"] = soup_parent.find("InstrmId").find("OthrId").find("Tp")\
            .find("Prtry").text
        dict_["MktIdrCd"] = soup_parent.find("InstrmId").find("PlcOfListg")\
            .find("MktIdrCd").text
        dict_["InstrmNm"] = soup_parent.find("InstrmInf").find("InstrmNm").text
        dict_["Desc"] = soup_parent.find("InstrmInf").find("Desc").text
        dict_["Sgmt"] = soup_parent.find("InstrmInf").find("Sgmt").text
        dict_["Mkt"] = soup_parent.find("InstrmInf").find("Mkt").text
        dict_["Asst"] = soup_parent.find("InstrmInf").find("Asst").text
        dict_["SctyCtgy"] = soup_parent.find("InstrmInf").find("SctyCtgy").text
        dict_["TpCd"] = soup_parent.find("InstrmDtls").find("EcncIndsInf").find("TpCd").text
        dict_["EcncIndDesc"] = soup_parent.find("InstrmDtls").find("EcncIndsInf")\
            .find("EcncIndDesc").text
        dict_["BaseCd"] = soup_parent.find("InstrmDtls").find("EcncIndsInf")\
            .find("BaseCd").text
        dict_["ValTpCd"] = soup_parent.find("InstrmDtls").find("EcncIndsInf")\
            .find("ValTpCd").text
        dict_["NtryRefCd"] = soup_parent.find("InstrmDtls").find("EcncIndsInf")\
            .find("NtryRefCd").text
        dict_["DcmlPrcsn"] = soup_parent.find("InstrmDtls").find("EcncIndsInf")\
            .find("DcmlPrcsn").text
        dict_["Mtrty"] = soup_parent.find("InstrmDtls").find("EcncIndsInf").find("Mtrty").text

        return dict_
    

class B3FeeDailyUnitCost(ABCB3SearchByTradingSession):
    """B3 Fee Daily Unit Cost BVBG.044.01 Fee Daily Unit Cost."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=DI{}.zip"
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYY-MM-DD",
        cols_from_case: str = "pascal",
        cols_to_case: str = "upper_constant",
        str_table_name: str = "br_b3_fee_daily_unit_cost"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "ACTVTY_IND": str,
                "FRQCY": str,
                "NET_POS_ID": str,
                "DT": "date",
                "ID": str, 
                "PRTRY": str, 
                "MKT_IDR_CD": str, 
                "INSTRM_NM": str, 
                "DESC": str, 
                "SGMT": str, 
                "MKT": str, 
                "ASST": str, 
                "SCTY_CTGY": str, 
                "TP_CD": str, 
                "ECNC_IND_DESC": str, 
                "BASE_CD": str, 
                "VAL_TP_CD": str, 
                "NTRY_REF_CD": str, 
                "DCML_PRCSN": str, 
                "MTRTY": str, 
                "FILE_NAME": str,
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            cols_from_case=cols_from_case, 
            cols_to_case=cols_to_case,
            str_table_name=str_table_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        soup_xml = self.cls_xml_handler.memory_parser(file)
        soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag="FeeInstrmInf")
        list_ser: list[dict[str, Union[str, int, float]]] = []

        for soup_parent in soup_node:
            list_ser.append(self._instrument_indicator_node(soup_parent))

        df_ = pd.DataFrame(list_ser)
        df_["FILE_NAME"] = file_name

        return df_
    
    def _instrument_indicator_node(self, soup_parent: Tag) -> dict[str, Union[str, int, float]]:
        """Get node information from BeautifulSoup XML.
        
        Parameters
        ----------
        soup_parent : Tag
            Parsed XML document
        
        Returns
        -------
        dict[str, Union[str, int, float]]
            Dictionary containing node information
        """
        dict_: dict[str, Union[str, int, float]] = {}

        dict_["Sgmt"] = soup_parent.find("FinInstrmAttrbts").find("Sgmt").text
        dict_["Mkt"] = soup_parent.find("FinInstrmAttrbts").find("Mkt").text
        dict_["Asst"] = soup_parent.find("FinInstrmAttrbts").find("Asst").text
        dict_["XprtnCd"] = soup_parent.find("FinInstrmAttrbts").find("XprtnCd").text
        dict_["DayTradInd"] = soup_parent.find("TradInf").find("DayTradInd").text
        dict_["TradTxTp"] = soup_parent.find("TradInf").find("TradTxTp").text
        dict_["AmtXchgFeeUnitCost"] = soup_parent.find("XchgFeeUnitCost").find("Amt").text
        dict_["AmtXchgFeeUnitCostCcy"] = soup_parent.find("XchgFeeUnitCost").find("Amt").get("Ccy")
        dict_["AmtRegnFeeUnitCost"] = soup_parent.find("RegnFeeUnitCost").find("Amt").text
        dict_["AmtRegnFeeUnitCostCcy"] = soup_parent.find("RegnFeeUnitCost").find("Amt").get("Ccy")

        return dict_


class B3FeeUnitCost(ABCB3SearchByTradingSession):
    """B3 Fee Daily Unit Cost BVBG.043.01 Fee Unit Cost."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=UN{}.zip"
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYY-MM-DD",
        cols_from_case: str = "pascal",
        cols_to_case: str = "upper_constant",
        str_table_name: str = "br_b3_instruments_fee_unit_cost"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "ACTVTY_IND": str,
                "FRQCY": str,
                "NET_POS_ID": str,
                "DT": "date",
                "ID": str, 
                "PRTRY": str, 
                "MKT_IDR_CD": str, 
                "INSTRM_NM": str, 
                "DESC": str, 
                "SGMT": str, 
                "MKT": str, 
                "ASST": str, 
                "SCTY_CTGY": str, 
                "TP_CD": str, 
                "ECNC_IND_DESC": str, 
                "BASE_CD": str, 
                "VAL_TP_CD": str, 
                "NTRY_REF_CD": str, 
                "DCML_PRCSN": str, 
                "MTRTY": str, 
                "FILE_NAME": str,
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            cols_from_case=cols_from_case, 
            cols_to_case=cols_to_case,
            str_table_name=str_table_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        soup_xml = self.cls_xml_handler.memory_parser(file)
        soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag="FeeInstrmInf")
        list_ser: list[dict[str, Union[str, int, float]]] = []

        for soup_parent in soup_node:
            list_ser.append(self._instrument_indicator_node(soup_parent))

        df_ = pd.DataFrame(list_ser)
        df_["FILE_NAME"] = file_name

        return df_
    
    def _instrument_indicator_node(self, soup_parent: Tag) -> dict[str, Union[str, int, float]]:
        """Get node information from BeautifulSoup XML.
        
        Parameters
        ----------
        soup_parent : Tag
            Parsed XML document
        
        Returns
        -------
        dict[str, Union[str, int, float]]
            Dictionary containing node information
        """
        dict_: dict[str, Union[str, int, float]] = {}

        dict_["Sgmt"] = soup_parent.find("FinInstrmAttrbts").find("Sgmt").text
        dict_["Mkt"] = soup_parent.find("FinInstrmAttrbts").find("Mkt").text
        dict_["Asst"] = soup_parent.find("FinInstrmAttrbts").find("Asst").text
        dict_["DayTradInd"] = soup_parent.find("TradInf").find("DayTradInd").text
        dict_["TradTxTp"] = soup_parent.find("TradInf").find("TradTxTp").text
        dict_["AmtXchgFeeUnitCost"] = soup_parent.find("XchgFeeUnitCost").find("Amt").text
        dict_["AmtXchgFeeUnitCostCcy"] = soup_parent.find("XchgFeeUnitCost").find("Amt").get("Ccy")
        dict_["AmtRegnFeeUnitCost"] = soup_parent.find("RegnFeeUnitCost").find("Amt").text
        dict_["AmtRegnFeeUnitCostCcy"] = soup_parent.find("RegnFeeUnitCost").find("Amt").get("Ccy")

        return dict_
    

class B3PrimitiveRiskFactors(ABCB3SearchByTradingSession):
    """B3 Primitive Risk Factors (PRFs)."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=FP{}.zip"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_primitive_risk_factors"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "TIPO_REGISTRO": str,
                "ID_FPR": str,
                "NOME_FPR": str,
                "FORMATO_VARIACAO": str,
                "ID_GRUPO_FPR": str,
                "ID_CAMARA_INDICADOR": str,
                "ID_INSTRUMENTO_INDICADOR": str,
                "ORIGEM_INSTRUMENTO_INDICADO": str,
                "BASE": str,
                "BASE_INTERPOLACAO": str,
                "CRITERIO_CAPITALIZACAO": str, 
                "FILE_NAME": str,
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            str_table_name=str_table_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        df_ = pd.read_csv(file, sep=";", skiprows=1, names=[
            "TIPO_REGISTRO",
            "ID_FPR",
            "NOME_FPR",
            "FORMATO_VARIACAO",
            "ID_GRUPO_FPR",
            "ID_CAMARA_INDICADOR",
            "ID_INSTRUMENTO_INDICADOR",
            "ORIGEM_INSTRUMENTO_INDICADO",
            "BASE",
            "BASE_INTERPOLACAO",
            "CRITERIO_CAPITALIZACAO",
        ])
        df_["FILE_NAME"] = file_name

        return df_
    

class B3RiskFormulas(ABCB3SearchByTradingSession):
    """B3 DailY Liquidity Limits."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=FR{}.zip"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "TIPO_REGISTRO": str,
            "ID_FORMULA": str,
            "NOME_FORMULA": str,
            "FILE_NAME": str,
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_primitive_risk_formulas"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           str_table_name=str_table_name)

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        df_ = pd.read_csv(file, sep=";", skiprows=1, names=[
            "TIPO_REGISTRO",
            "ID_FORMULA",
            "NOME_FORMULA",
        ])
        df_["FILE_NAME"] = file_name

        return df_
    

class B3VariableFees(ABCB3SearchByTradingSession):
    """B3 Variable Fees BVBG.024.01 Fee Variables."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=VA{}.zip"
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "FRQCY": str,
            "RPT_NB": str,
            "DT": str,
            "FR_DT_TM": str,
            "TO_DT_TM": str,
            "SGMT": str,
            "ASST": str,
            "REF_DT": str,
            "CONVS_IND_VAL": str,
            "ID": str,
            "PRTY": str,
            "MKT_IDR_CD": str, 
            "FILE_NAME": str
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        cols_from_case: str = "pascal",
        cols_to_case: str = "upper_constant",
        str_table_name: str = "br_b3_variable_fees"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           cols_from_case=cols_from_case, cols_to_case=cols_to_case,
                           str_table_name=str_table_name)

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        soup_xml = self.cls_xml_handler.memory_parser(file)
        soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag="FeeVarblInf")
        list_ser: list[dict[str, Union[str, int, float]]] = []

        for soup_parent in soup_node:
            list_ser.append(self._instrument_indicator_node(soup_parent))

        df_ = pd.DataFrame(list_ser)
        df_["FILE_NAME"] = file_name

        return df_
    
    def _instrument_indicator_node(self, soup_parent: Tag) -> dict[str, Union[str, int, float]]:
        """Get node information from BeautifulSoup XML.
        
        Parameters
        ----------
        soup_parent : Tag
            Parsed XML document
        
        Returns
        -------
        dict[str, Union[str, int, float]]
            Dictionary containing node information
        """
        dict_: dict[str, Union[str, int, float]] = {}

        dict_["Frqcy"] = soup_parent.find("RptParams").find("Frqcy").text
        dict_["RptNb"] = soup_parent.find("RptParams").find("RptNb").text
        dict_["Dt"] = soup_parent.find("RptParams").find("RptDtAndTm").find("Dt").text
        dict_["FrDtTm"] = soup_parent.find("VldtyPrd").find("FrDtTm").text
        dict_["ToDtTm"] = soup_parent.find("VldtyPrd").find("ToDtTm").text
        dict_["Sgmt"] = soup_parent.find("FeeInf").find("FinInstrmAttrbts").find("Sgmt").text
        dict_["Asst"] = soup_parent.find("FeeInf").find("FinInstrmAttrbts").find("Asst").text
        dict_["RefDt"] = soup_parent.find("FeeInf").find("OthrFeeQtnInf").find("RefDt").text
        dict_["ConvsIndVal"] = soup_parent.find("FeeInf").find("OthrFeeQtnInf")\
            .find("ConvsIndVal").text
        dict_["Id"] = soup_parent.find("FeeInf").find("ConvsInd").find("OthrId").find("Id").text
        dict_["Prtry"] = soup_parent.find("FeeInf").find("ConvsInd").find("OthrId").find("Tp")\
            .find("Prtry").text
        dict_["MktIdrCd"] = soup_parent.find("FeeInf").find("OthrFeeQtnInf")\
            .find("PlcOfListg").find("MktIdrCd").text

        return dict_
    

class B3DailyLiquidityLimits(ABCB3SearchByTradingSession):
    """B3 Daily Liquidity Limits."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=LD{}.zip"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "ID_CAMARA": str,
            "ORIGMEM_INSTRUMENTO": str,
            "ID_INSTRUMENTO": str,
            "SIMBOLO_INSTRUMENTO": str,
            "LIMITE_LIQUIDEZ": str, 
            "FILE_NAME": str,
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_daily_liquidity_limits"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           str_table_name=str_table_name)


    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        df_ = pd.read_csv(file, sep=";", skiprows=1, usecols=[0, 1, 2, 3, 4], names=[
            "ID_CAMARA",
            "ORIGMEM_INSTRUMENTO",
            "ID_INSTRUMENTO",
            "SIMBOLO_INSTRUMENTO",
            "LIMITE_LIQUIDEZ",
        ])
        df_["FILE_NAME"] = file_name

        return df_


class B3OtherDailyLiquidityLimits(ABCB3SearchByTradingSession):
    """B3 Other Daily Liquidity Limits."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=LA{}.zip"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "ID_CAMARA": str,
            "ORIGMEM_INSTRUMENTO": str,
            "ID_INSTRUMENTO": str,
            "SIMBOLO_INSTRUMENTO": str,
            "LIMITE_LIQUIDEZ": str, 
            "FILE_NAME": str,
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_other_daily_liquidity_limits"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           str_table_name=str_table_name)

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        df_ = pd.read_csv(file, sep=";", skiprows=1, usecols=[0, 1, 2, 3, 4], names=[
            "ID_CAMARA",
            "ORIGMEM_INSTRUMENTO",
            "ID_INSTRUMENTO",
            "SIMBOLO_INSTRUMENTO",
            "LIMITE_LIQUIDEZ",
        ])
        df_["FILE_NAME"] = file_name

        return df_
    

class B3TradableSecurityList(ABCB3SearchByTradingSession):
    """B3 Tradable Security List."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=SecurityList{}.zip"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "SYMBOL": str,
            "SECURITY_ID": int,
            "SECURITY_ID_SOURCE": "category",
            "SECURITY_EXCHANGE": "category",
            "NO_APPL_IDS": "category",
            "APPL_ID": str,
            "PUT_OR_CALL": "category",
            "PRODUCT": "category",
            "CFI_CODE": "category",
            "SECURITY_GROUP": str,
            "SECURITY_TYPE": "category",
            "SECURITY_SUB_TYPE": "category",
            "MATURITY_MONTH_YEAR": int,
            "MATURITY_DATE": "date",
            "ISSUE_DATE": "date",
            "COUNTRY_OF_ISSUE": "category",
            "STRIKE_PRICE": float,
            "STRIKE_CURRENCY": "category",
            "EXERCISE_STYLE": "category",
            "CONTRACT_MULTIPLIER": float,
            "SECURITY_DESC": str,
            "CONTRACT_SETTL_MONTH": int,
            "DATED_DATE": int,
            "SETTL_TYPE": "category",
            "SETTL_DATE": "date",
            "PRICE_DIVISOR": int,
            "MIN_PRICE_INCREMENT": float,
            "TICK_SIZE_DENOMINATOR": int,
            "MIN_ORDER_QTY": int,
            "MAX_ORDER_QTY": int,
            "MULTI_LEG_MODEL": int,
            "MULTI_LEG_PRICE_METHOD": int,
            "INDEX_PCT": str,
            "NO_INSTR_ATTRIB": int,
            "INSTR_ATTRIB_TYPE": str,
            "INSTR_ATTRIB_VALUE": str,
            "START_DATE": str,
            "END_DATE": str,
            "NO_UNDERLYINGS": int,
            "UNDERLYING_SYMBOL": str,
            "UNDERLYING_SECURITY_ID": str,
            "UNDERLYING_SECURITY_ID_SOURCE": str,
            "UNDERLYING_SECURITY_EXCHANGE": "category",
            "INDEX_THEORETICAL_QTY": str,
            "CURRENCY": "category",
            "SETTL_CURRENCY": "category",
            "SECURITY_STRATEGY_TYPE": "category",
            "ASSET": "category",
            "NO_SHARES_ISSUED": int,
            "SECURITY_VALIDITY_TIMESTAMP": str,
            "MARKET_SEGMENT_ID": int,
            "GOVERNANCE_INDICATOR": "category",
            "CORPORATE_ACTION_EVENT_ID": int,
            "SECURITY_MATCH_TYPE": int,
            "NO_LEGS": int,
            "LEG_SYMBOL": str,
            "LEG_SECURITY_ID": str,
            "LEG_SECURITY_ID_SOURCE": str,
            "LEG_SECURITY_TYPE": "category",
            "LEG_SECURITY_EXCHANGE": "category",
            "LEG_RATIO_QTY": "category",
            "LEG_SIDE": "category",
            "NO_TICK_RULES": str,
            "NO_LOT_TYPE_RULES": int,
            "LOT_TYPE": "category",
            "MIN_LOT_SIZE": int,
            "IMPLIED_MARKET_INDICATOR": "category",
            "MIN_CROSS_QTY": int,
            "ISIN_NUMBER": str,
            "CLEARING_HOUSE_ID": int,
            "FILE_NAME": str,
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_tradable_security_list"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           str_table_name=str_table_name)

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        df_ = pd.read_csv(file, sep=",", skiprows=0, names=[
            "SYMBOL",
            "SECURITY_ID",
            "SECURITY_ID_SOURCE",
            "SECURITY_EXCHANGE",
            "NO_APPL_IDS",
            "APPL_ID",
            "PUT_OR_CALL",
            "PRODUCT",
            "CFI_CODE",
            "SECURITY_GROUP",
            "SECURITY_TYPE",
            "SECURITY_SUB_TYPE",
            "MATURITY_MONTH_YEAR",
            "MATURITY_DATE",
            "ISSUE_DATE",
            "COUNTRY_OF_ISSUE",
            "STRIKE_PRICE",
            "STRIKE_CURRENCY",
            "EXERCISE_STYLE",
            "CONTRACT_MULTIPLIER",
            "SECURITY_DESC",
            "CONTRACT_SETTL_MONTH",
            "DATED_DATE",
            "SETTL_TYPE",
            "SETTL_DATE",
            "PRICE_DIVISOR",
            "MIN_PRICE_INCREMENT",
            "TICK_SIZE_DENOMINATOR",
            "MIN_ORDER_QTY",
            "MAX_ORDER_QTY",
            "MULTI_LEG_MODEL",
            "MULTI_LEG_PRICE_METHOD",
            "INDEX_PCT",
            "NO_INSTR_ATTRIB",
            "INSTR_ATTRIB_TYPE",
            "INSTR_ATTRIB_VALUE",
            "START_DATE",
            "END_DATE",
            "NO_UNDERLYINGS",
            "UNDERLYING_SYMBOL",
            "UNDERLYING_SECURITY_ID",
            "UNDERLYING_SECURITY_ID_SOURCE",
            "UNDERLYING_SECURITY_EXCHANGE",
            "INDEX_THEORETICAL_QTY",
            "CURRENCY",
            "SETTL_CURRENCY",
            "SECURITY_STRATEGY_TYPE",
            "ASSET",
            "NO_SHARES_ISSUED",
            "SECURITY_VALIDITY_TIMESTAMP",
            "MARKET_SEGMENT_ID",
            "GOVERNANCE_INDICATOR",
            "CORPORATE_ACTION_EVENT_ID",
            "SECURITY_MATCH_TYPE",
            "NO_LEGS",
            "LEG_SYMBOL",
            "LEG_SECURITY_ID",
            "LEG_SECURITY_ID_SOURCE",
            "LEG_SECURITY_TYPE",
            "LEG_SECURITY_EXCHANGE",
            "LEG_RATIO_QTY",
            "LEG_SIDE",
            "NO_TICK_RULES",
            "NO_LOT_TYPE_RULES",
            "LOT_TYPE",
            "MIN_LOT_SIZE",
            "IMPLIED_MARKET_INDICATOR",
            "MIN_CROSS_QTY",
            "ISIN_NUMBER",
            "CLEARING_HOUSE_ID",
        ])
        df_["FILE_NAME"] = file_name

        return df_
    
    
class B3MappingOTCInstrumentGroups(ABCB3SearchByTradingSession):
    """B3 Mapping OTC Instrument Groups."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=MO{}.zip"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_mapping_otc_instrument_groups"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "TIPO_REGISTRO": str,
                "ID_GRUPO_INSTRUMENTOS": int,
                "ID_CAMARA_ATIVO_OBJETO": "category",
                "ID_INSTRUMENTO_ATIVO_OBJETO": int,
                "ORIGEM_INSTRUMENTO_ATIVO_OBJETO": "category",
                "ID_FORMULA_RISCO": int,
                "ID_FPR": int,
                "ID_QUALIFICADOR": int,
                "DESCRICAO_QUALIFICADOR": "category",
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            str_table_name=str_table_name
        )

    def transform_data(self, file: StringIO) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        df_ = pd.read_csv(file, sep=";", skiprows=1, names=[
            "TIPO_REGISTRO",
            "ID_GRUPO_INSTRUMENTOS",
            "ID_CAMARA_ATIVO_OBJETO",
            "ID_INSTRUMENTO_ATIVO_OBJETO",
            "ORIGEM_INSTRUMENTO_ATIVO_OBJETO",
            "ID_FORMULA_RISCO",
            "ID_FPR",
            "ID_QUALIFICADOR",
            "DESCRICAO_QUALIFICADOR",
        ])
        df_["FILE_NAME"] = file.name

        return df_


class B3MappingStandardizedInstrumentGroups(ABCB3SearchByTradingSession):
    """B3 Mapping Standardized Instrument Groups."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=MP{}.zip"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_mapping_standardized_instrument_groups"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "TIPO_REGISTRO": str,
                "ID_GRUPO_INSTRUMENTOS": int,
                "ID_FORMULA_RISCO": int,
                "ID_FPR": int,
                "ID_QUALIFICADOR": int,
                "DESCRICAO_QUALIFICADOR": "category",
                "DATA_INICIAL_INTERVALO_VENCIMENTOS": "date",
                "DATA_FINAL_INTERVALO_VENCIMENTOS": "date",
                "INDICADOR_FPR_INDEPENDENTE": int,
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            str_table_name=str_table_name
        )

    def transform_data(self, file: StringIO) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        df_ = pd.read_csv(file, sep=";", skiprows=1, names=[
                "TIPO_REGISTRO",
                "ID_GRUPO_INSTRUMENTOS",
                "ID_FORMULA_RISCO",
                "ID_FPR",
                "ID_QUALIFICADOR",
                "DESCRICAO_QUALIFICADOR",
                "DATA_INICIAL_INTERVALO_VENCIMENTOS", 
                "DATA_FINAL_INTERVALO_VENCIMENTOS",
                "INDICADOR_FPR_INDEPENDENTE",
            ]
        )
        df_["FILE_NAME"] = file.name

        return df_


class B3MaximumTheoreticalMargin(ABCB3SearchByTradingSession):
    """B3 Maximum Theoretical Margin."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=MT{}.zip"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_maximum_theoretical_margin"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "INSTRUMENT_MTM": str,
                "HOLDING_DAY": str,
                "MTM_MAX_C_PHI1": str,
                "MTM_MAX_V_PHI1": str,
                "MIN_MARGIN_CREDIT_COLLATERAL_PHI1": str,
                "MIN_MARGIN_CREDIT_COLLATERAL_PHI2": str,
                "TICKER": str
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            str_table_name=str_table_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        df_ = pd.read_csv(file, sep=";", skiprows=1, usecols=[0, 1, 2, 3, 4, 5, 6], names=[
                "INSTRUMENT_MTM",
                "HOLDING_DAY",
                "MTM_MAX_C_PHI1",
                "MTM_MAX_V_PHI1",
                "MIN_MARGIN_CREDIT_COLLATERAL_PHI1",
                "MIN_MARGIN_CREDIT_COLLATERAL_PHI2",
                "TICKER",
            ]
        )
        df_["FILE_NAME"] = file_name

        return df_
    

class B3EquitiesOptionReferencePremiums(ABCB3SearchByTradingSession):
    """B3 Equities Option Reference Premiums."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=PE{}.ex_"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_equities_option_reference_premiums"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "TICKER_SYMBOL": str,
                "OPTION_TYPE": str,
                "OPTION_STYLE": str,
                "EXPIRY_DATE": int,
                "EXERCISE_PRICE": float,
                "REFERENCE_PREMIUM": float,
                "IMPLIED_VOLATILITY": float,
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            str_table_name=str_table_name
        )

    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver], 
        prefix: str = "b3_option_premiums_",
        file_name: str = "b3_equities_option_reference_premiums_"
    ) -> StringIO:
        """Parse the raw file content by executing Windows executable with Wine.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The parsed content.
            
        Raises
        ------
        RuntimeError
            If Wine execution fails or output file is not found
        ValueError
            If no .ex_ file found in ZIP or multiple files found
        """
        self.parse_raw_ex_file(
            resp_req=resp_req,
            prefix=prefix, 
            file_name=file_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        df_ = pd.read_csv(file, sep=";", skiprows=1, names=[
            "TICKER_SYMBOL",
            "OPTION_TYPE",
            "OPTION_STYLE",
            "EXPIRY_DATE",
            "EXERCISE_PRICE",
            "REFERENCE_PREMIUM",
            "IMPLIED_VOLATILITY",
        ])
        df_["FILE_NAME"] = file_name

        return df_
    

class B3FXMarketContractedTransactions(ABCB3SearchByTradingSession):
    """B3 FX Market Contracted Transactions.
    
    Traded rates, opening parameters and contracted transactions.
    """

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=CT{}.zip"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYYMMDD",
        str_table_name: str = "br_b3_fx_market_contracted_transactions"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "ID_TRANSACAO": str,
                "DATA_REFERENCIA": "date",
                "DATA_CONTRATACAO_TXS_PRATICADAS": "date",
                "DATA_LIQUIDACAO_TXS_PRATICADAS": "date",
                "TX_PRATICADA_MX": float,
                "TX_PRATICADA_MIN": float,
                "TX_PRATICADA_MEDIA": float,
                "DATA_CONTRATACAO_PARAMETROS_ABERTURA": "date",
                "DATA_LIQUIDACAO_PARAMETROS_ABERTURA": "date",
                "TAXA_ABERTURA": float,
                "PERCENTAUL_GARANTIDO": float,
                "DATA_CONTRATACAO_OPERACOES_CONTRATADAS": "date",
                "DATA_LIQUIDACAO_OPERACOES_CONTRATADAS": "date",
                "NUMERO_OPERACOES_CONTRATADAS": int,
                "VALOR_OPERACOES_CONTRATADAS_DOLAR": float,
                "VALOR_OPERACOES_CONTRATADAS_REAIS": float,
                "FILE_NAME": str,
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            str_table_name=str_table_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        colspecs = [
            (0, 8),    # ID_TRANSACAO
            (8, 16),   # DATA_REFERENCIA  
            (16, 24),  # DATA_CONTRATACAO_TXS_PRATICADAS
            (24, 32),  # DATA_LIQUIDACAO_TXS_PRATICADAS
            (32, 41),  # TX_PRATICADA_MX
            (41, 50),  # TX_PRATICADA_MIN
            (50, 59),  # TX_PRATICADA_MEDIA
            (59, 67),  # DATA_CONTRATACAO_PARAMETROS_ABERTURA
            (67, 75),  # DATA_LIQUIDACAO_PARAMETROS_ABERTURA
            (75, 84),  # TAXA_ABERTURA
            (84, 89),  # PERCENTAUL_GARANTIDO
            (89, 97),  # DATA_CONTRATACAO_OPERACOES_CONTRATADAS
            (97, 105), # DATA_LIQUIDACAO_OPERACOES_CONTRATADAS
            (105, 113), # NUMERO_OPERACOES_CONTRATADAS
            (113, 126), # VALOR_OPERACOES_CONTRATADAS_DOLAR
            (126, 139)  # VALOR_OPERACOES_CONTRATADAS_REAIS
        ]
        
        column_names = [
            "ID_TRANSACAO",
            "DATA_REFERENCIA",
            "DATA_CONTRATACAO_TXS_PRATICADAS",
            "DATA_LIQUIDACAO_TXS_PRATICADAS",
            "TX_PRATICADA_MX",
            "TX_PRATICADA_MIN",
            "TX_PRATICADA_MEDIA",
            "DATA_CONTRATACAO_PARAMETROS_ABERTURA",
            "DATA_LIQUIDACAO_PARAMETROS_ABERTURA",
            "TAXA_ABERTURA",
            "PERCENTAUL_GARANTIDO",
            "DATA_CONTRATACAO_OPERACOES_CONTRATADAS",
            "DATA_LIQUIDACAO_OPERACOES_CONTRATADAS",
            "NUMERO_OPERACOES_CONTRATADAS",
            "VALOR_OPERACOES_CONTRATADAS_DOLAR",
            "VALOR_OPERACOES_CONTRATADAS_REAIS"
        ]
        
        df_ = pd.read_fwf(file, colspecs=colspecs, names=column_names, header=None)
        df_["FILE_NAME"] = file_name
        
        return df_


class B3FXMarketVolumeSettled(ABCB3SearchByTradingSession):
    """B3 FX Market Volume Settled.
    
    Volume settled on a net basis.
    """

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=CV{}.zip"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYYMMDD",
        str_table_name: str = "br_b3_fx_market_volume_settled"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "ID_TRANSACAO": str,
                "DATA_REFERENCIA": "date",
                "DATA_LIQUIDACAO_VALORES_LIQUIDOS_COMPENSADO": "date",
                "VALOR_LIQUIDO_COMPENSADO_DOLAR": float,
                "VALOR_LIQUIDO_COMPENSADO_REAL": float,
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            str_table_name=str_table_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        colspecs = [
            (0, 8),    # ID_TRANSACAO
            (8, 16),   # DATA_REFERENCIA  
            (16, 24),  # DATA_LIQUIDACAO_VALORES_LIQUIDOS_COMPENSADO
            (24, 37),  # VALOR_LIQUIDO_COMPENSADO_DOLAR
            (37, 50),  # VALOR_LIQUIDO_COMPENSADO_REAL
        ]
        
        column_names = [
            "ID_TRANSACAO",
            "DATA_REFERENCIA",
            "DATA_LIQUIDACAO_VALORES_LIQUIDOS_COMPENSADO",
            "VALOR_LIQUIDO_COMPENSADO_DOLAR",
            "VALOR_LIQUIDO_COMPENSADO_REAL"
        ]
        
        df_ = pd.read_fwf(file, colspecs=colspecs, names=column_names, header=None)
        df_["FILE_NAME"] = file_name
        
        return df_


class B3DerivativesMarketMarginScenarios(ABCB3SearchByTradingSession):
    """B3 Derivatives Market Margin Scenarios.
    
    Margin scenarios for liquid assets.
    """

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=CN{}.zip"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_derivatives_market_margin_scenarios"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "TIPO_REGISTRO": str,
                "FATOR_PRIMITIVO_RISCO": str,
                "VERTICE_CODIGO_DISTRIBUICAO": str,
                "ID_CENARIO": str,
                "VALOR": float,
                "FATOR_CENARIO": str,
                "CHOQUE_CENARIO_POSITIVO": str,
                "CHOQUE_CENARIO_NEGATIVO": str,
                "TIPO_CHOQUE": str
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            str_table_name=str_table_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        df_ = pd.read_csv(file, sep=";", skiprows=1, names=[
            "TIPO_REGISTRO", 
            "FATOR_PRIMITIVO_RISCO", 
            "VERTICE_CODIGO_DISTRIBUICAO", 
            "ID_CENARIO", 
            "VALOR", 
            "FATOR_CENARIO", # fixed value with +00000000000000000
            "CHOQUE_CENARIO_POSITIVO", # fixed value with +00000000000000000
            "CHOQUE_CENARIO_NEGATIVO", # fixed value with +00000000000000000
            "TIPO_CHOQUE", # A (aditivo / sum) or M (multiplicativo / product)
        ])
        df_["FILE_NAME"] = file_name

        return df_
    

class B3DerivativesMarketConsiderationFactors(ABCB3SearchByTradingSession):
    """B3 Derivatives Market - Consideration Factors."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=GL{}.zip"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYYMMDD",
        str_table_name: str = "br_b3_derivatives_market_consideration_factors"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "DATA_BASE": str,
                "ATIVO_BASE_FPR": str,
                "DATA_FUTURA": str,
                "ATIVO_FPR": str,
                "TIPO_CENARIO_INDICADOR": str,
                "CODIGO_INTERNO": str,
                "VALOR_LIQUIDO_COMPENSADO_DOLAR": str,
                "VALOR_LIQUIDO_COMPENSADO_REAL": str
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            str_table_name=str_table_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        colspecs = [
            (0, 8),   # DATA_BASE
            (8, 16),  # ATIVO_BASE_FPR
            (16, 24), # DATA_FUTURA
            (24, 44), # ATIVO_FPR
            (44, 47), # TIPO_CENARIO_INDICADOR
            (47, 49), # CODIGO_INTERNO
            (49, 64),  # VALOR_LIQUIDO_COMPENSADO_DOLAR
            (64, 85),  # VALOR_LIQUIDO_COMPENSADO_REAL
        ]
        
        column_names = [
            "DATA_BASE",
            "ATIVO_BASE_FPR",
            "DATA_FUTURA",
            "ATIVO_FPR",
            "TIPO_CENARIO_INDICADOR",
            "CODIGO_INTERNO",
            "VALOR_LIQUIDO_COMPENSADO_DOLAR",
            "VALOR_LIQUIDO_COMPENSADO_REAL"
        ]
        
        df_ = pd.read_fwf(file, colspecs=colspecs, names=column_names, header=None)
        df_["FILE_NAME"] = file_name

        return df_


class B3DerivativesMarketEconomicAgriculturalIndicators(ABCB3SearchByTradingSession):
    """B3 Derivatives Market - Economic and Agricultural Indicators."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=ID{}.ex_"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_derivatives_market_economic_agricultural_indicators"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "ID_TRANSACAO": str,
                "COMPLEMENTO_TRANSACAO": str,
                "TIPO_REGISTRO": str,
                "DATA_BASE": str,
                "GRUPO_INDICADOR": str,
                "CODIGO_INDICADOR": str,
                "VALOR_INDICADOR": str,
                "NUMERO_DECIMAIS_VALOR": str,
                "FILLER": str,
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            str_table_name=str_table_name
        )

    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver], 
        prefix: str = "b3_derivatives_mkt_ec_ag_",
        file_name: str = "b3_derivatives_market_economic_agricultural_indicators"
    ) -> StringIO:
        """Parse the raw file content by executing Windows executable with Wine.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The parsed content.
            
        Raises
        ------
        RuntimeError
            If Wine execution fails or output file is not found
        ValueError
            If no .ex_ file found in ZIP or multiple files found
        """
        self.parse_raw_ex_file(
            resp_req=resp_req,
            prefix=prefix, 
            file_name=file_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        colspecs = [
            (0, 6),    # ID_TRANSACAO
            (6, 9),    # COMPLEMENTO_TRANSACAO
            (9, 11),   # TIPO_REGISTRO
            (11, 19),  # DATA_BASE
            (19, 21),  # GRUPO_INDICADOR
            (21, 46),  # CODIGO_INDICADOR
            (46, 71),  # VALOR_INDICADOR
            (71, 73),  # NUMERO_DECIMAIS_VALOR
            (73, 109), # FILLER
        ]
        
        column_names = [
            "ID_TRANSACAO",
            "COMPLEMENTO_TRANSACAO",
            "TIPO_REGISTRO",
            "DATA_BASE",
            "GRUPO_INDICADOR",
            "CODIGO_INDICADOR",
            "VALOR_INDICADOR",
            "NUMERO_DECIMAIS_VALOR",
            "FILLER",
        ]
        
        df_ = pd.read_fwf(file, colspecs=colspecs, names=column_names, header=None)
        df_["FILE_NAME"] = file_name

        return df_
    

class B3DerivativesMarketOTCMarketTrades(ABCB3SearchByTradingSession):
    """B3 Derivatives Market - OTC Market Trades."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=BE{}.ex_"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_derivatives_market_otc_market_trades"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "ID_TRANSACAO": str,
                "COMPLEMENTO_TRANSACAO": str,
                "TIPO_REGISTRO": str,
                "DATA_GERACAO_ARQUIVO": str,
                "TIPO_NEGOCIACAO": str,
                "CODIGO_MERCADORIA": str,
                "CODIGO_MERCADO": str,
                "DATA_BASE": str,
                "DATA_VENCIMENTO": str,
                "VOLUME_DIA_BRL": float,
                "VOLUME_DIA_USD": float,
                "QTD_CONTRATOS_ABERTO_APOS_LIQUIDACAO": float,
                "QTD_NEGOCIOS_EFETUADOS": float,
                "QTD_CONTRATOS_NEGOCIADOS": float,
                "QTD_CONTRATOS_ABERTOS_ANTES_LIQUIDACAO": float,
                "QTD_CONTRATOS_LIQUIDADOS": float,
                "QTD_CONTRATOS_ABERTO_FINAL": float,
                "TAXA_MEDIA_SWAP_PREMIO_MEIO_OPC_FLEX": float,
                "SINAL_TAXA_MEDIA_PREMIO_MEDIO": str,
                "VOLUME_ABERTO_BRL": float,
                "VOLUME_ABERTO_USD": float,
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            str_table_name=str_table_name
        )

    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver], 
        prefix: str = "b3_derivatives_mkt_otc_trades_",
        file_name: str = "b3_derivatives_market_otc_market_trades"
    ) -> StringIO:
        """Parse the raw file content by executing Windows executable with Wine.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The parsed content.
            
        Raises
        ------
        RuntimeError
            If Wine execution fails or output file is not found
        ValueError
            If no .ex_ file found in ZIP or multiple files found
        """
        self.parse_raw_ex_file(
            resp_req=resp_req,
            prefix=prefix, 
            file_name=file_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        colspecs = [
            (0, 6),    # ID_TRANSACAO
            (6, 9),    # COMPLEMENTO_TRANSACAO
            (9, 11),   # TIPO_REGISTRO
            (11, 19),  # DATA_GERACAO_ARQUIVO
            (19, 21),  # TIPO_NEGOCIACAO
            (21, 24), # CODIGO_MERCADORIA
            (24, 25), # CODIGO_MERCADO
            (25, 33), # DATA_BASE
            (33, 41), # DATA_VENCIMENTO
            (41, 54), # VOLUME_DIA_BRL
            (54, 67), # VOLUME_DIA_USD
            (67, 75), # QTD_CONTRATOS_ABERTO_APOS_LIQUIDACAO
            (75, 83), # QTD_NEGOCIOS_EFETUADOS
            (83, 91), # QTD_CONTRATOS_NEGOCIADOS
            (91, 99), # QTD_CONTRATOS_ABERTOS_ANTES_LIQUIDACAO
            (99, 107), # QTD_CONTRATOS_LIQUIDADOS
            (107, 115), # QTD_CONTRATOS_ABERTO_FINAL
            (115, 124), # TAXA_MEDIA_SWAP_PREMIO_MEIO_OPC_FLEX
            (124, 125), # SINAL_TAXA_MEDIA_PREMIO_MEDIO
            (125, 126), # TIPO_TAXA_MEDIA
            (126, 148), # PRECO_EXERCICIO_MEDIO_OPC_FLEX
            (148, 149), # SINAL_PRECO_MEDIO_EXERCICIO
            (149, 162), # VOLUME_ABERTO_BRL
            (162, 175), # VOLUME_ABERTO_USD
        ]
        
        column_names = [
            "ID_TRANSACAO",
            "COMPLEMENTO_TRANSACAO",
            "TIPO_REGISTRO",
            "DATA_GERACAO_ARQUIVO",
            "TIPO_NEGOCIACAO",
            "CODIGO_MERCADORIA",
            "CODIGO_MERCADO",
            "DATA_BASE",
            "DATA_VENCIMENTO",
            "VOLUME_DIA_BRL",
            "VOLUME_DIA_USD",
            "QTD_CONTRATOS_ABERTO_APOS_LIQUIDACAO",
            "QTD_NEGOCIOS_EFETUADOS",
            "QTD_CONTRATOS_NEGOCIADOS",
            "QTD_CONTRATOS_ABERTOS_ANTES_LIQUIDACAO",
            "QTD_CONTRATOS_LIQUIDADOS",
            "QTD_CONTRATOS_ABERTO_FINAL",
            "TAXA_MEDIA_SWAP_PREMIO_MEIO_OPC_FLEX",
            "SINAL_TAXA_MEDIA_PREMIO_MEDIO",
            "TIPO_TAXA_MEDIA",
            "PRECO_EXERCICIO_MEDIO_OPC_FLEX",
            "SINAL_PRECO_MEDIO_EXERCICIO",
            "VOLUME_ABERTO_BRL",
            "VOLUME_ABERTO_USD",
        ]
        
        df_ = pd.read_fwf(file, colspecs=colspecs, names=column_names, header=None)
        df_["FILE_NAME"] = file_name
        
        return df_
    

class B3DerivativesMarketCombinedPositions(ABCB3SearchByTradingSession):
    """B3 Derivatives Market Combined Positions."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=PT{}.ex_"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_derivatives_market_combined_positions"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "DATA_PREGAO": str,
                "TIPO_NEGOCIACAO": str,
                "CODIGO_MERCADORIA": str,
                "TIPO_MERCADO": str,
                "VENCIMENTO": str,
                "QTD_CONTRATOS_TRAVADOS": float,
                "QTD_CONTRATOS_BAIXADOS": float
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            str_table_name=str_table_name
        )

    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver], 
        prefix: str = "br_derivatives_market_combined_positions_",
        file_name: str = "b3_derivatives_market_combined_positions"
    ) -> StringIO:
        """Parse the raw file content by executing Windows executable with Wine.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The parsed content.
            
        Raises
        ------
        RuntimeError
            If Wine execution fails or output file is not found
        ValueError
            If no .ex_ file found in ZIP or multiple files found
        """
        self.parse_raw_ex_file(
            resp_req=resp_req,
            prefix=prefix, 
            file_name=file_name
        )

    def transform_data(self, file: StringIO) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        colspecs = [
            (0, 8), # DATA_PREGAO
            (8, 10), # TIPO_NEGOCIACAO
            (10, 13), # CODIGO_MERCADORIA
            (13, 14), # TIPO_MERCADO
            (14, 18), # VENCIMENTO
            (18, 29), # QTD_CONTRATOS_TRAVADOS
            (29, 40), # QTD_CONTRATOS_BAIXADOS
        ]
        
        column_names = [
            "DATA_PREGAO",
            "TIPO_NEGOCIACAO",
            "CODIGO_MERCADORIA",
            "TIPO_MERCADO",
            "VENCIMENTO",
            "QTD_CONTRATOS_TRAVADOS",
            "QTD_CONTRATOS_BAIXADOS",
        ]
        
        df_ = pd.read_fwf(file, colspecs=colspecs, names=column_names, header=None)
        df_["FILE_NAME"] = file.name
        
        return df_
    

class B3DerivativesMarketOptionReferencePremium(ABCB3SearchByTradingSession):
    """B3 Derivatives Market Option Reference Premium."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=RE{}.ex_"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYYMMDD",
        str_table_name: str = "br_b3_derivatives_market_option_reference_premiums"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "ID_TRANSACAO": str,
                "COMPLEMENTO_TRANSACAO": str,
                "TIPO_REGISTRO": str,
                "DATA_BASE": str,
                "CODIGO_MERCADORIA": str,
                "TIPO_MERCADO": str,
                "SERIE_OPCOES": str,
                "INDICADOR_TIPO_OPCAO": str,
                "TIPO_OPCAO": str,
                "SERIE_OPCOES": str,
                "INDICADOR_TIPO_OPCAO": str,
                "TIPO_OPCAO": str,
                "DATA_VENCIMENTO_CONTRATO": str,
                "PRECO_EXERCICIO_OPCOES": float,
                "PRECO_REFERENCIA_OPCOES": float,
                "NUMERO_CASAS_DECIMAIS": int
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            str_table_name=str_table_name
        )

    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver], 
        prefix: str = "br_derivatives_market_option_reference_premiums_",
        file_name: str = "b3_derivatives_market_option_reference_premiums"
    ) -> StringIO:
        """Parse the raw file content by executing Windows executable with Wine.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The parsed content.
            
        Raises
        ------
        RuntimeError
            If Wine execution fails or output file is not found
        ValueError
            If no .ex_ file found in ZIP or multiple files found
        """
        self.parse_raw_ex_file(
            resp_req=resp_req,
            prefix=prefix, 
            file_name=file_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        colspecs = [
            (0, 6), # ID_TRANSACAO
            (6, 9), # COMPLEMENTO_TRANSACAO
            (9, 11), # TIPO_REGISTRO
            (11, 19), # DATA_BASE
            (19, 22), # CODIGO_MERCADORIA
            (22, 23), # TIPO_MERCADO - 3 (Options on available) / 4 (Options on futures)
            (23, 27), # SERIE_OPCOES
            (27, 28), # INDICADOR_TIPO_OPCAO - C (Buy) / V (Sell)
            (28, 29), # TIPO_OPCAO - A (American) / E (European)
            (29, 37), # DATA_VENCIMENTO_CONTRATO
            (37, 52), # PRECO_EXERCICIO_OPCOES
            (52, 67), # PRECO_REFERENCIA_OPCOES
            (67, 68), # NUMERO_CASAS_DECIMAIS
        ]
        
        column_names = [
            "ID_TRANSACAO",
            "COMPLEMENTO_TRANSACAO",
            "TIPO_REGISTRO",
            "DATA_BASE",
            "CODIGO_MERCADORIA",
            "TIPO_MERCADO",
            "SERIE_OPCOES",
            "INDICADOR_TIPO_OPCAO",
            "TIPO_OPCAO",
            "SERIE_OPCOES",
            "INDICADOR_TIPO_OPCAO",
            "TIPO_OPCAO",
            "DATA_VENCIMENTO_CONTRATO",
            "PRECO_EXERCICIO_OPCOES",
            "PRECO_REFERENCIA_OPCOES",
            "NUMERO_CASAS_DECIMAIS"
        ]
        
        df_ = pd.read_fwf(file, colspecs=colspecs, names=column_names, header=None)
        df_["FILE_NAME"] = file_name

        return df_
    

class B3DerivatiesMarketListISINCPRs(ABCB3SearchByTradingSession):
    """B3 Derivatives Market List ISIN Numbers for CPRs."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=IC{}.ex_"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYYMMDD",
        str_table_name: str = "br_b3_derivatives_market_option_reference_premiums_isin"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "DATA_CADASTRO": str,
                "EMISSOR": str,
                "CNPJ": str,
                "DATA_EMISSAO": str,
                "VALOR_NOMINAL": float,
                "DATA_VENCIMENTO": str,
                "CODIGO_ISIN": str, 
                "FILE_NAME": str,
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            str_table_name=str_table_name
        )

    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver], 
        prefix: str = "br_derivatives_market_option_reference_premiums_",
        file_name: str = "b3_derivatives_market_option_reference_premiums"
    ) -> StringIO:
        """Parse the raw file content by executing Windows executable with Wine.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The parsed content.
            
        Raises
        ------
        RuntimeError
            If Wine execution fails or output file is not found
        ValueError
            If no .ex_ file found in ZIP or multiple files found
        """
        self.parse_raw_ex_file(
            resp_req=resp_req,
            prefix=prefix, 
            file_name=file_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        colspecs = [
            (0, 8), # DATA_CADASTRO
            (8, 12), # EMISSOR
            (12, 26), # CNPJ
            (26, 34), # DATA_EMISSAO
            (34, 54), # VALOR_NOMINAL
            (54, 62), # DATA_VENCIMENTO
            (62, 74), # CODIGO_ISIN
        ]
        
        column_names = [
            "DATA_CADASTRO",
            "EMISSOR",
            "CNPJ",
            "DATA_EMISSAO",
            "VALOR_NOMINAL",
            "DATA_VENCIMENTO",
            "CODIGO_ISIN",
        ]
        
        df_ = pd.read_fwf(file, colspecs=colspecs, names=column_names, header=None)
        df_["FILE_NAME"] = file_name

        return df_
    

class B3DerivativesMarketListISINDerivativesContracts(ABCB3SearchByTradingSession):
    """B3 Derivatives Market List ISIN Numbers for Deri1vatives Contracts."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=IV{}.ex_"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_fmt_dt: str = "YYYYMMDD",
        str_table_name: str = "br_b3_derivatives_market_option_reference_premiums_isin"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "DATA_CADASTRO": str,
                "CODIGO_MERCADORIA": str,
                "TIPO_MERCADO": str,
                "VENCIMENTO_SERIE_PRAZO": str,
                "CODIGO_ISIN": str, 
                "FILE_NAME": str,
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            str_table_name=str_table_name
        )

    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver], 
        prefix: str = "br_derivatives_market_option_reference_premiums_",
        file_name: str = "b3_derivatives_market_option_reference_premiums"
    ) -> StringIO:
        """Parse the raw file content by executing Windows executable with Wine.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The parsed content.
            
        Raises
        ------
        RuntimeError
            If Wine execution fails or output file is not found
        ValueError
            If no .ex_ file found in ZIP or multiple files found
        """
        self.parse_raw_ex_file(
            resp_req=resp_req,
            prefix=prefix, 
            file_name=file_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        colspecs = [
            (0, 8), # DATA_CADASTRO
            (8, 11), # CODIGO_MERCADORIA
            (11, 14), # TIPO_MERCADO
            (14, 18), # VENCIMENTO_SERIE_PRAZO
            (18, 30), # CODIGO_ISIN
        ]
        
        column_names = [
            "DATA_CADASTRO",
            "CODIGO_MERCADORIA",
            "TIPO_MERCADO",
            "VENCIMENTO_SERIE_PRAZO",
            "CODIGO_ISIN",
        ]
        
        df_ = pd.read_fwf(file, colspecs=colspecs, names=column_names, header=None)
        df_["FILE_NAME"] = file_name

        return df_
    

class B3DerivativesMarketListISINSwaps(ABCB3SearchByTradingSession):
    """B3 Derivatives Market List ISIN Numbers for Swaps."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=IS{}.ex_"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYYMMDD",
        str_table_name: str = "br_b3_derivatives_market_list_isin_swaps"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "DATA_CADASTRO": str,
                "CONTRATO": str,
                "NOME_CONTRATO": str,
                "CODIGO_ISIN": str, 
                "FILE_NAME": str,
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            str_table_name=str_table_name
        )

    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver], 
        prefix: str = "b3_derivatives_market_list_isin_swaps_",
        file_name: str = "b3_derivatives_market_list_isin_swaps"
    ) -> StringIO:
        """Parse the raw file content by executing Windows executable with Wine.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The parsed content.
            
        Raises
        ------
        RuntimeError
            If Wine execution fails or output file is not found
        ValueError
            If no .ex_ file found in ZIP or multiple files found
        """
        self.parse_raw_ex_file(
            resp_req=resp_req,
            prefix=prefix, 
            file_name=file_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        colspecs = [
            (0, 8), # DATA_CADASTRO
            (8, 13), # CONTRATO
            (13, 63), # NOME_CONTRATO
            (63, 75), # CODIGO_ISIN
        ]
        
        column_names = [
            "DATA_CADASTRO",
            "CONTRATO",
            "NOME_CONTRATO",
            "CODIGO_ISIN",
        ]
        
        df_ = pd.read_fwf(file, colspecs=colspecs, names=column_names, header=None)
        df_["FILE_NAME"] = file_name

        return df_
    

class B3DerivativesMarketDollarSwap(ABCB3SearchByTradingSession):
    """B3 Derivatives Market - IDxUS Dollar Swap - Mark-to-Market."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=MM{}.ex_"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_fmt_dt: str = "YYYYMMDD",
        str_table_name: str = "br_b3_derivatives_market_dollar_swap"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            dict_dtypes={
                "ID_TRANSACAO": str,
                "COMPLEMENTO_TRANSACAO": str,
                "TIPO_REGISTRO": str,
                "DATA_GERACAO_ARQUIVO": str,
                "REFERENCIA": str,
                "CODIGO_PRODUTO": str,
                "SERIE": str,
                "INDICADOR_AJUSTE_PERIODO": str,
                "DATA_VENCIMENTO": str,
                "PRAZO_DIAS_CORRIDOS": str,
                "SINAL_TAXA_MERCADO": str,
                "TAXA_MERCADO": str,
                "FATOR_DESCONTO": str,
                "VALOR_MERCADO": str,
                "FATOR_DESCONTO_TAXA_OPERACIONAL_BASICA": str
            },
            timeout=timeout, 
            bool_verify=bool_verify, 
            bool_insert_or_ignore=bool_insert_or_ignore, 
            str_fmt_dt=str_fmt_dt,
            str_table_name=str_table_name
        )

    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver], 
        prefix: str = "b3_derivatives_market_list_isin_swaps_",
        file_name: str = "b3_derivatives_market_list_isin_swaps"
    ) -> StringIO:
        """Parse the raw file content by executing Windows executable with Wine.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The parsed content.
            
        Raises
        ------
        RuntimeError
            If Wine execution fails or output file is not found
        ValueError
            If no .ex_ file found in ZIP or multiple files found
        """
        self.parse_raw_ex_file(
            resp_req=resp_req,
            prefix=prefix, 
            file_name=file_name
        )

    def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        colspecs = [
            (0, 6), # ID_TRANSACAO
            (6, 9), # COMPLEMENTO_TRANSACAO
            (9, 11), # TIPO_REGISTRO
            (11, 19), # DATA_GERACAO_ARQUIVO
            (19, 25), # REFERENCIA
            (25, 28), # CODIGO_PRODUTO
            (28, 32), # SERIE
            (32, 33), # INDICADOR_AJUSTE_PERIODO
            (33, 41), # DATA_VENCIMENTO
            (41, 46), # PRAZO_DIAS_CORRIDOS
            (46, 47), # SINAL_TAXA_MERCADO
            (47, 59), # TAXA_MERCADO
            (59, 75), # FATOR_DESCONTO
            (75, 97), # VALOR_MERCADO
            (97, 113), # FATOR_DESCONTO_TAXA_OPERACIONAL_BASICA
        ]
        
        column_names = [
            "ID_TRANSACAO",
            "COMPLEMENTO_TRANSACAO",
            "TIPO_REGISTRO",
            "DATA_GERACAO_ARQUIVO",
            "REFERENCIA",
            "CODIGO_PRODUTO",
            "SERIE",
            "INDICADOR_AJUSTE_PERIODO",
            "DATA_VENCIMENTO",
            "PRAZO_DIAS_CORRIDOS",
            "SINAL_TAXA_MERCADO",
            "TAXA_MERCADO",
            "FATOR_DESCONTO",
            "VALOR_MERCADO",
            "FATOR_DESCONTO_TAXA_OPERACIONAL_BASICA",
        ]
        
        df_ =  pd.read_fwf(file, colspecs=colspecs, names=column_names, header=None)
        df_["FILE_NAME"] = file_name

        return df_


class B3DerivativesMarketSwapMarketRates(ABCB3SearchByTradingSession):
    """B3 Derivatives Market - Swap Market Rates."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=TS{}.ex_"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "ID_TRANSACAO": str,
            "COMPLEMENTO_TRANSACAO": str,
            "TIPO_REGISTRO": str,
            "DATA_GERACAO_ARQUIVO": str,
            "CODIGO_CURVAS_A_TERMO": str,
            "CODIGO_TAXA": str,
            "DESCRICAO_TAXA": str,
            "NUMERO_DIAS_CORRIDOS_TAXA_JURO": str,
            "NUMERO_SAQUES_TAXA_JURO": str,
            "SINAL_TAXA_TEORICA": str,
            "TAXA_TEORICA": str,
            "CARACTERISTICA_VERTICE": str,
            "CODIGO_VERTICE": str
        },
        str_fmt_dt: str = "YYYYMMDD",
        str_table_name: str = "br_b3_derivatives_market_swap_market_rates"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           str_table_name=str_table_name)

    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver], 
        prefix: str = "b3_derivatives_market_swap_market_rates_",
        file_name: str = "b3_derivatives_market_swap_market_rates"
    ) -> StringIO:
        """Parse the raw file content by executing Windows executable with Wine.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The parsed content.
            
        Raises
        ------
        RuntimeError
            If Wine execution fails or output file is not found
        ValueError
            If no .ex_ file found in ZIP or multiple files found
        """
        self.parse_raw_ex_file(
            resp_req=resp_req,
            prefix=prefix, 
            file_name=file_name
        )

    def transform_data(self, file: StringIO) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        colspecs = [
            (0, 6), # ID_TRANSACAO
            (6, 9), # COMPLEMENTO_TRANSACAO
            (9, 11), # TIPO_REGISTRO
            (11, 19), # DATA_GERACAO_ARQUIVO
            (19, 21), # CODIGO_CURVAS_A_TERMO
            (21, 26), # CODIGO_TAXA
            (26, 41), # DESCRICAO_TAXA
            (41, 46), # NUMERO_DIAS_CORRIDOS_TAXA_JURO
            (46, 51), # NUMERO_SAQUES_TAXA_JURO
            (51, 52), # SINAL_TAXA_TEORICA
            (52, 66), # TAXA_TEORICA
            (66, 67), # CARACTERISTICA_VERTICE
            (67, 72), # CODIGO_VERTICE
        ]
        
        column_names = [
            "ID_TRANSACAO", 
            "COMPLEMENTO_TRANSACAO",
            "TIPO_REGISTRO",
            "DATA_GERACAO_ARQUIVO",
            "CODIGO_CURVAS_A_TERMO",
            "CODIGO_TAXA",
            "DESCRICAO_TAXA",
            "NUMERO_DIAS_CORRIDOS_TAXA_JURO",
            "NUMERO_SAQUES_TAXA_JURO",
            "SINAL_TAXA_TEORICA",
            "TAXA_TEORICA",
            "CARACTERISTICA_VERTICE",
            "CODIGO_VERTICE",
        ]
        
        return pd.read_fwf(file, colspecs=colspecs, names=column_names, header=None)
    

class B3SecuritiesMarketGovernmentSecuritiesPrices(ABCB3SearchByTradingSession):
    """B3 Securities Market - Government Securities Reference Prices."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=PU{}.ex_"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "TIPO_REGISTRO": str,
            "CODIGO_TITULO": str,
            "DESCRICAO_TITULO": str,
            "DATA_EMISSAO_TITULO": "data",
            "DATA_VENCIMENTO_TITULO": "data",
            "VALOR_MERCADO_PU": str,
            "VALOR_PU_CENARIO_ESTRESSE": str,
            "VALOR_MTM_PU_D_MAIS_1": str
        },
        str_fmt_dt: str = "YYYYMMDD",
        str_table_name: str = "br_b3_securities_market_government_securities_prices"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           str_table_name=str_table_name)

    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver], 
        prefix: str = "b3_securities_market_government_securities_prices_",
        file_name: str = "b3_securities_market_government_securities_prices"
    ) -> StringIO:
        """Parse the raw file content by executing Windows executable with Wine.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The parsed content.
            
        Raises
        ------
        RuntimeError
            If Wine execution fails or output file is not found
        ValueError
            If no .ex_ file found in ZIP or multiple files found
        """
        self.parse_raw_ex_file(
            resp_req=resp_req,
            prefix=prefix, 
            file_name=file_name
        )

    def transform_data(self, file: StringIO) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return pd.read_csv(file, sep=";", skiprows=1, names=[
                "TIPO_REGISTRO", 
                "CODIGO_TITULO", 
                "DESCRICAO_TITULO",
                "DATA_EMISSAO_TITULO", 
                "DATA_VENCIMENTO_TITULO",
                "VALOR_MERCADO_PU", 
                "VALOR_PU_CENARIO_ESTRESSE", 
                "VALOR_MTM_PU_D_MAIS_1"
            ]
        )
    

class B3InstrumentGroupParameters(ABCB3SearchByTradingSession):
    """B3 Instrument Group Parameters."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=PG{}.zip"
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "TIPO": str,
            "ID_GRUPO_INSTRUMENTOS": str,
            "MERCADO": str,
            "NOME_CLASSIFICACAO": str,
            "PRAZO_MINIMO_ENCERRAMENTO": str,
            "LIMITE_LIQUIDEZ": str,
            "PRAZO_SUBCARTEIRA": str,
            "PRAZO_LIQUIDACAO": str,
            "PRAZO_LIQUIDACAO_NO_VENCIMENTO": str,
            "DATA_INICIAL_INTERVALO_VENCIMENTOS": str,
            "DATA_FINAL_INTERVALO_VENCIMENTOS": str,
            "INDICADOR_MODELO_GENERICO": str
        },
        str_fmt_dt: str = "DD/MM/YYYY",
        str_table_name: str = "br_b3_instrument_group_parameters"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           str_table_name=str_table_name)

    def transform_data(self, file: StringIO) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return pd.read_csv(file, sep=";", skiprows=1, names=[
                "TIPO", 
                "ID_GRUPO_INSTRUMENTOS", 
                "MERCADO", 
                "NOME_CLASSIFICACAO",
                "PRAZO_MINIMO_ENCERRAMENTO",
                "LIMITE_LIQUIDEZ",
                "PRAZO_SUBCARTEIRA", 
                "PRAZO_LIQUIDACAO", 
                "PRAZO_LIQUIDACAO_NO_VENCIMENTO", 
                "DATA_INICIAL_INTERVALO_VENCIMENTOS", 
                "DATA_FINAL_INTERVALO_VENCIMENTOS",
                "INDICADOR_MODELO_GENERICO",
            ]
        )


class B3FixedIncome(ABCB3SearchByTradingSession):
    """B3 Fixed Income."""

    def __init__(
        self,
        date_ref: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref,
            logger=logger,
            cls_db=cls_db,
            url="https://www.b3.com.br/pesquisapregao/download?filelist=RF{}.ex_"
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "TICKER": str,
            "DATE": str,
            "PRAZO_DIAS_UTEIS": str,
            "PRAZO_DIAS_CORRIDOS": str,
            "PRECO_UNITARIO": str,
            "TAXA_COMPRA": str,
            "TAXA_VENDA": str,
            "TAXA_INDICATIVA": str
        },
        str_fmt_dt: str = "YYYYMMDD",
        str_table_name: str = "br_b3_fixed_income"
    ) -> Optional[pd.DataFrame]:
        return super().run(
            timeout=timeout, 
            bool_verify=bool_verify,
            bool_insert_or_ignore=bool_insert_or_ignore,
            dict_dtypes=dict_dtypes, 
            str_fmt_dt=str_fmt_dt,
            str_table_name=str_table_name
        )

    def parse_raw_file(
        self,
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> StringIO:
        """Parse the raw file content by executing Windows executable with Wine.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The parsed content.
            
        Raises
        ------
        RuntimeError
            If Wine execution fails or output file is not found
        ValueError
            If no .ex_ file found in ZIP or multiple files found
        """
        return self.parse_raw_ex_file(
            resp_req=resp_req,
            prefix="b3_fixed_income_",
            file_name="b3_fixed_income"
        )

    def transform_data(self, file: StringIO) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return pd.read_csv(file, sep=";", skiprows=1, names=[
            "TICKER",
            "DATE",
            "PRAZO_DIAS_UTEIS",
            "PRAZO_DIAS_CORRIDOS",
            "PRECO_UNITARIO",
            "TAXA_COMPRA",
            "TAXA_VENDA",
            "TAXA_INDICATIVA",
        ])


class B3EquitiesFeePublicInformation(ABCB3SearchByTradingSession):
    """B3 Equities Fee Public Information."""

    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        super().__init__(
            date_ref=date_ref, 
            logger=logger, 
            cls_db=cls_db, 
            url="https://www.b3.com.br/pesquisapregao/download?filelist=TX{}.zip"
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "FRQCY": str, 
            "RPT_NB": str,
            "DT": str,
            "FR_DT": str,
            "TO_DT": str,
            "FEE_GRP_MKT": str,
            "DAY_TRAD_IND": str,
            "TIER_INITL_VAL": str,
            "TIER_FNL_VAL": str,
            "FEE_TP": str,
            "FEE_COST_VAL": str,
            "FEE_COST_CCY": str,
            "CLNT_CTGY": str,
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        cols_from_case: str = "pascal",
        cols_to_case: str = "upper_constant",
        str_table_name: str = "br_b3_instruments_fee_public_information"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           cols_from_case=cols_from_case, cols_to_case=cols_to_case,
                           str_table_name=str_table_name)

    def transform_data(self, file: StringIO) -> pd.DataFrame:
        """Transform file content into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        soup_xml = self.cls_xml_handler.memory_parser(file)
        soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag="EqtsFeeInf")
        list_ser: list[dict[str, Union[str, int, float]]] = []

        for soup_parent in soup_node:
            # _instrument_indicator_node now returns a list of records
            records = self._instrument_indicator_node(soup_parent)
            list_ser.extend(records)

        return pd.DataFrame(list_ser)
    
    def _instrument_indicator_node(
        self, 
        soup_parent: Tag
    ) -> list[dict[str, Union[str, int, float]]]:
        """Get node information from BeautifulSoup XML.
        
        Parameters
        ----------
        soup_parent : Tag
            Parsed XML document
        
        Returns
        -------
        list[dict[str, Union[str, int, float]]]
            List of dictionaries containing node information for each fee record
        """
        list_records: list[dict[str, Union[str, int, float]]] = []
        
        # Extract common report information
        rpt_params = soup_parent.find("RptParams")
        validity_period = soup_parent.find("VldtyPrd")
        
        frqcy = rpt_params.find("Frqcy").text if rpt_params.find("Frqcy") else None
        rpt_nb = rpt_params.find("RptNb").text if rpt_params.find("RptNb") else None
        dt = rpt_params.find("RptDtAndTm").find("Dt").text if rpt_params.find("RptDtAndTm") \
            and rpt_params.find("RptDtAndTm").find("Dt") else None
        fr_dt = validity_period.find("FrDt").text if validity_period.find("FrDt") else None
        to_dt = validity_period.find("ToDt").text if validity_period.find("ToDt") else None
        
        # extract fee details for each instrument
        fee_details = soup_parent.find("EqtsFeeDtls")
        if fee_details:
            fee_instrm_infos = fee_details.find_all("FeeInstrmInf")
            
            for fee_instrm_info in fee_instrm_infos:
                fee_grp_mkt = fee_instrm_info.find("FeeGrpMkt").text \
                    if fee_instrm_info.find("FeeGrpMkt") else None
                
                # get all fee nodes within this instrument
                fees = fee_instrm_info.find_all("Fee")
                
                for fee in fees:
                    day_trad_ind = fee.find("DayTradInd").text if fee.find("DayTradInd") else None
                    
                    # get all TierAndCost nodes within this fee
                    tier_and_costs = fee.find_all("TierAndCost")
                    
                    for tier_and_cost in tier_and_costs:
                        dict_record: dict[str, Union[str, int, float]] = {}
                        
                        # common fields
                        dict_record["Frqcy"] = frqcy
                        dict_record["RptNb"] = rpt_nb
                        dict_record["Dt"] = dt
                        dict_record["FrDt"] = fr_dt
                        dict_record["ToDt"] = to_dt
                        dict_record["FeeGrpMkt"] = fee_grp_mkt
                        dict_record["DayTradInd"] = day_trad_ind
                        
                        # tier information (optional)
                        tier_initl_val = tier_and_cost.find("TierInitlVal")
                        tier_fnl_val = tier_and_cost.find("TierFnlVal")
                        dict_record["TierInitlVal"] = tier_initl_val.text if tier_initl_val \
                            else None
                        dict_record["TierFnlVal"] = tier_fnl_val.text if tier_fnl_val else None
                        
                        # fee type
                        fee_tp = tier_and_cost.find("FeeTp")
                        dict_record["FeeTp"] = fee_tp.text if fee_tp else None
                        
                        # cost information
                        cost_inf = tier_and_cost.find("CostInf")
                        if cost_inf:
                            fee_cost_val = cost_inf.find("FeeCostVal")
                            dict_record["FeeCostVal"] = fee_cost_val.text if fee_cost_val \
                                else None
                            dict_record["FeeCostCcy"] = fee_cost_val.get("Ccy") if fee_cost_val \
                                else None
                            
                            # client category (optional)
                            clnt_ctgy_dtls = cost_inf.find("ClntCtgyDtls")
                            if clnt_ctgy_dtls:
                                clnt_ctgy = clnt_ctgy_dtls.find("ClntCtgy")
                                dict_record["ClntCtgy"] = clnt_ctgy.text if clnt_ctgy else None
                            else:
                                dict_record["ClntCtgy"] = None
                        
                        list_records.append(dict_record)
        
        return list_records