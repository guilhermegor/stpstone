"""Implementation of ingestion instance."""

from abc import abstractmethod
from datetime import date
from io import BytesIO, StringIO
from logging import Logger
from pathlib import Path
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
    ) -> StringIO:
        """Parse the raw file content.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The parsed content.
        """
        return self.cls_dir_files_management.recursive_unzip_in_memory(
            BytesIO(resp_req.content))[0]
    
    @abstractmethod
    def transform_data(
        self, 
        file: StringIO
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The parsed content.
        
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
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "COL_1": str,
            "COL_2": float, 
            "COL_3": int, 
            "COL_4": "date"
        },
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
        file = self.parse_raw_file(resp_req)
        df_ = self.transform_data(file=file)
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
            "ID_CAMARA", 
            "ID_INSTRUMENTO", 
            "ORIGEM_INSTRUMENTO",
        ])
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "TIPO": str,
            "ID_GRUPO_INSTRUMENTOS": str, 
            "ID_CAMARA": str, 
            "ID_INSTRUMENTO": str, 
            "ORIGEM_INSTRUMENTO": str
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_standardized_instrument_groups"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           str_table_name=str_table_name)


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
                dict_[tag + attribute] = soup_content_tag.get(attribute) if soup_content_tag else None
            list_ser.append(dict_)

        return pd.DataFrame(list_ser)
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
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
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        cols_from_case: str = "pascal",
        cols_to_case: str = "upper_constant",
        str_table_name: str = "br_b3_standardized_instrument_groups"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           cols_from_case=cols_from_case, cols_to_case=cols_to_case,
                           str_table_name=str_table_name)


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
                dict_[tag + attribute] = soup_content_tag.get(attribute) if soup_content_tag else None
            list_ser.append(dict_)

        return pd.DataFrame(list_ser)
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
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
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        cols_from_case: str = "pascal",
        cols_to_case: str = "upper_constant",
        str_table_name: str = "br_b3_standardized_instrument_groups"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           cols_from_case=cols_from_case, cols_to_case=cols_to_case,
                           str_table_name=str_table_name)


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

    def _get_cached_file_path(self) -> Path:
        """Get the cached file path for the current date.
        
        Returns
        -------
        Path
            Path to the cached XML file
        """
        filename = f"instruments_{self.date_ref.strftime('%y%m%d')}.xml"
        return self.temp_dir / filename

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

    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> StringIO:
        """Parse the raw file content and cache it.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The parsed content.
        """
        file_io = self.cls_dir_files_management.recursive_unzip_in_memory(
            BytesIO(resp_req.content))[0]
        
        xml_content = file_io.getvalue()
        self._save_to_cache(xml_content)
        
        # reset file pointer
        file_io.seek(0)
        return file_io

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

    def cleanup_cache(self) -> None:
        """Clean up the temporary directory and all cached files.
        
        Raises
        ------
        ValueError
            If failing to cleanup cache
        """
        try:
            import shutil
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

    def __del__(self):
        """Cleanup on destruction."""
        try:
            self.cleanup_cache()
        except:
            # ignore errors during cleanup in destructor
            pass

    def transform_data(
        self, 
        file: StringIO, 
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
        return pd.DataFrame(list_ser)
    
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
        file = self.get_cached_or_fetch(timeout=timeout, bool_verify=bool_verify)
        df_ = self.transform_data(file=file)
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
        return super().transform_data(
            file=file, 
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
            }, 
            str_fmt_dt="YYYY-MM-DD",
            cols_from_case="pascal", 
            cols_to_case="upper_constant",
            str_table_name=str_table_name,
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
        return super().transform_data(
            file=file, 
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
            }, 
            str_fmt_dt="YYYY-MM-DD",
            cols_from_case="pascal", 
            str_table_name=str_table_name
        )
    

class B3InstrumentsFileOptnOnSpotAndFutrs(B3InstrumentsFile):
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
        return super().transform_data(
            file=file, 
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

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_b3_instruments_file_optn_on_spot_and_futrs"
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
            }, 
            str_fmt_dt="YYYY-MM-DD",
            cols_from_case="pascal", 
            str_table_name=str_table_name
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
        return super().transform_data(
            file=file, 
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
                "DLVRY_TP": str
            }, 
            str_fmt_dt="YYYY-MM-DD",
            cols_from_case="pascal", 
            str_table_name=str_table_name
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
        return super().transform_data(
            file=file, 
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
                "TRADG_CCY": str
            }, 
            str_fmt_dt="YYYY-MM-DD",
            cols_from_case="pascal", 
            str_table_name=str_table_name
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
        return super().transform_data(
            file=file, 
            tag_parent="BTCInf", 
            list_tags_children=[
                "SctyCtgy",
                "TckrSymb",
                "FngbINd", 
                "PmtTp",
            ], 
            list_tups_attributes=[]
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
                "PMT_TP": str
            }, 
            str_fmt_dt="YYYY-MM-DD",
            cols_from_case="pascal", 
            str_table_name=str_table_name
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
        return super().transform_data(
            file=file, 
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
                "PRIC_FCTR": int
            }, 
            str_fmt_dt="YYYY-MM-DD",
            cols_from_case="pascal", 
            str_table_name=str_table_name
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
        return super().transform_data(
            file=file, 
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
            }, 
            str_fmt_dt="YYYY-MM-DD",
            cols_from_case="pascal", 
            str_table_name=str_table_name
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
        soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag="Inds")
        list_ser: list[dict[str, Union[str, int, float]]] = []

        for soup_parent in soup_node:
            list_ser.append(self._instrument_indicator_node(soup_parent))

        return pd.DataFrame(list_ser)
    
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
            "MTRTY": str
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
        soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag="FeeInstrmInf")
        list_ser: list[dict[str, Union[str, int, float]]] = []

        for soup_parent in soup_node:
            list_ser.append(self._instrument_indicator_node(soup_parent))

        return pd.DataFrame(list_ser)
    
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
            "MTRTY": str
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        cols_from_case: str = "pascal",
        cols_to_case: str = "upper_constant",
        str_table_name: str = "br_b3_fee_daily_unit_cost"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           cols_from_case=cols_from_case, cols_to_case=cols_to_case,
                           str_table_name=str_table_name)


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
        soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag="FeeInstrmInf")
        list_ser: list[dict[str, Union[str, int, float]]] = []

        for soup_parent in soup_node:
            list_ser.append(self._instrument_indicator_node(soup_parent))

        return pd.DataFrame(list_ser)
    
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
            "MTRTY": str
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        cols_from_case: str = "pascal",
        cols_to_case: str = "upper_constant",
        str_table_name: str = "br_b3_instruments_fee_unit_cost"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           cols_from_case=cols_from_case, cols_to_case=cols_to_case,
                           str_table_name=str_table_name)
    

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
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "TIPO": str,
            "ID_FPR": str,
            "NOME_FPR": str,
            "FORMATO_VARIACAO": str,
            "ID_GRUPO_FPR": str,
            "ID_CAMARA_INDICADOR": str,
            "ID_INSTRUMENTO_INDICADOR": str,
            "ORIGEM_INSTRUMENTO_INDICADO": str,
            "BASE": str,
            "BASE_INTERPOLACAO": str,
            "CRITERIO_CAPITALIZACAO": str
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_primitive_risk_factors"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           str_table_name=str_table_name)
    

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
            "ID_FORMULA",
            "NOME_FORMULA",
        ])
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        dict_dtypes: dict[str, Union[str, int, float]] = {
            "TIPO": str,
            "ID_FORMULA": str,
            "NOME_FORMULA": str,
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_primitive_risk_formulas"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           str_table_name=str_table_name)
    

class B3VariableFees(ABCB3SearchByTradingSession):
    """B3 Variable Fees - BVBG.024.01 Fee Variables."""

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
        soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag="FeeVarblInf")
        list_ser: list[dict[str, Union[str, int, float]]] = []

        for soup_parent in soup_node:
            list_ser.append(self._instrument_indicator_node(soup_parent))

        return pd.DataFrame(list_ser)
    
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
            "MKT_IDR_CD": str
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
        return pd.read_csv(file, sep=";", skiprows=1, usecols=[0, 1, 2, 3, 4], names=[
            "ID_CAMARA",
            "ORIGMEM_INSTRUMENTO",
            "ID_INSTRUMENTO",
            "SIMBOLO_INSTRUMENTO",
            "LIMITE_LIQUIDEZ",
        ])
    
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
            "LIMITE_LIQUIDEZ": str
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_daily_liquidity_limits"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           str_table_name=str_table_name)


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
        return pd.read_csv(file, sep=";", skiprows=1, usecols=[0, 1, 2, 3, 4], names=[
            "ID_CAMARA",
            "ORIGMEM_INSTRUMENTO",
            "ID_INSTRUMENTO",
            "SIMBOLO_INSTRUMENTO",
            "LIMITE_LIQUIDEZ",
        ])
    
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
            "LIMITE_LIQUIDEZ": str
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_other_daily_liquidity_limits"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           str_table_name=str_table_name)
    

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
        return pd.read_csv(file, sep=",", skiprows=0, names=[
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
            "FILE_NAME"
        ])
    
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
            "STRIKE_PRICE": "float",
            "STRIKE_CURRENCY": "category",
            "EXERCISE_STYLE": "category",
            "CONTRACT_MULTIPLIER": "float",
            "SECURITY_DESC": str,
            "CONTRACT_SETTL_MONTH": int,
            "DATED_DATE": int,
            "SETTL_TYPE": "category",
            "SETTL_DATE": "date",
            "PRICE_DIVISOR": int,
            "MIN_PRICE_INCREMENT": "float",
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
            "FILE_NAME": str
        },
        str_fmt_dt: str = "YYYY-MM-DD",
        str_table_name: str = "br_b3_tradable_security_list"
    ) -> Optional[pd.DataFrame]:
        return super().run(timeout=timeout, bool_verify=bool_verify, 
                           bool_insert_or_ignore=bool_insert_or_ignore, 
                           dict_dtypes=dict_dtypes, str_fmt_dt=str_fmt_dt,
                           str_table_name=str_table_name)
    
    