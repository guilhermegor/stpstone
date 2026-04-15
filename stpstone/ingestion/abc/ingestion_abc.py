"""Abstract base class for ingestion operations.

This module defines an abstract base class (ABC) for ingestion operations,
providing a common interface for fetching and transforming data.
"""

from abc import abstractmethod
from datetime import date, datetime
from io import BytesIO, StringIO
from logging import Logger
import re
from typing import Optional, Union

import fitz
import pandas as pd
import pdfplumber
from playwright.sync_api import Page as PlaywrightPage
from requests import Response, Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.transformations.standardization.standardizer_df import (
    DFStandardization,
    TypeErrorActionAsTypeDataFrame,
    TypeFillnaStrategy,
    TypeKeepDuplicatedDataFrame,
)
from stpstone.transformations.validation.metaclass_type_checker import (
    ABCTypeCheckerMeta,
    TypeChecker,
)
from stpstone.utils.calendars.calendar_abc import TypeDateFormatInput
from stpstone.utils.loggs.db_logs import DBLogs
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.str import StrHandler, TypeCaseFrom, TypeCaseTo


class ABCIngestion(metaclass=ABCTypeCheckerMeta):
    """Abstract base class for ingestion operations."""
    
    @abstractmethod
    def __init__(self, cls_db: Optional[Session] = None) -> None:
        """Initialize the ABCIngestion class.
        
        Parameters
        ----------
        cls_db : Optional[Session]
            The database session, by default None.
        
        Returns
        -------
        None
        """
        self.cls_db = cls_db

    @abstractmethod
    def get_response(
        self, 
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0)
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Return a response object.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
            The timeout value, by default (12.0, 21.0)
        
        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.

        Raises
        ------
        NotImplementedError
            If the method is not implemented in a subclass.
        """
        raise NotImplementedError
    
    @abstractmethod
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

        Raises
        ------
        NotImplementedError
            If the method is not implemented in a subclass.
        """
        raise NotImplementedError

    @abstractmethod
    def transform_data(
        self, 
        file: StringIO
    ) -> pd.DataFrame:
        """Transform a response object into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The file content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.

        Raises
        ------
        NotImplementedError
            If the method is not implemented in a subclass.
        """
        raise NotImplementedError
    
    @abstractmethod
    def run(
        self, 
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0), 
        bool_verify: bool = True, 
        bool_insert_or_ignore: bool = True, 
        str_table_name: str = "<COUNTRY>_<SOURCE>_<TABLE_NAME>"
    ) -> None:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
            The timeout, by default (12.0, 21.0)
        bool_verify : bool
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool
            Whether to insert or ignore the data, by default True
        str_table_name : str
            The name of the table, by default "<COUNTRY>_<SOURCE>_<TABLE_NAME>"

        Returns
        -------
        None

        Raises
        ------
        NotImplementedError
            If the method is not implemented in a subclass.
        """
        raise NotImplementedError


class CoreIngestion(metaclass=TypeChecker):
    """Abstract base class for ingestion operations."""

    def __init__(self) -> None:
        """Initialize the CoreIngestion class.
        
        Returns
        -------
        None
        """
        self.cls_db_logs = DBLogs()
    
    def standardize_dataframe(
        self, 
        df_: pd.DataFrame,  
        date_ref: date,
        dict_dtypes: dict[str, Union[str, int, float, date, datetime]], 
        cols_from_case: Optional[TypeCaseFrom] = None,
        cols_to_case: Optional[TypeCaseTo] = None, 
        list_cols_drop_dupl: Optional[list[str]] = None, 
        dict_fillna_strt: TypeFillnaStrategy = None,
        str_fmt_dt: TypeDateFormatInput = "YYYY-MM-DD",
        type_error_action: TypeErrorActionAsTypeDataFrame = "raise",
        strategy_keep_when_dupl: TypeKeepDuplicatedDataFrame = "first",
        str_data_fillna: str = "-99999",
        str_dt_fillna: Optional[str] = None,
        logger: Optional[Logger] = None,
        url: Optional[str] = None,
        bool_format_log_as_str: bool = True,
    ) -> pd.DataFrame:
        """Standardize the DataFrame.
        
        Parameters
        ----------
        df_ : pd.DataFrame
            The DataFrame to standardize.
        url : Optional[str]
            The URL of the file.
        date_ref : date
            The date of reference.
        dict_dtypes : dict[str, Union[str, int, float, date, datetime]]
            The dictionary of data types.
        bool_format_log_as_str : bool
            Whether to format the log as a string, by default True.
        cols_from_case : Optional[TypeCaseFrom]
            Case conversion for column names, by default None.
        cols_to_case : Optional[TypeCaseTo]
            Case conversion for column names, by default None.
        list_cols_drop_dupl : Optional[list[str]]
            List of columns to drop duplicates, by default None.
        dict_fillna_strt : TypeFillnaStrategy
            Dictionary of fillna strategies, by default None.
        str_fmt_dt : TypeDateFormatInput
            Format for date columns, by default "YYYY-MM-DD".
        type_error_action : TypeErrorActionAsTypeDataFrame
            Action to take when a type error occurs, by default "raise".
        strategy_keep_when_dupl : TypeKeepDuplicatedDataFrame
            Strategy to keep when duplicates occur, by default "first".
        str_data_fillna : str
            Value to fill missing data, by default "-99999".
        str_dt_fillna : Optional[str]
            Value to fill missing dates, by default None.
        logger : Optional[Logger]
            Logger object, by default None.

        Returns
        -------
        pd.DataFrame
            The standardized DataFrame.
        """
        cls_df_standardization = DFStandardization(
            dict_dtypes=dict_dtypes,
            cols_from_case=cols_from_case,
            cols_to_case=cols_to_case,
            list_cols_drop_dupl=list_cols_drop_dupl,
            dict_fillna_strt=dict_fillna_strt,
            str_fmt_dt=str_fmt_dt,
            type_error_action=type_error_action,
            strategy_keep_when_dupl=strategy_keep_when_dupl,
            str_data_fillna=str_data_fillna,
            str_dt_fillna=str_dt_fillna,
            logger=logger,
        )
        df_ = cls_df_standardization.pipeline(df_)
        df_ = self.cls_db_logs.audit_log(
            df_, 
            url, 
            date_ref, 
            bool_format_log_as_str
        )
        return df_

    def insert_table_db(
        self, 
        cls_db: Session, 
        str_table_name: str, 
        df_: pd.DataFrame, 
        bool_insert_or_ignore: bool = False
    ) -> None:
        """Insert a DataFrame into a database table.
        
        Parameters
        ----------
        cls_db : Session
            The database session.
        str_table_name : str
            The name of the table.
        df_ : pd.DataFrame
            The DataFrame to insert.
        bool_insert_or_ignore : bool
            Whether to use INSERT OR IGNORE or INSERT WITHOUT IGNORING CONFLICTS (default: False)

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If str_table_name is empty
        """
        if not str_table_name:
            raise ValueError("str_table_name cannot be empty")
        list_ser = df_.to_dict(orient="records")
        cls_db.insert(
            list_ser,
            str_table_name=str_table_name,
            bool_insert_or_ignore=bool_insert_or_ignore,
        )


class ContentAggregator(metaclass=TypeChecker):
    """Content aggregator mixin class."""

    def __init__(self) -> None:
        """Initialize the ContentAggregator class.
        
        Returns
        -------
        None
        """
        self.cls_handling_dicts = HandlingDicts()
        self.cls_str_handler = StrHandler()

    def paginate_text_blocks(
        self, 
        stream_file: fitz.Document, 
        int_pages_join: int
    ) -> list[str]:
        """Paginate text blocks.
        
        Parameters
        ----------
        stream_file : fitz.Document
            The document to paginate.
        int_pages_join : int
            The number of pages to join.
        
        Returns
        -------
        list[str]
            A list of paginated text blocks.

        Raises
        ------
        ValueError
            If int_pages_join is not a positive integer
        """
        if not isinstance(int_pages_join, int) or int_pages_join <= 0:
            raise ValueError("int_pages_join must be a positive integer")
        str_ = ""
        list_blocks_pages: list[str] = []

        for i in range(0, len(stream_file)):
            str_ += "\n" + stream_file[i].get_text("text")
            if i % int_pages_join == 0 or i == len(stream_file) - 1:
                str_ = self.cls_str_handler.remove_diacritics_nfkd(
                    str_, bool_lower_case=True)
                list_blocks_pages.append(str_)
                str_ = ""

        if not list_blocks_pages:
            list_blocks_pages.append("")

        return list_blocks_pages


class ContentParser(ContentAggregator):
    """Content parser class."""

    def get_file(self, resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]) -> StringIO:
        """Get the file from a response object.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
            The file.
        """
        return StringIO(resp_req.text)
    
    def pdf_docx_tables_response(self, bytes_file: BytesIO) -> pd.DataFrame:
        """Parse a PDF/DOCX file using tabula.
        
        Parameters
        ----------
        bytes_file : BytesIO
            The file to parse.
        
        Returns
        -------
        pd.DataFrame
            The parsed data.
        """
        list_ser = list()
        with pdfplumber.open(bytes_file) as doc:
            for page in doc.pages:
                list_ = page.extract_tables()
                list_ser.extend(
                    self.cls_handling_dicts.pair_keys_with_values(list_[0][0], list_[0][1:]))
        return pd.DataFrame(list_ser)
    
    def pdf_docx_regex(
        self, 
        bytes_file: BytesIO, 
        str_file_extension: str, 
        int_pages_join: int, 
        dict_regex_patterns: dict[str, dict[str, str]]
    ) -> pd.DataFrame:
        """Parse a PDF/DOCX file using regular expressions.
        
        Parameters
        ----------
        bytes_file : BytesIO
            The file to parse.
        str_file_extension : str
            The file extension.
        int_pages_join : int
            The number of pages to join.
        dict_regex_patterns : dict[str, dict[str, str]]
            The regular expressions to match.
        
        Returns
        -------
        pd.DataFrame
            The parsed data.
        """
        list_blocks_pages = self.paginate_text_blocks(
            stream_file=fitz.open(
                stream=bytes_file,
                filetype=str_file_extension,
            ), 
            int_pages_join=int_pages_join
        )
        list_matches = self._regex_patterns_match(
            list_blocks_pages=list_blocks_pages,
            dict_regex_patterns=dict_regex_patterns
        )

        df_ = pd.DataFrame(list_matches)
        df_ = df_\
            .drop_duplicates()\
            .sort_values(by=["EVENT", "MATCH_PATTERN"], ascending=[True, True])\
            .drop_duplicates(subset=["EVENT"], keep="first")

        return pd.DataFrame(list_matches)

    def _regex_patterns_match(
        self,
        list_blocks_pages: list[str],
        dict_regex_patterns: dict[str, dict[str, str]], 
    ) -> list[dict[str, str]]:
        """Match regular expressions.
        
        Parameters
        ----------
        list_blocks_pages : list[str]
            The text blocks to match.
        dict_regex_patterns : dict[str, dict[str, str]]
            The regular expressions to match.
        
        Returns
        -------
        list[dict[str, str]]
            The matches.
        """
        dict_count_matches: dict[str, int] = {}
        list_matches: list[dict[str, str]] = []

        for str_page in list_blocks_pages:
            if len(dict_count_matches) > 0 \
                and all(count > 0 for count in dict_count_matches.values()): 
                break

            for str_event, dict_l1 in dict_regex_patterns.items():
                for str_condition, regex_pattern in dict_l1.items():
                    if str_event not in dict_count_matches:
                        dict_count_matches[str_event] = 0
                    if dict_count_matches[str_event] > 0:
                        break
                    regex_pattern = self.cls_str_handler.remove_diacritics_nfkd(
                        regex_pattern, 
                        bool_lower_case=False
                    )
                    regex_match = re.search(regex_pattern, str_page)
                    if regex_match and regex_match.group(0) and len(regex_match.group(0)) > 0:
                        dict_count_matches[str_event] += 1
                        dict_ = {
                            "EVENT": str_event.upper(), 
                            "MATCH_PATTERN": str_condition.upper(),
                            "PATTERN_REGEX": regex_pattern,
                        }
                        for i in range(0, len(regex_match.groups()) + 1):
                            regex_group = regex_match.group(i).replace('\n', ' ')
                            regex_group = re.sub(r"\s+", " ", regex_group).strip()
                            dict_[f"REGEX_GROUP_{i}"] = regex_group.upper()
                        list_matches.append(dict_)
                
                # fallback for blocks of page without any match
                if dict_count_matches[str_event] == 0:
                    list_matches.append({
                        "EVENT": str_event.upper(), 
                        "MATCH_PATTERN": "ZZNN/A",
                        "PATTERN_REGEX": "ZZN/A",
                    })

        return list_matches


class ABCIngestionOperations(ABCIngestion, CoreIngestion, ContentParser):
    """Abstract base class for ingestion operations."""
    
    pass