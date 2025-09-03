"""Abstract base class for ingestion operations.

This module defines an abstract base class (ABC) for ingestion operations,
providing a common interface for fetching and transforming data.
"""

from abc import abstractmethod
from datetime import date, datetime
from logging import Logger
from typing import Optional, Union

import pandas as pd
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
from stpstone.utils.parsers.str import TypeCaseFrom, TypeCaseTo


class ABCIngestion(metaclass=ABCTypeCheckerMeta):
    """Abstract base class for ingestion operations."""
    
    @abstractmethod
    def get_response(self) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Return a response object.
        
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
    def transform_response(
        self, resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> pd.DataFrame:
        """Transform a response object into a DataFrame.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
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


class CoreIngestion(metaclass=TypeChecker):
    """Abstract base class for ingestion operations."""

    def __init__(
        self, 
        url: str,
        date_ref: date,
        dict_dtypes: dict[str, Union[str, int, float, date, datetime]], 
        bool_format_log_as_str: bool = True,
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
    ) -> None:
        """Initialize the CoreIngestion class.
        
        Parameters
        ----------
        url : str
            URL of the data source.
        date_ref : date
            Reference date for logging.
        dict_dtypes : dict[str, Union[str, int, float, date, datetime]]
            Dictionary of column names and data types.
        bool_format_log_as_str : bool, optional
            Boolean flag for timestamp logging, by default True.
        cols_from_case : Optional[TypeCaseFrom], optional
            Case conversion for column names, by default None.
        cols_to_case : Optional[TypeCaseTo], optional
            Case conversion for column names, by default None.
        list_cols_drop_dupl : Optional[list[str]], optional
            List of columns to drop duplicates, by default None.
        dict_fillna_strt : TypeFillnaStrategy, optional
            Dictionary of column names and fillna values, by default None.
        str_fmt_dt : TypeDateFormatInput, optional
            Date format string, by default "YYYY-MM-DD".
        type_error_action : TypeErrorActionAsTypeDataFrame, optional
            Type error action, by default "raise".
        strategy_keep_when_dupl : TypeKeepDuplicatedDataFrame, optional
            Strategy for keeping when duplicates, by default "first".
        str_data_fillna : str, optional
            Data fillna value, by default "-99999".
        str_dt_fillna : Optional[str], optional
            Date fillna value, by default None.
        logger : Optional[Logger], optional
            Logger object, by default None.

        Returns
        -------
        None
        """
        self.url = url
        self.date_ref = date_ref
        self.bool_format_log_as_str = bool_format_log_as_str
        self.cls_df_standardization = DFStandardization(
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
        self.cls_db_logs = DBLogs()
    
    def standardize_dataframe(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Standardize the DataFrame.
        
        Parameters
        ----------
        df_ : pd.DataFrame
            The DataFrame to standardize.
        
        Returns
        -------
        pd.DataFrame
            The standardized DataFrame.
        """
        df_ = self.cls_df_standardization.pipeline(df_)
        df_ = self.cls_db_logs.audit_log(
            df_, 
            self.url, 
            self.date_ref, 
            self.bool_format_log_as_str
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
        bool_insert_or_ignore : bool, optional
            Whether to use INSERT OR IGNORE or INSERT WITHOUT IGNORING CONFLICTS (default: False)
        """
        list_ser = df_.to_dict(orient="records")
        cls_db.insert(
            list_ser,
            str_table_name=str_table_name,
            bool_insert_or_ignore = bool_insert_or_ignore,
        )


class ABCIngestionOperations(ABCIngestion, CoreIngestion):
    """Abstract base class for ingestion operations."""
    
    pass