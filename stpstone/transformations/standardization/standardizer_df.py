"""Module for standardizing DataFrames.

This module provides a class for standardizing DataFrames, including:
- Data type conversion and validation
- Data cleaning and transformation
"""

from contextlib import suppress
from logging import Logger
from typing import Any, Literal, Optional, TypeVar

import numpy as np
import pandas as pd

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker, type_checker
from stpstone.utils.calendars.calendar_abc import DateFormatter, TypeDateFormatInput
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.lists import ListHandler
from stpstone.utils.parsers.str import StrHandler, TypeCaseFrom, TypeCaseTo
from stpstone.utils.pipelines.generic import generic_pipeline


pd.set_option("future.no_silent_downcasting", True)

TypeFillnaStrategy = TypeVar(
    "TypeFillnaStrategy",
    bound=Optional[dict[str, Literal['bfill', 'ffill']]]
)
TypeErrorActionAsTypeDataFrame = TypeVar(
    "TypeErrorActionAsTypeDataFrame",
    bound=Literal['raise', 'ignore', 'coerce']
)
TypeKeepDuplicatedDataFrame = TypeVar(
    "TypeKeepDuplicatedDataFrame",
    bound=Literal['first', 'last', False]
)


class DFStandardization(metaclass=TypeChecker):
    """Class for standardizing DataFrames."""

    def __init__(
        self,
        dict_dtypes: Optional[dict[str, Any]] = None,
        cols_from_case: Optional[TypeCaseFrom] = None,
        cols_to_case: Optional[TypeCaseTo] = None,
        list_cols_drop_dupl: list[str] = None,
        dict_fillna_strt: TypeFillnaStrategy = None,
        str_fmt_dt: TypeDateFormatInput = "YYYY-MM-DD",
        type_error_action: TypeErrorActionAsTypeDataFrame = "raise",
        strategy_keep_when_dupl: TypeKeepDuplicatedDataFrame = "first",
        str_data_fillna: str = "-99999",
        str_dt_fillna: Optional[str] = None,
        logger: Optional[Logger] = None,
    ) -> None:
        """Initialize the DFStandardization class.

        Parameters
        ----------
        dict_dtypes : Optional[dict[str, Any]]
            Dictionary of column names and data types, by default {}.
        cols_from_case : Optional[TypeCaseFrom]
            Case conversion for column names, by default None.
        cols_to_case : Optional[TypeCaseTo]
            Case conversion for column names, by default None.
        list_cols_drop_dupl : list[str]
            List of columns to drop duplicates, by default None.
        dict_fillna_strt : TypeFillnaStrategy
            Dictionary of column names and fillna values, by default None.
        str_fmt_dt : TypeDateFormatInput
            Date format string, by default "YYYY-MM-DD".
        type_error_action : TypeErrorActionAsTypeDataFrame
            Action to take on type errors, by default "raise".
        strategy_keep_when_dupl : TypeKeepDuplicatedDataFrame
            Strategy for keeping when duplicates, by default "first".
        str_data_fillna : str
            Fillna value for non-date columns, by default "-99999".
        str_dt_fillna : Optional[str]
            Fillna value for date columns, by default None.
        logger : Optional[Logger]
            Logger, by default None.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If dict_dtypes is empty
        """
        if dict_dtypes is not None and not dict_dtypes:
            raise ValueError("dict_dtypes cannot be empty")
        self.dict_dtypes = dict_dtypes if dict_dtypes is not None else {}
        self.cols_from_case = cols_from_case
        self.cols_to_case = cols_to_case
        self.list_cols_drop_dupl = list_cols_drop_dupl or []
        self.dict_fillna_strt = dict_fillna_strt or {}
        self.str_fmt_dt = str_fmt_dt
        self.type_error_action = type_error_action
        self.strategy_keep_when_dupl = strategy_keep_when_dupl
        self.str_data_fillna = str_data_fillna
        self.str_dt_fillna = str_dt_fillna if str_dt_fillna else (
            "2099-12-31" if self.str_fmt_dt == "YYYY-MM-DD" else
            "31/12/2099" if self.str_fmt_dt == "DD/MM/YYYY" else
            "31/12/99" if self.str_fmt_dt == "DD/MM/YY" else
            "31122099" if self.str_fmt_dt == "DDMMYYYY" else
            "311299" if self.str_fmt_dt == "DDMMYY" else
            "20991231" if self.str_fmt_dt == "YYYYMMDD" else
            4102358400 if self.str_fmt_dt == "unix_ts" else
            "31122099"
        )
        self.list_cols_dt = [key for key, value in self.dict_dtypes.items() if value == "date"]
        self.logger = logger
        self.cls_dates = DateFormatter()
        self.cls_create_log = CreateLog()
        self.cls_list_handler = ListHandler()

    def check_if_empty(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Check if the DataFrame is empty.

        Parameters
        ----------
        df_ : pd.DataFrame
            The DataFrame to check.

        Returns
        -------
        pd.DataFrame
            The DataFrame if it is not empty.

        Raises
        ------
        ValueError
            If the DataFrame is empty.
        """
        if df_.empty:
            raise ValueError("DataFrame is empty")
        return df_

    def clean_encoding_issues(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Clean encoding issues in the DataFrame.

        Parameters
        ----------
        df_ : pd.DataFrame
            The DataFrame to clean.

        Returns
        -------
        pd.DataFrame
            The cleaned DataFrame.
        """
        @type_checker
        def clean_cell(
            x: Any # noqa ANN401: typing.Any is not allowed
        ) -> Any: # noqa ANN401: typing.Any is not allowed
            """Clean encoding issues in a cell.

            Parameters
            ----------
            x : Any
                The cell to clean.

            Returns
            -------
            Any
                The cleaned cell.
            """
            if isinstance(x, str):
                x = StrHandler().remove_diacritics(x)
                x = x.encode("ascii", errors="ignore").decode("ascii")
            return x
        df_ = df_.copy()
        for col in df_.select_dtypes(include=["object"]).columns:
            df_[col] = df_[col].apply(clean_cell)
        return df_

    def coluns_names_case(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Change the case of the columns names.

        Parameters
        ----------
        df_ : pd.DataFrame
            The DataFrame to change the case of the columns names.

        Returns
        -------
        pd.DataFrame
            The DataFrame with the columns names changed.
        """
        if self.cols_from_case and self.cols_to_case:
            list_cols = [
                StrHandler().convert_case(
                    StrHandler().remove_diacritics(col_),
                    self.cols_from_case,
                    self.cols_to_case,
                )
                for col_ in list(df_.columns)
            ]
            df_.columns = list_cols
        return df_

    def limit_columns_to_dtypes(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Limit the columns to the specified data types.

        Parameters
        ----------
        df_ : pd.DataFrame
            The DataFrame to limit the columns to.

        Returns
        -------
        pd.DataFrame
            The DataFrame with the columns limited to the specified data types.
        """
        list_cols_excluded = [
            x for x in list(df_.columns)
            if x not in list(self.dict_dtypes.keys())
        ]
        self.cls_create_log.log_message(
            self.logger,
            f"list cols dataframe before filtering: {list(df_.columns)}",
            "info"
        )
        self.cls_create_log.log_message(
            self.logger,
            f"list cols to filter: {list(self.dict_dtypes.keys())}",
            "info"
        )
        self.cls_create_log.log_message(
            self.logger,
            f"list of columns excluded: {list_cols_excluded}",
            "info"
        )
        list_cols = list(self.dict_dtypes.keys())
        df_ = df_[list_cols]
        return df_

    def delete_empty_rows(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Delete empty rows from the DataFrame.

        Parameters
        ----------
        df_ : pd.DataFrame
            The DataFrame to delete empty rows from.

        Returns
        -------
        pd.DataFrame
            The DataFrame with empty rows deleted.
        """
        list_mask = df_.apply(lambda row: row.isin([None, "", np.nan]).all(), axis=1)
        df_cleaned = df_[~list_mask]
        return df_cleaned

    def filler(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Fill missing values in the DataFrame.

        Parameters
        ----------
        df_ : pd.DataFrame
            The DataFrame to fill missing values in.

        Returns
        -------
        pd.DataFrame
            The DataFrame with missing values filled.

        Raises
        ------
        ValueError
            If the DataFrame is empty.
        """
        df_ = df_.replace("", np.nan)
        list_cols = [col_ for col_ in list(df_.columns) if col_ not in self.list_cols_dt]
        if self.dict_fillna_strt and len(self.dict_fillna_strt) > 0:
            for col_, strt_fill_na in self.dict_fillna_strt.items():
                if col_ not in list_cols:
                    continue
                elif strt_fill_na == "ffill":
                    df_[col_] = df_[col_].ffill()
                elif strt_fill_na == "bfill":
                    df_[col_] = df_[col_].bfill()
                else:
                    raise ValueError(f"Invalid fillna strategy: {strt_fill_na}")
        if self.list_cols_dt:
            df_[self.list_cols_dt] = df_[self.list_cols_dt].fillna(self.str_dt_fillna)
        df_[list_cols] = df_[list_cols].fillna(self.str_data_fillna)
        return df_

    def replace_num_delimiters(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Replace comma decimal separators with periods in numeric string columns and convert.

        Only processes columns that:
        1. Are of object type (strings)
        2. Contain data with either "." or "," characters
        3. Appear to contain numeric values with European-style formatting

        Parameters
        ----------
        df_ : pd.DataFrame
            The DataFrame to process.

        Returns
        -------
        pd.DataFrame
            The DataFrame with numeric string columns converted to float.
        """
        df_ = df_.copy()

        for col in df_.columns:
            if df_[col].dtype == "object":
                try:
                    sample_data = df_[col].dropna().head(10)

                    has_numeric_pattern = any(
                        isinstance(val, str) and
                        ('.' in val or ',' in val) and
                        any(char.isdigit() for char in val) and
                        (',' in val and val.replace(',', '').replace('.', '').isdigit() or
                        '.' in val and val.count('.') > 1)
                        for val in sample_data if pd.notna(val)
                    )

                    if has_numeric_pattern:
                        df_[col] = df_[col].str.replace(r"\.", "", regex=True)
                        df_[col] = df_[col].str.replace(",", ".", regex=True)
                        df_[col] = pd.to_numeric(df_[col], errors="coerce")

                except (AttributeError, TypeError):
                    continue

        return df_

    def change_dtypes(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Change the data types of the DataFrame.

        Parameters
        ----------
        df_ : pd.DataFrame
            The DataFrame to change the data types of.

        Returns
        -------
        pd.DataFrame
            The DataFrame with the data types changed.
        """
        df_ = df_.copy()
        dict_dtypes = {
            k: ("str" if v == "date" else v) for k, v in self.dict_dtypes.items()
        }
        df_ = df_.astype(dict_dtypes, errors=self.type_error_action)
        for col_ in self.list_cols_dt:
            str_fmt_dt = self.str_fmt_dt if col_ != "REF_DATE" else "YYYY-MM-DD"
            df_[col_] = df_[col_].apply(
                lambda x, fmt=str_fmt_dt: (
                    self.cls_dates.str_date_to_datetime(x, fmt)
                    if pd.notna(x) and x != self.str_dt_fillna
                    else pd.Timestamp(self.str_dt_fillna)
                )
            )
        list_cols_missing = [
            c for c in df_.columns
            if c not in self.cls_list_handler.extend_lists(list(dict_dtypes.keys()),
                                                        self.list_cols_dt)
        ]
        if list_cols_missing:
            df_[list_cols_missing] = df_[list_cols_missing].astype(str)
        return df_

    def strip_hidden_characters(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Strip hidden characters from the DataFrame.

        Parameters
        ----------
        df_ : pd.DataFrame
            The DataFrame to strip hidden characters from.

        Returns
        -------
        pd.DataFrame
            The DataFrame with hidden characters stripped.
        """
        string_cols = [
            c for c in df_.select_dtypes(include=["object", "category"]).columns
            if c not in self.list_cols_dt
        ]

        for col_ in string_cols:
            try:
                # Check if the column actually contains string data
                if df_[col_].dtype in ["object", "category"]:
                    # Additional check: ensure the column has string values
                    sample_values = df_[col_].dropna().head(5)
                    if len(sample_values) > 0 and all(isinstance(val, str)
                                                      for val in sample_values):
                        df_[col_] = df_[col_].str.replace(
                            r"[\u200B\u200C\u200D\uFEFF]", "", regex=True)
            except AttributeError:
                # Skip columns that don't support string operations
                continue
            except Exception as e:
                # Log the error and continue with other columns
                if self.logger:
                    self.cls_create_log.log_message(
                        self.logger,
                        f"Warning: Could not strip hidden characters from column {col_}: {e}",
                        "warning"
                    )
                continue

        return df_

    def strip_data(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Strip data from the DataFrame.

        Parameters
        ----------
        df_ : pd.DataFrame
            The DataFrame to strip data from.

        Returns
        -------
        pd.DataFrame
            The DataFrame with data stripped.
        """
        list_cols = [
            col_ for col_ in df_.select_dtypes(["object"]).columns
            if col_ not in self.list_cols_dt
        ]
        for col_ in list_cols:
            with suppress(AttributeError):
                df_[col_] = df_[col_].str.strip()
        return df_

    def remove_duplicated_cols(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicated columns from the DataFrame.

        Parameters
        ----------
        df_ : pd.DataFrame
            The DataFrame to remove duplicated columns from.

        Returns
        -------
        pd.DataFrame
            The DataFrame with duplicated columns removed.
        """
        df_ = df_.loc[:, ~df_.columns.duplicated(keep=self.strategy_keep_when_dupl)]
        return df_

    def data_remove_dupl(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicated data from the DataFrame.

        Parameters
        ----------
        df_ : pd.DataFrame
            The DataFrame to remove duplicated data from.

        Returns
        -------
        pd.DataFrame
            The DataFrame with duplicated data removed.
        """
        if self.list_cols_drop_dupl:
            df_ = df_.drop_duplicates(
                subset=self.list_cols_drop_dupl, keep=self.strategy_keep_when_dupl
            )
        return df_

    def pipeline(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Pipeline for standardizing data.

        Parameters
        ----------
        df_ : pd.DataFrame
            The DataFrame to standardize.

        Returns
        -------
        pd.DataFrame
            The standardized DataFrame.
        """
        steps = [
            self.check_if_empty,
            self.clean_encoding_issues,
            self.coluns_names_case,
            self.limit_columns_to_dtypes,
            self.delete_empty_rows,
            self.filler,
            self.replace_num_delimiters,
            self.change_dtypes,
            self.strip_hidden_characters,
            self.strip_data,
            self.remove_duplicated_cols,
            self.data_remove_dupl,
        ]
        df_ = generic_pipeline(df_, steps)
        return df_
