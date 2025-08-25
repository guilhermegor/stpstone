"""Pandas DataFrame utilities for data manipulation and Excel export.

This module provides a class for handling pandas DataFrame operations including Excel export,
datetime conversion, merging, and column analysis. It includes integration with Windows Excel
functionality when available.
"""

import contextlib
from logging import Logger
import os
from typing import Literal

import pandas as pd

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.cals.cal_abc import DatesBR
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


if os.name == "nt":
    from stpstone.utils.microsoft_apps.excel import DealingExcel


class DealingPd(metaclass=TypeChecker):
    """Class for handling pandas DataFrame operations and Excel integration."""

    def __init__(self) -> None:
        """Initialize with Office sensitivity labels dictionary.

        Parameters
        ----------
        None
        
        Returns
        -------
        None

        Raises
        ------
        ImportError
            If required Excel engines 'xlsxwriter' and 'openpyxl' are not installed.
        """
        self.dict_sensitivity_labels_office = {
            "public": "e6a9157b-bcf3-4eac-b03e-7cf007ba9fdf",
            "internal": "d5bef5af-bbc1-4d24-bb43-47a55a90f763",
            "confidential": "f33522dc-133a-44f0-ba7f-cdd6f493ffbd",
            "restricted": "69878aea-41a3-47a7-b03f-1211df6e7785"
        }
        try:
            import openpyxl # noqa F401: imported but unused
            import xlsxwriter # noqa F401: imported but unused
        except ImportError as err:
            raise ImportError("Required Excel engines 'xlsxwriter' and 'openpyxl'.") from err
        if os.name == "nt":
            self.cls_dealing_xl = DealingExcel()

    def _validate_sensitivity_label(self, label_sensitivity: str) -> None:
        """Validate sensitivity label against known options.

        Parameters
        ----------
        label_sensitivity : str
            Sensitivity label to validate

        Raises
        ------
        ValueError
            If label is not one of: public, internal, confidential, restricted
        """
        valid_labels = ["public", "internal", "confidential", "restricted"]
        if label_sensitivity.lower() not in valid_labels:
            raise ValueError(f"Invalid sensitivity label. Must be one of: {valid_labels}")

    def _validate_dataframe(self, df_: pd.DataFrame) -> None:
        """Validate DataFrame is not empty.

        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame to validate

        Raises
        ------
        ValueError
            If DataFrame is empty
        """
        if df_.empty:
            raise ValueError("DataFrame cannot be empty")

    def append_df_to_Excel(
        self,
        filename: str,
        list_tup_df_sheet_name: list[tuple[pd.DataFrame, str]],
        bool_header: bool = True,
        bool_index: bool = False,
        mode: Literal["w", "a"] = "w",
        label_sensitivity: Literal["public", "internal", "confidential", "restricted"] = \
            "internal",
        bool_set_sensitivity_label: bool = False,
        engine: str = "openpyxl"
    ) -> None:
        """Append DataFrame to Excel file with optional sensitivity labeling.

        Parameters
        ----------
        filename : str
            Path to Excel file
        list_tup_df_sheet_name : list[tuple[pd.DataFrame, str]]
            list of tuples containing DataFrame and sheet name pairs
        bool_header : bool
            Whether to write headers (default: True)
        bool_index : bool
            Whether to write index (default: False)
        mode : Literal['w', 'a']
            Write mode ('w' for write, 'a' for append) (default: "w")
        label_sensitivity : Literal['public', 'internal', 'confidential', 'restricted']
            Sensitivity label for Office files (default: "internal")
        bool_set_sensitivity_label : bool
            Whether to set sensitivity label (Windows only) (default: False)
        engine : str
            Excel engine to use (default: "openpyxl")
        
        Raises
        ------
        ValueError
            If filename is empty
            If list_tup_df_sheet_name is empty
            If Windows-only features are used on non-Windows OS
        """
        if not filename:
            raise ValueError("Filename cannot be empty")
        if not list_tup_df_sheet_name:
            raise ValueError("DataFrame and sheet name list cannot be empty")

        with pd.ExcelWriter(filename, engine=engine, mode=mode) as writer:
            for df_, sheet_name in list_tup_df_sheet_name:
                self._validate_dataframe(df_)
                if not bool_index:
                    df_ = df_.reset_index(drop=True)
                df_.to_excel(
                    excel_writer=writer,
                    sheet_name=sheet_name,
                    index=bool_index,
                    header=bool_header
                )

        if bool_set_sensitivity_label:
            if os.name != "nt":
                raise ValueError("Sensitivity labels only supported on Windows")
            self._validate_sensitivity_label(label_sensitivity)
            self.cls_dealing_xl.xlsx_sensitivity_label(
                filename,
                self.dict_sensitivity_labels_office,
                label_sensitivity.capitalize(),
            )

    def export_xl(
        self,
        logger: Logger,
        path_xlsx: str,
        list_tup_df_sheet_name: list[tuple[pd.DataFrame, str]],
        range_columns: str = "A:CC",
        bool_adjust_layout: bool = False,
    ) -> bool:
        """Export DataFrame to Excel with optional layout adjustment.

        Parameters
        ----------
        logger : Logger
            Logger object for error reporting
        path_xlsx : str
            Path to Excel file
        list_tup_df_sheet_name : list[tuple[pd.DataFrame, str]]
            list of tuples containing DataFrame and sheet name pairs
        range_columns : str
            Column range for autofit (default: "A:CC")
        bool_adjust_layout : bool
            Whether to adjust column layout (default: False)

        Returns
        -------
        bool
            True if export succeeded, False otherwise

        Raises
        ------
        Exception
            If file fails to save
        """
        self.append_df_to_Excel(
            path_xlsx,
            list_tup_df_sheet_name,
            bool_header=True,
            bool_index=False,
        )

        blame_xpt = DirFilesManagement().object_exists(path_xlsx)
        if not blame_xpt:
            CreateLog().log_message(
                logger, 
                f"File not saved to hard drive: {path_xlsx}", 
                "warning"
            )
            raise Exception(f"File not saved to hard drive: {path_xlsx}")

        if bool_adjust_layout and os.name == "nt":
            for _, sheet_name in list_tup_df_sheet_name:
                xl_app, wb = self.cls_dealing_xl.open_xl(path_xlsx)
                self.cls_dealing_xl.autofit_range_columns(
                    sheet_name, range_columns, xl_app, wb
                )
                self.cls_dealing_xl.close_wb(wb)

        return blame_xpt

    def settingup_pandas(
        self,
        int_decimal_places: int = 3,
        bool_wrap_repr: bool = False,
        int_max_rows: int = 25,
    ) -> None:
        """Configure pandas display options.

        Parameters
        ----------
        int_decimal_places : int
            Number of decimal places to display (default: 3)
        bool_wrap_repr : bool
            Whether to wrap DataFrame repr (default: False)
        int_max_rows : int
            Maximum rows to display (default: 25)
        """
        pd.set_option("display.precision", int_decimal_places)
        pd.set_option("display.expand_frame_repr", bool_wrap_repr)
        pd.set_option("display.max_rows", int_max_rows)

    def convert_datetime_columns(
        self,
        df_: pd.DataFrame,
        list_col_date: list[str],
        bool_pandas_convertion: bool = True,
    ) -> pd.DataFrame:
        """Convert specified columns to datetime format.

        Parameters
        ----------
        df_ : pd.DataFrame
            Input DataFrame
        list_col_date : list[str]
            list of date column names to convert
        bool_pandas_convertion : bool
            Use pandas conversion (True) or Excel date handling (False) (default: True)

        Returns
        -------
        pd.DataFrame
            DataFrame with converted date columns

        Raises
        ------
        ValueError
            If list_col_date is empty
        """
        if not list_col_date:
            raise ValueError("Date columns list cannot be empty")

        if bool_pandas_convertion:
            for col_date in list_col_date:
                df_.loc[:, col_date] = pd.to_datetime(df_[col_date]).dt.date
        else:
            df_ = df_.astype(dict(zip(list_col_date, [str] * len(list_col_date))))
            for index, row in df_.iterrows():
                for col_date in list_col_date:
                    if "-" in row[col_date]:
                        ano = int(row[col_date].split(" ")[0].split("-")[0])
                        mes = int(row[col_date].split(" ")[0].split("-")[1])
                        dia = int(row[col_date].split(" ")[0].split("-")[2])
                        df_.loc[index, col_date] = DatesBR().build_date(ano, mes, dia)
                    else:
                        df_.loc[index, col_date] = DatesBR().excel_float_to_date(
                            int(row[col_date]))
        return df_

    def merge_dfs_into_df(
        self,
        df_1: pd.DataFrame,
        df_2: pd.DataFrame,
        list_cols: list[str],
    ) -> pd.DataFrame:
        """Merge two DataFrames while removing intersections.

        Parameters
        ----------
        df_1 : pd.DataFrame
            First DataFrame to merge
        df_2 : pd.DataFrame
            Second DataFrame to merge
        list_cols : list[str]
            list of columns to merge on

        Returns
        -------
        pd.DataFrame
            Merged DataFrame with unique rows from df_2

        Raises
        ------
        ValueError
            If merge columns list is empty
        """
        if not list_cols:
            raise ValueError("Merge columns list cannot be empty")

        df_intersec = df_1.merge(df_2, how="inner", on=list_cols)
        df_merge = df_2.merge(
            df_intersec, how="outer", on=list_cols, indicator=True
        )
        df_merge = df_merge.loc[df_merge._merge == "left_only"]
        df_merge = df_merge.dropna(how="all", axis=1)
        
        with contextlib.suppress(KeyError):
            df_merge = df_merge.drop(columns="_merge")
            
        return df_merge

    def max_chrs_per_column(self, df_: pd.DataFrame) -> dict[str, int]:
        """Calculate maximum character length for each column.

        Parameters
        ----------
        df_ : pd.DataFrame
            Input DataFrame

        Returns
        -------
        dict[str, int]
            Dictionary mapping column names to maximum character lengths

        Raises
        ------
        ValueError
            If input DataFrame is empty
        """
        if df_.empty:
            raise ValueError("Input DataFrame cannot be empty")

        return {
            col_: df_[col_].astype(str).str.len().max()
            for col_ in df_.columns
        }