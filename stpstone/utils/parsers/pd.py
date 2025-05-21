### PANDAS DATAFRAME MODULE ###

# pypi.org libs
import os
from logging import Logger
from typing import Any, Dict, List, Tuple

import pandas as pd

# local libs
from stpstone._config.global_slots import YAML_MICROSOFT_APPS
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement

# check if this is a windows machine
if os.name == "nt":
    from stpstone.utils.microsoft_apps.excel import DealingExcel


class DealingPd:

    def __init__(self):
        self.dict_sensitivity_labels_office = {
            "public": "e6a9157b-bcf3-4eac-b03e-7cf007ba9fdf",
            "internal": "d5bef5af-bbc1-4d24-bb43-47a55a90f763",
            "confidential": "f33522dc-133a-44f0-ba7f-cdd6f493ffbd",
            "restricted": "69878aea-41a3-47a7-b03f-1211df6e7785"
        }

    def append_df_to_Excel(
        self,
        filename: str,
        list_tup_df_sheet_name: List[Tuple[pd.DataFrame, str]],
        bl_header: bool = True,
        bl_index: int = 0,
        mode: str = "w",
        label_sensitivity: str = "internal",
        bl_set_sensitivity_label: bool = False,
        bl_debug_mode: bool = False,
    ) -> None:
        """
        Append dataframe to Excel

        Args:
            filename (str): Path to Excel file
            list_tup_df_sheet_name (List[Tuple[pd.DataFrame, str]]): List of tuples with dataframe and sheet name
            bl_header (bool): Whether to write headers
            bl_index (int): Whether to write index
            mode (str): Mode to open Excel file
            label_sensitivity (str): Label sensitivity
            bl_set_sensitivity_label (bool): Whether to set sensitivity label
            bl_debug_mode (bool): Whether to print debug messages

        Returns:
            None
        """
        if bl_debug_mode == True:
            print("LIST_TUP_DF_SHEET_NAME: {}".format(list_tup_df_sheet_name))
            print("FILENAME: {}".format(filename))
            print("MODE: {}".format(mode))
            print("BL_INDEX: {}".format(bl_index))
        with pd.ExcelWriter(filename, engine="xlsxwriter", mode=mode) as writer:
            for df_, sheet_name in list_tup_df_sheet_name:
                if bl_index == 0:
                    df_.reset_index(drop=True, inplace=True)
                df_.to_excel(writer, sheet_name, index=bl_index, header=bl_header)
        if (bl_set_sensitivity_label == True) and (os.name == "nt"):
            DealingExcel().xlsx_sensitivity_label(
                filename,
                self.dict_sensitivity_labels_office,
                label_sensitivity.capitalize(),
            )

    def export_xl(
        self,
        logger: Logger,
        path_xlsx: str,
        list_tup_df_sheet_name: List[Tuple[pd.DataFrame, str]],
        range_columns: str = "A:CC",
        bl_adjust_layout: bool = False,
        bl_debug_mode: bool = False,
    ) -> bool:
        """
        Export dataframe to Excel

        Args:
            - logger (logging.Logger): Logger object
            - path_xlsx (str): Path to Excel file
            - list_tup_df_sheet_name (list): List of tuples of DataFrames and sheet names
            - range_columns (str): Range of columns to autofit
            - bl_adjust_layout (bool): Whether to adjust layout of Excel file
            - bl_debug_mode (bool): Whether to print debug messages

        Returns:
            bool
        """
        self.append_df_to_Excel(
            path_xlsx,
            list_tup_df_sheet_name,
            bl_header=True,
            bl_index=0,
            bl_debug_mode=bl_debug_mode,
        )
        blame_xpt = DirFilesManagement().object_exists(path_xlsx)
        if blame_xpt == True:
            if bl_adjust_layout == True:
                for _, sheet_name in list_tup_df_sheet_name:
                    xl_app, wb = DealingExcel().open_xl(path_xlsx)
                    DealingExcel().autofit_range_columns(
                        sheet_name, range_columns, xl_app, wb
                    )
                    DealingExcel().close_wb(wb)
        else:
            CreateLog().warning(
                logger, "File not saved to hard drive: {}".format(path_xlsx)
            )
            raise Exception("File not saved to hard drive: {}".format(path_xlsx))
        return blame_xpt

    def settingup_pandas(
        self,
        int_decimal_places: int = 3,
        bl_wrap_repr: bool = False,
        int_max_rows: int = 25,
    ) -> None:
        """
        Setting up pandas options

        Args:
            int_decimal_places (int): Number of decimal places to display in output
            bl_wrap_repr (bool): Whether to wrap repr(DataFrame) across additional lines
            int_max_rows (int): Maximum number of rows to display in output

        Returns:
            None
        """
        pd.set_option("display.precision", int_decimal_places)
        pd.set_option("display.expand_frame_repr", bl_wrap_repr)
        pd.set_option("display.max_rows", int_max_rows)

    def convert_datetime_columns(
        self,
        df_: pd.DataFrame,
        list_col_date: List[str],
        bl_pandas_convertion: bool = True,
    ) -> pd.DataFrame:
        """
        Convert datetime columns

        Args:
            df_ (pandas.DataFrame): DataFrame to convert
            list_col_date (list): List of columns to convert
            bl_pandas_convertion (bool): Whether to use pandas conversion or excel format

        Returns:
            pd.DataFrame
        """
        # checking wheter to covert through a pandas convertion, or resort to a excel format
        #   transformation of data in date column format
        if bl_pandas_convertion:
            for col_date in list_col_date:
                df_.loc[:, col_date] = pd.to_datetime(df_[col_date], unit="s").dt.date
        else:
            # corventing list column dates to string type
            df_ = df_.astype(dict(zip(list_col_date, [str] * len(list_col_date))))
            # looping through each row
            for index, row in df_.iterrows():
                for col_date in list_col_date:
                    if "-" in row[col_date]:
                        ano = int(row[col_date].split(" ")[0].split("-")[0])
                        mes = int(row[col_date].split(" ")[0].split("-")[1])
                        dia = int(row[col_date].split(" ")[0].split("-")[2])
                        df_.loc[index, col_date] = DatesBR().build_date(ano, mes, dia)
                    else:
                        df_.loc[index, col_date] = DatesBR().excel_float_to_date(
                            int(row[col_date])
                        )
        # returning dataframe with date column to datetime forma
        return df_

    def merge_dfs_into_df(
        self, df_1: pd.DataFrame, df_2: pd.DataFrame, list_cols: List[str]
    ) -> pd.DataFrame:
        """
        Merging two dataframes and removing their intersections into a list of columns

        Args:
            df_1 (pandas.DataFrame): First dataframe to merge
            df_2 (pandas.DataFrame): Second dataframe to merge
            list_cols (list): List of columns to merge

        Returns:
            pd.DataFrame
        """
        df_intersec = pd.merge(df_1, df_2, how="inner", on=list_cols)
        df_merge = pd.merge(
            df_2, df_intersec, how="outer", on=list_cols, indicator=True
        )
        df_merge = df_merge.loc[df_merge._merge == "left_only"]
        df_merge.dropna(how="all", axis=1, inplace=True)
        try:
            df_merge = df_merge.drop(columns="_merge")
        except:
            pass
        for column in df_merge.columns:
            if column in list_cols:
                pass
            else:
                df_merge = df_merge.rename(columns={str(column): str(column)[:-2]})
        return df_merge

    def max_chrs_per_column(df_: pd.DataFrame) -> Dict[str, int]:
        """
        Calculate the maximum number of characters per column

        Args:
            df_ (pandas.DataFrame): DataFrame to calculate

        Returns:
            Dict[str, int
        """
        dict_ = dict()
        for col_ in list(df_.columns):
            dict_[col_] = df_[col_].astype(str).str.len().max()
        return dict_
