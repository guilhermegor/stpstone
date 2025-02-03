
### PANDAS DATAFRAME MODULE ###

# pypi.org libs
import os
import pandas as pd
from typing import List, Dict, Any, Tuple
from logging import Logger
# local libs
from stpstone.settings._global_slots import YAML_MICROSOFT_APPS
from stpstone.cals.handling_dates import DatesBR
from stpstone.handling_data.folders import DirFilesManagement
from stpstone.loggs.create_logs import CreateLog
# check if this is a windows machine
if os.name == 'nt':
    from stpstone.microsoft_apps.excel import DealingExcel


class DealingPd():

    def append_df_to_Excel(self, filename:str, list_tup_df_sheet_name:List[Tuple[pd.DataFrame, str]], 
                           bl_header:bool=True, bl_index:int=0, mode:str='w', 
                           label_sensitivity:str='internal', bl_set_sensitivity_label:bool=False, 
                           bl_debug_mode:bool=False) -> None:
        """
        Append dataframe to Excel
        Args:
            - filename (str): Path to Excel file
            - list_tup_df_sheet_name (List[Tuple[pd.DataFrame, str]]): List of tuples with dataframe and sheet name
            - bl_header (bool): Whether to write headers
            - bl_index (int): Whether to write index
            - mode (str): Mode to open Excel file
            - label_sensitivity (str): Label sensitivity
            - bl_set_sensitivity_label (bool): Whether to set sensitivity label
            - bl_debug_mode (bool): Whether to print debug messages
        Returns:
            None
        """
        if bl_debug_mode == True:
            print('LIST_TUP_DF_SHEET_NAME: {}'.format(list_tup_df_sheet_name))
            print('FILENAME: {}'.format(filename))
            print('MODE: {}'.format(mode))
            print('BL_INDEX: {}'.format(bl_index))
        with pd.ExcelWriter(filename, engine='xlsxwriter', mode=mode) as writer:
            for df_, sheet_name in list_tup_df_sheet_name:
                if bl_index == 0:
                    df_.reset_index(drop=True, inplace=True)
                df_.to_excel(writer, sheet_name, index=bl_index, header=bl_header)
        if \
            (bl_set_sensitivity_label == True) \
            and (os.name == 'nt'):
            DealingExcel().xlsx_sensitivity_label(filename, YAML_MICROSOFT_APPS[
                'sensitivity_labels_office'], 
                label_sensitivity.capitalize())

    def export_xl(self, logger:Logger, path_xlsx:str, 
                  list_tup_df_sheet_name:List[Tuple[pd.DataFrame, str]], 
                  range_columns:str='A:CC', bl_adjust_layout:bool=False, 
                  bl_debug_mode:bool=False) -> bool:
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
            path_xlsx, list_tup_df_sheet_name, bl_header=True, bl_index=0, 
            bl_debug_mode=bl_debug_mode)
        blame_xpt = DirFilesManagement().object_exists(path_xlsx)
        if blame_xpt == True:
            if bl_adjust_layout == True:
                for _, sheet_name in list_tup_df_sheet_name:
                    xl_app, wb = DealingExcel().open_xl(path_xlsx)
                    DealingExcel().autofit_range_columns(sheet_name, range_columns, xl_app, wb)
                    DealingExcel().close_wb(wb)
        else:
            CreateLog().warnings(logger, 'File not saved to hard drive: {}'.format(
                path_xlsx))
            raise Exception('File not saved to hard drive: {}'.format(path_xlsx))
        return blame_xpt

    def settingup_pandas(self, int_decimal_places:int=3, bl_wrap_repr:bool=False, 
                         int_max_rows:int=25) -> None:
        """
        Setting up pandas options
        Args:
            - int_decimal_places (int): Number of decimal places to display in output
            - bl_wrap_repr (bool): Whether to wrap repr(DataFrame) across additional lines
            - int_max_rows (int): Maximum number of rows to display in output
        Returns:
            None
        """
        pd.set_option("display.precision", int_decimal_places)
        pd.set_option("display.expand_frame_repr", bl_wrap_repr)
        pd.set_option("display.max_rows", int_max_rows)

    def convert_datetime_columns(self, df_:pd.DataFrame, list_col_date:List[str], 
                                 bl_pandas_convertion:bool=True) -> pd.DataFrame:
        """
        Convert datetime columns
        Args:
            - df_ (pandas.DataFrame): DataFrame to convert
            - list_col_date (list): List of columns to convert
            - bl_pandas_convertion (bool): Whether to use pandas conversion or excel format
        Returns:
            pd.DataFrame
        """
        # checking wheter to covert through a pandas convertion, or resort to a excel format 
        #   transformation of data in date column format
        if bl_pandas_convertion:
            for col_date in list_col_date:
                df_.loc[:, col_date] = pd.to_datetime(
                    df_[col_date], unit='s').dt.date
        else:
            # corventing list column dates to string type
            df_ = df_.astype(
                dict(zip(list_col_date, [str] * len(list_col_date))))
            # looping through each row
            for index, row in df_.iterrows():
                for col_date in list_col_date:
                    if '-' in row[col_date]:
                        ano = int(row[col_date].split(' ')[0].split('-')[0])
                        mes = int(row[col_date].split(' ')[0].split('-')[1])
                        dia = int(row[col_date].split(' ')[0].split('-')[2])
                        df_.loc[index, col_date] = DatesBR(
                        ).build_date(ano, mes, dia)
                    else:
                        df_.loc[index, col_date] = DatesBR(
                        ).excel_float_to_date(int(row[col_date]))
        # returning dataframe with date column to datetime forma
        return df_

    def merge_dfs_into_df(self, df_1:pd.DataFrame, df_2:pd.DataFrame, list_cols:List[str]
                          ) -> pd.DataFrame:
        """
        Merging two dataframes and removing their intersections into a list of columns
        Args:
            - df_1 (pandas.DataFrame): First dataframe to merge
            - df_2 (pandas.DataFrame): Second dataframe to merge
            - list_cols (list): List of columns to merge
        Returns:
            pd.DataFrame
        """
        df_intersec = pd.merge(df_1, df_2, how='inner', on=list_cols)
        df_merge = pd.merge(df_2, df_intersec, how='outer',
                            on=list_cols, indicator=True)
        df_merge = df_merge.loc[df_merge._merge == 'left_only']
        df_merge.dropna(how='all', axis=1, inplace=True)
        try:
            df_merge = df_merge.drop(columns='_merge')
        except:
            pass
        for column in df_merge.columns:
            if column in list_cols:
                pass
            else:
                df_merge = df_merge.rename(
                    columns={str(column): str(column)[:-2]})
        return df_merge

    def max_chrs_per_column(df_:pd.DataFrame) -> Dict[str, int]:
        """
        Calculate the maximum number of characters per column
        Args:
            - df_ (pandas.DataFrame): DataFrame to calculate
        Returns:
            Dict[str, int
        """
        dict_ = dict()
        for col_ in list(df_.columns):
            dict_[col_] = df_[col_].astype(str).str.len().max()
        return dict_

    def change_dtypes(self, df_:pd.DataFrame, dict_dtypes:Dict[str, Any], list_cols_dt:List[str], 
        str_fmt_dt:str='YYYY-MM-DD', errors:str='raise') -> pd.DataFrame:
        """
        Change columns dtypes
        Args:
            - df_ (pandas.DataFrame): DataFrame to change
            - dict_dtypes (dict): Dictionary of columns and dtypes
            - list_cols_dt (list): List of columns to change
            - str_fmt_dt (str): Date format
            - errors (str): Error handling mode
        Returns:
            pd.DataFrame
        """
        if any([col_ not in list(dict_dtypes.keys()) for col_ in list_cols_dt]):
            for col_ in list(dict_dtypes.keys()):
                dict_dtypes[col_] = str
        df_ = df_.astype(dict_dtypes, errors=errors)
        for col_ in list_cols_dt:
            df_[col_] = [DatesBR().str_date_to_datetime(d, str_fmt_dt) for d in df_[col_]]
        return df_

    def strip_all_obj_dtypes(self, df_:pd.DataFrame, list_cols_dt:List[str]) -> pd.DataFrame:
        """
        Strip values from columns with object dtype
        Args:
            - df_ (pandas.DataFrame): DataFrame to strip
            - list_cols_dt (list): List of columns to strip
        Returns:
            pd.DataFrame
        """
        list_cols = [col_ for col_ in list(df_.select_dtypes(['object']).columns) if col_ 
            not in list_cols_dt]
        for col_ in list_cols:
            df_[col_] = [x.strip() for x in df_[col_]]
        return df_

    def fillna_data(self, df_:pd.DataFrame, list_cols_dt:List[str], str_dt_fillna:str='2100-12-31', 
        str_data_fillna:str='-1') -> pd.DataFrame:
        """
        Fill NaN values
        Args:
            - df_ (pandas.DataFrame): DataFrame to fill
            - list_cols_dt (list): List of date type columns
            - str_dt_fillna (str): Date fillna value
            - str_data_fillna (str): Data fillna value
        Returns:
            pd.DataFrame
        """
        list_cols = [col_ for col_ in list(df_.columns) if col_ not in list_cols_dt]
        if list_cols_dt is not None:
            df_[list_cols_dt] = df_[list_cols_dt].fillna(str_dt_fillna)
        df_[list_cols] = df_[list_cols].fillna(str_data_fillna)
        return df_

    def pipeline_df_startup(self, df_:pd.DataFrame, dict_dtypes:Dict[str, Any], 
        list_cols_dt:List[str], str_dt_fillna:str='2100-12-31', str_data_fillna:str='-1', 
        str_fmt_dt:str='YYYY-MM-DD') -> pd.DataFrame:
        """
        Pipepline for dataframe cleaning
        Args:
            - df_ (pandas.DataFrame): DataFrame to clean
            - dict_dtypes (dict): Dictionary of columns and dtypes
            - list_cols_dt (list): List of columns to change
            - str_dt_fillna (str): Date fillna value
            - str_data_fillna (str): Data fillna value
            - str_fmt_dt (str): Date format
        Returns:
            pd.DataFrame
        """
        df_ = self.fillna_data(df_, list_cols_dt, str_dt_fillna, str_data_fillna)
        df_ = self.change_dtypes(df_, dict_dtypes, list_cols_dt, str_fmt_dt)
        df_ = self.strip_all_obj_dtypes(df_, list_cols_dt)
        return df_

    def cols_remove_dupl(self, df_:pd.DataFrame) -> pd.DataFrame:
        """
        Columns names remove duplicates
        Args:
            - df_ (pandas.DataFrame): DataFrame to remove
        Returns:
            pd.DataFrame
        """
        return df_.loc[:, ~df_.columns.duplicated()]
