### DATAFRAME STANDARDIZATION ###

# pypi.org
from logging import Logger
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, StandardScaler

from stpstone._config.global_slots import YAML_GEN
from stpstone.transformations.validation.metaclass_type_checker import \
    TypeChecker
# local libs
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.lists import HandlingLists
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.pipelines.generic import generic_pipeline


class DFStandardization(metaclass=TypeChecker):

    def __init__(
        self,
        dict_dtypes:Dict[str, Any],
        cols_from_case:Optional[str]=None, cols_to_case:Optional[str]=None,
        list_cols_drop_dupl:List[str]=None, str_fmt_dt:str='YYYY-MM-DD',
        type_error_action:str='raise', strategy_keep_when_dupl:str='first',
        str_data_fillna:str='-1', str_dt_fillna:Optional[str]=None, str_tz:str='UTC',
        encoding:str='latin-1', bl_debug:bool=False, logger:Optional[Logger]=None
    ):
        self.dict_dtypes = dict_dtypes
        self.cols_from_case = cols_from_case
        self.cols_to_case = cols_to_case
        self.list_cols_drop_dupl = list_cols_drop_dupl
        self.str_fmt_dt = str_fmt_dt
        self.type_error_action = type_error_action
        self.strategy_keep_when_dupl = strategy_keep_when_dupl
        self.str_data_fillna = str_data_fillna
        self.str_dt_fillna = str_dt_fillna if str_dt_fillna is not None else (
            '2099-12-31' if self.str_fmt_dt == 'YYYY-MM-DD' else
            '31/12/2099' if self.str_fmt_dt == 'DD/MM/YYYY' else
            '31/12/99' if self.str_fmt_dt == 'DD/MM/YY' else
            '31122099' if self.str_fmt_dt == 'DDMMYYYY' else
            '311299' if self.str_fmt_dt == 'DDMMYY' else
            '20991231' if self.str_fmt_dt == 'YYYYMMDD' else
            4102358400 if self.str_fmt_dt == 'unix_ts' else
            '31122099'
        )
        self.list_cols_dt = [key for key, value in dict_dtypes.items() if value == 'date']
        self.str_tz = str_tz
        self.encoding = encoding
        self.bl_debug = bl_debug
        self.logger = logger

    def check_if_empty(self, df_:pd.DataFrame) -> pd.DataFrame:
        if df_.empty == True:
            raise ValueError('DataFrame is empty')
        return df_

    def clean_encoding_issues(self, df_:pd.DataFrame) -> pd.DataFrame:
        """
        Cleans encoding issues in a DataFrame by removing non-decodable characters.
        Args:
            df_ (pd.DataFrame): The DataFrame to clean.
        Returns:
            pd.DataFrame: A DataFrame with encoding issues removed.
        """
        def clean_cell(x):
            if isinstance(x, str):
                # remove non-printable and non-ASCII characters
                x = ''.join(char for char in x if char.isprintable() and ord(char) < 128)
                # replace problematic characters with a placeholder
                x = x.encode(self.encoding, errors='ignore').decode(self.encoding)
            return x
        return df_.apply(clean_cell)

    def strip_hidden_characters(self, df_:pd.DataFrame) -> pd.DataFrame:
        """
        Strips hidden characters (e.g., zero-width spaces) from string columns.
        Args:
            df_ (pd.DataFrame): The DataFrame to clean.
        Returns:
            pd.DataFrame: A DataFrame with hidden characters removed.
        """
        for col in df_.select_dtypes(include=['object']).columns:
            df_[col] = df_[col].str.replace(r'[\u200B\u200C\u200D\uFEFF]', '', regex=True)
        return df_

    def column_names(self, df_:pd.DataFrame) -> List[str]:
        if all([x is not None for x in [self.cols_from_case, self.cols_to_case]]):
            list_cols = [
                StrHandler().convert_case(
                    StrHandler().remove_diacritics(col_),
                    self.cols_from_case,
                    self.cols_to_case
                )
                for col_ in list(df_.columns)
            ]
        else:
            list_cols = list(df_.columns)
        df_.columns = list_cols
        return df_

    def limit_columns_to_dtypes(self, df_: pd.DataFrame) -> pd.DataFrame:
        """
        Limit the DataFrame columns to the keys of self.dict_dtypes
        Args:
            df_ (pd.DataFrame): DataFrame to filter
        Returns:
            pd.DataFrame
        """
        if self.logger is not None:
            CreateLog().infos(self.logger, f'List cols dataframe before filtering: {list(df_.columns)}')
            CreateLog().infos(self.logger, f'List cols to filter: {list(self.dict_dtypes.keys())}')
            CreateLog().infos(
                self.logger, 'List of columns excluded: '
                + f'{[x for x in list(df_.columns) if x not in list(self.dict_dtypes.keys())]}'
            )
        if self.bl_debug == True:
            print(f'List cols dataframe before filtering: {list(df_.columns)}')
            print(f'List cols to filter: {list(self.dict_dtypes.keys())}')
            print(
                'List of columns excluded: '
                + f'{[x for x in list(df_.columns) if x not in list(self.dict_dtypes.keys())]}'
            )
        list_cols = list(self.dict_dtypes.keys())
        df_ = df_[list_cols]
        return df_

    def delete_empty_rows(self, df_: pd.DataFrame) -> pd.DataFrame:
        list_mask = df_.apply(lambda row: row.isin([None, '']).all(), axis=1)
        df_cleaned = df_[~list_mask]
        return df_cleaned

    def completeness(self, df_:pd.DataFrame) -> pd.DataFrame:
        """
        Completeness - ensure that all required fields have valid (non-null) entries
        Args:
            df_ (pandas.DataFrame): DataFrame to fill
        Returns:
            pd.DataFrame
        """
        list_cols = [col_ for col_ in list(df_.columns) if col_ not in self.list_cols_dt]
        if self.list_cols_dt is not None:
            df_[self.list_cols_dt] = df_[self.list_cols_dt].fillna(self.str_dt_fillna)
        df_[list_cols] = df_[list_cols].fillna(self.str_data_fillna)
        return df_

    def replace_separators(self, df_):
        """
        Replace separators in DataFrame's columns that are numbers with dots as decimal separator
            and commas as thousands separator.
        Args:
            df_ (pandas.DataFrame): DataFrame to replace separators
        Returns:
            pd.DataFrame
        """
        list_cols_numerical = [
            v for k, v in self.dict_dtypes.items()
            if v in ['int64', 'float64', 'int32', 'float32', int, float]
        ]
        for col in list_cols_numerical:
            if \
                (df_[col].dtype == 'object') \
                and (df_[col].str.contains(r'^\d{1,3}(?:\.\d{3})*(,\d+)?$', regex=True).any()):
                df_[col] = df_[col].str.replace('.', '', regex=False)
                df_[col] = df_[col].str.replace(',', '.', regex=False)
                df_[col] = pd.to_numeric(df_[col], errors='coerce')
        return df_

    def change_dtypes(self, df_:pd.DataFrame) -> pd.DataFrame:
        # general columns types
        dict_dtypes = {k: ('str' if v == 'date' else v) for k, v in self.dict_dtypes.items()}
        if self.bl_debug == True:
            df_.to_excel(
                rf'{DirFilesManagement().find_project_root()}\data\test-dataframe_'
                + rf'{DatesBR().curr_date.strftime('%Y%m%d')}_{DatesBR().curr_time.strftime('%H%M%S')}.xlsx',
                index=False
            )
        df_ = df_.astype(dict_dtypes, errors=self.type_error_action)
        # dates types
        for col_ in self.list_cols_dt:
            if col_ != YAML_GEN['audit_log_cols']['ref_date']:
                str_fmt_dt = self.str_fmt_dt
            else:
                str_fmt_dt = YAML_GEN['audit_log_cols']['str_fmt_dt']
            df_[col_] = [
                DatesBR().str_date_to_datetime(d, str_fmt_dt)
                if str_fmt_dt != 'unix_ts'
                else DatesBR().unix_timestamp_to_date(d, str_tz=self.str_tz)
                for d in df_[col_]
            ]
        # missing columns - leftovers from previous steps
        list_cols_missing = [
            c for c in list(df_.columns)
            if c not in HandlingLists().extend_lists(list(dict_dtypes.keys()), self.list_cols_dt)
        ]
        if len(list_cols_missing) > 0:
            df_[list_cols_missing] = df_[list_cols_missing].astype(str)
        return df_

    def strip_all_obj_dtypes(self, df_:pd.DataFrame) -> pd.DataFrame:
        list_cols = [col_ for col_ in list(df_.select_dtypes(['object']).columns) if col_
            not in self.list_cols_dt]
        for col_ in list_cols:
            try:
                df_[col_] = [x.strip() for x in df_[col_]]
            except AttributeError:
                pass
        return df_

    def remove_duplicated_cols(self, df_:pd.DataFrame) -> pd.DataFrame:
        df_ = df_.loc[:, ~df_.columns.duplicated()]
        return df_

    def data_remove_dupl(self, df_:pd.DataFrame) -> pd.DataFrame:
        df_ = df_.drop_duplicates(subset=self.list_cols_drop_dupl,
                                  keep=self.strategy_keep_when_dupl)
        return df_

    def pipeline(
        self, df_:pd.DataFrame
    ) -> pd.DataFrame:
        steps = [
            self.check_if_empty,
            self.clean_encoding_issues,
            self.strip_hidden_characters,
            self.column_names,
            self.limit_columns_to_dtypes,
            self.delete_empty_rows,
            self.completeness,
            self.replace_separators,
            self.change_dtypes,
            self.strip_all_obj_dtypes,
            self.remove_duplicated_cols,
            self.data_remove_dupl
        ]
        df_ = generic_pipeline(df_, steps)
        return df_


class DFStandardizationML(DFStandardization):

    def handle_outliers(self, df_:pd.DataFrame, method='iqr') -> pd.DataFrame:
        """
        Remove or adjust outliers using IQR or Z-score method
        Args:
            method (str): Method to use. Options: ['iqr', 'zscore']
        Returns:
            pd.DataFrame
        """
        for col in df_.select_dtypes(include=['number']).columns:
            if method == 'iqr':
                q1, q3 = df_[col].quantile([0.25, 0.75])
                iqr = q3 - q1
                lower_bound, upper_bound = q1 - 1.5 * iqr, q3 + 1.5 * iqr
                df_[col] = np.clip(df_[col], lower_bound, upper_bound)
            elif method == 'zscore':
                mean, std = df_[col].mean(), df_[col].std()
                df_[col] = np.clip(df_[col], mean - 3 * std, mean + 3 * std)
        return df_

    def scale_numeric_data(self, df_:pd.DataFrame, method='minmax') -> pd.DataFrame:
        """
        Normalize or scale numerical data
        Args:
            method (str): Method to use. Options: ['minmax', 'standard']
        Returns:
            pd.DataFrame
        """
        scaler = MinMaxScaler() if method == 'minmax' else StandardScaler()
        num_cols = df_.select_dtypes(include=['number']).columns
        df_[num_cols] = scaler.fit_transform(df_[num_cols])

    def pipeline_ml(
        self, df_:pd.DataFrame,
        cols_from_case:Optional[str]=None, cols_to_case:Optional[str]=None,
        list_cols_drop_dupl:List[str]=None, str_fmt_dt:str='YYYY-MM-DD',
        method_handle_outliers:str='iqr',
        method_scale_numeric_data:str='minmax'
    ) -> pd.DataFrame:
        """
        Pipepline for dataframe cleaning
        Args:
            df_ (pandas.DataFrame): DataFrame to clean
            dict_dtypes (dict): Dictionary of columns and dtypes
            list_cols_dt (list): List of columns to change
            str_dt_fillna (str): Date fillna value
            str_data_fillna (str): Data fillna value
            str_fmt_dt (str): Date format
        Returns:
            pd.DataFrame
        """
        steps = [
            self.pipeline(df_, cols_from_case, cols_to_case, list_cols_drop_dupl, str_fmt_dt),
            self.handle_outliers(df_, method_handle_outliers),
            self.scale_numeric_data(df_, method_scale_numeric_data)
        ]
        return generic_pipeline(df_, steps)
