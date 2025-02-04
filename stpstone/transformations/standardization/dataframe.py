### DATAFRAME STANDARDIZATION ###

# pypi.org
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from sklearn.preprocessing import MinMaxScaler, StandardScaler
# local libs
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.parsers.str import StrHandler
from stpstone.utils.pipelines.generic import generic_pipeline
from stpstone.parsers.lists import HandlingLists


class DFStandardization:

    def check_if_empty(self, df_:pd.DataFrame) -> pd.DataFrame:
        """
        Check if DataFrame is empty
        Args:
            - df_ (pandas.DataFrame): DataFrame to check
        Returns:
            pd.DataFrame
        """
        if df_.empty == True:
            raise ValueError('DataFrame is empty')
        return df_
    
    def column_names(
        self, 
        df_:pd.DataFrame, 
        cols_from_case:Optional[str]=None, 
        cols_to_case:Optional[str]=None
    ) -> List[str]:
        """
        Converts column names between different naming conventions:
            - camelCase
            - PascalCase
            - snake_case
            - kebab-case
            - UPPER_SNAKE_CASE
        Args:
            - cols_from_case (str): Current case of the string. Options: ['camel', 'pascal', 'snake', 'kebab', 'upper_snake']
            - cols_to_case (str): Desired case of the string. Options: ['camel', 'pascal', 'snake', 'kebab', 'upper_snake']
        Returns:
            list
        """
        if all([x is not None for x in [cols_from_case, cols_to_case]]):
            list_cols = [
                StrHandler().convert_case(col_, cols_from_case, cols_to_case) 
                for col_ in df_.columns
            ]
        else:
            list_cols = df_.columns
        df_.columns = list_cols
        print(f'DF AFTER 1: \n{df_}')
        return df_

    def completeness(
        self, df_:pd.DataFrame, list_cols_dt:List[str], str_dt_fillna:str='2100-12-31', 
        str_data_fillna:str='-1'
    ) -> pd.DataFrame:
        """
        Completeness - ensure that all required fields have valid (non-null) entries
        Args:
            - df_ (pandas.DataFrame): DataFrame to fill
            - list_cols_dt (list): List of date type columns
            - str_dt_fillna (str): Date fillna value
            - str_data_fillna (str): Data fillna value
        Returns:
            pd.DataFrame
        """
        list_cols = [col_ for col_ in list(df_.columns) if col_ not in list_cols_dt]
        print(list_cols)
        if list_cols_dt is not None:
            df_[list_cols_dt] = df_[list_cols_dt].fillna(str_dt_fillna)
        df_[list_cols] = df_[list_cols].fillna(str_data_fillna)
        print(f'DF AFTER 2: \n{df_}')
        return df_

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
        print(f'DICT DTYPES: {dict_dtypes}')
        print(f'LIST COLS DT: {list_cols_dt}')
        df_ = df_.astype(dict_dtypes, errors=errors)
        for col_ in list_cols_dt:
            df_[col_] = [DatesBR().str_date_to_datetime(d, str_fmt_dt) for d in df_[col_]]
        list_cols_dtypes = HandlingLists().extend_lists(list(dict_dtypes.keys()), list_cols_dt)
        if any([col_ not in list(dict_dtypes.keys()) for col_ in list_cols_dtypes]):
            for col_ in list(dict_dtypes.keys()):
                dict_dtypes[col_] = str
        print(f'DF AFTER 3: \n{df_}')
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
        print(f'DF AFTER 4: \n{df_}')
        return df_
    
    def cols_remove_dupl(self, df_:pd.DataFrame) -> pd.DataFrame:
        """
        Columns names remove duplicates
        Args:
            - df_ (pandas.DataFrame): DataFrame to remove
        Returns:
            pd.DataFrame
        """
        df_ = df_.loc[:, ~df_.columns.duplicated()]
        print(f'DF AFTER 5: \n{df_}')
        return df_

    def data_remove_dupl(self, df_:pd.DataFrame, list_cols:Optional[List[str]]=None) -> pd.DataFrame:
        """
        Remove duplicates from specific columns in a DataFrame
        Args:
            - df_ (pandas.DataFrame): DataFrame to remove
            - list_cols (list): List of columns to remove duplicates
        Returns:
            pd.DataFrame
        """
        if list_cols is not None:
            df_ = df_.drop_duplicates(subset=list_cols)
        print(f'DF AFTER 6: \n{df_}')
        return df_

    def _pipeline(
        self, df_:pd.DataFrame, dict_dtypes:Dict[str, Any], list_cols_dt:List[str], 
        cols_from_case:Optional[str]=None, cols_to_case:Optional[str]=None, 
        list_cols_remove_dupl:List[str]=None, str_fmt_dt:str='YYYY-MM-DD'
    ) -> pd.DataFrame:
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
        print(f'DF BEFORE: \n{df_}')
        # steps = [
        #     lambda df_: self.column_names(df_, cols_from_case, cols_to_case),
        #     lambda df_: self.change_dtypes(df_, dict_dtypes, list_cols_dt, str_fmt_dt),
        #     lambda df_: self.strip_all_obj_dtypes(df_, list_cols_dt),
        #     lambda df_: self.completeness(df_, list_cols_dt),
        #     self.cols_remove_dupl,
        #     lambda df_: self.data_remove_dupl(df_, list_cols_remove_dupl)
        # ]
        df_ = self.check_if_empty(df_)
        df_ = self.column_names(df_, cols_from_case, cols_to_case)
        df_ = self.completeness(df_, list_cols_dt)
        df_ = self.change_dtypes(df_, dict_dtypes, list_cols_dt, str_fmt_dt)
        df_ = self.strip_all_obj_dtypes(df_, list_cols_dt)
        df_ = self.cols_remove_dupl(df_)
        df_ = self.data_remove_dupl(df_, list_cols_remove_dupl)
        # df_ = generic_pipeline(df_, steps)
        print(f'DF FINAL: \n{df_}')
        raise Exception('BREAK')
        return df_


class DFStandardizationML(DFStandardization):

    def handle_outliers(self, df_:pd.DataFrame, method='iqr'):
        """
        Remove or adjust outliers using IQR or Z-score method
        Args:
            - method (str): Method to use. Options: ['iqr', 'zscore']
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

    def scale_numeric_data(self, df_:pd.DataFrame, method='minmax'):
        """
        Normalize or scale numerical data
        Args:
            - method (str): Method to use. Options: ['minmax', 'standard']
        Returns:
            pd.DataFrame
        """
        scaler = MinMaxScaler() if method == 'minmax' else StandardScaler()
        num_cols = df_.select_dtypes(include=['number']).columns
        df_[num_cols] = scaler.fit_transform(df_[num_cols])
    
    def _pipeline_ml(
        self, df_:pd.DataFrame, dict_dtypes:Dict[str, Any], list_cols_dt:List[str], 
        cols_from_case:Optional[str]=None, cols_to_case:Optional[str]=None, 
        list_cols_remove_dupl:List[str]=None, str_fmt_dt:str='YYYY-MM-DD', 
        method_handle_outliers:str='iqr', 
        method_scale_numeric_data:str='minmax'
    ) -> pd.DataFrame:
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
        steps = [
            self._pipeline(df_, cols_from_case, cols_to_case, dict_dtypes, list_cols_dt, 
                         list_cols_remove_dupl, str_fmt_dt),
            lambda df_: self.handle_outliers(df_, method_handle_outliers),
            lambda df_: self.scale_numeric_data(df_, method_scale_numeric_data)
        ]
        return generic_pipeline(df_, steps)