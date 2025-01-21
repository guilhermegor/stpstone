
### DEAL WITH PANDAS ISSUES ###

import os
import pandas as pd
from typing import List, Dict, Any
from stpstone.settings._global_slots import YAML_MICROSOFT_APPS
from stpstone.cals.handling_dates import DatesBR
from stpstone.directories_files_manag.managing_ff import DirFilesManagement
from stpstone.loggs.create_logs import CreateLog
from stpstone.microsoft_apps.excel import DealingExcel


class DealingPd(DealingExcel):
    '''
    DOCSTRING: COMMON FUNCTIONS TO DEAL WITH DATA IMPORT TO PANDAS DATAFRAMES OR TIMESERIES
    '''

    def append_df_to_Excel(self, filename, list_tup_df_sheet_name, bl_header=True, bl_index=0, 
        mode='w', label_sensitivity='internal', bl_set_sensitivity_label=False, bl_debug_mode=False):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # debug mode
        if bl_debug_mode == True:
            print('LIST_TUP_DF_SHEET_NAME: {}'.format(list_tup_df_sheet_name))
            print('FILENAME: {}'.format(filename))
            print('MODE: {}'.format(mode))
            print('BL_INDEX: {}'.format(bl_index))
        # write excel
        with pd.ExcelWriter(filename, engine='xlsxwriter', mode=mode) as writer:
            for df_, sheet_name in list_tup_df_sheet_name:
                # # checking df_
                # if df_.empty == True: continue
                # removing indexes
                if bl_index == 0:
                    df_.reset_index(drop=True, inplace=True)
                    # df_ = df_.style.hide_index()
                df_.to_excel(writer, sheet_name, index=bl_index, header=bl_header)
        # setting label
        if bl_set_sensitivity_label == True:
            self.xlsx_sensitivity_label(filename, YAML_MICROSOFT_APPS[
                'sensitivity_labels_office'], 
                label_sensitivity.capitalize())

    def export_xl(self, logger, nome_completo_xlsx_exportacao, list_tup_df_sheet_name, 
        range_colunas='A:CC', bl_adjust_layout=False, bl_debug_mode=False):
        '''
        DOCSTRING: EXPORTANDO DAFRAME PARA EXCEL
        INPUTS: LOGGER, NOME COMPLETO XLSX DE EXPOTAÇÃO, DATAFRAME DE EXPORTAÇÃO, NOME DA PLANILHA, 
            RANGE DE COLUNAS, BOOLEAN AUTOAJUSTE (DEFAULT)
        OUTPUTS: BOOLEAN
        '''
        #   exportando dataframe para excel
        DealingPd().append_df_to_Excel(
            nome_completo_xlsx_exportacao, list_tup_df_sheet_name, bl_header=True, bl_index=0, 
            bl_debug_mode=bl_debug_mode)
        #   validando se o arquivo foi exportado com sucesso
        blame_exportacao_xlsx = DirFilesManagement().object_exists(
            nome_completo_xlsx_exportacao)
        if blame_exportacao_xlsx == 'NOK':
            CreateLog().warnings(logger, 'Arquivo não salvo na rede: {}'.format(
                nome_completo_xlsx_exportacao))
            raise Exception('Arquivo {} não salvo na rede, favor validar'.format(
                nome_completo_xlsx_exportacao))
        else:
            if bl_adjust_layout == True:
                for _, plan_nome in list_tup_df_sheet_name:
                    #   corrigindo formato do xlsx exportado, e aplicando um autofit nas colunas 
                    #       de interesse
                    xla, wb = self.open_xl(nome_completo_xlsx_exportacao)
                    self.autofit_range_columns(plan_nome, range_colunas, xla, wb)
                    self.close_wb(wb)
        return blame_exportacao_xlsx

    def json_to_excel(self, json_path_name, xlsx_path_name):
        '''
        DOCSTRING: EXPORT JSON FILE TO EXCEL XLSX
        INPUTS: JSON AND XLSX COMPLETE NAME
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        '''
        pd.read_json(json_path_name).to_excel(xlsx_path_name)
        if os.path.exists(xlsx_path_name):
            return 'OK'
        else:
            return 'NOK'

    def json_normalizer(self, json_path):
        '''
        DOCSTRING: JSON NORMALIZER FROM PANDAS
        INPUTS: JSON PATH
        OUTPUTS: JSON IN DATAFRAME
        '''
        return pd.read_json(json_path)

    def settingup_pandas(self):
        '''
        DOCSTRING: CONFIGURAÇÃO BÁSICA DO PANDAS
        INPUTS: COLUNA COMO STR
        OUTPUTS: -
        '''
        # Use 3 decimal places in output display
        pd.set_option("display.precision", 3)
        # Don't wrap repr(DataFrame) across additional lines
        pd.set_option("display.expand_frame_repr", False)
        # Set max rows displayed in output to 25
        pd.set_option("display.max_rows", 25)

    def dataframe_to_dict(self, dataframe_export, orientation='records'):
        '''
        REFERENCES: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_dict.html
        DOCSTRING: EXPORT DATAFRAME TO JSON
        INPUTS: DATAFRAME TO BE EXPORTED, ORIENTATION(SPLIT, RECORDS (DEFAULT), INDEX, COLUMNS, VALUES, 
            TABLE)
        OUTPUTS: JSON
        '''
        return dataframe_export.to_dict(orient=orientation)

    def convert_datetime_columns(self, df_, list_col_date, bl_pandas_convertion=True):
        '''
        DOCSTRING: CONVERTING DATE COLUMNS TO DATETIME
        INPUTS: DATAFRAME, COLUMNS WITH DATE INFO
        OUTPUTS: DATAFRAME
        '''
        # checking wheter to covert through a pandas convertion, or resort to a excel format transformation
        #   of data in date column format
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

    def merge_dfs_into_df(self, df_1, df_2, cols_list):
        '''
        DOCSTRING: MERGING TWO DFS INTO A NEW ONE, REMOVING THEIR INTERSECTION ON A LIST OF COLUMNS
        INPUTS: TWO DATAFRAMES AND A LIST WITH COLUMNS NAMES
        OUTPUTS: DATAFRAME
        '''
        intersec_df = pd.merge(df_1, df_2, how='inner', on=cols_list)
        merge_df = pd.merge(df_2, intersec_df, how='outer',
                            on=cols_list, indicator=True)
        merge_df = merge_df.loc[merge_df._merge == 'left_only']
        merge_df.dropna(how='all', axis=1, inplace=True)
        try:
            merge_df = merge_df.drop(columns='_merge')
        except:
            pass
        for column in merge_df.columns:
            if column in cols_list:
                pass
            else:
                merge_df = merge_df.rename(
                    columns={str(column): str(column)[:-2]})
        return merge_df

    def max_chrs_per_column(df_:pd.DataFrame, dict_:dict=dict()) -> dict:
        '''
        DOCSTRING: CALCULATE THE MAXIMUM NUMBER OF CHARACTERS IN A SPECIFIED COLUMN OF A DATAFRAME
        INPUTS: DATAFRAME
        OUTPUTS: DICT
        '''
        for col_ in list(df_.columns):
            dict_[col_] = df_[col_].astype(str).str.len().max()
        return dict_

    def change_dtypes(self, df_:pd.DataFrame, dict_dtypes:Dict[str, Any], list_cols_dt:List[str], 
        str_fmt_dt:str='YYYY-MM-DD', errors:str='raise') -> pd.DataFrame:
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        if any([col_ not in list(dict_dtypes.keys()) for col_ in list_cols_dt]):
            for col_ in list(dict_dtypes.keys()):
                dict_dtypes[col_] = str
        df_ = df_.astype(dict_dtypes, errors=errors)
        for col_ in list_cols_dt:
            df_[col_] = [DatesBR().str_date_to_datetime(d, str_fmt_dt) for d in df_[col_]]
        return df_

    def strip_all_obj_dtypes(self, df_:pd.DataFrame, list_cols_dt:List[str]) -> pd.DataFrame:
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        list_cols = [col_ for col_ in list(df_.select_dtypes(['object']).columns) if col_ 
            not in list_cols_dt]
        for col_ in list_cols:
            df_[col_] = [x.strip() for x in df_[col_]]
        return df_

    def fillna_data(self, df_:pd.DataFrame, list_cols_dt:List[str], str_dt_fillna:str='2100-12-31', 
        str_data_fillna:str='-1') -> pd.DataFrame:
        '''
        DOCSTRING: FILLNA DATA/DATES
        INPUTS: DATAFRAME, LIST COLS DT, LIST COLS DATA
        OUTPUTS: DATAFRAME
        '''
        list_cols = [col_ for col_ in list(df_.columns) if col_ not in list_cols_dt]
        if list_cols_dt is not None:
            df_[list_cols_dt] = df_[list_cols_dt].fillna(str_dt_fillna)
        df_[list_cols] = df_[list_cols].fillna(str_data_fillna)
        return df_

    def pipeline_df_startup(self, df_:pd.DataFrame, dict_dtypes:Dict[str, Any], 
        list_cols_dt:List[str], str_dt_fillna:str='2100-12-31', str_data_fillna:str='-1', 
        str_fmt_dt:str='YYYY-MM-DD'):
        '''
        DOCSTRING: PIPELINE DATAFRAME STARTUP
        INPUTS: DATAFRAME, DICT DTYPES, LIST OF DATE TYPE COLUMNS, STR DT FILLNA, STR DATA FILLNA, 
            STR DATE FORMAT INPUT
        OUTPUTS:
        '''
        df_ = self.fillna_data(df_, list_cols_dt, str_dt_fillna, str_data_fillna)
        df_ = self.change_dtypes(df_, dict_dtypes, list_cols_dt, str_fmt_dt)
        df_ = self.strip_all_obj_dtypes(df_, list_cols_dt)
        return df_

    def cols_remove_dupl(self, df_):
        '''
        DOCSTRING: REMOVE DUPLICATES WITHIN COLUMNS AND KEEP JUST THE FIRST ONE
        INPUTS: DATAFRAME
        OUTPUTS: DATAFRAME
        '''
        return df_.loc[:, ~df_.columns.duplicated()]
