### CONNECTING TO SQLITE DATABASE ###

import sqlite3
from logging import Logger
import pandas as pd
from typing import List, Dict, Any, Optional
from stpstone.cals.handling_dates import DatesBR
from stpstone.handling_data.json import JsonFiles
from stpstone.loggs.create_logs import CreateLog


class SQLiteDB:

    def __init__(self, db_path:str, logger:Optional[Logger]=None) -> None:
        '''
        DOCSTRING: INITIALIZES THE CONNECTION TO THE SQLITE DATABASE
        INPUTS: DB_PATH
        OUTPUTS: -
        '''
        self.db_path = db_path
        self.logger = logger
        self.conn:sqlite3.Connection = sqlite3.connect(self.db_path)
        self.cursor:sqlite3.Cursor = self.conn.cursor()

    def _execute(self, str_query:str) -> None:
        '''
        DOCSTRING: RUN QUERY WITH DML ACCESS
        INPUTS: QUERY
        OUTPUTS: -
        '''
        self.cursor.execute(str_query)

    def _read(self, str_query:str, dict_type_cols:Dict[str, Any], 
        list_cols_dt:Optional[List[str]], str_fmt_dt:Optional[str]) -> pd.DataFrame:
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # retrieving dataframe
        df_ = pd.read_sql_query(str_query, self.conn)
        # changing data types
        df_ = df_.astype(dict_type_cols)
        for col_ in list_cols_dt:
            df_[col_] = [DatesBR().str_date_to_datetime(d, str_fmt_dt) for d in df_[col_]]
        # return dataframe
        return df_

    def _insert(self, json_data:List[Dict[str, Any]], str_table_name:str) -> None:
        '''
        DOCSTRING: INSERTS DATA FROM A JSON OBJECT INTO A SQLITE TABLE
        INPUTS: JSON_DATA
        OUTPUTS: -
        '''
        # validate json, in order to have the same keys
        json_data = JsonFiles().normalize_json_keys(json_data)
        # sql insert statement
        list_columns = ', '.join(json_data[0].keys())
        list_data = ', '.join(['?' for _ in json_data[0]])
        str_query = f'INSERT OR IGNORE INTO {str_table_name} ' \
            + f'({list_columns}) VALUES ({list_data})'
        try:
            # insert each record
            for record in json_data:
                self.cursor.execute(str_query, tuple(record.values()))
            self.conn.commit()
            if self.logger is not None:
                CreateLog().infos(
                    self.logger, 
                    f'Succesful commit in db {self.db_path} ' 
                    + f'/ table {str_table_name}.'
                )
        except Exception as e:
            self.conn.rollback()
            self._close
            if self.logger is not None:
                CreateLog().errors(
                    self.logger, 
                    'ERROR WHILE INSERTING DATA\n'
                    + f'DB_PATH: {self.db_path}\n'
                    + f'TABLE_NAME: {str_table_name}\n'
                    + f'JSON_DATA: {json_data}\n'
                    + f'ERROR_MESSAGE: {e}'
                )
            raise Exception(
                'ERROR WHILE INSERTING DATA\n'
                + f'DB_PATH: {self.db_path}\n'
                + f'TABLE_NAME: {str_table_name}\n'
                + f'JSON_DATA: {json_data}\n'
                + f'ERROR_MESSAGE: {e}'
            )

    @property
    def _close(self) -> None:
        '''
        DOCSTRING: CLOSES THE CONNECTION TO THE DATABASE
        INPUTS: -
        OUTPUTS: -
        '''
        self.conn.close()