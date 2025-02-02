### CONNECTING TO SQLITE DATABASE ###

import sqlite3
from logging import Logger
from typing import Any, Dict, List, Optional

import backoff
import pandas as pd

from stpstone.cals.handling_dates import DatesBR
from stpstone.handling_data.json import JsonFiles
from stpstone.handling_data.pd import DealingPd
from stpstone.loggs.create_logs import CreateLog


class SQLiteDB:
    def __init__(self, db_path: str, logger: Optional[Logger] = None) -> None:
        """
        DOCSTRING: INITIALIZES THE CONNECTION TO THE SQLITE DATABASE
        INPUTS: DB_PATH
        OUTPUTS: -
        """
        self.db_path = db_path
        self.logger = logger
        self.conn: sqlite3.Connection = sqlite3.connect(self.db_path)
        self.cursor: sqlite3.Cursor = self.conn.cursor()

    @backoff.on_exception(
        backoff.constant,
        sqlite3.OperationalError,
        interval=10,
        max_tries=20,
    )
    def _execute(self, str_query: str) -> None:
        """
        DOCSTRING: RUN QUERY WITH DML ACCESS
        INPUTS: QUERY
        OUTPUTS: -
        """
        self.cursor.execute(str_query)

    @backoff.on_exception(
        backoff.constant,
        sqlite3.OperationalError,
        interval=10,
        max_tries=20,
    )
    def _read(
        self,
        str_query: str,
        dict_dtypes: Dict[str, Any],
        list_cols_dt: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # retrieving dataframe
        df_ = pd.read_sql_query(str_query, self.conn)
        if df_.empty == False:
            #   changing data types
            df_ = DealingPd().pipeline_df_startup(
                df_, dict_dtypes, list_cols_dt
            )
        # return dataframe
        return df_

    @backoff.on_exception(
        backoff.constant,
        sqlite3.OperationalError,
        interval=10,
        max_tries=20,
    )
    def _insert(
        self,
        json_data: List[Dict[str, Any]],
        str_table_name: str,
        bl_insert_or_ignore: bool = False,
    ) -> None:
        """
        DOCSTRING: INSERTS DATA FROM A JSON OBJECT INTO A SQLITE TABLE
        INPUTS: JSON_DATA
        OUTPUTS: -
        """
        # validate json, in order to have the same keys
        json_data = JsonFiles().normalize_json_keys(json_data)
        # sql insert statement
        list_columns = ', '.join(json_data[0].keys())
        list_data = ', '.join(['?' for _ in json_data[0]])
        if bl_insert_or_ignore == True:
            str_query = (
                f'INSERT OR IGNORE INTO {str_table_name} '
                + f'({list_columns}) VALUES ({list_data})'
            )
        else:
            str_query = (
                f'INSERT INTO {str_table_name} '
                + f'({list_columns}) VALUES ({list_data})'
            )
        try:
            # insert each record
            for record in json_data:
                self.cursor.execute(str_query, tuple(record.values()))
            self.conn.commit()
            if self.logger is not None:
                CreateLog().infos(
                    self.logger,
                    f'Succesful commit in db {self.db_path} '
                    + f'/ table {str_table_name}.',
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
                    + f'ERROR_MESSAGE: {e}',
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
        """
        DOCSTRING: CLOSES THE CONNECTION TO THE DATABASE
        INPUTS: -
        OUTPUTS: -
        """
        self.conn.close()
