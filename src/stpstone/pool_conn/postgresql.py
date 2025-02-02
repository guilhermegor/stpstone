import os
import subprocess
from logging import Logger
from typing import Any, Dict, List, Optional

import pandas as pd
import psycopg2
from psycopg2.extensions import connection as Connection
from psycopg2.extensions import cursor as Cursor
from psycopg2.extras import execute_values

from stpstone.cals.handling_dates import DatesBR
from stpstone.handling_data.json import JsonFiles
from stpstone.loggs.create_logs import CreateLog


class PostgreSQLDB:
    def __init__(
        self,
        dbname: str,
        user: str,
        password: str,
        host: str,
        port: int,
        str_schema: str = 'public',
        logger: Optional[Logger] = None,
    ) -> None:
        """
        DOCSTRING: INITIALIZES THE CONNECTION TO THE POSTGRESQL DATABASE
        INPUTS:
            - DBNAME:STR
            - USER:STR
            - PASSWORD:STR
            - HOST:STR
            - PORT:INT
            - SCHEMA:STR
            - LOGGER:OPTIONAL[LOGGER]
        OUTPUTS: -
        """
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.str_schema = str_schema
        self.logger = logger
        self.dict_db_config = {
            'dbname': self.dbname,
            'user': self.user,
            'password': self.password,
            'host': self.host,
            'port': self.port,
        }
        self.conn: Connection = psycopg2.connect(**self.dict_db_config)
        self.cursor: Cursor = self.conn.cursor()
        self._execute(f"SET search_path TO '{self.str_schema}';")

    def _execute(self, str_query: str) -> None:
        """
        DOCSTRING: RUN QUERY WITH DML ACCESS
        INPUTS: QUERY
        OUTPUTS: -
        """
        self.cursor.execute(str_query)

    def _read(
        self,
        str_query: str,
        dict_type_cols: Optional[Dict[str, Any]] = None,
        list_cols_dt: Optional[List[str]] = None,
        str_fmt_dt: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        DOCSTRING: RUN QUERY AND RETURN RESULTS AS DATAFRAME
        INPUTS: QUERY, DICT_TYPE_COLS, LIST_COLS_DT, STR_FMT_DT
        OUTPUTS: Pandas DataFrame
        """
        # retrieving dataframe
        df_ = pd.read_sql_query(str_query, self.conn)
        # changing data types
        if all(
            [x is not None for x in [dict_type_cols, list_cols_dt, str_fmt_dt]]
        ):
            df_ = df_.astype(dict_type_cols)
            for col_ in list_cols_dt:
                df_[col_] = [
                    DatesBR().str_date_to_datetime(d, str_fmt_dt)
                    for d in df_[col_]
                ]
        # return dataframe
        return df_

    def _insert(
        self,
        json_data: List[Dict[str, Any]],
        str_table_name: str,
        bl_insert_or_ignore: bool = False,
    ) -> None:
        """
        DOCSTRING: INSERTS DATA FROM A JSON OBJECT INTO A POSTGRESQL TABLE
        INPUTS: JSON_DATA
        OUTPUTS: -
        """
        # validate json, in order to have the same keys
        json_data = JsonFiles().normalize_json_keys(json_data)
        # sql insert statement
        list_columns = ', '.join(json_data[0].keys())
        list_placeholders = ', '.join(['%s' for _ in json_data[0]])
        if bl_insert_or_ignore == True:
            str_query = (
                f'INSERT INTO {str_table_name} ({list_columns}) VALUES '
                + f'({list_placeholders}) ON CONFLICT DO NOTHING'
            )
        else:
            str_query = (
                f'INSERT INTO {str_table_name} ({list_columns}) VALUES '
                + f'({list_placeholders})'
            )
        try:
            execute_values(
                self.cursor,
                str_query,
                [tuple(record.values()) for record in json_data],
            )
            self.conn.commit()
            if self.logger is not None:
                CreateLog().infos(
                    self.logger,
                    f'Successful commit in db {self.dict_db_config["dbname"]} '
                    + f'/ table {str_table_name}.',
                )
        except Exception as e:
            self.conn.rollback()
            self._close
            if self.logger is not None:
                CreateLog().errors(
                    self.logger,
                    'ERROR WHILE INSERTING DATA\n'
                    + f'DB_CONFIG: {self.dict_db_config}\n'
                    + f'TABLE_NAME: {str_table_name}\n'
                    + f'JSON_DATA: {json_data}\n'
                    + f'ERROR_MESSAGE: {e}',
                )
            raise Exception(
                'ERROR WHILE INSERTING DATA\n'
                + f'DB_CONFIG: {self.dict_db_config}\n'
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

    def _bkp_db(self, str_backup_dir: str, str_bkp_name: str = None) -> str:
        """
        DOCSTRING: BACKUP DATABASE - NEED PG_DUMP TO BE CONFIGURED WITHIN ENVIRONMENT VARIABLES
        INPUTS:
            - BACKUP_DIR (STR): DIRECTORY WHERE THE BACKUP FILE WILL BE SAVED
        OUTPUTS: STR: PATH TO THE BACKUP FILE, OR AN ERROR MESSAGE.
        """
        try:
            # ensure the backup directory exists
            os.makedirs(str_backup_dir, exist_ok=True)
            # generate the backup file name
            backup_file = os.path.join(str_backup_dir, str_bkp_name)
            # set the environment variable for self.password authentication
            env = os.environ.copy()
            env['PGPASSWORD'] = self.password
            # run the pg_dump command
            command = [
                'pg_dump',
                '-h',
                self.host,
                '-p',
                str(self.port),
                '-U',
                self.user,
                '-F',
                'c',  # custom format
                '-b',  # include large objects
                '-f',
                backup_file,  # output file
                self.dbname,
            ]
            subprocess.run(command, check=True, env=env)
            return f'Backup successful! File saved at: {backup_file}'
        except subprocess.CalledProcessError as e:
            return f'Backup failed: {e}'
        except Exception as e:
            return f'An error occurred: {e}'
