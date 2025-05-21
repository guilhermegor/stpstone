import psycopg2
import subprocess
import os
import pandas as pd
from psycopg2.extensions import connection as Connection, cursor as Cursor
from psycopg2.extras import execute_values
from logging import Logger
from typing import List, Dict, Any, Optional
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.parsers.json import JsonFiles
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.connections.databases.abc import DatabaseConnection


class PostgreSQLDB(DatabaseConnection):

    def __init__(self, dbname:str, user:str, password:str, host:str, port:int,
                 str_schema:str="public", logger:Optional[Logger]=None) -> None:
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.str_schema = str_schema
        self.logger = logger
        self.dict_db_config = {
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "port": self.port
        }
        self.conn: Connection = psycopg2.connect(**self.dict_db_config)
        self.cursor: Cursor = self.conn.cursor()
        self.execute(f"SET search_path TO '{self.str_schema}';")

    def execute(self, str_query:str) -> None:
        self.cursor.execute(str_query)

    def read(self, str_query:str, dict_type_cols:Optional[Dict[str, Any]]=None,
              list_cols_dt:Optional[List[str]]=None, str_fmt_dt:Optional[str]=None) -> pd.DataFrame:
        # retrieving dataframe
        df_ = pd.read_sql_query(str_query, self.conn)
        # changing data types
        if all([x is not None for x in [dict_type_cols, list_cols_dt, str_fmt_dt]]):
            df_ = df_.astype(dict_type_cols)
            for col_ in list_cols_dt:
                df_[col_] = [DatesBR().str_date_to_datetime(d, str_fmt_dt) for d in df_[col_]]
        # return dataframe
        return df_

    def insert(self, json_data:List[Dict[str, Any]], str_table_name:str,
        bl_insert_or_ignore:bool=False) -> None:
        # validate json, in order to have the same keys
        json_data = JsonFiles().normalize_json_keys(json_data)
        # sql insert statement
        list_columns = ", ".join(json_data[0].keys())
        list_placeholders = ", ".join(["%s" for _ in json_data[0]])
        if bl_insert_or_ignore == True:
            str_query = f"INSERT INTO {str_table_name} ({list_columns}) VALUES " \
                + f"({list_placeholders}) ON CONFLICT DO NOTHING"
        else:
            str_query = f"INSERT INTO {str_table_name} ({list_columns}) VALUES " \
                + f"({list_placeholders})"
        try:
            execute_values(
                self.cursor,
                str_query,
                [tuple(record.values()) for record in json_data]
            )
            self.conn.commit()
            if self.logger is not None:
                CreateLog().info(
                    self.logger,
                    f"Successful commit in db {self.dict_db_config["dbname"]} "
                    + f"/ table {str_table_name}."
                )
        except Exception as e:
            self.conn.rollback()
            self.close
            if self.logger is not None:
                CreateLog().error(
                    self.logger,
                    "ERROR WHILE INSERTING DATA\n"
                    + f"DB_CONFIG: {self.dict_db_config}\n"
                    + f"TABLE_NAME: {str_table_name}\n"
                    + f"JSON_DATA: {json_data}\n"
                    + f"ERROR_MESSAGE: {e}"
                )
            raise Exception(
                "ERROR WHILE INSERTING DATA\n"
                + f"DB_CONFIG: {self.dict_db_config}\n"
                + f"TABLE_NAME: {str_table_name}\n"
                + f"JSON_DATA: {json_data}\n"
                + f"ERROR_MESSAGE: {e}"
            )

    def close(self) -> None:
        self.conn.close()

    def backup(self, str_backup_dir:str, str_bkp_name:str=None) -> str:
        try:
            # ensure the backup directory exists
            os.makedirs(str_backup_dir, exist_ok=True)
            # generate the backup file name
            backup_file = os.path.join(str_backup_dir, str_bkp_name)
            env = os.environ.copy()
            env["PGPASSWORD"] = self.password
            # run the pg_dump command
            command = [
                "pg_dump",
                "-h", self.host,
                "-p", str(self.port),
                "-U", self.user,
                "-F", "c",  # custom format
                "-b",       # include large objects
                "-f", backup_file,  # output file
                self.dbname
            ]
            subprocess.run(command, check=True, env=env)
            return f"Backup successful! File saved at: {backup_file}"
        except subprocess.CalledProcessError as e:
            return f"Backup failed: {e}"
        except Exception as e:
            return f"An error occurred: {e}"