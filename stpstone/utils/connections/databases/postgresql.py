from logging import Logger
import os
import subprocess
from typing import Any, Dict, List, Optional

import pandas as pd
from psycopg import connect, Connection, Cursor
from psycopg.rows import dict_row
from psycopg.sql import Composable, Identifier, SQL

from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.databases.abc import ABCDatabase
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.json import JsonFiles


class PostgreSQLDB(ABCDatabase):
    def __init__(
        self,
        dbname: str,
        user: str,
        password: str,
        host: str,
        port: int,
        str_schema: str = "public",
        logger: Optional[Logger] = None,
    ) -> None:
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
            "port": self.port,
        }
        try:
            self.conn: Connection = connect(**self.dict_db_config, row_factory=dict_row)
            self.cursor: Cursor = self.conn.cursor()
            self.execute("SELECT 1")
        except Exception as e:
            CreateLog().error(self.logger, f"Error connecting to database: {str(e)}")
        self.execute(SQL("SET search_path TO {}").format(Identifier(self.str_schema)))

    def execute(self, str_query: str | Composable) -> None:
        if not isinstance(str_query, (str, Composable)):
            raise TypeError("Query must be string or Composable")
        self.cursor.execute(str_query)

    def read(
        self,
        str_query: str,
        dict_type_cols: Optional[Dict[str, Any]] = None,
        list_cols_dt: Optional[List[str]] = None,
        str_fmt_dt: Optional[str] = None,
    ) -> pd.DataFrame:
        # retrieving dataframe
        self.cursor.execute(str_query)
        data = self.cursor.fetchall()
        df_ = pd.DataFrame(data)

        # changing data types
        if all([x is not None for x in [dict_type_cols, list_cols_dt, str_fmt_dt]]):
            df_ = df_.astype(dict_type_cols)
            for col_ in list_cols_dt:
                df_[col_] = [
                    DatesBR().str_date_to_datetime(d, str_fmt_dt) for d in df_[col_]
                ]
        return df_

    def insert(
        self,
        json_data: List[Dict[str, Any]],
        str_table_name: str,
        bl_insert_or_ignore: bool = False,
    ) -> None:
        # validate json, in order to have the same keys
        json_data = JsonFiles().normalize_json_keys(json_data)

        if not json_data:
            return

        columns = json_data[0].keys()
        placeholders = SQL(",").join([SQL("%s") for _ in columns])
        table = Identifier(str_table_name)
        cols = SQL(",").join(map(Identifier, columns))

        if bl_insert_or_ignore:
            query = SQL("""
                INSERT INTO {table} ({cols})
                VALUES ({placeholders})
                ON CONFLICT DO NOTHING
            """).format(table=table, cols=cols, placeholders=placeholders)
        else:
            query = SQL("""
                INSERT INTO {table} ({cols})
                VALUES ({placeholders})
            """).format(table=table, cols=cols, placeholders=placeholders)

        try:
            # Convert each record to a tuple in the same order as columns
            records = [tuple(record[col] for col in columns) for record in json_data]

            # Execute with all records at once
            self.cursor.executemany(query, records)
            self.conn.commit()

            if self.logger is not None:
                CreateLog().info(
                    self.logger,
                    f"Successful commit in db {self.dict_db_config['dbname']} "
                    + f"/ table {str_table_name}.",
                )
        except Exception as e:
            self.conn.rollback()
            self.close()
            if self.logger is not None:
                CreateLog().error(
                    self.logger,
                    "ERROR WHILE INSERTING DATA\n"
                    + f"DB_CONFIG: {self.dict_db_config}\n"
                    + f"TABLE_NAME: {str_table_name}\n"
                    + f"JSON_DATA: {json_data}\n"
                    + f"ERROR_MESSAGE: {e}",
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

    def backup(self, str_backup_dir: str, str_bkp_name: Optional[str] = None) -> str:
        try:
            os.makedirs(str_backup_dir, exist_ok=True)
            backup_file = os.path.join(str_backup_dir, str_bkp_name)
            env = os.environ.copy()
            env["PGPASSWORD"] = self.password

            command = [
                "pg_dump",
                "-h",
                self.host,
                "-p",
                str(self.port),
                "-U",
                self.user,
                "-F",
                "c",
                "-b",
                "-f",
                backup_file,
                self.dbname,
            ]
            subprocess.run(command, check=True, env=env)
            return f"Backup successful! File saved at: {backup_file}"
        except subprocess.CalledProcessError as e:
            return f"Backup failed: {e}"
        except Exception as e:
            return f"An error occurred: {e}"
