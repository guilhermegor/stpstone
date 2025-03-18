### CONNECTING TO MYSQL DATABASE ###

import pymysql
import json
import pandas as pd
from sqlalchemy import create_engine


class MySQLDatabase:

    def read_sql(self, host, port, database, user, password, query, timeout=7200):
        """
        DOCSTRING:
        INPUTS: Database connection parameters and SQL query
        OUTPUTS: pandas DataFrame containing the query results
        """
        # creating connection object
        conn = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}',
                             connect_args={'connect_timeout': timeout})
        # return SQL result as DataFrame
        return pd.read_sql(query, con=conn)

    def engine(self, host, port, database, user, password, query, bl_insert_db=False):
        """
        DOCSTRING:
        RUN SQL QUERIES FOR MYSQL DATABASE
        INPUTS: Database connection parameters and SQL query
        OUTPUTS: Query result (if SELECT) or success message (if INSERT/UPDATE/DELETE)
        """
        try:
            #   making MySQL connection
            conn = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database
            )
            cur = conn.cursor()
            cur.execute(query)
            #   if it's not an insert query, fetch the results
            if not bl_insert_db:
                list_rows = cur.fetchall()
            else:
                conn.commit()
                list_rows = True
        except pymysql.MySQLError as e:
            return f"Error while executing query: {e}"
        finally:
            #   close database connection
            if conn:
                cur.close()
                conn.close()
        return list_rows

    def insert_data(self, host, port, database, user, password, table_name, from_json=True,
                    json_data_path=None, json_mem=None, timeout_secs=3):
        """
        DOCSTRING: INSERT DATA INTO MYSQL (FROM JSON FILE OR MEMORY)
        INPUTS: Database connection parameters, table name, JSON data (from file or memory)
        OUTPUTS: Insertion status message
        """
        if from_json:
            #   defining record list from json
            if json_data_path:
                with open(json_data_path, 'r') as file:
                    record_list = json.load(file)
            elif json_mem:
                record_list = json_mem
            else:
                return 'Error: Neither JSON file path nor in-memory data provided.'
            #   create a SQL insert string
            if isinstance(record_list, list):
                # get the column names from the first record
                columns = list(record_list[0].keys())
                sql_string = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES "
                # insert data into database
                values_list = []
                for record_dict in record_list:
                    values = [
                        f"'{str(val).replace('\'', '\'\'')}'" if isinstance(val, str)
                        else str(val)
                        for val in record_dict.values()
                    ]
                    values_list.append(f"({', '.join(values)})")

                sql_string += ', '.join(values_list) + ';'
            # perform insertion
            try:
                conn = pymysql.connect(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    database=database
                )
                cur = conn.cursor()
                cur.execute(sql_string)
                conn.commit()
                cur.close()
                conn.close()
                return 'OK'
            except pymysql.MySQLError as e:
                return f"Error while inserting data: {e}"
        else:
            return 'Error: Data insertion was not initiated through JSON.'
