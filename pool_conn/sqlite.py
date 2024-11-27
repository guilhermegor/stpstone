### CONNECTING TO SQLITE DATABASE ###

import sqlite3
import json
import pandas as pd
from sqlalchemy import create_engine


class SQLiteDatabase:

    def read_sql(self, db_name, query):
        '''
        DOCSTRING: READ SQL QUERY RESULT FROM SQLITE
        INPUTS: DATABASE NAME AND QUERY TO EXECUTE
        OUTPUTS: DATAFRAME CONTAINING THE RESULT OF THE QUERY
        '''
        # creating connection object
        conn = create_engine(f'sqlite:///{db_name}')
        # return sql query result as DataFrame
        return pd.read_sql(query, con=conn)

    def engine(self, db_name, query, bl_insert_db=False):
        '''
        DOCSTRING: RUN SQL QUERIES FOR SQLITE DATABASE
        INPUTS: DATABASE NAME AND SQL QUERY
        OUTPUTS: DATA (IF SELECT QUERY) OR SUCCESS STATUS (FOR NON-SELECT QUERIES)
        '''
        try:
            # making sqlite connection
            conn = sqlite3.connect(db_name)
            cur = conn.cursor()
            cur.execute(query)
            # if not an insert query, fetch the results
            if not bl_insert_db:
                list_rows = cur.fetchall()
            else:
                conn.commit()
                list_rows = True
        except sqlite3.Error as e:
            return f"Error while executing query: {e}"
        finally:
            # close database connection
            if conn:
                cur.close()
                conn.close()
        return list_rows

    def insert_data(self, db_name, table_name, from_json=True, json_data_path=None, json_mem=None):
        '''
        DOCSTRING: INSERT DATA INTO SQLITE (FROM JSON FILE OR MEMORY)
        INPUTS: DATABASE NAME, TABLE NAME, JSON DATA (FROM FILE OR MEMORY)
        OUTPUTS: STATUS OF INSERTION
        '''
        if from_json:
            # defining record list from json
            if json_data_path:
                with open(json_data_path, 'r') as file:
                    record_list = json.load(file)
            elif json_mem:
                record_list = json_mem
            else:
                return 'Error: Neither JSON file path nor in-memory data provided.'

            # create an SQL insert string
            if isinstance(record_list, list):
                # get the column names from the first record
                columns = record_list[0].keys()
                sql_string = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES "

                # insert data to database
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
                conn = sqlite3.connect(db_name)
                cur = conn.cursor()
                cur.execute(sql_string)
                conn.commit()
                cur.close()
                conn.close()
                return 'OK'
            except sqlite3.Error as e:
                return f"Error while inserting data: {e}"
        else:
            return 'Error: Data insertion was not initiated through JSON.'
