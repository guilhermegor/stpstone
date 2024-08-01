### LIB TO HANDLE POSTGRE DATABASES ###

import psycopg2
import json
from sqlalchemy import create_engine
import pandas as pd


class PostgreSQL:

    def read_sql(self, host, port, database, user, password, query, timeout=7200):
        '''
        REFERENCES: https://www.cdata.com/kb/tech/postgresql-python-pandas.rst
        DOCSTRING:
        INPUTS:
        OUTPUTS: DATAFRAME PANDAS
        '''
        # creating connection object
        conn = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
            user, password, host, port, database
        ), connect_args={'connect_timeout': timeout})
        # return sql
        return pd.read_sql(query, con=conn)

    def engine(self, host, port, database, user, password, query, bl_insert_db=False):
        '''
        REFERENCES: https://www.postgresqltutorial.com/postgresql-python/insert/
        DOCSTRING: RUN SQL QUERIES FROM POSTGRE WITH PSYCOPG2
        INPUTS: DBNAME AND SQL QUERY
        OUTPUTS: STRING
        '''
        try:
            #   making postgresql connection
            conn = psycopg2.connect(user=user,
                                    password=password,
                                    host=host,
                                    port=port,
                                    database=database)
            cur = conn.cursor()
            cur.execute(query)
            #   evaluating wheter the query is for insertion or not
            if bl_insert_db == False:
                list_rows = cur.fetchall()
            else:
                conn.commit()
                list_rows = True
        except (Exception, psycopg2.Error) as error:
            return 'Error while fetching data from PostgreSQL: {}'.format(error)
        finally:
            #   close database connection
            if conn:
                cur.close()
                conn.close()
        return list_rows

    def insert_data(self, dbname, username, password, host, table_name, from_json=True,
                    json_data_path=None, json_mem=None, timeout_secs=3):
        '''
        DOCSTRING: INSERT DATA INTO POSTGRE (THAT WOULD BE A JSON FILE AS A LIST OF DICTS)
        REFERENCE: https://kb.objectrocket.com/postgresql/insert-json-data-into-postgresql-using-python-part-2-1248
        INPUTS: DATABASE NAME, USERNAME, PASSWORD, HOST, TABLENAME, FROM JSON (BOOLEAN), 
            JSON DATA PATH (LIST OF DICTS), TIMEOUT (IN SECONDS)
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        '''
        if from_json == True:
            # defining record list from json
            if json_data_path:
                record_list = json.load(json_data_path)
            elif json_mem:
                record_list = json_mem
            else:
                return 'NOK/NOR JSON DATA PATH, NOR JSON MEMORY WERE DEFINED'
            # create a SQL insert string
            if type(record_list) == list:
                # get the column names from the first record
                columns = [list(x.keys()) for x in record_list[0]]
                # insert data to database
                for i, record_dict in enumerate(record_list):
                    # declare empty list for values
                    values = list()
                    # append each value to a new list of values
                    for col_names, val in record_dict.items():
                        # postgre strings must be enclosed with single quotes
                        if type(val) == str:
                            val = val.replace("'", "''")
                            val = "'" + val + "'"
                        values += [str(val)]
                    # join the list of values and enclose record in parenthesis
                    sql_string += "(" + ', '.join(values) + "),\n"
                # remove the last comma and end statement with a semicolon
                sql_string = sql_string[:-2] + ";"
                # concatenate the SQL string
                sql_string = 'INSERT INTO {} ({})\nVALUES {}'.format(
                    table_name, ', '.join(columns), sql_string)
            # create cursor
            try:
                # declaring a new PostgreSQL connection object
                conn = psycopg2.connect(
                    dbname=dbname, user=username, password=password, host=host,
                    connection_timeout=timeout_secs)
                cur = conn.cursor()
            except (Exception, psycopg2.Error):
                # close the connection
                conn = None
                cur = None
            # performe insertion
            if cur != None:
                try:
                    cur.execute(sql_string)
                    conn.commit()
                    cur.close()
                    conn.close()
                    return 'OK'
                except:
                    conn.rollback()
                    cur.close()
                    conn.close()
                    return 'NOK/QUERY ERROR'
            else:
                cur.close()
                conn.close()
                return 'NOK/CONN ERROR'
        else:
            return 'NOK/NOT ESTABLISHED INSERTION THROUGH PROCESS RATHER THAN JSON'
