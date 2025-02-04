### CONNECTION TO OMSDB DATABASE ###

import pandas as pd
from pyodbc import connect


class SqlServerDB:

    def db_connection(self, driver_sql, server, port, database, user_id, password, timeout=7200):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # creating connection string
        if driver_sql == '{SQL Server}':
            str_conex = r'Driver={};Server={};Database={};U={};PWD={}'.format(
                driver_sql, server, database, user_id, password)
        elif driver_sql == '{ODBC Driver 17 for SQL Server}':
            str_conex = r'Driver={};Server='.format(driver_sql) \
                + server \
                + ';PORT={};Database='.format(port) \
                + database \
                + ';Trusted_Connection=no;Encrypt=yes;TrustServerCertificate=no;UID=' \
                + user_id + ';PWD=' + password
        else:
            raise Exception(
                'DRIVER SQL NOT IDENTIFIED: {}'.format(driver_sql))
        # retrieve connection object to be used in pandas dataframe
        return connect(str_conex, autocommit=True, timeout=timeout)

    def read_sql(self, driver_sql, server, port, database, user_id, password, query, timeout=7200):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS: DATAFRAME PANDAS
        """
        # creating connection object
        conn = self.db_connection(driver_sql, server, port,
                                  database, user_id, password, timeout)
        # return sql
        return pd.read_sql(query, con=conn)
