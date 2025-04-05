### MODULE TO CONNECT IN DATABRICKS SQL ###

import sys
import os
import pyodbc as pyo
import pandas as pd
from func_timeout import func_timeout, FunctionTimedOut
sys.path.append('\\'.join([d for d in os.path.dirname(
    os.path.realpath(__file__)).split('\\')][:-2]))
from stpstone.utils.loggs.create_logs import CreateLog


class Databricks:

    def __init__(self, str_query, list_dsns, logger=None, max_error_attempts=3):
        self.str_query = str_query
        self.list_dsns = list_dsns
        self.logger = logger
        self.max_error_attempts = max_error_attempts

    def conn_dsn_databricks(self, dsn_conn, int_timeout=108000, bl_autocommit=True):
        """
        DOCSTRING: ESTABLISH CONNECTION WITH DATABRICKS
        INPUTS: CONNECTION STRING
        OUTPUTS: OBJECT WITH DATABRICKS CONNECTION
        """
        return pyo.connect('DSN={}'.format(dsn_conn), autocommit=bl_autocommit, timeout=int_timeout)

    def fetch_data_from_databricks(self, connection):
        """
        DOCSTRING: ATTEMPT TO CONSULT DATABRICKS
        INPUTS: QUERY AND CONNECTION OBJECT
        OUTPUTS: PANDAS DATAFRAME
        """
        return pd.read_sql(self.str_query, connection)

    def conn_databricks(self, bl_conn=False, int_max_wait=480, str_error='CONNECTION TIMEOUT EXPIRED',
                        bl_kill_process_when_databricks_down=False):
        """
        DOCSTRING: CONNECTION WITH DATABRICKS IN THE FIRST VALID DSN
        INPUTS: QUERT, LIST OF DSNS, LOGGER
        OUTPUTS: PANDAS DATAFRAME
        """
        # looping through dsns configuring
        for dsn in self.list_dsns:
            try:
                #   creating connection with the sql database
                connection = self.conn_dsn_databricks(dsn)
                df_databricks = func_timeout(int_max_wait, self.fetch_data_from_databricks,
                                             args=(connection))
                #   if the connection was established break the loop, otherwise log error
                bl_conn = True
                break
            except FunctionTimedOut:
                if self.logger != None:
                    CreateLog().warning(self.logger, 'Connection could not be established in '
                                         + 'the DSN {}'.format(dsn))
            except Exception as e:
                if self.logger != None:
                    CreateLog().warning(self.logger, 'Connection could not be established '
                                         + 'in the DSN {}. Error: {}. '.format(dsn, e))
        # caso a conexão não tenha sido estabelecida retornar um erro
        if bl_conn == False:
            if self.logger != None:
                CreateLog().warning(self.logger, 'Connection to Databricks could not be established '
                                     + 'in any of the DSNs, please validate the stability of the service. List of DSNs '
                                     + 'configured on the machine: {}'.format(self.list_dsns))
                if bl_kill_process_when_databricks_down == True:
                    raise Exception(self.logger, 'Connection to Databricks could not be established '
                                    + 'in any of the DSNs, please validate the stability of the service. List of DSNs '
                                    + 'configured on the machine: {}'.format(self.list_dsns))
            return str_error
        # return desired dataframe
        return df_databricks

    def retrieve_query_data(self, int_max_wait=480, i=0, df_databricks=''):
        """
        DOCSTRING: RETRIEVE QUERY DATA
        INPUTS: -
        OUTPUTS: DATAFRAME
        """
        # connection class
        class_databricks = Databricks(
            self.str_query, self.list_dsns, self.logger)
        # looping until overclock iterator
        while (type(df_databricks) == str) and (i <= self.max_error_attempts):
            #   attempting connection
            df_databricks = class_databricks.conn_databricks(
                int_max_wait=int_max_wait)
            #   logger-wise, if it is None print message, otherwise call the logger object to store
            #       data in network
            if self.logger is None:
                print('#{} Attempting connection with DSNs to Databricks'.format(i))
            else:
                CreateLog().info(self.logger,
                                  '#{} Attempting connection with DSNs to Databricks'.format(i))
            #   looping iterator to next count
            i += 1
        # return dataframe or error message
        return df_databricks
