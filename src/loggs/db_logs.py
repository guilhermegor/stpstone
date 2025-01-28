### DATABASE LOGS

from stpstone.settings._global_slots import YAML_GEN
from stpstone.cals.handling_dates import DatesBR


class DBLogs:

    def audit_log(self, df_, str_url, dt_last_update):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # adding audit columns
        df_[YAML_GEN['audit_log_cols']['url']] = str_url
        df_[YAML_GEN['audit_log_cols']['last_update']] = dt_last_update
        df_[YAML_GEN['audit_log_cols']['log_timestamp']] = DatesBR().utc_log_ts
        # returning dataframe
        return df_