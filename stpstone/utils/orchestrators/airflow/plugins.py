### GENERIC PLUGING FOR AIRFLOW ###

# pypi.org libs
from typing import Dict, Any
# project modules
from stpstone.utils.cals.handling_dates import DatesBR


class AirflowPlugins:

    def validate_working_day(self, **kwargs: Dict[str, Any]) -> None:
        """
        Validates whether the provided date (`kwargs['ds']`) is a working day, otherwise stop the DAG.

        Args:
            kwargs (dict): Airflow context dictionary. Must include 'ds' key.

        Returns:
            None
        """
        ti = kwargs['ti']
        bl_workng_day = DatesBR().is_working_day(kwargs['ds'])
        ti.xcom_push(key='bl_continue', value=bl_workng_day)
