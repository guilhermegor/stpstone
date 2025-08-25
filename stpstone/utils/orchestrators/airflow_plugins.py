"""Airflow plugin utilities for date validation.

This module provides a class for validating working days in Airflow DAGs using Brazilian date 
handling.
"""

from typing import Any

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.cals.cal_abc import DatesBR


class AirflowPlugins(metaclass=TypeChecker):
    """Class containing Airflow plugin utilities."""

    def _validate_ds(self, ds: str) -> None:
        """Validate the ds parameter from Airflow context.

        Parameters
        ----------
        ds : str
            Date string to validate

        Raises
        ------
        ValueError
            If ds is empty or not a string
        """
        if not ds:
            raise ValueError("Date string (ds) cannot be empty")

    def validate_working_day(self, **kwargs: dict[str, Any]) -> None:
        """Validate if the provided date is a working day.

        Parameters
        ----------
        kwargs : dict[str, Any]
            Airflow context dictionary. Must include 'ds' key.

        Returns
        -------
        None

        Raises
        ------
        KeyError
            If 'ds' or 'ti' keys are missing from kwargs
        ValueError
            If date string is invalid
        """
        try:
            self._validate_ds(kwargs['ds'])
            ti = kwargs['ti']
            bool_working_day = DatesBR().is_working_day(kwargs['ds'])
            ti.xcom_push(key='bool_continue', value=bool_working_day)
        except KeyError as err:
            raise KeyError(f"Missing required key in context: {str(err)}") from err
        except Exception as err:
            raise ValueError(f"Date validation failed: {str(err)}") from err