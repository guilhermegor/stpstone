"""Logging utilities for initializing and managing log messages.

This module provides a function for initializing logging with proper validation
and formatting of log messages, including directory creation and operator information.
"""

from getpass import getuser
from logging import Logger
from typing import Optional

from stpstone.transformations.validation.metaclass_type_checker import type_checker
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog


@type_checker
def _validate_path_log(path_log: Optional[str]) -> None:
    """Validate the log path.

    Parameters
    ----------
    path_log : Optional[str]
        Path for log file directory

    Raises
    ------
    ValueError
        If path_log is an empty string
    """
    if path_log == "":
        raise ValueError("Log path cannot be an empty string")


@type_checker
def initiate_logging(logger: Logger, path_log: Optional[str] = None) -> None:
    """Initialize logging with directory creation and operator information.

    Parameters
    ----------
    logger : Logger
        Logger instance for logging messages
    path_log : Optional[str]
        Path for log file directory (default: None)

    Raises
    ------
    RuntimeError
        If unexpected dispatch value is returned from directory creation

    References
    ----------
    .. [1] https://docs.python.org/3/library/logging.html
    """
    _validate_path_log(path_log)

    log_creator = CreateLog()

    if path_log is not None:
        dispatch = log_creator.creating_parent_folder(path_log)
        log_creator.log_message(
            logger,
            f"Logs parent directory: {path_log}",
            "info"
        )
        if dispatch is True:
            log_creator.log_message(
                logger,
                "Logs parent directory created successfully.",
                "info"
            )
        elif dispatch is False:
            log_creator.log_message(
                logger,
                "Logs parent directory could not be created.",
                "info"
            )
        else:
            raise RuntimeError(f"Unexpected dispatch value: {dispatch}") from None

    log_creator.log_message(
        logger,
        f"Routine started at {DatesBRAnbima().curr_datetime()}",
        "info"
    )
    log_creator.log_message(
        logger,
        f"Routine operator {getuser()}",
        "info"
    )