### INITIAL LOGGING SETUP ###

# pypi.org libs
from getpass import getuser
from logging import Logger
from typing import Optional
# local libs
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.loggs.create_logs import CreateLog


def iniciating_logging(logger:Logger, path_log:Optional[str]=None) -> None:
    if path_log != None:
        dispatch = CreateLog().creating_parent_folder(path_log)
        CreateLog().infos(logger, 'Logs parent directory: {}'.format(path_log))
        if dispatch == 'OK':
            CreateLog().infos(logger, 'Logs parent directory created successfully.')
        elif dispatch == 'NOK':
            CreateLog().infos(
                logger, 'Logs parent directory could not be created.')
        else:
            raise Exception(
                'Unexpected dispatch value: {}'.format(dispatch))
    # iniciating routine
    CreateLog().infos(logger, 'Rotina iniciada em {}'.format(
        str(DatesBR().curr_date_time())))
    CreateLog().infos(logger, 'Operador da rotina {}'.format(str(getuser())))
