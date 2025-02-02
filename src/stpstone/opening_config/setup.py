### CONFIGURAÇÕES BÁSICAS PARA RODAR TODAS AS ROTINAS ###

from getpass import getuser

import yaml

from stpstone.cals.handling_dates import DatesBR
from stpstone.loggs.create_logs import CreateLog


def reading_yaml(yaml_path):
    """
    DOCSTRING: ABRIR ARQUIVOS .YAML
    INPUTS: CAMINHO
    OUTPUTS: ATRIBUIÇÃO DE INPUTS
    """
    return yaml.load(open(yaml_path), Loader=yaml.FullLoader)


def iniciating_logging(logger_name, parent_destination_log=None):
    """
    DOCSTRING: INICIALIZING LOGGING LIB
    INPUTS: COMPLETE PATH TO LOGGING DIRECTORY
    OUTPUTS: -
    """
    # creating parent directory
    if parent_destination_log != None:
        dispatch = CreateLog().creating_parent_folder(parent_destination_log)
        CreateLog().infos(
            logger_name,
            'Diretório pai de logs: {}'.format(parent_destination_log),
        )
        if dispatch == 'OK':
            CreateLog().infos(logger_name, 'Diretório pai criado com sucesso.')
        elif dispatch == 'NOK':
            CreateLog().infos(
                logger_name,
                'Diretório pai já havia sido criado, nenhuma medida foi tomada.',
            )
        else:
            raise Exception(
                'O valor retornado na função deve ser OK ou NOK para criação do diretório'
            )
    # iniciating routine
    CreateLog().infos(
        logger_name,
        'Rotina iniciada em {}'.format(str(DatesBR().curr_date_time())),
    )
    CreateLog().infos(
        logger_name, 'Operador da rotina {}'.format(str(getuser()))
    )
