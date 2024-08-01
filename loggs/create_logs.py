### CRIACAO DE LOGS ###

import logging
import sys
import os
import time


class CreateLog:
    '''
    DOCSTRING: PREENCHIMENTO DE LOGS AO LONGO DA ROTINA
    INPUTS: -
    OUTPUTS: INFOS
    '''

    def creating_parent_folder(self, new_path):
        '''
        DOCSTRING: CRIAÇÃO DE PASTA NA REDE PARA EMBARCAR LOGS
        INPUTS: NEW PATH
        OUTPUTS: INFOS
        '''
        if not os.path.exists(new_path):
            os.makedirs(new_path)
            return 'OK'
        else:
            return 'NOK'

    def basic_conf(self, complete_path=None, basic_level='info'):
        '''
        DOCSTRING: INICIO ROTINA DE FORNECIMENTO DE LOGS
        INPUTS: -
        OUTPUTS: LOGGER
        '''
        # defining basic level of logging
        if basic_level == 'info':
            level = logging.INFO
        elif basic_level == 'debug':
            level = logging.DEBUG
        else:
            raise Exception(
                'Level was not properly defined in basic config of logging, please check')
        # logging message format
        logging.basicConfig(
            level=level,
            filename=complete_path,
            format=(
                '%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] '
                '%(message)s'
            ),
            datefmt='%Y-%m-%d,%H:%M:%S',
        )
        # console stream handler
        console = logging.StreamHandler()
        console.setLevel
        # defining logger object
        logger = logging.getLogger(__name__)
        # return logger
        return logger

    def infos(self, logger, msg_str):
        '''
        DOCSTRING: FORNECIMENTO DE INFORMAÇÕES SOBRE O ANDAMENTO DO CÓDIGO
        INPUTS: MENSAGEM EM STRING
        OUTPUTS: INFO
        '''
        return logger.info(msg_str)

    def warnings(self, logger, msg_str):
        '''
        DOCSTRING: ALERTAS PARA PONTOS DE ATENÇÃO NO CÓDIGO
        INPUTS: MENSAGEM EM STRING
        OUTPUTS: WARNING
        '''
        return logger.warning(msg_str)

    def errors(self, logger, msg_str):
        '''
        DOCSTRING: FALHAS POR PARTE DO USUÁRIO OU CONCEPÇÃO DO CÓDIGO
        INPUTS: MENSAGEM EM STRING
        OUTPUTS: ERROR
        '''
        return logger.error(msg_str, exc_info=True)

    def critical(self, logger, msg_str):
        '''
        DOCSTRING: APONTAR ERROS CRÍTICOS NO CÓDIGO
        INPUTS: MENSAGEM EM STRING
        OUTPUTS: ERROR
        '''
        return logger.error(msg_str)


# decorators
def timeit(method):
    '''
    REFERENCES: https://medium.com/pythonhive/python-decorator-to-measure-the-execution-time-of-methods-fa04cb6bb36d
    DOCSTRING: TIMING DECORRATOR TO MEASURE ELAPSED TIME TO EXECUTE A FUNCTION
    INPUTS: -
    OUTPUTS: ELAPSED TIME PRINTED
    '''
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f ms' %
                  (method.__name__, (te - ts) * 1000))
        return result
    return timed
