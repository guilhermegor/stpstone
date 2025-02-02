import os
import sys
from datetime import datetime
from io import StringIO
from typing import Any, Dict

import numpy as np
import pandas as pd

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
)
from stpstone.cals.handling_dates import DatesBR
from stpstone.handling_data.folders import DirFilesManagement
from stpstone.loggs.create_logs import CreateLog


class DebenturesSecondaryMarket:
    def __init__(
        self,
        str_base_host: str = 'https://www.debentures.com.br/exploreosnd/consultaadados/',
        logger: object = None,
        str_dt_error: str = '2100-01-01',
        str_format_dt_input: str = 'YYYY-MM-DD',
        int_val_err: int = 0,
        dict_fund_class_subclass_register=None,
    ):
        self.str_host_cvm = str_base_host
        self.logger = logger
        self.str_dt_error = str_dt_error
        self.str_format_dt_input = str_format_dt_input
        self.int_val_err = int_val_err

        url_base: str = (
            'https://www.debentures.com.br/exploreosnd/consultaadados/',
        )
        ativo: str = 'AALM12'
        isin: str = 'BRAALMDBS017'
        dt_ini: datetime = '20250110'
        dt_fim = '20250115'
        url = (
            f'{self.str_host_cvm}/mercadosecundario/precosdenegociacao_e.asp?'
            f'op_exc=Nada&emissor=&isin={isin}&ativo={ativo}&dt_ini={dt_ini}&dt_fim={dt_fim}'
        )

    def funds_register(self, str_app: str = 'FI/CAD/DADOS/cad_fi.csv'):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # url
        url = f'{self.str_host_cvm}{str_app}'

        df = pd.read_csv(
            url,
            sep=';',
            encoding='latin1',
            decimal='.',
            thousands=',',
            low_memory=False,
        )

        return df
