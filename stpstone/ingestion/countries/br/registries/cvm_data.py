### CVM DATA - https://dados.cvm.gov.br/dados

import pandas as pd
import numpy as np
from io import StringIO
from typing import Dict, Any
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.parsers.folders import DirFilesManagement


class CVMDATA:

    def __init__(
        self,
        str_host_cvm:str='https://dados.cvm.gov.br/dados/',
        logger:object=None,
        str_dt_error:str='2100-01-01',
        str_format_dt_input:str='YYYY-MM-DD',
        int_val_err:int=0,
        dict_fund_class_subclass_register:Dict[str, Any]=None
    ):
        self.str_host_cvm = str_host_cvm
        self.logger = logger
        self.str_dt_error = str_dt_error
        self.str_format_dt_input = str_format_dt_input
        self.int_val_err = int_val_err
        self.dict_fund_class_subclass_register = self.funds_classes_subclasses_register_raw \
            if dict_fund_class_subclass_register is None else dict_fund_class_subclass_register

    @property
    def funds_register(
        self,
        str_app:str='FI/CAD/DADOS/cad_fi.csv'
    ):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # url
        url = f'{self.str_host_cvm}{str_app}'
        # read the csv file from the url into a pandas dataframe
        reader = pd.read_csv(url, sep=';', encoding='latin1', decimal='.', thousands=',')
        df_funds_register = pd.DataFrame(reader)
        # validate the content of dataframe
        if (df_funds_register.empty == True) \
            and (self.logger is not None):
            CreateLog().error(
                self.logger, 'Error reading funds register within url: {}'.format(url))
            raise Exception('Error reading funds register within url: {}'.format(url))
        elif df_funds_register.empty == True:
            raise Exception('Error reading funds register within url: {}'.format(url))
        # fill na values
        list_cols = list(df_funds_register.columns)
        list_cols_dts = [
            'DT_REG',
            'DT_CONST',
            'DT_CANCEL',
            'DT_INI_SIT',
            'DT_INI_ATIV',
            'DT_INI_EXERC',
            'DT_FIM_EXERC',
            'DT_INI_CLASSE',
            'DT_PATRIM_LIQ'
        ]
        for col_dt in list_cols_dts:
            df_funds_register[col_dt] = df_funds_register[col_dt].fillna(self.str_dt_error)
        for col_ in [c for c in list_cols if c not in list_cols_dts]:
            df_funds_register[col_] = df_funds_register[col_].fillna(self.int_val_err)
        # changing coumn datatypes
        df_funds_register = df_funds_register.astype({
            'TP_FUNDO': 'category',
            'CNPJ_FUNDO': str,
            'DENOM_SOCIAL': str,
            'CD_CVM': np.int64,
            'DT_REG': str,
            'DT_CONST': str,
            'CD_CVM': int,
            'DT_CANCEL': str,
            'SIT': str,
            'DT_INI_SIT': str,
            'DT_INI_ATIV': str,
            'DT_INI_EXERC': str,
            'DT_FIM_EXERC': str,
            'CLASSE': str,
            'DT_INI_CLASSE': str,
            'RENTAB_FUNDO': str,
            'CONDOM': str,
            'FUNDO_COTAS': str,
            'FUNDO_EXCLUSIVO': str,
            'TRIB_LPRAZO': str,
            'PUBLICO_ALVO' : str,
            'ENTID_INVEST': str,
            'TAXA_PERFM': str,
            'INF_TAXA_PERFM': str,
            'TAXA_ADM': str,
            'INF_TAXA_ADM': str,
            'VL_PATRIM_LIQ': float,
            'DT_PATRIM_LIQ': str,
            'DIRETOR': str,
            'CNPJ_ADMIN': str,
            'ADMIN': str,
            'PF_PJ_GESTOR': str,
            'CPF_CNPJ_GESTOR': str,
            'CNPJ_AUDITOR': str,
            'AUDITOR': str,
            'CNPJ_CUSTODIANTE': str,
            'CUSTODIANTE': str,
            'CNPJ_CONTROLADOR': str,
            'CONTROLADOR': str,
            'INVEST_CEMPR_EXTER': str,
            'CLASSE_ANBIMA': str
        })
        for col_dt in list_cols_dts:
            df_funds_register[col_dt] = [
                DatesBR().str_date_to_datetime(d, self.str_format_dt_input)
                for d in df_funds_register[col_dt]
            ]
        # return the dataframe
        return df_funds_register

    @property
    def funds_classes_subclasses_register_raw(
        self,
        str_app:str='FI/CAD/DADOS/registro_fundo_classe.zip'
    ):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # setting variables
        dict_ = dict()
        # url
        url = f'{self.str_host_cvm}{str_app}'
        # downloading zip file into memory
        list_main_zip = RemoteFiles().get_zip_from_web_in_memory(
            url,
            bl_io_interpreting=False,
            bl_verify=False
        )
        # iterate through files in the ZIP
        for file_info in list_main_zip:
            #   extracting csv files
            if file_info.name.endswith('.csv'):
                dict_[file_info.name] = file_info.read()
        return dict_

    def funds_raw_infos(
        self,
        key_file_name,
        list_cols_dts,
        dict_cols_types
    ):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # assuming self.dict_fund_class_subclass_register[key_file_name] is a bytes object
        file_data = self.dict_fund_class_subclass_register[key_file_name]
        # trying to decode with 'ISO-8859-1' or 'latin1' encoding
        try:
            file_data_str = StringIO(file_data.decode('utf-8'))
        except UnicodeDecodeError:
            file_data_str = StringIO(file_data.decode('ISO-8859-1'))
        # reading csv
        df_ = pd.read_csv(file_data_str, delimiter=';')
        # fill na values
        list_cols = list(df_.columns)
        for col_dt in list_cols_dts:
            df_[col_dt] = df_[col_dt].fillna(self.str_dt_error)
        for col_ in [c for c in list_cols if c not in list_cols_dts]:
            df_[col_] = df_[col_].fillna(self.int_val_err)
        # changing datatypes
        df_ = df_.astype(dict_cols_types)
        for col_dt in list_cols_dts:
            df_[col_dt] = [
                DatesBR().str_date_to_datetime(d, self.str_format_dt_input)
                for d in df_[col_dt]
            ]
        # return the dataframe
        return df_

    @property
    def funds_classes(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # getting dataframe with funds
        df_funds_classes = self.funds_raw_infos(
            'registro_classe.csv',
            [
                'Data_Registro',
                'Data_Constituicao',
                'Data_Inicio',
            ],
            {
                'ID_Registro_Fundo': np.int64,
                'ID_Registro_Classe': np.int64,
                'CNPJ_Classe': str,
                'Codigo_CVM': np.int64,
                'Data_Registro': str,
                'Data_Constituicao': str,
                'Data_Inicio': str,
                'Tipo_Classe': str,
                'Denominacao_Social': str,
                'Situacao': str,
                'Classificacao': str,
                'Indicador_Desempenho': str,
                'Classe_Cotas': str,
                'Classificacao_Anbima': str,
                'Tributacao_Longo_Prazo': str,
                'Entidade_Investimento': str,
                'Permitido_Aplicacao_CemPorCento_Exterior': str,
                'Classe_ESG': str,
                'Forma_Condominio': str,
                'Exclusivo': str,
                'Publico_Alvo': str,
                'CNPJ_Auditor': str,
                'Auditor': str,
                'CNPJ_Custodiante': str,
                'Custodiante': str,
                'CNPJ_Controlador': str,
                'Controlador': str
            }
        )
        # return the dataframe
        return df_funds_classes

    @property
    def funds_register_2(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        df_funds_register = self.funds_raw_infos(
            'registro_fundo.csv',
            [
                'Data_Registro',
                'Data_Constituicao',
                'Data_Cancelamento',
                'Data_Inicio_Situacao',
                'Data_Adaptacao_RCVM175',
                'Data_Inicio_Exercicio_Social',
                'Data_Fim_Exercicio_Social',
                'Data_Patrimonio_Liquido'
            ],
            {
                'ID_Registro_Fundo': np.int64,
                'CNPJ_Fundo': str,
                'Codigo_CVM': np.int64,
                'Data_Registro': str,
                'Data_Constituicao': str,
                'Tipo_Fundo': str,
                'Denominacao_Social': str,
                'Data_Cancelamento': str,
                'Situacao': str,
                'Data_Inicio_Situacao': str,
                'Data_Adaptacao_RCVM175': str,
                'Data_Inicio_Exercicio_Social': str,
                'Data_Fim_Exercicio_Social': str,
                'Patrimonio_Liquido': np.float64,
                'Data_Patrimonio_Liquido': str,
                'Diretor': str,
                'CNPJ_Administrador': str,
                'Administrador': str,
                'Tipo_Pessoa_Gestor': str,
                'CPF_CNPJ_Gestor': str,
                'Gestor': str
            }
        )
        return df_funds_register

    @property
    def funds_subclasses(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        df_funds_subclasses = self.funds_raw_infos(
            'registro_subclasse.csv',
            [
                'Data_Constituicao',
                'Data_Inicio'
            ],
            {
                'ID_Registro_Classe': np.int64,
                'ID_Subclasse': str,
                'Codigo_CVM': pd.Int64Dtype(),
                'Data_Constituicao': str,
                'Data_Inicio': str,
                'Denominacao_Social': str,
                'Situacao': str,
                'Forma_Condominio': str,
                'Exclusivo': str,
                'Publico_Alvo': str
            }
        )
        return df_funds_subclasses
