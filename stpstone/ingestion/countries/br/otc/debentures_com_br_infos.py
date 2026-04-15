"""Debentures.com.br Infos scraping.

This module provides a class for ingesting debenture characteristics data from debentures.com.br.

Notes
-----
[1] Metadata: https://www.debentures.com.br/exploreosnd/exploreosnd.asp
[2] Special thanks to Rodrigo Prado (https://github.com/royopa) for helping to develop this class.
"""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import backoff
import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import requests
from requests import Response, Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.abc.ingestion_abc import (
    ABCIngestionOperations,
    ContentParser,
    CoreIngestion,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


class DebenturesComBrInfos(ABCIngestionOperations):
    """Debentures.com.br Infos."""

    def __init__(
        self,
        date_start: Optional[date] = None,
        date_end: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the ingestion class.

        Parameters
        ----------
        date_start : Optional[date]
            The start date, by default None.
        date_end : Optional[date]
            The end date, by default None.
        logger : Optional[Logger]
            The logger, by default None.
        cls_db : Optional[Session]
            The database session, by default None.

        Returns
        -------
        None
        """
        super().__init__(cls_db=cls_db)
        CoreIngestion.__init__(self)
        ContentParser.__init__(self)

        self.logger = logger
        self.cls_db = cls_db
        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()
        self.cls_create_log = CreateLog()
        self.cls_dates_br = DatesBRAnbima()
        self.date_start = date_start or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -5)
        self.date_end = date_end or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.url = "https://www.debentures.com.br/exploreosnd/consultaadados/emissoesdedebentures/caracteristicas_e.asp"

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_debentures_com_br_infos",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.

        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0).
        bool_verify : bool
            Whether to verify the SSL certificate, by default True.
        bool_insert_or_ignore : bool
            Whether to insert or ignore the data, by default False.
        str_table_name : str
            The name of the table, by default 'br_debentures_com_br_infos'.

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        file = self.parse_raw_file(resp_req)
        df_ = self.transform_data(file=file)
        df_ = self.standardize_dataframe(
            df_=df_,
            date_ref=self.date_end,
            dict_dtypes={
                "CODIGO_DO_ATIVO": str,
                "EMPRESA": str,
                "SERIE": str,  # codespell:ignore
                "EMISSAO": str,
                "IPO": str,
                "SITUACAO": str,
                "ISIN": str,
                "REGISTRO_CVM_DA_EMISSAO": str,
                "DATA_DE_REGISTRO_CVM_DA_EMISSAO": "date",
                "REGISTRO_CVM_DO_PROGRAMA": str,
                "DATA_DE_REGISTRO_CVM_DO_PROGRAMA": "date",
                "DATA_DE_EMISSAO": "date",
                "DATA_DE_VENCIMENTO": "date",
                "MOTIVO_DE_SAIDA": str,
                "DATA_DE_SAIDA_NOVO_VENCIMENTO": "date",
                "DATA_DO_INICIO_DA_RENTABILIDADE": "date",
                "DATA_DO_INICIO_DA_DISTRIBUICAO": "date",
                "DATA_DA_PROXIMA_REPACTUACAO": "date",
                "ATO_SOCIETARIO_1": str,
                "DATA_DO_ATO_1": "date",
                "ATO_SOCIETARIO_2": str,
                "DATA_DO_ATO_2": "date",
                "FORMA": str,
                "GARANTIA_ESPECIE": str,
                "CLASSE": "category",  # codespell:ignore
                "QUANTIDADE_EMITIDA": int,
                "ARTIGO_14": str,
                "ARTIGO_24": str,
                "QUANTIDADE_EM_MERCADO": int,
                "QUANTIDADE_EM_TESOURARIA": int,
                "QUANTIDADE_RESGATADA": int,
                "QUANTIDADE_CANCELADA": int,
                "QUANTIDADE_CONVERTIDA_NO_SND": int,
                "QUANTIDADE_CONVERTIDA_FORA_DO_SND": str,
                "QUANTIDADE_PERMUTADA_NO_SND": int,
                "QUANTIDADE_PERMUTADA_FORA_DO_SND": str,
                "UNIDADE_MONETARIA_VALOR_NOMINAL_EMISSAO": str,
                "VALOR_NOMINAL_NA_EMISSAO": float,
                "UNIDADE_MONETARIA_VALOR_NOMINAL_ATUAL": str,
                "VALOR_NOMINAL_ATUAL": float,
                "DATA_ULT_VNA": "date",
                "INDICE": str,
                "TIPO": str,
                "CRITERIO_DE_CALCULO": str,
                "DIA_DE_REFERENCIA_PARA_INDICE_DE_PRECOS": str,
                "CRITERIO_PARA_INDICE": str,
                "CORRIGE_A_CADA": str,
                "PERCENTUAL_MULTIPLICADOR_RENTABILIDADE": float,
                "LIMITE_DA_TJLP": float,
                "TIPO_DE_TRATAMENTO_DO_LIMITE_DA_TJLP": str,
                "JUROS_CRITERIO_ANTIGO_DO_SND": str,
                "PREMIOS_CRITERIO_ANTIGO_DO_SND": str,
                "AMORTIZACAO_TAXA": float,
                "AMORTIZACAO_CADA": str,
                "AMORTIZACAO_UNIDADE": str,
                "AMORTIZACAO_CARENCIA": str,
                "AMORTIZACAO_CRITERIO": str,
                "TIPO_DE_AMORTIZACAO": str,
                "JUROS_CRITERIO_NOVO_TAXA": float,
                "JUROS_CRITERIO_NOVO_PRAZO": str,
                "JUROS_CRITERIO_NOVO_CADA": str,
                "JUROS_CRITERIO_NOVO_UNIDADE": str,
                "JUROS_CRITERIO_NOVO_CARENCIA": str,
                "JUROS_CRITERIO_NOVO_CRITERIO": str,
                "JUROS_CRITERIO_NOVO_TIPO": str,
                "PREMIO_CRITERIO_NOVO_TAXA": float,
                "PREMIO_CRITERIO_NOVO_PRAZO": int,
                "PREMIO_CRITERIO_NOVO_CADA": str,
                "PREMIO_CRITERIO_NOVO_UNIDADE": str,
                "PREMIO_CRITERIO_NOVO_CARENCIA": str,
                "PREMIO_CRITERIO_NOVO_CRITERIO": str,
                "PREMIO_CRITERIO_NOVO_TIPO": str,
                "PARTICIPACAO_TAXA": float,
                "PARTICIPACAO_CADA": str,
                "PARTICIPACAO_UNIDADE": str,
                "PARTICIPACAO_CARENCIA": str,
                "PARTICIPACAO_DESCRICAO": str,
                "BANCO_MANDATARIO": str,
                "AGENTE_FIDUCIARIO": str,
                "INSTITUICAO_DEPOSITARIA": str,
                "COORDENADOR_LIDER": str,
                "CNPJ": str,
                "DEB_INCENT_LEI_12431": str,
                "ESCRITURA_PADRONIZADA": str,
                "RESGATE_ANTECIPADO": str,
            },
            str_fmt_dt="DD/MM/YYYY",
            url=self.url,
        )
        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db,
                str_table_name=str_table_name,
                df_=df_,
                bool_insert_or_ignore=bool_insert_or_ignore,
            )
        else:
            return df_

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.HTTPError,
        max_time=60,
    )
    def get_response(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Return a list of response objects.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0).
        bool_verify : bool
            Verify the SSL certificate, by default True.

        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            A list of response objects.
        """
        resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()
        return resp_req

    def parse_raw_file(
        self,
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> StringIO:
        """Parse the raw file content.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.

        Returns
        -------
        StringIO
            The parsed content.
        """
        return self.get_file(resp_req=resp_req)

    def transform_data(
        self,
        file: StringIO,
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.

        Parameters
        ----------
        file : StringIO
            The parsed content.

        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return pd.read_csv(
            file,
            sep="\t",
            skiprows=5,
            names=[
                "CODIGO_DO_ATIVO",
                "EMPRESA",
                "SERIE",  # codespell:ignore
                "EMISSAO",
                "IPO",
                "SITUACAO",
                "ISIN",
                "REGISTRO_CVM_DA_EMISSAO",
                "DATA_DE_REGISTRO_CVM_DA_EMISSAO",
                "REGISTRO_CVM_DO_PROGRAMA",
                "DATA_DE_REGISTRO_CVM_DO_PROGRAMA",
                "DATA_DE_EMISSAO",
                "DATA_DE_VENCIMENTO",
                "MOTIVO_DE_SAIDA",
                "DATA_DE_SAIDA_NOVO_VENCIMENTO",
                "DATA_DO_INICIO_DA_RENTABILIDADE",
                "DATA_DO_INICIO_DA_DISTRIBUICAO",
                "DATA_DA_PROXIMA_REPACTUACAO",
                "ATO_SOCIETARIO_1",
                "DATA_DO_ATO_1",
                "ATO_SOCIETARIO_2",
                "DATA_DO_ATO_2",
                "FORMA",
                "GARANTIA_ESPECIE",
                "CLASSE",  # codespell:ignore
                "QUANTIDADE_EMITIDA",
                "ARTIGO_14",
                "ARTIGO_24",
                "QUANTIDADE_EM_MERCADO",
                "QUANTIDADE_EM_TESOURARIA",
                "QUANTIDADE_RESGATADA",
                "QUANTIDADE_CANCELADA",
                "QUANTIDADE_CONVERTIDA_NO_SND",
                "QUANTIDADE_CONVERTIDA_FORA_DO_SND",
                "QUANTIDADE_PERMUTADA_NO_SND",
                "QUANTIDADE_PERMUTADA_FORA_DO_SND",
                "UNIDADE_MONETARIA_VALOR_NOMINAL_EMISSAO",
                "VALOR_NOMINAL_NA_EMISSAO",
                "UNIDADE_MONETARIA_VALOR_NOMINAL_ATUAL",
                "VALOR_NOMINAL_ATUAL",
                "DATA_ULT_VNA",
                "INDICE",
                "TIPO",
                "CRITERIO_DE_CALCULO",
                "DIA_DE_REFERENCIA_PARA_INDICE_DE_PRECOS",
                "CRITERIO_PARA_INDICE",
                "CORRIGE_A_CADA",
                "PERCENTUAL_MULTIPLICADOR_RENTABILIDADE",
                "LIMITE_DA_TJLP",
                "TIPO_DE_TRATAMENTO_DO_LIMITE_DA_TJLP",
                "JUROS_CRITERIO_ANTIGO_DO_SND",
                "PREMIOS_CRITERIO_ANTIGO_DO_SND",
                "AMORTIZACAO_TAXA",
                "AMORTIZACAO_CADA",
                "AMORTIZACAO_UNIDADE",
                "AMORTIZACAO_CARENCIA",
                "AMORTIZACAO_CRITERIO",
                "TIPO_DE_AMORTIZACAO",
                "JUROS_CRITERIO_NOVO_TAXA",
                "JUROS_CRITERIO_NOVO_PRAZO",
                "JUROS_CRITERIO_NOVO_CADA",
                "JUROS_CRITERIO_NOVO_UNIDADE",
                "JUROS_CRITERIO_NOVO_CARENCIA",
                "JUROS_CRITERIO_NOVO_CRITERIO",
                "JUROS_CRITERIO_NOVO_TIPO",
                "PREMIO_CRITERIO_NOVO_TAXA",
                "PREMIO_CRITERIO_NOVO_PRAZO",
                "PREMIO_CRITERIO_NOVO_CADA",
                "PREMIO_CRITERIO_NOVO_UNIDADE",
                "PREMIO_CRITERIO_NOVO_CARENCIA",
                "PREMIO_CRITERIO_NOVO_CRITERIO",
                "PREMIO_CRITERIO_NOVO_TIPO",
                "PARTICIPACAO_TAXA",
                "PARTICIPACAO_CADA",
                "PARTICIPACAO_UNIDADE",
                "PARTICIPACAO_CARENCIA",
                "PARTICIPACAO_DESCRICAO",
                "BANCO_MANDATARIO",
                "AGENTE_FIDUCIARIO",
                "INSTITUICAO_DEPOSITARIA",
                "COORDENADOR_LIDER",
                "CNPJ",
                "DEB_INCENT_LEI_12431",
                "ESCRITURA_PADRONIZADA",
                "RESGATE_ANTECIPADO",
            ],
            header=None,
            decimal=",",
            thousands=".",
            encoding="latin-1",
            na_values=["-", "-  ", "  -", " -", " - ", " "],
            engine="python",
        )
