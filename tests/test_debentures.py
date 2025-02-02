#!/usr/bin/env python3
from datetime import datetime
from unittest import TestCase, main

import pandas as pd


class Debentures(TestCase):
    def get_pu_historico(
        self,
        ativo: str = None,
        dt_ini: datetime = None,
        dt_fim: datetime = None,
    ):
        url_base: str = (
            'https://www.debentures.com.br/exploreosnd/consultaadados/'
        )

        if ativo is None:
            ativo = ''

        if dt_ini:
            dt_ini = dt_ini.strftime('%d/%m/%Y')

        if dt_fim is None:
            dt_fim = datetime.now()

        if dt_fim:
            dt_fim = dt_fim.strftime('%d/%m/%Y')

        url = f'{url_base}emissoesdedebentures/puhistorico_e.asp?op_exc=Nada&ativo=&dt_ini={dt_ini}&dt_fim={dt_fim}&Submit.x=30&Submit.y=6'
        print('Loading data...', url, sep='\n')

        df = pd.read_csv(
            url,
            encoding='latin1',
            decimal=',',
            thousands='.',
            low_memory=False,
            skiprows=2,
            sep='\t',
        )

        a_renomear = {
            'Data do PU': 'DT_REF',
            'Ativo': 'CO_ATIVO',
            'Valor Nominal': 'VR_NOMINAL',
            'Juros': 'PC_JUROS',
            'Prêmio': 'VR_PREMIO',
            'Preço Unitário': 'VR_PU',
            'Critério de Cálculo': 'DE_CRITERIO_CALCULO',
            'Situação': 'DE_SITUACAO',
        }

        del df['Unnamed: 8']

        df.rename(columns=a_renomear, inplace=True)

        df['DT_REF'] = pd.to_datetime(
            df['DT_REF'], format='%d/%m/%Y', errors='coerce'
        )

        return df

    def get_caracteristicas(self, ativo: str = None):
        url_base: str = (
            'https://www.debentures.com.br/exploreosnd/consultaadados/'
        )

        if ativo is None:
            ativo = ''

        url = f'{url_base}emissoesdedebentures/caracteristicas_e.asp?Ativo={ativo}'
        print('Loading data...', url, sep='\n')

        try:
            df = pd.read_csv(
                url,
                encoding='latin1',
                decimal=',',
                thousands='.',
                low_memory=False,
                skiprows=2,
                sep='\t',
            )

            return df

        except Exception as e:
            print('Error:', e)
            return None

    def get_precos_negociacao(
        self,
        isin: str = None,
        ativo: str = None,
        dt_ini: datetime = None,
        dt_fim: datetime = None,
    ):
        url_base: str = (
            'https://www.debentures.com.br/exploreosnd/consultaadados/'
        )

        if isin is None:
            isin = ''

        if ativo is None:
            ativo = ''

        if dt_ini:
            dt_ini = dt_ini.strftime('%Y%m%d')

        if dt_fim is None:
            dt_fim = datetime.now()

        if dt_fim:
            dt_fim = dt_fim.strftime('%Y%m%d')

        url = f'{url_base}mercadosecundario/precosdenegociacao_e.asp?op_exc=Nada&emissor=&isin={isin}&ativo={ativo}&dt_ini={dt_ini}&dt_fim={dt_fim}'
        print('Loading data...', url, sep='\n')

        df = pd.read_csv(
            url,
            encoding='latin1',
            decimal=',',
            thousands='.',
            low_memory=False,
            skiprows=2,
            sep='\t',
        )

        a_renomear = {
            'Data': 'DT_REF',
            'Emissor': 'NO_EMISSOR',
            'Código do Ativo': 'CO_ATIVO',
            'ISIN': 'CO_ISIN',
            'Quantidade': 'NU_QUANTIDADE',
            'Número de Negócios': 'NU_NEGOCIOS',
            'PU Mínimo': 'VR_PU_MINIMO',
            'PU Médio': 'VR_PU_MEDIO',
            'PU Máximo': 'VR_PU_MAXIMO',
            '% PU da Curva': 'PC_PU_CURVA',
        }
        df.rename(columns=a_renomear, inplace=True)

        df['DT_REF'] = pd.to_datetime(
            df['DT_REF'], format='%d/%m/%Y', errors='coerce'
        )

        return df

    def test_get_prices_with_ativo(self):
        isin: str = 'BRAALMDBS017'
        ativo: str = 'AALM12'
        dt_ini: datetime = datetime(2024, 1, 1)
        dt_fim: datetime = datetime(2025, 1, 30)

        df: pd.DataFrame = self.get_precos_negociacao(
            isin, ativo, dt_ini, dt_fim
        )

        self.assertEqual(df.shape[0], 21)

    def test_get_prices_without_ativo(self):
        isin, ativo = None, None
        dt_ini: datetime = datetime(2025, 1, 1)
        dt_fim: datetime = datetime(2025, 1, 30)

        df: pd.DataFrame = self.get_precos_negociacao(
            isin, ativo, dt_ini, dt_fim
        )

        self.assertGreaterEqual(df.shape[0], 12000)

    def test_get_caracteristicas_without_ativo(self):
        ativo = None
        df: pd.DataFrame = self.get_caracteristicas(ativo)
        self.assertGreaterEqual(df.shape[0], 8300)

    def test_get_caracteristicas_with_ativo(self):
        ativo = 'AALM12'
        df: pd.DataFrame = self.get_caracteristicas(ativo)
        self.assertGreaterEqual(df.shape[0], 2)

        df = self.get_caracteristicas('blablabla')
        self.assertEqual(df, None)

    def test_get_pu_historico_with_ativo(self):
        ativo: str = 'AALM12'
        dt_ini: datetime = datetime(2025, 1, 1)
        dt_fim: datetime = datetime(2025, 1, 15)

        df: pd.DataFrame = self.get_pu_historico(ativo, dt_ini, dt_fim)

        self.assertGreaterEqual(df.shape[0], 29100)


if __name__ == '__main__':
    main()
