#### SEARCH BY TRADING - B3

import pandas as pd
import numpy as np
from zipfile import ZipFile
from stpstone._config.global_slots import YAML_B3
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.handling_data.lists import ListHandler
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.parsers.json import JsonFiles
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.handling_data.txt import HandlingTXTFiles
from stpstone.utils.parsers.str import StrHandler
from stpstone.finance.b3.up2data_web import UP2DATAB3
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.handling_data.xml import XMLFiles
from stpstone.finance.derivatives.options.european import EuropeanOptions
from stpstone.finance.b3.market_data import MDB3
from stpstone.utils.loggs.db_logs import DBLogs


class TradingFilesB3:

    def __init__(self, int_wd_bef:int=1, str_fillna:str='-1', str_fillna_dt:str='2100-12-31',
                 str_fillna_ts:str='2100-12-31 00:00:00', str_fmt_dt:str='YYYY-MM-DD') -> None:
        self.int_wd_bef = int_wd_bef
        self.str_fillna = str_fillna
        self.str_fillna_dt = str_fillna_dt
        self.str_fillna_ts = str_fillna_ts
        self.str_fmt_dt = str_fmt_dt

    def cenarios_tipo_curva(self, list_cenarios_tipo_curva=None):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # setting variables
        list_ser_exportacao_curvas = list()
        # validando tipo de variáveis de interesse
        if list_cenarios_tipo_curva != None:
            #   convertendo para lista, caso não seja
            if type(list_cenarios_tipo_curva) != list:
                list_cenarios_tipo_curva = [list_cenarios_tipo_curva]
            #   alterando tipo de instâncias
            list_cenarios_tipo_curva = [str(x)
                                        for x in list_cenarios_tipo_curva]
        # definindo url com cenários de risco - tipo de curva
        url_cenarios_tipo_curva = YAML_B3['cenarios_risco_tipo_curva']['tipo_curva']['url'].format(
            DatesBR().sub_working_days(DatesBR().curr_date, YAML_B3['cenarios_risco_tipo_curva'][
                'tipo_curva']['self.int_wd_bef']).strftime('%y%m%d'))
        # baixando em memória zip com cenário de tipo curva da b3
        list_txts_tipos_curva_cenarios = RemoteFiles().get_zip_from_web_in_memory(
            url_cenarios_tipo_curva, bl_verify=YAML_B3['cenarios_risco_tipo_curva']['bl_verify'],
            bl_io_interpreting=YAML_B3['cenarios_risco_tipo_curva']['bl_io_interpreting'],
            timeout=YAML_B3['cenarios_risco_tipo_curva']['tipo_curva']['timeout'])
        # loopando em torno dos arquivos em txt, abrindo em memória no dataframe pandas
        for extracted_file_tipo_curva_cenario in list_txts_tipos_curva_cenarios:
            #   caso a variável list_cenarios_tipo_curva seja diferente de none, o usuário tem
            #       interesse que apenas uma parte dos cenários seja considerado, com isso, caso
            #       o cenário corrente não esteja nessa amostra passar para o próximo
            if (list_cenarios_tipo_curva != None) and (all([StrHandler().find_substr_str(
                    extracted_file_tipo_curva_cenario.name, substr) == False for substr in
                    list_cenarios_tipo_curva])):
                continue
            #   abrindo em memória arquivo corrente - pandas dataframe
            reader = pd.read_csv(extracted_file_tipo_curva_cenario, sep=YAML_B3[
                'cenarios_risco_tipo_curva']['sep_instancias'], skiprows=1, header=None,
                names=YAML_B3['cenarios_risco_tipo_curva']['cols_cenarios_tipo_curvas'], decimal=',')
            df_cenarios_passagem = pd.DataFrame(reader)
            #   preenchendo com valor padrão campos com valor indisponível
            df_cenarios_passagem.fillna(YAML_B3['cenarios_risco_tipo_curva']['fill_na_padrao'],
                                        inplace=True)
            #   alterando para dicionário dataframe corrente, visando consolidar em uma lista e
            #       importar para uma dataframe para exportação
            list_passagem = df_cenarios_passagem.to_dict(orient=YAML_B3[
                'cenarios_risco_tipo_curva']['orient_df'])
            list_ser_exportacao_curvas.extend(list_passagem)
        # adicionando lista a um dataframe a ser exportado
        df_cenarios_tipo_curva = pd.DataFrame.from_dict(
            list_ser_exportacao_curvas)
        # alterando tipo de variáveis de colunas de interesse
        df_cenarios_tipo_curva = df_cenarios_tipo_curva.astype({
            YAML_B3['cenarios_risco_tipo_curva']['cols_cenarios_tipo_curvas'][0]: int,
            YAML_B3['cenarios_risco_tipo_curva']['cols_cenarios_tipo_curvas'][1]: int,
            YAML_B3['cenarios_risco_tipo_curva']['cols_cenarios_tipo_curvas'][2]: int,
            YAML_B3['cenarios_risco_tipo_curva']['cols_cenarios_tipo_curvas'][3]: int,
            YAML_B3['cenarios_risco_tipo_curva']['cols_cenarios_tipo_curvas'][4]: int,
            YAML_B3['cenarios_risco_tipo_curva']['cols_cenarios_tipo_curvas'][5]: int,
            YAML_B3['cenarios_risco_tipo_curva']['cols_cenarios_tipo_curvas'][6]: int,
            YAML_B3['cenarios_risco_tipo_curva']['cols_cenarios_tipo_curvas'][7]: float,
            YAML_B3['cenarios_risco_tipo_curva']['cols_cenarios_tipo_curvas'][8]: float,
        })
        # adding logging
        df_cenarios_tipo_curva = DBLogs().audit_log(
            df_cenarios_tipo_curva,
            url_cenarios_tipo_curva,
            DatesBR().utc_from_dt(DatesBR().curr_date)
        )
        # exportando dataframe de interesse
        return df_cenarios_tipo_curva

    def cenarios_risco_tipo_spot(self, list_cenarios_tipo_spot=None):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # setting variables
        list_ser_exportacao_curvas = list()
        # validando tipo de variáveis de interesse
        if list_cenarios_tipo_spot != None:
            #   convertendo para lista, caso não seja
            if type(list_cenarios_tipo_spot) != list:
                list_cenarios_tipo_spot = [list_cenarios_tipo_spot]
            #   alterando tipo de instâncias
            list_cenarios_tipo_spot = [str(x) for x in list_cenarios_tipo_spot]
        # definindo url com cenários de risco - tipo de curva
        url_cenarios_tipo_spot = YAML_B3['cenarios_risco_tipo_spot']['tipo_curva']['url'].format(
            DatesBR().sub_working_days(DatesBR().curr_date, YAML_B3['cenarios_risco_tipo_spot'][
                'tipo_curva']['self.int_wd_bef']).strftime('%y%m%d'))
        # baixando em memória zip com cenário de tipo curva da b3
        list_txts_tipos_spot_cenarios = RemoteFiles().get_zip_from_web_in_memory(
            url_cenarios_tipo_spot, bl_verify=YAML_B3['cenarios_risco_tipo_spot']['bl_verify'],
            bl_io_interpreting=YAML_B3['cenarios_risco_tipo_spot']['bl_io_interpreting'],
            timeout=YAML_B3['cenarios_risco_tipo_spot']['tipo_curva']['timeout'])
        # loopando em torno dos arquivos em txt, abrindo em memória no dataframe pandas
        for extracted_file_tipo_curva_cenario in list_txts_tipos_spot_cenarios:
            #   caso a variável list_cenarios_tipo_spot seja diferente de none, o usuário tem
            #       interesse que apenas uma parte dos cenários seja considerado, com isso, caso o
            #       cenário corrente não esteja nessa amostra passar para o próximo
            if (list_cenarios_tipo_spot != None) and (all([StrHandler().find_substr_str(
                    extracted_file_tipo_curva_cenario.name, substr) == False for substr in
                    list_cenarios_tipo_spot])):
                continue
            #   abrindo em memória arquivo corrente - pandas dataframe
            reader = pd.read_csv(extracted_file_tipo_curva_cenario, sep=YAML_B3[
                'cenarios_risco_tipo_spot']['sep_instancias'], skiprows=1, header=None,
                names=YAML_B3['cenarios_risco_tipo_spot']['cols_cenarios_tipo_curvas'], decimal=',')
            df_cenarios_passagem = pd.DataFrame(reader)
            #   preenchendo com valor padrão campos com valor indisponível
            df_cenarios_passagem.fillna(
                YAML_B3['cenarios_risco_tipo_spot']['fill_na_padrao'], inplace=True)
            #   alterando para dicionário dataframe corrente, visando consolidar em uma lista e
            #       importar para uma dataframe para exportação
            list_passagem = df_cenarios_passagem.to_dict(orient=YAML_B3[
                'cenarios_risco_tipo_spot']['orient_df'])
            list_ser_exportacao_curvas.extend(list_passagem)
        # adicionando lista a um dataframe a ser exportado
        df_cenarios_tipo_spot = pd.DataFrame.from_dict(
            list_ser_exportacao_curvas)
        # alterando tipo de variáveis de colunas de interesse
        df_cenarios_tipo_spot = df_cenarios_tipo_spot.astype({
            YAML_B3['cenarios_risco_tipo_spot']['cols_cenarios_tipo_curvas'][0]: int,
            YAML_B3['cenarios_risco_tipo_spot']['cols_cenarios_tipo_curvas'][1]: int,
            YAML_B3['cenarios_risco_tipo_spot']['cols_cenarios_tipo_curvas'][2]: int,
            YAML_B3['cenarios_risco_tipo_spot']['cols_cenarios_tipo_curvas'][3]: int,
            YAML_B3['cenarios_risco_tipo_spot']['cols_cenarios_tipo_curvas'][4]: int,
            YAML_B3['cenarios_risco_tipo_spot']['cols_cenarios_tipo_curvas'][5]: float,
            YAML_B3['cenarios_risco_tipo_spot']['cols_cenarios_tipo_curvas'][6]: float,
        })
        # adicionando colunas de interesse - nome tipo do cenário
        df_cenarios_tipo_spot[YAML_B3['cenarios_risco_tipo_spot']['col_criar_nome_tipo_cenario']] = \
            [YAML_B3['cenarios_risco_tipo_spot']['variaveis_tipo_cenarios_curvas'][x] for x in
                df_cenarios_tipo_spot[YAML_B3['cenarios_risco_tipo_spot'][
                    'cols_cenarios_tipo_curvas'][3]].tolist()]
        # adding logging
        df_cenarios_tipo_spot = DBLogs().audit_log(
            df_cenarios_tipo_spot,
            url_cenarios_tipo_spot,
            DatesBR().utc_from_dt(DatesBR().curr_date)
        )
        # exportando dataframe de interesse
        return df_cenarios_tipo_spot

    def fprs_cenarios_curva_spot_merge(self,
                                       list_cenarios_tipo_curva=['2962', '2906', '2945', '2924',
                                                                 '2944', '2925', '2946',
                                                                 '2901', '2902', '2934'],
                                       list_cenarios_tipo_spot=['2962', '2906', '2945', '2924',
                                                                '2944', '2925', '2946',
                                                                '2901', '2902', '2934'],
                                       int_cenario_sup_avaliacao=529, bl_debug=True):
        """
        DOCSTRING: UNINDO FATORES PRIMITIVOS DE RISCO, CURVAS SPOT E POR TIPO EM UM DATAFRAME COM
            OS IDS DE CENÁRIOS DE ITNERESSE
        INPUTS: -
        OUTPUTS: DATAFRAME
        """
        # criando dataframe de tipos de frps de interesse
        df_tipos_fprs = pd.DataFrame(
            YAML_B3['fatores_primitivos_risco_b3']['tipos_fprs'])
        # convertendo tipo de colunas de interesse
        df_tipos_fprs = df_tipos_fprs.astype({
            list(YAML_B3['fatores_primitivos_risco_b3']['tipos_fprs'].keys())[0]: int,
            list(YAML_B3['fatores_primitivos_risco_b3']['tipos_fprs'].keys())[1]: str,
        })
        if bl_debug == True:
            print('*** TIPO FRPS ***')
            print(df_tipos_fprs)
        # * arquivos de pregão b3 - cenários tipo curva
        df_cenarios_tipo_curva = self.cenarios_tipo_curva(list_cenarios_tipo_curva)
        # removendo colunas de interesse
        df_cenarios_tipo_curva.drop([
            YAML_B3['cenarios_risco_tipo_curva']['col_tipo'],
            YAML_B3['cenarios_risco_tipo_curva']['col_phi_2']
        ], axis=1, inplace=True)
        # limitando cenários de tipos de curvas dentro do range de interesse
        df_cenarios_tipo_curva = df_cenarios_tipo_curva[df_cenarios_tipo_curva[YAML_B3[
            'cenarios_risco_tipo_curva']['col_id_cenario']].isin(range(
                YAML_B3['cenarios_risco_tipo_curva']['int_inf_range_cenarios'],
                YAML_B3['cenarios_risco_tipo_curva']['int_sup_range_cenarios']))].reset_index(
                    drop=True)
        # eliminando cenários prospectivos
        df_cenarios_tipo_curva = df_cenarios_tipo_curva[df_cenarios_tipo_curva['ID_CENARIO'].isin(
            range(1, int_cenario_sup_avaliacao))].reset_index(drop=True)
        if bl_debug == True:
            print('*** TIPO CURVA ***')
            print(df_cenarios_tipo_curva.info())
            print(df_cenarios_tipo_curva)
        # * arquivos de pregão b3 - cenários risco tipo spot
        df_cenarios_tipo_spot = self.cenarios_risco_tipo_spot(list_cenarios_tipo_spot)
        # removendo colunas de interesse
        df_cenarios_tipo_spot.drop([
            YAML_B3['cenarios_risco_tipo_curva']['col_tipo'],
            YAML_B3['cenarios_risco_tipo_curva']['col_phi_2']
        ], axis=1, inplace=True)
        # limitando cenários de curvas spot
        df_cenarios_tipo_spot = df_cenarios_tipo_spot[df_cenarios_tipo_spot[YAML_B3[
            'cenarios_risco_tipo_spot']['col_id_cenario']].isin(range(
                YAML_B3['cenarios_risco_tipo_spot']['int_inf_range_cenarios'],
                YAML_B3['cenarios_risco_tipo_spot']['int_sup_range_cenarios']))].reset_index(
                    drop=True)
        # eliminando cenários prospectivos
        df_cenarios_tipo_spot = df_cenarios_tipo_spot[df_cenarios_tipo_spot['ID_CENARIO'].isin(
            range(1, int_cenario_sup_avaliacao))].reset_index(drop=True)
        if bl_debug == True:
            print('*** TIPO SPOT ***')
            print(df_cenarios_tipo_spot.info())
            print(df_cenarios_tipo_spot)
        # * arquivos de pregão b3 - fatores primitivos de risco
        df_fpr_b3 = self.fatores_primitivos_risco_b3
        # removendo colunas de interesse
        df_fpr_b3.drop([
            YAML_B3['fatores_primitivos_risco_b3']['col_tipo']
        ], axis=1, inplace=True)
        # limitando fprs de interesse para fins de cálculo de swaps
        df_fpr_b3 = df_fpr_b3[df_fpr_b3[
            YAML_B3['fatores_primitivos_risco_b3']['col_id_fpr']].isin(list(YAML_B3[
                'fatores_primitivos_risco_b3']['dict_fprs'].keys()))]
        if bl_debug == True:
            print('*** TIPO FPR ***')
            print(df_fpr_b3.info())
            print(df_fpr_b3)
        # * início tratamento de dados
        # inner-join fprs com dataframe de tipos de frps
        list_colunas_frps = df_fpr_b3.columns
        if bl_debug == True:
            print('*** COLUNAS FRPS ***')
            print(list_colunas_frps)
        list_colunas_frp = ['ID_FPR', 'ID_CENARIO', 'TIPO_FPR'] + list_colunas_frps.drop([
            'ID_FPR', 'NOME_FPR']).to_list()
        if bl_debug == True:
            print('*** TIPO FPR B3 ***')
            print(df_fpr_b3)
            print('*** TIPOS FPRS ***')
            print(df_tipos_fprs)
        df_fpr_b3 = df_fpr_b3.merge(
            df_tipos_fprs, how='inner', left_on='ID_FPR', right_on='ID_FPR')
        df_fpr_b3 = df_fpr_b3.reindex(columns=list_colunas_frp)
        # concat cenários tipo curva e spot
        df_cenarios_tipo_curva = pd.concat([df_cenarios_tipo_curva, df_cenarios_tipo_spot],
                                           ignore_index=True, sort=False)
        if bl_debug == True:
            print('*** CENÁRIOS TIPO CURVA & SPOT ***')
            print(df_cenarios_tipo_curva)
        # inner-join fprs com cenarios spot e tipo curvas
        df_fpr_b3 = df_fpr_b3.merge(df_cenarios_tipo_curva, how='inner', left_on='ID_FPR',
                                    right_on='ID_FPR')
        # preenchendo coluna nome fpr
        df_fpr_b3['NOME_FPR'] = [YAML_B3['fatores_primitivos_risco_b3']['dict_fprs'][x]
                                 for x in df_fpr_b3['ID_FPR']]
        # alterando tipo de colunas de interesse
        df_fpr_b3 = df_fpr_b3.astype({
            'DIAS_CORRIDOS_VERTICE': 'Int64',
            'DIAS_HOLDING_PERIOD': 'Int64',
            'DIAS_SAQUE_VERTICE': 'Int64',
        })
        # limitando colunas de interesse
        df_fpr_b3 = df_fpr_b3[[
            'ID_FPR',
            'NOME_FPR',
            'TIPO_FPR',
            'FORMATO_VARIACAO',
            'ID_GRUPO_FPR',
            'ID_CAMARA_INDICADOR',
            'ID_INSTRUMENTO_INDICADOR',
            'ORIGEM_INSTURMENTO_INDICADOR',
            'BASE',
            'BASE_INTERPOLACAO',
            'CRITERIO_CAPITALIZACAO',
            'ID_CENARIO_y',
            'INT_TIPO_CENARIO',
            'DIAS_CORRIDOS_VERTICE',
            'DIAS_HOLDING_PERIOD',
            'DIAS_SAQUE_VERTICE',
            'VALOR_PHI_1',
        ]]
        # alterando nome de colunas de interesse
        df_fpr_b3.rename(columns={
            'ID_CENARIO_y': 'ID_CENARIO'
        }, inplace=True)
        if bl_debug == True:
            print('*** DF FPRS CONSOLIDADO ***')
            print(df_fpr_b3)
        # adding logging
        df_fpr_b3 = DBLogs().audit_log(
            df_fpr_b3,
            None,
            DatesBR().utc_from_dt(DatesBR().curr_date)
        )
        # retornando fpr completo
        return df_fpr_b3

    def cenarios_risco_tipo_spot(self, list_cenarios_tipo_spot=None):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # setting variables
        list_ser_exportacao_curvas = list()
        # validando tipo de variáveis de interesse
        if list_cenarios_tipo_spot != None:
            #   convertendo para lista, caso não seja
            if type(list_cenarios_tipo_spot) != list:
                list_cenarios_tipo_spot = [list_cenarios_tipo_spot]
            #   alterando tipo de instâncias
            list_cenarios_tipo_spot = [str(x) for x in list_cenarios_tipo_spot]
        try:
            # definindo url com cenários de risco - tipo de curva
            url_cenarios_tipo_spot = YAML_B3['cenarios_risco_tipo_spot']['tipo_curva'][
                'url'].format(DatesBR().sub_working_days(
                    DatesBR().curr_date, YAML_B3['cenarios_risco_tipo_spot'][
                        'tipo_curva']['self.int_wd_beferiores']).strftime('%y%m%d'))
            # baixando em memória zip com cenário de tipo curva da b3
            list_txts_tipos_spot_cenarios = RemoteFiles().get_zip_from_web_in_memory(
                url_cenarios_tipo_spot, bl_verify=YAML_B3['cenarios_risco_tipo_spot']['bl_verify'],
                bl_io_interpreting=YAML_B3['cenarios_risco_tipo_spot']['bl_io_interpreting'],
                timeout=YAML_B3['cenarios_risco_tipo_spot']['tipo_curva']['timeout'])
        except:
            url_cenarios_tipo_spot = YAML_B3['cenarios_risco_tipo_spot'][
                'tipo_curva']['url'].format(
            DatesBR().sub_working_days(DatesBR().curr_date, (YAML_B3['cenarios_risco_tipo_spot'][
                'tipo_curva']['self.int_wd_beferiores'] + 1)).strftime('%y%m%d'))
            list_txts_tipos_spot_cenarios = RemoteFiles().get_zip_from_web_in_memory(
                url_cenarios_tipo_spot, bl_verify=YAML_B3['cenarios_risco_tipo_spot']['bl_verify'],
                bl_io_interpreting=YAML_B3['cenarios_risco_tipo_spot']['bl_io_interpreting'],
                timeout=YAML_B3['cenarios_risco_tipo_spot']['tipo_curva']['timeout'])
        # loopando em torno dos arquivos em txt, abrindo em memória no dataframe pandas
        for extracted_file_tipo_curva_cenario in list_txts_tipos_spot_cenarios:
            #   caso a variável list_cenarios_tipo_spot seja diferente de none, o usuário tem
            #       interesse que apenas uma parte dos cenários seja considerado, com isso, caso
            #       o cenário corrente não esteja nessa amostra passar para o próximo
            if (list_cenarios_tipo_spot != None) and (all([StrHandler().find_substr_str(
                extracted_file_tipo_curva_cenario.name, substr) == False for substr in
                list_cenarios_tipo_spot])): continue
            #   abrindo em memória arquivo corrente - pandas dataframe
            reader = pd.read_csv(extracted_file_tipo_curva_cenario, sep=YAML_B3[
                'cenarios_risco_tipo_spot'][
                'sep_instancias'], skiprows=1, header=None, names=YAML_B3[
                    'cenarios_risco_tipo_spot']['cols_cenarios_tipo_curvas'], decimal=',')
            df_cenarios_passagem = pd.DataFrame(reader)
            #   preenchendo com valor padrão campos com valor indisponível
            df_cenarios_passagem.fillna(YAML_B3['cenarios_risco_tipo_spot']['fill_na_padrao'],
                                        inplace=True)
            #   alterando para dicionário dataframe corrente, visando consolidar em uma lista
            #       e importar para uma dataframe para exportação
            list_passagem = df_cenarios_passagem.to_dict(orient=YAML_B3[
                'cenarios_risco_tipo_spot']['orient_df'])
            list_ser_exportacao_curvas.extend(list_passagem)
        # adicionando lista a um dataframe a ser exportado
        df_cenarios_tipo_spot = pd.DataFrame.from_dict(list_ser_exportacao_curvas)
        # alterando tipo de variáveis de colunas de interesse
        df_cenarios_tipo_spot = df_cenarios_tipo_spot.astype({
            YAML_B3['cenarios_risco_tipo_spot']['cols_cenarios_tipo_curvas'][0]: int,
            YAML_B3['cenarios_risco_tipo_spot']['cols_cenarios_tipo_curvas'][1]: int,
            YAML_B3['cenarios_risco_tipo_spot']['cols_cenarios_tipo_curvas'][2]: int,
            YAML_B3['cenarios_risco_tipo_spot']['cols_cenarios_tipo_curvas'][3]: int,
            YAML_B3['cenarios_risco_tipo_spot']['cols_cenarios_tipo_curvas'][4]: int,
            YAML_B3['cenarios_risco_tipo_spot']['cols_cenarios_tipo_curvas'][5]: float,
            YAML_B3['cenarios_risco_tipo_spot']['cols_cenarios_tipo_curvas'][6]: float,
        })
        # adicionando colunas de interesse - nome tipo do cenário
        df_cenarios_tipo_spot[YAML_B3['cenarios_risco_tipo_spot'][
            'col_criar_nome_tipo_cenario']] = \
            [YAML_B3['cenarios_risco_tipo_spot']['variaveis_tipo_cenarios_curvas'][x] for x in
                df_cenarios_tipo_spot[YAML_B3['cenarios_risco_tipo_spot'][
                    'cols_cenarios_tipo_curvas'][3]].tolist()]
        # adding logging
        df_cenarios_tipo_spot = DBLogs().audit_log(
            df_cenarios_tipo_spot,
            url_cenarios_tipo_spot,
            DatesBR().utc_from_dt(DatesBR().curr_date)
        )
        # exportando dataframe de interesse
        return df_cenarios_tipo_spot


# print(self.cenarios_tipo_curva(list_cenarios_tipo_curva=[2939, 2940, 2941]))
# print(self.cenarios_risco_tipo_spot(list_cenarios_tipo_spot=[3012, 3134, 3135]))
# df_fpr_b3 = self.fprs_cenarios_curva_spot_merge
# print(df_fpr_b3)
# df_mtm_b3 = self.mtm_compra_venda()
# df_mtm_b3.to_excel(r"D:\Downloads\mtm-b3_20220928_1426.xlsx", index=False)
# print(self.collateral_acc_spot_bov_b3)
# df_fpr_b3 = self.fprs_cenarios_curva_spot_merge()
# df_fpr_b3.to_excel(
#     r"D:\Downloads\teste-fpr-b3_20220929_1832.xlsx", index=False)
# print(self.mtm_compra_venda())
# print(self.fatores_primitivos_risco_b3())
