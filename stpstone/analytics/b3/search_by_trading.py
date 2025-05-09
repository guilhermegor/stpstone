#### SEARCH BY TRADING - B3

import pandas as pd
import numpy as np
from zipfile import ZipFile
from stpstone._config.global_slots import YAML_B3
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.handling_data.lists import HandlingLists
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

    def options_b3(self, float_value_error=-1000.0, bl_debug=False,
                   float_days_year=365.0):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # setting variables
        list_ser = list()
        # raw maximum theoretical margins
        list_rows_mtm = self.carga_mtm_b3
        # b3 financial indicators
        df_fin_ind = MDB3().financial_indicators_b3
        # risk-free rate
        r = df_fin_ind[df_fin_ind[YAML_B3['financial_indicators']['col_si_code']] \
                       == YAML_B3['financial_indicators']['int_si_code_selic']][
                           YAML_B3['financial_indicators']['col_rate']
                       ].tolist()[0]
        if bl_debug == True:
            print('RISK-FREE RATE: {}'.format(r))
        # options traded in b3 exchange - in zip file
        url = YAML_B3['options_traded_b3'][
            'url'].format(DatesBR().sub_working_days(DatesBR().curr_date,
                                                     self.int_wd_bef).strftime('%y%m%d'))
        # carga para a memória margens teóricas máximas b3
        zipfile = RemoteFiles().get_zip_from_web_in_memory(
            url, bl_io_interpreting=YAML_B3[
                'options_traded_b3']['bl_io_interpreting'],
            bl_verify=YAML_B3['options_traded_b3']['bl_verify']
        )
        # retorno é um novo zip com dois xmls
        zipfile = ZipFile(zipfile)
        # iterate through the files in the zip
        for file_info in zipfile.infolist():
            #   opennig just xml files
            if file_info.filename.endswith('.xml'):
                xml_file = zipfile.open(file_info)
                xml_file = xml_file.read()
            else:
                continue
            #   soup parser to xml trading options
            soup_xml = XMLFiles().xml_memory_parser(xml_file)
            #   looping through tags of BOV & BMF markets
            for key, value in YAML_B3['options_traded_b3']['tags'].items():
                #   finding key of interest within soup
                for soup_content in soup_xml.find_all(key):
                    #   reseting variables
                    dict_ = dict()
                    bl_break_opt_price = False
                    bl_break_spt_price = False
                    #   adding all tags availabe to the dictionary
                    for tag in value:
                        dict_[tag] = soup_content.find(tag).get_text()
                        if bl_debug == True:
                            print(soup_content.find(tag).get_text())
                    if bl_debug == True:
                        print('ISIN: {}'.format(soup_content.find(YAML_B3['options_traded_b3'][
                            'keys']['isin']).get_text()))
                        print('TICKER: {}'.format(dict_[YAML_B3['options_traded_b3']['keys'][
                            'ticker']]))
                    #   spot ticker
                    str_isin = soup_content.find(YAML_B3['options_traded_b3']['keys']['isin']).get_text()
                    dict_[YAML_B3['options_traded_b3']['keys']['spot']] = \
                        str_isin[2:6] + str_isin[6].replace('9', '11')
                    if bl_debug == True:
                        print('SPOT TICKER: {}'.format(dict_[YAML_B3['options_traded_b3'][
                            'keys']['spot']]))
                    #   calendar days to expiration
                    dict_[YAML_B3['options_traded_b3']['keys']['cd_exp']] = \
                        DatesBR().delta_calendar_days(
                            DatesBR().sub_working_days(
                                DatesBR().curr_date, abs(-self.int_wd_bef-1)),
                                DatesBR().str_date_to_datetime(str(dict_[
                                        YAML_B3['options_traded_b3']['keys']['xpr_dt']
                                    ]), YAML_B3['options_traded_b3']['dt_input_format']
                            )
                        )
                    if bl_debug == True:
                        print('CALENDAR DAYS TO EXPIRATION: {}'.format(
                                DatesBR().delta_calendar_days(
                                DatesBR().sub_working_days(
                                    DatesBR().curr_date, abs(-self.int_wd_bef-1)),
                                    DatesBR().str_date_to_datetime(str(dict_[
                                            YAML_B3['options_traded_b3']['keys']['xpr_dt']
                                        ]), YAML_B3['options_traded_b3']['dt_input_format']
                                    )
                                )
                            )
                        )
                    #   closing price of spot and option
                    while \
                        (bl_break_spt_price == False) \
                        and (bl_break_opt_price == False):
                        for i, _ in enumerate(list_rows_mtm):
                            if i < len(list_rows_mtm) - 3:
                                #   option close
                                if list_rows_mtm[i][0] == YAML_B3['margens_teoricas_maximas_b3'][
                                    'key_instrumentos'] \
                                    and list_rows_mtm[i][4] != '0' \
                                    and list_rows_mtm[i][5] == dict_[
                                        YAML_B3['options_traded_b3']['keys']['ticker']]:
                                    dict_[YAML_B3['options_traded_b3']['keys']['close_opt']] = \
                                        float(list_rows_mtm[i][4].replace(",", "."))
                                    bl_break_opt_price = True
                                    if bl_debug == True:
                                        print('OPT CLOSE: {}'.format(float(
                                            list_rows_mtm[i][4].replace(",", "."))))
                                #   spot close
                                if list_rows_mtm[i][0] == YAML_B3['margens_teoricas_maximas_b3'][
                                    'key_instrumentos'] \
                                    and list_rows_mtm[i][4] != '0' \
                                    and list_rows_mtm[i][5] == dict_[YAML_B3['options_traded_b3'][
                                        'keys']['spot']]:
                                    dict_[YAML_B3['options_traded_b3']['keys']['close_spot']] = \
                                        float(list_rows_mtm[i][4].replace(",", "."))
                                    bl_break_spt_price = True
                                    if bl_debug == True:
                                        print('SPOT CLOSE: {}'.format(float(
                                            list_rows_mtm[i][4].replace(",", "."))))
                    #   risk-free rate
                    dict_[YAML_B3['options_traded_b3']['keys']['risk_free']] = r
                    #   days to maturity ratio
                    dict_[YAML_B3['options_traded_b3']['keys']['days_maturity_ratio']] = \
                        float(dict_[YAML_B3['options_traded_b3']['keys']['cd_exp']]) \
                        / float_days_year
                    #   implied volatility
                    if bl_debug == True:
                        print('*** INPUTS VOL IMP ***')
                        print(
                            float(dict_[YAML_B3['options_traded_b3']['keys']['close_spot']]),
                            float(dict_[YAML_B3['options_traded_b3']['keys']['k']]),
                            float(r),
                            float(dict_[YAML_B3['options_traded_b3']['keys'][
                                'days_maturity_ratio']]),
                            float(0.5),
                            float(0),
                            float(r),
                            float(dict_[YAML_B3['options_traded_b3']['keys']['close_opt']]),
                            str(dict_[YAML_B3['options_traded_b3']['keys'][
                                'opt_type']].lower().replace('putt', 'put'))
                        )
                    try:
                        dict_[YAML_B3['options_traded_b3']['keys']['imp_vol']] = \
                            EuropeanOptions().implied_volatility(
                                float(dict_[YAML_B3['options_traded_b3']['keys']['close_spot']]),
                                float(dict_[YAML_B3['options_traded_b3']['keys']['k']]),
                                float(r),
                                float(float(dict_[YAML_B3['options_traded_b3']['keys']['cd_exp']]) \
                                    / 365.0),
                                float(0.5),
                                float(0),
                                float(r),
                                float(dict_[YAML_B3['options_traded_b3']['keys']['close_opt']]),
                                str(dict_[YAML_B3['options_traded_b3']['keys']['opt_type']
                                        ].lower().replace('putt', 'put')
                                )
                            )
                    except KeyError:
                        dict_[YAML_B3['options_traded_b3']['keys']['imp_vol']] = float_value_error
                    #   delta
                    try:
                        dict_[YAML_B3['options_traded_b3']['keys']['delta']] = \
                            EuropeanOptions().delta(
                                float(dict_[YAML_B3['options_traded_b3']['keys']['close_spot']]),
                                float(dict_[YAML_B3['options_traded_b3']['keys']['k']]),
                                float(r),
                                float(float(dict_[YAML_B3['options_traded_b3']['keys']['cd_exp']]) \
                                    / 365.0),
                                dict_[YAML_B3['options_traded_b3']['keys']['imp_vol']],
                                float(0),
                                float(r),
                                str(dict_[YAML_B3['options_traded_b3']['keys']['opt_type']
                                    ].lower().replace('putt', 'put')
                                )
                            )
                    except KeyError:
                        dict_[YAML_B3['options_traded_b3']['keys']['delta']] = float_value_error
                    if bl_debug == True:
                        print('DELTA: {}'.format(dict_[YAML_B3['options_traded_b3']['keys'][
                            'delta']]))
                        print('IV: {}'.format(dict_[YAML_B3['options_traded_b3']['keys'][
                            'imp_vol']]))
                    #   appendig dictionary to list
                    list_ser.append(dict_)
                    if bl_debug == True:
                        print(dict_)
        # uploading list of dictionaries to pandas
        df_opt_in = pd.DataFrame(list_ser)
        # removing duplicates
        df_opt_in.drop_duplicates(inplace=True)
        # adding logging
        df_opt_in = DBLogs().audit_log(
            df_opt_in,
            url,
            DatesBR().utc_from_dt(
                DatesBR().sub_working_days(DatesBR().curr_date, self.int_wd_bef)
            )
        )
        # returning dataframe
        return df_opt_in

    @property
    def carga_mtm_b3(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # url de exportação do arquivo de margens teóricas máximas b3
        url_mtm = YAML_B3['margens_teoricas_maximas_b3'][
            'url'].format(DatesBR().sub_working_days(DatesBR().curr_date,
                                                     self.int_wd_bef).strftime('%y%m%d'))
        # carga para a memória margens teóricas máximas b3
        zipfile = RemoteFiles().get_zip_from_web_in_memory(
            url_mtm, bl_io_interpreting=YAML_B3[
                'margens_teoricas_maximas_b3']['bl_io_interpreting'],
            bl_verify=YAML_B3['margens_teoricas_maximas_b3']['bl_verify'])
        # retorno é um novo zip com um xlsx
        zipfile = ZipFile(zipfile)
        # coletando nome do arquivo txt contido dentro do zip
        list_zip_files = zipfile.namelist()
        file_name = list_zip_files.pop()
        # extraindo arquivo com fatores de risco primitivos b3 - txt
        name_file_extract = zipfile.open(file_name)
        # lendo csv com margens teóricas máximas b3
        list_rows_mtm = [
            str(str_).replace(r'\n', '').replace(
                "b'", '').replace("'", '').split(';')
            for str_ in name_file_extract.readlines() if len(
                str(str_).replace(r'\n', '').replace("b'", '').replace(
                    "'", '').split(';')) > 1
        ]
        list_rows_mtm = list_rows_mtm[1:]
        # retornando lista de linhas do csv corrente
        return list_rows_mtm

    @property
    def mtm_compra_venda(self, den_desagios=None,
                         key_stocks='stocks', key_funds='funds', key_etfs='etfs', key_bdrs='bdrs'):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS
        """
        # setting variables
        list_num_desagios_compra = list()
        list_num_desagios_venda = list()
        list_exportacao = list()
        # carga de margens teóricas máximas crua
        list_rows_mtm = self.carga_mtm_b3
        # carga de ativos bovespa e bmf por tipo
        dict_ativos_bov_bmf_tipo = UP2DATAB3().security_category_name
        # selecionando apenas lista de ativos de interesse - ações, fiis, etfs, e bdrs
        list_ativos_bov_vista = HandlingLists().extend_lists(
            dict_ativos_bov_bmf_tipo[key_stocks], dict_ativos_bov_bmf_tipo[key_funds],
            dict_ativos_bov_bmf_tipo[key_etfs], dict_ativos_bov_bmf_tipo[key_bdrs])
        # removendo eventuais duplicatas
        list_ativos_bov_vista = HandlingLists().remove_duplicates(list_ativos_bov_vista)
        # alocando margens teóricas máximas em lista de dicionários
        for i, _ in enumerate(list_rows_mtm):
            if i < len(list_rows_mtm) - 3:
                #   setando variáveis de passagem
                dict_exportacao = dict()
                #   margem B3 requerida para passar posicionado no papel
                if list_rows_mtm[i][0] == YAML_B3['margens_teoricas_maximas_b3'][
                    'key_instrumentos'] \
                    and list_rows_mtm[i][4] != '0' \
                    and (len(list_rows_mtm[i][5]) == 5 or
                         len(list_rows_mtm[i][5]) == 6):
                    den_desagios = float(
                        list_rows_mtm[i][4].replace(",", "."))
                #   numerador de preço com choque para cálculo de deságio por cenário
                #       dias úteis considerados: 1 a 10 dias úteis
                #       garantias exigidas calculadas de compra e venda
                for dias_uteis_choque in range(1, 11):
                    try:
                        if int(list_rows_mtm[i + dias_uteis_choque][1]) == \
                            dias_uteis_choque \
                                and list_rows_mtm[i + dias_uteis_choque][2] != '':
                            list_num_desagios_compra.append(float(
                                list_rows_mtm[i + dias_uteis_choque][2].replace(",", ".")))
                        if int(list_rows_mtm[i + dias_uteis_choque][
                            1]) == dias_uteis_choque and list_rows_mtm[
                                i + dias_uteis_choque][3] != '':
                            list_num_desagios_venda.append(float(
                                list_rows_mtm[i + dias_uteis_choque][3].replace(",", ".")))
                    except:
                        pass
                # executar o loop para listas vazias
                if (list_num_desagios_compra != []) and (list_num_desagios_venda != []):
                    # denominador com preço spot no fechamento para cálculo do deságio
                    #   B3 por cenário
                    if (list_rows_mtm[i][0] == YAML_B3['margens_teoricas_maximas_b3'][
                        'key_instrumentos']) and (list_rows_mtm[i][4] != '0') \
                            and all([type(x) == float for x in
                                     list_num_desagios_compra]) and (type(
                            den_desagios) == float):
                        # setando dicionário para o ticker de interesse
                        dict_exportacao[YAML_B3['margens_teoricas_maximas_b3'][
                            'col_incluir_ticker']] = list_rows_mtm[i][5]
                        # definindo preço de fechamento de referência
                        dict_exportacao['PRECO_FECHAMENTO'] = den_desagios
                        # definindo deságios para cenários de choque para o período de
                        #   interesse
                        for dias_uteis_choque in range(1, 11):
                            # deságio de compra/venda para ativo corrente negociado em mercado
                            #   secundário da b3
                            if list_rows_mtm[i][5] in list_ativos_bov_vista:
                                dict_exportacao[YAML_B3['margens_teoricas_maximas_b3'][
                                    'col_incluir_margem_b3_compra_d_mais_n'] + str(
                                    dias_uteis_choque)] = abs(den_desagios * (
                                        1 - list_num_desagios_compra[
                                            dias_uteis_choque - 1] / den_desagios))
                                dict_exportacao[YAML_B3['margens_teoricas_maximas_b3'][
                                    'col_incluir_desagio_b3_compra_d_mais_n'] + str(
                                    dias_uteis_choque)] = min([1, abs(1 - list_num_desagios_compra[
                                        dias_uteis_choque - 1] / den_desagios)])
                                dict_exportacao[YAML_B3['margens_teoricas_maximas_b3'][
                                    'col_incluir_margem_b3_venda_d_mais_n'] + str(
                                    dias_uteis_choque)] = \
                                    abs(den_desagios * (
                                        1 - list_num_desagios_venda[
                                            dias_uteis_choque - 1] / den_desagios))
                                dict_exportacao[YAML_B3['margens_teoricas_maximas_b3'][
                                    'col_incluir_desagio_b3_venda_d_mais_n'] + str(
                                    dias_uteis_choque)] = \
                                    min([1, abs(1 - list_num_desagios_venda[
                                        dias_uteis_choque - 1] / den_desagios)])
                            else:
                                dict_exportacao[YAML_B3['margens_teoricas_maximas_b3'][
                                    'col_incluir_margem_b3_compra_d_mais_n'] + str(
                                    dias_uteis_choque)] = abs(
                                    -list_num_desagios_compra[
                                        dias_uteis_choque - 1])
                                dict_exportacao[YAML_B3['margens_teoricas_maximas_b3'][
                                    'col_incluir_desagio_b3_compra_d_mais_n'] + str(
                                    dias_uteis_choque)] = \
                                    min([1, abs(list_num_desagios_compra[
                                        dias_uteis_choque - 1] / den_desagios - 1)])
                                dict_exportacao[YAML_B3['margens_teoricas_maximas_b3'][
                                    'col_incluir_margem_b3_venda_d_mais_n'] + str(
                                    dias_uteis_choque)] = \
                                    abs(-list_num_desagios_venda[dias_uteis_choque - 1])
                                dict_exportacao[YAML_B3['margens_teoricas_maximas_b3'][
                                    'col_incluir_desagio_b3_venda_d_mais_n'] + str(
                                    dias_uteis_choque)] = \
                                    min([1, abs(list_num_desagios_venda[
                                        dias_uteis_choque - 1] / den_desagios - 1)])
                    # appendando à lista de exportação
                    list_exportacao.append(dict_exportacao)
                    # ressetiando variável de passagem
                    list_num_desagios_compra = list()
                    list_num_desagios_venda = list()
        # appendando lista de exportação a um dataframe
        df_mtm_b3 = pd.DataFrame(list_exportacao)
        # drop not available rows
        df_mtm_b3.dropna(inplace=True)
        # sort tickers in alphabetic order
        df_mtm_b3.sort_values([YAML_B3['margens_teoricas_maximas_b3'][
            'col_incluir_ticker']], ascending=[True], inplace=True)
        # adding logging
        df_mtm_b3 = DBLogs().audit_log(
            df_mtm_b3,
            YAML_B3['margens_teoricas_maximas_b3'][
                'url'].format(DatesBR().sub_working_days(
                    DatesBR().curr_date, self.int_wd_bef).strftime('%y%m%d')),
            DatesBR().utc_from_dt(
                DatesBR().sub_working_days(DatesBR().curr_date, self.int_wd_bef)
            )
        )
        # retornando deságios b3 de interesse
        return df_mtm_b3

    @property
    def collateral_acc_spot_bov_b3(self, dict_repalce_str_erros={
            'ETF': ''}, col_ticker='TICKER', col_isin='ISIN', col_limite_qtds='LIMITE_QUANTIDADES',
            col_mes_arquivo='MES_ARQUIVO', key_limite_qtds='Limite (quantidade)'):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # setting variables
        list_exportacao = list()
        # importing to memory html, in order to catch the url of the collateral accepted by b3,
        #   regarding spot bov
        html_parser_acoes_units_etfs_aceitos_garantia_b3 = HtmlHandler().html_lxml_parser(
            YAML_B3['collateral_acc_spot_bov_b3']['url'], bl_verify=False)
        url_acoes_units_etfs_aceitos_garantia_b3 = HtmlHandler().html_lxml_xpath(
            html_parser_acoes_units_etfs_aceitos_garantia_b3,
            YAML_B3['collateral_acc_spot_bov_b3'][
                'xpath_url_acoes_units_etfs_aceitos_garantia_b3'])[0]
        url_acoes_units_etfs_aceitos_garantia_b3 = StrHandler().get_string_after_substr(
            url_acoes_units_etfs_aceitos_garantia_b3, YAML_B3['collateral_acc_spot_bov_b3'][
            'data_files_collateral_acc_spot_bov_b3'])
        # creating complete url
        url_acoes_units_etfs_aceitos_garantia_b3 = \
            YAML_B3['collateral_acc_spot_bov_b3']['url_raiz_documentos_b3'] \
            + YAML_B3['collateral_acc_spot_bov_b3'][
            'data_files_collateral_acc_spot_bov_b3'] + \
            url_acoes_units_etfs_aceitos_garantia_b3
        # openning zip in memory
        list_unzipped_files = RemoteFiles().get_zip_from_web_in_memory(
            url_acoes_units_etfs_aceitos_garantia_b3, bl_verify=False)
        if str(type(list_unzipped_files)) != "<class 'list'>":
            list_unzipped_files = [list_unzipped_files]
        # looping within files in zip
        for file_unz in list_unzipped_files[1:]:
            #   creating columns month-file
            str_mes_arquivo_garantias_b3_corrente = StrHandler().find_between(
                str(file_unz), '_', '.xlsx').replace(' ', '').lower()
            #   looping within sheet names
            for nome_plan_ativa in [
                StrHandler().latin_characters(
                    YAML_B3['collateral_acc_spot_bov_b3']['plan_acoes_bdrs_etfs_units']),
                YAML_B3['collateral_acc_spot_bov_b3']['plan_adrs']
            ]:
                #   defining column ticker
                if nome_plan_ativa == 'ADR':
                    col_cod_ticker = 'Código'
                else:
                    col_cod_ticker = 'Código'
                #   openning dataframe in memory
                reader = pd.read_excel(
                    file_unz, sheet_name=nome_plan_ativa)
                df_coll_acc_spot_bov = pd.DataFrame(reader)
                #   creating serialized list with spot bov collateral accepted
                for _, row in df_coll_acc_spot_bov.iterrows():
                    list_exportacao.append({
                        col_mes_arquivo: str_mes_arquivo_garantias_b3_corrente.upper(),
                        col_ticker: row[col_cod_ticker],
                        col_isin: row[col_isin],
                        col_limite_qtds: row[key_limite_qtds]
                    })
        # creating dataframe
        df_coll_acc_spot_bov = pd.DataFrame(list_exportacao)
        # removing duplicates
        df_coll_acc_spot_bov[col_ticker] = [StrHandler().replace_all(
            str(x), dict_repalce_str_erros) for x in df_coll_acc_spot_bov[
                col_ticker].tolist()]
        # adding logging
        df_fpr_b3 = DBLogs().audit_log(
            df_fpr_b3,
            url_acoes_units_etfs_aceitos_garantia_b3,
            DatesBR().utc_from_dt(DatesBR().curr_date)
        )
        # returning dataframe
        return df_coll_acc_spot_bov

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
