import pandas as pd
from datetime import datetime
from typing import Optional, List
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response, request
from time import sleep
from zipfile import ZipFile
from stpstone._config.global_slots import YAML_B3_SEARCH_BY_TRADING_SESSION
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.parsers.lists import ListHandler
from stpstone.utils.parsers.folders import RemoteFiles
from stpstone.utils.parsers.object import HandlingObjects
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.str import StrHandler


class SearchByTradingB3(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        dt_ref:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None,
        token:Optional[str]=None,
        list_slugs:Optional[List[str]]=None
    ) -> None:
        super().__init__(
            dict_metadata=YAML_B3_SEARCH_BY_TRADING_SESSION,
            session=session,
            dt_ref=dt_ref,
            cls_db=cls_db,
            logger=logger,
            token=token,
            list_slugs=list_slugs
        )
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.token = token,
        self.list_slugs = list_slugs
        self.dt_ref_yymmdd = self.dt_ref.strftime('%y%m%d')
        self.dt_inf_month = DatesBR().dates_inf_sup_month(self.dt_ref)[0]
        self.dt_inf_month_yymmdd = self.dt_inf_month.strftime('%y%m%d')

    def instruments_register_raw(self, method_request='GET', key_token='token',
                                 key_ticker_symbol='TckrSymb'):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # api consulta lotes padrão xp
        url_token = "https://arquivos.b3.com.br/api/download/requestname?fileName=InstrumentsConsolidated&date={}".format(
            DatesBR().curr_date)
        url_lotes_padrao_b3 = "https://arquivos.b3.com.br/api/download/?token={}"
        # coletando token para consulta da API da B3
        token_api_b3 = \
            HandlingObjects().literal_eval_data(
                request(method_request, url_token).text)[key_token]
        url_lotes_padrao_b3 = str(url_lotes_padrao_b3).format(token_api_b3)
        # print('URL LOTE PADRAO B3: {}'.format(url_lotes_padrao_b3))
        req_resp = request(
            method_request, url_lotes_padrao_b3.format(token_api_b3))
        req_resp.raise_for_status()
        req_resp_text = req_resp.content.decode('iso-8859-1')
        # print('RESPONSE API B3 LOTE PADRAO: {}'.format(req_resp_text[:1000]))
        # tratando dados provindos da API
        list_response_api_b3 = req_resp_text.split("\n")
        list_headers = list_response_api_b3[1].split(";")
        # print(f"LIST HEADERS: {list_headers[:10]}")
        list_response_api_b3 = list_response_api_b3[2:]
        # print(f"LIST RESPONSE API B3: {list_response_api_b3[:10]}")
        list_ser = list()
        for row in list_response_api_b3:
            list_row = row.split(';')
            list_ser.append(dict(zip(list_headers, list_row)))
        # criando dataframe à partir do dicionário de interesse
        df_instruments_register_b3_raw = pd.DataFrame(list_ser)
        # changing column types
        list_cols_df = list(df_instruments_register_b3_raw.columns)
        df_instruments_register_b3_raw = df_instruments_register_b3_raw.astype(
            dict(zip(list_cols_df, [str] * len(list_cols_df))))
        # retornando dicionário com dados cadastrais de instrumentos negociados na b3
        return df_instruments_register_b3_raw

    def security_category_name(self, dict_export=dict(), key_ticker='TckrSymb',
                               col_security_category_name='SctyCtgyNm',
                               col_market_name='MktNm',
                               list_markets_classified=list(),
                               bl_return_markets_not_classified=False):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # fetch in memory instruments register of assets traded in b3 exchange
        df_instruments_register_b3_raw = self.instruments_register_raw()
        # print('*** REIGSTER B3 RAW ***')
        # print(df_instruments_register_b3_raw)
        # creating dictionary with instruments according to each type of market
        for security_division, col_ in [
            ('securities_by_category_name', col_security_category_name),
                ('securities_by_market_name', col_market_name)]:
            for market, list_source_names in YAML_B3_SEARCH_BY_TRADING_SESSION['up2data_b3'][
                security_division].items():
                #   validating wheter the market is already in the exporting dictionary and extending
                #       or creating a new list
                if market not in dict_export:
                    dict_export[market] = ListHandler().remove_duplicates(
                        df_instruments_register_b3_raw[
                            df_instruments_register_b3_raw[col_].isin(
                                list_source_names)][key_ticker].tolist())
                else:
                    dict_export[market].extend(ListHandler().remove_duplicates(
                        df_instruments_register_b3_raw[
                            df_instruments_register_b3_raw[col_].isin(
                                list_source_names)][key_ticker].tolist()))
                list_markets_classified.extend(list_source_names)
        # defining markets not classified, if is user's will
        if bl_return_markets_not_classified == True:
            return ListHandler().remove_duplicates(
                df_instruments_register_b3_raw[
                    ~df_instruments_register_b3_raw[col_security_category_name].isin(
                        list_markets_classified)][col_security_category_name].tolist())
        else:
            #   returing dictionary with tickers accordingly to the market type which participates
            return dict_export

    def carga_mtm_b3(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # url de exportação do arquivo de margens teóricas máximas b3
        url_mtm = "https://www.b3.com.br/pesquisapregao/download?filelist=MT{}.zip".format(
            self.dt_ref.strftime('%y%m%d'))
        # carga para a memória margens teóricas máximas b3
        req_resp = request("GET", url_mtm)
        zipfile = RemoteFiles().get_zip_from_web_in_memory(
            req_resp, bl_io_interpreting=False)
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
        list_rows_mtm = self.carga_mtm_b3()
        # carga de ativos bovespa e bmf por tipo
        dict_ativos_bov_bmf_tipo = self.security_category_name()
        # selecionando apenas lista de ativos de interesse - ações, fiis, etfs, e bdrs
        list_ativos_bov_vista = ListHandler().extend_lists(
            dict_ativos_bov_bmf_tipo[key_stocks], dict_ativos_bov_bmf_tipo[key_funds],
            dict_ativos_bov_bmf_tipo[key_etfs], dict_ativos_bov_bmf_tipo[key_bdrs])
        # removendo eventuais duplicatas
        list_ativos_bov_vista = ListHandler().remove_duplicates(list_ativos_bov_vista)
        # alocando margens teóricas máximas em lista de dicionários
        for i, _ in enumerate(list_rows_mtm):
            if i < len(list_rows_mtm) - 3:
                #   setando variáveis de passagem
                dict_exportacao = dict()
                #   margem B3 requerida para passar posicionado no papel
                if list_rows_mtm[i][0] == "INSTRUMENT" \
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
                    if (list_rows_mtm[i][0] == "INSTRUMENT") and (list_rows_mtm[i][4] != '0') \
                            and all([type(x) == float for x in
                                     list_num_desagios_compra]) and (type(
                            den_desagios) == float):
                        # setando dicionário para o ticker de interesse
                        dict_exportacao["TICKER"] = list_rows_mtm[i][5]
                        # definindo preço de fechamento de referência
                        dict_exportacao["PRECO_FECHAMENTO"] = den_desagios
                        # definindo deságios para cenários de choque para o período de
                        #   interesse
                        for dias_uteis_choque in range(1, 11):
                            # deságio de compra/venda para ativo corrente negociado em mercado
                            #   secundário da b3
                            if list_rows_mtm[i][5] in list_ativos_bov_vista:
                                dict_exportacao["MARGEM_B3_COMPRA_D_" + str(
                                    dias_uteis_choque)] = abs(den_desagios * (
                                        1 - list_num_desagios_compra[
                                            dias_uteis_choque - 1] / den_desagios))
                                dict_exportacao["DESAGIO_B3_COMPRA_D_" + str(
                                    dias_uteis_choque)] = min([1, abs(1 - list_num_desagios_compra[
                                        dias_uteis_choque - 1] / den_desagios)])
                                dict_exportacao["MARGEM_B3_VENDA_D_" + str(
                                    dias_uteis_choque)] = \
                                    abs(den_desagios * (
                                        1 - list_num_desagios_venda[
                                            dias_uteis_choque - 1] / den_desagios))
                                dict_exportacao["DESAGIO_B3_VENDA_D_" + str(
                                    dias_uteis_choque)] = \
                                    min([1, abs(1 - list_num_desagios_venda[
                                        dias_uteis_choque - 1] / den_desagios)])
                            else:
                                dict_exportacao["MARGEM_B3_COMPRA_D_" + str(
                                    dias_uteis_choque)] = abs(
                                    -list_num_desagios_compra[
                                        dias_uteis_choque - 1])
                                dict_exportacao["DESAGIO_B3_COMPRA_D_" + str(
                                    dias_uteis_choque)] = \
                                    min([1, abs(list_num_desagios_compra[
                                        dias_uteis_choque - 1] / den_desagios - 1)])
                                dict_exportacao["MARGEM_B3_VENDA_D_" + str(
                                    dias_uteis_choque)] = \
                                    abs(-list_num_desagios_venda[dias_uteis_choque - 1])
                                dict_exportacao["DESAGIO_B3_VENDA_D_" + str(
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
        df_mtm_b3.sort_values(["TICKER"], ascending=[True], inplace=True)
        # print(f"MTM B3: \n{df_mtm_b3}")
        # retornando deságios b3 de interesse
        return df_mtm_b3

    def collateral_accepted_spot_bov_b3(self, dict_repalce_str_erros={
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
        req_resp = request("GET", "https://www.b3.com.br/pt_br/produtos-e-servicos/compensacao-e-liquidacao/clearing/administracao-de-riscos/garantias/limites-de-renda-variavel/")
        html_parser_acoes_units_etfs_aceitos_garantia_b3 = HtmlHandler().lxml_parser(req_resp)
        url_acoes_units_etfs_aceitos_garantia_b3 = HtmlHandler().lxml_xpath(
            html_parser_acoes_units_etfs_aceitos_garantia_b3,
            '//*[@id="panel1a"]/ul/li[2]/a/@href')[0]
        url_acoes_units_etfs_aceitos_garantia_b3 = StrHandler().get_string_after_substr(
            url_acoes_units_etfs_aceitos_garantia_b3, "data/files")
        # creating complete url
        url_acoes_units_etfs_aceitos_garantia_b3 = \
            "https://www.b3.com.br/" \
            + "data/files" + \
            url_acoes_units_etfs_aceitos_garantia_b3
        # openning zip in memory
        req_resp = request("GET", url_acoes_units_etfs_aceitos_garantia_b3)
        list_unzipped_files = RemoteFiles().get_zip_from_web_in_memory(req_resp)
        if str(type(list_unzipped_files)) != "<class 'list'>":
            list_unzipped_files = [list_unzipped_files]
        # looping within files in zip
        for file_unz in list_unzipped_files[1:]:
            #   creating columns month-file
            print(str(file_unz))
            str_mes_arquivo_garantias_b3_corrente = StrHandler().get_between(
                str(file_unz), '_', '.xlsx').replace(' ', '').lower()
            #   looping within sheet names
            for nome_plan_ativa in [
                "Ações, BDRs, ETFs e Units",
                "ADR", 
                "DEB"
            ]:
                #   openning dataframe in memory
                reader = pd.read_excel(
                    file_unz, sheet_name=nome_plan_ativa)
                df_coll_acc_spot_bov = pd.DataFrame(reader)
                #   creating serialized list with spot bov collateral accepted
                for _, row in df_coll_acc_spot_bov.iterrows():
                    list_exportacao.append({
                        col_mes_arquivo: str_mes_arquivo_garantias_b3_corrente.upper(),
                        col_ticker: row["Código"],
                        col_isin: row[col_isin],
                        col_limite_qtds: row[key_limite_qtds]
                    })
        # creating dataframe
        df_coll_acc_spot_bov = pd.DataFrame(list_exportacao)
        # removing duplicates
        df_coll_acc_spot_bov[col_ticker] = [StrHandler().replace_all(
            str(x), dict_repalce_str_erros) for x in df_coll_acc_spot_bov[
                col_ticker].tolist()]
        # returning dataframe
        return df_coll_acc_spot_bov

    def req_trt_injection(self, req_resp:Response) -> Optional[pd.DataFrame]:
        source = self.get_query_params(req_resp.url, "source").lower()
        if source == "mtm_b3":
            return self.mtm_compra_venda()
        elif source == "spot_accepted_collateral_b3":
            return self.collateral_accepted_spot_bov_b3()
        return None
