### CALCULADORA DE TESOURO DIRETO ###

from datetime import date, datetime
import sys
sys.path.append(r'C:\Users\Guilherme\OneDrive\Dev\Python\Packages')
from stpstone.finance.performance_apprraisal.financial_math import FinancialMath
from stpstone.utils.cals.handling_dates import DatesBR


class PrecificacaoTD:

    def ltn(self, ytm, du, valor_nominal=1000, du_1_ano=252, periodicidade_capitalizacao=1):
        """
        DOCSTRING: TÍTULO PRÉ-FIXADO COM PAGAMENTO DE PRINCIPAL E JUROS NO VENCIMENTO
        INPUTS: YIELD TO MATURITY, OU MARCAÇÃO A MERCADO, DU ATÉ O VENCIMENTO, VALOR NOMINAL,
            POR PADRÃO R$ 1.000,00 E DU
        OUTPUTS: VALOR PRESENTE DO TÍTULO, OU MARCAÇÃO A MERADO DO MESMO
        """
        return FinancialMath().present_value(
            FinancialMath().compound_interest(
                ytm, du_1_ano, periodicidade_capitalizacao), du, 0,
            valor_nominal)

    def ntn_f(self, ytm, lista_dus_fluxos_caixa, taxa_nominal_cupom=0.1, valor_nominal=1000,
              du_1_ano=252, periodicidade_pagmento_cupons=126, periodicidade_capitalizacao=1):
        """
        DOCSTRING: TÍTULO PRÉ-FIXADO COM PAGAMENTO DE JUROS SEMESTRALMENTE E PRINCIPAL NO
            VENCIMENTO
        INPUTS: YIELD DO MATURITY, DU ATÉ O VENCIMENTO, CUPOM PAGO AO ANO
            (PERCENTUAL DO VALOR NOMINAL),
        OUTPUTS: VALOR PRESENTE DO FLUXO DE CAIXA
        """
        ytm_real = FinancialMath().compound_interest(
            ytm, du_1_ano, periodicidade_capitalizacao)
        cupom_semestral = valor_nominal \
            * FinancialMath().compound_interest(taxa_nominal_cupom, du_1_ano,
                                                periodicidade_pagmento_cupons)
        print(cupom_semestral)
        print(lista_dus_fluxos_caixa)
        list_fluxos_caixa = [FinancialMath().present_value(ytm_real, du, 0, cupom_semestral)
                             for du in lista_dus_fluxos_caixa]
        list_fluxos_caixa.append(FinancialMath().present_value(ytm_real, lista_dus_fluxos_caixa[-1],
                                                               0, valor_nominal))
        # pu
        return {
            'lista_dus_ntnf': lista_dus_fluxos_caixa,
            'lista_fluxos_caixa_ntnf': list_fluxos_caixa,
            'retorno_bruto_ntnf': sum(list_fluxos_caixa)
        }

    def pr1(self, data_negociacao=DatesBR().add_working_days(DatesBR().curr_date(),
                                                             1).strftime('%d/%m/%Y'),
            dia_atualizacao_vna=15):
        """
        DOCSTRING: PROPORÇÃO ENTRE OS DIAS CORRIDOS DESDE O ÚLTIMO IPCA ATÉ A DATA DE VIGÊNCIA DO
            PRÓXIMO
        INPUTS: DATA DE NEGOCIAÇÃO EM DD/MM/AAAA
        OUTPUTS: INT
        """
        # convertendo a data str para formato datetime
        data_negociacao = DatesBR().str_date_to_datetime(data_negociacao, 'DD/MM/AAAA')
        data_negociacao = DatesBR().add_working_days(data_negociacao, 1)
        # data de negociação antes da data de divulgação do vna, mo mês corrente
        if date(data_negociacao.year, data_negociacao.month,
                dia_atualizacao_vna) > data_negociacao:
            data_proximo_vna = date(
                data_negociacao.year, data_negociacao.month, dia_atualizacao_vna)
            if data_negociacao.month == 1:
                data_vna_anterior = date(
                    data_negociacao.year - 1, 12, dia_atualizacao_vna)
            else:
                data_vna_anterior = date(
                    data_negociacao.year, data_negociacao.month - 1, dia_atualizacao_vna)
        # data de negociação depois da data de divulgação do vna, mo mês corrente
        else:
            if data_negociacao.month == 12:
                data_proximo_vna = date(
                    data_negociacao.year + 1, 1, dia_atualizacao_vna)
            else:
                data_proximo_vna = date(
                    data_negociacao.year, data_negociacao.month + 1, dia_atualizacao_vna)
            data_vna_anterior = date(
                data_negociacao.year, data_negociacao.month, dia_atualizacao_vna)
        # retornar pr1
        return DatesBR().delta_calendar_days(data_negociacao, data_vna_anterior) \
            / DatesBR().delta_calendar_days(data_proximo_vna, data_vna_anterior)

    def vna_projetado_ntnb(self, vna_ultimo_disponivel_ntnb, ipca_projetado_aa, pr1,
                           nper_dias_uteis_aa=252, nper_dias_corridos_am=30):
        """
        DOCSTRING: VNA PROJETADO PARA DIAS DE NEGOCIAÇÃO QUE NÃO TENHA O VNA DISPONÍVEL
        INPUTS: VNA ÚLTIMO DISPONÍVEL (TRUNCADO NA SEXTA CASA), IPCA PROJETADO E PR1
        OUTPUTS: VNA PROJETADO (BOOLEAN)
        """
        ipca_projetado_adu = FinancialMath().compound_interest(ipca_projetado_aa, nper_dias_uteis_aa,
                                                               nper_dias_corridos_am)
        return vna_ultimo_disponivel_ntnb * (1 + ipca_projetado_adu) ** pr1

    def ntn_b_principal(self, ytm, du, vna_ultimo_disponivel_ntnb, ipca_projetado_aa,
                        data_negociacao=DatesBR().add_working_days(DatesBR().curr_date(),
                                                                   1).strftime('%d/%m/%Y'),
                        dia_atualizacao_vna=15,
                        nper_dias_uteis_aa=252, nper_dias_corridos_am=30,
                        valor_nominal=100):
        """
        DOCSTRING: PRECIFICAÇÃO NTN-B PRINCIPAL
        INPUTS: YTM, DU, VNA ÚLTIMO DISPONÍVEL, IPCA PROJETADO AA, DATA DE NEGOCIAÇÃO (
            POR PADRÃO D+1), DIA DE ATUALIZAÇÃO DO VNA (PADRÃO 15), NÚMERO DE DIAS ÚTEIS EM UM ANO,
            NÚMERO DE DIAS CORRIDOS EM UM MÊS (PADRÃO 30) E VALOR NOMINAL DO TÍTULO (PADRÃO 100)
        OUTPUTS: FLOAT
        """
        # pr1
        pr1 = PrecificacaoTD().pr1(data_negociacao, dia_atualizacao_vna)
        # vna projetado
        vna_projetado_ntnb = PrecificacaoTD().vna_projetado_ntnb(vna_ultimo_disponivel_ntnb,
                                                                 ipca_projetado_aa, pr1,
                                                                 nper_dias_uteis_aa,
                                                                 nper_dias_corridos_am)
        # cotacao
        cotacao = valor_nominal / (1 + FinancialMath().compound_interest(ytm, nper_dias_uteis_aa,
                                                                         du)) / 100
        # pu
        return vna_projetado_ntnb * cotacao

    def ntn_b(self, ytm, lista_dus_fluxos_caixa, vna_ultimo_disponivel_ntnb, ipca_projetado_aa,
              data_negociacao=DatesBR().add_working_days(DatesBR().curr_date(),
                                                         1).strftime('%d/%m/%Y'),
              dia_atualizacao_vna=15,
              nper_dias_uteis_aa=252, nper_dias_corridos_am=30,
              valor_nominal=100, taxa_cupom_aa=0.06, periodicidade_pagmento_cupons=126):
        """
        DOCSTRING: PRECIFICAÇÃO NTN-B (COM PAGAMENTO DE CUPONS)
        INPUTS: YTM, LISTA DUS, VNA ÚLTIMO DISPONÍVEL, IPCA PROJETADO AA, DATA DE NEGOCIAÇÃO (
            POR PADRÃO D+1), DIA DE ATUALIZAÇÃO DO VNA (PADRÃO 15), NÚMERO DE DIAS ÚTEIS EM UM ANO,
            NÚMERO DE DIAS CORRIDOS EM UM MÊS (PADRÃO 30) E VALOR NOMINAL DO TÍTULO (PADRÃO 100)
        OUTPUTS: FLOAT
        """
        # pr1
        pr1 = PrecificacaoTD().pr1(data_negociacao, dia_atualizacao_vna)
        # vna projetado
        vna_projetado_ntnb = PrecificacaoTD().vna_projetado_ntnb(vna_ultimo_disponivel_ntnb,
                                                                 ipca_projetado_aa, pr1,
                                                                 nper_dias_uteis_aa,
                                                                 nper_dias_corridos_am)
        # cupom semestral
        cupom_semestral = FinancialMath().compound_interest(taxa_cupom_aa, nper_dias_uteis_aa,
                                                            periodicidade_pagmento_cupons)
        # criacao fluxo de caixa
        lista_cotacao = list()
        for du in lista_dus_fluxos_caixa:
            # cotacao
            lista_cotacao.append(cupom_semestral / (
                1 + FinancialMath().compound_interest(ytm, nper_dias_uteis_aa, du)))
        lista_cotacao.append(1 / (1 + FinancialMath().compound_interest(ytm, nper_dias_uteis_aa,
                                                                        lista_dus_fluxos_caixa[-1])))
        cotacao = sum(lista_cotacao)
        # pu
        return {
            'lista_dus_ntnb': lista_dus_fluxos_caixa,
            'lista_fluxos_caixa_ntnb': [x * vna_projetado_ntnb for x in lista_cotacao],
            'retorno_bruto_ntnb': vna_projetado_ntnb * cotacao
        }

    def vna_projetado_lft(self, vna_ultimo_disponivel_lft, taxa_selic_projetada_aa,
                          periodicidade_capitalizacao=1, nper_dias_uteis_aa=252):
        """
        DOCSTRING: VNA PROJETADO LFT
        INPUTS: VNA ÚLTIMO DISPONÍVEL (LFT), TAXA SELIC PROJETADA NOMINAL, PERIODICIDADE DE
            CAPITALIZAÇÃO (POR PADRÃO 1), DIAS ÚTEIS EM UM ANO (POR PADRÃO 252)
        OUTPUT: FLOAT
        """
        return vna_ultimo_disponivel_lft * \
            (1 + FinancialMath().compound_interest(taxa_selic_projetada_aa,
                                                   nper_dias_uteis_aa, periodicidade_capitalizacao))

    def lft(self, ytm, vna_ultimo_disponivel_lft, taxa_selic_projetada_aa,
            periodicidade_capitalizacao=1, nper_dias_uteis_aa=252, valor_nominal=1):
        """
        DOCSTRING:
        INPUTS:
        OUTPUT:
        """
        # vna atualizado
        vna_projetado_lft = PrecificacaoTD().vna_projetado_lft(vna_ultimo_disponivel_lft,
                                                               taxa_selic_projetada_aa,
                                                               periodicidade_capitalizacao,
                                                               nper_dias_uteis_aa)
        # cotacao
        cotacao = valor_nominal / (1 + FinancialMath().compound_interest(ytm, nper_dias_uteis_aa,
                                                                         periodicidade_capitalizacao))
        # pu
        return vna_projetado_lft * cotacao

    def rentabilidade_liquida(self, rentabilidade_bruta, periodo_vigencia):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        pass

    def dus_vencimento(self, data_vencimento, data_referencia=DatesBR().curr_date()):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # convertendo datas para datetime
        if type(data_vencimento) == str:
            data_vencimento = DatesBR().str_date_to_datetime(data_vencimento, 'DD/MM/AAAA')
        # lista últimos dias do ano entre o intervalo
        list_years = DatesBR().list_years_within_dates(data_referencia, data_vencimento)
        list_last_week_year_day = DatesBR().list_last_days_of_years(list_years)
        # retornando dias úteis para o vencimento
        return DatesBR().get_working_days_delta(data_referencia, data_vencimento) \
            + DatesBR().add_holidays_not_considered_anbima(data_referencia, data_vencimento,
                                                           list_last_week_year_day)

    def dus_pagamento_cupons(self, prim_dia_util, papel, vencimento):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS
        """
        # criando variáveis de passagem
        dict_cupons = dict()
        # convertendo datas para datetime
        if type(prim_dia_util) == str:
            prim_dia_util = DatesBR().str_date_to_datetime(prim_dia_util, 'DD/MM/AAAA')
        if type(vencimento) == str:
            vencimento = DatesBR().str_date_to_datetime(vencimento, 'DD/MM/AAAA')
        vencimento = DatesBR().add_working_days(
            DatesBR().sub_working_days(vencimento, 1), 1)
        # lista últimos dias do ano entre o intervalo
        list_years = DatesBR().list_years_within_dates(prim_dia_util, vencimento)
        list_last_week_year_day = DatesBR().list_last_days_of_years(list_years)
        if papel == 'ntn-f':
            # mês e dia de pagamentos de cupom
            mes_pagamento_cupom_1 = 1
            mes_pagamento_cupom_2 = 7
            dia_pagamento_cupom = 1
        # determinando a primeira data de pagamento
        if DatesBR().testing_dates(prim_dia_util, date(prim_dia_util.year,
                                                       mes_pagamento_cupom_2,
                                                       dia_pagamento_cupom)) == 'OK':
            prim_pagamento_cupom = \
                DatesBR().add_working_days(
                    DatesBR().sub_working_days(date(prim_dia_util.year,
                                                    mes_pagamento_cupom_2,
                                                    dia_pagamento_cupom), 1), 1)
        else:
            prim_pagamento_cupom = \
                DatesBR().add_working_days(
                    DatesBR().sub_working_days(date(prim_dia_util.year + 1,
                                                    mes_pagamento_cupom_1,
                                                    dia_pagamento_cupom), 1), 1)
        # * determinando o dicionário de dias de pagamentos de cupons, limitado ao vencimento
        # datas de passagem
        data_corrente = prim_dia_util
        data_prox_pagamento = prim_pagamento_cupom
        iterador = DatesBR().testing_dates(data_prox_pagamento, vencimento)
        while iterador == 'OK':
            # alterando a data corrente
            data_corrente = data_prox_pagamento
            # data próximo pagamento
            if data_prox_pagamento.month == mes_pagamento_cupom_1:
                data_prox_pagamento = \
                    DatesBR().add_working_days(DatesBR().sub_working_days(
                        date(data_prox_pagamento.year,
                             mes_pagamento_cupom_2, dia_pagamento_cupom), 1), 1)
            else:
                data_prox_pagamento = \
                    DatesBR().add_working_days(DatesBR().sub_working_days(
                        date(data_prox_pagamento.year + 1, mes_pagamento_cupom_1,
                             dia_pagamento_cupom), 1), 1)
            # preenchendo dicionário
            dict_cupons[data_corrente] = DatesBR().get_working_days_delta(
                prim_dia_util, data_corrente) \
                + DatesBR().add_holidays_not_considered_anbima(prim_dia_util, data_corrente,
                                                               list_last_week_year_day)
            # iterador
            iterador = DatesBR().testing_dates(data_prox_pagamento, vencimento)
        # resultado - dicionário de cupons
        return dict_cupons

# print(PrecificacaoTD().ltn(0.1045, 148))
# # # output
# # -886.9059241913519
# print(PrecificacaoTD().ntn_f(0.1298, [120, 248, 372, 499]))
# # output
# -953.7585159875025
# print(PrecificacaoTD().ntn_f(0.1298, [127, 251]))
# # output
# -974.6628428224586
# print(PrecificacaoTD().ntn_f(0.1298, [127, 251]) /
#       PrecificacaoTD().ntn_f(0.1298, [120, 248, 372, 499]) - 1)
# # output
# 0.021917840296620694


# ntnb-principal
# pr1
# print(PrecificacaoTD().pr1('05/01/2017'))
# # output
# 0.7096774193548387
# print(PrecificacaoTD().vna_projetado_ntnb(2494.977146,
#                                      0.0683, PrecificacaoTD().pr1('05/01/2017')))
# # output
# 2508.942630915996

# print(PrecificacaoTD().ntn_b_principal(0.0613, 1089,
#                                        PrecificacaoTD().vna_projetado_ntnb(2494.977146,
#                                                                       0.0683,
#                                                                       PrecificacaoTD().pr1(
#                                                                           '05/01/2017'))))
# # output
# 1940.139464570165


# print(PrecificacaoTD().ntn_b_principal(0.005, 1089,
#                                        PrecificacaoTD().vna_projetado_ntnb(2736.989929,
#                                                                       0.0683,
#                                                                       PrecificacaoTD().pr1(
#                                                                           '05/01/2018'))) /
#       PrecificacaoTD().ntn_b_principal(0.0613, 1089,
#                                        PrecificacaoTD().vna_projetado_ntnb(2494.977146,
#                                                                       0.0683,
#                                                                       PrecificacaoTD().pr1(
#                                                                           '05/01/2017'))) - 1)
# # output
# 0.38907055571072546

# print(PrecificacaoTD().ntn_b_principal(0.0613, 1089,
#                                        PrecificacaoTD().vna_projetado_ntnb(2494.977146,
#                                                                       0.0683,
#                                                                       PrecificacaoTD().pr1(
#                                                                           '05/01/2017'))))

# print(PrecificacaoTD().ntn_b_principal(
#     0.0613, 1089, 2494.977146, 0.0683, '05/01/2017'))
# # output
# 1940.139464570165

# print(type(DatesBR().add_working_days(DatesBR().curr_date(),
#                                       1).strftime('%d/%m/%Y')))

# print(PrecificacaoTD().ntn_b(
#     0.0610, [127, 250, 374, 500], 2508.949127, 0.0949127, '14/08/2017'))
# # # output
# # 2506.659044950116
# print(PrecificacaoTD().ntn_b(
#     0.0610, [124, 250], 2752.31, 0.0970, '14/08/2018'))
# # output
# 2751.0456728647796
# print(PrecificacaoTD().lft(0, 6543.016794, 0.1175))
# # output
# 6545.90191489939
# print(type(DatesBR().curr_date()))

# print(PrecificacaoTD().dus_pagamento_cupons(
#     DatesBR().curr_date(), 'ntn-f', '01/01/2031'))
# # output
# {datetime.date(2021, 1, 4): 3649, datetime.date(2021, 7, 1): 3471, datetime.date(2022, 1, 3): 3285, datetime.date(2022, 7, 1): 3106, datetime.date(2023, 1, 2): 2921, datetime.date(2023, 7, 3): 2739, datetime.date(2024, 1, 2): 2556, datetime.date(2024, 7, 1): 2375, datetime.date(2025, 1, 2): 2190, datetime.date(2025, 7, 1): 2010, datetime.date(2026, 1, 2): 1825, datetime.date(2026, 7, 1): 1645, datetime.date(2027, 1, 4): 1458, datetime.date(2027, 7, 1): 1280, datetime.date(2028, 1, 3): 1094, datetime.date(2028, 7, 3): 912, datetime.date(2029, 1, 2): 729, datetime.date(2029, 7, 2): 548, datetime.date(2030, 1, 2): 364, datetime.date(2030, 7, 1): 184}

# print(list(PrecificacaoTD().dus_pagamento_cupons(
#     '02/09/2020', 'ntn-f', '01/01/2031').values()))
# # output
# {datetime.date(2021, 1, 4): 3774, datetime.date(2021, 7, 1): 3774, datetime.date(2022, 1, 3): 3774, datetime.date(2022, 7, 1): 3774, datetime.date(2023, 1, 2): 3774, datetime.date(2023, 7, 3): 3774, datetime.date(2024, 1, 2): 3774, datetime.date(2024, 7, 1): 3774, datetime.date(2025, 1, 2): 3774, datetime.date(2025, 7, 1): 3774, datetime.date(
#     2026, 1, 2): 3774, datetime.date(2026, 7, 1): 3774, datetime.date(2027, 1, 4): 3774, datetime.date(2027, 7, 1): 3774, datetime.date(2028, 1, 3): 3774, datetime.date(2028, 7, 3): 3774, datetime.date(2029, 1, 2): 3774, datetime.date(2029, 7, 2): 3774, datetime.date(2030, 1, 2): 3774, datetime.date(2030, 7, 1): 3774, datetime.date(2031, 1, 2): 3774}

# int_bzd =\
#     DatesBR().get_working_days_delta(DatesBR().str_date_to_datetime('10/08/2020', 'DD/MM/AAAA'),
#                                      DatesBR().str_date_to_datetime('04/01/2021', 'DD/MM/AAAA'))
# print(int_bzd)
# print(len(DatesBR().list_rng_wds(DatesBR().str_date_to_datetime('10/08/2020', 'DD/MM/AAAA'),
#                                       DatesBR().str_date_to_datetime('04/01/2021', 'DD/MM/AAAA'))))
# print(DatesBR().list_rng_wds(DatesBR().str_date_to_datetime('10/08/2020', 'DD/MM/AAAA'),
#                                   DatesBR().str_date_to_datetime('04/01/2021', 'DD/MM/AAAA')))
# print(PrecificacaoTD().ntn_f(0.06846,
#                              list(PrecificacaoTD().dus_pagamento_cupons(
#                                  '10/08/2020',
#                                  'ntn-f', '01/01/2031').values())))
# # # output
# # -1235.9845992549463

# print(PrecificacaoTD().dus_vencimento('01/04/2021'))
# # output - 09/10/2020
# 118

# print(PrecificacaoTD().dus_vencimento('02/01/2024'))
# # output
# 808
# ! PAREI PG 62
