### ENDPOINT PARA CONEXÃO COM O SITE DADOS DE MERCADO ###

import requests
from pprint import pprint


class DadosDeMercado:
    """
    REFERENCES: DADOS DE MERCADO ENPOINT:https://www.dadosdemercado.com.br/api/docs/autenticacao
    DOCSTRING: STOCK MARKET DATA FROM B3
    INPUTS: TOKEN
    OUTPUTS: CLASS
    """

    def __init__(self, token):
        self.token = token

    def fetch_data(self, app, payload=dict(), version_endpoint='1',
                   endpoint='https://api.dadosdemercado.com.br/v{}/{}', method='GET'):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        headers = {
            'Authorization': 'Bearer {}'.format(self.token)
        }
        return requests.request(method, url=endpoint.format(version_endpoint, app),
                                headers=headers, data=payload).json()

    def companies_trading_b3_stock_market(self, app='companies'):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.fetch_data(app)

    def tickers_companies_trading_b3_stock_market(self, app='tickers'):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.fetch_data(app)

    def stock_market_indexes(self, app='indexes'):
        """
        DOCSTRING: STOCK MARKET INDEXES - NOT PROPERLY WORKING
        INPUTS: APP (INDEXES AS DEFAULT)
        OUTPUTS: JSON
        """
        return self.fetch_data(app)

    def company_registration_data(self, cvm_code, app='/companies/{}/tickers'):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        if type(cvm_code) != str:
            cvm_code = str(cvm_code)
        return self.fetch_data(app.format(cvm_code))

    def dividends(self, cvm_code, app='/companies/{}/dividends'):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        if type(cvm_code) != str:
            cvm_code = str(cvm_code)
        return self.fetch_data(app.format(cvm_code))

    def splits_inplits(self, cvm_code, app='/companies/{}/splits'):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        if type(cvm_code) != str:
            cvm_code = str(cvm_code)
        return self.fetch_data(app.format(cvm_code))

    def quotes(self, ticker, app='/tickers/{}/quotes'):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.fetch_data(app.format(ticker))

    def yield_curves(self, app='/macro/yield_curves'):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.fetch_data(app)


# # inputs
# token = 'ffc65ca86d9edb68a7ba464b691c1614'
# class_dados_mercado = DadosDeMercado(token)
# cod_cvm = '14311'
# ticker = 'CPLE6'

# # outputs
# json_companies_b3 = class_dados_mercado.companies_trading_b3_stock_market()
# print(json_companies_b3)
# json_tickers_b3 = class_dados_mercado.tickers_companies_trading_b3_stock_market()
# print(json_tickers_b3)
# json_indexes_b3 = class_dados_mercado.stock_market_indexes()
# print(json_indexes_b3)
# json_companies_b3 = class_dados_mercado.company_registration_data(cod_cvm)
# print(json_companies_b3)
# json_dividendos_b3 = class_dados_mercado.dividends(cod_cvm)
# pprint(json_dividendos_b3)
# json_splits_b3 = class_dados_mercado.splits_inplits(cod_cvm)
# print(json_splits_b3)
# json_quotes = class_dados_mercado.quotes(ticker)
# print(json_quotes)
# json_yield_curves = class_dados_mercado.yield_curves()
# print(json_yield_curves)
