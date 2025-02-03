### CHARTS FOR RISK MANAGEMENT ###

import matplotlib.pyplot as plt
from stpstone.finance.b3.infos_fechamento import B3Fcto


class StockMarket:

    def stock_price_evolution(self, list_stocks_tickers, date_inf, date_sup, country, str_x_label,
                              str_y_label):
        """
        DOCSTRING: CHART OF STOCK PRICE EVOLUTION
        INPUTS: LIST OF STOCKS TICKERS, INFERIOR AND SUPERIOR DATES, COUNTRY 
            (EX.: BRAZIL/ UNITED STATES), STR WITH X LABEL, AND ITS Y LABEL AXIS NAMES 
            FOR THE CHART
        OUTPUTS:
        """
        # define plt style
        plt.style.use('fivethirtyeight')

        # close price of stocks in portfolio for a given range of dates (n-observations)
        dict_stocks_market_data = dict()
        for k, v in B3Fcto().cotacoes_serie_historica(list_stocks_tickers, date_inf,
                                                      date_sup, country).items():
            list_current_dates = [dict_current_price_data['date'] for dict_current_price_data
                                  in v['historical']]
            list_current_prices = [dict_current_price_data['close'] for dict_current_price_data
                                   in v['historical']]
            dict_stocks_market_data[k] = {
                'dates': list_current_dates,
                'close': list_current_prices
            }

        # plot chart of stock market evoulution through range of time
        plt.figure(figsize=(14, 7))
        for k, v in dict_stocks_market_data.items():
            plt.plot(v['dates'], v['close'], lw=3, alpha=0.8, label=k)
        plt.legend(loc='upper left', fontsize=12)
        plt.xlabel(str_x_label)
        plt.ylabel(str_y_label)
        plt.show()


# StockMarket().stock_price_evolution(['AAPL', 'AMZN', 'GOOGL', 'FB'], '01/01/2016',
#                                     '31/12/2017', 'United States', 'dates', 'price in USD')

# StockMarket().stock_price_evolution(['PETR4', 'PETR3', 'MGLU3', 'VALE3'], '01/01/2018',
#                                     '04/09/2020', 'brazil', 'dates', 'price in BRL')
