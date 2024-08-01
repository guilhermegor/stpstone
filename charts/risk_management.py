### CHARTS FOR RISK MANAGEMENT ###

import matplotlib.pyplot as plt
from stpstone.finance.financial_risk.market_risk import MarketRiskManagement


class ChartsRskManagement:

    def efficient_frontier_makowitz(self, list_stocks_weights_portfolio, list_stocks_tickers,
                                    date_inf, date_sup, risk_free_nominal_rate,
                                    num_portfolios=25000, num_working_days_year=252,
                                    country='brazil'):
        '''
        REFERENCES: https://towardsdatascience.com/efficient-frontier-portfolio-optimisation-in-python-e7844051e7f
        DOCSTRING: CHART OF 
        INPUTS:
        OUTPUTS:
        '''
        # setup for markowitz portfolio
        setup_markowitz_portfolio = \
            MarketRiskManagement().setup_markowitz_portfolio(list_stocks_weights_portfolio,
                                                             list_stocks_tickers,
                                                             date_inf, date_sup,
                                                             num_working_days_year, country)

        # json n markowitz portfolios
        json_markowitz_random_portfolios = \
            MarketRiskManagement().efficient_frontier_portfolio(
                setup_markowitz_portfolio['array_daily_stocks_returns'],
                setup_markowitz_portfolio['array_mean_daily_stocks_returns'],
                risk_free_nominal_rate, num_portfolios, num_working_days_year)

        # arguments for plot: std, return and sharpe ratio, respectively
        x_axis_std = [d['markowitz_portfolio']['std_portfolio_nominal']
                      for d in json_markowitz_random_portfolios['n_potfolios'].values()]
        y_axis_returns = [d['markowitz_portfolio']['return_portfolio_stocks_nominal']
                          for d in json_markowitz_random_portfolios['n_potfolios'].values()]
        colorify_trigger = [d['markowitz_portfolio']['sharpe_ratio']
                            for d in json_markowitz_random_portfolios['n_potfolios'].values()]

        # plot - std x return
        plt.figure(figsize=(10, 7))
        plt.scatter(x_axis_std, y_axis_returns, c=colorify_trigger, cmap='YlGnBu', marker='o', s=10,
                    alpha=0.3)
        plt.colorbar()

        # maximum sharpe ratio
        plt.scatter(json_markowitz_random_portfolios['max_sharpe_ratio_portfolio']['std_nominal'],
                    json_markowitz_random_portfolios['max_sharpe_ratio_portfolio']['return_nominal'],
                    marker='*', color='r', s=500, label='Maximum Sharpe Ratio')

        # minimum volatility
        plt.scatter(json_markowitz_random_portfolios['min_volatility_portfolio']['std_nominal'],
                    json_markowitz_random_portfolios['min_volatility_portfolio']['return_nominal'],
                    marker='*', color='g', s=500, label='Minimum Volatility')

        # chart elements
        plt.title('Simulated Portfolio Optimization Based on Efficient Frontier')
        plt.xlabel('Annualised Volatility')
        plt.ylabel('Annualised Returns')
        plt.legend(labelspacing=0.8)
        plt.show()


# ChartsRskManagement().efficient_frontier_makowitz([0.3, 0.25, 0.45],
#                                                   ['PETR3', 'ITSA4', 'WEGE3'],
#                                                   '01/01/2016', '04/09/2020', 0.02, 100000)

# ChartsRskManagement().efficient_frontier_makowitz([0.25, 0.25, 0.25, 0.25],
#                                                   ['AAPL', 'AMZN', 'GOOGL', 'FB'],
#                                                   '01/01/2016', '31/12/2017', 0.02, 100000,
#                                                   country='United States')
