"""Anbima IPCA Forecasts for the Current Month."""

from stpstone.ingestion.countries.br.macroeconomics.anbima_ipca_forecasts_current_month import (
    AnbimaIPCAForecastsCurrentMonth,
)


cls_ = AnbimaIPCAForecastsCurrentMonth(
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF ANBIMA IPCA FORECASTS CURRENT MONTH: \n{df_}")
df_.info()
