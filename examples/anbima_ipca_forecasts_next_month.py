"""Anbima IPCA Forecasts for the Next Month."""

from stpstone.ingestion.countries.br.macroeconomics.anbima_ipca_forecasts_next_month import (
    AnbimaIPCAForecastsNextMonth,
)


cls_ = AnbimaIPCAForecastsNextMonth(
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF ANBIMA IPCA FORECASTS NEXT MONTH: \n{df_}")
df_.info()
