"""Anbima IPCA Forecasts for the Last Twelve Months."""

from stpstone.ingestion.countries.br.macroeconomics.anbima_ipca_forecasts_ltm import (
    AnbimaIPCAForecastsLTM,
)


cls_ = AnbimaIPCAForecastsLTM(
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF ANBIMA IPCA FORECASTS LTM: \n{df_}")
df_.info()
