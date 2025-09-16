"""Example of ingestion of ANBIMA IPCA Forecasts data."""

from stpstone.ingestion.countries.br.macroeconomics.anbima_ipca_forecasts import (
    AnbimaIPCAForecastsCurrentMonth,
    AnbimaIPCAForecastsLTM,
    AnbimaIPCAForecastsNextMonth,
)


cls_ = AnbimaIPCAForecastsNextMonth(
    logger=None, 
    cls_db=None
)

df_ = cls_.run()
print(f"DF ANBIMA IPCA FORECASTS: \n{df_}")
df_.info


cls_ = AnbimaIPCAForecastsCurrentMonth(
    logger=None, 
    cls_db=None
)

df_ = cls_.run()
print(f"DF ANBIMA IPCA FORECASTS: \n{df_}")
df_.info


cls_ = AnbimaIPCAForecastsLTM(
    logger=None, 
    cls_db=None
)

df_ = cls_.run()
print(f"DF ANBIMA IPCA FORECASTS: \n{df_}")
df_.info
