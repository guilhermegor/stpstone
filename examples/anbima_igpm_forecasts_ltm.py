"""Anbima IGPM Forecasts for the Last Twelve Months."""

from stpstone.ingestion.countries.br.macroeconomics.anbima_igpm_forecasts_ltm import (
    AnbimaIGPMForecastsLTM,
)


cls_ = AnbimaIGPMForecastsLTM(
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF ANBIMA IGPM FORECASTS LTM: \n{df_}")
df_.info()
