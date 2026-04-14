"""Anbima IGPM Forecasts for the Next Month."""

from stpstone.ingestion.countries.br.macroeconomics.anbima_igpm_forecasts_next_month import (
    AnbimaIGPMForecastsNextMonth,
)


cls_ = AnbimaIGPMForecastsNextMonth(
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF ANBIMA IGPM FORECASTS NEXT MONTH: \n{df_}")
df_.info()
