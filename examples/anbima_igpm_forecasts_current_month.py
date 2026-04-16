"""Anbima IGPM Forecasts for the Current Month."""

from stpstone.ingestion.countries.br.macroeconomics.anbima_igpm_forecasts_current_month import (
    AnbimaIGPMForecastsCurrentMonth,
)


cls_ = AnbimaIGPMForecastsCurrentMonth(
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF ANBIMA IGPM FORECASTS CURRENT MONTH: \n{df_}")
df_.info()
