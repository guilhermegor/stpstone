"""Example of ingestion of ANBIMA IGPM Forecasts data."""

from stpstone.ingestion.countries.br.macroeconomics.anbima_igpm_forecasts import (
	AnbimaIGPMForecastsCurrentMonth,
	AnbimaIGPMForecastsLTM,
	AnbimaIGPMForecastsNextMonth,
)


cls_ = AnbimaIGPMForecastsLTM(logger=None, cls_db=None)

df_ = cls_.run()
print(f"DF ANBIMA IGPM FORECASTS: \n{df_}")
df_.info


cls_ = AnbimaIGPMForecastsNextMonth(logger=None, cls_db=None)

df_ = cls_.run()
print(f"DF ANBIMA IGPM FORECASTS: \n{df_}")
df_.info


cls_ = AnbimaIGPMForecastsCurrentMonth(logger=None, cls_db=None)

df_ = cls_.run()
print(f"DF ANBIMA IGPM FORECASTS: \n{df_}")
df_.info
