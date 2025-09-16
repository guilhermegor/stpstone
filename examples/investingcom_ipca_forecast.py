"""Example of ingestion of Investing.com IPCA forecast data."""

from stpstone.ingestion.countries.br.macroeconomics.investingcom_ipca_forecast import (
    InvetingComIPCAForecast,
)


cls_ = InvetingComIPCAForecast(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=True)
print(f"DF INVESTINGCOM IPCA FORECAST: \n{df_}")
df_.info()