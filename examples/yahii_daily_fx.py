"""Yahii Daily FX Rates (USD/BRL and EUR/BRL) for Brazil"""

from stpstone.ingestion.countries.br.macroeconomics.yahii_daily_fx import YahiiDailyFX


cls_ = YahiiDailyFX(
	str_currency="usd",
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF YAHII DAILY FX (USD/BRL): \n{df_}")
df_.info()
