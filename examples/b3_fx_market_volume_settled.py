"""B3 FX market volume settled in USD and BRL net amounts."""

from stpstone.ingestion.countries.br.exchange.b3_fx_market_volume_settled import (
	B3FXMarketVolumeSettled,
)


cls_ = B3FXMarketVolumeSettled(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 F X MARKET VOLUME SETTLED: \n{df_}")
df_.info()
