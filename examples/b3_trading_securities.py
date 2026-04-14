"""Example of ingestion of B3 trading securities data."""

from stpstone.ingestion.countries.br.registries.b3_trading_securities import B3Instruments


cls_ = B3Instruments(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 INSTRUMENTS: \n{df_}")
df_.info()
