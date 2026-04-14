"""B3 instruments file for economic and financial indicators."""

from stpstone.ingestion.countries.br.exchange.b3_instruments_file_indicators import (
	B3InstrumentsFileIndicators,
)


cls_ = B3InstrumentsFileIndicators(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 INSTRUMENTS FILE INDICATORS: \n{df_}")
df_.info()
