"""B3 standardized instrument groups by trading session."""

from stpstone.ingestion.countries.br.exchange.b3_standardized_instrument_groups import (
	B3StandardizedInstrumentGroups,
)


cls_ = B3StandardizedInstrumentGroups(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 STANDARDIZED INSTRUMENT GROUPS: \n{df_}")
df_.info()
