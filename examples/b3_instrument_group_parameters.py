"""B3 instrument group parameters with settlement and liquidity rules."""

from stpstone.ingestion.countries.br.exchange.b3_instrument_group_parameters import (
	B3InstrumentGroupParameters,
)


cls_ = B3InstrumentGroupParameters(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 INSTRUMENT GROUP PARAMETERS: \n{df_}")
df_.info()
