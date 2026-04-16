"""B3 instruments file for fixed income securities."""

from stpstone.ingestion.countries.br.exchange.b3_instruments_file_fxd_incm import (
	B3InstrumentsFileFxdIncm,
)


cls_ = B3InstrumentsFileFxdIncm(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 INSTRUMENTS FILE FXD INCM: \n{df_}")
df_.info()
