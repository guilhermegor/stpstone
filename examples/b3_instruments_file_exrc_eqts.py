"""B3 instruments file for exercise of equities."""

from stpstone.ingestion.countries.br.exchange.b3_instruments_file_exrc_eqts import (
	B3InstrumentsFileExrcEqts,
)


cls_ = B3InstrumentsFileExrcEqts(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 INSTRUMENTS FILE EXRC EQTS: \n{df_}")
df_.info()
