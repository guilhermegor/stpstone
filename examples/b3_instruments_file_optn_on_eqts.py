"""B3 instruments file for options on equities."""

from stpstone.ingestion.countries.br.exchange.b3_instruments_file_optn_on_eqts import (
	B3InstrumentsFileOptnOnEqts,
)


cls_ = B3InstrumentsFileOptnOnEqts(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 INSTRUMENTS FILE OPTN ON EQTS: \n{df_}")
df_.info()
