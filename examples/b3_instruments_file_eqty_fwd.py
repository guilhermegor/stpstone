"""B3 instruments file for equity forward contracts."""

from stpstone.ingestion.countries.br.exchange.b3_instruments_file_eqty_fwd import (
	B3InstrumentsFileEqtyFwd,
)


cls_ = B3InstrumentsFileEqtyFwd(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 INSTRUMENTS FILE EQTY FWD: \n{df_}")
df_.info()
