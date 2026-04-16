"""B3 instruments file for equity securities."""

from stpstone.ingestion.countries.br.exchange.b3_instruments_file_eqty import B3InstrumentsFileEqty


cls_ = B3InstrumentsFileEqty(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 INSTRUMENTS FILE EQTY: \n{df_}")
df_.info()
