"""B3 instruments file for securities lending (BTC)."""

from stpstone.ingestion.countries.br.exchange.b3_instruments_file_btc import B3InstrumentsFileBTC


cls_ = B3InstrumentsFileBTC(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 INSTRUMENTS FILE B T C: \n{df_}")
df_.info()
