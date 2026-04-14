"""B3 instruments file for American Depositary Receipts."""

from stpstone.ingestion.countries.br.exchange.b3_instruments_file_adr import B3InstrumentsFileADR


cls_ = B3InstrumentsFileADR(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 INSTRUMENTS FILE A D R: \n{df_}")
df_.info()
