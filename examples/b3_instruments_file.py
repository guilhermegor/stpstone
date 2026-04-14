"""B3 instruments file with security attributes cached from XML."""

from stpstone.ingestion.countries.br.exchange.b3_instruments_file import B3InstrumentsFile


cls_ = B3InstrumentsFile(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 INSTRUMENTS FILE: \n{df_}")
df_.info()
