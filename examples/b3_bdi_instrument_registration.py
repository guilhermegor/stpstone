"""B3 BDI daily register of OTC/structured instruments (InstrumentRegistration table)."""

from stpstone.ingestion.countries.br.registries.b3_bdi_instrument_registration import (
	B3BdiInstrumentRegistration,
)


cls_ = B3BdiInstrumentRegistration(
	date_ref=None,
	logger=None,
	cls_db=None,
	int_page_min=1,
	int_page_max=10,
)

df_ = cls_.run()
print(f"DF B3 BDI INSTRUMENT REGISTRATION: \n{df_}")
df_.info()
