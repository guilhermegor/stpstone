"""B3 BDI daily register of OTC/structured instruments (InstrumentRegistration table)."""

from stpstone.ingestion.countries.br.registries.b3_bdi_fixed_income_instruments_registration import (
	B3BdiFixedIncomeInstrumentsRegistration,
)


cls_ = B3BdiFixedIncomeInstrumentsRegistration(
	date_ref=None,
	logger=None,
	cls_db=None,
	int_page_min=1,
	int_page_max=10,
)

df_ = cls_.run()
print(f"DF B3 BDI INSTRUMENT REGISTRATION: \n{df_}")
df_.info()
