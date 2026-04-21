"""B3 BDI equities in custody for the ADR program ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_clearing_adr_custody import (
	B3BdiClearingAdrCustody,
)


cls_ = B3BdiClearingAdrCustody(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Clearing ADR Custody: \n{df_}")
df_.info()
