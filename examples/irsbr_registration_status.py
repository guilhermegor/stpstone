"""Brazilian IRS (Receita Federal) Registration Status (Motivos) Open Data."""

from stpstone.ingestion.countries.br.taxation.irsbr_registration_status import (
	IRSBRRegistrationStatus,
)


cls_ = IRSBRRegistrationStatus(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF IRSBR REGISTRATION STATUS: \n{df_}")
df_.info()
