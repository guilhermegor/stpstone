"""B3 equities fee public information with tiered cost schedules."""

from datetime import date

from stpstone.ingestion.countries.br.exchange.b3_equities_fee_public_information import (
	B3EquitiesFeePublicInformation,
)


cls_ = B3EquitiesFeePublicInformation(
	date_ref=date(2025, 9, 1),
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 EQUITIES FEE PUBLIC INFORMATION: \n{df_}")
df_.info()
