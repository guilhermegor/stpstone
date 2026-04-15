"""B3 Warranties - International Securities Accepted as Collateral."""

from stpstone.ingestion.countries.br.exchange.b3_warranties_international_securities import (
	B3WarrantiesInternationalSecurities,
)


cls_ = B3WarrantiesInternationalSecurities(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 WARRANTIES INTERNATIONAL SECURITIES: \n{df_}")
df_.info()
