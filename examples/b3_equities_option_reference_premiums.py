"""B3 equities option reference premiums with implied volatility."""

from stpstone.ingestion.countries.br.exchange.b3_equities_option_reference_premiums import (
	B3EquitiesOptionReferencePremiums,
)


cls_ = B3EquitiesOptionReferencePremiums(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 EQUITIES OPTION REFERENCE PREMIUMS: \n{df_}")
df_.info()
