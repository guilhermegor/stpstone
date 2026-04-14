"""B3 primitive risk factors used in margin and risk calculations."""

from stpstone.ingestion.countries.br.exchange.b3_primitive_risk_factors import (
	B3PrimitiveRiskFactors,
)


cls_ = B3PrimitiveRiskFactors(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 PRIMITIVE RISK FACTORS: \n{df_}")
df_.info()
