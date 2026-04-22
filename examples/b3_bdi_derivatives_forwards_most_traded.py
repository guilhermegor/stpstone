"""B3 BDI most traded equities in forward contracts ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_derivatives_forwards_most_traded import (
    B3BdiDerivativesForwardsMostTraded,
)


cls_ = B3BdiDerivativesForwardsMostTraded(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Derivatives Forwards Most Traded: \n{df_}")
df_.info()
