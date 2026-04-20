"""B3 BDI most traded call options ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_derivatives_options_most_traded import (
    B3BdiDerivativesOptionsMostTraded,
)


cls_ = B3BdiDerivativesOptionsMostTraded(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Derivatives Options Most Traded: \n{df_}")
df_.info()
