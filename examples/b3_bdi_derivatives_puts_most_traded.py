"""B3 BDI most traded put options ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_derivatives_puts_most_traded import (
    B3BdiDerivativesPutsMostTraded,
)


cls_ = B3BdiDerivativesPutsMostTraded(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Derivatives Puts Most Traded: \n{df_}")
df_.info()
