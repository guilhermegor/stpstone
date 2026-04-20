"""B3 BDI Derivatives - valid lots and settlement for arabica coffee contracts."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_derivatives_valid_lots_settlement_arabica_coffee_contracts import (  # noqa: E501
    B3BdiDerivativesValidLotsSettlementArabicaCoffeeContracts,
)


cls_ = B3BdiDerivativesValidLotsSettlementArabicaCoffeeContracts(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI DERIVATIVES VALID LOTS SETTLEMENT ARABICA COFFEE CONTRACTS: \n{df_}")
df_.info()
