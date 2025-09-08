"""Consolidated Trades After Market."""

from stpstone.ingestion.countries.br.exchange.b3_consolidated_trades_after_mkt import (
    B3ConsolidatedTradesAfterMarket,
)


cls_ = B3ConsolidatedTradesAfterMarket(
    date_ref=None, 
    logger=None, 
    cls_db=None
)

df_ = cls_.run()

print(f"DF CONSOLIDATED TRADES AFTER MARKET: \n{df_}")
df_.info()

