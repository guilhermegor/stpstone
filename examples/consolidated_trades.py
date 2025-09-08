"""Consolidated Trades."""

from stpstone.ingestion.countries.br.exchange.b3_consolidated_trades import (
    B3ConsolidatedTrades,
)


cls_ = B3ConsolidatedTrades(
    date_ref=None, 
    logger=None, 
    cls_db=None
)

df_ = cls_.run()

print(f"DF CONSOLIDATED TRADES AFTER MARKET: \n{df_}")
df_.info()

