"""B3 FX market contracted transactions with rates and opening parameters."""

from stpstone.ingestion.countries.br.exchange.b3_fx_market_contracted_transactions import (
	B3FXMarketContractedTransactions,
)


cls_ = B3FXMarketContractedTransactions(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 F X MARKET CONTRACTED TRANSACTIONS: \n{df_}")
df_.info()
