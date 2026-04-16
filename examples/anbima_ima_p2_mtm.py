"""ANBIMA IMA-P2 index mark-to-market data."""

from stpstone.ingestion.countries.br.exchange.anbima_ima_p2_mtm import AnbimaExchangeBRIMAP2MTMs


cls_ = AnbimaExchangeBRIMAP2MTMs(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF ANBIMA IMA P2 MTMs: \n{df_}")
df_.info()
