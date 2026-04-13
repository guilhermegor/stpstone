"""ANBIMA Brazilian Government Treasuries secondary market data."""

from stpstone.ingestion.countries.br.otc.anbima_br_treasuries import AnbimaExchangeInfosBRTreasuries


cls_ = AnbimaExchangeInfosBRTreasuries(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF ANBIMA BR TREASURIES: \n{df_}")
df_.info()
