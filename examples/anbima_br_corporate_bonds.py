"""ANBIMA Brazilian corporate bonds (debentures) OTC market data."""

from stpstone.ingestion.countries.br.otc.anbima_br_corporate_bonds import AnbimaExchangeInfosBRCorporateBonds


cls_ = AnbimaExchangeInfosBRCorporateBonds(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF ANBIMA BR CORPORATE BONDS: \n{df_}")
df_.info()
