"""B3 Warranties - Brazilian Sovereign Bonds Accepted as Collateral."""

from stpstone.ingestion.countries.br.exchange.b3_warranties_br_sovereign_bonds import (
	B3WarrantiesBRSovereignBonds,
)


cls_ = B3WarrantiesBRSovereignBonds(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 WARRANTIES BR SOVEREIGN BONDS: \n{df_}")
df_.info()
