"""World Government Bonds sovereign bond spreads."""

from stpstone.ingestion.countries.ww.macroeconomics.world_gov_bonds_sovereign_spreads import (
    WorldGovBondsSovereignSpreads,
)


cls_ = WorldGovBondsSovereignSpreads(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF WORLD GOV BONDS SOVEREIGN SPREADS: \n{df_}")
df_.info()
