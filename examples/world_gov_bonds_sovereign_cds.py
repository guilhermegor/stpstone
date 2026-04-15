"""World Government Bonds sovereign CDS spreads."""

from stpstone.ingestion.countries.ww.macroeconomics.world_gov_bonds_sovereign_cds import (
    WorldGovBondsSovereignCds,
)


cls_ = WorldGovBondsSovereignCds(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF WORLD GOV BONDS SOVEREIGN CDS: \n{df_}")
df_.info()
