"""World Government Bonds country credit ratings."""

from stpstone.ingestion.countries.ww.macroeconomics.world_gov_bonds_countries_ratings import (
    WorldGovBondsCountriesRatings,
)


cls_ = WorldGovBondsCountriesRatings(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF WORLD GOV BONDS COUNTRIES RATINGS: \n{df_}")
df_.info()
