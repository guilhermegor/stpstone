"""S&P Global corporate rating actions paginated orchestrator."""

from stpstone.ingestion.countries.ww.registries.ratings_corp_spglobal import RatingsCorpSPGlobal


cls_ = RatingsCorpSPGlobal(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.get_corp_ratings
print(f"DF RATINGS CORP SP GLOBAL: \n{df_}")
df_.info()
