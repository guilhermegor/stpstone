"""S&P Global corporate rating actions for a single result page."""

from stpstone.ingestion.countries.ww.registries.ratings_corp_spglobal_one_page import (
    RatingsCorpSPGlobalOnePage,
)


cls_ = RatingsCorpSPGlobalOnePage(
    bearer="",
    token=None,
    pg_number=1,
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF RATINGS CORP SP GLOBAL ONE PAGE: \n{df_}")
df_.info()
