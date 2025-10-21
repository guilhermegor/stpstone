"""Example of how to use FIFDailyInfos class to ingest data from CVM."""

from stpstone.ingestion.countries.br.registries.cvm_data import FIFDailyInfos, FIFMonthlyProfile


# cls_ = FIFDailyInfos(
#     date_ref=None, 
#     logger=None, 
#     cls_db=None,
# )

# df_ = cls_.run()
# print(f"DF CVM DATA: \n{df_}")
# df_.info()


cls_ = FIFMonthlyProfile(
    date_ref=None, 
    logger=None, 
    cls_db=None,
)

df_ = cls_.run()
print(f"DF CVM MONTHLY PROFILE: \n{df_}")
df_.info()