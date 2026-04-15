"""CVM Liquid Funds Daily Information from the FIF Inf Diario dataset."""

from stpstone.ingestion.countries.br.registries.fif_daily_infos import FIFDailyInfos


cls_ = FIFDailyInfos(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF FIF Daily Infos: \n{df_}")
df_.info()
