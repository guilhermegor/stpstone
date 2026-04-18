"""CVM Liquid Funds Daily Information from the FIF Inf Diario dataset."""

from stpstone.ingestion.countries.br.registries.cvm_fif_daily_infos import CvmFIFDailyInfos


cls_ = CvmFIFDailyInfos(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF FIF Daily Infos: \n{df_}")
df_.info()
