"""CVM Liquid Funds Monthly Profile from the FIF Perfil Mensal dataset."""

from stpstone.ingestion.countries.br.registries.cvm_fif_monthly_profile import CvmFIFMonthlyProfile


cls_ = CvmFIFMonthlyProfile(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF FIF Monthly Profile: \n{df_}")
df_.info()
