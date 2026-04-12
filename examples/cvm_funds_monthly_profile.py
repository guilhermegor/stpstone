"""CVM Funds Monthly Profile - Brazilian fund monthly profile report from CVM open data."""

from stpstone.ingestion.countries.br.registries.cvm_funds_monthly_profile import (
	CvmFundsMonthlyProfile,
)


cls_ = CvmFundsMonthlyProfile(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF CVM FUNDS MONTHLY PROFILE: \n{df_}")
df_.info()
