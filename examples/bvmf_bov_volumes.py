"""BVMF BOV Trading Volumes."""

from stpstone.ingestion.countries.br.exchange.bvmf_bov_volumes import BVMFVBOVTradingVolumes


cls_ = BVMFVBOVTradingVolumes(date_ref=None, logger=None, cls_db=None)

df_ = cls_.run()
print(f"DF BVMF BOV TRADING VOLUMES: \n{df_}")
df_.info()
