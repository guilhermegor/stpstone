"""Debentures.com.br OTC secondary market negotiation prices."""

from stpstone.ingestion.countries.br.otc.debentures_com_br_otc_pvs import DebenturesComBrOTCPVs


cls_ = DebenturesComBrOTCPVs(
    date_start=None,
    date_end=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF DEBENTURES COM BR OTC PVs: \n{df_}")
df_.info()
