"""Debentures.com.br debenture characteristics and issuance information."""

from stpstone.ingestion.countries.br.otc.debentures_com_br_infos import DebenturesComBrInfos


cls_ = DebenturesComBrInfos(
    date_start=None,
    date_end=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF DEBENTURES COM BR INFOS: \n{df_}")
df_.info()
