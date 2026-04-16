"""Debentures.com.br mark-to-market historical PU data."""

from stpstone.ingestion.countries.br.otc.debentures_com_br_mtm import DebenturesComBrMTM


cls_ = DebenturesComBrMTM(
    date_start=None,
    date_end=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF DEBENTURES COM BR MTM: \n{df_}")
df_.info()
