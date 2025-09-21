"""Example for Debentures.com.br."""
from stpstone.ingestion.countries.br.otc.debentures_com_br import (
    DebenturesComBrInfos,
    DebenturesComBrMTM,
    DebenturesComBrOTCPVs,
)
from stpstone.utils.calendars.calendar_br import DatesBRAnbima


cls_ = DebenturesComBrOTCPVs(
    date_start=None,
    date_end=None,
    logger=None,
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF DEBENTURES COM BR OTC PVs: \n{df_}")
df_.info()


cls_ = DebenturesComBrInfos(
    date_start=None,
    date_end=None,
    logger=None,
    cls_db=None
)

timestamp_ = DatesBRAnbima().current_timestamp_string()
df_ = cls_.run(bool_verify=False)
print(f"DF DEBENTURES COM BR INFOS: \n{df_}")
df_.info()


cls_ = DebenturesComBrMTM(
    date_start=None,
    date_end=None,
    logger=None,
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF DEBENTURES COM BR MTM: \n{df_}")
df_.info()