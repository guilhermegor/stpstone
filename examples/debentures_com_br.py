from stpstone.ingestion.countries.br.otc.debentures_com_br import DebenturesComBrMTM


cls_ = DebenturesComBrMTM(
    date_start=None,
    date_end=None,
    logger=None,
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF DEBENTURES COM BR MTM: \n{df_}")
df_.info()