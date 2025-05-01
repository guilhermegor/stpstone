from stpstone.ingestion.countries.br.exchange.anbima_site_infos import AnbimaInfos

cls_ = AnbimaInfos()

# df_ = cls_.source("br_treasuries", bl_fetch=True)
# print(f"DF ANBIMA SITE INFOS - BR TREASURIES: \n{df_}")
# df_.info()

# df_ = cls_.source("corporate_bonds", bl_fetch=True)
# print(f"DF ANBIMA SITE INFOS - CORPORATE BONDS: \n{df_}")
# df_.info()

df_ = cls_.source("ima_p2_pvs", bl_fetch=True)
print(f"DF ANBIMA SITE INFOS - IMA P2 PVs: \n{df_}")
df_.info()

df_ = cls_.source("ima_p2_th_portf", bl_fetch=True)
print(f"DF ANBIMA SITE INFOS - IMA P2 TH PORTF: \n{df_}")
df_.info()
