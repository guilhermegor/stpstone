from getpass import getuser
from stpstone.ingestion.countries.br.registries.mais_retorno_instruments import MaisRetornoFunds
from stpstone.utils.cals.handling_dates import DatesBR


# ### --- AVAILABLE FUNDS --- ###

# cls_ = MaisRetornoFunds(list_slugs=range(1, 10), int_wait_load_seconds=60, 
#                         int_delay_seconds=30, bl_save_html=False,
#                         bl_headless=False, bl_incognito=True)

# df_ = cls_.source("avl_funds", bl_fetch=True)
# print(f"DF MAIS RETORNO AVAILABLE FUNDS: \n{df_}")
# df_.to_csv("data/mais-retorno-available-funds_{}_{}_{}.csv".format(
#     getuser(),
#     DatesBR().curr_date.strftime('%Y%m%d'),
#     DatesBR().curr_time.strftime('%H%M%S')
# ), index=False)
# df_.info()


# ### --- FUNDS PROPERTIES --- ###

# list_slugs = ["aasl-fia", "spx-falcon-2-fif-cic-acoes-rl", 
#               "abn-amro-as-apliacacao-cotas-fi-financeiro", 
#               "abn-amro-as-aplicacao-cotas-fi-financeiro-hematita", 
#               "abn-amro-as-aplicacao-cotas-fi-marconi", "abn-amro-as-fic-fim-4e", 
#               "051-mhx-rv-fi-financeiro-multimercado-cp-rl", "051-mm-hv-fic-fim-cp-ie", 
#               "051-pe-i-fi-financeiro-multimercado", "zurich-bnpp-zv-1-fim-previdenciario", 
#               "zurich-bnpp-fic-fi-rf-previdenciario", "zurich-bnpp-fic-fim-previdenciario", 
#               "zurich-anga-previdenciario-cp-fim", "zunar-prev-fim", "zula-fif-mult-cp-rl-1",
#               "zula-2-fif-acoes-rl-1"]

# cls_ = MaisRetornoFunds(int_wait_load_seconds=60, int_delay_seconds=30, bl_save_html=False,
#                         bl_headless=False, bl_incognito=True, list_slugs=list_slugs)

# df_ = cls_.source("fund_properties")
# print(f"DF MAIS RETORNO FUNDS: \n{df_}")
# df_.to_csv("data/mais-retorno-fund-properties_{}_{}_{}.csv".format(
#     getuser(),
#     DatesBR().curr_date.strftime('%Y%m%d'),
#     DatesBR().curr_time.strftime('%H%M%S')
# ), index=False)
# df_.info()


# ### --- AVAILABLE INSTRUMENTS --- ###

# for instruments_class in ["lista-fi-infra", "lista-fip", "lista-fiagro"]:
#     cls_ = MaisRetornoFunds(list_slugs=range(1, 10), int_wait_load_seconds=60, 
#                             int_delay_seconds=30, bl_save_html=False,
#                             bl_headless=True, bl_incognito=True, 
#                             instruments_class=instruments_class)
#     print(f"\n*** INSTURMENT CLASS - {instruments_class.upper()} ***")
#     df_ = cls_.source("avl_instruments", bl_fetch=True)
#     print(f"DF MAIS RETORNO AVAILABLE INSTRUMENTS - {instruments_class.upper()}: \n{df_}")
#     df_.to_csv("data/mais-retorno-available-instruments_{}_{}_{}.csv".format(
#         getuser(),
#         DatesBR().curr_date.strftime('%Y%m%d'),
#         DatesBR().curr_time.strftime('%H%M%S')
#     ), index=False)
#     df_.info()


# ### --- INSTRUMENTS HISTORIC RENTABILITY --- ###


# for list_slugs, url_slug in [
#     (["abcp11", "afhi11", "aiec11", "ajfi11", "almi11", "alzc11", "alzm11", "brco11", "icri11"], "fii"),
#     (["cdi", "dolar", "bdrx", "gptw", "ibbr", "ibhb", "iblv", "ibov-usd", "ibov"], "indice"),
#     (["bidb11", "binc11", "bodb11", "cdii11", "cpti11", "divs11", "exif11", "ifra11", "ifri11"], "fi-infra"),
# ]:
#     cls_ = MaisRetornoFunds(int_wait_load_seconds=60, int_delay_seconds=30, bl_save_html=False,
#                             bl_headless=True, bl_incognito=True, list_slugs=list_slugs, 
#                             instruments_class=url_slug)
#     df_ = cls_.source("instruments_historical_rentability", bl_fetch=True)
#     print(f"DF MAIS RETORNO - INSTRUMENTS HISTORIC RENTABILITY - CLASS: {url_slug.upper()}: \n{df_}")
#     df_.to_csv("data/mais-retorno-instruments-historic-rentability_{}_{}_{}.csv".format(
#         getuser(),
#         DatesBR().curr_date.strftime('%Y%m%d'),
#         DatesBR().curr_time.strftime('%H%M%S')
#     ), index=False)
#     df_.info()


### --- INSTRUMENTS STATS --- ###

for list_slugs, url_slug in [
    (["abcp11", "afhi11", "aiec11", "ajfi11", "almi11", "alzc11", "alzm11", "brco11", "icri11"], "fii"),
    (["cdi", "dolar", "bdrx", "gptw", "ibbr", "ibhb", "iblv", "ibov-usd", "ibov"], "indice"),
    (["bidb11", "binc11", "bodb11", "cdii11", "cpti11", "divs11", "exif11", "ifra11", "ifri11"], "fi-infra"),
]:
# for list_slugs, url_slug in [
#     (["abcp11", "afhi11"], "fii"),
# ]:
    cls_ = MaisRetornoFunds(int_wait_load_seconds=60, int_delay_seconds=30, bl_save_html=False,
                            bl_headless=True, bl_incognito=True, list_slugs=list_slugs, 
                            instruments_class=url_slug)
    df_ = cls_.source("instruments_stats", bl_fetch=True)
    print(f"DF MAIS RETORNO - INSTRUMENTS STATS - CLASS: {url_slug.upper()}: \n{df_}")
    df_.to_csv("data/mais-retorno-instruments-stats_{}_{}_{}.csv".format(
        getuser(),
        DatesBR().curr_date.strftime('%Y%m%d'),
        DatesBR().curr_time.strftime('%H%M%S')
    ))