from getpass import getuser
from stpstone.ingestion.countries.br.registries.anbima_data_debentures import AnbimaDataDebentures
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy
from stpstone.utils.connections.netops.scraping.user_agents import UserAgents
from stpstone.utils.cals.handling_dates import DatesBR


cls_session = YieldFreeProxy(
    bl_new_proxy=True,
    str_country_code="BR",
    str_continent_code=None,
    bl_alive=True,
    list_anonymity_value=["elite", "anonymous"],
    list_protocol=["http", "https"],
    bl_ssl=None,
    float_min_ratio_times_alive_dead=None,
    float_max_timeout=10000,
)

if cls_session.session.proxies == {}:
    raise Exception("No proxies available")
print(f"Proxies available: {cls_session.session.proxies}")

cls_ = AnbimaDataDebentures(
    session=cls_session.session,
    list_slugs=["AEAM22", "AALM11"],
    str_user_agent=UserAgents().get_random_user_agent,
    int_wait_load_seconds=30,
    bl_headless=False,
    bl_incognito=True
)

# df_ = cls_.source("debentures_available", bl_fetch=True)
# print(f"DF DEBENTURES AVL: \n{df_}")
# df_.info()
# df_.to_csv("data/anbima-debentures-avl_{}_{}_{}.csv".format(
#     getuser(),
#     DatesBR().curr_date.strftime('%Y%m%d'),
#     DatesBR().curr_time.strftime('%H%M%S')
# ), index=False)

df_ = cls_.source("debentures_registries", bl_fetch=True)
print(f"DF DEBENTURES REGISTRIES: \n{df_}")
df_.info()
df_.to_csv("data/anbima-debentures-registries_{}_{}_{}.csv".format(
    getuser(),
    DatesBR().curr_date.strftime('%Y%m%d'),
    DatesBR().curr_time.strftime('%H%M%S')
), index=False)
