from getpass import getuser

from stpstone.ingestion.countries.br.registries.anbima_data_debentures import AnbimaDataDebentures
from stpstone.utils.calendars.calendar_abc import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy
from stpstone.utils.connections.netops.scraping.user_agents import UserAgents


cls_session = YieldFreeProxy(
    bool_new_proxy=True,
    str_country_code="BR",
    str_continent_code=None,
    bool_alive=True,
    list_anonymity_value=["elite", "anonymous"],
    list_protocol=["http", "https"],
    bool_ssl=None,
    float_min_ratio_times_alive_dead=None,
    float_max_timeout=10000,
)

if cls_session.session.proxies == {}:
    raise Exception("No proxies available")
print(f"Proxies available: {cls_session.session.proxies}")

cls_ = AnbimaDataDebentures(
    session=cls_session.session,
    list_slugs=["AEAM22", "AALM11"],
    str_user_agent=UserAgents().get_random_user_agent(),
    int_wait_load_seconds=30,
    bool_headless=False,
    bool_incognito=True
)

# df_ = cls_.source("debentures_available", bool_fetch=True)
# print(f"DF DEBENTURES AVL: \n{df_}")
# df_.info()
# df_.to_csv("data/anbima-debentures-avl_{}_{}_{}.csv".format(
#     getuser(),
#     DatesBR().curr_date().strftime('%Y%m%d'),
#     DatesBR().curr_time().strftime('%H%M%S')
# ), index=False)

df_ = cls_.source("debentures_registries", bool_fetch=True)
print(f"DF DEBENTURES REGISTRIES: \n{df_}")
df_.info()
df_.to_csv("data/anbima-debentures-registries_{}_{}_{}.csv".format(
    getuser(),
    DatesBR().curr_date().strftime('%Y%m%d'),
    DatesBR().curr_time().strftime('%H%M%S')
), index=False)
