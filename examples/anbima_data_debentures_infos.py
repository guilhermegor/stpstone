from stpstone.ingestion.countries.br.registries.anbima_data_debentures import AnbimaDataDebentures
from stpstone.utils.connections.netops.sessions.proxy_nova import ProxyNova
from stpstone.utils.connections.netops.user_agents import UserAgents


session = ProxyNova("br").sessions

cls_ = AnbimaDataDebentures(
    session=session,
    list_slugs=["AEAM22", "AALM11"],
    str_user_agent=UserAgents().get_random_user_agent
)

df_deb_infos = cls_.source("debentures_registries", bl_fetch=True, bl_debug=False)
print(f"DF: \n{df_deb_infos}")
df_deb_infos.info()
