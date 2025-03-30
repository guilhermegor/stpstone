from stpstone.ingestion.countries.br.registries.anbima_data_debentures import AnbimaDataDebentures
from stpstone.utils.connections.netops.sessions.proxy_nova import ProxyNova

session = ProxyNova("br").sessions

cls_ = AnbimaDataDebentures(

)
