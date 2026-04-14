from stpstone.utils.connections.netops.proxies._free.free_proxy_list_net import FreeProxyNet
from stpstone.utils.connections.netops.proxies._free.spysme import SpysMeCountries
from stpstone.utils.connections.netops.proxies._free.spysone import SpysOneCountry


# cls_ = SpysMeCountries(
#     bool_new_proxy=True,
#     dict_proxies=None,
#     int_retries=10,
#     int_backoff_factor=1,
#     bool_alive=True,
#     list_anonymity_value=["anonymous", "elite"],
#     list_protocol="http",
#     str_continent_code=None,
#     str_country_code="BR",
#     bool_ssl=None,
#     float_min_ratio_times_alive_dead=0.02,
#     float_max_timeout=600,
#     bool_use_timer=False,
#     list_status_forcelist=[429, 500, 502, 503, 504],
#     logger=None
# )

# print(cls_._available_proxies())

# cls_ = FreeProxyNet(
#     bool_new_proxy=True,
#     dict_proxies=None,
#     int_retries=10,
#     int_backoff_factor=1,
#     bool_alive=True,
#     list_anonymity_value=["anonymous", "elite"],
#     list_protocol=["http", "https"],
#     str_continent_code=None,
#     str_country_code="BR",
#     bool_ssl=None,
#     float_min_ratio_times_alive_dead=0.02,
#     float_max_timeout=600,
#     bool_use_timer=False,
#     list_status_forcelist=[429, 500, 502, 503, 504],
#     logger=None
# )

# print(cls_._available_proxies())

cls_ = SpysOneCountry(
	bool_new_proxy=True,
	dict_proxies=None,
	int_retries=10,
	int_backoff_factor=1,
	bool_alive=True,
	list_anonymity_value=["anonymous", "elite"],
	list_protocol=["http", "https"],
	str_continent_code=None,
	str_country_code="BR",
	bool_ssl=None,
	float_min_ratio_times_alive_dead=0.02,
	float_max_timeout=600,
	bool_use_timer=False,
	list_status_forcelist=[429, 500, 502, 503, 504],
	logger=None,
)

print(cls_._available_proxies())
