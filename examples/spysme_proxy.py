from stpstone.utils.connections.netops.sessions.spysme import SpysMeCountries

cls_ = SpysMeCountries(
    bl_new_proxy=True,
    dict_proxies=None,
    int_retries=10,
    int_backoff_factor=1,
    bl_alive=True,
    list_anonymity_value=["anonymous", "elite"],
    list_protocol="http",
    str_continent_code=None,
    str_country_code="BR",
    bl_ssl=None,
    float_min_ratio_times_alive_dead=0.02,
    float_max_timeout=600,
    bl_use_timer=False,
    list_status_forcelist=[429, 500, 502, 503, 504],
    logger=None
)

print(cls_._available_proxies)
