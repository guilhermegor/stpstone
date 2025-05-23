from stpstone.utils.connections.netops.proxies.load_testing import ProxyLoadTester


cls_load_test_proxies = ProxyLoadTester(
    int_retries_new_proxies_not_mapped=20,
    max_iter_find_healthy_proxy=30,
    str_country_code="BR",
    bl_new_proxy=True,
    str_continent_code=None,
    bl_alive=True,
    list_anonymity_value=["elite", "anonymous"],
    list_protocol=["http", "https"],
    bl_ssl=None,
    float_min_ratio_times_alive_dead=None,
    float_max_timeout=10000,
    bl_use_timer=False,
    list_status_forcelist=[429, 500, 502, 503, 504],
    logger=None,
    str_plan_id_webshare="free",
    timeout_session=1000.0,
    int_wait_load_seconds=100,
)

cls_load_test_proxies.run_tests(n_trials=20)
