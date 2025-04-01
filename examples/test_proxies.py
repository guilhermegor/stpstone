from stpstone.utils.connections.netops.sessions.manager import YieldProxy


test_proxies = YieldProxy(
    bl_new_proxy=True,
    str_country_code="BR",
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
    max_iter_find_healthy_proxy=10,
)

try:
    session = next(test_proxies)
    print(f"Proxy: {session.proxies}")
    response = session.get("https://jsonplaceholder.typicode.com/todos/1")
    print(f"Proxy Test Successful. Data: {response.json()}")
except Exception as e:
    print(f"Test failed: {str(e)}")
