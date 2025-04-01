import time
from stpstone.utils.connections.netops.sessions.manager import YieldProxy
from stpstone.utils.loggs.create_logs import CreateLog

time_ = time.time()

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
    max_iter_find_healthy_proxy=20,
    timeout_session=1000.0,
)

def test_proxy_session(session, test_num):
    try:
        print(f"\n--- Testing Proxy #{test_num} ---")
        print(f"Proxy: {session.proxies}")
        resp_req = session.get("https://jsonplaceholder.typicode.com/todos/1", timeout=10)
        resp_req.raise_for_status()
        print("Proxy Test Successful!")
        print(f"Response Status: {resp_req.status_code}")
        print(f"Response Data: {resp_req.json()}")
        return True
    except Exception as e:
        print(f"Proxy Test Failed: {str(e)}")
        return False

successful_tests = 0
n_trials = 20

for i in range(1, n_trials + 1):
    try:
        session = next(test_proxies)
        if test_proxy_session(session, i):
            successful_tests += 1
    except StopIteration:
        CreateLog().log_message(None, "\nNo more proxies available", "critical")
        break
    except Exception as e:
        CreateLog().log_message(None, f"\nError getting proxy #{i}: {str(e)}", "critical")

CreateLog().log_message(None, f"\n--- Test Summary ---", "infos")
CreateLog().log_message(None, f"Total tests attempted: {n_trials}", "infos")
CreateLog().log_message(None, f"Successful tests: {successful_tests}", "infos")
CreateLog().log_message(None, f"Success rate: {successful_tests/n_trials*100:.1f}%", "infos")
CreateLog().log_message(None, f"Elapsed time for {n_trials} trials: "
                        + f"{time.time() - time_:.2f} seconds", "infos")
