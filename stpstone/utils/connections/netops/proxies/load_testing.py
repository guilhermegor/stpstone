import time
from typing import Dict, Any, Union, List, Optional
from requests import Session
from logging import Logger
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy
from stpstone.utils.loggs.create_logs import CreateLog


class ProxyLoadTester:

    def __init__(
        self,
        bl_new_proxy: bool = True,
        dict_proxies: Union[Dict[str, str], None] = None,
        int_retries_new_proxies_not_mapped: int = 10,
        int_backoff_factor: int = 1,
        bl_alive: bool = True,
        list_anonymity_value: List[str] = ["anonymous", "elite"],
        list_protocol: str = 'http',
        str_continent_code: Union[str, None] = None,
        str_country_code: Union[str, None] = None,
        bl_ssl: Union[bool, None] = None,
        float_min_ratio_times_alive_dead: Optional[float] = 0.02,
        float_max_timeout: Optional[float] = 600,
        bl_use_timer: bool = False,
        list_status_forcelist: List[int] = [429, 500, 502, 503, 504],
        logger: Optional[Logger] = None,
        str_plan_id_webshare: str = "free",
        max_iter_find_healthy_proxy: int = 30,
        timeout_session: Optional[float] = 1000.0,
        int_wait_load_seconds: Optional[int] = 10,
    ) -> None:
        self.bl_new_proxy = bl_new_proxy
        self.dict_proxies = dict_proxies
        self.int_retries_new_proxies_not_mapped = int_retries_new_proxies_not_mapped
        self.int_backoff_factor = int_backoff_factor
        self.bl_alive = bl_alive
        self.list_anonymity_value = list_anonymity_value
        self.list_protocol = list_protocol
        self.str_continent_code = str_continent_code
        self.str_country_code = str_country_code
        self.bl_ssl = bl_ssl
        self.float_min_ratio_times_alive_dead = float_min_ratio_times_alive_dead
        self.float_max_timeout = float_max_timeout
        self.bl_use_timer = bl_use_timer
        self.list_status_forcelist = list_status_forcelist
        self.logger = logger
        self.str_plan_id_webshare = str_plan_id_webshare
        self.max_iter_find_healthy_proxy = max_iter_find_healthy_proxy
        self.timeout_session = timeout_session
        self.int_wait_load_seconds = int_wait_load_seconds
        self.create_log = CreateLog()
        self.time_ = time.time()
        self.set_used_proxies = set()

        self.cls_yield_proxy = YieldFreeProxy(
            bl_new_proxy=bl_new_proxy,
            dict_proxies=dict_proxies,
            int_retries=int_retries_new_proxies_not_mapped,
            int_backoff_factor=int_backoff_factor,
            bl_alive=bl_alive,
            list_anonymity_value=list_anonymity_value,
            list_protocol=list_protocol,
            str_continent_code=str_continent_code,
            str_country_code=str_country_code,
            bl_ssl=bl_ssl,
            float_min_ratio_times_alive_dead=float_min_ratio_times_alive_dead,
            float_max_timeout=float_max_timeout,
            bl_use_timer=bl_use_timer,
            list_status_forcelist=list_status_forcelist,
            logger=logger,
            str_plan_id_webshare=str_plan_id_webshare,
            max_iter_find_healthy_proxy=max_iter_find_healthy_proxy,
            timeout_session=timeout_session,
            int_wait_load_seconds=int_wait_load_seconds,
        )

    def test_proxy_session(self, session: Session, test_num: int) -> bool:
        try:
            self.create_log.log_message(self.logger, f"\n--- Testing Proxy #{test_num} ---")
            self.create_log.log_message(self.logger, f"Proxy: {session.proxies}")
            resp_req = session.get("https://jsonplaceholder.typicode.com/todos/1", timeout=10)
            resp_req.raise_for_status()
            self.create_log.log_message(self.logger, "Proxy Test Successful!")
            self.create_log.log_message(self.logger, f"Response Status: {resp_req.status_code}")
            self.create_log.log_message(self.logger, f"Response Data: {resp_req.json()}")
            return True
        except Exception as e:
            self.create_log.log_message(self.logger, f"Proxy Test Failed: {str(e)}")
            return False

    def run_tests(self, n_trials: int = 20) -> None:
        successful_tests = 0
        for i in range(1, n_trials + 1):
            self.create_log.log_message(self.logger, f"\n--- Testing Proxy #{i} ---", "info")
            int_try = 0
            try:
                session = next(self.cls_yield_proxy)
                while (session.proxies["http"] in self.set_used_proxies) \
                    and (int_try < self.int_retries_new_proxies_not_mapped):
                    self.create_log.log_message(
                        self.logger,
                        f"Proxy {session.proxies} is already used - attempt #{int_try + 1} "
                        + f"/ {self.int_retries_new_proxies_not_mapped}",
                        "warnings"
                    )
                    session = next(self.cls_yield_proxy)
                    int_try += 1
                if int_try >= self.int_retries_new_proxies_not_mapped:
                    self.create_log.log_message(
                        self.logger, f"\n--- Max retries reached for Proxy #{i} ---", "critical")
                    break
                self.set_used_proxies.add(session.proxies["http"])
                if self.test_proxy_session(session, i):
                    successful_tests += 1
            except StopIteration:
                self.create_log.log_message(self.logger, "\nNo more proxies available", "critical")
                break
            except Exception as e:
                self.create_log.log_message(
                    self.logger, f"\nError getting proxy #{i}: {str(e)}", "critical")
        self.create_log.log_message(self.logger, f"\n--- Test Summary ---", "info")
        self.create_log.log_message(self.logger, f"Total tests attempted: {n_trials}", "info")
        self.create_log.log_message(self.logger, f"Successful tests: {successful_tests}", "info")
        self.create_log.log_message(
            self.logger, f"Success rate: {successful_tests/n_trials*100:.1f}%", "info")
        self.create_log.log_message(self.logger,
                                    f"Number of unique proxies used: {len(self.set_used_proxies)}",
                                    "infos")
        self.create_log.log_message(self.logger, f"Elapsed time for {n_trials} trials: "
                                + f"{time.time() - self.time_:.2f} seconds", "info")
