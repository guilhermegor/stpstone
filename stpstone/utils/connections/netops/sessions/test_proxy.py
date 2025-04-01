import time
from typing import Dict, Any, Union, List
from urllib3.util import Retry
from requests import Session
from requests.adapters import HTTPAdapter


class ProxyTester:

    def __init__(self, ip: str, port: int) -> None:
        self.ip = ip
        self.port = port
        self.list_status_forcelist: List[int] = [429, 500, 502, 503, 504]

    def _configure_session(self, dict_proxy:Union[Dict[str, str], None]=None,
                            int_retries:int=10, int_backoff_factor:int=1) -> Session:
            retry_strategy = Retry(
                total=int_retries,
                backoff_factor=int_backoff_factor,
                status_forcelist=self.list_status_forcelist
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session = Session()
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            if dict_proxy is not None:
                session.proxies.update(dict_proxy)
            return session

    @property
    def test_specific_proxy(self, test_url: str = "https://lumtest.com/myip.json") \
        -> Dict[str, Any]:
        """
        Tests a specific proxy IP and port combination

        Args:
            ip: Proxy IP address to test
            port: Proxy port to test
            test_url: URL to use for testing (default checks IP)

        Returns:
            Dictionary with test results containing:
            - success: bool indicating if proxy worked
            - response_time: float with response time in seconds
            - error: str with error message if failed
            - response: dict with response data if successful
        """
        result = {
            'success': False,
            'response_time': None,
            'error': None,
            'response': None
        }
        start_time = time.time()
        try:
            session = self._configure_session(
                dict_proxy={
                    "http": f"http://{self.ip}:{self.port}",
                    "https": f"http://{self.ip}:{self.port}"
                },
                int_retries=0,
                int_backoff_factor=0
            )
            response = session.get(
                test_url,
                headers={
                    "accept": "application/json",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
                },
                timeout=(5, 10)
            )
            response.raise_for_status()
            result.update({
                'success': True,
                'response_time': time.time() - start_time,
                'response': response.json()
            })
        except Exception as e:
            result['error'] = str(e)
        return result
