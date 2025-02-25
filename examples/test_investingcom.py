from requests import Session, Response
from typing import Optional, Dict, Any, Tuple
import random
import time
from fake_useragent import UserAgent
from urllib3.exceptions import InsecureRequestWarning
import urllib3

class ImprovedRequestHandler:
    def __init__(self):
        self.session = Session()
        self.user_agent = UserAgent()
        # Suppress only the specific InsecureRequestWarning
        urllib3.disable_warnings(InsecureRequestWarning)
        
    def _get_random_headers(self) -> Dict[str, str]:
        """Generate random headers to mimic browser behavior."""
        return {
            'User-Agent': self.user_agent.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        }
    
    def _add_delay(self):
        """Add random delay between requests."""
        time.sleep(random.uniform(1, 3))
    
    def make_request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        verify: bool = True,
        timeout: Tuple[float, float] = (10, 30),
        max_retries: int = 3,
        backoff_factor: float = 2
    ) -> Response:
        """
        Make an HTTP request with improved handling.
        
        Args:
            method: HTTP method ('GET' or 'POST')
            url: Target URL
            headers: Optional custom headers
            params: Optional URL parameters
            verify: Whether to verify SSL certificates
            timeout: Request timeout (connect, read)
            max_retries: Maximum number of retry attempts
            backoff_factor: Multiplicative factor for backoff between retries
            
        Returns:
            Response object
        """
        # Merge default headers with custom headers
        request_headers = self._get_random_headers()
        if headers:
            request_headers.update(headers)
            
        attempt = 0
        last_exception = None
        
        while attempt < max_retries:
            try:
                self._add_delay()
                
                response = self.session.request(
                    method=method,
                    url=url,
                    headers=request_headers,
                    params=params,
                    verify=verify,
                    timeout=timeout
                )
                
                # Check for 403/429 status codes
                if response.status_code in (403, 429):
                    wait_time = backoff_factor ** attempt
                    time.sleep(wait_time)
                    # Rotate headers on retry
                    request_headers = self._get_random_headers()
                    attempt += 1
                    continue
                    
                response.raise_for_status()
                return response
                
            except Exception as e:
                last_exception = e
                wait_time = backoff_factor ** attempt
                time.sleep(wait_time)
                attempt += 1
                
        # If we've exhausted all retries, raise the last exception
        raise last_exception or Exception("Request failed after all retries")

# Usage example for ADVFN request
def get_investincom(ticker:str) -> Response:
    handler = ImprovedRequestHandler()
    
    url = f"https://tvc4.investing.com/725910b675af9252224ca6069a1e73cc/1631836267/1/1/8/symbols?symbol={ticker}"
    print(f'URL: {url}')
    params = {}
    
    return handler.make_request(
        method="GET",
        url=url,
        params=params,
        verify=False  # Since you're using a proxy
    )

print(get_investincom('PETR4'))