import backoff
import requests
import random
from typing import List
from lxml import html
from requests.exceptions import (ReadTimeout, ConnectTimeout, ChunkedEncodingError,
                                 RequestException, HTTPError)


class UserAgents:

    @property
    @backoff.on_exception(
        backoff.expo,
        (RequestException, HTTPError, ReadTimeout, ConnectTimeout, ChunkedEncodingError),
        max_tries=20,
        base=2,
        factor=2,
        max_value=1200
    )
    def fetch_user_agents(self) -> List[str]:
        list_ = list()
        url = "https://gist.github.com/pzb/b4b6f57144aea7827ae4"
        xpath = '//*[@id="file-user-agents-txt-LC{}"]/text()'
        i = 1
        dict_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        resp_req = requests.get(url, headers=dict_headers, timeout=10)
        resp_req.raise_for_status()
        tree = html.fromstring(resp_req.content)
        while True:
            agent = tree.xpath(xpath.format(i))
            if not agent:
                break
            list_.append(agent[0].strip())
            i += 1
        return list_

    @property
    def get_random_user_agent(self):
        fallback_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/119.0",
        ]
        list_ = self.fetch_user_agents
        if list_ and len(list_) > 0:
            random_index = random.randint(0, len(list_) - 1)
            return list_[random_index]
        else:
            return random.choice(fallback_agents)
