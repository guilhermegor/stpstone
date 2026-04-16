"""User agent string retrieval and management utilities.

This module provides a class for fetching and managing user agent strings from online sources
using requests and lxml for web scraping with robust error handling and fallback mechanisms.
"""

import random

import backoff
from lxml import html
import requests
from requests.exceptions import (
	ChunkedEncodingError,
	ConnectTimeout,
	HTTPError,
	ReadTimeout,
	RequestException,
)

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class UserAgents(metaclass=TypeChecker):
	"""Class for managing user agent string retrieval and selection."""

	@backoff.on_exception(
		backoff.expo,
		(RequestException, HTTPError, ReadTimeout, ConnectTimeout, ChunkedEncodingError),
		max_tries=20,
		base=2,
		factor=2,
		max_value=1200,
	)
	def fetch_user_agents(self) -> list[str]:
		"""Fetch user agent strings from online source.

		Returns
		-------
		list[str]
			list of user agent strings

		Raises
		------
		ValueError
			If response content cannot be parsed or is empty

		References
		----------
		.. [1] https://gist.github.com/pzb/b4b6f57144aea7827ae4
		"""
		url = "https://gist.github.com/pzb/b4b6f57144aea7827ae4"
		xpath = '//*[@id="file-user-agents-txt-LC{}"]/text()'
		list_ = list()
		i = 1

		dict_headers = {
			"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",  # noqa E501: line too long
			"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
			"Accept-Language": "en-US,en;q=0.5",
		}

		self._validate_url(url)
		resp_req = requests.get(url, headers=dict_headers, timeout=10)
		resp_req.raise_for_status()

		try:
			tree = html.fromstring(resp_req.content)
			while True:
				agent = tree.xpath(xpath.format(i))
				if not agent:
					break
				list_.append(agent[0].strip())
				i += 1
			return list_
		except Exception as err:
			raise ValueError("Failed to parse HTML content") from err

	def get_random_user_agent(self) -> str:
		"""Get a random user agent string from fetched list or fallback options.

		Returns
		-------
		str
			Randomly selected user agent string
		"""
		fallback_agents = [
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",  # noqa E501: line too long
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",  # noqa E501: line too long
			"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",  # noqa E501: line too long
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/119.0",
		]

		try:
			list_ = self.fetch_user_agents()
			if list_ and len(list_) > 0:
				random_index = random.randint(0, len(list_) - 1)  # noqa S311: pseoud-random generators are not suitable for cryptography
				return list_[random_index]
			return random.choice(fallback_agents)  # noqa S311: pseoud-random generators are not suitable for cryptography
		except Exception:
			return random.choice(fallback_agents)  # noqa S311: pseoud-random generators are not suitable for cryptography

	def _validate_url(self, url: str) -> None:
		"""Validate URL format and content.

		Parameters
		----------
		url : str
			URL to validate

		Raises
		------
		ValueError
			If URL is empty
			If URL does not start with http:// or https://
		"""
		if not url:
			raise ValueError("URL cannot be empty")
		if not (url.startswith("http://") or url.startswith("https://")):
			raise ValueError("URL must start with http:// or https://")
