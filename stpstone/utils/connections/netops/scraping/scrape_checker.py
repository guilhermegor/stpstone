"""Web scraping permission checker using urllib.robotparser.

This module provides a class for checking if web scraping is allowed for a given URL
based on its robots.txt file, following standard web crawling protocols.
"""

from urllib import robotparser

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class ScrapeChecker(metaclass=TypeChecker):
	"""Class for checking web scraping permissions using robots.txt."""

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

	def _validate_user_agent(self, user_agent: str) -> None:
		"""Validate user agent string.

		Parameters
		----------
		user_agent : str
			User agent string to validate

		Raises
		------
		ValueError
			If user agent is empty
		"""
		if not user_agent:
			raise ValueError("User agent cannot be empty")

	def is_allowed(self, url: str, user_agent: str = "*") -> bool:
		"""Check if scraping is allowed for a given URL and user agent.

		Parameters
		----------
		url : str
			URL to check for scraping permission
		user_agent : str
			User agent string (default: "*")

		Returns
		-------
		bool
			True if scraping is allowed, False otherwise

		Raises
		------
		RuntimeError
			If robots.txt cannot be fetched or parsed

		References
		----------
		.. [1] https://docs.python.org/3/library/urllib.robotparser.html
		"""
		self._validate_url(url)
		self._validate_user_agent(user_agent)

		try:
			rp = robotparser.RobotFileParser()
			rp.set_url(f"{url}/robots.txt")
			rp.read()
			return rp.can_fetch(user_agent, url)
		except Exception as err:
			raise RuntimeError(f"Failed to fetch or parse robots.txt: {str(err)}") from err
