"""Worldwide timezone and geography utilities.

This module provides classes for working with timezones and geographic information
using pycountry and countryinfo libraries. Includes country code validation,
timezone lookups, and continent identification.
"""

from contextlib import suppress
from datetime import datetime
from typing import Literal, Optional, TypedDict
from zoneinfo import ZoneInfo
from zoneinfo._common import ZoneInfoNotFoundError

from countryinfo import CountryInfo
import pycountry
import pycountry_convert as pc

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class ReturnGetCountryDetails(TypedDict):
	"""Typed dictionary for country details return value.

	Parameters
	----------
	name : str
		Country name
	alpha_2 : str
		2-letter country code
	alpha_3 : str
		3-letter country code
	official_name : str
		Official country name (may be empty)
	"""

	name: str
	alpha_2: str
	alpha_3: str
	official_name: str


class WWTimezones(metaclass=TypeChecker):
	"""Class for worldwide timezone operations."""

	def _validate_country_code(self, country_code: str) -> None:
		"""Validate country code format.

		Parameters
		----------
		country_code : str
			Country code to validate (2 or 3 letters)

		Raises
		------
		ValueError
			If country code is empty
			If country code is not a string
			If country code length is not 2 or 3
		"""
		if not country_code:
			raise ValueError("Country code cannot be empty")
		if not isinstance(country_code, str):
			raise ValueError("Country code must be a string")
		if len(country_code) not in (2, 3):
			raise ValueError("Country code must be 2 or 3 characters")

	def get_timezones_by_country_code(self, country_code: str) -> Optional[list[str]]:
		"""Get all timezones for a given country code.

		Parameters
		----------
		country_code : str
			2-letter or 3-letter country code

		Returns
		-------
		Optional[list[str]]
			List of timezone names or None if country not found
		"""
		self._validate_country_code(country_code)
		try:
			country = CountryInfo(country_code.upper())
			return country.timezones()
		except KeyError:
			return None

	def get_countries_in_timezone(self, timezone_name: str) -> list[str]:
		"""Get all country codes using a specific timezone.

		Parameters
		----------
		timezone_name : str
			Timezone name (e.g. 'Europe/London')

		Returns
		-------
		list[str]
			List of 2-letter country codes using this timezone
		"""
		countries = []
		for country in pycountry.countries:
			with suppress(Exception):
				if timezone_name in CountryInfo(country.alpha_2).timezones():
					countries.append(country.alpha_2)
		return countries

	def get_current_time_in_country(self, country_code: str, int_tz: int = 0) -> Optional[str]:
		"""Get current time in a country's timezone.

		Parameters
		----------
		country_code : str
			2-letter or 3-letter country code
		int_tz : int
			Index of timezone to use if multiple exist (default: 0)

		Returns
		-------
		Optional[str]
			Current time in ISO format or None if country not found
		"""
		timezones = self.get_timezones_by_country_code(country_code.upper())
		if not timezones:
			return None
		try:
			tz = ZoneInfo(timezones[int_tz])
			return datetime.now(tz).isoformat(sep=" ")
		except ZoneInfoNotFoundError:
			return None

	def get_country_from_timezone(self, timezone_name: str) -> list[str]:
		"""Alias for get_countries_in_timezone.

		Parameters
		----------
		timezone_name : str
			Timezone name (e.g. 'Europe/London')

		Returns
		-------
		list[str]
			List of 2-letter country codes using this timezone
		"""
		return self.get_countries_in_timezone(timezone_name)

	def get_all_timezones_grouped(self) -> dict[str, list[str]]:
		"""Get all timezones grouped by country code.

		Returns
		-------
		dict[str, list[str]]
			Dictionary dict_ country codes to their timezones
		"""
		dict_ = {}
		for country in pycountry.countries:
			with suppress(Exception):
				dict_[country.alpha_2] = CountryInfo(country.alpha_2).timezones()
		return dict_


class WWGeography(metaclass=TypeChecker):
	"""Class for worldwide geographic operations."""

	def _validate_country_code(self, country_code: str) -> None:
		"""Validate country code format.

		Parameters
		----------
		country_code : str
			Country code to validate (2 or 3 letters)

		Raises
		------
		ValueError
			If country code is empty
			If country code is not a string
			If country code length is not 2 or 3
		"""
		if not country_code:
			raise ValueError("Country code cannot be empty")
		if not isinstance(country_code, str):
			raise ValueError("Country code must be a string")
		if len(country_code) not in (2, 3):
			raise ValueError("Country code must be 2 or 3 characters")

	def get_country_details(self, country_code: str) -> Optional[ReturnGetCountryDetails]:
		"""Get detailed information about a country.

		Parameters
		----------
		country_code : str
			2-letter or 3-letter country code

		Returns
		-------
		Optional[ReturnGetCountryDetails]
			Dictionary with country details or None if not found
		"""
		self._validate_country_code(country_code)
		country = pycountry.countries.get(alpha_2=country_code.upper()) or pycountry.countries.get(
			alpha_3=country_code.upper()
		)
		if not country:
			return None
		return {
			"name": country.name,
			"alpha_2": country.alpha_2,
			"alpha_3": country.alpha_3,
			"official_name": getattr(country, "official_name", ""),
		}

	def is_valid_country_code(self, country_code: str) -> bool:
		"""Check if a country code is valid.

		Parameters
		----------
		country_code : str
			Country code to validate

		Returns
		-------
		bool
			True if valid country code, False otherwise
		"""
		self._validate_country_code(country_code)
		return bool(
			pycountry.countries.get(alpha_2=country_code.upper())
			or pycountry.countries.get(alpha_3=country_code.upper())
		)

	def get_country_flag_emoji(self, country_code: str) -> Optional[str]:
		"""Get flag emoji for a country code.

		Parameters
		----------
		country_code : str
			2-letter country code

		Returns
		-------
		Optional[str]
			Flag emoji string or None if invalid code
		"""
		if not self.is_valid_country_code(country_code.upper()):
			return None
		return "".join(chr(ord(c) + 127397) for c in country_code.upper())

	def get_country_details_by_name(self, name: str) -> Optional[ReturnGetCountryDetails]:
		"""Get country details by fuzzy name matching.

		Parameters
		----------
		name : str
			Country name or partial name

		Returns
		-------
		Optional[ReturnGetCountryDetails]
			Dictionary with country details or None if not found
		"""
		try:
			country = pycountry.countries.search_fuzzy(name)
			if country:
				return self.get_country_details(country[0].alpha_2)
		except LookupError:
			return None
		return None

	def get_continent_by_country_code(self, country_code: str) -> Optional[str]:
		"""Get continent name for a country code.

		Parameters
		----------
		country_code : str
			2-letter country code

		Returns
		-------
		Optional[str]
			Continent name or None if not found
		"""
		if not self.is_valid_country_code(country_code):
			return None
		try:
			continent_code = pc.country_alpha2_to_continent_code(country_code.upper())
			return pc.convert_continent_code_to_continent_name(continent_code)
		except (KeyError, ValueError):
			return None

	def get_continent_code_by_country_code(
		self, country_code: str
	) -> Optional[Literal["AF", "AS", "EU", "NA", "SA", "OC", "AN"]]:
		"""Get continent code for a country code.

		Parameters
		----------
		country_code : str
			2-letter country code

		Returns
		-------
		Optional[Literal['AF', 'AS', 'EU', 'NA', 'SA', 'OC', 'AN']]
			Continent code or None if not found
		"""
		if not self.is_valid_country_code(country_code):
			return None
		try:
			return pc.country_alpha2_to_continent_code(country_code.upper()).upper()
		except (KeyError, ValueError):
			return None
