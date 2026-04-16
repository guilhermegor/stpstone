"""Unit tests for worldwide timezone and geography utilities.

Tests the functionality of WWTimezones and WWGeography classes including:
- Country code validation
- Timezone lookups
- Current time retrieval
- Country details retrieval
- Continent identification
"""

from typing import Any
from unittest.mock import MagicMock, patch

import pycountry_convert as pc
import pytest

from stpstone.utils.geography.geo_ww import WWGeography, WWTimezones


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def ww_timezones() -> Any:  # noqa ANN401: typing.Any is not allowed
	"""Fixture providing WWTimezones instance.

	Returns
	-------
	Any
		Instance of WWTimezones class
	"""
	return WWTimezones()


@pytest.fixture
def ww_geography() -> Any:  # noqa ANN401: typing.Any is not allowed
	"""Fixture providing WWGeography instance.

	Returns
	-------
	Any
		Instance of WWGeography class
	"""
	return WWGeography()


@pytest.fixture
def mock_country_info() -> Any:  # noqa ANN401: typing.Any is not allowed
	"""Fixture providing mocked CountryInfo instance.

	Returns
	-------
	Any
		Mocked CountryInfo instance
	"""
	with patch("stpstone.utils.geography.geo_ww.CountryInfo") as mock:
		yield mock


@pytest.fixture
def mock_pycountry() -> Any:  # noqa ANN401: typing.Any is not allowed
	"""Fixture providing mocked pycountry module.

	Returns
	-------
	Any
		Mocked pycountry module
	"""
	with patch("stpstone.utils.geography.geo_ww.pycountry.countries") as mock:
		yield mock


@pytest.fixture
def mock_datetime_now() -> Any:  # noqa ANN401: typing.Any is not allowed
	"""Fixture providing mocked datetime.now().

	Returns
	-------
	Any
		Mocked datetime.now()
	"""
	with patch("stpstone.utils.geography.geo_ww.datetime") as mock:
		yield mock


# --------------------------
# Test Classes
# --------------------------
class TestWWTimezones:
	"""Test cases for WWTimezones class."""

	def test_validate_country_code_empty(
		self,
		ww_timezones: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test raises ValueError when country code is empty.

		Verifies
		--------
		That providing an empty country code raises ValueError
		with appropriate error message.

		Parameters
		----------
		ww_timezones : Any
			Instance of the class being tested

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Country code cannot be empty"):
			ww_timezones._validate_country_code("")

	def test_validate_country_code_non_string(
		self,
		ww_timezones: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test raises ValueError when country code is not a string.

		Verifies
		--------
		That providing a non-string country code raises ValueError
		with appropriate error message.

		Parameters
		----------
		ww_timezones : Any
			Instance of the class being tested

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be of type"):
			ww_timezones._validate_country_code(123)  # type: ignore

	def test_validate_country_code_invalid_length(
		self,
		ww_timezones: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test raises ValueError when country code length is invalid.

		Verifies
		--------
		That providing a country code with invalid length raises ValueError
		with appropriate error message.

		Parameters
		----------
		ww_timezones : Any
			Instance of the class being tested

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Country code must be 2 or 3 characters"):
			ww_timezones._validate_country_code("A")

	def test_get_timezones_by_country_code_valid(
		self,
		ww_timezones: Any,  # noqa ANN401: typing.Any is not allowed
		mock_country_info: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test returns timezones for valid country code.

		Verifies
		--------
		That providing a valid country code returns expected timezones.

		Parameters
		----------
		ww_timezones : Any
			Instance of the class being tested
		mock_country_info : Any
			Mocked CountryInfo instance

		Returns
		-------
		None
		"""
		mock_instance = mock_country_info.return_value
		mock_instance.timezones.return_value = ["Timezone1", "Timezone2"]
		result = ww_timezones.get_timezones_by_country_code("US")
		assert result == ["Timezone1", "Timezone2"]
		mock_country_info.assert_called_once_with("US")

	def test_get_timezones_by_country_code_invalid(
		self,
		ww_timezones: Any,  # noqa ANN401: typing.Any is not allowed
		mock_country_info: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test returns None for invalid country code.

		Verifies
		--------
		That providing an invalid country code returns None.

		Parameters
		----------
		ww_timezones : Any
			Instance of the class being tested
		mock_country_info : Any
			Mocked CountryInfo instance

		Returns
		-------
		None
		"""
		mock_country_info.side_effect = KeyError("Invalid country")
		assert ww_timezones.get_timezones_by_country_code("XX") is None

	def test_get_countries_in_timezone(
		self,
		ww_timezones: Any,  # noqa ANN401: typing.Any is not allowed
		mock_pycountry: Any,  # noqa ANN401: typing.Any is not allowed
		mock_country_info: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test returns countries using specified timezone.

		Verifies
		--------
		That providing a timezone name returns expected country codes.

		Parameters
		----------
		ww_timezones : Any
			Instance of the class being tested
		mock_pycountry : Any
			Mocked pycountry module
		mock_country_info : Any
			Mocked CountryInfo instance

		Returns
		-------
		None
		"""
		# Create a mock country object
		mock_country = MagicMock()
		mock_country.alpha_2 = "US"

		# Mock the countries iteration
		mock_pycountry.__iter__.return_value = [mock_country]

		# Mock CountryInfo to return the desired timezone
		mock_country_info.return_value.timezones.return_value = ["America/New_York"]

		result = ww_timezones.get_countries_in_timezone("America/New_York")
		assert result == ["US"]

	def test_get_current_time_in_country_valid(
		self,
		ww_timezones: Any,  # noqa ANN401: typing.Any is not allowed
		mock_country_info: Any,  # noqa ANN401: typing.Any is not allowed
		mock_datetime_now: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test returns current time for valid country code.

		Verifies
		--------
		That providing a valid country code returns current time in ISO format.

		Parameters
		----------
		ww_timezones : Any
			Instance of the class being tested
		mock_country_info : Any
			Mocked CountryInfo instance
		mock_datetime_now : Any
			Mocked datetime.now() function

		Returns
		-------
		None
		"""
		# Mock CountryInfo to return a valid timezone
		mock_instance = mock_country_info.return_value
		mock_instance.timezones.return_value = ["America/New_York"]

		# Mock datetime.now() to return a mock datetime object
		mock_dt = MagicMock()
		mock_dt.isoformat.return_value = "2023-01-01 12:00:00"
		mock_datetime_now.now.return_value = mock_dt

		# Mock ZoneInfo to avoid actual timezone lookup
		with patch("stpstone.utils.geography.geo_ww.ZoneInfo") as mock_zoneinfo:
			mock_zoneinfo.return_value = "mocked_timezone"
			result = ww_timezones.get_current_time_in_country("US")
			assert result == "2023-01-01 12:00:00"

	def test_get_current_time_in_country_invalid(
		self,
		ww_timezones: Any,  # noqa ANN401: typing.Any is not allowed
		mock_country_info: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test returns None for invalid country code.

		Verifies
		--------
		That providing an invalid country code returns None.

		Parameters
		----------
		ww_timezones : Any
			Instance of the class being tested
		mock_country_info : Any
			Mocked CountryInfo instance

		Returns
		-------
		None
		"""
		mock_country_info.side_effect = KeyError("Invalid country")
		assert ww_timezones.get_current_time_in_country("XX") is None

	def test_get_all_timezones_grouped(
		self,
		ww_timezones: Any,  # noqa ANN401: typing.Any is not allowed
		mock_pycountry: Any,  # noqa ANN401: typing.Any is not allowed
		mock_country_info: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test returns all timezones grouped by country code.

		Verifies
		--------
		That method returns expected dictionary mapping country codes to timezones.

		Parameters
		----------
		ww_timezones : Any
			Instance of the class being tested
		mock_pycountry : Any
			Mocked pycountry module
		mock_country_info : Any
			Mocked CountryInfo instance

		Returns
		-------
		None
		"""
		# Create a mock country object
		mock_country = MagicMock()
		mock_country.alpha_2 = "US"

		# Mock the countries iteration
		mock_pycountry.__iter__.return_value = [mock_country]

		# Mock CountryInfo to return the desired timezones
		mock_country_info.return_value.timezones.return_value = ["America/New_York"]

		result = ww_timezones.get_all_timezones_grouped()
		assert result == {"US": ["America/New_York"]}


class TestWWGeography:
	"""Test cases for WWGeography class."""

	def test_get_country_details_valid_alpha2(
		self,
		ww_geography: Any,  # noqa ANN401: typing.Any is not allowed
		mock_pycountry: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test returns country details for valid alpha-2 code.

		Verifies
		--------
		That providing a valid alpha-2 country code returns expected details.

		Parameters
		----------
		ww_geography : Any
			Instance of the class being tested
		mock_pycountry : Any
			Mocked pycountry module

		Returns
		-------
		None
		"""
		mock_country = MagicMock()
		mock_country.name = "United States"
		mock_country.alpha_2 = "US"
		mock_country.alpha_3 = "USA"
		mock_country.official_name = "United States of America"

		mock_pycountry.get.return_value = mock_country

		result = ww_geography.get_country_details("US")
		assert result == {
			"name": "United States",
			"alpha_2": "US",
			"alpha_3": "USA",
			"official_name": "United States of America",
		}

	def test_get_country_details_valid_alpha3(
		self,
		ww_geography: Any,  # noqa ANN401: typing.Any is not allowed
		mock_pycountry: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test returns country details for valid alpha-3 code.

		Verifies
		--------
		That providing a valid alpha-3 country code returns expected details.

		Parameters
		----------
		ww_geography : Any
			Instance of the class being tested
		mock_pycountry : Any
			Mocked pycountry module

		Returns
		-------
		None
		"""
		mock_country = MagicMock()
		mock_country.name = "United States"
		mock_country.alpha_2 = "US"
		mock_country.alpha_3 = "USA"
		# Mock the case where official_name doesn't exist
		del mock_country.official_name

		# First call returns None (alpha_2 lookup), second returns the country (alpha_3 lookup)
		mock_pycountry.get.side_effect = [None, mock_country]

		result = ww_geography.get_country_details("USA")
		assert result == {
			"name": "United States",
			"alpha_2": "US",
			"alpha_3": "USA",
			"official_name": "",
		}

	def test_get_country_details_invalid(
		self,
		ww_geography: Any,  # noqa ANN401: typing.Any is not allowed
		mock_pycountry: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test returns None for invalid country code.

		Verifies
		--------
		That providing an invalid country code returns None.

		Parameters
		----------
		ww_geography : Any
			Instance of the class being tested
		mock_pycountry : Any
			Mocked pycountry module

		Returns
		-------
		None
		"""
		mock_pycountry.get.return_value = None
		assert ww_geography.get_country_details("XX") is None

	def test_is_valid_country_code_valid(
		self,
		ww_geography: Any,  # noqa ANN401: typing.Any is not allowed
		mock_pycountry: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test returns True for valid country code.

		Verifies
		--------
		That providing a valid country code returns True.

		Parameters
		----------
		ww_geography : Any
			Instance of the class being tested
		mock_pycountry : Any
			Mocked pycountry module

		Returns
		-------
		None
		"""
		mock_country = MagicMock()
		mock_pycountry.get.return_value = mock_country
		assert ww_geography.is_valid_country_code("US") is True

	def test_is_valid_country_code_invalid(
		self,
		ww_geography: Any,  # noqa ANN401: typing.Any is not allowed
		mock_pycountry: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test returns False for invalid country code.

		Verifies
		--------
		That providing an invalid country code returns False.

		Parameters
		----------
		ww_geography : Any
			Instance of the class being tested
		mock_pycountry : Any
			Mocked pycountry module

		Returns
		-------
		None
		"""
		mock_pycountry.get.return_value = None
		assert ww_geography.is_valid_country_code("XX") is False

	def test_get_country_flag_emoji_valid(
		self,
		ww_geography: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test returns flag emoji for valid country code.

		Verifies
		--------
		That providing a valid 2-letter country code returns flag emoji.

		Parameters
		----------
		ww_geography : Any
			Instance of the class being tested

		Returns
		-------
		None
		"""
		with patch.object(ww_geography, "is_valid_country_code", return_value=True):
			result = ww_geography.get_country_flag_emoji("US")
			assert result == "🇺🇸"

	def test_get_country_flag_emoji_invalid(
		self,
		ww_geography: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test returns None for invalid country code.

		Verifies
		--------
		That providing an invalid country code returns None.

		Parameters
		----------
		ww_geography : Any
			Instance of the class being tested

		Returns
		-------
		None
		"""
		with patch.object(ww_geography, "is_valid_country_code", return_value=False):
			assert ww_geography.get_country_flag_emoji("XX") is None

	def test_get_country_details_by_name_valid(
		self,
		ww_geography: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test returns country details for valid name.

		Verifies
		--------
		That providing a valid country name returns expected details.

		Parameters
		----------
		ww_geography : Any
			Instance of the class being tested

		Returns
		-------
		None
		"""
		with patch(
			"stpstone.utils.geography.geo_ww.pycountry.countries.search_fuzzy"
		) as mock_search:
			mock_country = MagicMock()
			mock_country.alpha_2 = "US"
			mock_search.return_value = [mock_country]

			with patch.object(
				ww_geography, "get_country_details", return_value={"name": "United States"}
			):
				result = ww_geography.get_country_details_by_name("United States")
				assert result == {"name": "United States"}

	def test_get_country_details_by_name_invalid(
		self,
		ww_geography: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test returns None for invalid name.

		Verifies
		--------
		That providing an invalid country name returns None.

		Parameters
		----------
		ww_geography : Any
			Instance of the class being tested

		Returns
		-------
		None
		"""
		with patch(
			"stpstone.utils.geography.geo_ww.pycountry.countries.search_fuzzy",
			side_effect=LookupError,
		):
			assert ww_geography.get_country_details_by_name("Nonexistent") is None

	def test_get_continent_by_country_code_valid(
		self,
		ww_geography: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test returns continent name for valid country code.

		Verifies
		--------
		That providing a valid country code returns expected continent name.

		Parameters
		----------
		ww_geography : Any
			Instance of the class being tested

		Returns
		-------
		None
		"""
		with (
			patch.object(ww_geography, "is_valid_country_code", return_value=True),
			patch.object(pc, "country_alpha2_to_continent_code", return_value="NA"),
			patch.object(
				pc, "convert_continent_code_to_continent_name", return_value="North America"
			),
		):
			result = ww_geography.get_continent_by_country_code("US")
			assert result == "North America"

	def test_get_continent_by_country_code_invalid(
		self,
		ww_geography: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test returns None for invalid country code.

		Verifies
		--------
		That providing an invalid country code returns None.

		Parameters
		----------
		ww_geography : Any
			Instance of the class being tested

		Returns
		-------
		None
		"""
		with patch.object(ww_geography, "is_valid_country_code", return_value=False):
			assert ww_geography.get_continent_by_country_code("XX") is None

	def test_get_continent_code_by_country_code_valid(
		self,
		ww_geography: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test returns continent code for valid country code.

		Verifies
		--------
		That providing a valid country code returns expected continent code.

		Parameters
		----------
		ww_geography : Any
			Instance of the class being tested

		Returns
		-------
		None
		"""
		with (
			patch.object(ww_geography, "is_valid_country_code", return_value=True),
			patch.object(pc, "country_alpha2_to_continent_code", return_value="na"),
		):
			result = ww_geography.get_continent_code_by_country_code("US")
			assert result == "NA"

	def test_get_continent_code_by_country_code_invalid(
		self,
		ww_geography: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test returns None for invalid country code.

		Verifies
		--------
		That providing an invalid country code returns None.

		Parameters
		----------
		ww_geography : Any
			Instance of the class being tested

		Returns
		-------
		None
		"""
		with patch.object(ww_geography, "is_valid_country_code", return_value=False):
			assert ww_geography.get_continent_code_by_country_code("XX") is None
