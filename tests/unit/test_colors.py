"""Unit tests for ColorIdentifier class.

Tests the color identification functionality with various input scenarios including:
- Initialization with valid/invalid hex colors
- Color family identification methods
- Edge cases and error conditions
"""

import pytest

from stpstone.utils.parsers.colors import ColorIdentifier


class TestColorIdentifierInitialization:
	"""Test cases for ColorIdentifier initialization and validation."""

	def test_init_with_valid_6char_hex(self) -> None:
		"""Test initialization with valid 6-character hex code.

		Verifies
		--------
		- Initialization succeeds with valid 6-char hex
		- RGB components are correctly parsed
		"""
		color = ColorIdentifier("#A1B2C3")
		assert color.hex == "a1b2c3"
		assert color.r == 0xA1
		assert color.g == 0xB2
		assert color.b == 0xC3

	def test_init_with_valid_3char_hex(self) -> None:
		"""Test initialization with valid 3-character hex code.

		Verifies
		--------
		- Initialization succeeds with valid 3-char hex
		- RGB components are correctly expanded
		"""
		color = ColorIdentifier("#ABC")
		assert color.hex == "aabbcc"
		assert color.r == 0xAA
		assert color.g == 0xBB
		assert color.b == 0xCC

	def test_init_without_hash_prefix(self) -> None:
		"""Test initialization with hex code without '#' prefix.

		Verifies
		--------
		- Initialization succeeds without '#' prefix
		- Hex string is properly stored
		"""
		color = ColorIdentifier("A1B2C3")
		assert color.hex == "a1b2c3"

	def test_init_with_invalid_length(self) -> None:
		"""Test initialization with invalid hex code length.

		Verifies
		--------
		- Raises ValueError for invalid lengths
		- Error message indicates length requirement
		"""
		with pytest.raises(ValueError, match="Hex color must be 3 or 6 characters long"):
			ColorIdentifier("#AB")  # Too short
		with pytest.raises(ValueError, match="Hex color must be 3 or 6 characters long"):
			ColorIdentifier("#A1B2C3D4")  # Too long

	def test_init_with_invalid_hex_chars(self) -> None:
		"""Test initialization with non-hex characters.

		Verifies
		--------
		- Raises ValueError when parsing non-hex characters
		"""
		with pytest.raises(ValueError, match="invalid literal for int"):
			ColorIdentifier("#G12H34")  # contains G and H


class TestColorValidationMethods:
	"""Test cases for color validation helper methods."""

	@pytest.mark.parametrize("threshold", [-1.0, 0.0])
	def test_validate_threshold_invalid(self, threshold: float) -> None:
		"""Test threshold validation with invalid values.

		Verifies
		--------
		- Raises ValueError for non-positive thresholds

		Parameters
		----------
		threshold : float
			Threshold value to validate

		Returns
		-------
		None
		"""
		color = ColorIdentifier("#FFFFFF")
		with pytest.raises(ValueError, match="Threshold must be positive"):
			color._validate_threshold(threshold)

	@pytest.mark.parametrize("intensity", [-1, 256])
	def test_validate_intensity_invalid(self, intensity: int) -> None:
		"""Test intensity validation with out-of-range values.

		Verifies
		--------
		- Raises ValueError for intensities outside 0-255

		Parameters
		----------
		intensity : int
			Intensity value to validate

		Returns
		-------
		None
		"""
		color = ColorIdentifier("#FFFFFF")
		with pytest.raises(ValueError, match="Intensity must be between 0-255"):
			color._validate_intensity(intensity)

	def test_validate_tolerance_invalid(self) -> None:
		"""Test tolerance validation with negative value.

		Verifies
		--------
		- Raises ValueError for negative tolerance

		Parameters
		----------
		tolerance : int
			Tolerance value to validate

		Returns
		-------
		None
		"""
		color = ColorIdentifier("#FFFFFF")
		with pytest.raises(ValueError, match="Tolerance must be non-negative"):
			color._validate_tolerance(-1)


class TestColorIdentificationMethods:
	"""Test cases for color family identification methods."""

	@pytest.fixture
	def green_color(self) -> ColorIdentifier:
		"""Fixture providing a green-dominant color.

		Returns
		-------
		ColorIdentifier
			Instance of ColorIdentifier with a green color.
		"""
		return ColorIdentifier("#00FF00")

	@pytest.fixture
	def red_color(self) -> ColorIdentifier:
		"""Fixture providing a red-dominant color.

		Returns
		-------
		ColorIdentifier
			Instance of ColorIdentifier with a green color.
		"""
		return ColorIdentifier("#FF0000")

	@pytest.fixture
	def blue_color(self) -> ColorIdentifier:
		"""Fixture providing a blue-dominant color.

		Returns
		-------
		ColorIdentifier
			Instance of ColorIdentifier with a green color.
		"""
		return ColorIdentifier("#0000FF")

	@pytest.fixture
	def white_color(self) -> ColorIdentifier:
		"""Fixture providing a white color.

		Returns
		-------
		ColorIdentifier
			Instance of ColorIdentifier with a green color.
		"""
		return ColorIdentifier("#FFFFFF")

	@pytest.fixture
	def black_color(self) -> ColorIdentifier:
		"""Fixture providing a black color.

		Returns
		-------
		ColorIdentifier
			Instance of ColorIdentifier with a green color.
		"""
		return ColorIdentifier("#000000")

	@pytest.fixture
	def gray_color(self) -> ColorIdentifier:
		"""Fixture providing a gray color.

		Returns
		-------
		ColorIdentifier
			Instance of ColorIdentifier with a green color.
		"""
		return ColorIdentifier("#808080")

	@pytest.fixture
	def yellow_color(self) -> ColorIdentifier:
		"""Fixture providing a yellow color.

		Returns
		-------
		ColorIdentifier
			Instance of ColorIdentifier with a green color.
		"""
		return ColorIdentifier("#FFFF00")

	@pytest.fixture
	def purple_color(self) -> ColorIdentifier:
		"""Fixture providing a purple color.

		Returns
		-------
		ColorIdentifier
			Instance of ColorIdentifier with a green color.
		"""
		return ColorIdentifier("#800080")

	def test_is_green(self, green_color: ColorIdentifier) -> None:
		"""Test green color identification.

		Verifies
		--------
		- Returns True for green-dominant colors
		- Returns False for non-green colors

		Parameters
		----------
		green_color : ColorIdentifier
			Instance of ColorIdentifier with a green color.

		Returns
		-------
		None
		"""
		assert green_color.is_green()
		assert not ColorIdentifier("#FF0000").is_green()

	def test_is_red(self, red_color: ColorIdentifier) -> None:
		"""Test red color identification.

		Verifies
		--------
		- Returns True for red-dominant colors
		- Returns False for non-red colors

		Parameters
		----------
		red_color : ColorIdentifier
			Instance of ColorIdentifier with a red color.

		Returns
		-------
		None
		"""
		assert red_color.is_red()
		assert not ColorIdentifier("#00FF00").is_red()

	def test_is_blue(self, blue_color: ColorIdentifier) -> None:
		"""Test blue color identification.

		Verifies
		--------
		- Returns True for blue-dominant colors
		- Returns False for non-blue colors

		Parameters
		----------
		blue_color : ColorIdentifier
			Instance of ColorIdentifier with a blue color.

		Returns
		-------
		None
		"""
		assert blue_color.is_blue()
		assert not ColorIdentifier("#00FF00").is_blue()

	def test_is_white(self, white_color: ColorIdentifier) -> None:
		"""Test white color identification.

		Verifies
		--------
		- Returns True for white colors
		- Returns False for non-white colors
		- Works with custom min_intensity

		Parameters
		----------
		white_color : ColorIdentifier
			Instance of ColorIdentifier with a white color.

		Returns
		-------
		None
		"""
		assert white_color.is_white()
		assert ColorIdentifier("#F0F0F0").is_white()
		assert ColorIdentifier("#F0F0F0").is_white(min_intensity=0xF0)

	def test_is_black(self, black_color: ColorIdentifier) -> None:
		"""Test black color identification.

		Verifies
		--------
		- Returns True for black colors
		- Returns False for non-black colors
		- Works with custom max_intensity

		Parameters
		----------
		black_color : ColorIdentifier
			Instance of ColorIdentifier with a black color.

		Returns
		-------
		None
		"""
		assert black_color.is_black()
		assert ColorIdentifier("#101010").is_black()
		assert ColorIdentifier("#101010").is_black(max_intensity=0x10)

	def test_is_gray(self, gray_color: ColorIdentifier) -> None:
		"""Test gray color identification.

		Verifies
		--------
		- Returns True for gray colors
		- Returns False for non-gray colors
		- Works with custom tolerance

		Parameters
		----------
		gray_color : ColorIdentifier
			Instance of ColorIdentifier with a gray color.

		Returns
		-------
		None
		"""
		assert gray_color.is_gray()
		assert ColorIdentifier("#807F80").is_gray(tolerance=1)
		assert ColorIdentifier("#807F80").is_gray(tolerance=2)

	def test_is_yellow(self, yellow_color: ColorIdentifier) -> None:
		"""Test yellow color identification.

		Verifies
		--------
		- Returns True for yellow colors
		- Returns False for non-yellow colors

		Parameters
		----------
		yellow_color : ColorIdentifier
			Instance of ColorIdentifier with a yellow color.

		Returns
		-------
		None
		"""
		assert yellow_color.is_yellow()
		assert not ColorIdentifier("#FF00FF").is_yellow()

	def test_is_purple(self, purple_color: ColorIdentifier) -> None:
		"""Test purple color identification.

		Verifies
		--------
		- Returns True for purple colors
		- Returns False for non-purple colors

		Parameters
		----------
		purple_color : ColorIdentifier
			Instance of ColorIdentifier with a purple color.

		Returns
		-------
		None
		"""
		assert purple_color.is_purple()
		assert not ColorIdentifier("#00FF00").is_purple()


class TestEdgeCases:
	"""Test edge cases and boundary conditions."""

	@pytest.mark.parametrize(
		"hex_code,expected",
		[
			("#010000", True),  # barely red
			("#000100", False),  # barely green
			("#000001", False),  # barely blue
			("#FEFFFF", False),  # barely white
			("#010101", False),  # barely not black
		],
	)
	def test_boundary_conditions(self, hex_code: str, expected: bool) -> None:
		"""Test color identification at boundary values.

		Verifies
		--------
		- Methods correctly identify colors at detection boundaries

		Parameters
		----------
		hex_code : str
			Hex color code to test
		expected : bool
			Expected result for color identification methods

		Returns
		-------
		None
		"""
		color = ColorIdentifier(hex_code)
		assert color.is_red(threshold=255.0) == expected

	def test_black_edge_case(self) -> None:
		"""Test black identification with edge intensity.

		Verifies
		--------
		- Correctly identifies black at max_intensity boundary

		Parameters
		----------
		None

		Returns
		-------
		None
		"""
		assert ColorIdentifier("#191919").is_black(max_intensity=25)
		assert not ColorIdentifier("#1A1A1A").is_black(max_intensity=25)
