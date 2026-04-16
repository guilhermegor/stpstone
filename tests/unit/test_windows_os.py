"""Unit tests for Windows window management utilities.

Tests the functionality of the DealingWindows class including:
- Window enumeration and searching
- Window closing operations
- Input validation and error handling
"""

import platform

import pytest


if platform.system() != "Windows":
	pytest.skip("Windows OS tests require Windows", allow_module_level=True)

from ctypes import create_unicode_buffer
import platform
from typing import Any
from unittest.mock import patch

import pytest
import win32con

from stpstone.utils.microsoft_apps.windows_os import DealingWindows


# Skip all tests if not on Windows
if platform.system() != "Windows":
	pytest.skip("Windows-only tests", allow_module_level=True)


class TestDealingWindows:
	"""Test cases for DealingWindows class."""

	@pytest.fixture
	def mock_windows(self) -> Any:  # noqa ANN401: typing.Any not allowed
		"""Fixture providing mocked window data.

		Returns
		-------
		Any
			List of mock window tuples (hwnd, title, classname, iconic)
		"""
		return [
			(123, "Test Window 1", "TestClass1", False),
			(456, "Another Window", "TestClass2", True),
			(789, "Final Window", "TestClass3", False),
		]

	@pytest.fixture
	def dealing_windows(self) -> Any:  # noqa ANN401: typing.Any not allowed
		"""Fixture providing DealingWindows instance.

		Returns
		-------
		Any
			Instance of DealingWindows class
		"""
		return DealingWindows()

	def test_init(
		self,
		dealing_windows: Any,  # noqa ANN401: typing.Any not allowed
	) -> None:
		"""Test class initialization.

		Verifies
		--------
		- The instance initializes correctly
		- The titles list starts empty

		Parameters
		----------
		dealing_windows : Any
			Instance of DealingWindows from fixture
		"""
		assert isinstance(dealing_windows.titles, list)
		assert len(dealing_windows.titles) == 0

	def test_validate_hwnd_valid(
		self,
		dealing_windows: Any,  # noqa ANN401: typing.Any not allowed
	) -> None:
		"""Test valid window handle validation.

		Verifies
		--------
		- Positive integer handles pass validation

		Parameters
		----------
		dealing_windows : Any
			Instance of DealingWindows from fixture
		"""
		dealing_windows._validate_hwnd(123)

	def test_validate_hwnd_invalid_type(
		self,
		dealing_windows: Any,  # noqa ANN401: typing.Any not allowed
	) -> None:
		"""Test invalid window handle type validation.

		Verifies
		--------
		- Non-integer handles raise ValueError

		Parameters
		----------
		dealing_windows : Any
			Instance of DealingWindows from fixture
		"""
		with pytest.raises(ValueError, match="Window handle must be a positive integer"):
			dealing_windows._validate_hwnd("not_an_int")

	def test_validate_hwnd_invalid_value(
		self,
		dealing_windows: Any,  # noqa ANN401: typing.Any not allowed
	) -> None:
		"""Test invalid window handle value validation.

		Verifies
		--------
		- Non-positive handles raise ValueError

		Parameters
		----------
		dealing_windows : Any
			Instance of DealingWindows from fixture
		"""
		with pytest.raises(ValueError, match="Window handle must be a positive integer"):
			dealing_windows._validate_hwnd(0)

	@patch("ctypes.windll.user32.IsWindowVisible", return_value=True)
	@patch("ctypes.windll.user32.GetWindowTextLengthW", return_value=10)
	@patch("ctypes.windll.user32.GetClassNameW")
	@patch("ctypes.windll.user32.GetWindowTextW")
	@patch("ctypes.windll.user32.IsIconic", return_value=False)
	def test_foreach_window(
		self,
		mock_iconic: Any,  # noqa ANN401: typing.Any not allowed
		mock_get_text: Any,  # noqa ANN401: typing.Any not allowed
		mock_get_class: Any,  # noqa ANN401: typing.Any not allowed
		mock_text_len: Any,  # noqa ANN401: typing.Any not allowed
		mock_visible: Any,  # noqa ANN401: typing.Any not allowed
		dealing_windows: Any,  # noqa ANN401: typing.Any not allowed
	) -> None:
		"""Test window enumeration callback.

		Verifies
		--------
		- Visible windows are added to titles list
		- Correct information is captured

		Parameters
		----------
		mock_iconic : Any
			Mock for IsIconic
		mock_get_text : Any
			Mock for GetWindowTextW
		mock_get_class : Any
			Mock for GetClassNameW
		mock_text_len : Any
			Mock for GetWindowTextLengthW
		mock_visible : Any
			Mock for IsWindowVisible
		dealing_windows : Any
			Instance of DealingWindows from fixture
		"""
		mock_get_text.return_value = 1
		mock_get_class.return_value = 1
		buff = create_unicode_buffer(100 + 1)
		buff.value = "Test Window"
		mock_get_text.return_value = buff
		class_buff = create_unicode_buffer(100 + 1)
		class_buff.value = "TestClass"
		mock_get_class.return_value = class_buff

		result = dealing_windows.foreach_window(123, 0)
		assert result is True
		assert len(dealing_windows.titles) == 1
		assert dealing_windows.titles[0][0] == 123
		assert dealing_windows.titles[0][3] is False

	@patch("ctypes.windll.user32.EnumWindows")
	def test_refresh_wins(
		self,
		mock_enum: Any,  # noqa ANN401: typing.Any not allowed
		dealing_windows: Any,  # noqa ANN401: typing.Any not allowed
		mock_windows: Any,  # noqa ANN401: typing.Any not allowed
	) -> None:
		"""Test window list refresh.

		Verifies
		--------
		- Titles list is cleared
		- EnumWindows is called
		- Callback is properly set

		Parameters
		----------
		mock_enum : Any
			Mock for EnumWindows
		dealing_windows : Any
			Instance of DealingWindows from fixture
		mock_windows : Any
			Mock window data from fixture
		"""
		dealing_windows.titles = mock_windows
		dealing_windows.refresh_wins()
		assert len(dealing_windows.titles) == 0
		mock_enum.assert_called_once()

	def test_find_window_success(
		self,
		dealing_windows: Any,  # noqa ANN401: typing.Any not allowed
		mock_windows: Any,  # noqa ANN401: typing.Any not allowed
	) -> None:
		"""Test successful window finding.

		Verifies
		--------
		- Correct handle is returned for matching title
		- Case-insensitive matching works

		Parameters
		----------
		dealing_windows : Any
			Instance of DealingWindows from fixture
		mock_windows : Any
			Mock window data from fixture
		"""
		dealing_windows.titles = mock_windows
		assert dealing_windows.find_window("another") == 456
		assert dealing_windows.find_window("FINAL") == 789

	def test_find_window_failure(
		self,
		dealing_windows: Any,  # noqa ANN401: typing.Any not allowed
		mock_windows: Any,  # noqa ANN401: typing.Any not allowed
	) -> None:
		"""Test failed window finding.

		Verifies
		--------
		- False is returned when no match found

		Parameters
		----------
		dealing_windows : Any
			Instance of DealingWindows from fixture
		mock_windows : Any
			Mock window data from fixture
		"""
		dealing_windows.titles = mock_windows
		assert dealing_windows.find_window("nonexistent") is False

	def test_find_window_invalid_input(
		self,
		dealing_windows: Any,  # noqa ANN401: typing.Any not allowed
	) -> None:
		"""Test invalid input for find_window.

		Verifies
		--------
		- Non-string input raises ValueError

		Parameters
		----------
		dealing_windows : Any
			Instance of DealingWindows from fixture
		"""
		with pytest.raises(ValueError, match="Title substring must be a string"):
			dealing_windows.find_window(123)

	@patch("win32gui.PostMessage")
	def test_close_window_success(
		self,
		mock_post: Any,  # noqa ANN401: typing.Any not allowed
		dealing_windows: Any,  # noqa ANN401: typing.Any not allowed
	) -> None:
		"""Test successful window closing.

		Verifies
		--------
		- PostMessage is called with correct parameters
		- Valid handle passes validation

		Parameters
		----------
		mock_post : Any
			Mock for PostMessage
		dealing_windows : Any
			Instance of DealingWindows from fixture
		"""
		dealing_windows.close_window(123)
		mock_post.assert_called_once_with(123, win32con.WM_CLOSE, 0, 0)

	@patch("win32gui.PostMessage")
	def test_close_window_invalid_handle(
		self,
		mock_post: Any,  # noqa ANN401: typing.Any not allowed
		dealing_windows: Any,  # noqa ANN401: typing.Any not allowed
	) -> None:
		"""Test window closing with invalid handle.

		Verifies
		--------
		- ValueError is raised for invalid handles

		Parameters
		----------
		mock_post : Any
			Mock for PostMessage
		dealing_windows : Any
			Instance of DealingWindows from fixture
		"""
		with pytest.raises(ValueError, match="Window handle must be a positive integer"):
			dealing_windows.close_window(-1)
		mock_post.assert_not_called()

	@patch("win32gui.PostMessage", side_effect=Exception("Test error"))
	def test_close_window_failure(
		self,
		mock_post: Any,  # noqa ANN401: typing.Any not allowed
		dealing_windows: Any,  # noqa ANN401: typing.Any not allowed
	) -> None:
		"""Test window closing failure.

		Verifies
		--------
		- Exceptions from PostMessage propagate

		Parameters
		----------
		mock_post : Any
			Mock for PostMessage
		dealing_windows : Any
			Instance of DealingWindows from fixture
		"""
		with pytest.raises(Exception, match="Test error"):
			dealing_windows.close_window(123)
		mock_post.assert_called_once_with(123, win32con.WM_CLOSE, 0, 0)
