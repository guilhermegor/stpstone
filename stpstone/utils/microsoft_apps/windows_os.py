"""Windows operating system window management utilities.

This module provides a class for interacting with and manipulating windows
in the Windows operating system using win32gui and ctypes.
"""

import platform


if platform.system() != "Windows":
    raise OSError("This module requires a Windows operating system to function properly.")

from ctypes import (
    POINTER,
    WINFUNCTYPE,
    c_bool,
    c_int,
    create_unicode_buffer,
    windll,
)
from typing import Union

import win32con
import win32gui

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


EnumWindows = windll.user32.EnumWindows
EnumWindowsProc = WINFUNCTYPE(c_bool, c_int, POINTER(c_int))
GetWindowText = windll.user32.GetWindowTextW
GetWindowTextLength = windll.user32.GetWindowTextLengthW
IsWindowVisible = windll.user32.IsWindowVisible
GetClassName = windll.user32.GetClassNameW
BringWindowToTop = windll.user32.BringWindowToTop
GetForegroundWindow = windll.user32.GetForegroundWindow


class DealingWindows(metaclass=TypeChecker):
    """Class for manipulating windows in Windows OS.

    Provides methods to find, refresh, and close windows based on their titles.

    References
    ----------
    .. [1] https://makble.com/how-to-find-window-with-wildcard-in-python-and-win32gui
    """

    def __init__(self) -> None:
        """Initialize DealingWindows with empty titles list."""
        self.titles: list[tuple[int, str, str, bool]] = []

    def _validate_hwnd(self, hwnd: int) -> None:
        """Validate window handle.

        Parameters
        ----------
        hwnd : int
            Window handle to validate

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If hwnd is not a positive integer
        """
        if not isinstance(hwnd, int) or hwnd <= 0:
            raise ValueError("Window handle must be a positive integer")

    def foreach_window(self, hwnd: int) -> bool:
        """Enumerate windows.

        Parameters
        ----------
        hwnd : int
            Window handle
        
        Returns
        -------
        bool
            True to continue enumeration, False to stop

        Notes
        -----
        Populates self.titles with visible windows information.
        """
        if IsWindowVisible(hwnd):
            length = GetWindowTextLength(hwnd)
            classname = create_unicode_buffer(100 + 1)
            GetClassName(hwnd, classname, 100 + 1)
            buff = create_unicode_buffer(length + 1)
            GetWindowText(hwnd, buff, length + 1)
            self.titles.append(
                (hwnd, buff.value, classname.value, bool(windll.user32.IsIconic(hwnd)))
            )
        return True

    def refresh_wins(self) -> None:
        """Refresh the list of open window titles.

        Returns
        -------
        None

        Notes
        -----
        Clears and repopulates self.titles with current window information.
        """
        self.titles = []
        EnumWindows(EnumWindowsProc(self.foreach_window), 0)

    def find_window(self, title_substring: str) -> Union[int, bool]:
        """Find window containing title substring.

        Parameters
        ----------
        title_substring : str
            Substring to search in window titles

        Returns
        -------
        Union[int, bool]
            Window handle if found, False otherwise

        Raises
        ------
        ValueError
            If title_substring is not a string

        Notes
        -----
        Performs case-insensitive search.
        """
        if not isinstance(title_substring, str):
            raise ValueError("Title substring must be a string")

        self.refresh_wins()
        for item in self.titles:
            if title_substring.lower() in item[1].lower():
                return item[0]
        return False

    def close_window(self, window_handler: int) -> None:
        """Close specified window.

        Parameters
        ----------
        window_handler : int
            Handle of window to close

        Returns
        -------
        None
        """
        self._validate_hwnd(window_handler)
        win32gui.PostMessage(window_handler, win32con.WM_CLOSE, 0, 0)