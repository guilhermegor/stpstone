### DEALING WINDOWS OPERATING SYSTEM ###

import win32con
from ctypes import *
import win32gui


EnumWindows = windll.user32.EnumWindows
EnumWindowsProc = WINFUNCTYPE(c_bool, c_int, POINTER(c_int))
GetWindowText = windll.user32.GetWindowTextW
GetWindowTextLength = windll.user32.GetWindowTextLengthW
IsWindowVisible = windll.user32.IsWindowVisible
GetClassName = windll.user32.GetClassNameW
BringWindowToTop = windll.user32.BringWindowToTop
GetForegroundWindow = windll.user32.GetForegroundWindow


class DealingWindows:
    """
    REFERENCE: makble.com/how-to-find-window-with-wildcard-in-python-and-win32gui
    DOCSTRING: MANIPULATE WINDOWS IN OS
    """

    def __init__(self):
        self.titles = []

    def foreach_window(self, hwnd, lParam):
        """
        DOCSTRING: SEARCH ALL WINDOWS OPENED
        INPUT: WINDOWS HANDLER
        OUTPUT: -
        """
        if IsWindowVisible(hwnd):
            length = GetWindowTextLength(hwnd)
            classname = create_unicode_buffer(100 + 1)
            GetClassName(hwnd, classname, 100 + 1)
            buff = create_unicode_buffer(length + 1)
            GetWindowText(hwnd, buff, length + 1)
            self.titles.append((hwnd, buff.value.encode, classname.value,
                                windll.user32.IsIconic(hwnd)))

    def refresh_wins(self):
        """
        DOCSTRING: REFRESH THE LIST OF WINDOWS TITLES OPENED
        INPUT: -
        OUTPUT: -
        """
        self.titles = []
        EnumWindows(EnumWindowsProc(self.foreach_window), 0)

    def find_window(self, title_substring):
        """
        DOCSTRING: SEARCH FOR WINDOW THAT CONTAINS THE TITLE SUBSTRING
        INPUT: TITLE SUBSTRING
        OUTPUT: WINDOW HANDLER
        """
        self.refresh_wins()
        for item in self.titles:
            if title_substring.lower() in item[2].lower():
                return item[0]
        return False

    def close_window(self, window_handler):
        """
        DOCSTRING: CLOSE THE WINDOW INDICATED BY WINDOW HANDLER
        INPUT: WINDOW HANDLER
        OUTPUT: -
        """
        win32gui.PostMessage(window_handler, win32con.WM_CLOSE, 0, 0)
