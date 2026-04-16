"""Terminal color formatting utilities for Microsoft CMD.

This module provides a class for applying ANSI color codes to text in Windows CMD terminal.
The colors can be used to highlight different types of messages (warnings, errors, etc.).
"""

import platform


if platform.system() != "Windows":
	raise OSError("This module requires a Windows operating system to function properly.")

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class BColors(metaclass=TypeChecker):
	"""Class containing ANSI color codes for terminal text formatting.

	References
	----------
	.. [1] https://stackoverflow.com/questions/4406389/if-else-in-a-list-comprehension
	"""

	HEADER: str = "\033[95m"
	OKBLUE: str = "\033[94m"
	OKCYAN: str = "\033[96m"
	OKGREEN: str = "\033[92m"
	WARNING: str = "\033[93m"
	FAIL: str = "\033[91m"
	ENDC: str = "\033[0m"
	BOLD: str = "\033[1m"
	UNDERLINE: str = "\033[4m"
