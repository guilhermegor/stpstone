"""Color identification utilities for hex color codes.

This module provides methods to identify color families (e.g., green, red) from hex color codes
by analyzing their RGB component dominance relationships.
"""

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class ColorIdentifier(metaclass=TypeChecker):
    """Identify if a hex color belongs to a specific color family (e.g., green, red).

    Parameters
    ----------
    hex_color : str
        Hex color code (with or without leading '#')

    Attributes
    ----------
    hex : str
        Cleaned hex color string (lowercase, no leading '#')
    r : int
        Red component (0-255)
    g : int
        Green component (0-255)
    b : int
        Blue component (0-255)
    """

    def __init__(self, hex_color: str) -> None:
        """Initialize ColorIdentifier with hex color string.
        
        Parameters
        ----------
        hex_color : str
            Hex color code (e.g., "#FF5733" or "FF5733")
        
        Returns
        -------
        None
        """
        self.hex = hex_color.lstrip("#").lower()
        self._validate_hex_length()
        
        # handle 3-character hex codes by expanding them
        if len(self.hex) == 3:
            self.hex = "".join([c * 2 for c in self.hex])
            
        self.r = int(self.hex[0:2], 16)
        self.g = int(self.hex[2:4], 16)
        self.b = int(self.hex[4:6], 16)
        self._validate_rgb_values()

    def _validate_hex_length(self) -> None:
        """Validate hex color string has correct length (3 or 6 characters).
        
        Returns
        -------
        None
        
        Raises
        ------
        ValueError
            If hex color string is not 3 or 6 characters long
        """
        if len(self.hex) not in (3, 6):
            raise ValueError("Hex color must be 3 or 6 characters long")

    def _validate_rgb_values(self) -> None:
        """Validate RGB components are within valid range (0-255).
        
        Returns
        -------
        None
        
        Raises
        ------
        ValueError
            If RGB components are out of range
        """
        for name, value in [("Red", self.r), ("Green", self.g), ("Blue", self.b)]:
            if not 0 <= value <= 255:
                raise ValueError(f"{name} component must be between 0-255, got {value}")

    def is_green(self, threshold: float = 1.5) -> bool:
        """Return True if green is dominant over red/blue by a threshold multiplier.

        Parameters
        ----------
        threshold : float
            Minimum dominance multiplier (default: 1.5)

        Returns
        -------
        bool
            True if green dominates both red and blue by threshold
        """
        self._validate_threshold(threshold)
        return (self.g > threshold * self.r) and (self.g > threshold * self.b)

    def is_red(self, threshold: float = 1.5) -> bool:
        """Return True if red is dominant over green/blue.

        Parameters
        ----------
        threshold : float
            Minimum dominance multiplier (default: 1.5)

        Returns
        -------
        bool
            True if red dominates both green and blue by threshold
        """
        self._validate_threshold(threshold)
        return (self.r > threshold * self.g) and (self.r > threshold * self.b)

    def is_blue(self, threshold: float = 1.5) -> bool:
        """Return True if blue is dominant over red/green.

        Parameters
        ----------
        threshold : float
            Minimum dominance multiplier (default: 1.5)

        Returns
        -------
        bool
            True if blue dominates both red and green by threshold
        """
        self._validate_threshold(threshold)
        return (self.b > threshold * self.r) and (self.b > threshold * self.g)

    def is_white(self, min_intensity: int = 230) -> bool:
        """Return True if all RGB components are near maximum (e.g., #ffffff).

        Parameters
        ----------
        min_intensity : int
            Minimum intensity value for white (default: 230)

        Returns
        -------
        bool
            True if all components >= min_intensity
        """
        self._validate_intensity(min_intensity)
        return all(c >= min_intensity for c in [self.r, self.g, self.b])

    def is_black(self, max_intensity: int = 25) -> bool:
        """Return True if all RGB components are near minimum (e.g., #000000).

        Parameters
        ----------
        max_intensity : int
            Maximum intensity value for black (default: 25)

        Returns
        -------
        bool
            True if all components <= max_intensity
        """
        self._validate_intensity(max_intensity)
        return all(c <= max_intensity for c in [self.r, self.g, self.b])

    def is_gray(self, tolerance: int = 10) -> bool:
        """Return True if R ≈ G ≈ B within a tolerance range.

        Parameters
        ----------
        tolerance : int
            Maximum allowed difference between components (default: 10)

        Returns
        -------
        bool
            True if all components within tolerance of each other
        """
        self._validate_tolerance(tolerance)
        avg = (self.r + self.g + self.b) / 3
        return (
            abs(self.r - avg) <= tolerance and
            abs(self.g - avg) <= tolerance and
            abs(self.b - avg) <= tolerance
        )

    def is_yellow(self, threshold: float = 1.5) -> bool:
        """Return True if red+green dominate and blue is low.

        Parameters
        ----------
        threshold : float
            Minimum dominance multiplier (default: 1.5)

        Returns
        -------
        bool
            True if red+green dominate blue by threshold and blue < 100
        """
        self._validate_threshold(threshold)
        return (self.r + self.g > threshold * self.b) and (self.b < 100)

    def is_purple(self, threshold: float = 1.5) -> bool:
        """Return True if red+blue dominate and green is low.

        Parameters
        ----------
        threshold : float
            Minimum dominance multiplier (default: 1.5)

        Returns
        -------
        bool
            True if red+blue dominate green by threshold and green < 100
        """
        self._validate_threshold(threshold)
        return (self.r + self.b > threshold * self.g) and (self.g < 100)

    def _validate_threshold(self, threshold: float) -> None:
        """Validate threshold is positive number.
        
        Parameters
        ----------
        threshold : float
            Threshold value
        
        Returns
        -------
        None
        
        Raises
        ------
        ValueError
            If threshold is not positive
        """
        if threshold <= 0:
            raise ValueError(f"Threshold must be positive, got {threshold}")

    def _validate_intensity(self, intensity: int) -> None:
        """Validate intensity is valid RGB value (0-255).
        
        Parameters
        ----------
        intensity : int
            Intensity value to validate
        
        Returns
        -------
        None
        
        Raises
        ------
        ValueError
            If intensity is not between 0-255
        """
        if not 0 <= intensity <= 255:
            raise ValueError(f"Intensity must be between 0-255, got {intensity}")

    def _validate_tolerance(self, tolerance: int) -> None:
        """Validate tolerance is non-negative.
        
        Parameters
        ----------
        tolerance : int
            Tolerance value to validate
        
        Returns
        -------
        None
        
        Raises
        ------
        ValueError
            If tolerance is negative
        """
        if tolerance < 0:
            raise ValueError(f"Tolerance must be non-negative, got {tolerance}")