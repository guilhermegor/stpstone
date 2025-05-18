class ColorIdentifier:
    """Identify if a hex color belongs to a specific color family (e.g., green, red)."""
    
    def __init__(self, hex_color: str):
        self.hex = hex_color.lstrip('#').lower()
        self.r = int(self.hex[0:2], 16)
        self.g = int(self.hex[2:4], 16)
        self.b = int(self.hex[4:6], 16)
    
    def bl_green(self, threshold: float = 1.5) -> bool:
        """Return True if green is dominant over red/blue by a threshold multiplier."""
        return (self.g > threshold * self.r) and (self.g > threshold * self.b)
    
    def bl_red(self, threshold: float = 1.5) -> bool:
        """Return True if red is dominant over green/blue."""
        return (self.r > threshold * self.g) and (self.r > threshold * self.b)
    
    def bl_blue(self, threshold: float = 1.5) -> bool:
        """Return True if blue is dominant over red/green."""
        return (self.b > threshold * self.r) and (self.b > threshold * self.g)
    
    def bl_white(self, min_intensity: int = 230) -> bool:
        """Return True if all RGB components are near maximum (e.g., #ffffff)."""
        return all(c >= min_intensity for c in [self.r, self.g, self.b])
    
    def bl_black(self, max_intensity: int = 25) -> bool:
        """Return True if all RGB components are near minimum (e.g., #000000)."""
        return all(c <= max_intensity for c in [self.r, self.g, self.b])
    
    def bl_gray(self, tolerance: int = 10) -> bool:
        """Return True if R ≈ G ≈ B within a tolerance range."""
        return (
            abs(self.r - self.g) <= tolerance and 
            abs(self.g - self.b) <= tolerance
        )

    def bl_yellow(self, threshold: float = 1.5) -> bool:
        """Return True if red+green dominate and blue is low."""
        return (self.r + self.g > threshold * self.b) and (self.b < 100)
    
    def bl_purple(self, threshold: float = 1.5) -> bool:
        """Return True if red+blue dominate and green is low."""
        return (self.r + self.b > threshold * self.g) and (self.g < 100)