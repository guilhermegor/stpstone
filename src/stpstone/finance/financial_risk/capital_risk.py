### CAPITAL RISK ASSESSMENT ###


class CR1Calculator:
    """
    DOCSTRING: A CLASS TO CALCULATE CAPITAL REQUIREMENT 1 (CR1) FOR FINANCIAL INSTRUMENTS
    INPUTS: EAD, PD, LGD
    OUTPUTS: K, CR1
    """

    def __init__(self, float_ead: float, float_pd: float, float_lgd: float):
        """
        DOCSTRING: INITIALIZE THE CALCULATION WITH BASIC PARAMETERS
        INPUTS:
            - EXPOSURE AT DEFAULT (EAD) IN MONETARY UNITS
            - PROBABILITY OF DEFAULT (PD) AS A FRACTION
            - LOSS GIVEN DEFAULT (LGD) AS A FRACTION
        """
        self.float_ead = float_ead
        self.float_pd = float_pd
        self.float_lgd = float_lgd

    @property
    def calculate_k(self) -> float:
        """
        DOCSTRING: CALCULATE THE RISK FACTOR (K) BASED ON THE PARAMETERS
        INPUTS: -
        OUTPUTS: FLOAT
        """
        # Simplified calculation for K, regulatory adjustments (R) can be added accordingly
        return self.float_lgd * self.float_pd

    @property
    def calculate_cr1(self) -> float:
        """
        DOCSTRING: CALCULATE THE CAPITAL REQUIREMENT 1 (CR1)
        INPUTS: -
        OUTPUTS: VALUE OF CR1 IN MONETARY UNITS.
        """
        return 12.5 * self.calculate_k * self.float_ead

    @property
    def summary(self) -> dict:
        """
        DOCSTRING: RETURN A SUMMARY OF THE PARAMETERS AND CALCULATIONS.
        INPUTS: -
        OUTPUTS: DICTIONARY CONTAINING EAD, PD, LGD, K, AND CR1.
        """
        return {
            'EAD': self.float_ead,
            'PD': self.float_pd,
            'LGD': self.float_lgd,
            'K': self.calculate_k,
            'CR1': self.calculate_cr1,
        }
