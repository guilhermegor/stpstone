### A CLASS TO CALCULATE CAPITAL REQUIREMENT 1 (CR1) FOR FINANCIAL INSTRUMENTS ###

from typing import Dict


class CR1Calculator:

    def __init__(self, float_ead: float, float_pd: float, float_lgd: float) -> None:
        """
        Args:
            float_ead (float): Exposure at Default.
            float_pd (float): Probability of Default.
            float_lgd (float): Loss Given Default.
        """
        self.float_ead = float_ead
        self.float_pd = float_pd
        self.float_lgd = float_lgd

    @property
    def calculate_k(self) -> float:
        """
        Calculates the capital requirement k, which is the product of the loss given default (LGD)
        and the probability of default (PD).

        Returns:
            float: The capital requirement k.
        """
        return self.float_lgd * self.float_pd

    @property
    def calculate_cr1(self) -> float:
        """
        Calculates the capital requirement 1 (CR1) by multiplying the exposure at default (EAD)
        by the capital requirement k, which is the product of the loss given default (LGD) and
        the probability of default (PD), and then by 12.5.

        Returns:
            float: The capital requirement 1 (CR1).
        """
        return 12.5 * self.calculate_k * self.float_ead

    @property
    def summary(self) -> Dict[str, float]:
        """
        A summary of all the values used to calculate the capital requirement 1 (CR1)

        Returns:
            Dict[str, float]: A dictionary containing the exposure at default (EAD),
            the probability of default (PD),
            the loss given default (LGD), the capital requirement k, and the capital
            requirement 1 (CR1).
        """
        return {
            'EAD': self.float_ead,
            'PD': self.float_pd,
            'LGD': self.float_lgd,
            'K': self.calculate_k,
            'CR1': self.calculate_cr1
        }
