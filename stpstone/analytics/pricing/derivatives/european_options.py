"""European options pricing models."""

from math import pi
from typing import Literal, Optional, TypedDict, Union

import numpy as np
from numpy.typing import NDArray
from scipy.optimize import fsolve, minimize

from stpstone.analytics.quant.prob_distributions import NormalDistribution
from stpstone.analytics.quant.regression import NonLinearEquations
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class ResultVommaPositiveOutsideInterval(TypedDict):
    """TypedDict for VommaPositiveOutsideInterval results."""

    lower_boundary: float
    upper_boundary: float


class ResultVegaGlobalMax(TypedDict):
    """TypedDict for VegaGlobalMax results."""
    
    t_max_global_vega: float
    s_max_global_vega: float
    vega_max_global: float


class BlackScholesMerton(metaclass=TypeChecker):
    """Black Scholes Merton Model.
    
    References
    ----------
    https://brilliant.org/wiki/black-scholes-merton/
    """

    def __init__(self) -> None:
        """Initialize the class.
        
        Returns
        -------
        None
        """
        self.cls_normal_dist = NormalDistribution()

    def d1(self, s: float, k: float, b: float, t: float, sigma: float, q: float) -> float:
        """d1 is the probability for the option to be in the money (d1 > 0).
        
        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        b : float
            cost of carry
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        
        Returns
        -------
        float
            d1 probability
        """
        if sigma == 0:
            return float('inf') if s != k else 0.0
        return (np.log(s / k) + (b + sigma ** 2 / 2) * t) / (sigma * np.sqrt(t))

    def d2(self, s: float, k: float, b: float, t: float, sigma: float, q: float) -> float:
        """d2 is the probability for the option to be in the money (d2 > 0).
        
        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        b : float
            cost of carry
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        
        Returns
        -------
        float
            d2 probability
        """
        return self.d1(s, k, b, t, sigma, q) - sigma * np.sqrt(t)

    def general_opt_price(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """General option price function for European options.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            risk free rate
        opt_type : Literal['call', 'put']
            option type ('call' or 'put')

        Returns
        -------
        float
            option price
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        if opt_type == 'call':
            if t == 0:
                return max(s - k, 0)
            return s * np.exp((b - r) * t) * \
                self.cls_normal_dist.cdf(self.d1(s, k, b, t, sigma, q)) \
                - k * np.exp(-r * t) * self.cls_normal_dist.cdf(self.d2(s, k, b, t, sigma, q))
        elif opt_type == 'put':
            if t == 0:
                return max(k - s, 0)
            return k * np.exp(-r * t) * self.cls_normal_dist.cdf(
                -self.d2(s, k, b, t, sigma, q)) - s * np.exp((b - r) * t) \
                    * self.cls_normal_dist.cdf(-self.d1(s, k, b, t, sigma, q))


class MonteCarlo(metaclass=TypeChecker):
    """Monte Carlo simulation for European Options pricing."""

    def vanilla_mc_price(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float,
        opt_type: Literal['call', 'put'], 
        n_simulations: int = 100_000,
        n_steps: int = 252, 
        random_seed: Optional[int] = None
    ) -> tuple[float, float]:
        r"""
        Price European options using Monte Carlo simulation with GBM.
        
        This function simulates trajectories of the underlying asset price using the
        Geometric Brownian Motion (GBM) model and calculates the expected present
        value of the option payoff.
        
        Parameters
        ----------
        s : float
            Current stock price.
        k : float
            Strike price of the option.
        r : float
            Risk-free interest rate.
        t : float
            Time to maturity in years.
        sigma : float
            Volatility of the underlying asset.
        opt_type : Literal['call', 'put']
            Option type ('call' or 'put').
        n_simulations : int
            Number of simulations. Default is 100,000.
        n_steps : int
            Number of time steps. Default is 252 (1 year).
        random_seed : Optional[int]
            Random seed for reproducibility. Default is None.
        
        Returns
        -------
        tuple[float, float]
            A tuple containing the estimated option price and its standard error.
            float_option_price : float
                Estimated option price discounted to present value.
            float_se : float
                Standard error of the estimate (for confidence interval calculation).
                95% confidence interval: price ± 1.96 * se
        
        Notes
        -----
        The model uses the GBM (Geometric Brownian Motion) equation:
        
        .. math::
            dS_t = r S_t dt + \sigma S_t dW_t
        
        Where:
        - S_t is the asset price at time t
        - r is the risk-free rate
        - \sigma is the volatility
        - W_t is a Wiener process (Brownian motion)
        
        The Euler discretization results in:
        
        .. math::
            S_{t+\Delta t} = S_t \exp\left[(r - \frac{\sigma^2}{2})\Delta t + 
                                            \sigma\sqrt{\Delta t} Z\right]
        
        Where Z ~ N(0,1)
        
        Examples
        --------
        >>> # European call: S0=100, K=105, T=1 year, r=5%, vol=20%
        >>> float_price, float_error = monte_carlo_option_pricing(
        ...     s=100.0,
        ...     k=105.0,
        ...     t=1.0,
        ...     r=0.05,
        ...     sigma=0.20,
        ...     str_option_type='call'
        ... )
        >>> print(f"Price: ${float_price:.2f} ± {float_error:.2f}")
        Price: $8.02 ± 0.03
        
        >>> # European put: S0=100, K=95, T=6 months, r=5%, vol=25%
        >>> float_price_put, float_error_put = monte_carlo_option_pricing(
        ...     s=100.0,
        ...     k=95.0,
        ...     t=0.5,
        ...     r=0.05,
        ...     sigma=0.25,
        ...     str_option_type='put'
        ... )
        >>> print(f"Put Price: ${float_price_put:.2f}")
        Put Price: $3.45
        
        See Also
        --------
        monte_carlo_barrier_option : Pricing of barrier options
        
        References
        ----------
        .. [1] Black, F., & Scholes, M. (1973). "The Pricing of Options and 
            Corporate Liabilities". Journal of Political Economy, 81(3), 637-654.
        .. [2] Hull, J. C. (2018). "Options, Futures, and Other Derivatives" 
            (10th ed.). Pearson.
        """
        if random_seed is not None:
            np.random.seed(random_seed)

        dt: float = t / n_steps
    
        # generate random shocks from standard normal distribution
        array_z: NDArray[np.float64] = np.random.standard_normal(
            (n_simulations, n_steps)
        )
        
        array_s: NDArray[np.float64] = np.zeros(
            (n_simulations, n_steps + 1)
        )
        array_s[:, 0] = s
        
        # simulate trajectories using geometric brownian motion
        for int_t in range(1, n_steps + 1):
            array_s[:, int_t] = array_s[:, int_t-1] * np.exp(
                (r - 0.5 * sigma**2) * dt +
                sigma * np.sqrt(dt) * array_z[:, int_t-1]
            )
        
        array_payoff: NDArray[np.float64]
        if opt_type == 'call':
            array_payoff = np.maximum(array_s[:, -1] - k, 0)
        else:
            array_payoff = np.maximum(k - array_s[:, -1], 0)
        
        # discount to present value
        float_option_price: float = np.exp(-r * t) * np.mean(array_payoff)
        float_se: float = (np.std(array_payoff) / np.sqrt(n_simulations) * 
                        np.exp(-r * t))
        
        return float_option_price, float_se
    
    def monte_carlo_barrier_option(
        s: float,
        k: float,
        r: float,
        t: float,
        sigma: float,
        h: float,
        str_barrier_type: Optional[Literal['down-and-out', 'up-and-out']] = 'down-and-out',
        str_option_type: Optional[Literal['call', 'put']] = 'call',
        n_simulations: int = 100_000,
        n_steps: int = 252
    ) -> tuple[float, float]:
        r"""
        Price barrier options using Monte Carlo simulation.
        
        Barrier options are path-dependent options whose payoff depends on whether
        the underlying asset price hits a predetermined level (barrier) during
        the option's life.
        
        Parameters
        ----------
        s : float
            Current stock price.
        k : float
            Strike price of the option.
        r : float
            Risk-free interest rate.
        t : float
            Time to maturity in years.
        sigma : float
            Volatility of the underlying asset.
        h : float
            Barrier level.
        str_barrier_type : Optional[Literal['down-and-out', 'up-and-out']]
            Type of barrier ('down-and-out' or 'up-and-out'). Default is 'down-and-out'.
        str_option_type : Optional[Literal['call', 'put']]
            Option type ('call' or 'put'). Default is 'call'.
        n_simulations : int
            Number of simulations. Default is 100,000.
        n_steps : int
            Number of time steps. Default is 252 (1 year).
        
        Returns
        -------
        tuple[float, float]
            A tuple containing the estimated barrier option price and its standard error.
            float_option_price : float
                Estimated price of the barrier option.
            float_se : float
                Standard error of the estimate.
        
        Notes
        -----
        Barrier options are cheaper than equivalent vanilla options (knock-out)
        or can offer additional leverage (knock-in).
        
        The barrier is monitored continuously (or discretely, according to n_steps)
        throughout the option's life. Once the barrier is:
        
        - Hit in knock-out: option cancels immediately and payoff = 0
        - Hit in knock-in: option becomes a standard vanilla option
        - Not hit in knock-out: option behaves like vanilla
        - Not hit in knock-in: option expires worthless
        
        Warnings
        --------
        Using few time steps (n_steps) can result in:
        
        1. Discretization bias: the barrier can be "crossed" between steps
        2. Underestimation of the probability of hitting the barrier
        3. Incorrect pricing, especially for barriers close to current price
        
        Recommendation: use n_steps >= 252 for barriers.
        
        Examples
        --------
        >>> # Down-and-Out Call: cancels if it falls below 90
        >>> float_price, float_error = monte_carlo_barrier_option(
        ...     s=100.0,
        ...     k=105.0,
        ...     r=0.05,
        ...     t=1.0,
        ...     sigma=0.20,
        ...     h=90.0,
        ...     str_barrier_type='down-and-out',
        ...     str_option_type='call'
        ... )
        >>> print(f"Price: ${float_price:.2f}")
        Price: $6.45
        
        >>> # Up-and-In Put: only activates if it rises above 110
        >>> float_price_uip, float_error_uip = monte_carlo_barrier_option(
        ...     s=100.0,
        ...     k=95.0,
        ...     r=0.05,
        ...     t=1.0,
        ...     sigma=0.30,
        ...     h=110.0,
        ...     str_barrier_type='up-and-in',
        ...     str_option_type='put'
        ... )
        >>> print(f"Up-and-In Put Price: ${float_price_uip:.2f}")
        Up-and-In Put Price: $1.23
        
        See Also
        --------
        monte_carlo_option_pricing : Pricing of simple European options
        
        References
        ----------
        .. [1] Merton, R. C. (1973). "Theory of Rational Option Pricing". 
            The Bell Journal of Economics and Management Science, 4(1), 141-183.
        .. [2] Rubinstein, M., & Reiner, E. (1991). "Breaking Down the Barriers". 
            Risk, 4(8), 28-35.
        .. [3] Hull, J. C. (2018). "Options, Futures, and Other Derivatives" 
            (10th ed.), Chapter 26: Exotic Options.
        """
        dt: float = t / n_steps
        
        array_z: NDArray[np.float64] = np.random.standard_normal(
            (n_simulations, n_steps)
        )
        
        array_s: NDArray[np.float64] = np.zeros(
            (n_simulations, n_steps + 1)
        )
        array_s[:, 0] = s

        # flag to track if barrier was hit
        array_barrier_hit: NDArray[np.bool_] = np.zeros(
            n_simulations, 
            dtype=bool
        )
        
        # simulate trajectories and monitor barrier
        int_t: int
        for int_t in range(1, n_steps + 1):
            # calculate new price using GBM
            array_s[:, int_t] = array_s[:, int_t-1] * np.exp(
                (r - 0.5 * sigma**2) * dt + 
                sigma * np.sqrt(dt) * array_z[:, int_t-1]
            )
            
            # check barrier (permanent memory with |=)
            #   |= accumulates hits flags over time steps
            if 'down' in str_barrier_type:
                array_barrier_hit |= (array_s[:, int_t] <= h)
            else:
                array_barrier_hit |= (array_s[:, int_t] >= h)
        
        # calculate payoff at maturity
        array_payoff: NDArray[np.float64]
        if str_option_type == 'call':
            array_payoff = np.maximum(array_s[:, -1] - k, 0)
        else:
            array_payoff = np.maximum(k - array_s[:, -1], 0)
        
        # apply barrier condition
        if 'out' in str_barrier_type:
            array_payoff[array_barrier_hit] = 0  # knock-out
        else:
            array_payoff[~array_barrier_hit] = 0  # knock-in
        
        # discount to present value
        float_option_price: float = np.exp(-r * t) * np.mean(array_payoff)
        float_se: float = (np.std(array_payoff) / np.sqrt(n_simulations) * 
                        np.exp(-r * t))
        
        return float_option_price, float_se


class Greeks(BlackScholesMerton):
    """Greeks for European Options using the Black-Scholes-Merton model.

    References
    ----------
    https://www.macroption.com/option-greeks-excel/, https://en.wikipedia.org/wiki/Greeks_(finance)
    """

    def delta(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Delta is the rate of change of the theoretical option value with respect to changes.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            risk free rate
        opt_type : Literal['call', 'put']
            option type ('call' or 'put')

        Returns
        -------
        float
            delta
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        if sigma == 0:
            if opt_type == 'call':
                return 1.0 if s > k else 0.0
            else:
                return -1.0 if s < k else 0.0
        
        d1 = self.d1(s, k, b, t, sigma, q)
        if opt_type == 'call':
            return np.exp((b - r) * t) * self.cls_normal_dist.cdf(d1)
        else:
            return np.exp((b - r) * t) * (self.cls_normal_dist.cdf(d1) - 1)

    def future_delta_from_spot_delta(self, delta: float, b: float, t: float) -> float:
        """Future delta from spot delta.

        Parameters
        ----------
        delta : float
            delta
        b : float
            risk free rate
        t : float
            time to maturity

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return delta * np.exp(-b * t)

    def strike_from_delta(
        self, 
        s: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        delta: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Strike from delta.

        Parameters
        ----------
        s : float
            spot price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            risk free rate
        delta : float
            delta
        opt_type : Literal['call', 'put']
            option type ('call' or 'put')

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        if opt_type == 'call':
            return s * np.exp(-self.cls_normal_dist.inv_cdf(delta * np.exp((r - b) * t))
                              * sigma * t ** 0.5 + (b + sigma ** 2 / 2.0) * t)
        elif opt_type == 'put':
            return s * np.exp(self.cls_normal_dist.inv_cdf(delta * np.exp((r - b) * t))
                              * sigma * t ** 0.5 + (b + sigma ** 2 / 2.0) * t)

    def gamma(self, s: float, k: float, r: float, t: float, sigma: float, q: float, b: float) \
        -> float:
        """Gamma.
        
        Rate of change of delta with respect to changes in the underlying asset's price.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            risk free rate

        Returns
        -------
        float
            gamma
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return self.cls_normal_dist.pdf(self.d1(s, k, b, t, sigma, q)) * np.exp((b - r) * t) \
            / (s * sigma * t ** 0.5)

    def saddle_gamma(self, k: float, r: float, sigma: float, q: float, b: float) -> float:
        """Saddle point for gamma, where gamma is zero.

        Parameters
        ----------
        k : float
            strike price
        r : float
            risk free rate
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            risk free rate

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return np.sqrt((np.exp(1) / pi) * ((2 * b - r) / sigma ** 2 + 1)) / k

    def gamma_p(self, s: float, k: float, r: float, t: float, sigma: float, q: float, b: float) \
        -> float:
        """Gamma percentage.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            risk free rate

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return self.cls_normal_dist.pdf(self.d1(s, k, b, t, sigma, q)) * np.exp((b - r) * t) \
            / (100.0 * sigma * t ** 0.5)

    def theta(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Theta is the rate of change of the theoretical option value with respect to time.
        
        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            risk free rate
        opt_type : Literal['call', 'put']
            option type ('call' or 'put')

        Returns
        -------
        float
            theta
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        if opt_type == 'call':
            return -(s * np.exp((b - r) * t) * self.cls_normal_dist.pdf(self.d1(
                s, k, b, t, sigma, q)) * sigma) / (2.0 * t ** 0.5) - (b - r) * s * np.exp(
                (b - r) * t) * self.cls_normal_dist.cdf(self.d1(s, k, b, t, sigma, q)) \
                - r * k * \
                np.exp(-r * t) * \
                self.cls_normal_dist.cdf(self.d2(s, k, b, t, sigma, q))
        elif opt_type == 'put':
            return -(s * np.exp((b - r) * t) * self.cls_normal_dist.pdf(self.d1(
                s, k, b, t, sigma, q)) * sigma) / (2.0 * t ** 0.5) + (b - r) * s * np.exp(
                (b - r) * t) * self.cls_normal_dist.cdf(-self.d1(s, k, b, t, sigma, q)) \
                + r * k * \
                np.exp(-r * t) * \
                self.cls_normal_dist.cdf(-self.d2(s, k, b, t, sigma, q))

    def vega(self, s: float, k: float, r: float, t: float, sigma: float, q: float, b: float) \
        -> float:
        """Vega.
        
        Rate of change of the theoretical option value with respect to changes in volatility.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            risk free rate

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return s * np.exp((b - r) * t) * self.cls_normal_dist.pdf(self.d1(
            s, k, b, t, sigma, q)) * np.sqrt(t)

    def vega_local_maximum(self, k: float, t: float, sigma: float, b: float) -> float:
        """Vega local maximum.

        Parameters
        ----------
        k : float
            strike price
        t : float
            time to maturity
        sigma : float
            volatility
        b : float
            risk free rate

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return k * np.exp(-b + sigma ** 2 / 2.0) * t

    def strike_maximizes_vega(self, s: float, t: float, sigma: float, b: float) -> float:
        """Strike that maximizes vega.

        Parameters
        ----------
        s : float
            spot price
        t : float
            time to maturity
        sigma : float
            volatility
        b : float
            risk free rate

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return s * np.exp(b + sigma ** 2 / 2.0) * t

    def time_to_maturity_maximum_vega(self, s: float, k: float, r: float, sigma: float, b: float) \
        -> float:
        """Time to maturity that maximizes vega.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        sigma : float
            volatility
        b : float
            risk free rate

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return 2 * (1.0 + (1.0 + (8.0 * r / sigma ** 2 + 1.0) * np.log(s / k) ** 2) ** 0.5) \
            / (8.0 * r + sigma ** 2)

    def vega_global_maximum(self, k: float, r: float, sigma: float, b: float) \
        -> ResultVegaGlobalMax:
        """Vega global maximum.
        
        Parameters
        ----------
        k : float
            strike price
        r : float
            risk free rate
        sigma : float
            volatility
        b : float
            risk free rate

        Returns
        -------
        ResultVegaGlobalMax

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        # global maximum time for vega
        t = 1 / (2.0 * r)
        # stock price at the maximum time
        s = k * np.exp((-b + sigma ** 2 / 2.0) * t)
        # vega at t
        vega = k / (2.0 * (r * np.exp(1) * pi) ** 0.5)
        return {
            't_max_global_vega': t,
            's_max_global_vega': s,
            'vega_max_global': vega
        }

    def vega_gamma_relationship(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float
    ) -> float:
        """Vega gamma relationship.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            risk free rate

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return self.gamma(s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b) * sigma * s ** 2 * t

    def vega_delta_relationship(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Vega delta relationship.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            risk free rate
        opt_type : Literal['call', 'put']
            option type ('call' or 'put')

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return np.exp((b - r) * t) * self.cls_normal_dist.pdf(self.cls_normal_dist.inv_cdf(
            np.exp((r - b) * t) * np.abs(self.delta(s, k, r, t, sigma, q, b, opt_type)))) \
            / (s * sigma * t ** 0.5)

    def vega_p(self, s: float, k: float, r: float, t: float, sigma: float, q: float, b: float) \
        -> float:
        """Vega percentual change.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float    
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            risk free rate

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return sigma / 10.0 * s * np.exp((b - r) * t) * self.cls_normal_dist.pdf(
            self.d1(s, k, b, t, sigma, q)) * t ** 0.5

    def vega_elasticity(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Vega elasticity is the ratio of vega to the option price, multiplied by the volatility.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry
        opt_type : Literal['call', 'put']
            option type ('call' or 'put')

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return self.vega(s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b) * sigma \
            / self.general_opt_price(s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b, opt_type=opt_type)

    def rho(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Rho.
        
        Rate of change of the theoretical option value with respect to changes in the risk free 
        rate.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry
        opt_type : Literal['call', 'put']
            option type ('call' or 'put')

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        if opt_type == 'call':
            return t * k * np.exp(- r * t) * self.cls_normal_dist.cdf(
                self.d2(s, k, b, t, sigma, q))
        elif opt_type == 'put':
            return -t * k * np.exp(- r * t) * self.cls_normal_dist.cdf(
                -self.d2(s, k, b, t, sigma, q))

    def lambda_greek(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Lambda.
        
        Rate of change of the theoretical option value with respect to changes in the dividend 
        yield.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry
        opt_type : Literal['call', 'put']
            option type ('call' or 'put')

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return self.delta(s, k, r, t, sigma, q, opt_type) * s / \
            self.general_opt_price(s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b, opt_type=opt_type)

    def vanna(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Vanna.
        
        Rate of change of delta with respect to changes in the implied volatility (sigma), or 
        the sensitivity of vega to changes in the underlying price (s).

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry
        opt_type : Literal['call', 'put']
            option type ('call' or 'put')

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug

        Notes
        -----
        Essentially, it tells how much the option's sensitivity to price changes (delta) will be 
        affected by changes in the market's expectation of price fluctuations (implied volatility).
        """
        return -np.exp((b - r) * t) * self.d2(s, k, b, t, sigma, q) * self.cls_normal_dist.pdf(
            self.d1(s, k, b, t, sigma, q)) / sigma

    def vanna_vol(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Vanna Vol.
         
        Rate of change of the theoretical option value with respect to changes in the volatility.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry
        opt_type : Literal['call', 'put']
            option type ('call' or 'put')

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return self.vanna(s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b, opt_type=opt_type) \
            * (1.0 / sigma) * (self.d1(s, k, b, t, sigma, q) * self.d2(s, k, b, t, sigma, q)
                - self.d1(s, k, b, t, sigma, q) / self.d2(s, k, b, t, sigma, q) - 1.0)

    def charm(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Charm is the rate of change of delta with respect to time.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry
        opt_type : Literal['call', 'put']
            option type ('call' or 'put')

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        if opt_type == 'call':
            return -np.exp((b - r) * t) * (self.cls_normal_dist.pdf(
                self.d1(s, k, b, t, sigma, q)) * (b / (sigma * t ** 0.5) - self.d2(
                    s, k, b, t, sigma, q) / (2.0 * t)) + (b - r) * self.cls_normal_dist.cdf(
                        self.d1(s, k, b, t, sigma, q)))
        elif opt_type == 'put':
            return -np.exp((b - r) * t) * (self.cls_normal_dist.pdf(
                self.d1(s, k, b, t, sigma, q)) * (b / (sigma * t ** 0.5) - self.d2(
                    s, k, b, t, sigma, q) / (2.0 * t)) - (b - r) * self.cls_normal_dist.cdf(
                        -self.d1(s, k, b, t, sigma, q)))

    def zomma(self, s: float, k: float, r: float, t: float, sigma: float, q: float, b: float) \
        -> float:
        """Zomma.
        
        Rate of change of the theoretical option value with respect to changes in the cost 
        of carry.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return self.gamma(s, k, r, t, sigma, q, b) * (self.d1(s, k, b, t, sigma, q) * self.d2(
            s, k, b, t, sigma, q) - 1) / sigma

    def zomma_p(self, s: float, k: float, r: float, t: float, sigma: float, q: float, b: float) \
        -> float:
        """Zomma percentage.
        
        Rate of change of the theoretical option value with respect to changes in the cost of 
        carry, expressed as a percentage.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return self.zomma(s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b) * s / 100

    def speed(self, s: float, k: float, r: float, t: float, sigma: float, q: float, b: float) \
        -> float:
        """Speed.
        
        Rate of change of gamma with respect to changes in the underlying asset's price.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return -self.gamma(s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b) \
            * (1.0 + self.d1(s, k, b, t, sigma, q) / (sigma * t ** 0.5)) / s

    def speed_p(self, s: float, k: float, r: float, t: float, sigma: float, q: float, b: float) \
        -> float:
        """Speed percentage.
        
        Rate of change of gamma with respect to changes in the underlying asset's price, 
        expressed as a percentage.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return self.speed(s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b) * s / 100

    def color(self, s: float, k: float, r: float, t: float, sigma: float, q: float, b: float) \
        -> float:
        """Color is the rate of change of gamma with respect to changes in the cost of carry.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return -self.gamma(s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b) * (r - b + b * self.d1(
            s, k, b, t, sigma, q) / (sigma * t ** 0.5) + (1.0 - self.d1(
                s, k, b, t, sigma, q) * self.d2(s, k, b, t, sigma, q)) / (2.0 * t))

    def color_p(self, s: float, k: float, r: float, t: float, sigma: float, q: float, b: float) \
        -> float:
        """Color percentage.
         
        Rate of change of gamma with respect to changes in the cost of carry, expressed as a 
        percentage.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return -self.gamma_p(s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b) \
            * (r - b + b * self.d1(s, k, b, t, sigma, q)
               / (sigma * t ** 0.5) + (1.0 - self.d1(s, k, b, t, sigma, q)
                                       * self.d2(s, k, b, t, sigma, q)) / (2.0 * t))

    def vomma(self, s: float, k: float, r: float, t: float, sigma: float, q: float, b: float) \
        -> float:
        """Vomma is the rate of change of vega with respect to changes in volatility.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return self.vega(s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b) \
            * self.d1(s, k, b, t, sigma, q) * self.d2(s, k, b, t, sigma, q) / sigma

    def vomma_p(self, s: float, k: float, r: float, t: float, sigma: float, q: float, b: float) \
        -> float:
        """Vomma percentage.
        
        Rate of change of vega with respect to changes in volatility, expressed as a percentage.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return self.vega_p(s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b) \
            * self.d1(s, k, b, t, sigma, q) * self.d2(s, k, b, t, sigma, q) / sigma

    def vomma_positive_outside_interval(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        bool_spot: bool = True
    ) -> ResultVommaPositiveOutsideInterval:
        """Vomma positive outside interval.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry
        bool_spot : bool
            True if s is the spot price, False if k is the strike price, by default True

        Returns
        -------
        ResultVommaPositiveOutsideInterval

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        # sign of cost of carry, according to s_k being a spot or strike value
        if bool_spot:
            sign_ = 1
            s_k = s
        else:
            sign_ = -1
            s_k = k
        # return vomma 0 boundaries
        return {
            'lower_boundary': s_k * np.exp((sign_ * b - sigma ** 2 / 2.0) * t),
            'upper_boundary': s_k * np.exp((sign_ * b + sigma ** 2 / 2.0) * t)
        }

    def ultima(self, s: float, k: float, r: float, t: float, sigma: float, q: float, b: float) \
        -> float:
        """Ultima is the rate of change of vomma with respect to changes in volatility.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return self.vomma(s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b) / sigma \
            * (self.d1(s, k, b, t, sigma, q) * self.d2(s, k, b, t, sigma, q)
               - self.d1(s, k, b, t, sigma, q) / self.d2(s, k, b, t, sigma, q)
                - self.d2(s, k, b, t, sigma, q) / self.d1(s, k, b, t, sigma, q) - 1.0)

    def d_vega_d_time(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float
    ) -> float:
        """Delta vega with respect to time.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return self.vega(s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b) * (r - b + b * self.d1(
            s, k, b, t, sigma, q) / (sigma * t ** 0.5) - (1.0 + self.d1(
                s, k, b, t, sigma, q) * self.d2(s, k, b, t, sigma, q)) / (2.0 * t))

    def variance_vega(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float
    ) -> float:
        """Variance vega.
        
        Rate of change of the theoretical option value with respect to changes in the variance 
        of the underlying asset.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return self.vega(s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b) / (2 * sigma)

    def variance_vanna(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float
    ) -> float:
        """Variance vanna.
        
        Rate of change of the theoretical option value with respect to changes in the variance 
        of the underlying asset.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return - s * np.exp((b - r) * t) * self.pdf(self.d1(s, k, b, t, sigma, q)) * self.d2(
            s, k, b, t, sigma, q) / (2.0 * sigma)

    def variance_vomma(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float
    ) -> float:
        """Variance vomma.
        
        Rate of change of vega with respect to changes in the implied volatility.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return s * np.exp((b - r) * t) * t ** 0.5 / (4.0 * sigma ** 3) * self.pdf(self.d1(
            s, k, b, t, sigma, q)) * (self.d1(s, k, b, t, sigma, q) * self.d2(
                s, k, b, t, sigma, q) - 1.0)

    def variance_ultima(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float
    ) -> float:
        """Variance ultima.
        
        Rate of change of vomma with respect to changes in the implied volatility.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return s * np.exp((b - r) * t) * t ** 0.5 / (8 * sigma ** 5) * self.pdf(self.d1(
            s, k, b, t, sigma, q)) * ((self.d1(s, k, b, t, sigma, q) * self.d2(
                s, k, b, t, sigma, q) - 1.0) * (self.d1(s, k, b, t, sigma, q) * self.d2(
                    s, k, b, t, sigma, q) - 3.0) - (self.d1(s, k, b, t, sigma, q) ** 2 + self.d2(
                        s, k, b, t, sigma, q) ** 2))

    def dbsm_dohm(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float
    ) -> float:
        """Rate of change of BSM's model regarding OHM (standard deviation - time).
        
        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry

        Returns
        -------
        float
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return s * self.cls_normal_dist.pdf((np.log(s / k) + t * sigma ** 2 / 2.0)
                                            / (sigma * t ** 0.5))

    def driftless_theta(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float
    ) -> float:
        """Driftless theta.
        
        Expected change in the option price over time, assuming no drift in the underlying 
        asset's price.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return -s * self.cls_normal_dist.pdf(self.d1(s, k, b, t, sigma, q)) * sigma / (
            2.0 * t ** 0.5)

    def theta_vega_relationship(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float
    ) -> float:
        """Theta-vega relationship.
        
        Relationship between theta and vega, which indicates how much the option's price will 
        change with respect to changes in implied volatility.
        
        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return -self.gamma(s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b) * sigma / (2.0 * t)

    def bleed_offset_volatility(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float
    ) -> float:
        """Bleed offset volatility.
        
        This is the amount of volatility that must increase to offset the theta-bleed/time decay.
        
        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry

        Returns
        -------
        float
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        
        Notes
        -----
        In the case of positive theta, one can actually have negative offset volatility.
        Deep in-the-money (DITM) European options can have positive theta, and in this case 
        the offset volatility will be negative.
        """
        return self.theta(s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b) \
            / self.vega(s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b)

    def theta_gamma_relationship_driftless(
        self, s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float
    ) -> float:
        """Theta-gamma relationship.
        
        Relationship between theta and gamma, which indicates how much the option's price will 
        change with respect to changes in implied volatility.

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return -2.0 * self.driftless_theta(s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b) / (
            s ** 2 * sigma ** 2)

    def phi(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Phi.
        
        The sensitivity of the option price to a change in the dividend yield (or foreign interest 
        rate in the case of a currency option).

        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry
        opt_type : Literal['call', 'put']
            option type

        Returns
        -------
        float
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        if opt_type == 'call':
            return -t * s * np.exp((b - r) * t) * self.cls_normal_dist.cdf(self.d1(
                s, k, b, t, sigma, q))
        elif opt_type == 'put':
            return t * s * np.exp((b - r) * t) * self.cls_normal_dist.cdf(-self.d1(
                s, k, b, t, sigma, q))

    def carry_rho(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Carry rho.
        
        The sensitivity of the option price to a change in the cost-of-carry rate.
        
        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry
        opt_type : Literal['call', 'put']
            option type

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        if opt_type == 'call':
            return t * s * np.exp((b - r) * t) * self.cls_normal_dist.cdf(self.d1(
                s, k, b, t, sigma, q))
        elif opt_type == 'put':
            return -t * s * np.exp((b - r) * t) * self.cls_normal_dist.cdf(-self.d1(
                s, k, b, t, sigma, q))

    def risk_neutral_prob_itm(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Risk-neutral probability for ending up ITM at maturity.
        
        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry
        opt_type : Literal['call', 'put']
            option type

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        if opt_type == 'call':
            return self.cls_normal_dist.cdf(self.d2(s, k, b, t, sigma, q))
        elif opt_type == 'put':
            return self.cls_normal_dist.cdf(-self.d2(s, k, b, t, sigma, q))

    def strike_given_risk_neutral_prob(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        p: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Strike price given a risk-neutral probability.
        
        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry
        p : float
            risk-neutral probability
        opt_type : Literal['call', 'put']
            option type

        Returns
        -------
        float
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        if opt_type == 'call':
            return s * np.exp(-self.cls_normal_dist.inv_cdf(p) * sigma * t ** 0.5
                              + (b - sigma ** 2 / 2.0) * t)
        elif opt_type == 'call':
            return s * np.exp(self.cls_normal_dist.inv_cdf(p) * sigma * t ** 0.5
                              + (b - sigma ** 2 / 2.0) * t)

    def d_zeta_d_vol(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Zeta's sensitivity to a small change in the implied volatility.
        
        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry
        opt_type : Literal['call', 'put']
            option type

        Returns
        -------
        float
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        if opt_type == 'call':
            return -self.cls_normal_dist.pdf(self.d2(s, k, b, t, sigma, q)) * self.d1(
                s, k, b, t, sigma, q) / sigma
        elif opt_type == 'put':
            return self.cls_normal_dist.pdf(self.d2(s, k, b, t, sigma, q)) * self.d1(
                s, k, b, t, sigma, q) / sigma

    def d_zeta_d_time(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Zeta's sensitivity to a small change in time.
        
        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry
        opt_type : Literal['call', 'put']
            option type

        Returns
        -------
        float
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        if opt_type == 'call':
            return self.pdf(self.d2(s, k, b, t, sigma, q)) * (b / (sigma * t ** 0.5) - self.d1(
                s, k, b, t, sigma, q) / (2.0 * t))
        elif opt_type == 'put':
            return -self.pdf(self.d2(s, k, b, t, sigma, q)) * (b / (sigma * t ** 0.5) - self.d1(
                s, k, b, t, sigma, q) / (2.0 * t))

    def risk_neutral_probability_density(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Risk-neutral probability density function for the option price.
        
        Second order BSM's formula regarding strike.
        
        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry
        opt_type : Literal['call', 'put']
            option type

        Returns
        -------
        float
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return self.cls_normal_dist.pdf(self.d2(s, k, b, t, sigma, q)) * np.exp(-r * t) / (
            k * sigma * t ** 0.5)

    def probability_ever_getting_itm(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Probability of ever getting ITM at maturity.
        
        Parameters
        ----------
        s : float
            spot price
        k : float
            strike price
        r : float
            risk free rate
        t : float
            time to maturity
        sigma : float
            volatility
        q : float
            dividend yield
        b : float
            cost of carry
        opt_type : Literal['call', 'put']
            option type (call or put)

        Returns
        -------
        float
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        mu = (b - sigma ** 2 / 2.0) / sigma ** 2
        lambda_ = (mu ** 2 + 2 * r / sigma ** 2) ** 0.5
        z = np.log(k / s) / (sigma * t ** 0.5) + lambda_ * sigma * t ** 0.5
        if opt_type == 'call':
            return (k / s) ** (mu + lambda_) * self.cls_normal_dist.cdf(-z) \
                + (k / s) ** (mu - lambda_) * self.cls_normal_dist.cdf(
                -z + 2 * lambda_ * sigma * t ** 0.5)
        elif opt_type == 'put':
            return (k / s) ** (mu + lambda_) * self.cls_normal_dist.cdf(z) \
                + (k / s) ** (mu - lambda_) * self.cls_normal_dist.cdf(
                z - 2 * lambda_ * sigma * t ** 0.5)

    def net_weighted_vega_exposure(self, psi_r: float, *dicts_opts: list[dict[str, float]]) \
        -> float:
        """Net weighted vega exposure.
        
        The sum of the vega exposures of all options, weighted by their respective volatilities 
        and correlations with the reference volatility.
        
        Parameters
        ----------
        psi_r : float
            volatility of reference volatility
        dicts_opts : tuple of dicts
            dictionaries containing the following keys:
            - 'q_vega': number of contracts with vega
            - 'vega_t': vega at time t
            - 'psi_t': volatility of volatility for the given set of options with same underlying
              and maturity
            - 'corr_t': correlation between the volatility with time to maturity t and the 
            reference volatility

        Returns
        -------
        float
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        return [float(dict_['q_vega']) * float(dict_['vega_t']) * float(dict_['psi_t'])
                * float(dict_['corr_t']) / float(psi_r) for dict_ in dicts_opts].sum()


class IterativeMethods(Greeks):
    """Iterative methods for European options."""

    def binomial_pricing_model(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        n: int, 
        u: float, 
        d: float, 
        opt_type: Literal['call', 'put'], 
        h_upper: Optional[float] = None, 
        h_lower: Optional[float] = None
    ) -> float:
        """Binomial pricing model for European options.

        Parameters
        ----------
        s : float
            Current stock price.
        k : float
            Strike price of the option.
        r : float
            Risk-free interest rate.
        t : float
            Time to maturity in years.  
        n : int
            Number of time steps.
        u : float
            Up-factor in binomial models.
        d : float
            Down-factor in binomial models.
        opt_type : Literal['call', 'put']
            Option style, either 'call' or 'put'.
        h_upper : Optional[float]
            Upper boundary price
        h_lower : Optional[float]
            Lower boundary price
        
        Returns
        -------
        float
        
        References
        ----------
        https://www.youtube.com/watch?v=a3906k9C0fM,
        https://www.youtube.com/watch?v=WxrRi9lNnqY
        """
        # precomute constants
        dt = t / n
        q = (np.exp(r * dt) - d) / (u - d)
        disc = np.exp(-r * dt)
        # initialise asset prices at maturity - time step n
        array_s_nodes = s * d**(np.arange(n, -1, -1)) * \
            u**(np.arange(0, n + 1, 1))
        # initialise option values at maturity - if intrinsic value is negative, consider zero
        if opt_type == 'call':
            array_cp = np.maximum(array_s_nodes - k, np.zeros(int(n) + 1))
        else:
            array_cp = np.maximum(k - array_s_nodes, np.zeros(int(n) + 1))
        # check s payoff, according to barriers, if values are different from none
        if h_upper is not None:
            array_cp[array_s_nodes >= h_upper] = 0
        if h_lower is not None:
            array_cp[array_s_nodes <= h_lower] = 0
        # step backwards recursion through tree
        for i in np.arange(int(n), 0, -1):
            array_cp = disc * \
                (q * array_cp[1:i + 1] + (1.0 - q) * array_cp[0:i])
        # returning the no-arbitrage price at node 0
        return array_cp[0]

    def crr_method(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        n: int, 
        sigma: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Cox, Ross and Rubinstein (CRR) method for European options.
        
        The CRR method is a binomial tree model that uses the Cox-Ross-Rubinstein parameters
        for the up and down factors, which are derived from the volatility of the underlying asset.
        
        Parameters
        ----------
        s : float
            Current stock price.
        k : float
            Strike price of the option.
        r : float
            Risk-free interest rate.
        t : float
            Time to maturity in years.
        n : int
            Number of time steps.
        sigma : float
            Volatility of the underlying asset.
        opt_type : Literal['call', 'put']
            Option style, either 'call' or 'put'.

        Returns
        -------
        float
        
        References
        ----------
        https://www.youtube.com/watch?v=nWslah9tHLk
        """
        # precomute constants
        dt = t / n
        u = np.exp(sigma * np.sqrt(dt))
        d = 1 / u
        q = (np.exp(r * dt) - d) / (u - d)
        disc = np.exp(-r * dt)
        # initialise asset prices at maturity - Time step N
        array_s_nodes = np.zeros(int(n) + 1)
        array_s_nodes[0] = s * d**n
        for j in range(1, int(n) + 1):
            array_s_nodes[j] = array_s_nodes[j - 1] * u / d
        # initialise option values at maturity
        array_cp = np.zeros(int(n) + 1)
        for _ in range(0, int(n) + 1):
            #   evaluating maximum value between fair and intrinsic value
            if opt_type == 'call':
                array_cp = np.maximum(array_cp, array_s_nodes - k)
            else:
                array_cp = np.maximum(array_cp, k - array_s_nodes)
        # step backwards through tree
        for i in np.arange(int(n), 0, -1):
            for j in range(0, i):
                array_cp[j] = disc * \
                    (q * array_cp[j + 1] + (1 - q) * array_cp[j])
        # returning the no-arbitrage price at node 0
        return array_cp[0]

    def jr_method(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        n: int, 
        sigma: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Jarrow and Rudd (JR) method for European options.
        
        The JR method is a binomial tree model that uses the Jarrow-Rudd parameters
        for the up and down factors, which are derived from the volatility of the underlying asset.
        
        Parameters
        ----------
        s : float
            Current stock price.
        k : float
            Strike price of the option.
        r : float
            Risk-free interest rate.
        t : float
            Time to maturity in years.
        n : int
            Number of time steps.
        sigma : float
            Volatility of the underlying asset.
        opt_type : Literal['call', 'put']
            Option style, either 'call' or 'put'.

        Returns
        -------
        float
        
        References
        ----------
        https://www.youtube.com/watch?v=nWslah9tHLk
        """
        # precomute constants
        dt = t / n
        nu = r - 0.5 * sigma**2
        u = np.exp(nu * dt + sigma * np.sqrt(dt))
        d = np.exp(nu * dt - sigma * np.sqrt(dt))
        q = 0.5
        disc = np.exp(-r * dt)
        # initialise asset prices at maturity - Time step N
        array_s_nodes = np.zeros(int(n) + 1)
        array_s_nodes[0] = s * d**n
        for j in range(1, int(n) + 1):
            array_s_nodes[j] = array_s_nodes[j - 1] * u / d
        # initialise option values at maturity
        array_cp = np.zeros(int(n) + 1)
        for _ in range(0, int(n) + 1):
            #   evaluating maximum value between fair and intrinsic value
            if opt_type == 'call':
                array_cp = np.maximum(array_cp, array_s_nodes - k)
            else:
                array_cp = np.maximum(array_cp, k - array_s_nodes)
        # step backwards through tree
        for i in np.arange(int(n), 0, -1):
            for j in range(0, i):
                array_cp[j] = disc * \
                    (q * array_cp[j + 1] + (1 - q) * array_cp[j])
        # returning the no-arbitrage price at node 0
        return array_cp[0]

    def eqp_method(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        n: int, 
        sigma: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Equal probabilities (EQP) method for European options.
        
        The EQP method is a binomial tree model that uses equal probabilities for the up and down
        factors, which are derived from the volatility of the underlying asset.
        
        Parameters
        ----------
        s : float
            Current stock price.
        k : float
            Strike price of the option.
        r : float
            Risk-free interest rate.
        t : float
            Time to maturity in years.
        n : int
            Number of time steps.
        sigma : float
            Volatility of the underlying asset.
        opt_type : Literal['call', 'put']
            Option style, either 'call' or 'put'.

        Returns
        -------
        float
        
        References
        ----------
        https://www.youtube.com/watch?v=nWslah9tHLk
        """
        # precomute constants
        dt = t / n
        nu = r - 0.5 * sigma**2
        dxu = 0.5 * nu * dt + 0.5 * \
            np.sqrt(4 * sigma**2 * dt - 3 * nu**2 * dt**2)
        dxd = 1.5 * nu * dt - 0.5 * \
            np.sqrt(4 * sigma**2 * dt - 3 * nu**2 * dt**2)
        pu = 0.5
        pd = 1 - pu
        disc = np.exp(-r * dt)
        # initialise asset prices at maturity - Time step N
        array_s_nodes = np.zeros(int(n) + 1)
        array_s_nodes[0] = s * np.exp(n * dxd)
        for j in range(1, int(n) + 1):
            array_s_nodes[j] = array_s_nodes[j - 1] * np.exp(dxu - dxd)
        # initialise option values at maturity
        array_cp = np.zeros(int(n) + 1)
        for _ in range(0, int(n) + 1):
            #   evaluating maximum value between fair and intrinsic value
            if opt_type == 'call':
                array_cp = np.maximum(array_cp, array_s_nodes - k)
            else:
                array_cp = np.maximum(array_cp, k - array_s_nodes)
        # step backwards through tree
        for i in np.arange(int(n), 0, -1):
            for j in range(0, i):
                array_cp[j] = disc * \
                    (pu * array_cp[j + 1] + pd * array_cp[j])
        # returning the no-arbitrage price at node 0
        return array_cp[0]

    def trg_method(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        n: int, 
        sigma: float, 
        opt_type: Literal['call', 'put']
    ) -> float:
        """Trigeorgis (TRG) method for European options.
        
        The TRG method is a binomial tree model that uses the Trigeorgis parameters
        for the up and down factors, which are derived from the volatility of the underlying asset.
        
        Parameters
        ----------
        s : float
            Current stock price.
        k : float
            Strike price of the option.
        r : float
            Risk-free interest rate.
        t : float
            Time to maturity in years.
        n : int
            Number of time steps.
        sigma : float
            Volatility of the underlying asset.
        opt_type : Literal['call', 'put']
            Option style, either 'call' or 'put'.

        Returns
        -------
        float
        
        References
        ----------
        https://www.youtube.com/watch?v=nWslah9tHLk
        """
        # precomute constants
        dt = t / n
        nu = r - 0.5 * sigma**2
        dxu = np.sqrt(sigma**2 * dt + nu**2 * dt**2)
        dxd = -dxu
        pu = 0.5 + 0.5 * nu * dt / dxu
        pd = 1 - pu
        disc = np.exp(-r * dt)
        # initialise asset prices at maturity - Time step N
        array_s_nodes = np.zeros(int(n) + 1)
        array_s_nodes[0] = s * np.exp(n * dxd)
        for j in range(1, int(n) + 1):
            array_s_nodes[j] = array_s_nodes[j - 1] * np.exp(dxu - dxd)
        # initialise option values at maturity
        array_cp = np.zeros(int(n) + 1)
        for _ in range(0, int(n) + 1):
            #   evaluating maximum value between fair and intrinsic value
            if opt_type == 'call':
                array_cp = np.maximum(array_cp, array_s_nodes - k)
            else:
                array_cp = np.maximum(array_cp, k - array_s_nodes)
        # step backwards through tree
        for i in np.arange(int(n), 0, -1):
            for j in range(0, i):
                array_cp[j] = disc * \
                    (pu * array_cp[j + 1] + pd * array_cp[j])
        # returning the no-arbitrage price at node 0
        return array_cp[0]


class EuropeanOptions(IterativeMethods):
    """European options pricing and volatility estimation.
    
    This class provides methods for pricing European options, including the Black-Scholes model,
    the Trigeorgis method, and the binomial tree model. It also includes methods for estimating
    the implied volatility of European options using various methods, such as the Newton-Raphson
    method, the bisection method, and the fsolve method.
    """

    def func_non_linear(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: Union[float, NDArray[np.float64]], 
        q: float, 
        b: float, 
        cp0: float,
        opt_type: Literal['call', 'put']
    ) -> float:
        """Calculate the cost function to be minimized for the Newton-Raphson and other methods.
        
        Parameters
        ----------
        s : float
            Current stock price.
        k : float
            Strike price of the option.
        r : float
            Risk-free interest rate.
        t : float
            Time to maturity in years.
        sigma : Union[float, NDArray[np.float64]]
            Volatility of the underlying asset.
        q : float
            Dividend yield.
        b : float
            Cost of carry.
        cp0 : float
            Current price of the option.
        opt_type : Literal['call', 'put']
            Option style, either 'call' or 'put'.
        
        Returns
        -------
        float
        """
        if isinstance(sigma, (np.ndarray, list)):
            sigma = sigma.item() if sigma.size == 1 else float(sigma[0])

        result = np.power(
            self.general_opt_price(
                s=s, k=k, r=r, t=t, sigma=sigma, q=q, b=b, opt_type=opt_type) - cp0, 
            2
        )

        return float(result)

    def implied_volatility(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        cp0: float, 
        opt_type: Literal['call', 'put'], 
        method: Literal['newton_raphson', 'bisection', 'fsolve', 'scipy_optimize_minimize', 
                        'differential_evolution'] = "fsolve", 
        tolerance: float = 1e-3, 
        epsilon: float = 1, 
        max_iter: int = 1000, 
        orig_vol: float = 0.5, 
        list_bounds: Optional[list[tuple[float, float]]] = None
    ) -> tuple[float, bool]:
        """Calculate the implied volatility of a European option using various methods.
        
        Parameters
        ----------
        s : float
            Spot price of the underlying asset.
        k : float
            Strike price of the option.
        r : float
            Risk-free interest rate.
        t : float
            Time to maturity in years.
        sigma : float
            Volatility of the underlying asset.
        q : float
            Dividend yield.
        b : float
            Cost of carry.
        cp0 : float
            Current price of the option.
        opt_type : Literal['call', 'put']
            Option type, either 'call' or 'put'.
        method : Literal['newton_raphson', 'bisection', 'fsolve', 'scipy_optimize_minimize', 'differential_evolution']
            Method to use for calculating the implied volatility, by default 'fsolve'.
            - 'newton_raphson': Newton-Raphson method.
            - 'bisection': Bisection method.
            - 'fsolve': fsolve method.
            - 'scipy_optimize_minimize': scipy.optimize.minimize method.
            - 'differential_evolution': differential_evolution method.
        tolerance : float
            Tolerance for the error, by default 1e-3.
        epsilon : float
            Initial guess for the implied volatility, by default 1.
        max_iter : int
            Maximum number of iterations, by default 1000.
        orig_vol : float
            Original volatility, by default 0.5.
        list_bounds : Optional[list[tuple[float, float]]]
            List of bounds for the implied volatility, by default [(0, 2)].
            
        Returns
        -------
        tuple[float, bool]
            Implied volatility and a boolean indicating if the maximum number of iterations was 
            hit.
        
        Raises
        ------
        ValueError
            If the method to return the root of the non-linear equation is not recognized.
        
        References
        ----------
        https://www.youtube.com/watch?v=Jpy3iCsijIU,
        https://www.option-price.com/documentation.php#impliedvolatility
        """
        if list_bounds is None:
            list_bounds = [(0, 2)]
        count = 0
        if method == "newton_raphson":
            # iterating until the error is meaningless
            while epsilon > tolerance:
                # passing parameters
                count += 1
                flag_max_iter_hitten = False
                if count == 1:
                    imp_vol = orig_vol
                # preventing infinite loop
                if count >= max_iter:
                    flag_max_iter_hitten = True
                    break
                # calculating the difference between call prize, by the implied vol and call price
                dif_calc_market = self.general_opt_price(s, k, r, t, imp_vol, q, b, opt_type) \
                    - cp0
                # newthon-hampson-model to check whether the zero of the function has been spoted
                #   working with a tolerance to assume the zero of the function has been found
                try:
                    imp_vol = -dif_calc_market / \
                        self.vega(s, k, r, t, imp_vol, q, b) + imp_vol
                except RuntimeWarning:
                    return imp_vol, flag_max_iter_hitten
                epsilon = abs((imp_vol - orig_vol) / imp_vol)
            # returning implied volatility and maximum iterations hitten
            return imp_vol, flag_max_iter_hitten
        elif method == "bisection":
            float_high = 5
            float_low = 0
            while (float_high - float_low) > epsilon:
                if self.general_opt_price(s, k, r, t, float(float_high + float_low) / 2.0, q,
                                          b, opt_type) > cp0:
                    float_high = float(float_high + float_low) / 2.0
                else:
                    float_low = float(float_high + float_low) / 2.0
            return (float_high + float_low) / 2, False
        elif method == "fsolve":
            func = \
                lambda sigma: self.func_non_linear(s, k, r, t, sigma, q, b, cp0, opt_type) # noqa: E731 - do not assign a `lambda` expression, use a def
            result = fsolve(func, orig_vol)
            imp_vol = result.item() if isinstance(result, np.ndarray) else \
                result[0] if isinstance(result, (list, tuple)) else float(result)
            return imp_vol, False
        elif method == "scipy_optimize_minimize":
            return minimize(
                self.func_non_linear(s, k, r, t, sigma, q, b, cp0, opt_type), 
                orig_vol, 
                method="CG"
            ), False
        elif method == "differential_evolution":
            return NonLinearEquations().differential_evolution(
                self.func_non_linear(s, k, r, t, sigma, q, b, cp0, opt_type), 
                list_bounds
            ), False
        else:
            raise ValueError("Method to return the root of the non-linear equation is not "
                            + "recognized, please revisit the parameter")

    def moneyness(self, s: float, k: float, r: float, t: float, sigma: float, q: float) -> float:
        """Moneyness of the option, measures how far the option is from being at-the-money.
        
        Parameters
        ----------
        s : float
            Spot price of the underlying asset.
        k : float
            Strike price of the option.
        r : float
            Risk-free interest rate.
        t : float
            Time to maturity in years.
        sigma : float
            Volatility of the underlying asset.
        q : float
            Dividend yield of the underlying asset.

        Returns
        -------
        float
            Moneyness of the option.
            
        References
        ----------
        Mercado de Opções, Conceitos e Estratégias / Author: Luiz Maurício da Silva /
            Pgs. 74, 75, 76, 77, 78
        """
        return (self.d1(s, k, r, t, sigma, q)
                + self.d2(s, k, r, t, sigma, q)) / 2

    def iaotm(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float,
        opt_type: Literal['call', 'put'],
        pct_moneyness_atm: float = 0.05
    ) -> str:
        """Determine whether the option is ITM, ATM, or OTM based on moneyness.
        
        Parameters
        ----------
        s : float
            Spot price of the underlying asset.
        k : float
            Strike price of the option.
        r : float
            Risk-free interest rate.
        t : float
            Time to maturity in years.
        sigma : float
            Volatility of the underlying asset.
        q : float
            Dividend yield of the underlying asset.
        opt_type : Literal['call', 'put']
            Option type, either 'call' or 'put'.
        pct_moneyness_atm : float
            Percentage of the moneyness that is considered ATM, by default 0.05.

        Returns
        -------
        str
            ITM/ATM/OTM
            
        Raises
        ------
        ValueError
            If the inputs do not return appropriate values.
        """
        if abs(self.moneyness(s, k, r, t, sigma, q)) < pct_moneyness_atm:
            return "ATM"
        elif (self.moneyness(s, k, r, t, sigma, q) < pct_moneyness_atm and
              opt_type == "call") or (self.moneyness(s, k, r, t, sigma, q) >
                                       pct_moneyness_atm and opt_type == "put"):
            return "OTM"
        elif (self.moneyness(s, k, r, t, sigma, q) > pct_moneyness_atm and
              opt_type == "call") or (self.moneyness(s, k, r, t, sigma, q) <
                                       pct_moneyness_atm and opt_type == "put"):
            return "ITM"
        else:
            raise ValueError(
                "Please revisit your inputs, request did not return appropriate values")