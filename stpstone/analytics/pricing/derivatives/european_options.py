"""European options pricing models."""

from functools import lru_cache
from math import pi
from typing import Literal, Optional

import numpy as np
from scipy.optimize import fsolve, minimize

from stpstone.analytics.quant.prob_distributions import NormalDistribution
from stpstone.analytics.quant.regression import NonLinearEquations
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class InitialSettings(metaclass=TypeChecker):
    """Initial settings for European options pricing models.

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
    opt_type : str
        option type ('call' or 'put')
    """

    def set_parameters(self, *params, opt_type='call'):
        """Set parameters.
        
        Parameters
        ----------
        params : list
            list of parameters
        opt_type : str
            option type ('call' or 'put')
        """
        if opt_type not in ['call', 'put']:
            raise Exception('Option ought be a call or a put')        
        list_params = [param[0] if isinstance(param, list) else param for
                       param in params]
        return [float(param) for param in list_params if isinstance(param, str) == False]


class BlackScholesMerton(InitialSettings):
    """Black Scholes Merton Model.
    
    References
    ----------
    https://brilliant.org/wiki/black-scholes-merton/
    """

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
        s, k, b, t, sigma, q = self.set_parameters(s, k, b, t, sigma, q)
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
        s, k, b, t, sigma, q = self.set_parameters(s, k, b, t, sigma, q)
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
        opt_type: Literal["call", "put"]
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
        opt_type : str
            option type ('call' or 'put')

        Returns
        -------
        float
            option price
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        if opt_type == 'call':
            return s * np.exp((b - r) * t) * \
                NormalDistribution().cdf(self.d1(s, k, b, t, sigma, q)) \
                - k * np.exp(-r * t) * NormalDistribution().cdf(BlackScholesMerton(
                ).d2(s, k, b, t, sigma, q))
        elif opt_type == 'put':
            return k * np.exp(-r * t) * NormalDistribution().cdf(
                -self.d2(s, k, b, t, sigma, q)) - s * np.exp((b - r) * t) * NormalDistribution().cdf(
                -self.d1(s, k, b, t, sigma, q))


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
        opt_type: Literal["call", "put"]
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
        opt_type : str
            option type ('call' or 'put')

        Returns
        -------
        float
            delta
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        if opt_type == 'call':
            return np.exp((b - r) * t) * NormalDistribution().cdf(self.d1(
                s, k, b, t, sigma, q))
        elif opt_type == 'put':
            return np.exp((b - r) * t) * (NormalDistribution().cdf(
                self.d1(s, k, b, t, sigma, q)) - 1)

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
        opt_type: Literal["call", "put"]
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
        opt_type : str
            option type ('call' or 'put')

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        s, r, t, sigma, q, b = self.set_parameters(
            s, r, t, sigma, q, b, opt_type)
        if opt_type == 'call':
            return s * np.exp(-NormalDistribution().inv_cdf(delta * np.exp((r - b) * t))
                              * sigma * t ** 0.5 + (b + sigma ** 2 / 2.0) * t)
        elif opt_type == 'put':
            return s * np.exp(NormalDistribution().inv_cdf(delta * np.exp((r - b) * t))
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
        s, k, r, t, sigma, q, b = self.set_parameters(s, k, r, t, sigma, q, b)
        return NormalDistribution().pdf(self.d1(s, k, b, t, sigma, q)) * np.exp((b - r) * t) \
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
        k, r, sigma, q, b = self.set_parameters(k, r, sigma, q, b)
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
        s, k, r, t, sigma, q, b = self.set_parameters(s, k, r, t, sigma, q, b)
        return NormalDistribution().pdf(self.d1(s, k, b, t, sigma, q)) * np.exp((b - r) * t) \
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
        opt_type: Literal["call", "put"]
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
        opt_type : str
            option type ('call' or 'put')

        Returns
        -------
        float
            theta
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        if opt_type == 'call':
            return -(s * np.exp((b - r) * t) * NormalDistribution().pdf(self.d1(
                s, k, b, t, sigma, q)) * sigma) / (2.0 * t ** 0.5) - (b - r) * s * np.exp(
                (b - r) * t) * NormalDistribution().cdf(self.d1(s, k, b, t, sigma, q)) \
                - r * k * \
                np.exp(-r * t) * \
                NormalDistribution().cdf(self.d2(s, k, b, t, sigma, q))
        elif opt_type == 'put':
            return -(s * np.exp((b - r) * t) * NormalDistribution().pdf(self.d1(
                s, k, b, t, sigma, q)) * sigma) / (2.0 * t ** 0.5) + (b - r) * s * np.exp(
                (b - r) * t) * NormalDistribution().cdf(-self.d1(s, k, b, t, sigma, q)) \
                + r * k * \
                np.exp(-r * t) * \
                NormalDistribution().cdf(-self.d2(s, k, b, t, sigma, q))

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
        s, k, r, t, sigma, q, b = self.set_parameters(s, k, r, t, sigma, q, b)
        return s * np.exp((b - r) * t) * NormalDistribution().pdf(self.d1(
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
        k, t, sigma, b = self.set_parameters(k, t, sigma, b)
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
        s, t, sigma, b = self.set_parameters(s, t, sigma, b)
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
        s, k, r, sigma, b = self.set_parameters(s, k, r, sigma, b)
        return 2 * (1.0 + (1.0 + (8.0 * r / sigma ** 2 + 1.0) * np.log(s / k) ** 2) ** 0.5) \
            / (8.0 * r + sigma ** 2)

    def vega_global_maximum(self, k: float, r: float, sigma: float, b: float) -> dict[str, float]:
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
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        k, r, sigma, b = self.set_parameters(k, r, sigma, b)
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
        s, k, r, t, sigma, q, b = self.set_parameters(s, k, r, t, sigma, q, b)
        return self.gamma(s, k, r, t, sigma, q, b) * sigma * s ** 2 * t

    def vega_delta_relationship(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal["call", "put"]
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
        opt_type : str
            option type ('call' or 'put')

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug"""
        s, k, r, t, sigma, q, b, opt_type = self.set_parameters(s, k, r, t, sigma, q, b,
                                                                 opt_type)
        return np.exp((b - r) * t) * NormalDistribution().pdf(NormalDistribution().inv_cdf(
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
        s, k, r, t, sigma, q, b = self.set_parameters(s, k, r, t, sigma, q, b)
        return sigma / 10.0 * s * np.exp((b - r) * t) * NormalDistribution().pdf(
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
        opt_type: Literal["call", "put"]
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
        opt_type : str
            option type ('call' or 'put')

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        s, k, r, t, sigma, q, b, opt_type = self.set_parameters(s, k, r, t, sigma, q, b,
                                                                 opt_type)
        return self.vega(s, k, r, t, sigma, q, b) * sigma \
            / self.general_opt_price(s, k, r, t, sigma, q, b, opt_type)

    def rho(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal["call", "put"]
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
        opt_type : str
            option type ('call' or 'put')

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        if opt_type == 'call':
            return t * k * np.exp(- r * t) * NormalDistribution().cdf(
                self.d2(s, k, b, t, sigma, q))
        elif opt_type == 'put':
            return -t * k * np.exp(- r * t) * NormalDistribution().cdf(
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
        opt_type: Literal["call", "put"]
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
        opt_type : str
            option type ('call' or 'put')

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        return self.delta(s, k, r, t, sigma, q, opt_type) * s / \
            self.general_opt_price(s, k, r, t, sigma, q, b, opt_type)

    def vanna(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        opt_type: Literal["call", "put"]
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
        opt_type : str
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
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        return -np.exp((b - r) * t) * self.d2(s, k, b, t, sigma, q) * NormalDistribution().pdf(
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
        opt_type: Literal["call", "put"]
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
        opt_type : str
            option type ('call' or 'put')

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        return self.vanna(s, k, r, t, sigma, q, b, opt_type) * (1.0 / sigma) \
            * (self.d1(s, k, b, t, sigma, q) * self.d2(s, k, b, t, sigma, q)
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
        opt_type: Literal["call", "put"]
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
        opt_type : str
            option type ('call' or 'put')

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        if opt_type == 'call':
            return -np.exp((b - r) * t) * (NormalDistribution().pdf(
                self.d1(s, k, b, t, sigma, q)) * (b / (sigma * t ** 0.5) - self.d2(
                    s, k, b, t, sigma, q) / (2.0 * t)) + (b - r) * NormalDistribution().cdf(
                        self.d1(s, k, b, t, sigma, q)))
        elif opt_type == 'put':
            return -np.exp((b - r) * t) * (NormalDistribution().pdf(
                self.d1(s, k, b, t, sigma, q)) * (b / (sigma * t ** 0.5) - self.d2(
                    s, k, b, t, sigma, q) / (2.0 * t)) - (b - r) * NormalDistribution().cdf(
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
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
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
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        return self.zomma(s, k, r, t, sigma, q, b) * s / 100

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
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        return -self.gamma(s, k, r, t, sigma, q, b) * (1.0 + self.d1(s, k, b, t, sigma, q)
                                                       / (sigma * t ** 0.5)) / s

    def speed_p(self, s: float, k: float, r: float, t: float, sigma: float, q: float, b: float) \
        -> float:
        """Speed percentage 
        
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
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        return self.speed(s, k, r, t, sigma, q, b) * s / 100

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
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        return -self.gamma(s, k, r, t, sigma, q, b) * (r - b + b * self.d1(
            s, k, b, t, sigma, q) / (sigma * t ** 0.5) + (1.0 - self.d1(
                s, k, b, t, sigma, q) * self.d2(s, k, b, t, sigma, q)) / (2.0 * t))

    def color_p(self, s: float, k: float, r: float, t: float, sigma: float, q: float, b: float) \
        -> float:
        """"Color percentage.
         
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
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        return -self.gamma_p(s, k, r, t, sigma, q, b) \
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
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        return self.vega(s, k, r, t, sigma, q, b) * self.d1(s, k, b, t, sigma, q) \
            * self.d2(s, k, b, t, sigma, q) / sigma

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
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        return self.vega_p(s, k, r, t, sigma, q, b) * self.d1(s, k, b, t, sigma, q) \
            * self.d2(s, k, b, t, sigma, q) / sigma

    def vomma_positive_outside_interval(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        q: float, 
        b: float, 
        bl_spot: bool = True
    ) -> dict[str, float]:
        """Vomma positive outside interval.

        Parameters
        ----------
        s_k : float
            spot or strike value
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
        bl_spot : bool, optional
            spot or strike value, by default True

        Returns
        -------
        dict
            lower boundary and upper boundary

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        # sign of cost of carry, according to s_k being a spot or strike value
        if bl_spot:
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
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        return self.vomma(s, k, r, t, sigma, q, b) / sigma \
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
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        return self.vega(s, k, r, t, sigma, q, b) * (r - b + b * self.d1(
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
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        return self.vega(s, k, r, t, sigma, q, b) / (2 * sigma)

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
        """Variance vann. 
        
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
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
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
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
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
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
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
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        return s * NormalDistribution().pdf((np.log(s / k) + t * sigma ** 2 / 2.0)
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
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        return -s * NormalDistribution().pdf(self.d1(s, k, b, t, sigma, q)) * sigma / (
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
        s, k, r, t, sigma, q, b = self.set_parameters(s, k, r, t, sigma, q, b)
        return -self.gamma(s, k, r, t, sigma, q, b) * sigma / (2.0 * t)

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
        s, k, r, t, sigma, q, b = self.set_parameters(s, k, r, t, sigma, q, b)
        return self.theta(s, k, r, t, sigma, q, b) / self.vega(s, k, r, t, sigma, q, b)

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
        s, k, r, t, sigma, q, b = self.set_parameters(s, k, r, t, sigma, q, b)
        return -2.0 * self.driftless_theta(s, k, r, t, sigma, q, b) / (
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
        opt_type: Literal["call", "put"]
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
        opt_type : str
            option type

        Returns
        -------
        float
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        if opt_type == 'call':
            return -t * s * np.exp((b - r) * t) * NormalDistribution().cdf(self.d1(
                s, k, b, t, sigma, q))
        elif opt_type == 'put':
            return t * s * np.exp((b - r) * t) * NormalDistribution().cdf(-self.d1(
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
        opt_type: Literal["call", "put"]
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
        opt_type : str
            option type

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        if opt_type == 'call':
            return t * s * np.exp((b - r) * t) * NormalDistribution().cdf(self.d1(
                s, k, b, t, sigma, q))
        elif opt_type == 'put':
            return -t * s * np.exp((b - r) * t) * NormalDistribution().cdf(-self.d1(
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
        opt_type: Literal["call", "put"]
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
        opt_type : str
            option type

        Returns
        -------
        float

        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        if opt_type == 'call':
            return NormalDistribution().cdf(self.d2(s, k, b, t, sigma, q))
        elif opt_type == 'put':
            return NormalDistribution().cdf(-self.d2(s, k, b, t, sigma, q))

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
        opt_type: Literal["call", "put"]
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
        opt_type : str
            option type

        Returns
        -------
        float
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        s, k, r, t, sigma, q, b, p = self.set_parameters(
            s, k, r, t, sigma, q, b, p, opt_type)
        if opt_type == 'call':
            return s * np.exp(-NormalDistribution().inv_cdf(p) * sigma * t ** 0.5
                              + (b - sigma ** 2 / 2.0) * t)
        elif opt_type == 'call':
            return s * np.exp(NormalDistribution().inv_cdf(p) * sigma * t ** 0.5
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
        opt_type: Literal["call", "put"]
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
        opt_type : str
            option type

        Returns
        -------
        float
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        if opt_type == 'call':
            return -NormalDistribution().pdf(self.d2(s, k, b, t, sigma, q)) * self.d1(
                s, k, b, t, sigma, q) / sigma
        elif opt_type == 'put':
            return NormalDistribution().pdf(self.d2(s, k, b, t, sigma, q)) * self.d1(
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
        opt_type: Literal["call", "put"]
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
        opt_type : str
            option type

        Returns
        -------
        float
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
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
        opt_type: Literal["call", "put"]
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
        opt_type : str
            option type

        Returns
        -------
        float
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        return NormalDistribution().pdf(self.d2(s, k, b, t, sigma, q)) * np.exp(-r * t) / (
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
        opt_type: Literal["call", "put"]
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
        opt_type : str
            option type (call or put)

        Returns
        -------
        float
        
        References
        ----------
        The Complete Guide To Option Pricing Formulas Espen Gaarder Haug
        """
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        # defining greek parameters
        mu = (b - sigma ** 2 / 2.0) / sigma ** 2
        lambda_ = (mu ** 2 + 2 * r / sigma ** 2) ** 0.5
        z = np.log(k / s) / (sigma * t ** 0.5) + lambda_ * sigma * t ** 0.5
        # return greek
        if opt_type == 'call':
            return (k / s) ** (mu + lambda_) * NormalDistribution().cdf(-z) \
                + (k / s) ** (mu - lambda_) * NormalDistribution().cdf(
                -z + 2 * lambda_ * sigma * t ** 0.5)
        elif opt_type == 'put':
            return (k / s) ** (mu + lambda_) * NormalDistribution().cdf(z) \
                + (k / s) ** (mu - lambda_) * NormalDistribution().cdf(
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
        psi_r = self.set_parameters(psi_r)
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
        opt_type: Literal["call", "put"], 
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
        opt_style : str
            Option style, either 'call' or 'put'.
        h_upper : float, optional
        h_lower : float, optional

        Returns
        -------
        float
        
        References
        ----------
        https://www.youtube.com/watch?v=a3906k9C0fM,
        https://www.youtube.com/watch?v=WxrRi9lNnqY
        """
        s, k, r, t, n, u, d = self.set_parameters(
            s, k, r, t, n, u, d, opt_type)
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
        if h_upper != None:
            array_cp[array_s_nodes >= h_upper] = 0
        if h_lower != None:
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
        opt_type: Literal["call", "put"]
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
        opt_style : str
            Option style, either 'call' or 'put'.

        Returns
        -------
        float
        
        References
        ----------
        https://www.youtube.com/watch?v=nWslah9tHLk,
        https://quantpy.com.au/binomial-tree-model/binomial-asset-pricing-model-choosing-
        parameters/
        """
        s, k, r, t, n, sigma = self.set_parameters(
            s, k, r, t, n, sigma, opt_type)
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
        for j in range(0, int(n) + 1):
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
        opt_type: Literal["call", "put"]
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
        opt_style : str
            Option style, either 'call' or 'put'.

        Returns
        -------
        float
        
        References
        ----------
        https://www.youtube.com/watch?v=nWslah9tHLk,
        https://quantpy.com.au/binomial-tree-model/binomial-asset-pricing-model-choosing-
        parameters/
        """
        s, k, r, t, n, sigma = self.set_parameters(
            s, k, r, t, n, sigma, opt_type)
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
        for j in range(0, int(n) + 1):
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
        opt_type: Literal["call", "put"]
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
        opt_style : str
            Option style, either 'call' or 'put'.

        Returns
        -------
        float
        
        References
        ----------
        https://www.youtube.com/watch?v=nWslah9tHLk,
        https://quantpy.com.au/binomial-tree-model/binomial-asset-pricing-model-choosing-
        parameters/
        """
        s, k, r, t, n, sigma = self.set_parameters(
            s, k, r, t, n, sigma, opt_type)
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
        for j in range(0, int(n) + 1):
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
        opt_type: Literal["call", "put"]
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
        opt_style : str
            Option style, either 'call' or 'put'.

        Returns
        -------
        float
        
        References
        ----------
        https://www.youtube.com/watch?v=nWslah9tHLk,
        https://quantpy.com.au/binomial-tree-model/binomial-asset-pricing-model-choosing-
        parameters/
        """
        s, k, r, t, n, sigma = self.set_parameters(
            s, k, r, t, n, sigma, opt_type)
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
        for j in range(0, int(n) + 1):
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

    @lru_cache()
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
        opt_type: Literal["call", "put"], 
        method: Literal["newton_raphson", "bisection", "fsolve", "scipy_optimize_minimize", 
                        "differential_evolution"] = "fsolve", 
        tolerance: float = 1E-3, 
        epsilon: float = 1, 
        max_iter: int = 1000, 
        orig_vol: float = 0.5, 
        list_bounds: list[tuple[float, float]] = [(0, 2)]
    ) -> tuple[float, bool]:
        """Calculates the implied volatility of a European option using various methods.
        
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
        opt_type : str
            Option type, either 'call' or 'put'.
        method : str, optional
            Method to use for calculating the implied volatility, by default 'fsolve'.
            - 'newton_raphson': Newton-Raphson method.
            - 'bisection': Bisection method.
            - 'fsolve': fsolve method.
            - 'scipy_optimize_minimize': scipy.optimize.minimize method.
            - 'differential_evolution': differential_evolution method.
        tolerance : float, optional
            Tolerance for the error, by default 1E-3.
        epsilon : float, optional
            Initial guess for the implied volatility, by default 1.
        max_iter : int, optional
            Maximum number of iterations, by default 1000.
        orig_vol : float, optional
            Original volatility, by default 0.5.
        list_bounds : list[tuple[float, float]], optional
            List of bounds for the implied volatility, by default [(0, 2)].
            
        Returns
        -------
        tuple[float, bool]
            Implied volatility and a boolean indicating if the maximum number of iterations was 
            hit.
        
        Raises
        ------
        Exception
            If the method to return the root of the non-linear equation is not recognized.
        
        References
        ----------
        https://www.youtube.com/watch?v=Jpy3iCsijIU,
        https://www.option-price.com/documentation.php#impliedvolatility
        """
        s, k, r, t, sigma, q, b, cp0 = self.set_parameters(
            s, k, r, t, sigma, q, b, cp0, opt_type)
        count = 0
        if method == 'newton_raphson':
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
        elif method == 'bisection':
            float_high = 5
            float_low = 0
            while (float_high - float_low) > epsilon:
                if self.general_opt_price(s, k, r, t, float(float_high + float_low) / 2.0, q,
                                          b, opt_type) > cp0:
                    float_high = float(float_high + float_low) / 2.0
                else:
                    float_low = float(float_high + float_low) / 2.0
            return (float_high + float_low) / 2, False
        elif method == 'fsolve':
            def func_non_linear(sigma: float) -> float: return np.power(
                self.general_opt_price(s, k, r, t, sigma, q, b, opt_type) - cp0, 2)
            return fsolve(func_non_linear, orig_vol), False
        elif method == 'scipy_optimize_minimize':
            def func_non_linear(sigma: float) -> float: return np.power(
                self.general_opt_price(s, k, r, t, sigma, q, b, opt_type) - cp0, 2)
            return minimize(func_non_linear, orig_vol, method='CG'), False
        elif method == 'differential_evolution':
            def func_non_linear(sigma: float) -> float: return np.power(
                self.general_opt_price(s, k, r, t, sigma, q, b, opt_type) - cp0, 2)
            return NonLinearEquations().differential_evolution(func_non_linear, list_bounds), False
        else:
            raise Exception('Method to return the root of the non-linear equation is not '
                            + 'recognized, please revisit the parameter')

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
        s, k, r, t, sigma, q = self.set_parameters(s, k, r, t, sigma, q)
        return (self.d1(s, k, r, t, sigma, q)
                + self.d2(s, k, r, t, sigma, q)) / 2

    def iaotm(
        self, 
        s: float, 
        k: float, 
        r: float, 
        t: float, 
        sigma: float, 
        opt_type: float, 
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
        opt_type : float
            Option type, either 'call' or 'put'.
        pct_moneyness_atm : float, optional
            Percentage of the moneyness that is considered ATM, by default 0.05.

        Returns
        -------
        str
            ITM/ATM/OTM
            
        Raises
        ------
        Exception
            If the inputs do not return appropriate values.
        """
        s, k, r, t, sigma = self.set_parameters(
            s, k, r, t, sigma, opt_type)
        if abs(self.moneyness(s, k, r, t, sigma)) < pct_moneyness_atm:
            return 'ATM'
        elif (self.moneyness(s, k, r, t, sigma) < pct_moneyness_atm and
              opt_type == 'call') or (self.moneyness(s, k, r, t, sigma) >
                                       pct_moneyness_atm and opt_type == 'put'):
            return 'OTM'
        elif (self.moneyness(s, k, r, t, sigma) > pct_moneyness_atm and
              opt_type == 'call') or (self.moneyness(s, k, r, t, sigma) <
                                       pct_moneyness_atm and opt_type == 'put'):
            return 'ITM'
        else:
            raise Exception(
                'Please revisit your inputs, request did not return appropriate values')