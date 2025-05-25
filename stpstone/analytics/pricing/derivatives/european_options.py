# OPTION PRCING FORMULAS, FOR EUROPEAN TYPE

import numpy as np
from math import pi
from scipy.optimize import fsolve, minimize
from functools import lru_cache
from stpstone.analytics.quant.prob_distributions import NormalDistribution
from stpstone.analytics.quant.regression import  NonLinearEquations


class InitialSettings:

    def set_parameters(self, *params, opt_type='call'):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # check wheter is a call or put option, in case the type is neither of the former raise
        #   error
        if opt_type not in ['call', 'put']:
            raise Exception('Option ought be a call or a put')
        # initial parameters
        list_params = [param[0] if isinstance(param, list) == True else param for
                       param in params]
        # return parameters
        return [float(param) for param in list_params if isinstance(param, str) == False]


class BlackScholesMerton(InitialSettings):
    """
    REFERENCES: https://brilliant.org/wiki/black-scholes-merton/
    """

    def d1(self, s, k, b, t, sigma, q):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: D1 OF UNDERLYING OPTION
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY), B (COST OF CARRY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET) AND Q (DIVIDEND YIELD)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, b, t, sigma, q = self.set_parameters(s, k, b, t, sigma, q)
        # return d1 probability
        return (np.log(s / k) + (b + sigma ** 2 / 2) * t) / (sigma * np.sqrt(t))

    def d2(self, s, k, b, t, sigma, q):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: D2 OF UNDERLYING OPTION (MONEYNESS)
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY), B (COST OF CARRY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET) AND Q (DIVIDEND YIELD)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, b, t, sigma, q = self.set_parameters(s, k, b, t, sigma, q)
        # return d2 probability
        return self.d1(s, k, b, t, sigma, q) - sigma * np.sqrt(t)

    def general_opt_price(self, s, k, r, t, sigma, q, b, opt_type):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: CALL/PUT PRICE OF AN UNDERLYING ASSET
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE),
            T (TIME TO MATURITY), SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD),
            B (COST OF CARRY - R FOR STOCK OPTION, R - Q FOR STOCK
            OPTION WITH CONTINUOUS DIVIDEND YIELD, 0 FOR FUTURES, 0 AND R 0 FOR MARGINED FUTURES
            OPTIONS, AND R - RF FOR CURRENCY OPTION MODEL) AND OPTION STYLE (CALL/PUT)
        OUTPUTS: CALL PRICE
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        # return option price
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
    """
    REFERENCES: https://www.macroption.com/option-greeks-excel/, https://en.wikipedia.org/wiki/Greeks_(finance)
    INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY), R (INTEREST RATE),
        SIGMA (VOLATILITY OF UNDERLYING ASSET) AND Q (DIVIDEND YIELD)
    """

    def delta(self, s, k, r, t, sigma, q, b, opt_type):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: RATE OF CHANGE OF THE THEORETICAL OPTION VALUE WITH RESPECT TO CHANGES IN THE
            UNDERLYING ASSET'S PRICE - FIRST DERIVATIVE OF THE OPTION VALUE WITH RESPECT TO THE
            UNDERLYING INSTRUMENT'S PRICE S
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD) B (COST OF CARRY)
            AND OPTION STYLE (CALL/PUT)
        OUTPUTS:
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        # return greek
        if opt_type == 'call':
            return np.exp((b - r) * t) * NormalDistribution().cdf(self.d1(
                s, k, b, t, sigma, q))
        elif opt_type == 'put':
            return np.exp((b - r) * t) * (NormalDistribution().cdf(
                self.d1(s, k, b, t, sigma, q)) - 1)

    def future_delta_from_spot_delta(self, delta, b, t):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: IN SOME MARKETS IT IS OPTIONAL TO HEDGE WITH THE STOCK ITSELF OR, ALTERNATIVELY,
            HEDGE WITH THE STOCK FUTURES - IN THE CASE WHERE ONE HEDGE WITH A FORWARD CONTRACT WITH
            THE SAME EXPIRATION AS THE OPTION THE FORMULA ALSO HOLDS TRUE - THIS IS PARTICULARLY
            USEFUL IN THE FX MARKET, WHERE TYPICALLY CAN BE CHOOSEN BETWEEN HEDGING WITH THE
            CURRENCY SPOT OR ALTERNATIVELY A FORWARD WITH EXPIRATION MATCHING THE OPTION EXPIRATION
        INPUTS:
        OUTPUTS:
        """
        return delta * np.exp(-b * t)

    def strike_from_delta(self, s, r, t, sigma, q, b, delta, opt_type):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: RATE OF CHANGE OF THE THEORETICAL OPTION VALUE WITH RESPECT TO CHANGES IN THE
            UNDERLYING ASSET'S PRICE - FIRST DERIVATIVE OF THE OPTION VALUE WITH RESPECT TO THE
            UNDERLYING INSTRUMENT'S PRICE S
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY)
            AND OPTION STYLE (CALL/PUT)
        OUTPUTS:
        """
        # initial parameters
        s, r, t, sigma, q, b = self.set_parameters(
            s, r, t, sigma, q, b, opt_type)
        # return greek
        if opt_type == 'call':
            return s * np.exp(-NormalDistribution().inv_cdf(delta * np.exp((r - b) * t))
                              * sigma * t ** 0.5 + (b + sigma ** 2 / 2.0) * t)
        elif opt_type == 'put':
            return s * np.exp(NormalDistribution().inv_cdf(delta * np.exp((r - b) * t))
                              * sigma * t ** 0.5 + (b + sigma ** 2 / 2.0) * t)

    def gamma(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: DELTA SENSITIVITY TO SMALL CHANGES IN THE UNDERLYING PRICE
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(s, k, r, t, sigma, q, b)
        # return greek
        return NormalDistribution().pdf(self.d1(s, k, b, t, sigma, q)) * np.exp((b - r) * t) \
            / (s * sigma * t ** 0.5)

    def saddle_gamma(self, k, r, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: CRITICAL POINT OF GAMMA - LOWER BOUNDARY
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY)
        OUTPUTS: FLOAT
        """
        # initial parameters
        k, r, sigma, q, b = self.set_parameters(k, r, sigma, q, b)
        # return greek
        return np.sqrt((np.exp(1) / pi) * ((2 * b - r) / sigma ** 2 + 1)) / k

    def gamma_p(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: PERCENTAGE CHAGENS IN DELTA FOR PERCENTAGE CHANGES IN THE UNDERLYING (GAMMA
            PERCENT)
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(s, k, r, t, sigma, q, b)
        # return greek
        return NormalDistribution().pdf(self.d1(s, k, b, t, sigma, q)) * np.exp((b - r) * t) \
            / (100.0 * sigma * t ** 0.5)

    def theta(self, s, k, r, t, sigma, q, b, opt_type):
        """
        DOCSTRING: SENSITIVITY  MEASUREMENT OF TIME  BEFORE EXPIRATION DATE (IT WILL LOSE VALUE
            PRICED INTO THE EXTRINSIC VALUE OVER TIME) - GAUGE HOW MUCH VALUE AN OPTION LOSES ON
            A DAILY BASIS
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), OPTION CALL OR PUT AND T
            (NUMBER OF DAY PER YEAR, GENERALLY 365 OR 252)
        OUTPUTS:
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        # return greek
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

    def vega(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: SENSITIVITY MEASUREMENT OF VOLATILITY OVER TIME
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD)
        OUTPUTS: VEGA
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(s, k, r, t, sigma, q, b)
        # return greek
        return s * np.exp((b - r) * t) * NormalDistribution().pdf(self.d1(
            s, k, b, t, sigma, q)) * np.sqrt(t)

    def vega_local_maximum(self, k, t, sigma, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: LOCAL MAXIMUM OF VEGA
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD)
        OUTPUTS: VEGA
        """
        # initial parameters
        k, t, sigma, b = self.set_parameters(k, t, sigma, b)
        # return greek
        return k * np.exp(-b + sigma ** 2 / 2.0) * t

    def strike_maximizes_vega(self, s, t, sigma, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: STRIKE THAT MAXIMIZES VEGA, GIVEN THE ASSET PRICE
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD)
        OUTPUTS: VEGA
        """
        # initial parameters
        s, t, sigma, b = self.set_parameters(s, t, sigma, b)
        # return greek
        return s * np.exp(b + sigma ** 2 / 2.0) * t

    def time_to_maturity_maximum_vega(self, s, k, r, sigma, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: TIME TO MATURITY WHEN VEGA IS THE GREATEST
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD)
        OUTPUTS: VEGA
        """
        # initial parameters
        s, k, r, sigma, b = self.set_parameters(s, k, r, sigma, b)
        # return greek
        return 2 * (1.0 + (1.0 + (8.0 * r / sigma ** 2 + 1.0) * np.log(s / k) ** 2) ** 0.5) \
            / (8.0 * r + sigma ** 2)

    def vega_global_maximum(self, k, r, sigma, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: TIME TO MATURITY WHEN VEGA IS THE GREATEST
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD)
        OUTPUTS: VEGA
        """
        # initial parameters
        k, r, sigma, b = self.set_parameters(k, r, sigma, b)
        # global maximum time for vega
        t = 1 / (2.0 * r)
        # stock price at the maximum time
        s = k * np.exp((-b + sigma ** 2 / 2.0) * t)
        # vega at t
        vega = k / (2.0 * (r * np.exp(1) * pi) ** 0.5)
        # return greek
        return {
            't_max_global_vega': t,
            's_max_global_vega': s,
            'vega_max_global': vega
        }

    def vega_gamma_relationship(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: RELATIONSHIP BETWEEN VEGA AND GAMMA - RETURNS VEGA
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD)
        OUTPUTS: FLOAT (VEGA)
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(s, k, r, t, sigma, q, b)
        # return greek
        return self.gamma(s, k, r, t, sigma, q, b) * sigma * s ** 2 * t

    def vega_delta_relationship(self, s, k, r, t, sigma, q, b, opt_type):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: RELATIONSHIP BETWEEN VEGA AND GAMMA - RETURNS VEGA
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD)
        OUTPUTS: FLOAT (VEGA)
        """
        # initial parameters
        s, k, r, t, sigma, q, b, opt_type = self.set_parameters(s, k, r, t, sigma, q, b,
                                                                 opt_type)
        # return greek
        return np.exp((b - r) * t) * NormalDistribution().pdf(NormalDistribution().inv_cdf(
            np.exp((r - b) * t) * np.abs(self.delta(s, k, r, t, sigma, q, b, opt_type)))) \
            / (s * sigma * t ** 0.5)

    def vega_p(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: VEGA PERCENTUAL CHANGE
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B(COST OF CARRY) AND
            OPTION STYLE (CALL/PUT)
        OUTPUTS: FLOAT (VEGA)
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(s, k, r, t, sigma, q, b)
        # return greek
        return sigma / 10.0 * s * np.exp((b - r) * t) * NormalDistribution().pdf(
            self.d1(s, k, b, t, sigma, q)) * t ** 0.5

    def vega_elasticity(self, s, k, r, t, sigma, q, b, opt_type):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: VEGA PERCENTUAL CHANGE
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B(COST OF CARRY) AND
            OPTION STYLE (CALL/PUT)
        OUTPUTS: FLOAT (VEGA)
        """
        # initial parameters
        s, k, r, t, sigma, q, b, opt_type = self.set_parameters(s, k, r, t, sigma, q, b,
                                                                 opt_type)
        # return greek
        return self.vega(s, k, r, t, sigma, q, b) * sigma \
            / self.general_opt_price(s, k, r, t, sigma, q, b, opt_type)

    def rho(self, s, k, r, t, sigma, q, b, opt_type):
        """
        DOCSTRING: OPTION'S SENSITIVITY TO SMALL CHANGES IN THE RISK-FREE INTEREST RATE
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), OPTION CALL OR PUT AND T
            (NUMBER OF DAY PER YEAR, GENERALLY 365 OR 252)
        OUTPUTS:
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        # return greek
        if opt_type == 'call':
            return t * k * np.exp(- r * t) * NormalDistribution().cdf(self.d2(s, k, b, t, sigma, q))
        elif opt_type == 'put':
            return -t * k * np.exp(- r * t) * NormalDistribution().cdf(-self.d2(s, k, b, t, sigma, q))

    def lambda_greek(self, s, k, r, t, sigma, q, b, opt_type):
        """
        DOCSTRING:
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD) OPTION CALL OR PUT
        OUTPUTS: LAMBDA
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        # return greek
        return self.delta(s, k, r, t, sigma, q, opt_type) * s / \
            self.general_opt_price(s, k, r, t, sigma, q, b, opt_type)

    def vanna(self, s, k, r, t, sigma, q, b, opt_type):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: RATE OF CHANGE OF VEGA ACCORDING TO SPOT, OR DELTA TO VOLATILITY (SIGMA)
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD) B (COST OF CARRY)
            AND OPTION STYLE (CALL/PUT)
        OUTPUTS:
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        # return greek
        return -np.exp((b - r) * t) * self.d2(s, k, b, t, sigma, q) * NormalDistribution().pdf(
            self.d1(s, k, b, t, sigma, q)) / sigma

    def vanna_vol(self, s, k, r, t, sigma, q, b, opt_type):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: RATE OF CHANGE OF VANNA ACCORDING TO VOLATILITY (SIGMA) (SECOND-ORDER PARTIAL
            DERIVATIVE OF DELTA WITH RESPECT TO VOLATILITY)
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD) B (COST OF CARRY)
            AND OPTION STYLE (CALL/PUT)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        # return greek
        return self.vanna(s, k, r, t, sigma, q, b, opt_type) * (1.0 / sigma) \
            * (self.d1(s, k, b, t, sigma, q) * self.d2(s, k, b, t, sigma, q)
                - self.d1(s, k, b, t, sigma, q) / self.d2(s, k, b, t, sigma, q) - 1.0)

    def charm(self, s, k, r, t, sigma, q, b, opt_type):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: RATE OF CHANGE OF DELTA TO TIME, ALSO KNOWN AS CHARM OR DELTA BLEED
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD) B (COST OF CARRY)
            AND OPTION STYLE (CALL/PUT)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        # return greek
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

    def zomma(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: RATE OF CHANGE OF GAMMA TO IMPLIED VOLATILITY, ALSO KNOWN AS ZOMMA
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY),
            GAMMA (OR GAMMA_P)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        # return greek
        return self.gamma(s, k, r, t, sigma, q, b) * (self.d1(s, k, b, t, sigma, q) * self.d2(
            s, k, b, t, sigma, q) - 1) / sigma

    def zomma_p(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: SPEED PERCENTAGE
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY),
            GAMMA (OR GAMMA_P)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        # return greek
        return self.zomma(s, k, r, t, sigma, q, b) * s / 100

    def speed(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: RATE OF CHANGE OF GAMMA TO SPOT PRICE, ALSO KNOWN AS SPEED
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY),
            GAMMA (OR GAMMA_P)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        # return greek
        return -self.gamma(s, k, r, t, sigma, q, b) * (1.0 + self.d1(s, k, b, t, sigma, q)
                                                       / (sigma * t ** 0.5)) / s

    def speed_p(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: SPEED PERCENTAGE
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY),
            GAMMA (OR GAMMA_P)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        # return greek
        return self.speed(s, k, r, t, sigma, q, b) * s / 100

    def color(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: RATE OF CHANGE OF GAMMA TO TIME, ALSO KNOWN AS COLOR
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY),
            GAMMA (OR GAMMA_P)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        # return greek
        return -self.gamma(s, k, r, t, sigma, q, b) * (r - b + b * self.d1(
            s, k, b, t, sigma, q) / (sigma * t ** 0.5) + (1.0 - self.d1(
                s, k, b, t, sigma, q) * self.d2(s, k, b, t, sigma, q)) / (2.0 * t))

    def color_p(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: RATE OF CHANGE OF GAMMA TO TIME, ALSO KNOWN AS COLOR
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY),
            GAMMA (OR GAMMA_P)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        # return greek
        return -self.gamma_p(s, k, r, t, sigma, q, b) \
            * (r - b + b * self.d1(s, k, b, t, sigma, q)
               / (sigma * t ** 0.5) + (1.0 - self.d1(s, k, b, t, sigma, q)
                                       * self.d2(s, k, b, t, sigma, q)) / (2.0 * t))

    def vomma(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: VEGA PERCENTUAL CONVEXITY, OR THE SENSITIVITY OF CHANGES IN IMPLIED VOLATILITY
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY),
            GAMMA (OR GAMMA_P)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        # return greek
        return self.vega(s, k, r, t, sigma, q, b) * self.d1(s, k, b, t, sigma, q) \
            * self.d2(s, k, b, t, sigma, q) / sigma

    def vomma_p(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: VEGA CONVEXITY, OR THE SENSITIVITY OF CHANGES IN IMPLIED VOLATILITY
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY),
            GAMMA (OR GAMMA_P)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        # return greek
        return self.vega_p(s, k, r, t, sigma, q, b) * self.d1(s, k, b, t, sigma, q) \
            * self.d2(s, k, b, t, sigma, q) / sigma

    def vomma_positive_outside_interval(self, s_k, r, t, sigma, q, b, bl_spot=True):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: INTERVAL BEYOND WHICH VOMMA BEGINS TO REGISTER POSITIVE VALUES -->
            INTERVAL TO SPOT OR STRIKE VALUES
        INPUTS: S (SPOT PRICE) OR K (STRIKE) (S_K), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY),
            GAMMA (OR GAMMA_P)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        # sign of cost of carry, according to s_k being a spot or strike value
        if bl_spot == True:
            sign_ = 1
        else:
            sign_ = -1
        # return vomma 0 boundaries
        return {
            'lower_boundary': s_k * np.exp((sign_ * b - sigma ** 2 / 2.0) * t),
            'upper_boundary': s_k * np.exp((sign_ * b + sigma ** 2 / 2.0) * t)
        }

    def ultima(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: VOMMA'S SENSITIVITY TO A CHANGE IN VOLATITLITY
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        # return greek
        return self.vomma(s, k, r, t, sigma, q, b) / sigma \
            * (self.d1(s, k, b, t, sigma, q) * self.d2(s, k, b, t, sigma, q)
               - self.d1(s, k, b, t, sigma, q) / self.d2(s, k, b, t, sigma, q)
                - self.d2(s, k, b, t, sigma, q) / self.d1(s, k, b, t, sigma, q) - 1.0)

    def d_vega_d_time(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: VEGA'S SENSITIVITY TO A CHANGE IN TIME
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        # return greek
        return self.vega(s, k, r, t, sigma, q, b) * (r - b + b * self.d1(
            s, k, b, t, sigma, q) / (sigma * t ** 0.5) - (1.0 + self.d1(
                s, k, b, t, sigma, q) * self.d2(s, k, b, t, sigma, q)) / (2.0 * t))

    def variance_vega(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: BSM'S FORMULA SENSITIVITY TO A SMALL CHANGE IN THE VARIANCE OF THE UNDERLYING
            ASSET'S INSTANTENEOUS RATE OF RETURN
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        # return greek
        return self.vega(s, k, r, t, sigma, q, b) / (2 * sigma)

    def variance_vanna(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: CHANGE IN DELTA FOR A CHANGE IN THE VARIANCE
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        # return greek
        return - s * np.exp((b - r) * t) * self.pdf(self.d1(s, k, b, t, sigma, q)) * self.d2(
            s, k, b, t, sigma, q) / (2.0 * sigma)

    def variance_vomma(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: VARIANCE VEGA'S SENSITIVITY TO A SMALL CHANGE IN THE VARIANCE
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        # return greek
        return s * np.exp((b - r) * t) * t ** 0.5 / (4.0 * sigma ** 3) * self.pdf(self.d1(
            s, k, b, t, sigma, q)) * (self.d1(s, k, b, t, sigma, q) * self.d2(
                s, k, b, t, sigma, q) - 1.0)

    def variance_ultima(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: VARIANCE ULTIMA IS THE THIRD DERIVATIVE OF BSM'S MODEL WITH RESPECT TO VARIANCE
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        # return greek
        return s * np.exp((b - r) * t) * t ** 0.5 / (8 * sigma ** 5) * self.pdf(self.d1(
            s, k, b, t, sigma, q)) * ((self.d1(s, k, b, t, sigma, q) * self.d2(
                s, k, b, t, sigma, q) - 1.0) * (self.d1(s, k, b, t, sigma, q) * self.d2(
                    s, k, b, t, sigma, q) - 3.0) - (self.d1(s, k, b, t, sigma, q) ** 2 + self.d2(
                        s, k, b, t, sigma, q) ** 2))

    def dbsm_dohm(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: RATE OF CHANGE OF BSM'S MODEL REGARDING OHM (STANDARD DEVIATION - TIME)
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        # return greek
        return s * NormalDistribution().pdf((np.log(s / k) + t * sigma ** 2 / 2.0)
                                            / (sigma * t ** 0.5))

    def driftless_theta(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: TIME DECAY WITHOUT TAKING INTO ACCOUNT THE DRIFT OF THE UNDERLYING OR DISCOUNTING
            - THE DRIFTLESS THETA THEREBY ISOLATES THE EFFECT TIME DECAY HAS ON UNCERTAINTY,
            ASSUMING CONSTANT VOLATILITY - UNCERTAINTY AFFECTS THE OPTION THROUGH BOTH TIME AND
            VOLATILITY, SINCE THE LATTER IS A MEASURE OF UNCERTAINTY DURING AN INFINITESIMAL TIME
            PERIOD
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY)
        OUTPUTS: FLOAT
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b)
        # return greek
        return -s * NormalDistribution().pdf(self.d1(s, k, b, t, sigma, q)) * sigma / (
            2.0 * t ** 0.5)

    def theta_vega_relationship(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: RELATIONSHIP BETWEEN THETA AND VEGA - RETURNS THETA
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD)
        OUTPUTS: FLOAT (VEGA)
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(s, k, r, t, sigma, q, b)
        # return greek
        return -self.gamma(s, k, r, t, sigma, q, b) * sigma / (2.0 * t)

    def bleed_offset_volatility(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: IT MEASURES HOW MUCH THE VOLATILITY MUST INCREASE TO OFFSET THE THETA-BLEED/TIME
            DECAY - IN THE CASE OF POSITIVE THETA, ONE CAN ACTUALLY HAVE NEGATIVA OFFSET VOLATILITY
            - DITM EUROPEAN OPTIONS CAN HAVE POSITIVE THETA, AND IN THIS CASE THE OFFSET VOLATILITY
            WILL BE NEGATIVE
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD)
        OUTPUTS: FLOAT (VEGA)
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(s, k, r, t, sigma, q, b)
        # return greek
        return self.theta(s, k, r, t, sigma, q, b) / self.vega(s, k, r, t, sigma, q, b)

    def theta_gamma_relationship_driftless(self, s, k, r, t, sigma, q, b):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: RELATIONSHIP BETWEEN DRIFTLESS GAMMA AND THETA - RETURNS DRIFTLESS THETA
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD)
        OUTPUTS: FLOAT (VEGA)
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(s, k, r, t, sigma, q, b)
        # return greek
        return -2.0 * self.driftless_theta(s, k, r, t, sigma, q, b) / (
            s ** 2 * sigma ** 2)

    def phi(self, s, k, r, t, sigma, q, b, opt_type):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: OPTION SENSITIVITY TO A CHANGE IN THE DIVIDEND YIELD (PHI, ALSO KNOWN AS RHO-2),
            OR THE FOREIGN INTEREST RATE IN THE CASE OF A CURRENCY OPTION
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD)
        OUTPUTS: FLOAT (VEGA)
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        # return greek
        if opt_type == 'call':
            return -t * s * np.exp((b - r) * t) * NormalDistribution().cdf(self.d1(
                s, k, b, t, sigma, q))
        elif opt_type == 'put':
            return t * s * np.exp((b - r) * t) * NormalDistribution().cdf(-self.d1(
                s, k, b, t, sigma, q))

    def carry_rho(self, s, k, r, t, sigma, q, b, opt_type):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: OPTION'S SENSITIVITY TO A SMALL CHANGE IN THE COST-OF-CARRY RATE
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD)
        OUTPUTS: FLOAT (VEGA)
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        # return greek
        if opt_type == 'call':
            return t * s * np.exp((b - r) * t) * NormalDistribution().cdf(self.d1(
                s, k, b, t, sigma, q))
        elif opt_type == 'put':
            return -t * s * np.exp((b - r) * t) * NormalDistribution().cdf(-self.d1(
                s, k, b, t, sigma, q))

    def risk_neutral_prob_itm(self, s, k, r, t, sigma, q, b, opt_type):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: RISK-NEUTRAL PROBABILITY FOR ENDING UP ITM AT MATURITY
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD)
        OUTPUTS: FLOAT (VEGA)
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        # return greek
        if opt_type == 'call':
            return NormalDistribution().cdf(self.d2(s, k, b, t, sigma, q))
        elif opt_type == 'put':
            return NormalDistribution().cdf(-self.d2(s, k, b, t, sigma, q))

    def strike_given_risk_neutral_prob(self, s, k, r, t, sigma, q, b, p, opt_type):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING:
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD), B (COST OF CARRY),
            P (RISK-NEUTRAL PROBABILITY) AND opt_type (OPTION STYLE)
        OUTPUTS: FLOAT (VEGA)
        """
        # initial parameters
        s, k, r, t, sigma, q, b, p = self.set_parameters(
            s, k, r, t, sigma, q, b, p, opt_type)
        # return greek
        if opt_type == 'call':
            return s * np.exp(-NormalDistribution().inv_cdf(p) * sigma * t ** 0.5
                              + (b - sigma ** 2 / 2.0) * t)
        elif opt_type == 'call':
            return s * np.exp(NormalDistribution().inv_cdf(p) * sigma * t ** 0.5
                              + (b - sigma ** 2 / 2.0) * t)

    def d_zeta_d_vol(self, s, k, r, t, sigma, q, b, opt_type):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: ZETA'S SENSITIVITY TO A SMALL CHANGE IN THE IMPLIED VOLATILITY
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD)
        OUTPUTS: FLOAT (VEGA)
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        # return greek
        if opt_type == 'call':
            return -NormalDistribution().pdf(self.d2(s, k, b, t, sigma, q)) * self.d1(
                s, k, b, t, sigma, q) / sigma
        elif opt_type == 'put':
            return NormalDistribution().pdf(self.d2(s, k, b, t, sigma, q)) * self.d1(
                s, k, b, t, sigma, q) / sigma

    def d_zeta_d_time(self, s, k, r, t, sigma, q, b, opt_type):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: THE ITM RISK-NEUTRAL PROBABILITY'S SENSITIVITY TO MOVING CLOSER TO MATURITY
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD)
        OUTPUTS: FLOAT (VEGA)
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        # return greek
        if opt_type == 'call':
            return self.pdf(self.d2(s, k, b, t, sigma, q)) * (b / (sigma * t ** 0.5) - self.d1(
                s, k, b, t, sigma, q) / (2.0 * t))
        elif opt_type == 'put':
            return -self.pdf(self.d2(s, k, b, t, sigma, q)) * (b / (sigma * t ** 0.5) - self.d1(
                s, k, b, t, sigma, q) / (2.0 * t))

    def risk_neutral_probability_density(self, s, k, r, t, sigma, q, b, opt_type):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: SECOND ORDER BSM'S FORMULA REGARDING STRIKE
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD)
        OUTPUTS: FLOAT (VEGA)
        """
        # initial parameters
        s, k, r, t, sigma, q, b = self.set_parameters(
            s, k, r, t, sigma, q, b, opt_type)
        # return greek
        return NormalDistribution().pdf(self.d2(s, k, b, t, sigma, q)) * np.exp(-r * t) / (
            k * sigma * t ** 0.5)

    def probability_ever_getting_itm(self, s, k, r, t, sigma, q, b, opt_type):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG
        DOCSTRING: THE ITM RISK-NEUTRAL PROBABILITY'S SENSITIVITY TO MOVING CLOSER TO MATURITY
        INPUTS: S (SPOT PRICE), K (STRIKE), R (INTEREST RATE), T (TIME TO MATURITY),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), Q (DIVIDEND YIELD)
        OUTPUTS: FLOAT (VEGA)
        """
        # initial parameters
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

    def net_weighted_vega_exposure(self, psi_r, *dicts_opts):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG - PG 119
        DOCSTRING: NET WEIGHTED VEGA EXPOSURE
        INPUTS: PSI_R (VOLATILITY OF REFERENCE VOLATILITY), DICTIONARIES (KEYS -
            NUMBER OF CONTRACTS WITH VEGA I, T (q_vega), VEGA I, T (vega_t), PSI T (VOLATILITY OF
            VOLATILITY FOR THE GIVEN SET OF OPTIONS WITH SAME UNDERLYING AND MATURITY) (psi_t),
            CORRELATION BETWEEN THE VOLATILITY WITH TIME TO MATURITY T AND THE REFERENCE VOLATILITY
            (corr_t))
        OUTPUTS: FLOAT (VEGA)
        """
        # initial parameters
        psi_r = self.set_parameters(psi_r)
        # return greek
        return [float(dict_['q_vega']) * float(dict_['vega_t']) * float(dict_['psi_t'])
                * float(dict_['corr_t']) / float(psi_r) for dict_ in dicts_opts].sum()


class IterativeMethods(Greeks):

    def binomial_pricing_model(self, s, k, r, t, n, u, d, opt_type, h_upper=None, h_lower=None):
        """
        REFERENCES:
            https://www.youtube.com/watch?v=a3906k9C0fM,
            https://www.youtube.com/watch?v=WxrRi9lNnqY,
        DOCSTRING: BINOMIAL ASSET PRICING MODEL - UPPER/LOWER BARRIERS FEATURES IMPLEMENTED
        INPUTS: S0 (INITIAL STOCK PRICING MODEL), K (STRIKE PRICE), T (TIME TO MATURITY IN YEARS),
            R (ANNUAL RISK-FREE RATE), N (NODES), U (UP-FACTOR IN BINOMIAL MODELS), D (DOWN-FACTOR
            - TO ENSURE RECOMBINING TREE USE 1/U), OPTION STYLE (CALL/PUT)
        OUTPUTS: FLOAT FROM ARRAY_CP[0] (CALL-PUT PRICING FOR EACH NODE)
        """
        # initial parameters
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

    def crr_method(self, s, k, r, t, n, sigma, opt_type):
        """
        REFERENCES:
            https://www.youtube.com/watch?v=nWslah9tHLk,
            https://quantpy.com.au/binomial-tree-model/binomial-asset-pricing-model-choosing-parameters/
        DOCSTRING: COX, ROSS AND RUBINSTEIN (CRR) METHOD
        INPUTS: SPOT (S), STRIKE (K), RISK-FREE RATE (R), T (TIME TO MATURITY IN YEARS), N (NODES),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), OPTION STYLE (CALL/PUT)
        OUTPUTS: FLOAT
        """
        # initial parameters
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

    def jr_method(self, s, k, r, t, n, sigma, opt_type):
        """
        REFERENCES:
            https://www.youtube.com/watch?v=nWslah9tHLk,
            https://quantpy.com.au/binomial-tree-model/binomial-asset-pricing-model-choosing-parameters/
        DOCSTRING: JARROW AND RUDD (JR) METHOD
        INPUTS: SPOT (S), STRIKE (K), RISK-FREE RATE (R), T (TIME TO MATURITY IN YEARS), N (NODES),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), OPTION STYLE (CALL/PUT)
        OUTPUTS: FLOAT
        """
        # initial parameters
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

    def eqp_method(self, s, k, r, t, n, sigma, opt_type):
        """
        REFERENCES:
            https://www.youtube.com/watch?v=nWslah9tHLk,
            https://quantpy.com.au/binomial-tree-model/binomial-asset-pricing-model-choosing-parameters/
        DOCSTRING: EQUAL PROBABILITIES (EQP) METHOD
        INPUTS: SPOT (S), STRIKE (K), RISK-FREE RATE (R), T (TIME TO MATURITY IN YEARS), N (NODES),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), OPTION STYLE (CALL/PUT)
        OUTPUTS: FLOAT
        """
        # initial parameters
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

    def trg_method(self, s, k, r, t, n, sigma, opt_type):
        """
        REFERENCES:
            https://www.youtube.com/watch?v=nWslah9tHLk,
            https://quantpy.com.au/binomial-tree-model/binomial-asset-pricing-model-choosing-parameters/
        DOCSTRING: TRIGEORGIS (TRG) METHOD
        INPUTS: SPOT (S), STRIKE (K), RISK-FREE RATE (R), T (TIME TO MATURITY IN YEARS), N (NODES),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), OPTION STYLE (CALL/PUT)
        OUTPUTS: FLOAT
        """
        # initial parameters
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

    @lru_cache()
    def implied_volatility(self, s, k, r, t, sigma, q, b, cp0, opt_type,
                           method='fsolve',
                           tolerance=1E-3, epsilon=1, max_iter=1000, orig_vol=0.5,
                           list_bounds=[(0, 2)]):
        """
        REFERENCES: https://www.youtube.com/watch?v=Jpy3iCsijIU,
            https://www.option-price.com/documentation.php#impliedvolatility
        DOCSTRING: CALCULATING THE IMPLIED VOLATILITY FOR A GIVEN
        INPUTS: OPTION CALL OR PUT, TOLERANCE, EPSILON, MAX ITERATIONS, ORIGINAL VOL
        OUTPUTS: IMPLIED VOLATILITY AND MAX ITERATION HITTED BOOLEAN, ALL ENCAPSULED IN A TUPLE
        """
        # initial parameters
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
                # calculating the difference betwwen call prize, by the implied vol and call price
                dif_calc_market = self.general_opt_price(s, k, r, t, imp_vol, q, b, opt_type) \
                    - cp0
                # newthon-hampson-model to check whether the zero of the function has been spoted
                #   working with a tolerance to assume the zero of the function has been found
                # if self.vega(s, k, r, t, imp_vol, q, b) != 0:
                try:
                    imp_vol = -dif_calc_market / \
                        self.vega(s, k, r, t, imp_vol, q, b) + imp_vol
                except RuntimeWarning:
                    return imp_vol, flag_max_iter_hitten
                # else:
                #     raise Exception("Vega musn't be zero")
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
            return (float_high + float_low) / 2
        elif method == 'fsolve':
            # defining a non-linear function as the difference of the theoratical price (Black
            #   & Scholes) and the fair price
            def func_non_linear(sigma): return np.power(
                self.general_opt_price(s, k, r, t, sigma, q, b, opt_type) - cp0, 2)
            # print(func_non_linear(orig_vol))
            # returning the least sigma for the cost function
            return fsolve(func_non_linear, orig_vol)
        elif method == 'scipy_optimize_minimize':
            # defining a non-linear function as the difference of the theoratical price (Black
            #   & Scholes) and the fair price
            def func_non_linear(sigma): return np.power(
                self.general_opt_price(s, k, r, t, sigma, q, b, opt_type) - cp0, 2)
            # print(func_non_linear(orig_vol))
            # returning the least sigma for the cost function
            return minimize(func_non_linear, orig_vol, method='CG')
        elif method == 'differential_evolution':
            # defining a non-linear function as the difference of the theoratical price (Black
            #   & Scholes) and the fair price
            def func_non_linear(sigma): return np.power(
                self.general_opt_price(s, k, r, t, sigma, q, b, opt_type) - cp0, 2)
            # print(func_non_linear(orig_vol))
            # returning the least sigma for the cost function
            return NonLinearEquations().differential_evolution(func_non_linear, list_bounds)
        else:
            raise Exception('Method to return the root of the non-linear equation is not '
                            + 'recognized, please revisit the parameter')

    def moneyness(self, s, k, r, t, sigma, q):
        """
        REFERENCES: MERCADO DE OPÇÕES, CONCEITOS E ESTRATÉGIAS / AUTOR: LUIZ MAURÍCIO DA SILVA /
            PGS. 74, 75, 76, 77, 78
        DOCSTRING: MEASURES WHETER THE OPTION WILL BE EXERCISED OR NOT TRANSLATED IN A PERCENTUAL
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY), R (INTEREST RATE) AND
            SIGMA (VOLATILITY OF UNDERLYING ASSET)
        OUTPUTS: PERCENTAGE
        """
        # initial parameters
        s, k, r, t, sigma, q = self.set_parameters(s, k, r, t, sigma, q)
        # returning moneyness
        return (self.d1(s, k, r, t, sigma, q)
                + self.d2(s, k, r, t, sigma, q)) / 2

    def iaotm(self, s, k, r, t, sigma, opt_type, pct_moneyness_atm=0.05):
        """
        DOCSTRING: ITM / ATM / OTM - OPTIONS PREDICT OF EXERCISING
        INPUTS: S (SPOT PRICE), K (STRIKE), T (TIME TO MATURITY), R (INTEREST RATE),
            SIGMA (VOLATILITY OF UNDERLYING ASSET), OPTION TYPE AND PERCENTAGE OF ATM
            (STANDARD VALUE OF 5%)
        OUTPUTS: ITM/ATM/OTM
        """
        # initial parameters
        s, k, r, t, sigma, q = self.set_parameters(
            s, k, r, t, sigma, q, opt_type)
        # determining iaotm
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


# print(BlackScholesMerton(50, 45, 0.02, 80 / 365, 0.3).call_price())
# # output: 6.021277654922962
# print(BlackScholesMerton(100, 105, 0.01, 30 / 365, 0.38).call_price())
# print(BlackScholesMerton(100, 105, 0.01, 30 / 365, 0.15).call_price())

# # option parameters
# s = 100.0
# k = 105.0
# t = 30.0 / 365.0
# r = 0.01
# q = 0
# sigma = 0
# c0 = 2.30
# opt_type = 'call'

# # result
# print(EuropeanOptions().implied_volatility(opt_type))
# # output: (0.3688563249135555, True)

# # result
# print(EuropeanOptions(100.0, 105.0, 0.01, 30.0 / 365.0,
#                      0, 0, 2.30, 365).implied_volatility('call'))
# # output: (0.3688563249135555, True)

# print(EuropeanOptions().implied_volatility(194.11, 210.0, 0.01, 38 / 365,
#                                           0, 0, 1.50, 'call'))
# # output: (0.25273873689374843, True)

# print(EuropeanOptions().call_price(50, 45, 0.02, 80 / 365, 0.3))
# # output: 6.021277654922962

# print(EuropeanOptions().delta_call(100, 105, 0.01, 30 / 365, 0.31, 0))
# # output: 0.3101960860984582

# print(EuropeanOptions().delta_put(100, 105, 0.01, 30 / 365, 0.31, 0))
# # output: -0.6898039139015417

# print(EuropeanOptions().gamma(100, 105, 0.01, 30 / 365, 0.31, 0))
# # output: 0.03970674807221808

# print(EuropeanOptions().theta_call(819.42, 1020, 0.01, 42 / 365, 0.6966, 0))
# # output: -0.6700017174199313

# print(EuropeanOptions().theta_put(819.42, 1020, 0.01, 42 / 365, 0.6966, 0))
# # output: -0.6420886495736411

# print(EuropeanOptions().delta_call(100, 105, 0.01, 30 / 365, 0.31, 0))
# # output: 0.3101960860984582

# print(EuropeanOptions().delta_put(100, 105, 0.01, 30 / 365, 0.31, 0))
# # output: -0.6898039139015417

# print(EuropeanOptions().gamma(100, 105, 0.01, 30 / 365, 0.31, 0))
# # output: 0.03970674807221808

# print(EuropeanOptions().implied_volatility(
#     100.0, 105.0, 0.01, 30.0 / 365.0, 0, 2.30, 'call'))
# # output: (0.3688563249135555, True)

# print(EuropeanOptions().basic_opt_price(50, 45, 0.02, 80 / 365, 0.3, 0, 'call'))
# # output: 6.021277654922962

# print(EuropeanOptions().basic_opt_price(50, 45, 0.02, 80 / 365, 0.3, 0, 'put'))
# # output: 0.8244491011813224

# print(EuropeanOptions().implied_volatility(194.11, 210.0, 0.01, 38 / 365,
#                                           0, 1.50, 'call'))
# # output: (0.25273873689374843, True)

# print(EuropeanOptions().implied_volatility(100.0, 95.0, 0.01, 30 / 365,
#                                           0, 2.30, 'put'))
# # output: (0.3931007096283138, True)

# print(EuropeanOptions().implied_volatility(100.0, 95.0, 0.01, 30 / 365,
#                                           0, 2.30, 'put'))

# print(EuropeanOptions().implied_volatility(
#     14.50, 15, 0.0225, 10 / 365, 0.0861, 0.25, 'call'))
# # output:(0.47974617961730276, True)

# print(EuropeanOptions().implied_volatility(
#     17.65, 15, 0.0225, 467 / 252, 0, 7.61, 'call'))
# # output:(0.6983823635729837, True)


# print(EuropeanOptions().implied_volatility(
#     11.33, 14.50, 0.0225, 3 / 365, 0.09076773878571428, 0.01, 'call',
#     orig_vol=0.812424814792))


# def newton_vol_call_div(s, k, t, c, r, q, sigma):

#     fx = s * np.exp(-q * t) * si.norm.cdf(EuropeanOptions().d1(s, k, r, t, sigma), 0.0, 1.0) - \
#         k * np.exp(-r * t) * si.norm.cdf(EuropeanOptions().d2(s,
#                                                              k, r, t, sigma), 0.0, 1.0) - c

#     vega = (1 / np.sqrt(2 * np.pi)) * s * np.exp(-q * t) * np.sqrt(t) * \
#         np.exp(
#             (-si.norm.cdf(EuropeanOptions().d1(s, k, r, t, sigma), 0.0, 1.0) ** 2) * 0.5)

#     tolerance = 0.000001
#     x0 = sigma
#     xnew = x0
#     xold = x0 - 1

#     while abs(xnew - xold) > tolerance:

#         xold = xnew
#         xnew = (xnew - fx - c) / vega

#         return abs(xnew)


# print(newton_vol_call_div(11.33, 14.50, 3 / 365,
#                           0.08, 0.0225, 0.09076773878571428, 0.812424814792))

# max(0.041276451234, '[]')

# print(EuropeanOptions().delta(5127.7930, 5300.0, 0.02, 60 / 365, 0.3, 0, 'call'))

# s = 24.87
# k = 40.25
# r = 0.0775
# t = 0.05555555555555555
# sigma = 1.9251923611193833

# print(EuropeanOptions().d1(s, k, r, t, sigma))
# # output: -0.8246155162915018
