# OPTION PRCING FORMULAS, FOR AMERICAN TYPE

import numpy as np


class InitialSettings:

    def set_parameters(self, *params, opt_style='call'):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # check wheter is a call or put option, in case the type is neither of the former raise
        #   error
        if opt_style not in ['call', 'put']:
            raise Exception('Option ought be a call or a put')
        # initial parameters
        list_params = [param[0] if isinstance(param, list) == True else param for
                       param in params]
        # return parameters
        return [float(param) for param in list_params if isinstance(param, str) == False]


class PricingModels(InitialSettings):

    def binomial(self, s, k, r, t, n, u, d, opt_style, h_upper=None, h_lower=None):
        """
        REFERENCES:
            https://www.youtube.com/watch?v=a3906k9C0fM,
            https://www.youtube.com/watch?v=WxrRi9lNnqY,
            https://www.youtube.com/watch?v=K2Iy8bCmXjk
        DOCSTRING: BINOMIAL ASSET PRICING MODEL - UPPER/LOWER BARRIERS FEATURES IMPLEMENTED
        INPUTS: S0 (INITIAL STOCK PRICING MODEL), K (STRIKE PRICE), T (TIME TO MATURITY IN YEARS),
            R (ANNUAL RISK-FREE RATE), N (NODES), U (UP-FACTOR IN BINOMIAL MODELS), D (DOWN-FACTOR
            - TO ENSURE RECOMBINING TREE USE 1/U), OPTION STYLE (CALL/PUT)
        OUTPUTS: FLOAT FROM ARRAY_CP[0] (CALL-PUT PRICING FOR EACH NODE)
        """
        # initial parameters
        s, k, r, t, n, u, d = self.set_parameters(
            s, k, r, t, n, u, d, opt_style)
        # precompute values
        dt = t / n
        q = (np.exp(r * dt) - d) / (u - d)
        disc = np.exp(-r * dt)
        # initialise stock prices at maturity
        array_s_nodes = s * d**(np.arange(n, -1, -1)) * \
            u**(np.arange(0, n + 1, 1))
        # initialise option values at maturity - if intrinsic value is negative, consider zero
        if opt_style == 'call':
            array_cp = np.maximum(array_s_nodes - k, np.zeros(int(n) + 1))
        else:
            array_cp = np.maximum(k - array_s_nodes, np.zeros(int(n) + 1))
        # check s payoff, according to barriers, if values are different from none
        if h_upper != None:
            array_cp[array_s_nodes >= h_upper] = 0
        if h_lower != None:
            array_cp[array_s_nodes <= h_lower] = 0
        # step backwards recursion through tree
        for i in np.arange(int(n) - 1, -1, -1):
            array_s_nodes = s * d**(np.arange(i, -1, -1)) * \
                u**(np.arange(0, i + 1, 1))
            array_cp[:i + 1] = disc * \
                (q * array_cp[1: i + 2] + (1 - q) * array_cp[0: i + 1])
            array_cp = array_cp[:-1]
            #   evaluating maximum value between fair and intrinsic value
            if opt_style == 'call':
                array_cp = np.maximum(array_cp, array_s_nodes - k)
            else:
                array_cp = np.maximum(array_cp, k - array_s_nodes)
        # returning the no-arbitrage price at node 0
        return array_cp[0]

    def barone_adesi_whaley(self, ):
        """
        REFERENCES: THE COMPLETE GUIDE TO OPTION PRICING FORMULAS - ESPEN GAARDER HAUG - PG 119
        DOCSTRING: QUADRATIC APPROXIMATION METHOD BY BARONE-ADESI AND WHALEY (1987) TO PRICE
            CALL AND PUT OPTIONS ON AN UNDERLYING ASSET WITH THE COST-OF-CARRY RATE B - WHEN THE
            B >= R, THE AMERICAN CALL VALUE IS EQUAL TO THE EUROPEAN CALL VALUE AND CAN THEN BE FOUND
            BY USING THE GENERALIZED BLACK-SHCOLES-MERTHON (BSM) FORMULA
        INPUTS: PSI_R (VOLATILITY OF REFERENCE VOLATILITY), DICTIONARIES (KEYS -
            NUMBER OF CONTRACTS WITH VEGA I, T (q_vega), VEGA I, T (vega_t), PSI T (VOLATILITY OF
            VOLATILITY FOR THE GIVEN SET OF OPTIONS WITH SAME UNDERLYING AND MATURITY) (psi_t),
            CORRELATION BETWEEN THE VOLATILITY WITH TIME TO MATURITY T AND THE REFERENCE VOLATILITY
            (corr_t))
        OUTPUTS: FLOAT (VEGA)
        """
