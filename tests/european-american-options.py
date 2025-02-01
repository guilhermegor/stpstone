# EUROPEAN OPTIONS UNIT TEST

import sys
from unittest import TestCase, main
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from stpstone.finance.derivatives.options.european import EuropeanOptions
from stpstone.finance.derivatives.options.american import AmericanOptions


class TestEuropeanDerivatives(TestCase):

    def test_binomial_pricing_model_european_call(self):
        '''
        DOCSTRING: TEST BINOMIAL PRICING MODEL
        INPUTS: -
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        s = 100
        k = 100
        r = 0.06
        t = 1
        n = 3
        u = 1.1
        d = 1 / 1.1
        opt_style = 'call'
        cp0 = 10.145735799928826
        self.assertAlmostEqual(
            EuropeanOptions().binomial_pricing_model(s, k, r, t, n, u, d, opt_style),
            cp0)

    def test_binomial_pricing_model_european_put(self):
        '''
        DOCSTRING: TEST BINOMIAL PRICING MODEL
        INPUTS: -
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        s = 100
        k = 100
        r = 0.06
        t = 1
        n = 3
        u = 1.1
        d = 1 / 1.1
        opt_style = 'put'
        cp0 = 4.322189158353709
        self.assertAlmostEqual(
            EuropeanOptions().binomial_pricing_model(s, k, r, t, n, u, d, opt_style),
            cp0)

    def test_binomial_pricing_model_european_call_w_barrier(self):
        '''
        DOCSTRING: TEST BINOMIAL PRICING MODEL
        INPUTS: -
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        s = 100
        k = 100
        r = 0.06
        t = 1
        n = 3
        u = 1.1
        d = 1 / 1.1
        h_upper = 125
        opt_style = 'call'
        cp0 = 4.00026736854323
        self.assertAlmostEqual(
            EuropeanOptions().binomial_pricing_model(
                s, k, r, t, n, u, d, opt_style, h_upper),
            cp0)

    def test_binomial_pricing_model_european_put_w_barrier(self):
        '''
        DOCSTRING: TEST BINOMIAL PRICING MODEL
        INPUTS: -
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        s = 100
        k = 100
        r = 0.06
        t = 1
        n = 3
        u = 1.1
        d = 1 / 1.1
        opt_style = 'put'
        cp0 = 4.322189158353709
        self.assertAlmostEqual(
            EuropeanOptions().binomial_pricing_model(s, k, r, t, n, u, d, opt_style),
            cp0)

    def test_binomial_pricing_model_american_call(self):
        '''
        DOCSTRING: TEST BINOMIAL PRICING MODEL
        INPUTS: -
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        s = 100
        k = 100
        r = 0.06
        t = 1
        n = 3
        u = 1.1
        d = 1 / 1.1
        opt_style = 'call'
        cp0 = 10.145735799928826
        self.assertAlmostEqual(
            AmericanOptions().binomial_pricing_model(s, k, r, t, n, u, d, opt_style),
            cp0)

    def test_binomial_pricing_model_american_put(self):
        '''
        DOCSTRING: TEST BINOMIAL PRICING MODEL
        INPUTS: -
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        s = 100
        k = 100
        r = 0.06
        t = 1
        n = 3
        u = 1.1
        d = 1 / 1.1
        opt_style = 'put'
        cp0 = 4.654588754602527
        self.assertAlmostEqual(
            AmericanOptions().binomial_pricing_model(s, k, r, t, n, u, d, opt_style),
            cp0)

    def test_crr_model_european_call(self):
        '''
        DOCSTRING: TEST COX, ROSS AND RUBINSTEIN (CRR) PRICING MODEL
        INPUTS: -
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        s = 100
        k = 110
        r = 0.06
        t = 0.5
        n = 100
        sigma = 0.3
        opt_style = 'call'
        cp0 = 5.77342630682585
        self.assertAlmostEqual(
            EuropeanOptions().crr_method(s, k, r, t, n, sigma, opt_style),
            cp0)

    def test_crr_model_european_put(self):
        '''
        DOCSTRING: TEST COX, ROSS AND RUBINSTEIN (CRR) PRICING MODEL
        INPUTS: -
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        s = 100
        k = 110
        r = 0.06
        t = 0.5
        n = 100
        sigma = 0.3
        opt_style = 'put'
        cp0 = 12.522434997161955
        self.assertAlmostEqual(
            EuropeanOptions().crr_method(s, k, r, t, n, sigma, opt_style),
            cp0)

    def test_jr_model_european_call(self):
        '''
        DOCSTRING: TEST JARROW AND RUDD (JR) PRICING MODEL
        INPUTS: -
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        s = 100
        k = 110
        r = 0.06
        t = 0.5
        n = 100
        sigma = 0.3
        opt_style = 'call'
        cp0 = 5.754089414567556
        self.assertAlmostEqual(
            EuropeanOptions().jr_method(s, k, r, t, n, sigma, opt_style),
            cp0)

    def test_jr_model_european_put(self):
        '''
        DOCSTRING: TEST JARROW AND RUDD (JR) PRICING MODEL
        INPUTS: -
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        s = 100
        k = 110
        r = 0.06
        t = 0.5
        n = 100
        sigma = 0.3
        opt_style = 'put'
        cp0 = 12.503266834513754
        self.assertAlmostEqual(
            EuropeanOptions().jr_method(s, k, r, t, n, sigma, opt_style),
            cp0)

    def test_eqp_model_european_call(self):
        '''
        DOCSTRING: TEST JARROW AND RUDD (JR) PRICING MODEL
        INPUTS: -
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        s = 100
        k = 110
        r = 0.06
        t = 0.5
        n = 100
        sigma = 0.3
        opt_style = 'call'
        cp0 = 5.7365844788666545
        self.assertAlmostEqual(
            EuropeanOptions().eqp_method(s, k, r, t, n, sigma, opt_style),
            cp0)

    def test_eqp_model_european_put(self):
        '''
        DOCSTRING: TEST JARROW AND RUDD (JR) PRICING MODEL
        INPUTS: -
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        s = 100
        k = 110
        r = 0.06
        t = 0.5
        n = 100
        sigma = 0.3
        opt_style = 'put'
        cp0 = 12.493729351434228
        self.assertAlmostEqual(
            EuropeanOptions().eqp_method(s, k, r, t, n, sigma, opt_style),
            cp0)

    def test_trg_model_european_call(self):
        '''
        DOCSTRING: TEST JARROW AND RUDD (JR) PRICING MODEL
        INPUTS: -
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        s = 100
        k = 110
        r = 0.06
        t = 0.5
        n = 100
        sigma = 0.3
        opt_style = 'call'
        cp0 = 5.773359020180677
        self.assertAlmostEqual(
            EuropeanOptions().trg_method(s, k, r, t, n, sigma, opt_style),
            cp0)

    def test_trg_model_european_put(self):
        '''
        DOCSTRING: TEST JARROW AND RUDD (JR) PRICING MODEL
        INPUTS: -
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        s = 100
        k = 110
        r = 0.06
        t = 0.5
        n = 100
        sigma = 0.3
        opt_style = 'put'
        cp0 = 12.5226489154105
        self.assertAlmostEqual(
            EuropeanOptions().trg_method(s, k, r, t, n, sigma, opt_style),
            cp0)


if __name__ == '__main__':
    main()
