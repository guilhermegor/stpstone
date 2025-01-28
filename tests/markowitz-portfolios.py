### MARKOWITZ PORTFOLIOS UNIT TESTS ###

import sys
import numpy as np
from unittest import TestCase, main
sys.path.append(r'C:\Users\Guilherme\OneDrive\Documentos\GitHub')
from stpstone.finance.financial_risk.market_risk import Markowitz


class TestMarkowitz(TestCase):

    def setUp(self):
        '''
        DOCSTRING: SETUP FOR THE TEST CLASS
        INPUTS: -
        OUTPUTS: -
        '''
        int_seed = 123
        np.random.seed(int_seed)

    def test_weights_wc_pos_min(self, float_rtol=1e-5, float_atol=1e-8):
        '''
        DOCSTRING: TEST MARKOWITZ PORTFOLIO WEIGHTS WITH CONSTRAINTS - POSITIVE MINIMUM VALUES
        INPUTS: - 
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        int_n_assets = 4 
        bl_constraints = True
        bl_multiplier = False
        array_min_w = [0.25, 1.0, 0.25, 1.0]
        array_expected_weights = np.array([1., 0., 0., 0.])
        array_actual_weights = Markowitz().random_weights(
            int_n_assets=int_n_assets,
            bl_constraints=bl_constraints,
            bl_multiplier=bl_multiplier,
            array_min_w=array_min_w
        )
        # numpy.allclose to compare the arrays
        self.assertTrue(
            np.allclose(
                array_actual_weights, 
                array_expected_weights, 
                rtol=float_rtol, 
                atol=float_atol
            )
        )
    
    def test_weights_wc_zero_min(self, float_rtol=1e-5, float_atol=1e-8):
        '''
        DOCSTRING: TEST MARKOWITZ PORTFOLIO WEIGHTS WITH CONSTRAINTS - ZERO MINIMUM VALUES
        INPUTS: - 
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        int_n_assets = 4 
        bl_constraints = True
        bl_multiplier = False
        array_min_w = [0.0, 0.0, 0.0, 0.0]
        # use assertRaises to check for ValueError
        with self.assertRaises(ValueError) as context:
            Markowitz().random_weights(
                int_n_assets=int_n_assets,
                bl_constraints=bl_constraints,
                bl_multiplier=bl_multiplier,
                array_min_w=array_min_w
            )
            #   check if the exception message is as expected
            self.assertEqual(
                str(context.exception), 
                'EVERY MIN_INVEST_PER_ASSET MUST BE GREATER THAN 0.'
            )
    
    def test_weights_wc_one_min(self, float_rtol=1e-5, float_atol=1e-8):
        '''
        DOCSTRING: TEST MARKOWITZ PORTFOLIO WEIGHTS WITH CONSTRAINTS - ZERO MINIMUM VALUES
        INPUTS: - 
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        int_n_assets = 4 
        bl_constraints = True
        bl_multiplier = False
        array_min_w = [1.0, 1.0, 1.0, 1.0]
        array_expected_weights = np.array([0, 0, 1, 0])
        array_actual_weights = Markowitz().random_weights(
            int_n_assets=int_n_assets,
            bl_constraints=bl_constraints,
            bl_multiplier=bl_multiplier,
            array_min_w=array_min_w
        )
        # numpy.allclose to compare the arrays
        self.assertTrue(
            np.allclose(
                array_actual_weights, 
                array_expected_weights, 
                rtol=float_rtol, 
                atol=float_atol
            )
        )
    
    def test_weights_wc_neg_min(self):
        '''
        DOCSTRING: TEST MARKOWITZ PORTFOLIO WEIGHTS WITH CONSTRAINTS - NEGATIVE MINIMUM VALUES
        INPUTS: - 
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        int_n_assets = 4 
        bl_constraints = True
        bl_multiplier = False
        array_min_w = [-0.25, 0.0, 0.25, 0.0]
        # use assertRaises to check for ValueError
        with self.assertRaises(ValueError) as context:
            Markowitz().random_weights(
                int_n_assets=int_n_assets,
                bl_constraints=bl_constraints,
                bl_multiplier=bl_multiplier,
                array_min_w=array_min_w
            )
            #   check if the exception message is as expected
            self.assertEqual(str(context.exception), 'MIN_INVEST_PER_ASSET MUST BE POSITIVE.')
    
    def test_weights_wc_min_unbalanced(self):
        '''
        DOCSTRING: TEST MARKOWITZ PORTFOLIO WEIGHTS WITH CONSTRAINTS - MINIMM VALUES UNBALANCED 
            WITH THE NUMBER OF ASSETS
        INPUTS: - 
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        int_n_assets = 4 
        bl_constraints = True
        bl_multiplier = False
        array_min_w = [-0.25, 0.0, 0.25, 0.0, 0.10]
        # use assertRaises to check for ValueError
        with self.assertRaises(ValueError) as context:
            Markowitz().random_weights(
                int_n_assets=int_n_assets,
                bl_constraints=bl_constraints,
                bl_multiplier=bl_multiplier,
                array_min_w=array_min_w
            )
            #   check if the exception message is as expected
            self.assertEqual(str(context.exception), 'THE LENGTH OF MIN_INVEST_PER_ASSET '
                             + 'MUST MATCH THE NUMBER OF ASSETS.')
    
    def test_weights_wc_none_min(self):
        '''
        DOCSTRING: TEST MARKOWITZ PORTFOLIO WEIGHTS WITH CONSTRAINTS - NONE MINIMUM VALUES
        INPUTS: - 
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        int_n_assets = 4 
        bl_constraints = True
        bl_multiplier = False
        array_min_w = None
        # use assertRaises to check for ValueError
        with self.assertRaises(ValueError) as context:
            Markowitz().random_weights(
                int_n_assets=int_n_assets,
                bl_constraints=bl_constraints,
                bl_multiplier=bl_multiplier,
                array_min_w=array_min_w
            )
            #   check if the exception message is as expected
            self.assertEqual(str(context.exception), 'MIN_INVEST_PER_ASSET MUST BE PROVIDED '
                             + 'AS A LIST WHEN CONSTRAINTS ARE ENABLED.')

    def test_weights_wc_numbers(self):
        '''
        DOCSTRING: TEST MARKOWITZ PORTFOLIO WEIGHTS WITH CONSTRAINTS - LIST OF MINIMUM VALUES MUST 
            BE NUMBERS
        INPUTS: - 
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        int_n_assets = 4 
        bl_constraints = True
        array_min_w = ['nan', 0.0, 0.25, 0.5]
        # use assertRaises to check for ValueError
        with self.assertRaises(ValueError) as context:
            Markowitz().random_weights(
                int_n_assets=int_n_assets,
                bl_constraints=bl_constraints,
                array_min_w=array_min_w
            )
            #   check if the exception message is as expected
            self.assertEqual(str(context.exception), 'MIN_INVEST_PER_ASSET MUST BE A LIST '
                             + 'OF NUMBERS.')
    
    def test_weights_wc_max_w(self):
        '''
        DOCSTRING: TEST MARKOWITZ PORTFOLIO WEIGHTS WITH CONSTRAINTS - MAX WEIGHT FOR EACH ASSET 
            WITHIN THE MINIMUM VALUES LIST
        INPUTS: - 
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        int_n_assets = 4 
        bl_constraints = True
        bl_multiplier = False
        array_min_w = [0.0, 0.0, 0.25, 2.0]
        # use assertRaises to check for ValueError
        with self.assertRaises(ValueError) as context:
            Markowitz().random_weights(
                int_n_assets=int_n_assets,
                bl_constraints=bl_constraints,
                bl_multiplier=bl_multiplier,
                array_min_w=array_min_w
            )
            #   check if the exception message is as expected
            self.assertEqual(str(context.exception), 'MIN_INVEST_PER_ASSET MUST BE BELOW 1.0')

    def test_weights_wc_multplier1(self, float_rtol=1e-5, float_atol=1e-8):
        '''
        DOCSTRING: TEST MARKOWITZ PORTFOLIO WEIGHTS WITH CONSTRAINTS
        INPUTS: - 
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        int_n_assets = 4 
        bl_constraints = True
        bl_multiplier = True
        array_min_w = [0, 0.8, 0.2, 1.0]
        # use assertRaises to check for ValueError
        with self.assertRaises(ValueError) as context:
            Markowitz().random_weights(
                int_n_assets=int_n_assets,
                bl_constraints=bl_constraints,
                bl_multiplier=bl_multiplier,
                array_min_w=array_min_w
            )
            #   check if the exception message is as expected
            self.assertEqual(
                str(context.exception), 
                'EVERY MIN_INVEST_PER_ASSET MUST BE GREATER THAN 0.'
            )
    
    def test_weights_wc_multplier2(self, float_rtol=1e-5, float_atol=1e-8):
        '''
        DOCSTRING: TEST MARKOWITZ PORTFOLIO WEIGHTS WITH CONSTRAINTS
        INPUTS: - 
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        int_n_assets = 4 
        bl_constraints = True
        bl_multiplier = True
        array_min_w = [1.0, 0.8, 0.2, 1.0]
        array_expected_weights = np.array([1., 0., 0., 0.])
        array_actual_weights = Markowitz().random_weights(
            int_n_assets=int_n_assets,
            bl_constraints=bl_constraints,
            bl_multiplier=bl_multiplier,
            array_min_w=array_min_w
        )
        # numpy.allclose to compare the arrays
        self.assertTrue(
            np.allclose(
                array_actual_weights, 
                array_expected_weights,
                rtol=float_rtol, 
                atol=float_atol
            )
        )

    def test_weights_woc(self, float_rtol=1e-5, float_atol=1e-8):
        '''
        DOCSTRING: TEST MARKOWITZ PORTFOLIO WEIGHTS WITHOUT CONSTRAINTS
        INPUTS: - 
        OUTPUTS: BOOLEAN WITH ACCOMPLISHMENT OF TEST
        '''
        int_n_assets = 4 
        bl_constraints = False
        bl_multiplier = False
        array_expected_weights = np.array([0.39554701, 0.16250763, 0.12883616, 0.3131092])
        array_actual_weights = Markowitz().random_weights(
            int_n_assets=int_n_assets,
            bl_constraints=bl_constraints,
            bl_multiplier=bl_multiplier
        )
        # numpy.allclose to compare the arrays
        self.assertTrue(
            np.allclose(
                array_actual_weights, 
                array_expected_weights,
                rtol=float_rtol, 
                atol=float_atol
            )
        )


if __name__ == '__main__':
    main()
