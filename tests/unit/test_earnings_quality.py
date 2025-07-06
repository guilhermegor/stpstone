import math
import unittest

from stpstone.analytics.perf_metrics.earnings_quality import EarningsManipulation


class TestEarningsManipulation(unittest.TestCase):
    def setUp(self):
        self.analyzer = EarningsManipulation()

    # test normal operations
    def test_normal_case(self):
        """Test with typical financial values."""
        ratios = self.analyzer.inputs_beneish_model(
            float_ar_t=50000.0,
            float_sales_t=1000000.0,
            float_ar_tm1=45000.0,
            float_sales_tm1=900000.0,
            float_gp_tm1=300000.0,
            float_gp_t=320000.0,
            float_ppe_t=200000.0,
            float_ca_t=300000.0,
            float_lti_t=100000.0,
            float_lti_tm1=90000.0,
            float_ta_t=1000000.0,
            float_ppe_tm1=180000.0,
            float_ca_tm1=280000.0,
            float_ta_tm1=950000.0,
            float_dep_tm1=20000.0,
            float_dep_t=25000.0,
            float_sga_t=150000.0,
            float_sga_tm1=140000.0,
            float_inc_cont_op=100000.0,
            float_cfo_t=90000.0,
            float_tl_t=400000.0,
            float_tl_tm1=380000.0,
        )

        # verify all ratios are calculated
        self.assertEqual(len(ratios), 8)
        self.assertAlmostEqual(ratios["float_dsr"], 1.0, places=2)
        self.assertAlmostEqual(ratios["float_gmi"], 1.04, places=2)
        self.assertAlmostEqual(ratios["float_aqi"], 0.95, places=2)
        self.assertAlmostEqual(ratios["float_sgi"], 1.11, places=2)
        self.assertAlmostEqual(ratios["float_depi"], 0.90, places=2)
        self.assertAlmostEqual(ratios["float_sgai"], 0.96, places=2)
        self.assertAlmostEqual(ratios["float_tata"], 0.01, places=2)
        self.assertAlmostEqual(ratios["float_lvgi"], 1.0, places=2)

        # test m-score calculation
        m_score = self.analyzer.beneish_model(**ratios)
        self.assertAlmostEqual(m_score, -2.34, places=2)

    def test_zero_sales(self):
        """Test handling of zero sales edge case."""
        with self.assertRaises(ZeroDivisionError):
            self.analyzer.inputs_beneish_model(
                float_ar_t=50000.0,
                float_sales_t=0.0,  # Zero sales
                float_ar_tm1=45000.0,
                float_sales_tm1=900000.0,
                float_gp_tm1=300000.0,
                float_gp_t=320000.0,
                float_ppe_t=200000.0,
                float_ca_t=300000.0,
                float_lti_t=100000.0,
                float_lti_tm1=90000.0,
                float_ta_t=1000000.0,
                float_ppe_tm1=180000.0,
                float_ca_tm1=280000.0,
                float_ta_tm1=950000.0,
                float_dep_tm1=20000.0,
                float_dep_t=25000.0,
                float_sga_t=150000.0,
                float_sga_tm1=140000.0,
                float_inc_cont_op=100000.0,
                float_cfo_t=90000.0,
                float_tl_t=400000.0,
                float_tl_tm1=380000.0,
            )

    def test_negative_values(self):
        """Test with negative financial values (should work mathematically)."""
        ratios = self.analyzer.inputs_beneish_model(
            float_ar_t=-50000.0,
            float_sales_t=1000000.0,
            float_ar_tm1=45000.0,
            float_sales_tm1=-900000.0,
            float_gp_tm1=-300000.0,
            float_gp_t=320000.0,
            float_ppe_t=200000.0,
            float_ca_t=300000.0,
            float_lti_t=100000.0,
            float_lti_tm1=90000.0,
            float_ta_t=1000000.0,
            float_ppe_tm1=180000.0,
            float_ca_tm1=280000.0,
            float_ta_tm1=950000.0,
            float_dep_tm1=20000.0,
            float_dep_t=25000.0,
            float_sga_t=150000.0,
            float_sga_tm1=140000.0,
            float_inc_cont_op=-100000.0,
            float_cfo_t=90000.0,
            float_tl_t=400000.0,
            float_tl_tm1=380000.0,
        )
        
        self.assertEqual(len(ratios), 8)

    def test_type_validation(self):
        """Test type validation of input parameters."""
        with self.assertRaises(TypeError):
            self.analyzer.inputs_beneish_model(
                float_ar_t="50000",
                float_sales_t=1000000.0,
                float_ar_tm1=45000.0,
                float_sales_tm1=900000.0,
                float_gp_tm1=300000.0,
                float_gp_t=320000.0,
                float_ppe_t=200000.0,
                float_ca_t=300000.0,
                float_lti_t=100000.0,
                float_lti_tm1=90000.0,
                float_ta_t=1000000.0,
                float_ppe_tm1=180000.0,
                float_ca_tm1=280000.0,
                float_ta_tm1=950000.0,
                float_dep_tm1=20000.0,
                float_dep_t=25000.0,
                float_sga_t=150000.0,
                float_sga_tm1=140000.0,
                float_inc_cont_op=100000.0,
                float_cfo_t=90000.0,
                float_tl_t=400000.0,
                float_tl_tm1=380000.0,
            )

    def test_extreme_values(self):
        """Test with extremely large/small values."""
        ratios = self.analyzer.inputs_beneish_model(
            float_ar_t=1e12,
            float_sales_t=1e15,
            float_ar_tm1=1e11,
            float_sales_tm1=1e14,
            float_gp_tm1=1e14,
            float_gp_t=1e14,
            float_ppe_t=1e12,
            float_ca_t=1e12,
            float_lti_t=1e11,
            float_lti_tm1=1e10,
            float_ta_t=1e15,
            float_ppe_tm1=1e12,
            float_ca_tm1=1e12,
            float_ta_tm1=1e15,
            float_dep_tm1=1e10,
            float_dep_t=1e10,
            float_sga_t=1e13,
            float_sga_tm1=1e13,
            float_inc_cont_op=1e14,
            float_cfo_t=1e14,
            float_tl_t=1e14,
            float_tl_tm1=1e14,
        )
        
        for val in ratios.values():
            self.assertFalse(math.isnan(val))
            self.assertFalse(math.isinf(val))

    def test_m_score_interpretation(self):
        """Test M-Score interpretation thresholds."""
        low_score = self.analyzer.beneish_model(
            float_dsr=1.0,
            float_gmi=1.0,
            float_aqi=1.0,
            float_sgi=1.0,
            float_depi=1.0,
            float_sgai=1.0,
            float_tata=0.0,
            float_lvgi=1.0,
        )
        self.assertLess(low_score, -1.78)
        
        # above threshold (potential manipulation)
        high_score = self.analyzer.beneish_model(
            float_dsr=1.5,
            float_gmi=1.5,
            float_aqi=1.5,
            float_sgi=1.5,
            float_depi=1.5,
            float_sgai=0.5,
            float_tata=0.1,
            float_lvgi=1.5,
        )
        self.assertGreater(high_score, -1.78)

    def test_nan_handling(self):
        """Test that NaN inputs raise appropriate exceptions."""
        with self.assertRaises(ValueError):
            self.analyzer.inputs_beneish_model(
                float_ar_t=float('nan'),
                float_sales_t=1000000.0,
                float_ar_tm1=45000.0,
                float_sales_tm1=900000.0,
                float_gp_tm1=300000.0,
                float_gp_t=320000.0,
                float_ppe_t=200000.0,
                float_ca_t=300000.0,
                float_lti_t=100000.0,
                float_lti_tm1=90000.0,
                float_ta_t=1000000.0,
                float_ppe_tm1=180000.0,
                float_ca_tm1=280000.0,
                float_ta_tm1=950000.0,
                float_dep_tm1=20000.0,
                float_dep_t=25000.0,
                float_sga_t=150000.0,
                float_sga_tm1=140000.0,
                float_inc_cont_op=100000.0,
                float_cfo_t=90000.0,
                float_tl_t=400000.0,
                float_tl_tm1=380000.0,
            )


if __name__ == '__main__':
    unittest.main()