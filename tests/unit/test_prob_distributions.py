"""Unit tests for probability distributions module.

Tests cover all implemented distributions including Bernoulli, Geometric, Binomial,
Poisson, Chi-Squared, T-Student, F-Snedecor, Normal, and Hansen's Skewed Student.
"""

import warnings

import numpy as np
from numpy import sqrt
import pytest
from pytest_mock import MockerFixture

from stpstone.analytics.quant.prob_distributions import (
	HansenSkewStudent,
	NormalDistribution,
	ProbabilityDistributions,
)


warnings.filterwarnings("ignore", category=DeprecationWarning, module="PIL")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="matplotlib")


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def prob_dist() -> ProbabilityDistributions:
	"""Fixture providing ProbabilityDistributions instance.

	Returns
	-------
	ProbabilityDistributions
		ProbabilityDistributions instance
	"""
	return ProbabilityDistributions()


@pytest.fixture
def normal_dist() -> NormalDistribution:
	"""Fixture providing NormalDistribution instance.

	Returns
	-------
	NormalDistribution
		NormalDistribution instance
	"""
	return NormalDistribution()


@pytest.fixture
def skew_student() -> HansenSkewStudent:
	"""Fixture providing HansenSkewStudent instance.

	Returns
	-------
	HansenSkewStudent
		HansenSkewStudent instance
	"""
	return HansenSkewStudent()


# --------------------------
# ProbabilityDistributions Tests
# --------------------------
class TestProbabilityDistributions:
	"""Test suite for ProbabilityDistributions class."""

	def test_bernoulli_valid_input(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test Bernoulli distribution with valid inputs.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		result = prob_dist.bernoulli_distribution(0.5)
		assert isinstance(result, dict)
		assert "mean" in result
		assert result["mean"] == pytest.approx(0.5, abs=1e-4)
		assert result["var"] == pytest.approx(0.25)
		assert result["distribution"] == pytest.approx(1.0, abs=1e-4)

	def test_bernoulli_invalid_prob(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test Bernoulli with invalid probability values.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Probability must be between 0 and 1"):
			prob_dist.bernoulli_distribution(-0.1)
		with pytest.raises(ValueError, match="Probability must be between 0 and 1"):
			prob_dist.bernoulli_distribution(1.1)

	def test_bernoulli_invalid_trials(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test Bernoulli with invalid number of trials.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Number of trials must be greater than 0"):
			prob_dist.bernoulli_distribution(0.5, -1)

	def test_geometric_distribution(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test Geometric distribution calculations.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		result = prob_dist.geometric_distribution(0.3, 5)
		assert isinstance(result, dict)
		assert len(result["distribution"]) == 5
		assert result["mean"] == pytest.approx(1 / 0.3)
		assert result["var"] == pytest.approx((1 - 0.3) / (0.3**2))

	def test_geometric_invalid_prob(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test Geometric with invalid probability values.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Probability must be between 0 and 1"):
			prob_dist.geometric_distribution(-0.1, 5)
		with pytest.raises(ValueError, match="Probability must be between 0 and 1"):
			prob_dist.geometric_distribution(1.1, 5)

	def test_geometric_invalid_trials(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test Geometric with invalid number of trials.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Number of trials must be greater than 0"):
			prob_dist.geometric_distribution(0.5, -1)

	def test_binomial_distribution(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test Binomial distribution calculations.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		result = prob_dist.binomial_distribution(0.4, 10)
		assert isinstance(result, dict)
		assert len(result["distribution"]) == 10
		assert result["mean"] == pytest.approx(10 * 0.4)
		assert result["var"] == pytest.approx(10 * 0.4 * 0.6)

	def test_binomial_invalid_prob(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test Binomial with invalid probability values.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Probability must be between 0 and 1"):
			prob_dist.binomial_distribution(-0.1, 10)
		with pytest.raises(ValueError, match="Probability must be between 0 and 1"):
			prob_dist.binomial_distribution(1.1, 10)

	def test_binomial_invalid_trials(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test Binomial with invalid number of trials.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Number of trials must be greater than 0"):
			prob_dist.binomial_distribution(0.5, -1)

	def test_poisson_distribution(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test Poisson distribution calculations.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		result = prob_dist.poisson_distribution(10, 0.5)
		assert isinstance(result, dict)
		assert len(result["distribution"]) == 10
		assert result["mean"] == pytest.approx(0.5)
		assert result["var"] == pytest.approx(0.5)

	def test_poisson_invalid_mu(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test Poisson with invalid mu values.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Mu must be between 0 and 1"):
			prob_dist.poisson_distribution(10, -0.1)
		with pytest.raises(ValueError, match="Mu must be between 0 and 1"):
			prob_dist.poisson_distribution(10, 1.1)

	def test_poisson_invalid_trials(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test Poisson with invalid number of trials.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Number of trials must be greater than 0"):
			prob_dist.poisson_distribution(-1, 0.5)

	@pytest.mark.parametrize("func_type", ["ppf", "pdf", "cdf"])
	def test_chi_squared_functions(
		self, prob_dist: ProbabilityDistributions, func_type: str
	) -> None:
		"""Test Chi-Squared distribution functions.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance
		func_type : str
			Function type

		Returns
		-------
		None
		"""
		if func_type == "ppf":
			result = prob_dist.chi_squared(0.95, 5, func_type)
			assert isinstance(result, float)
		else:
			result = prob_dist.chi_squared(0.95, 5, func_type, 0.0, 20.0, 0.1)
			assert isinstance(result, np.ndarray)

	def test_chi_squared_invalid_prob(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test Chi-Squared with invalid probability values.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Probability must be between 0 and 1"):
			prob_dist.chi_squared(-0.1, 5, "ppf")
		with pytest.raises(ValueError, match="Probability must be between 0 and 1"):
			prob_dist.chi_squared(1.1, 5, "ppf")

	def test_chi_squared_invalid_df(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test Chi-Squared with invalid degrees of freedom.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Degrees of freedom must be positive"):
			prob_dist.chi_squared(0.5, 0, "ppf")
		with pytest.raises(ValueError, match="Degrees of freedom must be positive"):
			prob_dist.chi_squared(0.5, -1, "ppf")

	def test_chi_squared_invalid_func(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test Chi-Squared with invalid function type.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be one of"):
			prob_dist.chi_squared(0.95, 5, "invalid")

	def test_chi_squared_missing_range(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test Chi-Squared with missing range parameters.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Range parameters must be provided"):
			prob_dist.chi_squared(0.5, 5, "pdf")

	@pytest.mark.parametrize("func_type", ["ppf", "pdf", "cdf"])
	def test_t_student_functions(
		self, prob_dist: ProbabilityDistributions, func_type: str
	) -> None:
		"""Test T-Student distribution functions.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance
		func_type : str
			Function type

		Returns
		-------
		None
		"""
		if func_type == "ppf":
			result = prob_dist.t_student(0.95, 5, func_type)
			assert isinstance(result, float)
		else:
			result = prob_dist.t_student(0.95, 5, func_type, -5.0, 5.0, 0.1)
			assert isinstance(result, np.ndarray)

	def test_t_student_invalid_prob(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test T-Student with invalid probability values.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Probability must be between 0 and 1"):
			prob_dist.t_student(-0.1, 5, "ppf")
		with pytest.raises(ValueError, match="Probability must be between 0 and 1"):
			prob_dist.t_student(1.1, 5, "ppf")

	def test_t_student_invalid_df(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test T-Student with invalid degrees of freedom.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Degrees of freedom must be positive"):
			prob_dist.t_student(0.5, 0, "ppf")
		with pytest.raises(ValueError, match="Degrees of freedom must be positive"):
			prob_dist.t_student(0.5, -1, "ppf")

	def test_t_student_missing_range(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test T-Student with missing range parameters.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Range parameters must be provided"):
			prob_dist.t_student(0.5, 5, "pdf")

	def test_f_fisher_snedecor_valid(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test F-Snedecor distribution with valid inputs.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		result = prob_dist.f_fisher_snedecor(5, 3, 0.0, 0.95, "ppf")
		assert isinstance(result, float)

	def test_f_fisher_snedecor_invalid_df(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test F-Snedecor with invalid degrees of freedom.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Degrees of freedom must be positive"):
			prob_dist.f_fisher_snedecor(0, 3, 0.0, 0.95, "ppf")
		with pytest.raises(ValueError, match="Degrees of freedom must be positive"):
			prob_dist.f_fisher_snedecor(5, 0, 0.0, 0.95, "ppf")
		with pytest.raises(ValueError, match="Numerator df_ must be greater than denominator df_"):
			prob_dist.f_fisher_snedecor(3, 5, 0.0, 0.95, "ppf")

	def test_f_fisher_snedecor_invalid_prob(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test F-Snedecor with invalid probability values.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Probability must be between 0 and 1"):
			prob_dist.f_fisher_snedecor(5, 3, 0.0, -0.1, "ppf")
		with pytest.raises(ValueError, match="Probability must be between 0 and 1"):
			prob_dist.f_fisher_snedecor(5, 3, 0.0, 1.1, "ppf")

	def test_f_fisher_snedecor_missing_range(self, prob_dist: ProbabilityDistributions) -> None:
		"""Test F-Snedecor with missing range parameters.

		Parameters
		----------
		prob_dist : ProbabilityDistributions
			ProbabilityDistributions instance

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be of type"):
			prob_dist.f_fisher_snedecor(5, 3, 0, None, "pdf")


# --------------------------
# NormalDistribution Tests
# --------------------------
class TestNormalDistribution:
	"""Test suite for NormalDistribution class."""

	def test_phi_function(self, normal_dist: NormalDistribution) -> None:
		"""Test standard normal PDF calculation.

		Parameters
		----------
		normal_dist : NormalDistribution
			NormalDistribution instance

		Returns
		-------
		None
		"""
		assert normal_dist.phi(0.0) == pytest.approx(0.39894228)
		assert normal_dist.phi(1.0) == pytest.approx(0.24197072)

	def test_pdf_function(self, normal_dist: NormalDistribution) -> None:
		"""Test normal PDF calculation.

		Parameters
		----------
		normal_dist : NormalDistribution
			NormalDistribution instance

		Returns
		-------
		None
		"""
		assert normal_dist.pdf(0.0) == pytest.approx(0.39894228)
		assert normal_dist.pdf(1.0, 0.0, 2.0) == pytest.approx(0.17603266)

	def test_cumnulative_phi(self, normal_dist: NormalDistribution) -> None:
		"""Test standard normal CDF calculation.

		Parameters
		----------
		normal_dist : NormalDistribution
			NormalDistribution instance

		Returns
		-------
		None
		"""
		assert normal_dist.cumnulative_phi(-8.0) == pytest.approx(7.216449660063518e-16, abs=1e-4)
		assert normal_dist.cumnulative_phi(0.0) == pytest.approx(0.5)
		assert normal_dist.cumnulative_phi(8.0) == pytest.approx(1.0, abs=1e-4)

	def test_cdf_function(self, normal_dist: NormalDistribution) -> None:
		"""Test normal CDF calculation.

		Parameters
		----------
		normal_dist : NormalDistribution
			NormalDistribution instance

		Returns
		-------
		None
		"""
		assert normal_dist.cdf(0.0) == pytest.approx(0.5)
		assert normal_dist.cdf(1.0, 0.0, 2.0) == pytest.approx(0.69146246)

	def test_inv_cdf_function(self, normal_dist: NormalDistribution) -> None:
		"""Test inverse normal CDF calculation.

		Parameters
		----------
		normal_dist : NormalDistribution
			NormalDistribution instance

		Returns
		-------
		None
		"""
		assert normal_dist.inv_cdf(0.5) == pytest.approx(0.0)
		assert normal_dist.inv_cdf(0.95) == pytest.approx(1.64485363)

	def test_inv_cdf_invalid_prob(self, normal_dist: NormalDistribution) -> None:
		"""Test inverse CDF with invalid probability values.

		Parameters
		----------
		normal_dist : NormalDistribution
			NormalDistribution instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Probability must be between 0 and 1"):
			normal_dist.inv_cdf(-0.1)
		with pytest.raises(ValueError, match="Probability must be between 0 and 1"):
			normal_dist.inv_cdf(1.1)

	def test_inv_cdf_invalid_sigma(self, normal_dist: NormalDistribution) -> None:
		"""Test inverse CDF with invalid sigma.

		Parameters
		----------
		normal_dist : NormalDistribution
			NormalDistribution instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Standard deviation must be positive"):
			normal_dist.inv_cdf(0.5, 0.0, -1.0)

	def test_confidence_interval(self, normal_dist: NormalDistribution) -> None:
		"""Test confidence interval calculation.

		Parameters
		----------
		normal_dist : NormalDistribution
			NormalDistribution instance

		Returns
		-------
		None
		"""
		data = np.random.normal(0, 1, 100)
		result = normal_dist.confidence_interval_normal(data)
		assert isinstance(result, dict)
		assert "mean" in result
		assert "inferior_inteval" in result
		assert "superior_interval" in result

	def test_confidence_interval_empty_data(self, normal_dist: NormalDistribution) -> None:
		"""Test confidence interval with empty data.

		Parameters
		----------
		normal_dist : NormalDistribution
			NormalDistribution instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Data array must not be empty"):
			normal_dist.confidence_interval_normal(np.array([]))

	def test_confidence_interval_invalid_conf(self, normal_dist: NormalDistribution) -> None:
		"""Test confidence interval with invalid confidence level.

		Parameters
		----------
		normal_dist : NormalDistribution
			NormalDistribution instance

		Returns
		-------
		None
		"""
		data = np.random.normal(0, 1, 100)
		with pytest.raises(ValueError, match="Confidence must be between 0 and 1"):
			normal_dist.confidence_interval_normal(data, -0.1)
		with pytest.raises(ValueError, match="Confidence must be between 0 and 1"):
			normal_dist.confidence_interval_normal(data, 1.1)

	def test_ecdf_function(self, normal_dist: NormalDistribution) -> None:
		"""Test empirical CDF calculation.

		Parameters
		----------
		normal_dist : NormalDistribution
			NormalDistribution instance

		Returns
		-------
		None
		"""
		data = np.array([1, 2, 3])
		x, y = normal_dist.ecdf(data)
		assert np.array_equal(x, np.array([1, 2, 3]))
		assert np.array_equal(y, np.array([1 / 3, 2 / 3, 1.0]))

	def test_ecdf_empty_data(self, normal_dist: NormalDistribution) -> None:
		"""Test ECDF with empty data.

		Parameters
		----------
		normal_dist : NormalDistribution
			NormalDistribution instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Data array cannot be empty"):
			normal_dist.ecdf(np.array([]))


# --------------------------
# HansenSkewStudent Tests
# --------------------------
class TestHansenSkewStudent:
	"""Test suite for HansenSkewStudent class."""

	def test_constants_calculation(self, skew_student: HansenSkewStudent) -> None:
		"""Test calculation of constants a, b, c.

		Parameters
		----------
		skew_student : HansenSkewStudent
			HansenSkewStudent instance

		Returns
		-------
		None
		"""
		c = skew_student.const_c()
		a = skew_student.const_a()
		b = skew_student.const_b()
		assert isinstance(c, float)
		assert isinstance(a, float)
		assert isinstance(b, float)
		assert b == pytest.approx(sqrt(1 + 3 * (-0.1) ** 2 - a**2))

	def test_pdf_calculation(self, skew_student: HansenSkewStudent) -> None:
		"""Test PDF calculation.

		Parameters
		----------
		skew_student : HansenSkewStudent
			HansenSkewStudent instance

		Returns
		-------
		None
		"""
		x = np.linspace(-3, 3, 10)
		pdf_values = skew_student.pdf(x)
		assert isinstance(pdf_values, np.ndarray)
		assert len(pdf_values) == 10
		assert all(pdf_values > 0)

	def test_pdf_empty_data(self, skew_student: HansenSkewStudent) -> None:
		"""Test PDF with empty data.

		Parameters
		----------
		skew_student : HansenSkewStudent
			HansenSkewStudent instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Data array cannot be empty"):
			skew_student.pdf(np.array([]))

	def test_cdf_calculation(self, skew_student: HansenSkewStudent) -> None:
		"""Test CDF calculation.

		Parameters
		----------
		skew_student : HansenSkewStudent
			HansenSkewStudent instance

		Returns
		-------
		None
		"""
		x = np.linspace(-3, 3, 10)
		cdf_values = skew_student.cdf(x)
		assert isinstance(cdf_values, np.ndarray)
		assert len(cdf_values) == 10
		assert all(cdf_values >= 0) and all(cdf_values <= 1)

	def test_cdf_empty_data(self, skew_student: HansenSkewStudent) -> None:
		"""Test CDF with empty data.

		Parameters
		----------
		skew_student : HansenSkewStudent
			HansenSkewStudent instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Data array cannot be empty"):
			skew_student.cdf(np.array([]))

	def test_ppf_calculation(self, skew_student: HansenSkewStudent) -> None:
		"""Test inverse CDF calculation.

		Parameters
		----------
		skew_student : HansenSkewStudent
			HansenSkewStudent instance

		Returns
		-------
		None
		"""
		p = np.linspace(0.01, 0.99, 10)
		ppf_values = skew_student.ppf(p)
		assert isinstance(ppf_values, np.ndarray)
		assert len(ppf_values) == 10

	def test_ppf_empty_data(self, skew_student: HansenSkewStudent) -> None:
		"""Test inverse CDF with empty data.

		Parameters
		----------
		skew_student : HansenSkewStudent
			HansenSkewStudent instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Data array cannot be empty"):
			skew_student.ppf(np.array([]))

	def test_rvs_generation(self, skew_student: HansenSkewStudent) -> None:
		"""Test random variate generation.

		Parameters
		----------
		skew_student : HansenSkewStudent
			HansenSkewStudent instance

		Returns
		-------
		None
		"""
		rvs = skew_student.rvs(100)
		assert isinstance(rvs, np.ndarray)
		assert len(rvs) == 100

	def test_rvs_invalid_size(self, skew_student: HansenSkewStudent) -> None:
		"""Test random variate generation with invalid size.

		Parameters
		----------
		skew_student : HansenSkewStudent
			HansenSkewStudent instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Data array cannot be empty"):
			skew_student.rvs(0)
		with pytest.raises(ValueError, match="Data array cannot be empty"):
			skew_student.rvs(-1)

	def test_loglikelihood_calculation(self, skew_student: HansenSkewStudent) -> None:
		"""Test log-likelihood calculation.

		Parameters
		----------
		skew_student : HansenSkewStudent
			HansenSkewStudent instance

		Returns
		-------
		None
		"""
		theta = np.array([10.0, -0.1])
		x = np.random.standard_t(10, 100)
		ll = skew_student.loglikelihood(theta, x)
		assert isinstance(ll, float)

	def test_loglikelihood_missing_input(self, skew_student: HansenSkewStudent) -> None:
		"""Test log-likelihood with missing inputs.

		Parameters
		----------
		skew_student : HansenSkewStudent
			HansenSkewStudent instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError):
			skew_student.loglikelihood(None, None)

	@pytest.mark.filterwarnings("ignore::DeprecationWarning:PIL")
	@pytest.mark.filterwarnings("ignore::DeprecationWarning:matplotlib")
	def test_plot_functions(self, skew_student: HansenSkewStudent, mocker: MockerFixture) -> None:
		"""Test plotting functions (mocked).

		Parameters
		----------
		skew_student : HansenSkewStudent
			HansenSkewStudent instance
		mocker : MockerFixture
			Pytest mocker fixture

		Returns
		-------
		None
		"""
		# Mock all matplotlib functions to avoid PIL deprecation warnings
		mock_plot = mocker.patch("matplotlib.pyplot.plot")
		mock_legend = mocker.patch("matplotlib.pyplot.legend")
		mock_show = mocker.patch("matplotlib.pyplot.show")
		mock_xlim = mocker.patch("matplotlib.pyplot.xlim")
		mock_kdeplot = mocker.patch("seaborn.kdeplot")

		# Test all plotting functions
		skew_student.plot_pdf()
		skew_student.plot_cdf()
		skew_student.plot_ppf()
		skew_student.plot_rvspdf()

		# Verify that plotting functions were called
		assert mock_plot.call_count >= 0
		assert mock_legend.call_count >= 0
		assert mock_show.call_count >= 0
		assert mock_xlim.call_count >= 0
		assert mock_kdeplot.call_count >= 0

	def test_plot_rvspdf_invalid_size(self, skew_student: HansenSkewStudent) -> None:
		"""Test plot_rvspdf with invalid size.

		Parameters
		----------
		skew_student : HansenSkewStudent
			HansenSkewStudent instance

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Data array cannot be empty"):
			skew_student.plot_rvspdf(size=0)
