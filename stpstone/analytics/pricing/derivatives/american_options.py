"""American options pricing models."""

from typing import Literal

import numpy as np

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class InitialSettings(metaclass=TypeChecker):
	"""Initial settings for American options pricing models."""

	def set_parameters(
		self,
		s: float,
		k: float,
		r: float,
		t: float,
		n: int,
		u: float,
		d: float,
		opt_style: Literal["call", "put"] = "call",
	) -> tuple[float, float, float, float, int, float, float]:
		"""Set parameters for the pricing model.

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
		opt_style : Literal['call', 'put']
			Option style ('call' or 'put')

		Returns
		-------
		Tuple[float, float, float, float, int, float, float]
			A tuple containing:
			- s_val: Current stock price
			- k_val: Strike price of the option
			- r_val: Risk-free interest rate
			- t_val: Time to maturity in years
			- n_val: Number of time steps
			- u_val: Up-factor in binomial models
			- d_val: Down-factor in binomial models

		Raises
		------
		ValueError
			If any of the parameters are invalid.
			If stock price is negative.
			If strike price is negative.
			If time to maturity is negative.
			If number of steps is negative.
			If up factor is not positive.
			If down factor is not positive.
			If up factor is not greater than down factor.
		"""
		try:
			# First validate the option style
			if opt_style not in ["call", "put"]:
				raise ValueError("Option style must be either 'call' or 'put'")

			# convert all numeric parameters to appropriate types
			s_val = float(s)
			k_val = float(k)
			r_val = float(r)
			t_val = float(t)
			n_val = int(float(n))
			u_val = float(u)
			d_val = float(d)

			# validate parameter ranges
			if s_val < 0:
				raise ValueError("Stock price must be non-negative")
			if k_val < 0:
				raise ValueError("Strike price must be non-negative")
			if t_val < 0:
				raise ValueError("Time to maturity must be non-negative")
			if n_val < 0:
				raise ValueError("Number of steps must be non-negative")
			if u_val <= 0:
				raise ValueError("Up factor must be positive")
			if d_val <= 0:
				raise ValueError("Down factor must be positive")
			if u_val <= d_val:
				raise ValueError("Up factor must be greater than down factor")

			# validate parameter ranges
			if any(x < 0 for x in (s_val, k_val, t_val, n_val, u_val, d_val)):
				raise ValueError(
					"All parameters must be non-negative except possibly interest rate"
				)
			if u_val <= d_val:
				raise ValueError("Up factor must be greater than down factor")
			if u_val <= 0 or d_val <= 0:
				raise ValueError("Up and down factors must be positive")

		except (TypeError, ValueError) as e:
			raise ValueError(f"Invalid parameter: {str(e)}") from e

		return s_val, k_val, r_val, t_val, n_val, u_val, d_val


class PricingModels(InitialSettings):
	"""Pricing models for American options."""

	def binomial(
		self,
		s: float,
		k: float,
		r: float,
		t: float,
		n: int,
		u: float,
		d: float,
		opt_style: Literal["call", "put"] = "call",
		h_upper: float = None,
		h_lower: float = None,
	) -> float:
		"""Binomial pricing model for American options.

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
		opt_style : Literal['call', 'put']
			Option style, either 'call' or 'put'.
		h_upper : float
			Upper barrier.
		h_lower : float
			Lower barrier.

		Returns
		-------
		float
			Option value.

		Raises
		------
		ValueError
			If any of the parameters are invalid.

		References
		----------
		https://www.youtube.com/watch?v=a3906k9C0fM,
		https://www.youtube.com/watch?v=WxrRi9lNnqY,
		https://www.youtube.com/watch?v=K2Iy8bCmXjk
		"""
		# initial parameters
		s, k, r, t, n, u, d = self.set_parameters(s, k, r, t, n, u, d, opt_style)
		# handle edge case when time to maturity is zero
		if t == 0:
			if opt_style == "call":
				return float(max(s - k, 0))
			else:
				return float(max(k - s, 0))
		# validate barriers
		if h_upper is not None and h_lower is not None and h_upper <= h_lower:
			raise ValueError("Upper barrier must be greater than lower barrier")
		# precompute values
		dt = t / n
		q = (np.exp(r * dt) - d) / (u - d)
		disc = np.exp(-r * dt)
		# initialise stock prices at maturity
		array_s_nodes = s * d ** (np.arange(n, -1, -1)) * u ** (np.arange(0, n + 1, 1))
		# initialise option values at maturity - if intrinsic value is negative, consider zero
		if opt_style == "call":
			array_cp = np.maximum(array_s_nodes - k, np.zeros(int(n) + 1))
		else:
			array_cp = np.maximum(k - array_s_nodes, np.zeros(int(n) + 1))
		# check s payoff, according to barriers, if values are different from none
		if h_upper is not None:
			array_cp[array_s_nodes >= h_upper] = 0
		if h_lower is not None:
			array_cp[array_s_nodes <= h_lower] = 0
		# step backwards recursion through tree
		for i in np.arange(int(n) - 1, -1, -1):
			array_s_nodes = s * d ** (np.arange(i, -1, -1)) * u ** (np.arange(0, i + 1, 1))
			array_cp[: i + 1] = disc * (q * array_cp[1 : i + 2] + (1 - q) * array_cp[0 : i + 1])
			array_cp = array_cp[:-1]
			#   evaluating maximum value between fair and intrinsic value
			if opt_style == "call":
				array_cp = np.maximum(array_cp, array_s_nodes - k)
			else:
				array_cp = np.maximum(array_cp, k - array_s_nodes)
		# returning the no-arbitrage price at node 0  # noqa: ERA001
		return array_cp[0]
