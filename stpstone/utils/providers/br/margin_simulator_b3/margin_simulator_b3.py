"""API interface for B3 Margin Simulator calculations.

This module provides a class for interacting with the B3 Margin Simulator API to calculate
total deficit/surplus values based on portfolio positions. It handles authentication,
request formatting, and response processing.
"""

from typing import Optional, Union

import requests

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.json import JsonFiles
from stpstone.utils.providers.br.margin_simulator_b3._dto import (
	ResultReferenceData,
	ResultRiskCalculationResponse,
)
from stpstone.utils.providers.br.margin_simulator_b3._ports import IMarginSimulatorB3


class MarginSimulatorB3(metaclass=TypeChecker):
	"""Adapter that fulfils the ``IMarginSimulatorB3`` port for the B3 Margin Simulator.

	``metaclass=TypeChecker`` wires up runtime argument-type enforcement on every
	public method. The explicit ``isinstance`` guard in ``__init__`` below provides
	an immediate ``NotImplementedError`` if a subclass accidentally breaks the port
	contract.

	Parameters
	----------
	dict_payload : ResultReferenceData
		Payload dictionary for the B3 Margin Simulator API.
	timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		Request timeout (default: (12.0, 21.0)).
	bool_verify : bool
		Verify SSL certificate (default: True).
	dict_cookies : Optional[str]
		Override the default browser session cookies. When ``None`` (default)
		the class-level ``_DEFAULT_COOKIES`` are used.

	Raises
	------
	NotImplementedError
		If this class does not satisfy the ``IMarginSimulatorB3`` port — i.e., if a
		required interface method is missing or has an incompatible signature.
	"""

	_DEFAULT_COOKIES: str = (
		"_ga=GA1.1.246440908.1756239909; "
		"OptanonAlertBoxClosed=2025-09-08T21:47:45.364Z; "
		"dtCookie=v_4_srv_28_sn_D8C7583A06A92F3B5347E4BE125CCF9B_perc_100000_ol_0_"
		"mul_1_app-3Afd69ce40c52bd20e_0_rcs-3Acss_0; "
		"OptanonConsent=isGpcEnabled=0&datestamp=Sat+Sep+13+2025+21%3A32%3A31+GMT-0300+"
		"(Hor%C3%A1rio+Padr%C3%A3o+de+Bras%C3%ADlia)&version=6.21.0&isIABGlobal=false"
		"&hosts=&landingPath=NotLandingPage&groups=C0003%3A0%2CC0001%3A1%2CC0004%3A0"
		"%2CC0002%3A0&AwaitingReconsent=false&geolocation=%3B; "
		"_clck=e74245%5E2%5Efzc%5E0%5E2064; "
		"_ga_SS7FXRTPP3=GS2.1.s1757937548$o18$g0$t1757937711$j60$l0$h0; "
		"TS0171d45d=011d592ce104c8f8f7eb32cd43a5289cff0828640d5136972c883f14fe0cffbd3"
		"5871d32222d9f68d635d5ef16ba61468320a3c911; "
		"_clsk=l5ecfm%5E1757941260711%5E6%5E1%5Es.clarity.ms%2Fcollect; "
		"__cf_bm=kk1r6b7iD3PSKjPmxijF1VgowjvaH96lB66ZQ_pYmUw-1757944631-1.0.1.1-"
		"8d0wQaHtK1weCEgl7OzTTwWQSesLVcCyrR2yBrtNkxJ3E7T0X_RaiugxcvFenp2ukZgNZK898"
		"CjH8tRWcuNkMlHX8gTJKMw.RzeLG85Dakg; "
		"cf_clearance=tj4BCMcvQRdG4PaUKGZsAGqvosvytlrcoXAs5kIXlpI-1757944633-1.2.1.1-"
		"4zSfuc8PRPiWiuxJ0zjY3D_wRaEutkUlH5qf8UnRHL36pe7XZK4vmy5nTETNaW8Jyz43pkIxYj"
		"e4LUJhJSduq_LPehuK8TtS2mi04UEqtYLf2rypcszSyDzmyp6Vm.MinysekS5iLBEpSIi0RK8v"
		"I9UABzHuA6p3Yh6xIS33lmAKkQ5JuDcxfj801imUkjaNinkq4bUg.LXA2Jwphm3rqeDICU5PCB"
		"f0UaNKsyRcI40; "
		"TS01f22489=011d592ce165f2d7b0e6491e551d112e9fde2758a0fdab27e59b814d3b6978dc7"
		"af072e191124ebe7854b5115b475a3cbbf315dc66; "
		"TS01871345=016e3b076f5c41242222e2b5ca3a615f7c4280e992458fe20f6d4ad9d80ba0db1"
		"a0d2286fd815f92bc9ef8233e1dc7a39d09754f48"
	)

	def __init__(
		self,
		dict_payload: ResultReferenceData,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		dict_cookies: Optional[str] = None,
	) -> None:
		"""Initialize the MarginSimulatorB3 adapter.

		Parameters
		----------
		dict_payload : ResultReferenceData
			Payload dictionary for the B3 Margin Simulator API.
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
			Request timeout (default: (12.0, 21.0)).
		bool_verify : bool
			Verify SSL certificate (default: True).
		dict_cookies : Optional[str]
			Override the default browser session cookies. When ``None`` (default)
			the class-level ``_DEFAULT_COOKIES`` are used.

		Raises
		------
		NotImplementedError
			If this class does not satisfy the ``IMarginSimulatorB3`` port.
		"""
		self.dict_payload = dict_payload
		self.timeout = timeout
		self.bool_verify = bool_verify
		self._cookies = dict_cookies or self._DEFAULT_COOKIES
		self.url_reference_data = (
			"https://simulador.b3.com.br/api/cors-app/web/V1.0/ReferenceData"
		)
		self.url_risk_calc = (
			"https://simulador.b3.com.br/api/cors-app/web/V1.0/RiskCalculation"
		)
		self.token = self._get_reference_data()["ReferenceData"]["referenceDataToken"]
		self.cls_json_files = JsonFiles()

		if not isinstance(self, IMarginSimulatorB3):
			raise NotImplementedError(
				f"{type(self).__name__} does not satisfy IMarginSimulatorB3 — "
				"implement _get_reference_data() and risk_calculation()."
			)

	def _get_reference_data(self) -> ResultReferenceData:
		"""Get Reference Data from the B3 Margin Simulator API.

		Returns
		-------
		ResultReferenceData
			JSON response as dictionary containing the reference data token and security groups.
		"""
		dict_headers = {
			"accept": "application/json, text/plain, */*",
			"accept-language": "en-US,en;q=0.9,pt;q=0.8,es;q=0.7",
			"priority": "u=1, i",
			"referer": "https://simulador.b3.com.br/",
			"sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
			"sec-ch-ua-mobile": "?0",
			"sec-ch-ua-platform": '"Linux"',
			"sec-fetch-dest": "empty",
			"sec-fetch-mode": "cors",
			"sec-fetch-site": "same-origin",
			"user-agent": (
				"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
				"(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
			),
			"Cookie": self._cookies,
		}
		resp_req = requests.get(
			self.url_reference_data,
			timeout=self.timeout,
			headers=dict_headers,
			verify=self.bool_verify,
		)
		resp_req.raise_for_status()
		return resp_req.json()

	def risk_calculation(self) -> ResultRiskCalculationResponse:
		"""Execute risk calculation via the B3 Margin Simulator API.

		Returns
		-------
		ResultRiskCalculationResponse
			JSON response as dictionary containing the risk metrics.
		"""
		dict_payload = self.dict_payload
		dict_payload["ReferenceData"] = {"referenceDataToken": self.token}
		dict_payload = self.cls_json_files.dict_to_json(dict_payload)

		dict_headers = {
			"accept": "application/json, text/plain, */*",
			"accept-language": "en-US,en;q=0.9,pt;q=0.8,es;q=0.7",
			"content-type": "application/json",
			"origin": "https://simulador.b3.com.br",
			"priority": "u=1, i",
			"referer": "https://simulador.b3.com.br/",
			"sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
			"sec-ch-ua-mobile": "?0",
			"sec-ch-ua-platform": '"Linux"',
			"sec-fetch-dest": "empty",
			"sec-fetch-mode": "cors",
			"sec-fetch-site": "same-origin",
			"user-agent": (
				"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
				"(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
			),
			"Cookie": self._cookies,
		}

		resp_req = requests.post(
			self.url_risk_calc,
			headers=dict_headers,
			data=dict_payload,
			timeout=self.timeout,
			verify=self.bool_verify,
		)
		resp_req.raise_for_status()

		return resp_req.json()
