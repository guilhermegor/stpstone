"""Protocols for B3 LINE API client.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
.. [2] https://line.bvmfnet.com.br/#/endpoints
"""

from datetime import date, datetime
from typing import Any, Literal, Optional, Protocol, Union, runtime_checkable


@runtime_checkable
class IConnectionApi(Protocol):
	"""Structural protocol for B3 LINE API connection adapters."""

	broker_code: str
	category_code: str

	def auth_header(
		self,
		int_max_retries: int = 2,
		timeout: Union[tuple[float, float], float, int] = 10,
	) -> str:
		"""Get authentication header for API requests.

		Parameters
		----------
		int_max_retries : int
			Maximum number of retries, by default 2.
		timeout : Union[tuple[float, float], float, int]
			Request timeout, by default 10.

		Returns
		-------
		str
			The authentication header.
		"""
		...

	def access_token(
		self,
		int_max_retries: int = 2,
		timeout: Union[tuple[float, float], float, int] = 10,
	) -> str:
		"""Get access token for API requests.

		Parameters
		----------
		int_max_retries : int
			Maximum number of retries, by default 2.
		timeout : Union[tuple[float, float], float, int]
			Request timeout, by default 10.

		Returns
		-------
		str
			The access token.
		"""
		...

	def app_request(
		self,
		method: Literal['GET', 'POST', 'DELETE'],
		app: str,
		dict_params: Optional[dict[str, Any]] = None,
		dict_payload: Optional[list[dict[str, Any]]] = None,
		bool_parse_dict_params_data: bool = False,
		bool_retry_if_error: bool = False,
		float_secs_sleep: Optional[float] = None,
		timeout: Union[tuple[float, float], float, int] = 10,
	) -> Union[list[dict[str, Any]], int]:
		"""Make an API request to the B3 LINE application.

		Parameters
		----------
		method : Literal['GET', 'POST', 'DELETE']
			The HTTP method.
		app : str
			The application endpoint.
		dict_params : Optional[dict[str, Any]]
			Query parameters, by default None.
		dict_payload : Optional[list[dict[str, Any]]]
			Request payload, by default None.
		bool_parse_dict_params_data : bool
			Whether to parse dict params data, by default False.
		bool_retry_if_error : bool
			Whether to retry on error, by default False.
		float_secs_sleep : Optional[float]
			Sleep duration between retries, by default None.
		timeout : Union[tuple[float, float], float, int]
			Request timeout, by default 10.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The API response or status code.
		"""
		...


@runtime_checkable
class IOperations(Protocol):
	"""Structural protocol for B3 LINE API operations adapters."""

	def app_request(
		self,
		method: Literal['GET', 'POST', 'DELETE'],
		app: str,
		dict_params: Optional[dict[str, Any]] = None,
		dict_payload: Optional[list[dict[str, Any]]] = None,
		bool_parse_dict_params_data: bool = False,
		bool_retry_if_error: bool = False,
		float_secs_sleep: Optional[float] = None,
		timeout: Union[tuple[float, float], float, int] = 10,
	) -> Union[list[dict[str, Any]], int]:
		"""Make an API request to the B3 LINE application.

		Parameters
		----------
		method : Literal['GET', 'POST', 'DELETE']
			The HTTP method.
		app : str
			The application endpoint.
		dict_params : Optional[dict[str, Any]]
			Query parameters, by default None.
		dict_payload : Optional[list[dict[str, Any]]]
			Request payload, by default None.
		bool_parse_dict_params_data : bool
			Whether to parse dict params data, by default False.
		bool_retry_if_error : bool
			Whether to retry on error, by default False.
		float_secs_sleep : Optional[float]
			Sleep duration between retries, by default None.
		timeout : Union[tuple[float, float], float, int]
			Request timeout, by default 10.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The API response or status code.
		"""
		...

	def exchange_limits(self) -> Union[list[dict[str, Any]], int]:
		"""Get exchange limits.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The exchange limits or status code.
		"""
		...

	def groups_authorized_markets(self) -> Union[list[dict[str, Any]], int]:
		"""Get authorized markets grouped.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The authorized markets groups or status code.
		"""
		...

	def intruments_per_group(
		self, group_id: str
	) -> Union[list[dict[str, Any]], int]:
		"""Get instruments for a group.

		Parameters
		----------
		group_id : str
			The group identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The instruments or status code.
		"""
		...

	def authorized_markets_instruments(
		self,
	) -> dict[str, Union[str, int, float]]:
		"""Get authorized market instruments.

		Returns
		-------
		dict[str, Union[str, int, float]]
			The authorized market instruments.
		"""
		...


@runtime_checkable
class IResources(Protocol):
	"""Structural protocol for B3 LINE API resource adapters."""

	def instrument_informations(self) -> Union[list[dict[str, Any]], int]:
		"""Get instrument information.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The instrument information or status code.
		"""
		...

	def instrument_infos_exchange_limits(
		self,
	) -> dict[str, dict[str, Union[str, int, float]]]:
		"""Get instrument exchange limits information.

		Returns
		-------
		dict[str, dict[str, Union[str, int, float]]]
			The exchange limits by instrument.
		"""
		...

	def instrument_id_by_symbol(
		self, symbol: str
	) -> Union[list[dict[str, Any]], int]:
		"""Get instrument ID by symbol.

		Parameters
		----------
		symbol : str
			The instrument symbol.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The instrument ID or status code.
		"""
		...


@runtime_checkable
class IAccountsData(Protocol):
	"""Structural protocol for B3 LINE API account data adapters."""

	def client_infos(self, account_code: str) -> Union[list[dict[str, Any]], int]:
		"""Get client information.

		Parameters
		----------
		account_code : str
			The account code.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The client information or status code.
		"""
		...

	def spxi_get(self, account_id: str) -> Union[list[dict[str, Any]], int]:
		"""Get SPXI data for account.

		Parameters
		----------
		account_id : str
			The account identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The SPXI data or status code.
		"""
		...

	def spxi_instrument_post(
		self,
		account_id: str,
		dict_payload: Optional[list[dict[str, Any]]],
	) -> Union[list[dict[str, Any]], int]:
		"""Post SPXI instrument for account.

		Parameters
		----------
		account_id : str
			The account identifier.
		dict_payload : Optional[list[dict[str, Any]]]
			The instrument data payload.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The response or status code.
		"""
		...

	def spxi_instrument_delete(
		self,
		account_id: str,
		dict_payload: Optional[list[dict[str, Any]]],
	) -> Union[list[dict[str, Any]], int]:
		"""Delete SPXI instrument for account.

		Parameters
		----------
		account_id : str
			The account identifier.
		dict_payload : Optional[list[dict[str, Any]]]
			The instrument data payload.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The response or status code.
		"""
		...

	def spxi_tmox_global_metrics_remove(
		self, account_id: str
	) -> Union[list[dict[str, Any]], int]:
		"""Remove SPXI TMOX global metrics.

		Parameters
		----------
		account_id : str
			The account identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The response or status code.
		"""
		...

	def specific_global_metric_remotion(
		self, account_id: str, metric: str
	) -> Union[list[dict[str, Any]], int]:
		"""Remove specific global metric.

		Parameters
		----------
		account_id : str
			The account identifier.
		metric : str
			The metric name.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The response or status code.
		"""
		...


@runtime_checkable
class IDocumentsData(Protocol):
	"""Structural protocol for B3 LINE API document data adapters."""

	def doc_info(self, doc_code: str) -> Union[list[dict[str, Any]], int]:
		"""Get document information.

		Parameters
		----------
		doc_code : str
			The document code.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The document information or status code.
		"""
		...

	def block_unblock_doc(self, doc_id: str) -> Union[list[dict[str, Any]], int]:
		"""Block or unblock a document.

		Parameters
		----------
		doc_id : str
			The document identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The response or status code.
		"""
		...

	def update_profile(
		self,
		doc_id: str,
		doc_profile_id: str,
		int_rmkt_evaluation: int = 0,
	) -> Union[list[dict[str, Any]], int]:
		"""Update document profile.

		Parameters
		----------
		doc_id : str
			The document identifier.
		doc_profile_id : str
			The document profile identifier.
		int_rmkt_evaluation : int
			The risk market evaluation, by default 0.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The response or status code.
		"""
		...

	def is_protection_mode(
		self, doc_id: str
	) -> Union[list[dict[str, Any]], int]:
		"""Check if document is in protection mode.

		Parameters
		----------
		doc_id : str
			The document identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The protection mode status or status code.
		"""
		...

	def client_infos(self, doc_id: str) -> Union[list[dict[str, Any]], int]:
		"""Get client information by document.

		Parameters
		----------
		doc_id : str
			The document identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The client information or status code.
		"""
		...

	def doc_profile(self, doc_id: str) -> dict[str, Union[str, int]]:
		"""Get document profile.

		Parameters
		----------
		doc_id : str
			The document identifier.

		Returns
		-------
		dict[str, Union[str, int]]
			The document profile.
		"""
		...

	def spxi_get(self, doc_id: str) -> Union[list[dict[str, Any]], int]:
		"""Get SPXI data by document.

		Parameters
		----------
		doc_id : str
			The document identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The SPXI data or status code.
		"""
		...

	def spxi_instrument_post(
		self,
		doc_id: str,
		dict_payload: Optional[list[dict[str, Any]]],
	) -> Union[list[dict[str, Any]], int]:
		"""Post SPXI instrument by document.

		Parameters
		----------
		doc_id : str
			The document identifier.
		dict_payload : Optional[list[dict[str, Any]]]
			The instrument data payload.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The response or status code.
		"""
		...

	def spxi_instrument_delete(
		self,
		doc_id: str,
		dict_payload: Optional[list[dict[str, Any]]],
	) -> Union[list[dict[str, Any]], int]:
		"""Delete SPXI instrument by document.

		Parameters
		----------
		doc_id : str
			The document identifier.
		dict_payload : Optional[list[dict[str, Any]]]
			The instrument data payload.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The response or status code.
		"""
		...


@runtime_checkable
class IProfessional(Protocol):
	"""Structural protocol for B3 LINE API professional data adapters."""

	def professional_code_get(self) -> Union[list[dict[str, Any]], int]:
		"""Get professional code.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The professional code or status code.
		"""
		...

	def professional_historic_position(
		self,
		professional_code: str,
		date_start: Union[datetime, date],
		date_end: Union[datetime, date],
		int_participant_perspective_type: int = 0,
		entity_type: int = 4,
	) -> Union[list[dict[str, Any]], int]:
		"""Get professional historic position.

		Parameters
		----------
		professional_code : str
			The professional code.
		date_start : Union[datetime, date]
			The start date.
		date_end : Union[datetime, date]
			The end date.
		int_participant_perspective_type : int
			The participant perspective type, by default 0.
		entity_type : int
			The entity type, by default 4.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The historic position or status code.
		"""
		...


@runtime_checkable
class IProfilesData(Protocol):
	"""Structural protocol for B3 LINE API risk profile data adapters."""

	def risk_profile(self) -> Union[list[dict[str, Any]], int]:
		"""Get risk profile.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The risk profile or status code.
		"""
		...

	def entities_associated_profile(
		self, id_profile: str
	) -> Union[list[dict[str, Any]], int]:
		"""Get entities associated with profile.

		Parameters
		----------
		id_profile : str
			The profile identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The associated entities or status code.
		"""
		...

	def profile_global_limits_get(
		self, prof_id: str
	) -> Union[list[dict[str, Any]], int]:
		"""Get profile global limits.

		Parameters
		----------
		prof_id : str
			The profile identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The global limits or status code.
		"""
		...

	def profile_market_limits_get(
		self, prof_id: str
	) -> Union[list[dict[str, Any]], int]:
		"""Get profile market limits.

		Parameters
		----------
		prof_id : str
			The profile identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The market limits or status code.
		"""
		...

	def profile_spxi_limits_get(
		self, prof_id: str
	) -> Union[list[dict[str, Any]], int]:
		"""Get profile SPXI limits.

		Parameters
		----------
		prof_id : str
			The profile identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The SPXI limits or status code.
		"""
		...

	def profile_tmox_limits_get(
		self, prof_id: str
	) -> Union[list[dict[str, Any]], int]:
		"""Get profile TMOX limits.

		Parameters
		----------
		prof_id : str
			The profile identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The TMOX limits or status code.
		"""
		...

	def spxi_instrument_post(
		self,
		prof_id: str,
		dict_payload: list[dict[str, Any]],
	) -> Union[list[dict[str, Any]], int]:
		"""Post SPXI instrument to profile.

		Parameters
		----------
		prof_id : str
			The profile identifier.
		dict_payload : list[dict[str, Any]]
			The instrument data payload.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The response or status code.
		"""
		...


@runtime_checkable
class IMonitoring(Protocol):
	"""Structural protocol for B3 LINE API monitoring adapters."""

	def alerts(self) -> Union[list[dict[str, Any]], int]:
		"""Get alerts.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The alerts or status code.
		"""
		...


@runtime_checkable
class ISystemEventManagement(Protocol):
	"""Structural protocol for B3 LINE API system event adapters."""

	def report(
		self,
		date_start: Union[datetime, date],
		date_end: Union[datetime, date],
		str_start_time: str = "00:00",
		str_sup_time: str = "23:59",
		int_entity_type: int = 3,
	) -> Union[list[dict[str, Any]], int]:
		"""Get system event report.

		Parameters
		----------
		date_start : Union[datetime, date]
			The start date.
		date_end : Union[datetime, date]
			The end date.
		str_start_time : str
			The start time, by default "00:00".
		str_sup_time : str
			The end time, by default "23:59".
		int_entity_type : int
			The entity type, by default 3.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			The event report or status code.
		"""
		...
