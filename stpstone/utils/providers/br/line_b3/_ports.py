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
	) -> str: ...

	def access_token(
		self,
		int_max_retries: int = 2,
		timeout: Union[tuple[float, float], float, int] = 10,
	) -> str: ...

	def app_request(
		self,
		method: Literal["GET", "POST", "DELETE"],
		app: str,
		dict_params: Optional[dict[str, Any]] = None,
		dict_payload: Optional[list[dict[str, Any]]] = None,
		bool_parse_dict_params_data: bool = False,
		bool_retry_if_error: bool = False,
		float_secs_sleep: Optional[float] = None,
		timeout: Union[tuple[float, float], float, int] = 10,
	) -> Union[list[dict[str, Any]], int]: ...


@runtime_checkable
class IOperations(Protocol):
	"""Structural protocol for B3 LINE API operations adapters."""

	def app_request(
		self,
		method: Literal["GET", "POST", "DELETE"],
		app: str,
		dict_params: Optional[dict[str, Any]] = None,
		dict_payload: Optional[list[dict[str, Any]]] = None,
		bool_parse_dict_params_data: bool = False,
		bool_retry_if_error: bool = False,
		float_secs_sleep: Optional[float] = None,
		timeout: Union[tuple[float, float], float, int] = 10,
	) -> Union[list[dict[str, Any]], int]: ...

	def exchange_limits(self) -> Union[list[dict[str, Any]], int]: ...

	def groups_authorized_markets(self) -> Union[list[dict[str, Any]], int]: ...

	def intruments_per_group(
		self, group_id: str
	) -> Union[list[dict[str, Any]], int]: ...

	def authorized_markets_instruments(
		self,
	) -> dict[str, Union[str, int, float]]: ...


@runtime_checkable
class IResources(Protocol):
	"""Structural protocol for B3 LINE API resource adapters."""

	def instrument_informations(self) -> Union[list[dict[str, Any]], int]: ...

	def instrument_infos_exchange_limits(
		self,
	) -> dict[str, dict[str, Union[str, int, float]]]: ...

	def instrument_id_by_symbol(
		self, symbol: str
	) -> Union[list[dict[str, Any]], int]: ...


@runtime_checkable
class IAccountsData(Protocol):
	"""Structural protocol for B3 LINE API account data adapters."""

	def client_infos(self, account_code: str) -> Union[list[dict[str, Any]], int]: ...

	def spxi_get(self, account_id: str) -> Union[list[dict[str, Any]], int]: ...

	def spxi_instrument_post(
		self,
		account_id: str,
		dict_payload: Optional[list[dict[str, Any]]],
	) -> Union[list[dict[str, Any]], int]: ...

	def spxi_instrument_delete(
		self,
		account_id: str,
		dict_payload: Optional[list[dict[str, Any]]],
	) -> Union[list[dict[str, Any]], int]: ...

	def spxi_tmox_global_metrics_remove(
		self, account_id: str
	) -> Union[list[dict[str, Any]], int]: ...

	def specific_global_metric_remotion(
		self, account_id: str, metric: str
	) -> Union[list[dict[str, Any]], int]: ...


@runtime_checkable
class IDocumentsData(Protocol):
	"""Structural protocol for B3 LINE API document data adapters."""

	def doc_info(self, doc_code: str) -> Union[list[dict[str, Any]], int]: ...

	def block_unblock_doc(self, doc_id: str) -> Union[list[dict[str, Any]], int]: ...

	def update_profile(
		self,
		doc_id: str,
		doc_profile_id: str,
		int_rmkt_evaluation: int = 0,
	) -> Union[list[dict[str, Any]], int]: ...

	def is_protection_mode(
		self, doc_id: str
	) -> Union[list[dict[str, Any]], int]: ...

	def client_infos(self, doc_id: str) -> Union[list[dict[str, Any]], int]: ...

	def doc_profile(self, doc_id: str) -> dict[str, Union[str, int]]: ...

	def spxi_get(self, doc_id: str) -> Union[list[dict[str, Any]], int]: ...

	def spxi_instrument_post(
		self,
		doc_id: str,
		dict_payload: Optional[list[dict[str, Any]]],
	) -> Union[list[dict[str, Any]], int]: ...

	def spxi_instrument_delete(
		self,
		doc_id: str,
		dict_payload: Optional[list[dict[str, Any]]],
	) -> Union[list[dict[str, Any]], int]: ...


@runtime_checkable
class IProfessional(Protocol):
	"""Structural protocol for B3 LINE API professional data adapters."""

	def professional_code_get(self) -> Union[list[dict[str, Any]], int]: ...

	def professional_historic_position(
		self,
		professional_code: str,
		date_start: Union[datetime, date],
		date_end: Union[datetime, date],
		int_participant_perspective_type: int = 0,
		entity_type: int = 4,
	) -> Union[list[dict[str, Any]], int]: ...


@runtime_checkable
class IProfilesData(Protocol):
	"""Structural protocol for B3 LINE API risk profile data adapters."""

	def risk_profile(self) -> Union[list[dict[str, Any]], int]: ...

	def entities_associated_profile(
		self, id_profile: str
	) -> Union[list[dict[str, Any]], int]: ...

	def profile_global_limits_get(
		self, prof_id: str
	) -> Union[list[dict[str, Any]], int]: ...

	def profile_market_limits_get(
		self, prof_id: str
	) -> Union[list[dict[str, Any]], int]: ...

	def profile_spxi_limits_get(
		self, prof_id: str
	) -> Union[list[dict[str, Any]], int]: ...

	def profile_tmox_limits_get(
		self, prof_id: str
	) -> Union[list[dict[str, Any]], int]: ...

	def spxi_instrument_post(
		self,
		prof_id: str,
		dict_payload: list[dict[str, Any]],
	) -> Union[list[dict[str, Any]], int]: ...


@runtime_checkable
class IMonitoring(Protocol):
	"""Structural protocol for B3 LINE API monitoring adapters."""

	def alerts(self) -> Union[list[dict[str, Any]], int]: ...


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
	) -> Union[list[dict[str, Any]], int]: ...
