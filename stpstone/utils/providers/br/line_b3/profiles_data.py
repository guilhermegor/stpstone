"""B3 LINE API risk profile data adapter.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
.. [2] https://line.bvmfnet.com.br/#/endpoints
"""

from typing import Any, Union

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.providers.br.line_b3._ports import IConnectionApi, IProfilesData


class ProfilesData(metaclass=TypeChecker):
	"""Adapter that fulfils the ``IProfilesData`` port for B3 LINE API risk profiles.

	Parameters
	----------
	conn : IConnectionApi
		Connection adapter that handles authentication and raw HTTP.

	Raises
	------
	NotImplementedError
		If this class does not satisfy the ``IProfilesData`` port.
	"""

	def __init__(self, conn: IConnectionApi) -> None:
		"""Initialize ProfilesData with an ``IConnectionApi`` dependency.

		Parameters
		----------
		conn : IConnectionApi
			Connection adapter that handles authentication and raw HTTP.

		Raises
		------
		NotImplementedError
			If this class does not satisfy the ``IProfilesData`` port.
		"""
		self._conn = conn

		if not isinstance(self, IProfilesData):
			raise NotImplementedError(
				f"{type(self).__name__} does not satisfy IProfilesData — "
				"implement all required profile methods."
			)

	def risk_profile(self) -> Union[list[dict[str, Any]], int]:
		"""Retrieve available risk profiles.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Profile data or status code.
		"""
		return self._conn.app_request(method="GET", app="/api/v1.0/riskProfile")

	def entities_associated_profile(self, id_profile: str) -> Union[list[dict[str, Any]], int]:
		"""Retrieve entities associated with a risk profile.

		Parameters
		----------
		id_profile : str
			Profile identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Entity data or status code.
		"""
		dict_params = {
			"id": id_profile,
			"participantCode": self._conn.broker_code,
			"pnpCode": self._conn.category_code,
		}
		return self._conn.app_request(
			method="GET",
			app="/api/v1.0/riskProfile/enty",
			dict_params=dict_params,
			bool_retry_if_error=True,
		)

	def profile_global_limits_get(self, prof_id: str) -> Union[list[dict[str, Any]], int]:
		"""Retrieve global limits for a risk profile.

		Parameters
		----------
		prof_id : str
			Profile identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Limits data or status code.
		"""
		return self._conn.app_request(method="GET", app=f"/api/v1.0/riskProfile/{prof_id}/lmt")

	def profile_market_limits_get(self, prof_id: str) -> Union[list[dict[str, Any]], int]:
		"""Retrieve market limits for a risk profile.

		Parameters
		----------
		prof_id : str
			Profile identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Limits data or status code.
		"""
		return self._conn.app_request(
			method="GET",
			app=f"/api/v1.0/riskProfile/{prof_id}/lmt/mkta",
			bool_retry_if_error=True,
		)

	def profile_spxi_limits_get(self, prof_id: str) -> Union[list[dict[str, Any]], int]:
		"""Retrieve SPCI/SPVI limits for a risk profile.

		Parameters
		----------
		prof_id : str
			Profile identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Limits data or status code.
		"""
		return self._conn.app_request(
			method="GET", app=f"/api/v1.0/riskProfile/{prof_id}/lmt/spxi"
		)

	def profile_tmox_limits_get(self, prof_id: str) -> Union[list[dict[str, Any]], int]:
		"""Retrieve TMOC/TMOV limits for a risk profile.

		Parameters
		----------
		prof_id : str
			Profile identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Limits data or status code.
		"""
		return self._conn.app_request(
			method="GET", app=f"/api/v1.0/riskProfile/{prof_id}/lmt/tmox"
		)

	def spxi_instrument_post(
		self,
		prof_id: str,
		dict_payload: list[dict[str, Any]],
	) -> Union[list[dict[str, Any]], int]:
		"""Set TMOC/TMOV limits for profile instruments.

		Parameters
		----------
		prof_id : str
			Profile identifier.
		dict_payload : list[dict[str, Any]]
			List of instrument limit dictionaries.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Result data or status code.
		"""
		return self._conn.app_request(
			method="POST",
			app=f"/api/v1.0/riskProfile/{prof_id}/lmt/tmox",
			dict_payload=dict_payload,
			bool_parse_dict_params_data=True,
			bool_retry_if_error=True,
		)
