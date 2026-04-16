"""B3 LINE API market resources adapter.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
.. [2] https://line.bvmfnet.com.br/#/endpoints
"""

from typing import Any, Union

import pandas as pd

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.providers.br.line_b3._ports import IOperations, IResources


class Resources(metaclass=TypeChecker):
	"""Adapter that fulfils the ``IResources`` port for B3 LINE API market resources.

	Receives an ``IOperations`` dependency which exposes both ``app_request`` (for
	direct API calls) and ``exchange_limits`` (for limit enrichment), so this adapter
	needs only a single injected collaborator.

	Parameters
	----------
	ops : IOperations
		Operations adapter providing API access and exchange limits.

	Raises
	------
	NotImplementedError
		If this class does not satisfy the ``IResources`` port.
	"""

	def __init__(self, ops: IOperations) -> None:
		"""Initialize Resources with an ``IOperations`` dependency.

		Parameters
		----------
		ops : IOperations
			Operations adapter providing API access and exchange limits.

		Raises
		------
		NotImplementedError
			If this class does not satisfy the ``IResources`` port.
		"""
		self._ops = ops

		if not isinstance(self, IResources):
			raise NotImplementedError(
				f"{type(self).__name__} does not satisfy IResources — "
				"implement instrument_informations(), instrument_infos_exchange_limits(), "
				"and instrument_id_by_symbol()."
			)

	def instrument_informations(self) -> Union[list[dict[str, Any]], int]:
		"""Retrieve basic instrument information.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Instrument data or status code.
		"""
		return self._ops.app_request(
			method="GET",
			app="/api/v1.0/symbol",
			bool_retry_if_error=True,
		)

	def instrument_infos_exchange_limits(
		self,
	) -> dict[str, dict[str, Union[str, int, float]]]:
		"""Combine instrument info with exchange limits.

		Returns
		-------
		dict[str, dict[str, Union[str, int, float]]]
			Combined instrument data keyed by symbol.
		"""
		df_exchange_limits = pd.DataFrame.from_dict(self._ops.exchange_limits())
		df_exchange_limits = df_exchange_limits.astype({"instrumentId": str})

		df_instrument_informations = pd.DataFrame.from_dict(self.instrument_informations())
		df_instrument_informations = df_instrument_informations.astype({"id": str})

		df_join_instruments = df_instrument_informations.merge(
			df_exchange_limits,
			how="left",
			left_on="id",
			right_on="instrumentId",
		)

		df_join_instruments = df_join_instruments.rename(columns={"symbol_x": "symbol"}).drop(
			columns=["symbol_y"]
		)

		return {
			row["symbol"]: {col_: row[col_] for col_ in df_join_instruments.columns}
			for _, row in df_join_instruments.iterrows()
		}

	def instrument_id_by_symbol(self, symbol: str) -> Union[list[dict[str, Any]], int]:
		"""Retrieve instrument ID by symbol.

		Parameters
		----------
		symbol : str
			Instrument symbol.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Instrument data or status code.
		"""
		return self._ops.app_request(
			method="GET",
			app=f"/api/v1.0/symbol/{symbol}",
		)
