"""Protocols for B3 Margin Simulator API client."""

from typing import Protocol, runtime_checkable

from stpstone.utils.providers.br.margin_simulator_b3._dto import (
	ResultReferenceData,
	ResultRiskCalculationResponse,
)


@runtime_checkable
class IMarginSimulatorB3(Protocol):
	"""Structural protocol for B3 Margin Simulator clients."""

	def _get_reference_data(self) -> ResultReferenceData: ...

	def risk_calculation(self) -> ResultRiskCalculationResponse: ...
