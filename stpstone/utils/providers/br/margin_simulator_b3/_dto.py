"""TypedDicts for B3 Margin Simulator API client."""

from typing import Optional, TypedDict


class ReferenceData(TypedDict):
	"""Typed dictionary for Reference Data.

	Attributes
	----------
	referenceDataToken : str
		Authentication token for reference data.
	"""

	referenceDataToken: str


class LiquidityResource(TypedDict):
	"""Typed dictionary for Liquidity Resource.

	Attributes
	----------
	value : int
		Liquidity resource value.
	"""

	value: int


class Security(TypedDict):
	"""Typed dictionary for Security.

	Attributes
	----------
	symbol : str
		Security symbol identifier.
	"""

	symbol: str


class SecurityGroup(TypedDict):
	"""Typed dictionary for Security Group.

	Attributes
	----------
	positionTypeCode : int
		Position type classification code.
	securityTypeCode : int
		Security type classification code.
	symbolList : list[str]
		List of symbol identifiers in this group.
	"""

	positionTypeCode: int
	securityTypeCode: int
	symbolList: list[str]


class Position(TypedDict):
	"""Typed dictionary for Position.

	Attributes
	----------
	longQuantity : int
		Long position quantity.
	shortQuantity : int
		Short position quantity.
	longPrice : int
		Long position price.
	shortPrice : int
		Short position price.
	"""

	longQuantity: int
	shortQuantity: int
	longPrice: int
	shortPrice: int


class RiskPosition(TypedDict):
	"""Typed dictionary for Risk Position.

	Attributes
	----------
	Security : Security
		Security information.
	SecurityGroup : SecurityGroup
		Security group classification.
	Position : Position
		Position quantities and prices.
	"""

	Security: Security
	SecurityGroup: SecurityGroup
	Position: Position


class ResultMarginSimulatorB3Payload(TypedDict):
	"""Typed dictionary for Margin Simulator B3 Payload.

	Attributes
	----------
	ReferenceData : ReferenceData
		Reference data token container.
	LiquidityResource : LiquidityResource
		Liquidity resource value.
	RiskPositionList : list[RiskPosition]
		List of risk positions.
	"""

	ReferenceData: ReferenceData
	LiquidityResource: LiquidityResource
	RiskPositionList: list[RiskPosition]


class SecurityGroupList(TypedDict):
	"""Typed dictionary for Security Group List.

	Attributes
	----------
	SecurityGroup : list[SecurityGroup]
		List of security groups.
	"""

	SecurityGroup: list[SecurityGroup]


class ResultReferenceData(TypedDict):
	"""Typed dictionary for Reference Data Response.

	Attributes
	----------
	referenceDataToken : str
		Authentication token.
	liquidityResourceLimit : int
		Maximum liquidity resource limit.
	SecurityGroupList : SecurityGroupList
		Available security groups.
	"""

	referenceDataToken: str
	liquidityResourceLimit: int
	SecurityGroupList: SecurityGroupList


class Risk(TypedDict):
	"""Typed dictionary for Risk.

	Attributes
	----------
	totalDeficitSurplus : float
		Total deficit or surplus value.
	totalDeficitSurplusSubPortfolio_1 : float
		Sub-portfolio 1 deficit/surplus.
	totalDeficitSurplusSubPortfolio_2 : float
		Sub-portfolio 2 deficit/surplus.
	totalDeficitSurplusSubPortfolio_1_2 : float
		Combined sub-portfolios 1+2 deficit/surplus.
	worstCaseSubPortfolio : int
		Index of the worst-case sub-portfolio.
	potentialLiquidityResource : float
		Potential liquidity resource available.
	totalCollateralValue : float
		Total value of collateral.
	riskWithoutCollateral : float
		Risk exposure without collateral.
	liquidityResource : float
		Available liquidity resource.
	calculationStatus : int
		Calculation status code.
	scenarioId : int
		Scenario identifier used for calculation.
	"""

	totalDeficitSurplus: float
	totalDeficitSurplusSubPortfolio_1: float
	totalDeficitSurplusSubPortfolio_2: float
	totalDeficitSurplusSubPortfolio_1_2: float
	worstCaseSubPortfolio: int
	potentialLiquidityResource: float
	totalCollateralValue: float
	riskWithoutCollateral: float
	liquidityResource: float
	calculationStatus: int
	scenarioId: int


class ResultRiskCalculationResponse(TypedDict):
	"""Typed dictionary for Risk Calculation Response.

	Attributes
	----------
	Risk : Risk
		Risk metrics from the calculation.
	BusinessStatusList : Optional[list]
		List of business status items, if any.
	"""

	Risk: Risk
	BusinessStatusList: Optional[list]
