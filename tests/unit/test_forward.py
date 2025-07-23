"""Unit tests for ForwardBR class.

Tests the forward contract pricing functionality including normal operations,
edge cases, error conditions, and type validation.
"""

from typing import Any

import pytest

from stpstone.analytics.pricing.derivatives.forward import ForwardBR


class TestForwardBR:
    """Test suite for ForwardBR class."""

    # --------------------------
    # Fixtures
    # --------------------------
    @pytest.fixture
    def forward_br(self) -> type[Any]:
        """Fixture providing a ForwardBR instance."""
        return ForwardBR()

    @pytest.fixture
    def valid_inputs(self) -> dict[str, type[Any]]:
        """Fixture providing valid input parameters."""
        return {
            'spot': 100.0,
            'bid_forward_contract': 95.0,
            'rate_cost_period': 0.05,
            'leverage': 2.0,
            'number_contracts': 10
        }

    # --------------------------
    # Normal Operations
    # --------------------------
    def test_normal_operation(
        self, 
        forward_br: type[Any], 
        valid_inputs: dict[str, type[Any]]
    ) -> None:
        """Test forward contract pricing with normal inputs.
        
        Parameters
        ----------
        forward_br : type[Any]
            ForwardBR instance
        valid_inputs : dict[str, type[Any]]
            Valid input parameters
        """
        result = forward_br.forward_contract_pricing(**valid_inputs)
        
        expected_mtm = (100.0 - 95.0 * 1.05) * 2.0 * 10
        expected_pct_return = (100.0 - 95.0 * 1.05) * 2.0 / 95.0
        expected_notional = 100.0 * 2.0 * 10
        
        assert isinstance(result, dict)
        assert pytest.approx(result['mtm']) == expected_mtm
        assert pytest.approx(result['pct_retun']) == expected_pct_return
        assert pytest.approx(result['notional']) == expected_notional

    def test_zero_rate_cost(
        self, 
        forward_br: type[Any], 
        valid_inputs: dict[str, type[Any]]
    ) -> None:
        """Test with zero rate cost period.
        
        Parameters
        ----------
        forward_br : type[Any]
            ForwardBR instance
        valid_inputs : dict[str, type[Any]]
            Valid input parameters
        """
        inputs = valid_inputs.copy()
        inputs['rate_cost_period'] = 0.0
        result = forward_br.forward_contract_pricing(**inputs)
        
        expected_mtm = (100.0 - 95.0) * 2.0 * 10
        assert pytest.approx(result['mtm']) == expected_mtm

    def test_single_contract(
        self, 
        forward_br: type[Any], 
        valid_inputs: dict[str, type[Any]]
    ) -> None:
        """Test with single contract.
        
        Parameters
        ----------
        forward_br : type[Any]
            ForwardBR instance
        valid_inputs : dict[str, type[Any]]
            Valid input parameters
        """
        inputs = valid_inputs.copy()
        inputs['number_contracts'] = 1
        result = forward_br.forward_contract_pricing(**inputs)
        
        expected_notional = 100.0 * 2.0 * 1
        assert pytest.approx(result['notional']) == expected_notional

    # --------------------------
    # Edge Cases
    # --------------------------
    def test_negative_spot_price(
        self, 
        forward_br: type[Any], 
        valid_inputs: dict[str, type[Any]]
    ) -> None:
        """Test with negative spot price.
        
        Parameters
        ----------
        forward_br : type[Any]
            ForwardBR instance
        valid_inputs : dict[str, type[Any]]
            Valid input parameters
        """
        inputs = valid_inputs.copy()
        inputs['spot'] = -50.0
        result = forward_br.forward_contract_pricing(**inputs)
        
        assert result['mtm'] < 0
        assert result['pct_retun'] < 0

    def test_zero_leverage(
        self, 
        forward_br: type[Any], 
        valid_inputs: dict[str, type[Any]]
    ) -> None:
        """Test with zero leverage.
        
        Parameters
        ----------
        forward_br : type[Any]
            ForwardBR instance
        valid_inputs : dict[str, type[Any]]
            Valid input parameters
        """
        inputs = valid_inputs.copy()
        inputs['leverage'] = 0.0
        result = forward_br.forward_contract_pricing(**inputs)
        
        assert pytest.approx(result['mtm']) == 0.0
        assert pytest.approx(result['pct_retun']) == 0.0
        assert pytest.approx(result['notional']) == 0.0

    def test_high_leverage(
        self, 
        forward_br: type[Any], 
        valid_inputs: dict[str, type[Any]]
    ) -> None:
        """Test with very high leverage.
        
        Parameters
        ----------
        forward_br : type[Any]
            ForwardBR instance
        valid_inputs : dict[str, type[Any]]
            Valid input parameters
        """
        inputs = valid_inputs.copy()
        inputs['leverage'] = 1000.0
        result = forward_br.forward_contract_pricing(**inputs)
        
        assert abs(result['mtm']) > 100
        assert abs(result['pct_retun']) > 1

    def test_spot_equals_forward(
        self, 
        forward_br: type[Any], 
        valid_inputs: dict[str, type[Any]]
    ) -> None:
        """Test when spot price equals forward price.
        
        Parameters
        ----------
        forward_br : type[Any]
            ForwardBR instance
        valid_inputs : dict[str, type[Any]]
            Valid input parameters
        """
        inputs = valid_inputs.copy()
        inputs['spot'] = inputs['bid_forward_contract']
        result = forward_br.forward_contract_pricing(**inputs)
        
        expected_mtm = (inputs['spot'] - inputs['bid_forward_contract'] * 1.05) * 2.0 * 10
        assert pytest.approx(result['mtm']) == expected_mtm

    # --------------------------
    # Error Conditions
    # --------------------------
    def test_missing_arguments(
        self, 
        forward_br: type[Any]
    ) -> None:
        """Test with missing required arguments.
        
        Parameters
        ----------
        forward_br : type[Any]
            ForwardBR instance
        """
        with pytest.raises(TypeError):
            forward_br.forward_contract_pricing()  # type: ignore

    def test_invalid_argument_types(
        self, 
        forward_br: type[Any], 
        valid_inputs: dict[str, type[Any]]
    ) -> None:
        """Test with invalid argument types.
        
        Parameters
        ----------
        forward_br : type[Any]
            ForwardBR instance
        valid_inputs : dict[str, type[Any]]
            Valid input parameters
        """
        # test string instead of float
        inputs = valid_inputs.copy()
        inputs['spot'] = "100"
        with pytest.raises(TypeError):
            forward_br.forward_contract_pricing(**inputs)

        # test float instead of int
        inputs = valid_inputs.copy()
        inputs['number_contracts'] = 10.5
        with pytest.raises(TypeError):
            forward_br.forward_contract_pricing(**inputs)

    def test_none_values(self, forward_br: type[Any], valid_inputs: dict[str, type[Any]]) -> None:
        """Test with None values.
        
        Parameters
        ----------
        forward_br : type[Any]
            ForwardBR instance
        valid_inputs : dict[str, type[Any]]
            Valid input parameters
        """
        inputs = valid_inputs.copy()
        inputs['leverage'] = None
        with pytest.raises(TypeError):
            forward_br.forward_contract_pricing(**inputs)

    # --------------------------
    # Type Validation
    # --------------------------
    def test_return_type(self, forward_br: type[Any], valid_inputs: dict[str, type[Any]]) -> None:
        """Test that the return type is correct.
        
        Parameters
        ----------
        forward_br : type[Any]
            ForwardBR instance
        valid_inputs : dict[str, type[Any]]
            Valid input parameters
        """
        result = forward_br.forward_contract_pricing(**valid_inputs)
        assert isinstance(result, dict)
        assert all(isinstance(key, str) for key in result)
        assert all(isinstance(value, float) for value in result.values())

    def test_integer_inputs(self, forward_br: ForwardBR) -> None:
        """Test that integer inputs must be explicitly converted to float.
        
        Parameters
        ----------
        forward_br : ForwardBR
            ForwardBR instance
        """
        with pytest.raises(TypeError):
            forward_br.forward_contract_pricing(
                spot=100,  # int - should fail
                bid_forward_contract=95.0,
                rate_cost_period=0.0,
                leverage=2.0,
                number_contracts=10
            )