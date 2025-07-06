import unittest
from unittest import TestCase

from stpstone.analytics.arithmetic.logic_gate import NANDGate, NORGate, XORGate


class TestLogicGates(TestCase):
    """Test cases for all logic gate implementations."""
    
    # NAND Gate Tests
    def test_nand_normal_operations(self):
        """Test NAND gate with normal input combinations."""
        self.assertTrue(bool(NANDGate(False, False)))
        self.assertTrue(bool(NANDGate(False, True)))
        self.assertTrue(bool(NANDGate(True, False)))
        self.assertFalse(bool(NANDGate(True, True)))
    
    def test_nand_repr(self):
        """Test NAND gate string representation."""
        self.assertEqual(repr(NANDGate(False, False)), 
                         "NANDGate(a=False, b=False, output=True)")
        self.assertEqual(repr(NANDGate(True, False)), 
                         "NANDGate(a=True, b=False, output=True)")
    
    # NOR Gate Tests
    def test_nor_normal_operations(self):
        """Test NOR gate with normal input combinations."""
        self.assertTrue(bool(NORGate(False, False)))
        self.assertFalse(bool(NORGate(False, True)))
        self.assertFalse(bool(NORGate(True, False)))
        self.assertFalse(bool(NORGate(True, True)))
    
    def test_nor_repr(self):
        """Test NOR gate string representation."""
        self.assertEqual(repr(NORGate(False, False)), 
                         "NORGate(a=False, b=False, output=True)")
        self.assertEqual(repr(NORGate(True, True)), 
                         "NORGate(a=True, b=True, output=False)")
    
    # XOR Gate Tests
    def test_xor_normal_operations(self):
        """Test XOR gate with normal input combinations."""
        self.assertFalse(bool(XORGate(False, False)))
        self.assertTrue(bool(XORGate(False, True)))
        self.assertTrue(bool(XORGate(True, False)))
        self.assertFalse(bool(XORGate(True, True)))
    
    def test_xor_repr(self):
        """Test XOR gate string representation."""
        self.assertEqual(repr(XORGate(False, True)), 
                         "XORGate(a=False, b=True, output=True)")
        self.assertEqual(repr(XORGate(True, True)), 
                         "XORGate(a=True, b=True, output=False)")
    
    # Edge Cases
    def test_edge_case_all_false(self):
        """Test all gates with all False inputs."""
        self.assertTrue(bool(NANDGate(False, False)))
        self.assertTrue(bool(NORGate(False, False)))
        self.assertFalse(bool(XORGate(False, False)))
    
    def test_edge_case_all_true(self):
        """Test all gates with all True inputs."""
        self.assertFalse(bool(NANDGate(True, True)))
        self.assertFalse(bool(NORGate(True, True)))
        self.assertFalse(bool(XORGate(True, True)))
    
    # Type Validation Tests
    def test_type_validation_nand(self):
        """Test NAND gate type validation."""
        with self.assertRaises(TypeError):
            NANDGate(1, True)  # First input not bool
        with self.assertRaises(TypeError):
            NANDGate(False, "True")  # Second input not bool
        with self.assertRaises(TypeError):
            NANDGate(1, "True")  # Both inputs wrong type
    
    def test_type_validation_nor(self):
        """Test NOR gate type validation."""
        with self.assertRaises(TypeError):
            NORGate(None, False)  # First input not bool
        with self.assertRaises(TypeError):
            NORGate(True, 0)  # Second input not bool
    
    def test_type_validation_xor(self):
        """Test XOR gate type validation."""
        with self.assertRaises(TypeError):
            XORGate(True, 1.0)  # Second input not bool
        with self.assertRaises(TypeError):
            XORGate([], False)  # First input not bool
    
    # Boolean Conversion Tests
    def test_boolean_conversion(self):
        """Test that gates work properly in boolean contexts."""
        self.assertTrue(NANDGate(False, False))
        self.assertFalse(NANDGate(True, True))
        
        self.assertTrue(NORGate(False, False))
        self.assertFalse(NORGate(True, False))
        
        self.assertTrue(XORGate(False, True))
        self.assertFalse(XORGate(False, False))
    
    # Input Preservation Tests
    def test_input_preservation(self):
        """Test that inputs are stored correctly and not modified."""
        nand = NANDGate(True, False)
        self.assertEqual(nand.a, True)
        self.assertEqual(nand.b, False)
        
        nor = NORGate(False, True)
        self.assertEqual(nor.a, False)
        self.assertEqual(nor.b, True)
        
        xor = XORGate(True, True)
        self.assertEqual(xor.a, True)
        self.assertEqual(xor.b, True)

if __name__ == "__main__":
    unittest.main()