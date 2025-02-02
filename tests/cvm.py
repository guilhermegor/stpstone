#!/usr/bin/env python3
### CVM UNIT TEST ###
import os
import sys
from unittest import TestCase, main
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from stpstone.finance.cvm.cvm_data import CVMDATA


class CVMTest(TestCase):

    def test_cvm_data(self):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''

        cvm_data = CVMDATA()
        df = cvm_data.funds_register
        self.assertGreater(len(df), 65000)

        df = cvm_data.funds_classes
        self.assertGreater(len(df), 15000)

        df = cvm_data.funds_subclasses
        self.assertGreater(len(df), 980)


if __name__ == '__main__':
    main()
