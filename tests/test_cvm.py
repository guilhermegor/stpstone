#!/usr/bin/env python3
from unittest import TestCase, main

from stpstone.finance.cvm.cvm_data import CVMDATA


class CvmTest(TestCase):
    def test_cvm_data(self):
        cvm_data = CVMDATA()
        df = cvm_data.funds_register
        self.assertGreater(len(df), 65000)

        df = cvm_data.funds_classes
        self.assertGreater(len(df), 15000)

        df = cvm_data.funds_subclasses
        self.assertGreater(len(df), 980)


if __name__ == '__main__':
    main()
