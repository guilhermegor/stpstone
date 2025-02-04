### BCB UNIT TEST ###
import os
import sys
from unittest import TestCase, main
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from stpstone.finance.macroeconomics.br_macro import BCB


class BCBTest(TestCase):

    def test_eda(self):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        bcb = BCB()
        dados = bcb.igpm('01/01/2020', '01/01/2020')
        dados_esperados = [{'data': '01/01/2020', 'valor': '0.48'}]
        self.assertAlmostEqual(dados, dados_esperados)


if __name__ == '__main__':
    main()
