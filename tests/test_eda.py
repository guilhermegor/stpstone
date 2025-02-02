### EDA UNIT TEST ###
import os
from unittest import TestCase, main

import numpy as np
import pandas as pd

from stpstone.quantitative_methods.eda import ExploratoryDataAnalysis


class EDA(TestCase):
    def test_eda(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """

        # hardcodes
        file_path_teste_mesa_eda = os.path.join(
            os.path.dirname(__file__),
            'bases',
            'data-customers-rating_20230921_2055.csv',
        )
        target_col = 'bad_customer'
        iv_target_number_missed_payments = 0.20542618611759142
        iv_target_state = 0.09767337364917127
        iv_target_use_online_streaming = 0.003143062167476093
        pp_numero_visitas_banco = 'medium predictor'
        # carregando dataframe com teste mesa para a a
        reader = pd.read_csv(file_path_teste_mesa_eda)
        df_eda = pd.DataFrame(reader)
        # determinando o número de bins para a análise da massa de dados
        max_bins = int(np.sqrt(df_eda.shape[0]))
        # definindo o information value e weight of evidence
        df_iv, _ = ExploratoryDataAnalysis().get_iv_woe(
            df_eda, target_col, max_bins
        )
        df_iv_label = ExploratoryDataAnalysis().iv_label_predictive_power(
            df_iv
        )
        # teste unitário da feature número de pagamentos com default
        self.assertAlmostEqual(
            df_iv_label[df_iv_label['feature'] == 'number_of_missed_payments'][
                'iv'
            ].tolist()[0],
            iv_target_number_missed_payments,
        )
        # teste unitário da feature estado
        self.assertAlmostEqual(
            df_iv_label[df_iv_label['feature'] == 'state']['iv'].tolist()[0],
            iv_target_state,
        )
        # teste unitário da feature uso de streaming
        self.assertAlmostEqual(
            df_iv_label[df_iv_label['feature'] == 'use_online_streaming'][
                'iv'
            ].tolist()[0],
            iv_target_use_online_streaming,
        )
        # teste unitário do poder de previsão da vrável número de visitas ao banco
        self.assertAlmostEqual(
            df_iv_label[df_iv_label['feature'] == 'number_of_bank_visits'][
                'predictive_power'
            ].tolist()[0],
            pp_numero_visitas_banco,
        )


if __name__ == '__main__':
    main()
