import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


class ExploratoryDataAnalysis:

    def is_monotonic(self, array_data):
        """
        REFERENCES: https://github.com/pankajkalania/IV-WOE/blob/main/iv_woe_code.py,
            https://gaurabdas.medium.com/weight-of-evidence-and-information-value-in-python-from-scratch-a8953d40e34#:~:text=Information%20Value%20gives%20us%20the,as%20good%20and%20bad%20customers.
        DOCSTRING: MONOTONIC IS A FUNCTION BETWEEN ORDERED SETS THAT PRESERVES OR REVERSES THE GIVEN
            ORDER
        INPUTS:
        OUTPUTS:
        """
        return all(array_data[i] <= array_data[i + 1] for i in range(len(array_data) - 1)) \
            or all(array_data[i] >= array_data[i + 1] for i in range(len(array_data) - 1))

    def prepare_bins(self, df, c_i, target_col, max_bins, force_bin=True, binned=False,
                     remarks=np.nan, name_bins='_bins',
                     remark_binned_monotonically='binned monotonically',
                     remark_binned_forcefully='binned forcefully',
                     remark_binned_error='could not bin'):
        """
        REFERENCES: https://github.com/pankajkalania/IV-WOE/blob/main/iv_woe_code.py,
            https://gaurabdas.medium.com/weight-of-evidence-and-information-value-in-python-from-scratch-a8953d40e34#:~:text=Information%20Value%20gives%20us%20the,as%20good%20and%20bad%20customers.
        DOCSTRING: BIN METHOD - 1. EQUI-SPACED BINS WITH AT LEAST 5% OF TOTAL OBSERVATIONS IN EACH
            BIN; 2. TO ENSURE 5% SAMPLE IN EACH CLASS A MAXIMUM OF 20 BINS CAN BE SET; 3. EVENT RATE
            FOR EACH BIN WILL BE MONOTONICALLY INCREASING OF MONOTONICALLY DECREASINGM IF A
            MONOTONOUS TREND IS NOT OBSERVED, A FEW OF THE BINS CAN BE COMBINED ACCORDINGLY TO
            ACHIEVE MONOTIONICITY; 4. SEPARATE BINS WILL BE CREATED FOR MISSING VALUES
        INPUTS:
        OUTPUTS:
        """
        # monotonic binning
        for n_bins in range(max_bins, 2, -1):
            try:
                df[c_i + name_bins] = pd.qcut(df[c_i],
                                              n_bins, duplicates='drop')
                array_data_monotonic = df.groupby(c_i + name_bins)[target_col].mean().reset_index(
                    drop=True)
                if self.is_monotonic(array_data_monotonic):
                    force_bin = False
                    binned = True
                    remarks = remark_binned_monotonically
            except:
                pass
        # force binning - creating 2 bins forcefully because 2 bins will always be monotonic
        if force_bin or (c_i + name_bins in df and df[c_i + name_bins].nunique() < 2):
            _min = df[c_i].min()
            _mean = df[c_i].mean()
            _max = df[c_i].max()
            df[c_i + name_bins] = pd.cut(df[c_i],
                                         [_min, _mean, _max], include_lowest=True)
            if df[c_i + name_bins].nunique() == 2:
                binned = True
                remarks = remark_binned_forcefully
        # returnning binned data
        if binned == True:
            return c_i + name_bins, remarks, df[[c_i, c_i + name_bins, target_col]].copy()
        else:
            remarks = remark_binned_error
            return c_i, remarks, df[[c_i, target_col]].copy()

    def reshape_1d_arrays(self, array_data):
        """
        DOCSTRING: RESHAPE A 1D ARRAY TO 2D IN ORDER TO APPLY FEATUR SCALING, OR LINEARITY TESTS,
            FOR INSTANCE
        INPUTS: ARRAY DATA
        OUTPUTS: ARRAY
        """
        # reshape array
        try:
            _= array_data[:, 0]
        except IndexError:
            array_data = np.reshape(array_data, (-1, 1))
        # return array reshaped
        return array_data

    def eda_database(self, df_data, bins=58, figsize=(20, 15)):
        """
        DOCSTRING: EXPLARATORY DATA ANALYSIS OF THE DATABASE
        INPUTS: DATAFRAME
        OUTPUTS: NONE
        """
        print('*** HEAD DATAFRAME ***')
        print(df_data.head())
        print('*** INFOS DATAFRAME ***')
        print(df_data.info())
        print('*** DESCRIBE STATISTICAL & PROBABILITY INFOS - DATAFRAME ***')
        print(df_data.describe())
        print('*** PLOTTING DATAFRAME ***')
        df_data.hist(bins=bins, figsize=figsize)
        plt.show()
