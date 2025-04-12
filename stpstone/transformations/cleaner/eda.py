import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


class ExploratoryDataAnalysis:

    def bendford_law(self, array_data, bl_list_number_occurrencies=False):
        """
        REFERENCES: https://brilliant.org/wiki/benfords-law/
        DOCSTRING: FRAUD DETECTION MODEL, WHICH EVALUATES THE NUMBER OF FIRST DIGITS OCCURRENCIES
            IN A SAMPLE AND THE EXPECTED SET
        INPUTS: LIST OF NUMBERS AND BOOLEAN WHICH INDICATES WHETHER THE LIST IS A SAMPLE OF REAL
            DATA OR THE NUMBER OF INTEGER FIRST NUMBERS FROM A REAL SAMPLE IN ASCENDING ORDER
            DISCARDING ZEROS
        OUTPUTS: DICT (BENFORD EXPECTED ARRAY AND REAL NUMBERS OBSERVED ARRAY KEYS)
        """
        # expected occurrency of first numbers in a list
        array_benford = np.zeros(9)
        for i in range(9):
            array_benford[i] = np.log10(i + 2) - np.log10(i + 1)
        # observed occurrency of first numbers in a list
        array_real_numbers = np.array(array_data)
        # percentual occurrency of each integer
        array_percentual_occurrency_digits = np.zeros(9)
        # check whether the given list is a sample of real data or a list with the number of
        #   occurrencies for a given number in ascending order of integer numbers
        if bl_list_number_occurrencies == False:
            # seggregate first number of each occurrency
            for i in range(len(array_real_numbers)):
                array_real_numbers = [str(x)[0]
                                      for x in array_real_numbers if str(x)[0] != '0']
            # count percentual occurrency of each int number
            for i in range(1, 10):
                array_percentual_occurrency_digits[i - 1] = \
                    array_real_numbers.count(
                        str(i)) / len(array_percentual_occurrency_digits)
        elif bl_list_number_occurrencies == True:
            for i in range(len(array_real_numbers)):
                array_percentual_occurrency_digits[i] = \
                    array_real_numbers[i] / array_real_numbers.sum()
        else:
            raise Exception(
                'Boolean list number of occurrencies ought true or false')
        # dict message benford array x array real numbers
        dict_message = {
            'benford_expected_array': array_benford,
            'real_numbers_observed_array': array_percentual_occurrency_digits
        }
        return dict_message

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
