
### DATA CLEANING TO PREPARE DATASET ###

import pandas as pd
import numpy as np
from zlib import crc32
from scipy.ndimage.interpolation import shift
from sklearn.model_selection import StratifiedShuffleSplit, train_test_split
from sklearn.impute import SimpleImputer, KNNImputer
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.preprocessing import OrdinalEncoder, OneHotEncoder, LabelEncoder
from sklearn.compose import ColumnTransformer
from stpstone.transformations.cleaner.eda import ExploratoryDataAnalysis


class DataCleaning:

    def test_set_check_hash(self, identifier, test_ratio):
        """
        REFERENCES: HANDS-ON MACHINE LEARNING WITH SCIKIT-LEARN, KERAS, AND TENSORFLOW,
            2ND EDITION, BY AURÉLIEN GÉRON (O’REILLY). COPYRIGHT 2019 KIWISOFT S.A.S.,
            978-1-492-03264-9.
        DOCSTRING: SET HASH FROM THE ROW IDENTIFIER TO REMAIN A TEST SAMPLE STABLE EVEN THOUGH ITS
            AN UPDATE IN THE CONSULTED DATABASE FETCHED TO MEMORY
        INPUTS: IDENTIFIER, TESTE RATIO
        OUTPUTS: IDENTIFIER HASH
        """
        return crc32(np.int64(identifier)) & 0xffffffff < test_ratio * 2**32

    def split_train_test(self, df_data, test_ratio=0.2, random_seed=42, stratify_col=None):
        """
        REFERENCES: HANDS-ON MACHINE LEARNING WITH SCIKIT-LEARN, KERAS, AND TENSORFLOW,
            2ND EDITION, BY AURÉLIEN GÉRON (O’REILLY). COPYRIGHT 2019 KIWISOFT S.A.S.,
            978-1-492-03264-9.
        DOCSTRING: CREATE A DATASET RANDOMLY WITHOUT DISTINGUISH OF ID HASH, SO WHEN THE DATASET
            IS REFRESHED IT WOULD BE A CHANGE IN THE TESTING AND TRAINING SETS
        INPUTS: DATAFRAME DATA, TEST RATIO AND RANDOM SEED
        OUTPUTS: TUPLE OF DATAFRAMES WITH TRAINING SET AND TEST SET
        """
        return train_test_split(df_data, test_size=test_ratio, random_state=random_seed,
                                stratify=stratify_col)

    def split_stratified_train_test(self, df_data, col_name, n_splits=1, test_size=0.2,
                                    random_state_seed=42):
        """
        REFERENCES: HANDS-ON MACHINE LEARNING WITH SCIKIT-LEARN, KERAS, AND TENSORFLOW,
            2ND EDITION, BY AURÉLIEN GÉRON (O’REILLY). COPYRIGHT 2019 KIWISOFT S.A.S.,
            978-1-492-03264-9.
        DOCSTRING: STRATIFIED TECHNIQUE TO CREATE SAMPLES FROM THE ORIGINAL DATASET BY COL NAME,
            AIMING TO NOT BE CHANGED BY A NEW FLOW OF DATA
        INPUTS: ORIGINAL DATAFRAME, ID COLUMN (STR), NUMBER OF SPLITS (1 AS DEFAULT),
            TEST SIZE (0.2 AS DEFAULT) AND RANDOM STATE SEED (42 AS DEFAULT)
        OUTPUTS: TRAIN AND TEST SET DATAFRAMES
        """
        # creating stratified categories
        split_class = StratifiedShuffleSplit(n_splits=n_splits, test_size=test_size,
                                             random_state=random_state_seed)
        for train_index, test_index in split_class.split(df_data, df_data[col_name]):
            df_strat_train_set = df_data.loc[train_index]
            df_strat_test_set = df_data.loc[test_index]
        return df_strat_train_set, df_strat_test_set

    def split_train_test_by_id(self, df_data, test_ratio, id_column):
        """
        REFERENCES: HANDS-ON MACHINE LEARNING WITH SCIKIT-LEARN, KERAS, AND TENSORFLOW,
            2ND EDITION, BY AURÉLIEN GÉRON (O’REILLY). COPYRIGHT 2019 KIWISOFT S.A.S.,
            978-1-492-03264-9.
        DOCSTRING: TEST SAMPLE OF DATA, RANDOMLY CHOSEN, TRYING TO PRESERVE ITS FEATURES
            TO PERFORM STATISTICAL TESTS IN ORDER TO EXPLAIN BEAHVIOURS OF THE POPULATION, WHEREAS
            THE TRAIN PORTION IS REMAINIG DATA PROVIDED TO ENHANCE THE CONCLUSIONS OF THE MODEL
            PROPOSED. SPLITTING BY ID USES INSTANCE'S IDENTIFIERS
        INPUTS: DF DATA, TEST RATIO AND ID_COLUMN (WHEN ITS NOT AVAILABLE IN THE DATA FRAME THERE
            ARE TWO OPTIONS: USING THE RESET_INDEX() METHOD IN PANDAS OR STABLE FEATURES AS
            FOREIGNER KEYS)
        OUTPUTS: TUPLE OF DATAFRAMES WITH TRAINING SET AND TEST SET
        """
        # defining ids from the data sample
        ids = df_data[id_column]
        # defining original hashes from the data set
        in_test_set = ids.apply(
            lambda id_: self.test_set_check_hash(id_, test_ratio))
        # returning sample test and training portion
        return df_data.loc[~in_test_set], df_data.loc[in_test_set]

    def create_category_stratified_train_test_set(self, df_data, id_column_original,
                                                  id_column_category, list_bins, list_labels):
        """
        DOCSTRING: CREATE A CATEGORY TO BE USED AS A PROXY FOR STRATIFIED DATASET PURPOSES
        INPUTS: DATAFRAME, ID ORIGINAL COLUMN, ID CATEGORY COLUMN, LIST OF BINS AND LIST OF LABELS
        OUTPUTS: DATAFRAME WITH CATEGORY COLUMN
        """
        # create a category, to be used as a proxy for stratification
        df_data[id_column_category] = pd.cut(df_data[id_column_original],
                                             bins=list_bins, labels=list_labels)
        return df_data

    def remove_category_stratified_train_test_set(self, df_train_set, df_test_set,
                                                  id_column_category):
        """
        DOCSTRING: REMOVE CATEGORICAL COLUMN FROM TRAINING AND TEST DATASET
        INPUTS: DATAFRAME TRAINING SET, DATAFRAME TEST SET, AND ID OF CATEGORICAL COLUMN
        OUTPUTS: TUPLE WITH DATAFRAME TRAINING SET AND DATAFRAME TEST SET
        """
        for set_ in (df_train_set, df_test_set):
            set_.drop(id_column_category, axis=1, inplace=True)
        return df_train_set, df_test_set

    def dataframe_id_column_prportions(self, df_data, id_column):
        """
        REFERENCES: HANDS-ON MACHINE LEARNING WITH SCIKIT-LEARN, KERAS, AND TENSORFLOW,
            2ND EDITION, BY AURÉLIEN GÉRON (O’REILLY). COPYRIGHT 2019 KIWISOFT S.A.S.,
            978-1-492-03264-9.
        DOCSTRING: DATAFRAME PROPORTIONS BY UNIQUE VALUES IN COLUMN ID
        INPUTS: DF_DATA AND ID COLUMN
        OUTPUTS: A GROUP BY OF PROPORTIONS, REGARDING TO THE TOTAL INSTANCES, EVALUATED OVER THE
            DATAFRAME COLUMN OF INTEREST
        """
        return df_data[id_column].value_counts() / len(df_data)

    def compare_stratified_random_samples_propotions(self, df_data_original, df_data_random_set,
                                                     df_data_stratified_set, id_column):
        """
        REFERENCES: HANDS-ON MACHINE LEARNING WITH SCIKIT-LEARN, KERAS, AND TENSORFLOW,
            2ND EDITION, BY AURÉLIEN GÉRON (O’REILLY). COPYRIGHT 2019 KIWISOFT S.A.S.,
            978-1-492-03264-9.
        DOCSTRING: COMPARE STRATIFIED AND RANDOM SAMPLES PORPOTIONS REGARDING ORIGINAL DATASET
        INPUTS: DF_DATA, TEST RATIO, ID COLUMN, RANDOM SEED(42 AS DEFAULT), N SPLITS (1 AS DEFAULT)
        OUTPUTS: DATAFRAME WITH PROPORTIONS FOR EACH SAMPLING METHOD (STRATIFIED AND RANDOM)
            REGARDING THE ORIGINAL DATA BASE
        """
        # comparing porpotions for each sample
        df_compare_props = pd.DataFrame({
            'Overall': self.dataframe_id_column_prportions(df_data_original, id_column),
            'Stratified': self.dataframe_id_column_prportions(df_data_stratified_set,
                                                              id_column),
            'Random': self.dataframe_id_column_prportions(df_data_random_set, id_column)
        }).sort_index()
        df_compare_props['Rand. %error'] = 100 * df_compare_props[
            'Random'] / df_compare_props["Overall"] - 100
        df_compare_props['Strat. %error'] = 100 * df_compare_props[
            'Stratified'] / df_compare_props['Overall'] - 100
        # returning comparison dataframe of both sampling methods
        return df_compare_props

    def replace_nan_values(self, array_data, strategy=None, missing_values=np.nan, n_neighbors=None):
        """
        DOCSTRING: REPLACE NAN WITH VALUES WITH DESIRED STRATEGY (MEAN, MEDIAN OR MOST FREQUENT),
            AS 0 PLACEMENT, MEDIAN, OR MEAN
        INPUTS: DATAFRAME OF INTEREST, AND STRATEGY
        OUTPUTS: DICTIONARY WITH STRATEGY, ARRAY REPLACERS, SAMPLE INCOMPLETE ROWS BEFORE CHANGES,
            SAMPLE INCOMPLETE ROWS AFTER CHANGES, DATAFRAME BEFORE ADJUSTMENTS AND DATAFRAME AFTER
            ADJUSTMENTS
        """
        # creating a copy of the original data just with numbers, since text will trigger an error
        #   when fitting a strategy for replacing nan values
        array_data_copy = array_data.copy()
        # creating imputer with the desired strategy
        if n_neighbors is None:
            imputer = SimpleImputer(strategy=strategy, missing_values=missing_values)
        else:
            imputer = KNNImputer(n_neighbors=n_neighbors, missing_values=np.nan)
        # fitting data
        imputer.fit(array_data_copy)
        # tansforming the output dataset
        array_data_copy = imputer.transform(array_data_copy)
        # retuning dict
        return {
            'strategy': imputer.strategy,
            'array_replacers': imputer.statistics_,
            'array_before_adustments': array_data,
            'array_after_adjustments': array_data_copy
        }

    def convert_categories_from_strings_to_array(self, array_data, list_idx_target_cols,
                                                 encoder_strategy='one_hot_encoder'):
        """
        DOCSTRING: CONVERT CATEGORIES FROM STRING TO NUMBERS
        INPUTS: ARRAY OF DATA TO BE ENCODED - STRATEGIES HANDLED: ONE_HOT_ENCODER (UNIQUE
            IDENTIFICATION ARRAY WITH N X N DIMENSION, BEING N THE NUMBER OF DIFERENT CATEGORIES),
            ORDINAL_ENCODER (ORDINAL NUMBERS FROM 0 TO N_CLASS-1),
            LABEL_ENCODER (ORDINAL NUMBERS FROM 0 TO N_CLASS-1)
        OUTPUTS: DICTIONARY WITH KEYS ARRAY LABELS, ARRAY DATA CATEGORIZED IN NUMBERS AND
            ARRAY DATA CATEGORIZED IN STRINGS
        """
        # checking wheter or not the array is a one hot encoding, when the category has one success
        #   category and a failure one
        if encoder_strategy == 'one_hot_encoder':
            #   column transformer
            ct = ColumnTransformer(transformers=[
                ('encoders', OneHotEncoder(), list_idx_target_cols)
            ], remainder='passthrough')
        elif encoder_strategy == 'ordinal_encoder':
            #   column transformer
            ct = ColumnTransformer(transformers=[
                ('encoders', OrdinalEncoder(), list_idx_target_cols)
            ], remainder='passthrough')
        elif encoder_strategy == 'label_encoder':
            #   column transformer - the label encoder is usually used for logistic regressions in
            #       order to configure the dependent variable as either 0 or 1
            ct = LabelEncoder()
        # paramether bl_one_hot_encoding ought be a boolean, in case another value is given
        #   return an error to the user and drop out the script
        else:
            raise Exception(
                'Paramether bl_one_hot_encoding ought be a boolean.')
        # turn string categories to numbers - for label transform the array data must be 1-dimension
        array_categoric_numbers = np.array(ct.fit_transform(array_data))
        # return dictionay with categories and labels
        return {
            'array_data_categorized_numbers': array_categoric_numbers,
            'array_data_categorized_strings': array_data
        }

    def feature_scaling(self, array_data, type_scaler='normalization', tup_feature_range=(0,1)):
        """
        REFERENCES:  “HANDS-ON MACHINE LEARNING WITH SCIKIT-LEARN, KERAS, AND TENSORFLOW,
            2ND EDITION, BY AURÉLIEN GÉRON (O’REILLY). COPYRIGHT 2019 KIWISOFT S.A.S.,
            978-1-492-03264-9.”,
            https://stackoverflow.com/questions/40758562/can-anyone-explain-me-standardscaler
        DOCSTRING: FEATURE SCALING NORMALIZATION (MIN-MAX SCALING, WITH A 0 TO 1 RANGE) OR
            STANDARDISATION (Z SCORE, WITH A -3 TO 3 RANGE, LESS INFLUENCED BY OUTLIERS)-WISE
        INPUTS: ARRAY DATA AND TYPE OF FEATURE SCALER
        OUTPUTS: DICTIONARY WITH DATA MAX, DATA MIN, SCALE, N SAMPLES, ARRAY WITH ORIGINAL DATA AND
            ARRAY WITH SCALED DATA
        """
        # checking wheter the array is unidimensional and reshaping it
        array_data = ExploratoryDataAnalysis().reshape_1d_arrays(array_data)
        # defining feature scaling strategy
        if type_scaler == 'normalization':
            scaler = MinMaxScaler(feature_range=tup_feature_range)
        elif type_scaler == 'standardisation':
            scaler = StandardScaler()
        else:
            raise Exception('Strategy {} is not defined'.format(type_scaler))
        # fitting to the model
        scaler.fit(array_data)
        # transfom input array data
        array_data_transformed = scaler.transform(array_data)
        # return dictionary with relevant information
        return {
            'scaler': scaler,
            'scale': scaler.scale_,
            'n_samples_seen': scaler.n_samples_seen_,
            'array_original_data': array_data,
            'array_scaled_data': array_data_transformed
        }

    def remove_noise_from_data(self, data_test, data_train):
        """
        REFERENCES: (MULTIOUTPUT CLASSIFICATION) https://colab.research.google.com/github/ageron/handson-ml2/blob/master/03_classification.ipynb#scrollTo=utQpplj4fGwa
        DOCSTRING: CLEAR NOISE AIMMING TO HELP ESTIMATORS TO FIT A DATASET AND PREDICT A PIXEL
            TO A TARGET LABEL
        INPUTS: DATA TEST AND DATA TRAIN
        OUTPUTS: TUPLE WITH DATA TEST AND TRAINING, ORIGINAL AND ENHANCED
        """
        # enhancing data pixels
        noise = np.random.randint(0, 100, (len(data_train), 784))
        data_train_original = data_train + noise
        noise = np.random.randint(0, 100, (len(data_test), 784))
        data_test_original = data_test + noise
        # returning orignal data test, training and each one enhanced
        return {
            'data_test_original': data_test_original,
            'data_test_enhanced': data_test,
            'data_training_original': data_train_original,
            'data_training_enhanced': data_train
        }

    def shift_image(self, image, dx, dy):
        """
        REFERENCES: (DATA AUGMENTATION) https://colab.research.google.com/github/ageron/handson-ml2/blob/master/03_classification.ipynb#scrollTo=vkIfY1tAfGwf
        DOCSTRING: SHIFT IMAGE POSITION FOR DATA AUGMENTATION, OR TRAINING SET EXPANSION, IN ORDER
            TO ENHANCE THE MODEL ESTIMATOR PERFORMANCE
        INPUTS: IMAGE, DX AND DY MOVEMENTS
        OUTPUTS: IMAGE
        """
        image = image.reshape((28, 28))
        shifted_image = shift(image, [dy, dx], cval=0, mode="constant")
        return shifted_image.reshape([-1])
