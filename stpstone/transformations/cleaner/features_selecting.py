### DIMENSIONALITY REDUCTION OF IVS

# pypi.org libs
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestClassifier
from mlxtend.feature_selection import SequentialFeatureSelector, ExhaustiveFeatureSelector
from sklearn.decomposition import PCA
# local libs
from stpstone.transformations.cleaner.eda import ExploratoryDataAnalysis


class DimensionalityReduction:

    def backward_elimination(self, array_x, array_y, str_estimator='linear_regression', int_cv=5,
                             int_verbose=0):
        """
        REFERENCES:
            https://rasbt.github.io/mlxtend/api_subpackages/mlxtend.feature_selection/#sequentialfeatureselector,
            https://rasbt.github.io/mlxtend/user_guide/feature_selection/SequentialFeatureSelector/
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # checking wheter the array is unidimensional and reshaping it
        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)
        # defining the estimator
        if str_estimator == 'linear_regression':
            class_estimator = LinearRegression()
            scoring = 'r2'
        elif str_estimator == 'rf_classifier':
            class_estimator = RandomForestClassifier(n_jobs=-1)
            scoring = 'accuracy'
        else:
            raise Exception('Estimator {} not defined, please revisit this parameter'.format(
                str_estimator
            ))
        # running the backward feature selection
        feature_selection = SequentialFeatureSelector(
            class_estimator,
            k_features=(1, array_x.shape[1]),
            forward=False,
            floating=False,
            verbose=int_verbose,
            scoring=scoring,
            cv=int_cv
        ).fit(array_x, array_y)
        # returning the most relevant columns
        return {
            'dr_feature_names': feature_selection.k_feature_names_,
            'dr_feature_indexes': feature_selection.k_feature_idx_,
            'dr_score': feature_selection.k_score_
        }

    def forward_elimination(self, array_x, array_y, str_estimator='linear_regression', int_cv=5,
                            int_verbose=0):
        """
        REFERENCES:
            https://rasbt.github.io/mlxtend/api_subpackages/mlxtend.feature_selection/#sequentialfeatureselector,
            https://rasbt.github.io/mlxtend/user_guide/feature_selection/SequentialFeatureSelector/
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # checking wheter the array is unidimensional and reshaping it
        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)
        # defining the estimator
        if str_estimator == 'linear_regression':
            class_estimator = LinearRegression()
            scoring = 'r2'
        elif str_estimator == 'rf_classifier':
            class_estimator = RandomForestClassifier()
            scoring = 'accuracy'
        else:
            raise Exception('Estimator {} not defined, please revisit this parameter'.format(
                str_estimator
            ))
        # running the backward feature selection
        feature_selection = SequentialFeatureSelector(
            class_estimator,
            k_features=(1, array_x.shape[1]),
            forward=True,
            floating=False,
            verbose=int_verbose,
            scoring=scoring,
            cv=int_cv
        ).fit(array_x, array_y)
        # returning the most relevant columns
        return {
            'dr_feature_names': feature_selection.k_feature_names_,
            'dr_feature_indexes': feature_selection.k_feature_idx_,
            'dr_score': feature_selection.k_score_
        }

    def exhaustive_feature_selection(self, array_x, array_y, str_estimator='linear_regression',
                                     max_features=None, int_cv=5):
        """
        REFERENCES:
            https://rasbt.github.io/mlxtend/api_subpackages/mlxtend.feature_selection/#exhaustivefeatureselector
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # checking wheter the array is unidimensional and reshaping it
        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)
        # maximum features
        if max_features is None:
            max_features=array_x.shape[1]
        # defining the estimator
        if str_estimator == 'linear_regression':
            class_estimator = LinearRegression()
            scoring = 'r2'
        elif str_estimator == 'rf_classifier':
            class_estimator = RandomForestClassifier()
            scoring = 'accuracy'
        else:
            raise Exception('Estimator {} not defined, please revisit this parameter'.format(
                str_estimator
            ))
        # running the backward feature selection
        feature_selection = ExhaustiveFeatureSelector(
            class_estimator,
            min_features=1,
            max_features=max_features,
            scoring=scoring,
            cv=int_cv,
            n_jobs=-1
        ).fit(array_x, array_y)
        # returning the most relevant columns
        return {
            'dr_feature_names': feature_selection.best_feature_names_ ,
            'dr_feature_indexes': feature_selection.best_idx_,
            'dr_score': feature_selection.best_score_
        }

    def iv_woe_iter(self, df_binned, target_col, class_col, name_missing_data='Missing',
                    name_bins='_bins', col_min='min', col_max='max',
                    col_non_event_count='non_event_count', col_sample_class='sample_class',
                    col_event_count='event_count', col_min_value='min_value',
                    col_max_value='max_value', col_sample_count='sample_count',
                    col_event_rate='event_rate', col_non_event_rate='non_event_rate',
                    col_feature='feature', col_sample_class_label='sample_class_label',
                    name_category='category', col_pct_non_event='pct_non_event',
                    col_pct_event='pct_event', col_first='first', col_count='count',
                    col_sum='sum', col_mean='mean', col_woe='woe', col_iv='iv'):
        """
        REFERENCES: https://github.com/pankajkalania/IV-WOE/blob/main/iv_woe_code.py,
            https://gaurabdas.medium.com/weight-of-evidence-and-information-value-in-python-from-scratch-a8953d40e34#:~:text=Information%20Value%20gives%20us%20the,as%20good%20and%20bad%20customers.
        DOCSTRING: INFORMATION VALUE (IV) AND WEIGHT OF EVIDENCE (WOE) USED TO UNDERSTAND
            THE RELATION BETWEEN DEPENDENT AND INDEPENDENT VARIABLES - PROVIDE THE STRENGTH OF
            A PREDICTOR TO MEASURE BINARY OUTCOMES
        INPUTS:
        OUTPUTS:
        """
        # replacing bins column names
        if name_bins in class_col:
            #   handle missing data
            df_binned[class_col] = df_binned[class_col].cat.add_categories([
                                                                           name_missing_data])
            df_binned[class_col] = df_binned[class_col].fillna(
                name_missing_data)
            #   dealing with _bins part of columns names
            df_tmp_groupby = df_binned.groupby(class_col).agg(
                {
                    class_col.replace(name_bins, ''): [col_min, col_max],
                    target_col: [col_count, col_sum, col_mean]
                }
            ).reset_index()
        else:
            #   handle missing data
            df_binned[class_col] = df_binned[class_col].fillna(
                name_missing_data)
            #   dealing with _bins part of columns names
            df_tmp_groupby = df_binned.groupby(class_col).agg(
                {
                    class_col: [col_first, col_first],
                    target_col: [col_count, col_sum, col_mean]
                }
            ).reset_index()
        # renaming colums of interest
        df_tmp_groupby.columns = [col_sample_class, col_min_value, col_max_value, col_sample_count,
                                  col_event_count, col_event_rate]
        # non event countage
        df_tmp_groupby[col_non_event_count] = df_tmp_groupby[col_sample_count] \
            - df_tmp_groupby[col_event_count]
        # non event rate
        df_tmp_groupby[col_non_event_rate] = 1 - df_tmp_groupby[col_event_rate]
        # limiting columns of interest
        df_tmp_groupby = df_tmp_groupby[[col_sample_class, col_min_value, col_max_value,
                                         col_sample_count,
                                         col_non_event_count, col_non_event_rate, col_event_count,
                                         col_event_rate]]
        # dealing with missing data
        if name_bins not in class_col and name_missing_data in df_tmp_groupby[col_min_value]:
            df_tmp_groupby[col_min_value] = df_tmp_groupby[col_min_value].replace({
                name_missing_data: np.nan})
            df_tmp_groupby[col_max_value] = df_tmp_groupby[col_max_value].replace({
                name_missing_data: np.nan
            })
        # creating feature column
        df_tmp_groupby[col_feature] = class_col
        # sampling labels
        if name_bins in class_col:
            df_tmp_groupby[col_sample_class_label] = df_tmp_groupby[col_sample_class].replace({
                name_missing_data: np.nan
            }).astype(name_category).cat.codes.replace({-1: np.nan})
        else:
            df_tmp_groupby[col_sample_class_label] = np.nan
        # limiting columns of interest
        df_tmp_groupby = df_tmp_groupby[[col_feature, col_sample_class, col_sample_class_label,
                                         col_sample_count, col_min_value, col_max_value,
                                         col_non_event_count,
                                         col_non_event_rate, col_event_count, col_event_rate]]
        # get percentage of wanted and unwanted evnts
        df_tmp_groupby[col_pct_non_event] = df_tmp_groupby[col_non_event_count] \
            / df_tmp_groupby[col_non_event_count].sum()
        df_tmp_groupby[col_pct_event] = df_tmp_groupby[col_event_count] \
            / df_tmp_groupby[col_event_count].sum()
        # calculating weight of evidence and information value
        df_tmp_groupby[col_woe] = np.log(df_tmp_groupby[col_pct_non_event]
                                         / df_tmp_groupby[col_pct_event])
        df_tmp_groupby[col_iv] = (df_tmp_groupby[col_pct_non_event]
                                  - df_tmp_groupby[col_pct_event]) * df_tmp_groupby[col_woe]
        # dealing with infinite numbers
        df_tmp_groupby[col_woe] = df_tmp_groupby[col_woe].replace(
            [np.inf, -np.inf], 0)
        df_tmp_groupby[col_iv] = df_tmp_groupby[col_iv].replace(
            [np.inf, -np.inf], 0)
        # returning datframe of interest
        return df_tmp_groupby

    def var_iter(self, df, target_col, max_bins,
                 min_bins_monotonic=2, col_feature='feature', col_remarks='remarks',
                 name_categorical='categorical', orient_str='records'):
        """
        REFERENCES: https://github.com/pankajkalania/IV-WOE/blob/main/iv_woe_code.py,
            https://gaurabdas.medium.com/weight-of-evidence-and-information-value-in-python-from-scratch-a8953d40e34#:~:text=Information%20Value%20gives%20us%20the,as%20good%20and%20bad%20customers.
        DOCSTRING: ITERATE OVER ALL FEATURES, CALCULATE WOE AND IV FOR THE CLASSES, AND
            APPEND TO ONE DATAFRAME WOE_IV OF EXPORTATION
        INPUTS:
        OUTPUTS:
        """
        # setting variables
        list_ser_woe_iv = list()
        list_remarks = list()
        # iterating over the columns within the dataframe
        for c_i in df.columns:
            if c_i not in [target_col]:
                #   check if binning is required, if so, then prepare bins and calculate woe and
                #       iv - logic: binning is done only when feature is continuous and non-binary;
                #       note: make sure dtype of continuous columns in dataframe is not object
                if np.issubdtype(df[c_i], np.number) and df[c_i].nunique() > min_bins_monotonic:
                    class_col, remarks, df_binned = self.prepare_bins(
                        df[[c_i, target_col]].copy(), c_i, target_col, max_bins)
                    #   calculating iv and woe
                    df_agg_data = self.iv_woe_iter(
                        df_binned.copy(), target_col, class_col)
                    list_remarks.append(
                        {col_feature: c_i, col_remarks: remarks})
                else:
                    df_agg_data = self.iv_woe_iter(
                        df[[c_i, target_col]].copy(), target_col, c_i)
                    list_remarks.append(
                        {col_feature: c_i, col_remarks: name_categorical})
                #   appending to list of dataframes
                list_ser_woe_iv.extend(
                    df_agg_data.to_dict(orient=orient_str))
        # returning dataframes of woe, iv and remarks
        return pd.DataFrame(list_ser_woe_iv), pd.DataFrame(list_remarks)

    def get_iv_woe(self, df, target_col, max_bins, name_missing_data='Missing', name_bins='_bins',
                   col_non_event_count='non_event_count', col_sample_class='sample_class',
                   col_event_count='event_count', col_min_value='min_value',
                   col_max_value='max_value', col_sample_count='sample_count',
                   col_non_event_rate='non_event_rate', col_feature='feature',
                   col_sample_class_label='sample_class_label', col_pct_non_event='pct_non_event',
                   col_pct_event='pct_event', col_count='count', col_woe='woe', col_sum='sum',
                   col_iv='iv', col_remarks='remarks', col_number_classes='number_of_classes',
                   col_feature_null_pct='feature_null_percent', col_iv_sum='iv_sum'):
        """
        REFERENCES: https://github.com/pankajkalania/IV-WOE/blob/main/iv_woe_code.py,
            https://gaurabdas.medium.com/weight-of-evidence-and-information-value-in-python-from-scratch-a8953d40e34#:~:text=Information%20Value%20gives%20us%20the,as%20good%20and%20bad%20customers.
        DOCSTRING: AFTER GETTING WOE AND IV FOR ALL CLASSES OF FEATURES, CALCULATE THE AGGREGATED
            IV VALUES FOR FEATURES
        INPUTS:
        OUTPUTS:
        """
        # calculating woe, iv and getting the remarks for each bin
        df_woe_iv, df_binning_remarks = self.var_iter(df, target_col, max_bins)
        # limiting the columns of interest
        df_woe_iv[col_feature] = df_woe_iv[col_feature].replace(
            name_bins, '', regex=True)
        df_woe_iv = df_woe_iv[[col_feature, col_sample_class, col_sample_class_label, col_sample_count,
                               col_min_value, col_max_value, col_non_event_count, col_non_event_rate,
                               col_event_count, col_pct_non_event, col_pct_event, col_woe, col_iv]]
        # getting the accumulated iv for each feature
        df_iv = df_woe_iv.groupby(col_feature)[[col_iv]].agg(
            [col_sum, col_count]).reset_index()
        # changing column names
        df_iv.columns = [col_feature, col_iv, col_number_classes]
        # handling null values
        df_null_percent = pd.DataFrame(df.isnull().mean()).reset_index()
        df_null_percent.columns = [col_feature, col_feature_null_pct]
        # left-join iv with null values
        df_iv = df_iv.merge(df_null_percent, on=col_feature, how='left')
        # left-join iv with binning remarks
        df_iv = df_iv.merge(df_binning_remarks, on=col_feature, how='left')
        # left-join woe_iv dataframe with aggregated iv
        df_woe_iv = df_woe_iv.merge(df_iv[[col_feature, col_iv, col_remarks]].rename(columns={
            col_iv: col_iv_sum}), on=col_feature, how='left')
        # remove duplicates
        df_iv.drop_duplicates(inplace=True)
        # sort values
        df_iv.sort_values([col_iv], ascending=[False], inplace=True)
        # return iv and woe_iv dataframe
        return df_iv, df_woe_iv.replace({name_missing_data: np.nan})

    def iv_label_predictive_power(self, df_iv, col_iv='iv', col_predictive_power='predictive_power'):
        """
        REFERENCES: https://github.com/pankajkalania/IV-WOE/blob/main/iv_woe_code.py,
            https://gaurabdas.medium.com/weight-of-evidence-and-information-value-in-python-from-scratch-a8953d40e34#:~:text=Information%20Value%20gives%20us%20the,as%20good%20and%20bad%20customers.
        DOCSTRING: LABEL PREDICTIVE POWER OF EACH FEATURE
        INPUTS: DATAFRAME INFORMATION VALUE(IV)
        OUTPUTS: DATAFRAME
        """
        # sort values
        df_iv.sort_values([col_iv], ascending=[False], inplace=True)
        # set label strength
        for index, row in df_iv.iterrows():
            if row[col_iv] < 0.02:
                df_iv.loc[index, col_predictive_power] = 'not useful'
            elif row[col_iv] < 0.1:
                df_iv.loc[index, col_predictive_power] = 'weak predictor'
            elif row[col_iv] < 0.3:
                df_iv.loc[index, col_predictive_power] = 'medium predictor'
            elif row[col_iv] < 0.5:
                df_iv.loc[index, col_predictive_power] = 'strong predictor'
            else:
                df_iv.loc[index, col_predictive_power] = 'suspicious'
        # return dataframe iv
        return df_iv

    def pca(self, array_data):
        """
        REFERENCES: https://leandrocruvinel.medium.com/pca-na-mão-e-no-python-d559e9c8f053
        DOCSTRING: PRINCIPAL COMPONENTS ANALYSIS
        INPUTS: DEPENDENT AND INDEPENDENT VARIABLES
        OUTPUTS:
        """
        # pca fiiting model
        model_fitted = PCA(n_components=len(array_data)).fit(array_data)
        # returning model infos
        return {
            'eigenvalues': model_fitted.explained_variance_,
            'eigenvectors': model_fitted.components_,
            'explained_variance_ratio': model_fitted.explained_variance_ratio_,
            'components': model_fitted.components_
        }
