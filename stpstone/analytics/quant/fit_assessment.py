### FITTING MODELS ASSESMENT

import numpy as np
from sklearn.model_selection import cross_val_score, cross_val_predict
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV
from sklearn.metrics import confusion_matrix, precision_score, recall_score, f1_score, \
    roc_auc_score, adjusted_rand_score, silhouette_score, accuracy_score, r2_score


class FitPerformance:

    def max_llf(self, array_x, array_y, array_y_hat):
        """
        REFERENCES: https://stackoverflow.com/questions/45033980/how-to-compute-aic-for-linear-regression-model-in-python
        DOCSTRING: MAXIMIZED LIKELIHOOD
        INPUTS: ARRAY INDEPENDENT, ARRAY DEPENDENT AN ARRAY OF PREDICTIONS
        OUTPUTS: FLOAT
        """
        #b number of observations
        nobs = float(array_x.shape[0])
        nobs2 = nobs / 2.0
        nobs = float(nobs)
        # residuals (vector of differences between dv and predictions)
        resid = array_y - array_y_hat
        # sum of squares due to regression
        ssr = np.sum((resid)**2)
        # log-likelihood
        llf = -nobs2*np.log(2*np.pi) - nobs2*np.log(ssr / nobs) - nobs2
        # return maximized log-likelihood
        return llf

    def aic(self, array_x, array_y, array_y_hat):
        """
        DOCSTRING: AKAIKE'S INFORMATION CRITERION (AIC) EVALUATES A COLLECTION OF MODELS
            THAT EXPLAINS THE SAME DV - THE LOWER THE BETTER - BETTER THAN BIC FOR PREDICTION
            PURPOSES
        INTPUTS: MODEL FITTED
        OUTPUTS: FLOAT
        """
        # number of features plus one dependent variable
        p = array_x.shape[1] + 2
        # log-likelihood
        llf = self.max_llf(array_x, array_y, array_y_hat)
        # return aic metric
        return -2.0 * llf + 2.0 * p

    def bic(self, array_x, array_y, array_y_hat):
        """
        REFERENCES: https://en.wikipedia.org/wiki/Bayesian_information_criterion
        DOCSTRING: SCHWARTZ'S BAYESIAN INFORMATION CRITERION (BIC) - THE LOWER THE BETTER - BETTER
            THAN AIC WHEN GOODNESS OF FIT MODEL IS PREFERRED
            - AIC AND BIC ARE TWO LOSS METRICS THAT EVALUATE MODELS ON HOW WELL THEY
                DESCRIBE/PREDICT BEHAVIOR AS WELL AS BASED ON THE NUMBER OF FREE PARAMETERS
                THE MODEL HAS
        INTPUTS: MODEL FITTED
        OUTPUTS: FLOAT
        """
        # number of parameters estimated by the model - q slopes + 1 intercept
        k = array_x.shape[1] + 2 # + 2 because the shape starts with 0
        # return bic
        return k * np.log(array_x.shape[0] + 1) + self.max_llf(array_x, array_y, array_y_hat)

    def cross_validation(self, model_fitted, array_x, array_y,
                         cross_validation_folds=3, scoring_method='neg_mean_squared_error',
                         cross_val_model='score', cross_val_model_method='predict_proba'):
        """
        REFERENCES: https://scikit-learn.org/stable/modules/cross_validation.html
        DOCSTRING: CROSS VALIDATION TO MEASURE ESTIMATOR PERFORMANCE
        INPUTS: MODEL ESTIMATOR, X ARRAY OF REAL NUMBERS, Y ARRAY OF REAL NUMBERS
        OUPUTS: DICT WITH SCORES, MEAN AND STANDARD DISTRIBUTION
        """
        # defining cross validation model for the current estimator
        if cross_val_model == 'score':
            scores = cross_val_score(
                model_fitted, array_x, array_y,
                cv=cross_validation_folds, scoring=scoring_method)
        elif cross_val_model == 'predict':
            scores = cross_val_predict(
                model_fitted, array_x, array_y,
                cv=cross_validation_folds, method=cross_val_model_method)
        else:
            raise Exception(
                'Cross validation method {} does not match with the current '.format(
                    cross_val_model)
                + 'possibilities: score and predict, please revisit the parameter.')
        # returning scores, mean and standard deviation
        return {
            'scores': scores,
            'mean': scores.mean(),
            'standard_deviation': scores.std()
        }

    def grid_search_optimal_hyperparameters(self, model_fitted, param_grid, scoring_method,
                                            array_x_real_numbers, array_y_real_numbers,
                                            num_cross_validation_splitting_strategy=5,
                                            bl_return_train_score=True, bl_randomized_search=True):
        """
        REFERENCES: (FINE-TUNE YOUR MODEL) https://colab.research.google.com/github/ageron/handson-ml2/blob/master/02_sup_to_sup_machine_learning_project.ipynb#scrollTo=HwzPGGhkEagH,
            https://towardsdatascience.com/machine-learning-gridsearchcv-randomizedsearchcv-d36b89231b10
        DOCSTRING: FIDDLE AN ESTIMATOR WITH A COMBINATION OF HYPERPARAMETHERS AND FINDING THE
            OPTIMAL SOLUTION
        INPUTS: MODEL ESTIMATOR (NORMALLY FROM SKLEARN), PARAMETERS GRID, SCORING METHOD,
            X ARRAY OF REAL NUMBERS, Y ARRAY OF REAL NUMBERS, NUMBER OF CROSS VALIDATION SPLITTING
            STRATEGY, BOOLEAN TO WHETER RETURN OF NOT TRAINNING SCORE AND RADOMIZED SEARCH BOOLEAN
        OUTPUTS: DICTIONARY WITH BEST PARAMETERS, SCORE, BEST ESTIMATOR, CV RESULTS (RETURN A
            LIST OF TUPLES WITH RMSE, OR ROOT MEAN SQUARED ERROR, IN WHICH LOWER IS BETTER, AND THE
            PARAMETHERS CONSIDERED), AND THE MODEL REGRESSION WITH THE INPUTS OPTIMIZED
        """
        # fiddle estimator with hyperparameters until finding the optimal combination
        if bl_randomized_search == True:
            grid_search_model = RandomizedSearchCV(model_fitted, param_grid,
                                                   cv=num_cross_validation_splitting_strategy,
                                                   scoring=scoring_method,
                                                   return_train_score=bl_return_train_score)
        elif bl_randomized_search == False:
            grid_search_model = GridSearchCV(model_fitted, param_grid,
                                             cv=num_cross_validation_splitting_strategy,
                                             scoring=scoring_method,
                                             return_train_score=bl_return_train_score)
        else:
            raise Exception('grid_search_model ought be a boolean, instead it was given: '
                            + '{}, please revisit the paramether.'.format(bl_randomized_search))
        # fitting model
        grid_search_model.fit(
            array_x_real_numbers, array_y_real_numbers)
        # predictions
        best_model_prediction = grid_search_model.best_estimator_.predict(
            array_x_real_numbers)
        # returning dictionary
        return {
            'best_parameters': grid_search_model.best_params_,
            'score': grid_search_model.best_score_,
            'best_estimator': grid_search_model.best_estimator_,
            'feature_importance': grid_search_model.best_estimator_.feature_importances_,
            'cv_results': grid_search_model.cv_results_,
            'model_regression': grid_search_model,
            'predict': best_model_prediction,
            'mse': self.mean_squared_error(array_y_real_numbers,
                                           best_model_prediction),
            'rmse': np.sqrt(self.mean_squared_error(array_y_real_numbers,
                                                    best_model_prediction))
        }

    def accuracy_predictions(self, model, array_y, array_x,
                             cross_validation_folds=3, scoring_method='accuracy',
                             cross_val_model='score', key_scores='scores',
                             f1_score_average='macro'):
        """
        REFERENCES: (BINARY CLASSIFIERS + ROC CURVE) https://colab.research.google.com/github/ageron/handson-ml2/blob/master/03_classification.ipynb#scrollTo=rUZ6ahZ7G0BO
        DOCSTRING: CROSS VALIDATION, CONFUSION MATRIX, INSTEAD OF RETURNING THE EVALUATION SCORE,
            IT RETURNS THE PREDICTIONS MADE ON EACH TEST FOLD OF THE CROSS VALIDATION, PLACED
            IN A MATRIX OF TRUE AND FALSE POSITIVES AND NEGATIVES
        INPUTS: MODEL FITTED, ARRAY WITH DATA, TARGET, CROSS VALIDATION FOLDS, SCORING METHOD,
            CROSS VALIDATION MODEL (SCORE AS DEFAULT), KEY SCORES (SCORES AS DEFAULT), AND
            F1 SCORE AVERAGE (MACRO AS DEFAULT, BUT IF THE DATASET HAS MORE INSTANCES OF ONE
            TARGET LABEL ONE MAY WANT TO USE WEIGHTED INSTEAD OF MACRO)
        OUTPUTS: CROSS VALIDATION, CONFUSION MATRIX (2X2 MATRIX WITH TRUE NEGATIVE AND TRUE
            POSITIVE VALUES IN THE MAIN DIAGONAL, AND FALSE POSITIVE AND FALSE NEGATIVE IN THE
            AUXILIARY DIAGONAL), PRECISION SCORE (TP / (TP + FP)), RECALL (TP / (TP + FN)),
            F1 (HARMONIC MEAN OF PRECISION AND RECALL) AND RERCEIVER OPERATING CHARACTERISTC -
            AREA UNDER THE CURVE (ROC - AUC), BEING THE ESTIMATOR CLOSER TO 1 BEST IN THIS SCORE
            BETTER
        """
        # cross validation scores
        array_cross_validation_scores = FitPerformance().cross_validation(
            model, array_x, array_y, cross_validation_folds,
            scoring_method)[key_scores]
        # confusion matrix
        # ! revisit the argument array_cross_validation_scores, shouldn't be an array of predicted
        # !     data?
        array_confusion_matrix = confusion_matrix(
            array_y, array_cross_validation_scores)
        # return predictions, confusion matrix, precision score, recall score, f1 score, and
        #   roc auc score
        return {
            'cross_validation_scores': array_cross_validation_scores,
            'confusion_matrix': array_confusion_matrix,
            'precision_score': precision_score(array_y, array_cross_validation_scores),
            'recall_score': recall_score(array_y, array_cross_validation_scores),
            'f1_score': f1_score(array_y, array_cross_validation_scores,
                                 average=f1_score_average),
            'roc_auc_score': roc_auc_score(array_y, array_cross_validation_scores)
        }

    def fitting_perf_eval(self, array_y, array_y_hat):
        """
        REFERENCES: https://medium.com/@maxgrossman10/accuracy-recall-precision-f1-score-with-python-4f2ee97e0d6
        DOCSTRING: FITTING PERFOMANCE EVALUATION METRICS - F1 USED WHEN THERE IS AN INBALANCE BETWEEN
            ERRORS TYPE I AND II, OTHERWISE ACCURACY WOULD BE USED
        INPUTS: Y ARRAY AND Y HAT ARRAY
        OUTPUTS: DICTIONARY
        """
        return {
            'accuracy': accuracy_score(array_y, array_y_hat),
            'precision': precision_score(array_y, array_y_hat),
            'recall_sensitivity': recall_score(array_y, array_y_hat),
            'f1_score': f1_score(array_y, array_y_hat),
            'r2_score': r2_score(array_y, array_y_hat)
        }
