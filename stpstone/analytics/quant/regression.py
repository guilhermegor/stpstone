### STATISTIC FUNCTIONS FOR SUPERVISED REGRESSION MACHINE LEARNING MODELS ###

import numpy as np
import statsmodels.api as sm
import statsmodels.stats.api as sms
from statsmodels.compat import lzip
from mystic.solvers import diffev2
from mystic.monitors import VerboseMonitor
from scipy.optimize import differential_evolution, curve_fit
from sklearn.linear_model import SGDRegressor, Ridge, Lasso, ElasticNet, LinearRegression, \
    LogisticRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.ensemble import RandomForestRegressor
from sklearn.neighbors import KNeighborsRegressor
from sklearn.metrics import confusion_matrix, classification_report, accuracy_score, \
    precision_score, recall_score, f1_score, r2_score
from sklearn.metrics import class_likelihood_ratios
from sklearn.tree import DecisionTreeRegressor
from sklearn.svm import SVR
from stpstone.transformations.cleaner.eda import ExploratoryDataAnalysis


class LinearRegressions:

    def normal_equation(self, array_x, array_y, bl_optimize=True):
        """
        REFERENCE: “HANDS-ON MACHINE LEARNING WITH SCIKIT-LEARN, KERAS, AND TENSORFLOW,
            2ND EDITION, BY AURÉLIEN GÉRON (O’REILLY). COPYRIGHT 2019 KIWISOFT S.A.S.,
            978-1-492-03264-9.”
        DOCSTRING: NORMAL EQUATION TO FIND THE VALUE OF THETA THAT MINIMIZES THE COST FUNCTION -
            LEAST SQUARES REGRESSION
        INPUTS: ARRAY DATA AND ARRAY TARGET
        OUTPUTS: ARRAY WITH BEST THETA VECTOR, RESIDUALS, RANK
        """
        if bl_optimize == True:
            return np.linalg.lstsq(array_x, array_y, rcond=None)
        else:
            return np.linalg.inv(array_x.T.dot(array_x)).dot(array_x.T).dot(array_y)

    def batch_gradient_descent(self, array_x, array_y, max_iter=1000, eta=0.1, m=100,
                               theta=np.random.randn(2, 1)):
        """
        REFERENCE: “HANDS-ON MACHINE LEARNING WITH SCIKIT-LEARN, KERAS, AND TENSORFLOW,
            2ND EDITION, BY AURÉLIEN GÉRON (O’REILLY). COPYRIGHT 2019 KIWISOFT S.A.S.,
            978-1-492-03264-9.”
        DOCSTRING: BATCH GRADIENT DESCENT TO FIND THE GLOBAL MINIMUM OF A LINEAR FUNCTION
        INPUTS: ARRAY DATA, ARRAY TARGET, MAX ITERATIONS, ETA (LEARNING RATE), M (ITERATIONS,
            100 AS DEFAULT), THETA (GIVEN A FIRST APROXIMATION AS DEFAULT)
        OUTPUTS: ARRAY WITH BEST THETA VECTOR
        OBSERVATIONS: 1. INCREASING THE ETA MAY IMPLY IN CONVERGING FASTER TO OPTIMAL THETA
        """
        for _ in range(max_iter):
            gradients = 2 / m * \
                array_x.T.dot(array_x.dot(theta) - array_y)
            theta = theta - eta * gradients
        return theta

    def stochastic_gradient_descent(self, array_x, array_y, method='sklearn',
                                    n_epochs=1000, t0=5, t1=50, m=100, theta=np.random.randn(2, 1),
                                    tolerance=1e-3, penalty=None, eta0=0.1):
        """
        REFERENCES: “HANDS-ON MACHINE LEARNING WITH SCIKIT-LEARN, KERAS, AND TENSORFLOW,
            2ND EDITION, BY AURÉLIEN GÉRON (O’REILLY). COPYRIGHT 2019 KIWISOFT S.A.S.,
            978-1-492-03264-9.”
        DOCSTRING: STOCHASTIC GRADIENT DESCENT TO COVER RANDOM INSTANCES OF THE TRAINING SET AT
            EVERY STEP AND COMPUTE THE GRADIENTS BASED ONLY ON THAT SINGLE INSTANCE, AIMING TO
            FIND THE CLOSEST SOLUTION TO THE OPTIMAL THETA
        INPUTS: ARRAY DATA, ARRAY TARGET, N_EPOCHS (MAXIMUM ITERATIONS - 1000 AS DEFAULT),
            T0 AND T1 LEARNING SCHEDULE HYPERPARAMETERS, M (ITERATIONS, 100 AS DEFAULT),
            THETA (RANDOM INITIALIZATION), TOLERANCE (1E-3 AS DEFAULT), PENALTY (NONE AS DEFAULT),
            ETA0 (INITIAL LEARNING RATE, 0.1 AS DEFAULT)
        """
        if method == 'implemented':
            # defining the learning rate at each iteration, which is gradually reduced to solve the
            #   dilema of escaping from local optimal, but never settling at the minimal
            def learning_schedule(t): return t0 / (t + t1)
            # calculating the global minimun through iteration
            for epoch in range(n_epochs):
                for i in range(m):
                    random_index = np.random.randint(m)
                    xi = array_x[random_index:random_index + 1]
                    yi = array_y[random_index:random_index + 1]
                    gradients = 2 * xi.T.dot(xi.dot(theta) - yi)
                    eta = learning_schedule(epoch * m + i)
                    theta = theta - eta * gradients
            return theta
        elif method == 'sklearn':
            # defining the model
            sgd_reg = SGDRegressor(
                max_iter=n_epochs, tol=tolerance, penalty=penalty, eta0=eta0)
            # fitting the model
            sgd_reg.fit(array_x, array_y.ravel())
            # providing predictions
            array_predictions = sgd_reg.predict(array_x)
            # return desired data
            return {
                'model_fitted': sgd_reg,
                'intercept': sgd_reg.intercept_,
                'coeficients': sgd_reg.coef_,
                'predictions': array_predictions
            }

    def linear_regression(self, array_x, array_y):
        """
        DOCSTRING: BEST FITTING LINE FOR A SAMPLE OF DATA IN X ARRAY, COMAPARED TO AN Y ARRAY
        INPUTS: TWO ARRAIES '[[]]', NO NEED TO IMPORT ARRAY FUNCTION TO RECOGNISE ITS BEHAVIOUR
        OUTPUTS: DICT WIHT SCORE, COEFICIENTS, INTERCEPT, PREDICT AND THETA BEST (VECTOR WITH
            INCLINATION)
        """
        # checking wheter the array is unidimensional and reshaping it
        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)
        # fitting the model
        model = LinearRegression().fit(np.array(array_x),
                                      np.array(array_y))
        # retrieving the results
        if array_x.shape[1] == 1:
            return {
                'model_fitted': model,
                'score': model.score(np.array(array_x), np.array(array_y)),
                'coeficients': model.coef_,
                'intercept': model.intercept_,
                'predictions': model.predict(np.array(array_x)),
                'theta_best': np.linalg.inv(np.array(array_x).T.dot(
                    np.array(array_x))).dot(
                        np.array(array_x).T).dot(array_y)
            }
        else:
            return {
                'model_fitted': model,
                'score': model.score(np.array(array_x), np.array(array_y)),
                'coeficients': model.coef_,
                'intercept': model.intercept_,
                'predictions': model.predict(np.array(array_x))
            }

    def k_neighbors_regression(self, array_x, array_y):
        """
        DOCSTRING: BEST FITTING LINE FOR A SAMPLE OF DATA IN X ARRAY, COMAPARED TO AN Y ARRAY
        INPUTS: TWO ARRAIES '[[]]', NO NEED TO IMPORT ARRAY FUNCTION TO RECOGNISE ITS BEHAVIOUR
        OUTPUTS: DICT WIHT SCORE, COEFICIENTS, INTERCEPT, PREDICT AND THETA BEST (VECTOR WITH
            INCLINATION)
        """
        model = KNeighborsRegressor().fit(np.array(array_x),
                                      np.array(array_y))
        return {
            'model_fitted': model,
            'intercept': model.intercept_,
            'coeficients': model.coef_,
            'score': model.score(np.array(array_x), np.array(array_y)),
            'predictions': model.predict(np.array(array_x)),
            'theta_best': np.linalg.inv(np.array(array_x).T.dot(
                np.array(array_x))).dot(
                    np.array(array_x).T).dot(array_y)
        }

    def polynomial_equations(self, array_x, array_y, int_degree=2, bl_include_bias=True):
        """
        DOCSTRING: POLYNOMIAL REGRESSION TO HANDLE WITH NON-LINEAR EQUATIONS WITH A LINEAR
            APPROXIMATION
        INPUTS: ARRAY DATA, INTEGER DEGREE (2 AS DEFAULT) AND BOOLEAN TO WHETER OR NOT INCLUDE BIAS
            (FALSE AS DEFAULT)
        OUTPUTS: DICTIONARY WITH MODEL FITTED, INTERCEPT AND COEFICIENTS
        """
        # checking wheter the array is unidimensional and reshaping it
        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)
        # defininig polynomial model
        poly_features = PolynomialFeatures(
            degree=int_degree, include_bias=bl_include_bias)
        # transform data to polynomial purposes
        array_x_polynom = poly_features.fit_transform(array_x)
        # fit model for polynomial equation
        model = LinearRegression()
        model.fit(array_x_polynom, array_y)
        # predict based on array provided
        array_predictions = model.predict(array_x_polynom)
        # return polynomial equation
        return {
            'model_fitted': model,
            'intercept': model.intercept_,
            'coeficients': model.coef_,
            'score': model.score(np.array(array_x_polynom), np.array(array_y)),
            'predictions': array_predictions,
            'poly_features': poly_features
        }

    def ridge_regression(self, array_x, array_y, alpha=0,
                         solver_ridge_regression='cholesky'):
        """
        REFERENCES: “HANDS-ON MACHINE LEARNING WITH SCIKIT-LEARN, KERAS, AND TENSORFLOW,
            2ND EDITION, BY AURÉLIEN GÉRON (O’REILLY). COPYRIGHT 2019 KIWISOFT S.A.S.,
            978-1-492-03264-9.”
        DOCSTRING: REGULARIZED VERSION OF LINEAR REGRESSION THAT ADDS A TERM TO THE COST
            FUNCTION, FORCING THE LEARNING ALGORITHM TO NOT ONLY FIT THE DATA, BUT ALSO KEEP
            THE MODEL WEIGHTS AS SMALL AS POSSIBLE
        INPUTS: ARRAY DATA, ARRAY TARGET, ALPHA (SENSITIVITY TO MODEL WIEGHTS - 0 AS DEFAULT),
            AND SOLVER (CHOLESKY AS DEFAULT)
        """
        # defining the model
        ridge_reg = Ridge(alpha=alpha, solver=solver_ridge_regression)
        # fit the data
        ridge_reg.fit(array_x, array_y)
        # predict based on array provided
        array_predictions = ridge_reg.predict(array_x)
        # return polynomial equation
        return {
            'model_fitted': ridge_reg,
            'intercept': ridge_reg.intercept_,
            'coeficients': ridge_reg.coef_,
            'predictions': array_predictions
        }

    def lasso_regression(self, array_x, array_y, alpha=0.1):
        """
        REFERENCES: “HANDS-ON MACHINE LEARNING WITH SCIKIT-LEARN, KERAS, AND TENSORFLOW,
            2ND EDITION, BY AURÉLIEN GÉRON (O’REILLY). COPYRIGHT 2019 KIWISOFT S.A.S.,
            978-1-492-03264-9.”
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # declaring the model
        model = Lasso(alpha=alpha)
        # fitting curve
        model.fit(array_x, array_y)
        # making predictions
        array_predictions = model.predict(array_x)
        # return polynomial equation
        return {
            'model_fitted': model,
            'intercept': model.intercept_,
            'coeficients': model.coef_,
            'predictions': array_predictions
        }

    def elastic_net_regression(self, array_x, array_y, alpha=0.1, l1_ratio=0.5):
        """
        REFERENCES: “HANDS-ON MACHINE LEARNING WITH SCIKIT-LEARN, KERAS, AND TENSORFLOW,
            2ND EDITION, BY AURÉLIEN GÉRON (O’REILLY). COPYRIGHT 2019 KIWISOFT S.A.S.,
            978-1-492-03264-9.”
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # declaring the model
        model = ElasticNet(alpha=alpha, l1_ratio=l1_ratio)
        # fitting curve
        model.fit(array_x, array_y)
        # making predictions
        array_predictions = model.predict(array_x)
        # return polynomial equation
        return {
            'model_fitted': model,
            'intercept': model.intercept_,
            'coeficients': model.coef_,
            'predictions': array_predictions
        }


class NonLinearRegressions:

    def decision_tree_regression(self, array_x, array_y, seed=None):
        """
        DOCSTRING: DECISION TREE REGRESSION
        INPUTS: TWO ARRAIES '[[]]', NO NEED TO IMPORT ARRAY FUNCTION TO RECOGNISE ITS BEHAVIOUR
        OUTPUTS: DICT WIHT SCORE AND PREDICT
        """
        model = DecisionTreeRegressor(random_state=seed).fit(
            np.array(array_x),
            np.array(array_y)
        )
        return {
            'model_fitted': model,
            'score': model.score(np.array(array_x), np.array(array_y)),
            'predictions': model.predict(np.array(array_x))
        }

    def random_forest_regression(self, array_x, array_y, seed=None, n_estimators=100):
        """
        DOCSTRING: RANDOM FOREST REGRESSION, FITTING SEVERAL DECISION TREES AND AVERAGING THE
            PREDICTIONS --> BUILIDING A MODEL ONT TOP OF MANY OTHER MODELS IS CALLED ENSEMBLE
            LEARNING
        INPUTS: TWO ARRAIES '[[]]', NO NEED TO IMPORT ARRAY FUNCTION TO RECOGNISE ITS BEHAVIOUR
        OUTPUTS: DICT WIHT SCORE AND PREDICT
        """
        model = RandomForestRegressor(random_state=seed, n_estimators=n_estimators).fit(
            np.array(array_x), np.array(array_y)
        )
        return {
            'model_fitted': model,
            'score': model.score(np.array(array_x), np.array(array_y)),
            'features_importance': model.feature_importances_,
            'predictions': model.predict(np.array(array_x))
        }

    def support_vector_regression(self, array_x, array_y, kernel='poly',
                                          int_degree=2, c_positive_floating_point_number=100,
                                          epsilon=0.1):
        """
        DOCSTRING: SUPPORT VECTOR MACHINE MODEL FOR REGRESSION PURPOSES
        INPUTS: ARRAY DATA, ARRAY TARGET, KERNEL (POLY AS DEFAULT), INT DEGREE (DEGREE OF THE
            POLYNOMINAL DISTRIBUTION, BEING 2 AS DEFAULT, C (FLOATING POINT NUMBER WITH 100
            AS DEFAULT) AND EPSILON (0.1 AS DEFAULT)
        OUTPUTS: DICTIONARY WITH SCORE, PREDICT AND MODEL KEYS
        """
        model = SVR(kernel=kernel, degree=int_degree, C=c_positive_floating_point_number,
                   epsilon=epsilon)
        model = model.fit(np.array(array_x), np.array(array_y))
        return {
            'model_fitted': model,
            'score': model.score(np.array(array_x), np.array(array_y)),
            'predictions': model.predict(np.array(array_x))
        }


class LogLinearRegressions:

    def logistic_regression_logit(self, array_x, array_y,
                                  c_positive_floating_point_number=1.0, l1_ratio=None,
                                  int_max_iter=100, solver='lbfgs', penalty='l2',
                                  mult_class_classifier='auto', float_tolerance=0.0001,
                                  intercept_scaling=1, random_state=0, verbose=0,
                                  bl_fit_intercept=True, bl_warm_start=False,
                                  class_weight=None, bl_dual=False, n_jobs=None):
        """
        REFERENCE: https://realpython.com/logistic-regression-python/,
            https://www.udemy.com/course/machinelearning/
        DOCSTRING: LOGIT MODEL
        INPUTS: ARRAY DATA; ARRAY TARGET; C POSITIVE FLOATING-POINT NUMBER,
            THAT DEFINES THE RELATIVE STRENGTH OF REGULARIZATION (SMALLER VALUES INDICATE STRONGER
            REGULARIZATION, FITTING-WISE ITS POORLY FITTED, WHEREAS LARGER C MEANS WEAKER
            REGULARIZATION, THEREFORE HIGHER COEF_ AND INTERCEPT_ FOR THE MODEL); SOLVER (liblinear
            FOR LOGISTIC REGRESSION LOGIT AND lbfgs FOR SOFTMAX REGRESSION); PENALTY (l1 OR l2 -
            SCIKIT-LEARN USES L2 AS DEFAULT)
        OUTPUTS: FIT (MODEL INSTANCE), CLASSES, INTERCEPT, COEFICIENT,
            PREDICT PROBABILITY(MATRIX OF PROBABILITIES THAT THE PREDICTED OUTPUT IS EQUAL
            TO ZERO, 1-p(x), OR ONE, p(x)), SCORE (RATIO OF OBSERVATIONS CLASSIFIED CORRECTLY,
            ALSO KNOWN AS ACCURACY), CONFUSION MATRIX (PROVIDE THE ACTUAL AND PREDICTED OUTPUTS
            REGARDING TRUE NEGATIVE (C0,0), FALSE NEGATIVE (C1,0), FALSE POSITIVES (C0,1)
            AND TRUE POSITIVES (C1,1))
        """
        # checking wheter the array is unidimensional and reshaping it
        array_x = ExploratoryDataAnalysis().reshape_1d_arrays(array_x)
        # fitting the model
        model = LogisticRegression(C=c_positive_floating_point_number,
                                   class_weight=class_weight, dual=bl_dual,
                                   fit_intercept=bl_fit_intercept,
                                   intercept_scaling=intercept_scaling,
                                   l1_ratio=l1_ratio, max_iter=int_max_iter,
                                   multi_class=mult_class_classifier, n_jobs=n_jobs,
                                   penalty=penalty, random_state=random_state,
                                   solver=solver, tol=float_tolerance, verbose=verbose,
                                   warm_start=bl_warm_start).fit(array_x, array_y)
        # returning model fitted
        return {
            'model_fitted': model,
            'classes': model.classes_,
            'intercept': model.intercept_,
            'coeficient': model.coef_,
            'predict_probability': model.predict_proba(array_x),
            'predictions': model.predict(array_x),
            'score': model.score(array_x, array_y),
            'confusion_matrix': confusion_matrix(array_y,
                                                 model.predict(array_x)),
            'classification_report': classification_report(array_y,
                                                           model.predict(
                                                               array_x),
                                                           output_dict=True),
            'log_likelihood': class_likelihood_ratios(array_y, model.predict(array_x))
        }


class NonLinearEquations:

    def differential_evolution(self, cost_func, list_bounds, method='scipy', max_iter=1000,
                               max_iterations_wo_improvement=100, int_verbose_monitor=10,
                               bl_print_convergence_messages=False, bl_print_warning_messages=True,
                               bl_inter_monitor=False, int_size_trial_solution_population=40,
                               tolerance=5e-5):
        """
        REFERENCES: https://stackoverflow.com/questions/21765794/python-constrained-non-linear-optimization,
            https://mystic.readthedocs.io/en/latest/mystic.html, https://docs.scipy.org/doc/scipy/reference/optimize.html
        DOCSTRING: PRICE & STORN DIFFERENTIAL EVOLUTION SOLVER
        INPUTS:
        OUTPUTS:
        """
        if method == 'scipy':
            return differential_evolution(cost_func, list_bounds, maxiter=max_iter, tol=tolerance,
                                          disp=bl_print_convergence_messages)
        elif method == 'mystic':
            if bl_inter_monitor == True:
                mon = VerboseMonitor(int_verbose_monitor)
                return diffev2(
                    cost_func, x0=list_bounds, bounds=list_bounds,
                    npop=int_size_trial_solution_population,
                    gtol=max_iterations_wo_improvement, disp=bl_print_convergence_messages,
                    full_output=bl_print_warning_messages, itermon=mon, maxiter=max_iter,
                    ftol=tolerance)
            else:
                return diffev2(
                    cost_func, x0=list_bounds, bounds=list_bounds,
                    npop=int_size_trial_solution_population,
                    gtol=max_iterations_wo_improvement, disp=bl_print_convergence_messages,
                    full_output=bl_print_warning_messages, maxiter=max_iter, ftol=tolerance)
        else:
            raise Exception(
                'Method not recognized, please revisit the parameter')

    def optimize_curve_fit(self, func, array_x, array_y):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return curve_fit(func, xdata=array_x, y_data=array_y)

    def polynomial_fit(self, array_x, array_y):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS: ARRAY ESTIMATIVES AND ARRAY POLYNOMIAL INTERPOLATED VALUES
        """
        # estimatives
        y_est = np.polyfit(array_x, array_y)
        # return estimatives and values interpolated
        return {
            'coefficients': y_est,
            'values_interpolated': np.polyval(y_est, array_x)
        }
