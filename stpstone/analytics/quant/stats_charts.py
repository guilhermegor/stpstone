### CHARTS FOR PROBABILITY AND STATISTICAL PROBLEMS ###

import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import statsmodels.api as sm
from matplotlib.colors import ListedColormap
from sklearn.metrics import precision_recall_curve, roc_curve, mean_squared_error
from sklearn.model_selection import train_test_split
from stpstone.quantitative_methods.regression import LogLinearRegressions
from stpstone.analytics.quant.prob_distributions import NormalDistribution
from stpstone.quantitative_methods.fit_assessment import FitPerformance


class ProbStatsCharts:

    def confusion_mtx_2x2(self, x_array_real_numbers, y_vector_real_numbers,
                          c_positive_floating_point_number=1.0):
        """
        DOCSTRING: CONFUSION MATRIX WITH PREDICTIONS (Y-AXIS) AND ACTUAL TARGETS (X-DATA) OF
            THE DATA TESTED
        INPUTS: X AND Y ARRAIES OF REAL NUMBERS, AS WELL AS C POSITIVE FLOATING-POINT NUMBER
            THAT DEFINES THE RELATIVE STRENGTH OF REGULARIZATION; SMALLER VALUES INDICATE STRONGER
            REGULARIZATION, FITTING-WISE ITS POORLY FITTED
        OUTPUTS: -
        """
        cm = LogLinearRegressions().logistic_regrression_logit(x_array_real_numbers,
                                                           y_vector_real_numbers,
                                                           c_positive_floating_point_number)[
                                                               'confusion_matrix']
        fig, ax = plt.subplots(figsize=(8, 8))
        ax.imshow(cm)
        ax.grid(False)
        ax.xaxis.set(ticks=(0, 1), ticklabels=('Predicted 0s', 'Predicted 1s'))
        ax.yaxis.set(ticks=(0, 1), ticklabels=('Actual 0s', 'Actual 1s'))
        ax.set_ylim(1.5, -0.5)
        for i in range(2):
            for j in range(2):
                ax.text(j, i, cm[i, j], ha='center', va='center', color='red')
        plt.show()

    def confusion_mtx_nxn(self, conf_mx, cmap=plt.get_cmap('gray'), bl_focus_errors=True,
                          complete_saving_path=None, int_fill_non_error_values=0):
        """
        DOCSTRING: HEATMAP OF CONFUSION MATRIX FOR COMPLEX MODELS, IN GRAY SCALE, BEING
            WHITER AN INDICATIVE OF HIGH NUMBER OF OCCURRENCES
        INPUTS: CONFUSION MATRIX, CMAP(GRAY AS DEFAULT), BOOLEAN FOCUS ON ERRORS, COMPLETE
            SAVING PATH, NUMBER TO BE FILLED IN NON ERROR VALUES (DIAGONAL)
        OUTPUTS: -
        """
        # checking wheter or not to focus on errors (occurrences not in diagonal)
        if bl_focus_errors == True:
            row_sums = conf_mx.sum(axis=1, keepdims=True)
            conf_mx = conf_mx / row_sums
            np.fill_diagonal(conf_mx, int_fill_non_error_values)
        # plot confusion matrix
        plt.matshow(conf_mx, cmap=cmap)
        # saving the plot, if is user's will
        if complete_saving_path != None:
            plt.savefig(complete_saving_path)
        # showing plot
        plt.show()

    def ecdf_chart(self, list_ser_data, legend_position='lower right'):
        """
        DOCSTRING: DISPLAY A EMPIRICAL CUMULATIVE DISTRIBUTION FUNCTION CHART
        INPUTS: LIST OF DICTIONARIES WITH DATA, X_LABEL, Y_LABEL AND LEGEND KEYS, BESIDE
            LEGEND POSITION
        OUTPUTS: -
        """
        # defining layout
        sns.set()
        # computing ecdf for the given data
        for i in range(list_ser_data):
            x_axis, y_axis = NormalDistribution().ecdf(
                list_ser_data[i]['data'])
            # generating plot
            _ = plt.plot(x_axis, y_axis, marker='.', linestyle='none')
            # label the axis
            _ = plt.xlabel(list_ser_data[i]['data']['x_axis_label'])
            _ = plt.ylabel(list_ser_data[i]['data']['y_axis_label'])
        # annotate the plot
        _ = plt.legend((x['legend']
                        for x in list_ser_data), loc=legend_position)
        # display the plot
        plt.show()

    def boxplot(self, df_data, x_axis_column_name, y_axis_column_name, x_label, y_label):
        """
        DOCSTRING: BOXPLOT TO EVALUATE INTER QUANTILE RANGE AND OUTLIERS
        INPUTS: DATAFRAME DATA, X axis COLUMN NAME, Y axis COLUMN NAME, X LABEL AND Y LABEL
        OUTPUTS: -
        """
        _ = sns.boxplot(x=x_axis_column_name,
                        y=y_axis_column_name, data=df_data)
        _ = plt.xlabel(x_label)
        _ = plt.ylabel(y_label)
        plt.show()

    def scatter_plot(self, x_axis_data, y_axis_data, x_axis_label, y_axis_label):
        """
        DOCSTRING: SCATTER PLOT TO INFERE ABOUT CORRELATION BETWEEN DATA
        INPUTS: X axis DATA, Y axis DATA, X axis LABEL AND Y axis LABEL
        OUTPUTS: -
        """
        # defining layout
        sns.set()
        # setting data to chart
        _ = plt.plot(x_axis_data, y_axis_data)
        # labelling axis
        _ = plt.xlabel(x_axis_label)
        _ = plt.ylabel(y_axis_label)
        # displaying plot
        plt.show()

    def pandas_histogram_columns(self, dataframe, bins=None, figsize=(20, 15),
                                 complete_saving_path=None):
        """
        DOCSTRING: EXPLORATORY DATA ANALYSIS OF DATAFRAME COLUMNS FROM PANDAS
        INPUTS: DATAFRAME, BINS (NONE AS DEFAULT), FIGSIZE (((20,15) TUPLE AS DEFAULT)),
            COMPLETE SAVING PATH (NONE AS DEFAULT)
        OUTPUTS: -
        """
        # defining number of bins in the chart
        if bins is None:
            bins = np.sqrt(dataframe.shape[0])
        # defining layout
        sns.set()
        # fetch to memory dataframe histogram of each column for a
        dataframe.hist(bins=bins, figsize=figsize)
        # save figure if its user interest
        if complete_saving_path != None:
            plt.savefig(complete_saving_path)
        # show plot
        plt.show()

    def plot_precision_recall_vs_threshold(self, model_fitted, array_data, array_target,
                                           cross_validation_folds=3, scoring_method='accuracy',
                                           localization_legend='center right',
                                           label_precision='Precision', label_recall='Recall',
                                           line_style_precision='b--', line_style_recall='g-',
                                           line_width=2, label_axis_x='Threshold',
                                           label_axis_y='Percentage', font_size=16, bl_grid=True,
                                           tup_fig_size=(8, 4),
                                           complete_saving_path=None):
        """
        REFERENCES: (BINARY CLASSIFIER) https://colab.research.google.com/github/ageron/handson-ml2/blob/master/03_classification.ipynb#scrollTo=rUZ6ahZ7G0BO
        DOCSTRING: PLOT TO COMPARE PRECISION-RECALL TRADE-OFF, CLASSIFICATION-WISE IN SUPERVISED
            LEARNING TASKS FOR MACHINE LEARNING MODELS
        INPUTS: MODEL FITTED, ARRAY DATA, ARRAY TARGET, CROSS VALIDATION FOLDS, SCORING METHODS,
            LOCALIZATION OF THE LEGEND, LABEL PRECISION CURVE, LABEL RECALL CURVE, LINE STYLE
            PRECISION, LIBE STYLE RECALL, LINE WIDTH, LABEL AXIS X, LABEL AXIS Y, FONT SIZE,
            BOOLEAN GRID, TUPPLE FIGURE SIZE AND COMPLETE SAVING PATH (NONE AS DEFAULT)
        OUTPUTS: -
        """
        # cross validating the model --> establishing accuracy
        array_target_scores = FitPerformance().cross_validation(
            model_fitted, array_data, array_target, cross_validation_folds, scoring_method)[
                'scores']
        # precision-recall curve
        precisions, recalls, thresholds = precision_recall_curve(
            array_target, array_target_scores)
        # precision curve
        plt.plot(thresholds, precisions[:-1], line_style_precision,
                 label=label_precision, linewidth=line_width)
        # recall curve
        plt.plot(thresholds, recalls[:-1], line_style_recall,
                 label=label_recall, linewidth=line_width)
        # legend position
        plt.legend(loc=localization_legend, fontsize=font_size)
        # labeling axis
        plt.xlabel(label_axis_x, fontsize=font_size)
        plt.ylabel(label_axis_y, fontsize=font_size)
        # wheter place a grid or not
        plt.grid(bl_grid)
        # plot figure
        plt.figure(figsize=tup_fig_size)
        # saving the plot, if is user's will
        if complete_saving_path != None:
            plt.savefig(complete_saving_path)
        # showing plot
        plt.show()

    def plot_roc_curve(self, model_fitted, array_data, array_target,
                       cross_validation_folds=3, scoring_method='accuracy',
                       plot_title=None, label_x_axis='False Positive Rate (Fall-Out)',
                       label_y_axis='True Positive Rate (Recall)', font_size=16,
                       bl_grid=True, tup_fig_size=(8, 4), complete_saving_path=None):
        """
        REFERENCES: (ROC CURVES) https://colab.research.google.com/github/ageron/handson-ml2/blob/master/03_classification.ipynb#scrollTo=rUZ6ahZ7G0BO
        DOCSTRING: RECEIVER OPERATING CHARACTERISTIC (ROC) CRUVE PLOTS SENSITIVITY (RECALL) VERSUS
            1 - SPECIFICITY (TRUE NEGATIVE RATIO, OR THE RATIO OF NEGATIVE INSTANCES THAT ARE
            CORRECTLY CLASSIFIED AS NEGATIVE)
        INPUTS: MODEL FIITED, ARRAY DATA, ARRAY TARGET, CROSS VALIDATION FOLDS, SCORING METHOD,
            PLOT TITLE, LABEL X AXIS, LABEL Y AXIS, FONT SIZE, BL GRID, TUP FIGURE SIZE, AND
            COMPLETE SAVING PATH OF THE FIGURE
        OUTPUTS: -
        """
        # cross validating the model --> establishing accuracy
        array_target_scores = HandlingClassification().cross_validation_score(
            model_fitted, array_data, array_target, cross_validation_folds, scoring_method)
        # receiver operating characteristics (roc curve responses)
        fpr, tpr, thresholds = precision_recall_curve(
            array_target, array_target_scores)
        plt.plot(fpr, tpr, linewidth=2, label=plot_title)
        # dashed diagonal
        plt.plot([0, 1], [0, 1], 'k--')
        plt.axis([0, 1, 0, 1])
        # label axis
        plt.xlabel(label_x_axis, fontsize=font_size)
        plt.ylabel(label_y_axis, fontsize=font_size)
        # wheter place a grid or not
        plt.grid(bl_grid)
        # saving the plot, if is user's will
        if complete_saving_path != None:
            plt.savefig(complete_saving_path)
        # plot figure
        plt.figure(figsize=tup_fig_size)
        # showing plot
        plt.show()

    def histogram(self, sample_vector, suptitle, subtitle_vector=list(),
                  ncols=None, nrows=None, nbins=100, limits=(-100, 100),
                  tick_label_size=30, size=(60, 30), suptitle_fontsize=80,
                  subtitle_fontsize=40, bl_save=True, filepath=r'C:/Temp/Teste.png'):
        """
        DOCSTRING: HISTOGRAM
        INPUTS: VECTOR WITH DATA TO BE PLOTED, TITLE
        OUTPUTS: -
        """
        if not ncols or not nrows:
            fig, ax = plt.subplots()
            ax.hist(sample_vector, nbins, range=limits)
            ax.tick_params(labelsize=tick_label_size)
            plt.gcf().set_size_inches(size[0], size[1])
            fig.suptitle(suptitle, fontsize=suptitle_fontsize)
            if bl_save:
                fig.savefig(filepath)
        elif ncols and nrows:
            fig = plt.figure(figsize=(size[0], size[1]))
            fig, ax = plt.subplots(nrows, ncols)
            for i in range(nrows):
                for j in range(ncols):
                    figure = ncols * i + j
                    if ncols * nrows != len(subtitle_vector):
                        if figure >= len(subtitle_vector):
                            break
                    ax[i, j].hist(
                        sample_vector[figure],
                        nbins,
                        limits
                    )
                    ax[i, j].set_title(subtitle_vector[figure],
                                       fontsize=subtitle_fontsize)
                    ax[i, j].tick_params(labelsize=tick_label_size)
            plt.gcf().set_size_inches(size[0], size[1])
            fig.suptitle(suptitle, fontsize=suptitle_fontsize)
            if bl_save:
                fig.savefig(filepath)

    def abline(self, plt_, slope, intercept):
        """
        DOCSTRING: PLOT A LINE FROM SLOPE AND INTERCEPT
        INPUTS: PLT_, SLOPE, ITERCEPT
        OUTPUTS
        """
        axes = plt_.gca()
        x_vals = np.array(axes.get_xlim())
        y_vals = intercept + slope * x_vals
        plt_.plot(x_vals, y_vals, '--')

    def add_identity(axes, *line_args, **line_kwargs):
        """
        DOCSTRING: PLOT A LINE FROM SLOPE AND INTERCEPT
        INPUTS: PLT_, SLOPE, ITERCEPT
        OUTPUTS
        """
        identity, = axes.plot([], [], *line_args, **line_kwargs)

        def callback(axes):
            low_x, high_x = axes.get_xlim()
            low_y, high_y = axes.get_ylim()
            low = max(low_x, low_y)
            high = min(high_x, high_y)
            identity.set_data([low, high], [low, high])
        callback(axes)
        axes.callbacks.connect('xlim_changed', callback)
        axes.callbacks.connect('ylim_changed', callback)
        return axes

    def qq_plot(self, list_ppf, list_raw_data, chart_title=None, complete_saving_path=None,
                bl_show_plot=True, j=0):
        """
        REFERENCES: https://towardsdatascience.com/understand-q-q-plot-using-simple-python-4f83d5b89f8f
        DOCSTRING:
        INPUTS:
        OUTPUTS: -
        """
        # setting variables
        list_quantiles = list()
        # determining the quantiles
        for i in range(1, len(list_raw_data) + 1):
            j = i / len(list_raw_data)
            try:
                list_quantiles.append(np.quantile(list_raw_data, j))
            except:
                print(list_quantiles)
                raise Exception('ERRO LIST QUANTILES')
        # sort output and plot
        fig, ax = plt.subplots(figsize=(10, 8))
        plt.plot(list_quantiles, sorted(list_ppf), 'x')
        # add red line in secondary diagonal
        # self.add_identity(plt, color='r', ls='--')
        plt.plot([0, 1], [0, 1], 'k--', linewidth=1,
                 transform=ax.transAxes, color='red')
        # chart title and axis names
        if chart_title != None:
            plt.title(chart_title)
        plt.ylabel('Theoretical Quantiles - PPF')
        plt.xlabel('Sample Quantiles')
        plt.grid()
        # saving the plot, if is user's will
        if complete_saving_path != None:
            plt.savefig(complete_saving_path)
        # showing plot
        if bl_show_plot == True:
            plt.show()
        # returning quantiles
        return list_quantiles

    def plot_learning_curves(
            model, array_data, array_target, complete_path_save_figure=None,
            float_test_size=0.2, list_axis=[0, 80, 0, 3],
            line_type_training_error='r-+', line_type_val_error='b-',
            line_width_training_error=2, line_width_val_error=3, label_training_error='trainig_data',
            label_val_error='validation_data', x_label='Training set size',
            y_label='Root Mean Squared Error (RMSE)', plt_label='Model Perform',
            legend_plot_position='upper right', int_font_size=14):
        """
        REFERENCES: “HANDS-ON MACHINE LEARNING WITH SCIKIT-LEARN, KERAS, AND TENSORFLOW,
            2ND EDITION, BY AURÉLIEN GÉRON (O’REILLY). COPYRIGHT 2019 KIWISOFT S.A.S.,
            978-1-492-03264-9.”
        DOCSTRING: LEARNING CURVES TO INFERE WHETER A MODEL PERFORMS WELL ON THE TRAINING SET AND
            GENERALIZES ACCORDINLGY TO THE VALIDATION SET
        INPUTS: ARRAY DATA, ARRAY TARGET AND COMPLETE PATH NAME TO SAVE THE FIGURE (NONE AS DEFAULT)
        OUTPUTS: -
        OBSERVATION: A GAP BETWEEN TRAINING AND VALIDATION DATA IS THE HALLMARK OF OVERFITTING
            MODELS - A WORKAROUND IS TO FEED MORE TRAINING DATA UNITL THE VALIDATION ERROR REACHES
            THE TRAINING ERROR
        """
        # creating test-training sets
        X_train, X_val, y_train, y_val = train_test_split(array_data, array_target,
                                                          test_size=float_test_size)
        # defining initial paremeters
        train_errors, val_errors = [], []
        # iterating through trainig set size from 1 to n
        for m in range(1, len(X_train)):
            # fit desired model to infere wheter or not its under or overfitted
            model.fit(X_train[:m], y_train[:m])
            # predict values using the fitted model
            y_train_predict = model.predict(X_train[:m])
            # identifying the noise of prediction, in comparison to target values
            y_val_predict = model.predict(X_val)
            # appendig data of
            train_errors.append(mean_squared_error(
                y_train[:m], y_train_predict))
            val_errors.append(mean_squared_error(y_val, y_val_predict))
        # defining plot title
        plt.title(label=plt_label)
        # axis
        plt.axis(list_axis)
        # ploting val and training errors, with RMSE (root mean square error) metric
        plt.plot(np.sqrt(train_errors), line_type_training_error,
                 linewidth=line_width_training_error, label=label_training_error)
        plt.plot(np.sqrt(val_errors), line_type_val_error, linewidth=line_width_val_error,
                 label=label_val_error)
        # configuring plot area
        plt.legend(loc=legend_plot_position, fontsize=int_font_size)
        plt.xlabel(x_label, fontsize=int_font_size)
        plt.ylabel(y_label, fontsize=int_font_size)
        # save figure
        if complete_path_save_figure != None:
            plt.savefig(complete_path_save_figure)
        # show plot
        plt.show()

    def classification_plot_2d_ivs(self, cls_scaler, array_x, array_y, cls_model_fiited,
                            label_yes, label_no, str_title,
                            str_xlabel, str_ylabel, complete_path_save_figure=None,
                            str_color_negative='salmon', str_color_positive='dodgerblue'):
        """
        REFERENCES: https://colab.research.google.com/drive/1-Slk6y5-E3eUnmM4vjtoRrGMoIKvD0hU#scrollTo=_NOjKvZRid5l
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # remove feature scaling from iv's
        X_set, y_set = cls_scaler.inverse_transform(array_x), array_y
        # linear interpolation of iv's
        X1, X2 = np.meshgrid(
            np.arange(start = X_set[:, 0].min() - 10, stop = X_set[:, 0].max() + 10, step = 0.25),
            np.arange(start = X_set[:, 1].min() - 1000, stop = X_set[:, 1].max() + 1000, step = 0.25)
        )
        # calculating predictions
        plt.contourf(X1, X2, cls_model_fiited.predict(cls_scaler.transform(
            np.array(
                [X1.ravel(), X2.ravel()]).T)).reshape(X1.shape),
                alpha = 0.75, cmap = ListedColormap((str_color_negative, str_color_positive))
        )
        # winsorizing ivs and dv
        plt.xlim(X1.min(), X1.max())
        plt.ylim(X2.min(), X2.max())
        for i, j in enumerate(np.unique(y_set)):
            #   defining data label name
            if j == 0:
                label = label_no
            elif j == 1:
                label = label_yes
            else:
                raise Exception('Label for the data {] not defined'.format(i))
            #   scatter plot binary result
            plt.scatter(
                X_set[y_set == j, 0],
                X_set[y_set == j, 1],
                c = ListedColormap(
                    (str_color_negative, str_color_positive)
                )(i),
                label = label
            )
        # define title
        plt.title(str_title)
        # axis labels
        plt.xlabel(str_xlabel)
        plt.ylabel(str_ylabel)
        # legend
        plt.legend()
        # save figure
        if complete_path_save_figure != None:
            plt.savefig(complete_path_save_figure)
        # show image
        plt.show()

# x = np.arange(10).reshape(-1, 1)
# y = np.array([0, 0, 0, 0, 1, 1, 1, 1, 1, 1])
# ProbStatsCharts().confusion_mtx(x, y, 7.0)
