### TERM STRUCTURE OF INTEREST RATE CURVE ###

import matplotlib.pyplot as plt
from stpstone.directories_files_manag.managing_ff import DirFilesManagement


class TSIR:

    def nper_ytm_curve(self, dict_nper_rates, chart_title, complete_path_save_fig,
                       flag_show_plot=False):
        '''
        REFERENCES: https://matplotlib.org/tutorials/introductory/pyplot.html
        DOCSTRING: YTM CURVE
        INPUTS: DICT NPER RATES, CHART TITLE, COMPLETE PATH TO SAVE THE FIGURE AND BOOLEAN 
            TO WHETER SHOW OR NOT THE CHART (FALSE AS STANDARD)
        OUTPUTS:
        '''
        plt.figure()
        plt.plot(dict_nper_rates.keys(), dict_nper_rates.values())
        plt.ylabel('Yield to Maturity')
        plt.xlabel('Working Days to Maturity')
        plt.title(chart_title)
        plt.savefig(complete_path_save_fig)
        if flag_show_plot == True:
            plt.show()
        return DirFilesManagement().object_exists(complete_path_save_fig)
