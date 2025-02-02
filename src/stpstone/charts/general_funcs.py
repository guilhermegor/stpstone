### GENERAL FUNCTIONS FOR PLOTS

from pathlib import Path

import matplotlib.pyplot as plt


class GeneralPltFuncs:
    def save_fig(
        fig_id,
        tight_layout=True,
        fig_extension='png',
        resolution=300,
        img_dir='images',
        sub_dir='end_to_end_project',
    ):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # creating directories
        images_path = Path() / img_dir / sub_dir
        images_path.mkdir(parents=True, exist_ok=True)
        # creating plot complente path name with layout of interest
        path = images_path / f'{fig_id}.{fig_extension}'
        if tight_layout:
            plt.tight_layout()
        # saving figure
        plt.savefig(path, format=fig_extension, dpi=resolution)
