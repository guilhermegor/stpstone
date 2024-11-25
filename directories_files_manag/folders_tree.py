### FOLDERS TREES ###

import os


class FoldersTree:

    def __init__(self, str_startpath):
        self.str_startpath = str_startpath
        self.str_tree = self.print_tree

    @property
    def print_tree(self, prefix=""):
        for item in os.listdir(self.str_startpath):
            path = os.path.join(self.str_startpath, item)
            print(f"{prefix}├── {item}")
            if os.path.isdir(path):
                self.print_tree(path, prefix + "│   ")