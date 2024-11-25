### FOLDERS TREE GENERATOR ###

import os


class FoldersTree:
    def __init__(self, str_path, bl_ignore_dot_folders=False, list_ignored_folders=None):
        '''
        DOCSTRING: INITIALIZE THE CLASS
        INPUTS: PATH, IGNORE DOT FOLDERS (OPTIONAL), LIST OF IGNORED FOLDERS (OPTIONAL)
        OUTPUTS: -
        '''
        self.str_path = str_path
        self.bl_ignore_dot_folders = bl_ignore_dot_folders
        self.list_ignored_folders = list_ignored_folders or ['__pycache__']

    def generate_tree(self, str_curr_path=None, bl_is_last=True, str_prefix='', bl_include_root=True, 
                      str_tree_structure=''):
        '''
        DOCSTRING: GENERATE A TREE STRUCTURE OF THE DIRECTORY
        INPUTS: CURRENT PATH (OPTIONAL), IS LAST ENTRY (OPTIONAL), PREFIX (OPTIONAL), 
            INCLUDE ROOT (OPTIONAL)
        OUTPUTS: A string representation of the directory tree structure.
        '''
        # initializing the tree structure
        if str_curr_path is None:
            str_curr_path = self.str_path
        # add the parent folder name as the first line if bl_include_root is True
        if bl_include_root:
            str_tree_structure += f'{os.path.basename(self.str_path)}\n'
            #   reset str_prefix for the root folder
            str_prefix = ''
        # sort the entries
        list_entries = sorted(os.listdir(str_curr_path))
        # loop through the entries
        for idx, str_entry in enumerate(list_entries):
            str_entry_path = os.path.join(str_curr_path, str_entry)
            # skip ignored folders
            if self.bl_ignore_dot_folders and str_entry.startswith('.'):
                continue
            if str_entry in self.list_ignored_folders:
                continue
            # creating brach prefix
            bl_is_directory = os.path.isdir(str_entry_path)
            bl_is_last_entry = idx == len(list_entries) - 1
            str_branch_prefix = '└── ' if bl_is_last_entry else '├── '
            str_tree_structure += f'{str_prefix}{str_branch_prefix}{str_entry}\n'
            # if the str_entry is a directory recursively add subdirectories
            if bl_is_directory:
                # Recursively add subdirectories
                str_new_prefix = str_prefix + ('    ' if bl_is_last_entry else '│   ')
                str_tree_structure += self.generate_tree(
                    str_entry_path, 
                    bl_is_last=bl_is_last_entry, 
                    str_prefix=str_new_prefix, 
                    bl_include_root=False
                )
        # return the tree structure
        return str_tree_structure

    @property
    def print_tree(self):
        '''
        DOCSTRING: PRINT THE TREE STRUCTURE
        INPUTS:
        OUTPUTS:
        '''
        print(self.generate_tree())

    def export_tree(self, filename=None):
        '''
        DOCSTRING: EXPORT THE TREE STRUCTURE TO A FILE
        INPUTS: FILENAME (OPTIONAL)
        OUTPUTS: -
        '''
        str_tree_structure = self.generate_tree()
        if filename:
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(str_tree_structure)
            print(f'Tree structure has been written to {filename}')
        else:
            return str_tree_structure
