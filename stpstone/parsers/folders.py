### DIRECTORY AND FILES MANAGEMENT ###

# pypi.org libs
import os
import shutil
import tempfile
import pycurl
import os
import fnmatch
import wget
import py7zr
from datetime import datetime
from zipfile import ZipFile, ZIP_DEFLATED
from io import BytesIO, TextIOWrapper
from typing import Tuple, List, Union, Optional, Dict
from requests import Session, Response
# local libs
from stpstone.parsers.str import StrHandler
from stpstone.parsers.dicts import HandlingDicts


class DirFilesManagement:

    @property
    def get_cur_dir(self):
        """
        DOCSTRING: GET CURRENT DIRECTORY
        INPUTS: -
        OUTPUTS: CURRENT DIRECTORY
        """
        return os.getcwd()

    def list_dir_files(self, dir_path=None):
        """
        DOCSTRING: RETURN SUBFOLDERS OR FILE NAMES
        INPUTS: DIR NAME (IN CASE THIS ARGUMENT IS NONE THE RETURNED VALUE
        IS FILES IN THE FOLDER)
        OUTPUTS: LIST OF FILES OR SUBFOLDERS
        """
        return os.listdir(dir_path)

    def change_dir(self, dir_path):
        """
        DOCSTRING: CHANGE CURRENT DIRECTORY
        INPUTS: DIRECTORY NAME
        OUTPUTS: -
        """
        os.chdir(dir_path)

    def mk_new_directory(self, dir_path):
        """
        DOCSTRING: MAKE A NEW DIRECTORY
        INPUSTS: NAME OF THE DIRECTORY
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        """
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)
            return True
        else:
            return False

    def move_file(self, old_file_name, new_file_name):
        """
        DOCSTRING: MOVE A FILE FROM ORIGINAL DIRECTORY TO ANOTHER (IT DELETES THE OLD ONE)
        INPUTS: OLD AND NEW COMPLETE PATH NAME, AND DELETE OLD ONE
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        """
        shutil.move(old_file_name, new_file_name)
        return self.object_exists(new_file_name)

    def rename_dir_file(self, old_object_name, new_object_name):
        """
        DOCSTRING: RENAMING FILES OR FOLDERS
        INPUTS: OLD AND NEW COMPLETE PATH
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        """
        # renaming
        os.rename(old_object_name, new_object_name)
        # return status of accomplishment
        if os.path.exists(new_object_name):
            return True
        else:
            return False

    def removing_dir(self, dir_path):
        """
        DOCSTRING: REMOVE A DIRECTORY
        INPUTS: COMPLETE PATH OF THE DIRECTORY
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        """
        if len(self.list_dir_files(dir_path)) == 0:
            os.rmdir(dir_path)
        else:
            shutil.rmtree(dir_path)
        if not os.path.exists(dir_path):
            return True
        else:
            return False

    def removing_file(self, file_path):
        """
        DOCSTRING: REMOVE A FILE
        INPUTS: COMPLETE NAME
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        """
        os.remove(file_path)
        if not os.path.exists(file_path):
            return True
        else:
            return False

    def object_exists(self, object_path):
        """
        DOCSTRING: BLAMES WHETER OR NOT FILE/FOLDER HAS BEEN CREATED
        INPUTS: OBJECT PATH
        OUTPUTS: OK/NOK
        """
        if os.path.exists(object_path):
            return True
        else:
            return False

    def time_last_edition(self, object_path, bl_to_datetime=False):
        """
        DOCSTRING: TIMESTAMP WITH LAST SAVED EDITION IN THE FILE
        INPUTS: OBJECT PATH
        OUTPUTS: TUPLE WITH TIMESTAMP OF LAST EDITION AND WHETER FILE EXISTS OR NOT
        """
        if os.path.exists(object_path):
            if bl_to_datetime == True:
                return (datetime.fromtimestamp(os.path.getmtime(object_path)), True)
            else:
                return (os.path.getmtime(object_path), True)
        else:
            return ('INTERNAL ERROR', False)

    def time_creation(self, object_path):
        """
        DOCSTRING: TIMESTAMP WITH CREATION OF FILE
        INPUTS: OBJECT PATH
        OUTPUTS: TUPLE WITH TIMESTAMP OF FILE CREATION AND WHETER FILE EXISTS OR NOT
        """
        if os.path.exists(object_path):
            return (os.path.getctime(object_path), True)
        else:
            return ('INTERNAL ERROR', False)

    def time_last_access(self, object_path):
        """
        DOCSTRING: TIMESTAMP WITH LAST ACCESS TO THE FILE
        INPUTS: OBJECT PATH
        OUTPUTS: TUPLE WITH TIMESTAMP OF FILE LAST ACCESS AND WHETER FILE EXISTS OR NOT
        """
        if os.path.exists(object_path):
            return (os.path.getatime(object_path), True)
        else:
            return ('INTERNAL ERROR', False)

    def get_file_name_path_split(self, complete_file_name):
        """
        DOCSTRING: GET FILE PATH AND NAME IN A TUPLE
        INPUTS: COMPLETE FILE NAME
        OUTPUT: RETURNS TUPLE WITH FILE NAME HEAD (PATH) AND TAIL (NAME)
        """
        return os.path.split(complete_file_name)

    def join_n_path_components(self, *path_components):
        """
        DOCSTRING: JOIN PATH COMPONENTS
        INPUTS: N-PATH COMPONENTS
        OUTPUTS: OUTPUT COMPLETE PATH
        """
        path_output = ''
        for path_component in path_components:
            path_output = os.path.join(path_output, path_component)
        return path_output

    def get_filename_parts_from_url(self, url):
        """
        DOCSTRING: GET FILE NAME FROM A COMPLETE URL
        INPUTS: COMPLETE URL
        OUTPUTS: FILENAME WITH AND WITHOUT EXTENSION, IN STR AND LIST TYPES, RESPECTIVELLY
        """
        fullname = url.split('/')[-1].split('#')[0].split('?')[0]
        t = list(os.path.splitext(fullname))
        if t[1]:
            t[1] = t[1][1:]
        return t

    def download_web_file(self, url, filepath=None):
        """
        DOCSTRING: DOWNLOAD FILE FROM WEB (DOWNLOADED TEMPORARY FILENAME IF NO FILEPATH IS PROVIDED)
        INPUTS: COMPLETE PATH TO FILE AND FILE NAME WITH EXTENSION
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        OBS: IF IT IS NEEDED TO PASS CREDENTIALS PARAMETER URL_PATH OUGHT BE DECLARED AS:
            'ftp://username:password@server/path/to/file'
        """
        # removing previous version
        if self.object_exists(filepath) == True:
            _ = self.removing_file(filepath)
        # fetching data
        if not filepath:
            _, suffix = self.get_filename_parts_from_url(url)
            f = tempfile.NamedTemporaryFile(suffix='.' + suffix, delete=False)
            filepath = f.name
        else:
            f = open(filepath, 'wb')
        c = pycurl.Curl()
        c.setopt(pycurl.URL, str(url))
        c.setopt(pycurl.WRITEFUNCTION, f.write)
        try:
            c.perform()
            c.close()
            f.close()
            return self.object_exists(filepath)
        except:
            c.close()
            f.close()
            _ = self.removing_file(filepath)
            wget.download(url, filepath)
            return self.object_exists(filepath)

    def get_zip_from_web_in_memory(self, req_resp:Response, 
                                   bl_io_interpreting:bool=False) -> List[ZipFile]:
        """
        REFERENCES: https://stackoverflow.com/questions/5710867/downloading-and-unzipping-a-zip-file-without-writing-to-disk
        DOCSTRING: DOWNLOAD A ZIP AND UNZIP IT, HANDLING FILE IN MEMORY
        INPUTS: FILE URL
        OUTPUTS: LIST OF OPENNED FILES UNZIPPED
        """
        zipfile = ZipFile(BytesIO(req_resp.content))
        # defining names of files from extraction
        zip_names = zipfile.namelist()
        # check wheter the exported content is a file or a list of files
        if len(zip_names) == 1:
            file_name = zip_names.pop()
            extracted_file = zipfile.open(file_name)
            #   decode stream to string, if is the user will, through io module
            if bl_io_interpreting == True:
                return TextIOWrapper(extracted_file)
            else:
                return extracted_file
        return [zipfile.open(file_name) for file_name in zip_names]

    def unzip_files_from_dir(self, destination_path):
        """
        DOCSTRING: UNZIP ALL FILES FROM A FOLDER
        INPUTS: DESTINATION PATH
        OUTPUTS: NONE
        """
        list_files_unz = list()
        files = os.listdir(destination_path)
        for file in files:
            if file.endswith('.zip'):
                filePath = destination_path + '/' + file
                zip_file = ZipFile(filePath)
                list_files_unz.append(zip_file.namelist())
                for names in zip_file.namelist():
                    zip_file.extract(names, destination_path)
                zip_file.close()
        return list_files_unz

    def unzip_file(self, zippedfile_path, dir_destiny):
        """
        DOCSTRING: UNZIP ONE SINGULAR ZIP FILE TO A DESTINATION
        INPUTS: ZIPPED FILE PATH AN DESTINATION PATH
        OUTPUTS: LIST OF UNZIPPED FILES
        """
        with ZipFile(zippedfile_path, 'r') as zipobj:
            list_zip_files = zipobj.namelist()
            zipobj.extractall(dir_destiny)
        return list_zip_files

    def compress_to_zip(self, list_files_archive, zfilename):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # creating object of zipfile compression
        zout = ZipFile(zfilename, 'w', ZIP_DEFLATED)
        # looping through archive files
        for fname in list_files_archive:
            zout.write(fname)
        return True

    def compress_to_7z_file(self, file_path_7z, object_to_compress, method='w'):
        """
        REFERENCES: https://github.com/miurahr/py7zr
        DOCSTRING: ZIP FILE TO 7ZR, OBJECT TO COMPRESS PATH, AND METHOD (WRTIE AS DEFAULT)
        INPUTS: 7ZR FILE NAME, DESTINY DIRECTORY
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        """
        with py7zr.SevenZipFile(file_path_7z, mode=method) as archive:
            archive.writeall(object_to_compress)
        return self.object_exists(file_path_7z)

    def decompress_7z_file(self, file_path_7z, method='r'):
        """
        REFERENCES: https://github.com/miurahr/py7zr
        DOCSTRING: ZIP FILE TO 7ZR
        INPUTS: 7ZR FILE NAME, STR ARCNAME (BASE AS DEFAULT) AND STR MODE (READ AS DEFAULT)
        OUTPUTS: LIST OF FILE NAMES COMPRESSED
        """
        with py7zr.SevenZipFile(file_path_7z, mode=method) as archive:
            list_file_names = archive.getnames()
            archive.extractall()
        return list_file_names

    def choose_last_saved_file_w_rule(self, parent_dir, name_like):
        """
        DOCSTRING: CHOOSE LAST SAVED FILE WITH RULE
        INPUTS: PARENT DIR AND PART OF THE NAME OF THE FILE
        OUTPUTS: NOK OR COMPLETE NAME OF THE FILE
        """
        # setting passaging variables
        files_dir = os.listdir(parent_dir)
        file_dir = None
        file_name_return = None
        # looping through all files in the folder and returning the last edited one with the
        #   name like given
        for file_dir in files_dir:
            if fnmatch.fnmatch(file_dir, name_like):
                if file_name_return == None:
                    file_name_return = file_dir
                else:
                    if os.path.getmtime(parent_dir
                                        + file_dir) > os.path.getmtime(
                                            os.path.join(parent_dir, file_name_return)):
                        file_name_return = file_dir
        # return the complete file path, or NOK whether it has not been found
        if file_name_return == None:
            return False
        else:
            return os.path.join(parent_dir, file_name_return)

    def copy_file(self, org_file_path, dest_direcory):
        """
        DOCSTRING: COPY FILE TO A FOLDER
        INPUTS: ORIGINAL AND DESTINATION COMPLETE FILE PATH
        OUTPUTS: STATUS OF ACCOMPLISHMENT - NO ORIGINAL FILE/OK
        """
        if os.path.exists(org_file_path):
            shutil.copy(org_file_path, dest_direcory)
            return True
        else:
            return 'NO ORIGINAL FILE'

    def walk_folder_subfolder_w_rule(self, root_directory, list_name_like):
        """
        DOCSTRING: WALK THROUGH ALL FILES IN A FOLDER AND ITS SUBFOLDERS, RETURNING COMPLETE PATH OF
            FILES WITH A NAME LIKE OF INTEREST
        INPUTS: ROOT DIRECTORY
        OUTPUTS: LIST OF FILE PATHS
        """
        list_paths = list()
        for directory, _, files in os.walk(root_directory):
            for file in files:
                if any([fnmatch.fnmatch(file, name_like) == True for name_like in list_name_like]):
                    list_paths.append(os.path.join(directory, file))

        return list_paths

    def walk_folder_subfolder(self, root_directory):
        """
        DOCSTRING: WALK THROUGH ALL FILES IN A FOLDER AND ITS SUBFOLDERS
        INPUTS: ROOT DIRECTORY
        OUTPUTS: LIST OF FILES PATHS
        """
        list_paths = list()
        for directory, _, files in os.walk(root_directory):
            for file in files:
                list_paths.append(os.path.join(directory, file))
        return list_paths

    def loop_files_w_rule(self, directory, name_like, bl_first_last_edited=True,
                          bl_to_datetime=True, key_file_name='file_name',
                          key_file_last_edition='file_last_edition'):
        """
        DOCSTRING: RETURN FILES FROM A FOLDER WITH A GIVEN RULE
        INPUTS: DIRECTORY AND RULE (NAME_LIKE)
        OUTPUTS: RETURNS FILES PATHS WITH A GIVEN RULE
        """
        # creating list of files in a given directory with a given part of name
        list_files_names_like = [file_name for file_name in os.listdir(directory)
                                 if StrHandler().match_string_like(file_name, name_like)]
        # checking whether it is necessary to retrieve the files in a last edition order
        if bl_first_last_edited == False:
            return list_files_names_like
        else:
            #   list of last edition times
            list_files_last_edition = [self.time_last_edition(os.path.join(
                directory, file_name), bl_to_datetime=bl_to_datetime)
                for file_name in list_files_names_like]
            #   creating a list of dictionaries with name and last edition time
            list_ser_file_name_last_edition = [{
                key_file_name: list_files_names_like[i],
                key_file_last_edition: list_files_last_edition[i][0]
            } for i in range(len(list_files_last_edition))]
            #   sort list of dictionaries
            return [os.path.join(directory, dict_[key_file_name]) for dict_
                    in HandlingDicts().multikeysort(list_ser_file_name_last_edition,
                                                    ['-' + key_file_last_edition])]

    def list_dir_files(self, dir_path=None):
        """
        DOCSTRING: RETURN SUBFOLDERS OR FILE NAMES
        INPUTS: DIR NAME (IN CASE THIS ARGUMENT IS NONE THE RETURNED VALUE
        IS FILES IN THE FOLDER)
        OUTPUTS: LIST OF FILES OR SUBFOLDERS
        """
        return os.listdir(dir_path)

    def get_file_format_from_file_name(self, filename):
        """
        DOCSTRING: GET FILE FORMAT FROM FILEN NAME
        INPUTS: FILE NAME
        OUTPUTS: FORMAT
        """
        return filename.split('.')[-1]

    def get_file_size(self, filename):
        """
        DOCSTRING: GET FILE SIZE IN BYTES
        INPUTS: FILENAME
        OUTPUTS: FLOAT
        """
        return os.path.getsize(filename)


class FoldersTree:
    def __init__(self, str_path, bl_ignore_dot_folders=False, list_ignored_folders=None, 
                 bl_add_linebreak_markdown=False):
        """
        DOCSTRING: INITIALIZE THE CLASS
        INPUTS: PATH, IGNORE DOT FOLDERS (OPTIONAL), LIST OF IGNORED FOLDERS (OPTIONAL)
        OUTPUTS: -
        """
        self.str_path = str_path
        self.bl_ignore_dot_folders = bl_ignore_dot_folders
        self.list_ignored_folders = list_ignored_folders or ['__pycache__']
        self.bl_add_linebreak_markdown = bl_add_linebreak_markdown

    def generate_tree(self, str_curr_path=None, bl_is_last=True, str_prefix='', bl_include_root=True, 
                      str_tree_structure=''):
        """
        DOCSTRING: GENERATE A TREE STRUCTURE OF THE DIRECTORY
        INPUTS: CURRENT PATH (OPTIONAL), IS LAST ENTRY (OPTIONAL), PREFIX (OPTIONAL), 
            INCLUDE ROOT (OPTIONAL)
        OUTPUTS: A string representation of the directory tree structure.
        """
        # initializing the tree structure
        if str_curr_path is None:
            str_curr_path = self.str_path
        # line break if bl_add_linebreak_markdown is True
        if self.bl_add_linebreak_markdown == True:
            str_linebreak_md = '<br>'
        else:
            str_linebreak_md = ''
        # add the parent folder name as the first line if bl_include_root is True
        if bl_include_root:
            str_tree_structure += f'{os.path.basename(self.str_path)}{str_linebreak_md}\n'
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
            str_tree_structure += f'{str_prefix}{str_branch_prefix}{str_entry}{str_linebreak_md}\n'
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
        """
        DOCSTRING: PRINT THE TREE STRUCTURE
        INPUTS:
        OUTPUTS:
        """
        print(self.generate_tree())

    def export_tree(self, filename=None):
        """
        DOCSTRING: EXPORT THE TREE STRUCTURE TO A FILE
        INPUTS: FILENAME (OPTIONAL)
        OUTPUTS: -
        """
        str_tree_structure = self.generate_tree()
        if filename:
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(str_tree_structure)
            print(f'Tree structure has been written to {filename}')
        else:
            return str_tree_structure
