### HANDLING TGZ FILES ###

import tarfile
import os
from stpstone.utils.parsers.folders import DirFilesManagement


class HandlingTGZFiles:

    def fetch_tgz_files(self, dir_exporting_path, tgz_exporting_name, url_source=None,
                        complete_source_path=None):
        """
        REFERENCES: "HANDS-ON MACHINE LEARNING WITH SCIKIT-LEARN, KERAS, AND TENSORFLOW,
            2ND EDITION, BY AURÉLIEN GÉRON (O’REILLY). COPYRIGHT 2019 KIWISOFT S.A.S.,
            978-1-492-03264-9.”
        DOCSTRING: EXTRACT FILES FROM TGZ COMPRESSION
        INPUTS: DIRECTORY EXPORTING PATH, TGZ FILE EXPORTING NAME, URL SOURCE (WHEN APPLICABLE),
            COMPLETE SOURCE PATH (IF THE FILE IS ALREADY IN THE DISK)
        OUTPUTS: DICTIONARY WITH BLAME DOWNLOAD TGZ FILE, DIR PATH AND EXTRACTED FILES NAMES
        """
        # checking wheter the directory to export the file is available or not
        if DirFilesManagement().object_exists(dir_exporting_path):
            DirFilesManagement().mk_new_directory(dir_exporting_path)
        # exporting complete path
        tgz_path = os.path.join(dir_exporting_path, tgz_exporting_name)
        # print(tgz_path)
        # requesting data when it is provided a valid url, whereas a complete source path have
        #   to be declared
        if all([x is None for x in [url_source, complete_source_path]]):
            raise Exception('Url source or complete path of source ought be passed in order to '
                            + 'data from the file.')
        elif url_source != None:
            blame_download_tgz_file = DirFilesManagement().download_web_file(
                url_source, tgz_path)
        else:
            blame_download_tgz_file = 'n/a'
        # extracting data from tgz file
        tgz_file = tarfile.open(tgz_path)
        tgz_members_names = tgz_file.getnames()
        tgz_file.extractall(path=dir_exporting_path)
        tgz_file.close()
        # return whether or not the file exists
        return {
            'blame_download_tgz_file': blame_download_tgz_file,
            'dir_path': dir_exporting_path,
            'extracted_files_names': tgz_members_names
        }
