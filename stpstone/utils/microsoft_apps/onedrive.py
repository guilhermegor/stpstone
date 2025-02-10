### HANDLING ONEDRIVE ISSUES ###

from getpass import getuser
from stpstone.utils.parsers.folders import DirFilesManagement


class OneDrive:

    @property
    def check_sync_status(self,
                          dir_path_business='C:\\Users\\{}\\AppData\\Local\\Microsoft\\OneDrive\\logs\\Business1\\',
                          name_like='SyncEngine-*', bl_to_datetime=True):
        """
        DOCSTRING: CHECK WHETHER SYNC SERVICE IN LOCAL MACHINE IS ALIVE
        INPUTS: -
        OUTPUTS: DATETIME
        """
        # definifing local dir_path_business to status file
        dir_path_business = dir_path_business.format(getuser())
        # catch the most recent edited file
        complete_status_file_path = DirFilesManagement().choose_last_saved_file_w_rule(
            dir_path_business, name_like)
        # return last edition datetime
        return DirFilesManagement().time_last_edition(complete_status_file_path,
                                                      bl_to_datetime=bl_to_datetime)
