### HANDLING TXT FILES ###

from re import sub
from typing import List, Optional
from stpstone.utils.parsers.folders import DirFilesManagement


class HandlingTXTFiles:

    def read_file(self, complete_path, method='r'):
        """
        DOCSTRING: READ A TXT FILE
        INPUTS: COMPLETE PATH AND METHOD (R, FROM READ, AS DEFAULT)
        OUTPUTS: CONTENT
        """
        with open(complete_path, method) as f:
            return f.read()

    def generator_file(self, complete_path, method='r', regex='[^A-Za-z0-9-.*-:-<-=-\s]+',
                       non_matching_regex_characaters='|'):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        with open(complete_path, method, encoding="ascii", errors='replace') as f:
            return [sub(regex, non_matching_regex_characaters, line) for line in
                    f.read().splitlines()]

    def read_first_line(self, complete_path, method='r'):
        """
        DOCSTRING: READ THE FIRST LINE OF A TXT FILE
        INPUTS: COMPLETE PATH AND METHOD (R, FROM READ, AS DEFAULT)
        OUTPUTS: CONTENT
        """
        with open(complete_path, method) as f:
            return f.readline().rstrip()

    def remove_first_n_lines(self, complete_path, n=1):
        """
        DOCSTRING: REMOVE THE FIRST N LINES OF A TXT FILE
        INPUTS: COMPLETE PATH AND N FROM NUMBER OF LINES (1 AS DEFAULT)
        OUTPUTS: ORIGINAL CONTENT WITHOUT THE FIRST N LINES
        """
        with open(complete_path, 'r') as file_in:
            data = file_in.read().splitlines(True)
        with open(complete_path, 'w') as file_out:
            file_out.writelines(data[n:])

    def write_file(self, file_complete_path, data_content, method='w'):
        """
        DOCSTRING: WRITE A TXT FILE
        INPUTS: FILE COMPLETE PATH, DATA CONTENT AND METHOD (DEFAULT AS W THAT STANDS FOR WRITE)
        OUTPUTS: STATUS ACCOMPLISHMENT
        """
        with open(file_complete_path, method) as file_output:
            file_output.write(data_content)
        return DirFilesManagement().object_exists(file_complete_path)

    def check_separator_consistency(
            self, file_path: str, list_sep: Optional[List[str]] = [',', ';', '\t']) -> bool:
        with open(file_path, 'r') as file:
            list_lines = file.readlines()
        for sep in list_sep:
            field_counts = [len(line.split(sep)) for line in list_lines]
            list_unique_field_counts = set(field_counts)
            if len(list_unique_field_counts) == 1:
                print(f"The file has a consistent separator: '{sep}'")
                return True
        print("The file does not have a consistent separator.")
        return False
