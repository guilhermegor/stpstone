### HANDLING PICKLE OBJECTS ###

import pickle
import os


class PickleFiles:

    def dump_message(self, message, pickle_file, pickle_protocol=pickle.HIGHEST_PROTOCOL,
                     pickle_protocol_error=-1):
        """
        REFERENCES: https://stackoverflow.com/questions/11218477/how-can-i-use-pickle-to-save-a-dict-or-any-other-python-object
        DOCSTRING: DUMP MESSAGE AND SAVE TO SPECIFIC DIRECTORY IN THE NETWORK
        INPUTS: MESSAGE (STR IN DICT FORMAT), PICKLE FILE AS A COMPLETE FILE .PICKLE NAME
        OUTPUTS: WHETER FILE SAVING WAS SUCCESFUL OR NOT (OK, NOK)
        """
        # store data
        with open(pickle_file, 'wb') as write_file:
            try:
                pickle.dump(message, write_file, protocol=pickle_protocol)
            except:
                pickle.dump(message, write_file,
                            protocol=pickle_protocol_error)
        # return status of accomplishment
        if not os.path.exists(pickle_file):
            return 'NOK'
        else:
            return 'OK'

    def load_message(self, pickle_file, encoding=None, decoding=None):
        """
        REFERENCES: https://stackoverflow.com/questions/11218477/how-can-i-use-pickle-to-save-a-dict-or-any-other-python-object
        DOCSTRING: LOAD MESSAGE FROM PICKLE
        INPUTS: PICKLE FILE
        OUTPUTS: DATA IN MEMORY
        """
        if encoding is None:
            with open(pickle_file, 'rb') as read_file:
                return pickle.load(read_file)
        else:
            with open(pickle_file, encoding=encoding) as read_file:
                return pickle.loads(read_file.read().encode(encoding).decode(decoding))
