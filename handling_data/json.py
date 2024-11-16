### HANDLING JSON FILES ###

import os
import ast
import json
from stpstone.pool_conn.postgre import PostgreSQL
from stpstone.handling_data.str import StrHandler


class JsonFiles:

    def dump_message(self, message, json_file):
        '''
        DOCSTRING: DUMP MESSAGE AND SAVE TO SPECIFIC DIRECTORY IN THE NETWORK
        INPUTS: MESSAGE (STR IN DICT FORMAT), JSON FILE AS A COMPLETE FILE .JSON NAME
        OUTPUTS: WHETER FILE SAVING WAS SUCCESFUL OR NOT (OK, NOK)
        '''
        with open(json_file, "w") as write_file:
            json.dump(message, write_file)
        if not os.path.exists(json_file):
            return 'NOK'
        else:
            return 'OK'

    def load_message(self, json_file, errors='ignore', encoding=None, decoding=None):
        '''
        DOCSTRING: LOAD MESSAGE FROM JSON
        INPUTS: JSON FILE
        OUTPUTS: DATA IN MEMORY
        '''
        if encoding == None:
            with open(json_file, "r", errors=errors) as read_file:
                return json.load(read_file)
        else:
            with open(json_file, errors=errors, encoding=encoding) as read_file:
                return json.loads(read_file.read().encode(encoding).decode(decoding))

    def loads_message_like(self, message):
        '''
        DOCSTRING: LOAD A REPRESENTATION IN STR FROM A VALID MESSAGE TYPE, LIKE LIST, BYTE OR DICT
        INPUTS: MESSAGE
        OUTPUTS: JSON
        '''
        return json.loads(message)

    def dict_to_json(self, message):
        '''
        DOCSTRING: LOAD MESSAGE INTO JSON FORMAT
        INPUTS: MESSAGE
        OUTPUTS: JSON
        '''
        return json.dumps(message)

    def send_json(self, message):
        '''
        DOCSTRING: SEND JSON IN MEMORY, FOR INSTANCE IN AN API RESPONSE
        INPUTS: MESSAGE
        OUTPUTS: JSON IN MEMORY
        '''
        return json.loads(json.dumps(message))

    def postgre_to_json(self, dbname, sql_query):
        '''
        DOCSTRING: RETURN A JSON FROM A POSTGRE SQL QUERY
        INPUTS: DBNAME, SQL QUERY
        OUTPUTS: JSON
        '''
        return json.dumps(PostgreSQL().engine(dbname, sql_query))

    def byte_to_json(self, byte_message):
        '''
        DOCSTRING: DECRIPT BYTE FORMAT TO JSON
        INPUTS: BYTE MESSAGE
        OUTPUTS: JSON MESSAGE
        '''
        jsonify_message = ast.literal_eval(StrHandler().find_between(
            str(byte_message), "b'", "'"))
        return JsonFiles().send_json(jsonify_message)
