### METHODS TO HANDLE STRINGS ###

import json
import uuid
import re
import ftfy
from base64 import b64encode
from basicauth import encode
from unidecode import unidecode
from unicodedata import normalize, combining
from string import ascii_uppercase, ascii_lowercase, digits
from fnmatch import fnmatch
from bs4 import BeautifulSoup


class StrHandler:

    @property
    def multi_map_reference(self):
        '''
        REFERENCES: “FLUENT PYTHON BY LUCIANO RAMALHO (O’REILLY). COPYRIGHT 2015 LUCIANO RAMALHO, 978-1-491-94600-8.”
        DOCSTRING: TRANSFORM SOME WESTERN TYPOGRAPHICAL SYMBOLS INTO ASCII, BUILDING MAPPING TABLE 
            FOR CHAR-TO-CHAR REPLACEMENT
        INPUTS: -
        OUTPUT: DICT
        '''
        single_map = str.maketrans(
            """‚ƒ„†ˆ‹‘’“”•–—˜›""", """'f"*^<''""---~>""")
        multi_map = str.maketrans({
            '€': '<euro>',
            '…': '...',
            'OE': 'OE',
            '™': '(TM)',
            'oe': 'oe',
            '‰': '<per mille>',
            '‡': '**',
        })
        return multi_map.update(single_map)

    def find_between(self, s, first, last):
        '''
        DOCSTRING: FIND STRINGS BETWEEN TWO SUBSTRINGS
        INPUTS: ORIGINAL STRING, INITAL AND FINAL DELIMITERS
        OUTPUTS: MID STRING
        '''
        try:
            start = s.index(first) + len(first)
            end = s.index(last, start)
            return s[start:end]
        except ValueError:
            return ""

    def find_substr_str(self, str_, substr_):
        '''
        DOCSTRING: FINDING A SUBSTRING IN A STRING
        INPUTS: STRING AND SUBSTRING
        OUTPUTS: BOOLEAN
        '''
        return substr_ in str_

    def match_string_like(self, str_, str_like):
        '''
        DOCSTRING: MATCHING STRING WITH RESPECTIVELY STRING LIKE
        INPUTS: STRING AND STRING LIKE
        OUTPUTS: BOOLEAN
        '''
        return fnmatch(str_, str_like)

    def latin_characters(self, str_):
        '''
        DOCSTRING: CORRECTING SPECIAL CHARACTERS
        INPUTS: STRING
        OUTPUTS: CORRECTED STRING
        '''
        return str_.encode('latin1').decode('utf-8')

    def decode_special_characters_ftfy(self, str_):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        return ftfy.fix_str_(str_)

    def removing_accents(self, str_):
        '''
        DOCSTRING: REMOVE ACCENTS FROM LATIN ALPHABET
        INPUTS: STRING
        OUTPUTS: STRING
        '''
        return unidecode(str_)

    def byte_to_latin_characters(self, str_):
        '''
        DOCSTRING: CORRECTING SPECIAL CHARACTERS
        INPUTS: STRING
        OUTPUTS: CORRECTED STRING        
        '''
        return str_.encode('latin1').decode('ISO-8859-1')

    def remove_diacritics(self, str_):
        '''
        REFERENCES: “FLUENT PYTHON BY LUCIANO RAMALHO (O’REILLY). COPYRIGHT 2015 
        LUCIANO RAMALHO, 978-1-491-94600-8.”
        DOCSITRNGS: REMOVE ALL DIACRITICS FROM A STRING, SUCH AS ACCENTS, CEDILLAS, ETC, FROM LATIN 
            AND NON-LATIN ALPHABET, LIKE GREEK.
        INPUTS: STRING
        OUTPUTS: STRING
        '''
        norm_txt = normalize('NFD', str_)
        shaved = ''.join(c for c in norm_txt if not combining(c))
        return normalize('NFC', shaved)

    def remove_end_period_marks(self, corpus, patterns='[!.?+]'):
        '''
        DOCSTRING: REMOVE END PERIOD MARKS
        INPUTS: CORPUS AND PATTERNS (DEFAULT)
        OUTPUTS: STRING
        '''
        return re.sub(patterns, '', corpus)

    def remove_only_latin_diacritics(self, str_, latin_base=False):
        '''
        REFERENCES: “FLUENT PYTHON BY LUCIANO RAMALHO (O’REILLY). COPYRIGHT 2015 
            LUCIANO RAMALHO, 978-1-491-94600-8.”        
        DOCSTRING: REMOVE ALL DISCRITIC MARKS FROM LATIN BASE CHARACTERS
        INPUTS: STRING, LATIN BASE (FALSE AS DEFAULT)
        OUTPUTS: STRING
        '''
        norm_txt = normalize('NFD', str_)
        keepers = []
        for c in norm_txt:
            if combining(c) and latin_base:
                continue  # ignore diacritic on Latin base char
            keepers.append(c)
            # if it isn't combining char, it's a new base char
            if not combining(c):
                latin_base = c in str_.ascii_letters
        shaved = ''.join(keepers)
        return normalize('NFC', shaved)

    def dewinize(self, str_):
        '''
        REFERENCES: “FLUENT PYTHON BY LUCIANO RAMALHO (O’REILLY). COPYRIGHT 2015 LUCIANO RAMALHO, 978-1-491-94600-8.”        
        DOCSTRING: REPLACE WIN1252 SYMBOLS WITH ASCII CHARS OR SEQUENCES
        INPUTS: STRING
        OUTPUTS: STRING
        '''
        return str_.translate(self.multi_map_reference)

    def asciize(self, str_):
        '''
        REFERENCES: “FLUENT PYTHON BY LUCIANO RAMALHO (O’REILLY). COPYRIGHT 2015 LUCIANO RAMALHO, 978-1-491-94600-8.”        
        DOCSTRING: APPLY NFKC NORMALIZATION TO COMPOSE CHARACTERS WITH THEIR COMPATIBILITY CODE 
            POINTS IN ASCII SYSTEM
        INPUTS: STRING
        OUTPUTS: STRING
        '''
        no_marks = self.remove_only_latin_diacritics(self.dewinize(str_))
        no_marks = no_marks.replace('ß', 'ss')
        return normalize('NFKC', no_marks)

    def remove_substr(self, str_, substr_):
        '''
        DOCSTRING: REMOVE A SUBSTRING FROM A GIVEN STRING
        INPUTS: STRING AND SUBSTRING
        OUTPUTS: STRING WITHOUT SUBSTRING
        '''
        return str_.replace(substr_, '')

    def get_string_until_substr(self, str_, substring):
        '''
        DOCSTRING: RETURN A STRING UNTIL FIND ITS SUBSTRING
        INPUTS: STRING, SUBSTRING
        OUTPUTS: STRING
        '''
        return str_.split(substring)[0]

    def get_string_after_substr(self, str_, substring):
        '''
        DOCSTRING: RETURN A STRING AFTER FIND ITS SUBSTRING
        INPUTS: STRING, SUBSTRING
        OUTPUTS: STRING
        '''
        return str_.split(substring)[1]

    def base64_encode(self, userid, password):
        '''
        DOCSTRING: ENCODING IN BASE 64 AN USER AND PASSWORD COMBINATION
        INPUTS: STRING TO ENCODE
        OUTPUTS: STRING ENCODED IN BASE64
        '''
        return encode(userid, password)

    def base64_str_encode(self, str_, code_method='ascii'):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # encode to bytes
        message_bytes = str_.encode(code_method)
        # encode bytes to base64
        base64_bytes = b64encode(message_bytes)
        base64_message = base64_bytes.decode(code_method)
        # return message
        return base64_message

    @property
    def universally_unique_identifier(self):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # make a new uuid
        uuid_identifier = uuid.uuid4()
        # return uudi
        return {
            'uuid': uuid_identifier,
            'uuid_hex_digits_str': str(uuid_identifier),
            'uuid_32_character_hexadecimal_str': uuid_identifier.hex
        }

    def letters_to_numbers(self, letters_in_alphabet=21, first_letter_alphabet='f',
                           list_not_in_range=['i', 'l', 'o', 'p', 'r', 's', 't', 'w', 'y']):
        '''
        DOCSTRING: JSON CORRELATING LETTERS AND NUMBERS
        INPUTS: LETTERS IN ALPHABET FROM THE FIRST ONE (21 AS DEFAULT), 
            FIRST LETTER IN ALPHABET (F AS DEFAULT), LIST NOT IN RANGE (I, L, O, P, R, S, T, W, Y 
            AS DEFAULT)
        OUTPUTS: JSON WITH LETTERS IN LOWER CASE AS KEYS
        '''
        # auxiliary variables
        dict_message = dict()
        i_aux = 0

        # dictionary correlating letters and numbers
        for i in range(ord(first_letter_alphabet), ord(first_letter_alphabet) + letters_in_alphabet):
            if chr(i) not in list_not_in_range:
                dict_message[chr(i)] = i - 101 - i_aux
            else:
                i_aux += 1

        # json to export
        return json.loads(json.dumps(dict_message))

    def alphabetic_range(self, case='upper'):
        '''
        DOCSTRING: ALPHABETIC RANGE IN UPPER OR LOWER CASE
        INPUTS: CASE
        OUTPUTS: LIST
        '''
        if case == 'upper':
            return list(ascii_uppercase)
        elif case == 'lower':
            return list(ascii_lowercase)
        else:
            raise Exception('Case ought be upper or lower, although {} was given, '.format(case)
                            + 'please revisit the case variable')

    def regex_match_alphanumeric(self, str_, regex_match='^[a-zA-Z0-9_]+$'):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        return re.match(regex_match, str_)

    def bl_has_numbers(self, str_):
        '''
        DOCSTRING: CHECK WHETER THE STRING HAS NUMBERS
        INPUTS:
        OUTPUTS: BOOLEAN
        '''
        return bool(re.search(r'\d', str_))

    def nfc_equal(self, str1, str2):
        '''
        REFERENCES: “FLUENT PYTHON BY LUCIANO RAMALHO (O’REILLY). COPYRIGHT 2015 LUCIANO RAMALHO, 978-1-491-94600-8.”
        DOCSTRING: UNICODE EQUIVALENCE TO IDENTIFY ENCODING STARDARDS THAT REPRESENT ESSENTIALLY 
            THE SAME CHARACTER
        INPUTS: STRING 1 AND 2
        OUTPUTS: BOOLEAN
        '''
        return normalize('NFC', str1) == normalize('NFC', str2)

    def casefold_equal(self, str1, str2):
        '''
        REFERENCES: “FLUENT PYTHON BY LUCIANO RAMALHO (O’REILLY). COPYRIGHT 2015 LUCIANO RAMALHO, 978-1-491-94600-8.”
        DOCSTRING: UNICODE EQUIVALENCE TO IDENTIFY ENCODING STARDARDS THAT REPRESENT ESSENTIALLY 
            THE SAME CASEFOLD FOR A GIVEN CHARACTER
        INPUTS: STRING 1 AND 2
        OUTPUTS: BOOLEAN
        '''
        return normalize('NFC', str1).casefold() == normalize('NFC', str2).casefold()

    def remove_non_alphanumeric_chars(self, str_, str_pattern_maintain='[\W_]',
                                      str_replace=''):
        '''
        DOCSTRING: REMOVE NON-ALPHANUMERIC CHARACTERS
        INPUTS: STRING TO BE REPLACED, STRING PATTERN ('[\W_]'  == [^a-zA-Z0-9_], AS DEFAULT), 
            AND STR REPLACE
        OUTPUTS: STRING
        '''
        return re.sub(str_pattern_maintain, str_replace, str_)

    def remove_numeric_chars(self, str_):
        '''
        REFERENCES: https://stackoverflow.com/questions/12851791/removing-numbers-from-str_
        DOCSTRING: REMOVE NUMERIC CHARACTERS
        INPUTS: STRING
        OUTPUTS: STRING
        '''
        def_remove_digits = str.maketrans('', '', digits)
        return str_.translate(def_remove_digits)

    def is_capitalized(self, str_, bl_simple_validation=True):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS
        '''
        # removing diacritcs
        str_ = self.remove_diacritics(str_)
        # removing non-alfanumeric characters
        str_ = self.remove_non_alphanumeric_chars(str_)
        #   returning wheter is capitalized or not
        try:
            if bl_simple_validation == True:
                if (str_[0].isupper() == True) and (str_[1].islower() == True):
                    return True
                else:
                    return False
            else:
                if (str_[0].isupper() == True) and (all([l.islower() for l in str_[1:]])):
                    return True
                else:
                    return False
        except:
            return False

    def split_re(self, str_, re_split=r'[;,\s]\s*'):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        return re.split(re_split, str_)

    def replace_case_insensitive(self, str_, str_replaced, str_replace):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        return re.sub(str_replaced, str_replace, str_, flags=re.IGNORECASE)

    def matchcase(str_):
        '''
        REFERENCES: PYTHON COOKBOOK - DAVID BEASZLEY, BRIAN K. JONES
        DOCSTRING: MATCHASE SENSE
        INPUTS: WORD
        OUTPUTS: STRING
        '''
        def replace(m):
            str_ = m.group()
            if str_.isupper():
                return str_.upper()
            elif str_.islower():
                return str_.lower()
            elif str_[0].isupper():
                return str_.capitalize()
            else:
                return str_
        return replace

    def replace_respecting_case(self, str_, str_replaced, str_replace):
        '''
        REFERENCES: PYTHON COOKBOOK - DAVID BEASZLEY, BRIAN K. JONES
        DOCSTRING: MATCHASE SENSE
        INPUTS: WORD
        OUTPUTS: STRING
        '''
        return re.sub(str_replaced, self.matchcase(str_replace), str_, flags=re.IGNORECASE)

    def replace_all(self, str_, dict_replacers):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        for i, j in dict_replacers.items():
            str_ = str_.replace(i, j)
        return str_

    def html_to_txt(self, html_):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        soup = BeautifulSoup(html_, features='lxml')
        return soup(html_)

    def extract_urls(self, str_):
        '''
        DOCSTRING: LIST OF URLS IN A GIVEN STRING
        INPUTS: STRING
        OUTPUTS: LIST
        '''
        # define a regular expression pattern to match URLs
        url_pattern = re.compile(r'https?://\S+|www\.\S+')
        # find all matches in the given str_
        list_urls = re.findall(url_pattern, str_)
        # return urls list
        return list_urls

    def is_word(self, _value):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS
        '''
        try:
            float(_value)
            return False
        except ValueError:
            return True
        
    def camel_to_snake(self, str_cammel):
        '''
        Cammel case to snake case
        Arguments:
            str_ (str): string to be converted
        Outputs: str
        '''
        str_snake = re.sub(r'(?<!^)(?=[A-Z])', '_', str_cammel).upper()
        return str_snake