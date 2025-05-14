### METHODS TO HANDLE STRINGS ###

import json
import re
import uuid
import ftfy
from base64 import b64encode
from fnmatch import fnmatch
from string import ascii_lowercase, ascii_uppercase, digits
from typing import Any, Dict, Union, List
from unicodedata import combining, normalize
from urllib.parse import urlparse, parse_qs
from basicauth import encode
from bs4 import BeautifulSoup
from unidecode import unidecode


class StrHandler:

    @property
    def multi_map_reference(self):
        """
        REFERENCES: “FLUENT PYTHON BY LUCIANO RAMALHO (O’REILLY). COPYRIGHT 2015 LUCIANO RAMALHO, 978-1-491-94600-8.”
        DOCSTRING: TRANSFORM SOME WESTERN TYPOGRAPHICAL SYMBOLS INTO ASCII, BUILDING MAPPING TABLE
            FOR CHAR-TO-CHAR REPLACEMENT
        INPUTS: -
        OUTPUT: DICT
        """
        single_map = str.maketrans("""‚ƒ„†ˆ‹‘’“”•–—˜›""', '""'f"*^<''""---~>""")
        multi_map = str.maketrans(
            {
                "€": "<euro>",
                "…": "...",
                "OE": "OE",
                "™": "(TM)",
                "oe": "oe",
                "‰": "<per mille>",
                "‡": "**",
            }
        )
        return multi_map.update(single_map)

    def get_between(self, s, first, last):
        """
        DOCSTRING: FIND STRINGS BETWEEN TWO SUBSTRINGS
        INPUTS: ORIGINAL STRING, INITAL AND FINAL DELIMITERS
        OUTPUTS: MID STRING
        """
        try:
            start = s.index(first) + len(first)
            end = s.index(last, start)
            return s[start:end]
        except ValueError:
            return ""
    
    def get_after(self, s, first):
        """
        DOCSTRING: FIND STRINGS BETWEEN TWO SUBSTRINGS
        INPUTS: ORIGINAL STRING, INITAL AND FINAL DELIMITERS
        OUTPUTS: MID STRING
        """
        try:
            start = s.index(first) + len(first)
            return s[start:]
        except ValueError:
            return ""

    def find_substr_str(self, str_, substr_):
        """
        DOCSTRING: FINDING A SUBSTRING IN A STRING
        INPUTS: STRING AND SUBSTRING
        OUTPUTS: BOOLEAN
        """
        return substr_ in str_

    def match_string_like(self, str_, str_like):
        """
        DOCSTRING: MATCHING STRING WITH RESPECTIVELY STRING LIKE
        INPUTS: STRING AND STRING LIKE
        OUTPUTS: BOOLEAN
        """
        return fnmatch(str_, str_like)

    def latin_characters(self, str_):
        """
        DOCSTRING: CORRECTING SPECIAL CHARACTERS
        INPUTS: STRING
        OUTPUTS: CORRECTED STRING
        """
        return str_.encode("latin1").decode("utf-8")

    def decode_special_characters_ftfy(self, str_):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return ftfy.fix_str_(str_)

    def removing_accents(self, str_):
        """
        DOCSTRING: REMOVE ACCENTS FROM LATIN ALPHABET
        INPUTS: STRING
        OUTPUTS: STRING
        """
        return unidecode(str_)

    def byte_to_latin_characters(self, str_):
        """
        DOCSTRING: CORRECTING SPECIAL CHARACTERS
        INPUTS: STRING
        OUTPUTS: CORRECTED STRING
        """
        return str_.encode("latin1").decode("ISO-8859-1")

    def remove_diacritics(self, str_):
        """
        REFERENCES: “FLUENT PYTHON BY LUCIANO RAMALHO (O’REILLY). COPYRIGHT 2015
        LUCIANO RAMALHO, 978-1-491-94600-8.”
        DOCSITRNGS: REMOVE ALL DIACRITICS FROM A STRING, SUCH AS ACCENTS, CEDILLAS, ETC, FROM LATIN
            AND NON-LATIN ALPHABET, LIKE GREEK.
        INPUTS: STRING
        OUTPUTS: STRING
        """
        norm_txt = normalize("NFD", str_)
        shaved = "".join(c for c in norm_txt if not combining(c))
        return normalize("NFC", shaved)

    def remove_diacritics_nfkd(self, str_: str, bl_lower_case: bool = True) -> str:
        if bl_lower_case == True:
            str_ = str_.lower()
        str_ = str_.replace("\n", "")
        return "".join(c for c in normalize("NFKD", str_) if not combining(c))

    def normalize_text(self, str_):
        return normalize("NFKD", str_).encode("ascii", "ignore").decode("utf-8")

    def remove_sup_period_marks(self, corpus, patterns=r"[!.?+]"):
        """
        DOCSTRING: REMOVE END PERIOD MARKS
        INPUTS: CORPUS AND PATTERNS (DEFAULT)
        OUTPUTS: STRING
        """
        return re.sub(patterns, "", corpus)

    def remove_only_latin_diacritics(self, str_, latin_base=False):
        """
        REFERENCES: “FLUENT PYTHON BY LUCIANO RAMALHO (O’REILLY). COPYRIGHT 2015
            LUCIANO RAMALHO, 978-1-491-94600-8.”
        DOCSTRING: REMOVE ALL DISCRITIC MARKS FROM LATIN BASE CHARACTERS
        INPUTS: STRING, LATIN BASE (FALSE AS DEFAULT)
        OUTPUTS: STRING
        """
        norm_txt = normalize("NFD", str_)
        keepers = []
        for c in norm_txt:
            if combining(c) and latin_base:
                continue  # ignore diacritic on Latin base char
            keepers.append(c)
            # if it isn't combining char, it's a new base char
            if not combining(c):
                latin_base = c in str_.ascii_letters
        shaved = "".join(keepers)
        return normalize("NFC", shaved)

    def dewinize(self, str_):
        """
        REFERENCES: “FLUENT PYTHON BY LUCIANO RAMALHO (O’REILLY). COPYRIGHT 2015 LUCIANO RAMALHO, 978-1-491-94600-8.”
        DOCSTRING: REPLACE WIN1252 SYMBOLS WITH ASCII CHARS OR SEQUENCES
        INPUTS: STRING
        OUTPUTS: STRING
        """
        return str_.translate(self.multi_map_reference)

    def asciize(self, str_):
        """
        REFERENCES: “FLUENT PYTHON BY LUCIANO RAMALHO (O’REILLY). COPYRIGHT 2015 LUCIANO RAMALHO, 978-1-491-94600-8.”
        DOCSTRING: APPLY NFKC NORMALIZATION TO COMPOSE CHARACTERS WITH THEIR COMPATIBILITY CODE
            POINTS IN ASCII SYSTEM
        INPUTS: STRING
        OUTPUTS: STRING
        """
        no_marks = self.remove_only_latin_diacritics(self.dewinize(str_))
        no_marks = no_marks.replace("ß", "ss")
        return normalize("NFKC", no_marks)

    def remove_substr(self, str_, substr_):
        """
        DOCSTRING: REMOVE A SUBSTRING FROM A GIVEN STRING
        INPUTS: STRING AND SUBSTRING
        OUTPUTS: STRING WITHOUT SUBSTRING
        """
        return str_.replace(substr_, "")

    def get_string_until_substr(self, str_, substring):
        """
        DOCSTRING: RETURN A STRING UNTIL FIND ITS SUBSTRING
        INPUTS: STRING, SUBSTRING
        OUTPUTS: STRING
        """
        return str_.split(substring)[0]

    def get_string_after_substr(self, str_, substring):
        """
        DOCSTRING: RETURN A STRING AFTER FIND ITS SUBSTRING
        INPUTS: STRING, SUBSTRING
        OUTPUTS: STRING
        """
        return str_.split(substring)[1]

    def base64_encode(self, userid, password):
        """
        DOCSTRING: ENCODING IN BASE 64 AN USER AND PASSWORD COMBINATION
        INPUTS: STRING TO ENCODE
        OUTPUTS: STRING ENCODED IN BASE64
        """
        return encode(userid, password)

    def base64_str_encode(self, str_, code_method="ascii"):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # encode to bytes
        message_bytes = str_.encode(code_method)
        # encode bytes to base64
        base64_bytes = b64encode(message_bytes)
        base64_message = base64_bytes.decode(code_method)
        # return message
        return base64_message

    @property
    def universally_unique_identifier(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # make a new uuid
        uuid_identifier = uuid.uuid4()
        # return uudi
        return {
            "uuid": uuid_identifier,
            "uuid_hex_digits_str": str(uuid_identifier),
            "uuid_32_character_hexadecimal_str": uuid_identifier.hex,
        }

    def letters_to_numbers(
        self,
        letters_in_alphabet=21,
        first_letter_alphabet="f",
        list_not_in_range=["i", "l", "o", "p", "r", "s", "t", "w", "y"],
    ):
        """
        DOCSTRING: JSON CORRELATING LETTERS AND NUMBERS
        INPUTS: LETTERS IN ALPHABET FROM THE FIRST ONE (21 AS DEFAULT),
            FIRST LETTER IN ALPHABET (F AS DEFAULT), LIST NOT IN RANGE (I, L, O, P, R, S, T, W, Y
            AS DEFAULT)
        OUTPUTS: JSON WITH LETTERS IN LOWER CASE AS KEYS
        """
        # auxiliary variables
        dict_message = dict()
        i_aux = 0

        # dictionary correlating letters and numbers
        for i in range(
            ord(first_letter_alphabet), ord(first_letter_alphabet) + letters_in_alphabet
        ):
            if chr(i) not in list_not_in_range:
                dict_message[chr(i)] = i - 101 - i_aux
            else:
                i_aux += 1

        # json to export
        return json.loads(json.dumps(dict_message))

    def alphabetic_range(self, case="upper"):
        """
        DOCSTRING: ALPHABETIC RANGE IN UPPER OR LOWER CASE
        INPUTS: CASE
        OUTPUTS: LIST
        """
        if case == "upper":
            return list(ascii_uppercase)
        elif case == "lower":
            return list(ascii_lowercase)
        else:
            raise Exception(
                "Case ought be upper or lower, although {} was given, ".format(case)
                + "please revisit the case variable"
            )

    def regex_match_alphanumeric(self, str_, regex_match="^[a-zA-Z0-9_]+$"):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return re.match(regex_match, str_)

    def bl_has_numbers(self, str_):
        """
        DOCSTRING: CHECK WHETER THE STRING HAS NUMBERS
        INPUTS:
        OUTPUTS: BOOLEAN
        """
        return bool(re.search(r"\d", str_))

    def nfc_equal(self, str1, str2):
        """
        REFERENCES: “FLUENT PYTHON BY LUCIANO RAMALHO (O’REILLY). COPYRIGHT 2015 LUCIANO RAMALHO, 978-1-491-94600-8.”
        DOCSTRING: UNICODE EQUIVALENCE TO IDENTIFY ENCODING STARDARDS THAT REPRESENT ESSENTIALLY
            THE SAME CHARACTER
        INPUTS: STRING 1 AND 2
        OUTPUTS: BOOLEAN
        """
        return normalize("NFC", str1) == normalize("NFC", str2)

    def casefold_equal(self, str1, str2):
        """
        REFERENCES: “FLUENT PYTHON BY LUCIANO RAMALHO (O’REILLY). COPYRIGHT 2015 LUCIANO RAMALHO, 978-1-491-94600-8.”
        DOCSTRING: UNICODE EQUIVALENCE TO IDENTIFY ENCODING STARDARDS THAT REPRESENT ESSENTIALLY
            THE SAME CASEFOLD FOR A GIVEN CHARACTER
        INPUTS: STRING 1 AND 2
        OUTPUTS: BOOLEAN
        """
        return normalize("NFC", str1).casefold() == normalize("NFC", str2).casefold()

    def remove_non_alphanumeric_chars(
        self, str_, str_pattern_maintain=r'[\W_]', str_replace=''
    ):
        """
        Remove non-alphanumeric characters from a string.
        Args:
            str_ (str): The input string.
            str_pattern_maintain (str): Regex pattern to match non-alphanumeric characters (default: r'[\W_]').
            str_replace (str): The string to replace matched characters with (default: '').
        Returns:
            str: The string with non-alphanumeric characters removed.
        """
        return re.sub(str_pattern_maintain, str_replace, str_)

    def remove_numeric_chars(self, str_):
        """
        REFERENCES: https://stackoverflow.com/questions/12851791/removing-numbers-from-str_
        DOCSTRING: REMOVE NUMERIC CHARACTERS
        INPUTS: STRING
        OUTPUTS: STRING
        """
        def_remove_digits = str.maketrans("", "", digits)
        return str_.translate(def_remove_digits)

    def is_capitalized(self, str_, bl_simple_validation=True):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS
        """
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
                if (str_[0].isupper() == True) and (
                    all([l.islower() for l in str_[1:]])
                ):
                    return True
                else:
                    return False
        except:
            return False

    def split_re(self, str_, re_split=r"[;,\s]\s*"):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return re.split(re_split, str_)

    def replace_case_insensitive(self, str_, str_replaced, str_replace):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return re.sub(str_replaced, str_replace, str_, flags=re.IGNORECASE)

    def matchcase(self, str_):
        """
        REFERENCES: PYTHON COOKBOOK - DAVID BEASZLEY, BRIAN K. JONES
        DOCSTRING: MATCHASE SENSE
        INPUTS: WORD
        OUTPUTS: STRING
        """
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
        """
        REFERENCES: PYTHON COOKBOOK - DAVID BEASZLEY, BRIAN K. JONES
        DOCSTRING: MATCHASE SENSE
        INPUTS: WORD
        OUTPUTS: STRING
        """
        return re.sub(
            str_replaced, self.matchcase(str_replace), str_, flags=re.IGNORECASE
        )

    def replace_all(self, str_, dict_replacers):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        for i, j in dict_replacers.items():
            str_ = str_.replace(i, j)
        return str_

    def html_to_txt(self, html_):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        soup = BeautifulSoup(html_, features="lxml")
        return soup(html_)

    def extract_urls(self, str_):
        """
        DOCSTRING: LIST OF URLS IN A GIVEN STRING
        INPUTS: STRING
        OUTPUTS: LIST
        """
        # define a regular expression pattern to match URLs
        url_pattern = re.compile(r"https?://\S+|www\.\S+")
        # find all matches in the given str_
        list_urls = re.findall(url_pattern, str_)
        # return urls list
        return list_urls

    def is_word(self, _value):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS
        """
        try:
            float(_value)
            return False
        except ValueError:
            return True

    def convert_case(self, str_: str, from_case: str, to_case: str) -> str:
        """
        Converts a string between different naming conventions:
            - camelCase - 'camel'
            - PascalCase - 'pascal'
            - kebab-case - 'kebab'
            - UPPER_CONSTANT - 'upper_constant'
            - lower_constant - 'lower_constant'
            - UpperFirst - 'upper_first'
            - Default (words separated by spaces, hyphens or underscores) - 'default'

        Args:
            from_case (str): Current case of the string
            to_case (str): Desired case of the string

        Returns:
            str: Transformed string
        """
        # from case
        if from_case == "camel":
            words = re.sub(r"([a-z])([A-Z])", r"\1_\2", str_)
            words = re.sub(r"([a-zA-Z])(\d)", r"\1_\2", words)
            words = re.sub(r"(\d)([a-zA-Z])", r"\1_\2", words)
            words = words.lower().split("_")
        elif from_case == "pascal":
            words = re.sub(r"([a-z])([A-Z])", r"\1_\2", str_).lower().split("_")
        elif from_case == "kebab":
            words = str_.lower().split("-")
        elif from_case == "upper_constant" or from_case == "lower_constant":
            if "-" in str_:
                words = str_.lower().split("-")
            else:
                words = str_.lower().split("_")
        elif from_case == "upper_first":
            words = [str_[0].upper() + str_[1:].lower()]
        elif from_case == "default":
            str_ = str_.replace(" - ", " ")
            str_ = str_.replace("-", " ")
            str_ = str_.replace("_", " ")
            str_ = str_.replace("+", " ")
            str_ = str_.replace(" (", " ")
            str_ = str_.replace(") ", " ")
            str_ = str_.replace(r"\n", " ")
            words = str_.lower().split()
        else:
            raise ValueError(
                "Invalid from_case. Choose from ['camel', 'pascal', 'snake', 'kebab', 'upper_constant', 'lower_constant', 'upper_first']"
            )
        # converting to case
        if to_case == "camel":
            return words[0] + "".join(word.capitalize() for word in words[1:])
        elif to_case == "pascal":
            return "".join(word.capitalize() for word in words)
        elif to_case == "snake":
            return "_".join(words).lower()
        elif to_case == "kebab":
            return "-".join(words).lower()
        elif to_case == "upper_constant":
            return "_".join(words).upper()
        elif to_case == "lower_constant":
            return "_".join(words).lower()
        elif to_case == "upper_first":
            return words[0].capitalize()
        else:
            raise ValueError(
                "Invalid to_case. Choose from ['camel', 'pascal', 'snake', 'kebab', 'upper_constant', 'lower_constant', 'upper_first']"
            )

    def extract_info_between_braces(
        self, str_: str, str_pattern: str = r"\{\{(.*?)\}\}"
    ) -> str:
        return re.findall(str_pattern, str_)

    def fill_placeholders(self, str_: str, dict_placeholders: Dict[str, Any]) -> str:
        """
        Fill fstr named placeholders

        Args:
            str_ (str): fstr
            dict_placeholders (Dict[str, Any]): named placeholders

        Returns:
            str
        """
        list_placeholders = self.extract_info_between_braces(str_)
        for placeholder in list_placeholders:
            if placeholder in dict_placeholders:
                str_ = str_.replace(
                    f"{{{{{placeholder}}}}}", str(dict_placeholders[placeholder])
                )
            else:
                str_ = str_.replace(f"{{{{{placeholder}}}}}", f"{{{{{placeholder}}}}}")
        return str_

    def fill_zeros(self, str_prefix: str, int_num: int, total_length: int = 11) -> str:
        str_num = str(int_num)
        required_zeros = total_length - len(str_prefix) - len(str_num)
        if required_zeros < 0:
            raise ValueError("Total length is too small for the given inputs.")
        return f"{str_prefix}{'0' * required_zeros}{str_num}"

    def get_url_query(self, url: str, bl_include_fragment: bool = False) \
        -> Dict[str, Union[str, List[str]]]:
        """
        Extracts parameters from a URL's query string or fragment.

        Args:
            url (str): The URL to parse.
            bl_include_fragment (bool): Whether to include parameters from the fragment (after #).
            Defaults to False.

        Returns:
            Dict[str, Union[str, List[str]]]: A dictionary of parameters. Single-value parameters
            are returned as strings, while multi-value parameters are returned as lists of strings.
        """
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        if bl_include_fragment == True:
            fragment_params = parse_qs(parsed_url.fragment)
            query_params.update(fragment_params)
        return {
            key: value[0] if len(value) == 1 else value
            for key, value in query_params.items()
        }

    def has_no_letters(self, str_: str) -> bool:
        """Check if a string has no letters (A-Z, a-z)."""
        return not any(char.isalpha() for char in str_)
