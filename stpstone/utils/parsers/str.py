"""Module containing the StrHandler class for comprehensive string manipulation.

This module provides a wide range of string processing utilities including:
- Character encoding/decoding
- Case conversion and validation
- Pattern matching and substitution
- URL and HTML processing
- Unicode normalization and diacritic removal
- String formatting and placeholder handling
"""

import base64
from fnmatch import fnmatch
import re
from string import ascii_letters, ascii_lowercase, ascii_uppercase, digits
from typing import Literal, Optional, TypedDict, TypeVar, Union
from unicodedata import combining, normalize
from urllib.parse import parse_qs, urlparse
import uuid

from basicauth import encode
from bs4 import BeautifulSoup
import ftfy
from unidecode import unidecode

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker, type_checker


TypeCaseFrom = TypeVar(
    "TypeCaseFrom", 
    bound=Literal["camel", "pascal", "kebab", "upper_constant", "lower_constant", "snake", 
                  "upper_first", "default"]
)

TypeCaseTo = TypeVar(
    "TypeCaseTo", 
    bound=Literal["camel", "pascal", "snake", "kebab", "upper_constant", "lower_constant", 
                  "upper_first"]
)

class ReturnUUID(TypedDict):
    """TypedDict for returning UUID information."""

    uuid: uuid.UUID
    uuid_hex_digits_str: str
    uuid_32_character_hexadecimal_str: str


class StrHandler(metaclass=TypeChecker):
    """A comprehensive string manipulation utility class.

    This class provides methods for handling various string operations including:
    - Encoding/decoding
    - Case conversion
    - Pattern matching
    - Unicode normalization
    - URL/HTML processing
    - And many other common string manipulations
    """

    def multi_map_reference(self) -> dict[str, str]:
        """Create mapping tables for character replacement.

        Builds mapping tables for transforming Western typographical symbols into ASCII,
        combining single character and multi-character replacements.

        Returns
        -------
        dict[str, str]
            Combined mapping table for character replacements.
        """
        single_map = {
            ord("‚"): '"',
            ord("ƒ"): "f", 
            ord("„"): '"',
            ord("†"): "*",
            ord("ˆ"): "^",
            ord("‹"): "<",
            ord("‘"): "'",
            ord("’"): "'",
            ord("“"): '"',
            ord("”"): '"',
            ord("•"): "-",
            ord("–"): "-",
            ord("—"): "-",
            ord("˜"): "~",
            ord("›"): ">"
        }
        multi_map = {
            ord("€"): "<euro>",
            ord("…"): "...",
            ord("™"): "(TM)",
            ord("‰"): "<per mille>",
            ord("‡"): "**"
        }
        multi_map.update(single_map)
        return multi_map

    def get_between(self, s: str, first: str, last: str) -> str:
        """Extract substring between two delimiters.

        Parameters
        ----------
        s : str
            The input string to search
        first : str
            The starting delimiter
        last : str
            The ending delimiter

        Returns
        -------
        str
            The substring between the delimiters, or empty string if not found
        """
        try:
            start = s.index(first) + len(first)
            end = s.index(last, start)
            return s[start:end]
        except ValueError:
            return ""

    def get_after(self, s: str, first: str) -> str:
        """Extract substring after a delimiter.

        Parameters
        ----------
        s : str
            The input string to search
        first : str
            The delimiter to find

        Returns
        -------
        str
            The substring after the delimiter, or empty string if not found
        """
        try:
            start = s.index(first) + len(first)
            return s[start:]
        except ValueError:
            return ""

    def find_substr_str(self, str_: str, substr_: str) -> bool:
        """Check if a substring exists in a string.

        Parameters
        ----------
        str_ : str
            The string to search in
        substr_ : str
            The substring to search for

        Returns
        -------
        bool
            True if substring is found, False otherwise
        """
        return substr_ in str_

    def match_string_like(self, str_: str, str_like: str) -> bool:
        """Match a string against a pattern with wildcards.

        Parameters
        ----------
        str_ : str
            The string to match
        str_like : str
            The pattern to match against (supports wildcards)

        Returns
        -------
        bool
            True if the string matches the pattern
        """
        return fnmatch(str_, str_like)

    def latin_characters(self, str_: str) -> str:
        """Convert string from latin1 to utf-8 encoding.

        Parameters
        ----------
        str_ : str
            The string to convert

        Returns
        -------
        str
            The converted string
        """
        try:
            return str_.encode("latin1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return str_

    def decode_special_characters_ftfy(self, str_: str) -> str:
        """Fix mojibake and other encoding issues using ftfy.

        Parameters
        ----------
        str_ : str
            The string with encoding issues

        Returns
        -------
        str
            The corrected string
        """
        return ftfy.fix_text(str_)

    def remove_accents(self, str_: str) -> str:
        """Remove accents from Latin alphabet characters.

        Parameters
        ----------
        str_ : str
            The string with accented characters

        Returns
        -------
        str
            The string with accents removed
        """
        return unidecode(str_)

    def byte_to_latin_characters(self, str_: str) -> str:
        """Convert string from latin1 to ISO-8859-1 encoding.

        Parameters
        ----------
        str_ : str
            The string to convert

        Returns
        -------
        str
            The converted string
        """
        return str_.encode("latin1").decode("ISO-8859-1")

    def remove_diacritics(self, str_: str) -> str:
        """Remove all diacritics from a string.

        Handles accents, cedillas, etc. from both Latin and non-Latin alphabets.

        Parameters
        ----------
        str_ : str
            The string with diacritics

        Returns
        -------
        str
            The string with diacritics removed
        """
        norm_txt = normalize("NFD", str_)
        shaved = "".join(c for c in norm_txt if not combining(c))
        return normalize("NFC", shaved)

    def remove_diacritics_nfkd(self, str_: str, bool_lower_case: bool = True) -> str:
        """Remove diacritics using NFKD normalization.

        Parameters
        ----------
        str_ : str
            The string to process
        bool_lower_case : bool
            Whether to convert to lowercase first, by default True

        Returns
        -------
        str
            The processed string
        """
        if bool_lower_case:
            str_ = str_.lower()
        str_ = str_.replace("\n", "")
        return "".join(c for c in normalize("NFKD", str_) if not combining(c))

    def normalize_text(self, str_: str) -> str:
        """Normalize text to ASCII, ignoring non-ASCII characters.

        Parameters
        ----------
        str_ : str
            The string to normalize

        Returns
        -------
        str
            The normalized ASCII string
        """
        return normalize("NFKD", str_).encode("ascii", "ignore").decode("utf-8")

    def remove_sup_period_marks(self, corpus: str, patterns: str = r"[!.?+]") -> str:
        """Remove sentence-ending punctuation marks.

        Parameters
        ----------
        corpus : str
            The text to process
        patterns : str
            Regex pattern matching punctuation to remove, by default r"[!.?+]"

        Returns
        -------
        str
            The text without ending punctuation
        """
        return re.sub(patterns, "", corpus)

    def remove_only_latin_diacritics(self, str_: str, latin_base: bool = False) -> str:
        """Remove diacritic marks only from Latin base characters.

        Parameters
        ----------
        str_ : str
            The string to process
        latin_base : bool
            Whether to only process Latin characters, by default False

        Returns
        -------
        str
            The processed string
        """
        norm_txt = normalize("NFD", str_)
        keepers = []
        for c in norm_txt:
            if combining(c) and latin_base:
                continue  # ignore diacritic on latin base char
            keepers.append(c)
            # if it isn't combining char, it's a new base char
            if not combining(c):
                latin_base = c in ascii_letters
        shaved = "".join(keepers)
        return normalize("NFC", shaved)

    def dewinize(self, str_: str) -> str:
        """Replace Win1252 symbols with ASCII equivalents.

        Parameters
        ----------
        str_ : str
            The string with Win1252 symbols

        Returns
        -------
        str
            The string with ASCII replacements
        """
        return str_.translate(self.multi_map_reference())

    def asciize(self, str_: str) -> str:
        """Normalize string to ASCII with compatibility replacements.

        Parameters
        ----------
        str_ : str
            The string to normalize

        Returns
        -------
        str
            The ASCII-compatible string
        """
        no_marks = self.remove_only_latin_diacritics(self.dewinize(str_))
        no_marks = no_marks.replace("ß", "ss")
        return normalize("NFKC", no_marks)

    def remove_substr(self, str_: str, substr_: str) -> str:
        """Remove all occurrences of a substring.

        Parameters
        ----------
        str_ : str
            The original string
        substr_ : str
            The substring to remove

        Returns
        -------
        str
            The string with substring removed
        """
        return str_.replace(substr_, "")

    def get_string_until_substr(self, str_: str, substring: str) -> str:
        """Get the portion of string before first occurrence of substring.

        Parameters
        ----------
        str_ : str
            The original string
        substring : str
            The substring to search for

        Returns
        -------
        str
            The portion before the substring
        """
        if not substring: 
            return ""
        return str_.split(substring)[0]

    def get_string_after_substr(self, str_: str, substring: str) -> str:
        """Get the portion of string after first occurrence of substring.

        Parameters
        ----------
        str_ : str
            The original string
        substring : str
            The substring to search for

        Returns
        -------
        str
            The portion after the substring
        """
        if not substring:
            return str_
        parts = str_.split(substring, 1)
        return parts[1] if len(parts) > 1 else str_

    def base64_encode(self, userid: str, password: str) -> str:
        """Encode user credentials in Base64.

        Parameters
        ----------
        userid : str
            The username
        password : str
            The password

        Returns
        -------
        str
            Base64 encoded credentials
        """
        return encode(userid, password)

    def base64_str_encode(self, str_: str) -> str:
        """Encode a string in Base64.

        Parameters
        ----------
        str_ : str
            The string to encode
        code_method : str
            The encoding method, by default "ascii"

        Returns
        -------
        str
            The Base64 encoded string
        """
        return base64.b64encode(str_.encode()).decode()

    def universally_unique_identifier(self) -> ReturnUUID:
        """Generate a universally unique identifier (UUID).

        Returns
        -------
        ReturnUUID
            Dictionary containing:
            - uuid: UUID object
            - uuid_hex_digits_str: UUID as hex string
            - uuid_32_character_hexadecimal_str: 32-char hex representation
        """
        uuid_identifier = uuid.uuid4()
        return {
            "uuid": uuid_identifier,
            "uuid_hex_digits_str": str(uuid_identifier),
            "uuid_32_character_hexadecimal_str": uuid_identifier.hex,
        }

    def letters_to_numbers(
        self,
        letters_in_alphabet: int = 21,
        first_letter_alphabet: str = "f",
        list_not_in_range: Optional[list] = None,
    ) -> dict:
        """Create mapping from letters to numbers.

        Parameters
        ----------
        letters_in_alphabet : int
            Number of letters to include, by default 21
        first_letter_alphabet : str
            Starting letter, by default "f"
        list_not_in_range : Optional[list]
            Letters to exclude, by default ["i", "l", "o", "p", "r", "s", "t", "w", "y"]

        Returns
        -------
        dict
            Mapping of letters to numbers
        """
        dict_ = dict()
        i_aux = 0

        if list_not_in_range is None:
            list_not_in_range = ["i", "l", "o", "p", "r", "s", "t", "w", "y"]

        for i in range(
            ord(first_letter_alphabet), ord(first_letter_alphabet) + letters_in_alphabet
        ):
            if chr(i) not in list_not_in_range:
                dict_[chr(i)] = i - 101 - i_aux
            else:
                i_aux += 1

        return dict_

    def alphabetic_range(self, case: str = "upper") -> list:
        """Get the alphabet as a list of letters.

        Parameters
        ----------
        case : str
            Either "upper" or "lower", by default "upper"

        Returns
        -------
        list
            List of alphabet letters

        Raises
        ------
        ValueError
            If case is not "upper" or "lower"
        """
        case = case.lower()
        if case == "upper":
            return list(ascii_uppercase)
        elif case == "lower":
            return list(ascii_lowercase)
        else:
            raise ValueError(
                f"case must be either 'upper' or 'lower', got '{case}'"
            )

    def regex_match_alphanumeric(self, str_: str, regex_match: str = "^[a-zA-Z0-9_]+$") -> bool:
        """Check if string matches alphanumeric pattern.

        Parameters
        ----------
        str_ : str
            The string to check
        regex_match : str
            The regex pattern, by default "^[a-zA-Z0-9_]+$"

        Returns
        -------
        bool
            True if string matches pattern
        """
        return bool(str_ and re.match(regex_match, str_))

    def has_numbers(self, str_: str) -> bool:
        """Check if string contains any digits.

        Parameters
        ----------
        str_ : str
            The string to check

        Returns
        -------
        bool
            True if string contains digits
        """
        return bool(re.search(r"\d", str_))

    def nfc_equal(self, str1: str, str2: str) -> bool:
        """Compare strings using NFC normalization.

        Parameters
        ----------
        str1 : str
            First string to compare
        str2 : str
            Second string to compare

        Returns
        -------
        bool
            True if strings are equivalent under NFC normalization
        """
        return normalize("NFC", str1) == normalize("NFC", str2)

    def casefold_equal(self, str1: str, str2: str) -> bool:
        """Compare strings using casefold normalization.

        Parameters
        ----------
        str1 : str
            First string to compare
        str2 : str
            Second string to compare

        Returns
        -------
        bool
            True if strings are equivalent under casefold
        """
        return normalize("NFC", str1).casefold() == normalize("NFC", str2).casefold()

    def remove_non_alphanumeric_chars(
        self, str_: str, str_pattern_maintain: str = r"[\W_]", str_replace: str = ""
    ) -> str:
        r"""Remove non-alphanumeric characters from a string.

        Parameters
        ----------
        str_ : str
            The input string
        str_pattern_maintain : str
            Regex pattern for characters to remove, by default r'[\W_]'
        str_replace : str
            Replacement string, by default ''

        Returns
        -------
        str
            String with non-alphanumeric characters removed
        """
        return re.sub(str_pattern_maintain, str_replace, str_)

    def remove_numeric_chars(self, str_: str) -> str:
        """Remove all numeric characters from a string.

        Parameters
        ----------
        str_ : str
            The input string

        Returns
        -------
        str
            String with digits removed
        """
        def_remove_digits = str.maketrans("", "", digits)
        return str_.translate(def_remove_digits)

    def is_capitalized(self, str_: str, bool_simple_validation: bool = True) -> bool:
        """Check if a string is properly capitalized.

        Parameters
        ----------
        str_ : str
            The string to check
        bool_simple_validation : bool
            If True, only checks first character is uppercase, by default True

        Returns
        -------
        bool
            True if string is properly capitalized
        """
        if not str_:  # handle empty string case
            return False
            
        str_ = self.remove_diacritics(str_)
        str_ = self.remove_non_alphanumeric_chars(str_)
        
        try:
            if bool_simple_validation:
                # for single character, just check if it's uppercase
                if len(str_) == 1:
                    return str_[0].isupper()
                # for longer strings, check first is upper and second is lower
                return str_[0].isupper() and str_[1].islower()
            else:
                # strict validation - first upper, rest lower
                return str_[0].isupper() and all(char.islower() for char in str_[1:])
        except IndexError:
            return False

    def split_re(self, str_: str, re_split: str = r"[;,\s]\s*") -> list:
        r"""Split string using regex pattern.

        Parameters
        ----------
        str_ : str
            The string to split
        re_split : str
            Regex pattern for splitting, by default r"[;,\s]\s*"

        Returns
        -------
        list
            List of split parts
        """
        return re.split(re_split, str_)

    def replace_case_insensitive(self, str_: str, str_replaced: str, str_replace: str) -> str:
        """Replace substring case-insensitively.

        Parameters
        ----------
        str_ : str
            The original string
        str_replaced : str
            The substring to replace
        str_replace : str
            The replacement string

        Returns
        -------
        str
            String with replacements made
        """
        if not str_replaced:  # handle empty string to replace
            return str_
            
        return re.sub(
            re.escape(str_replaced),  # escape special regex characters
            lambda m: str_replace,    # simple replacement (no case transformation)
            str_,
            flags=re.IGNORECASE
        )

    def matchcase(self) -> callable:
        """Create a case-matching replacement function.

        Parameters
        ----------
        str_ : str
            The string to use for case matching

        Returns
        -------
        callable
            A function that applies the same case pattern to matched text
        """

        @type_checker
        def replace(m: re.Match) -> str:
            """Replace matched text with original case.
            
            Parameters
            ----------
            m : re.Match
                The match object
            
            Returns
            -------
            str
                The matched text
            """
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

    def replace_respecting_case(self, str_: str, str_replaced: str, str_replace: str) -> str:
        """Replace substring while respecting original case.

        Parameters
        ----------
        str_ : str
            The original string
        str_replaced : str
            The substring to replace
        str_replace : str
            The replacement string

        Returns
        -------
        str
            String with case-respecting replacements
        """
        if not str_replaced:
            return str_

        @type_checker
        def matchcase(match: re.Match) -> str:
            """Replace matched text with original case.
            
            Parameters
            ----------
            match : re.Match
                The match object
            
            Returns
            -------
            str
                The matched text
            """
            text = match.group()
            if not text:
                return str_replace
            if text.isupper():
                return str_replace.upper()
            elif text[0].isupper():
                return str_replace[0].upper() + str_replace[1:].lower()
            else:
                return str_replace.lower()

        return re.sub(
            re.escape(str_replaced),
            matchcase,
            str_,
            flags=re.IGNORECASE
        )

    def replace_all(self, str_: str, dict_replacers: dict[str, str]) -> str:
        """Perform multiple string replacements.

        Parameters
        ----------
        str_ : str
            The original string
        dict_replacers : dict[str, str]
            Mapping of substrings to their replacements

        Returns
        -------
        str
            String with all replacements applied
        """
        for i, j in dict_replacers.items():
            str_ = str_.replace(i, j)
        return str_

    def html_to_txt(self, html_: str) -> str:
        """Convert HTML to plain text.

        Parameters
        ----------
        html_ : str
            The HTML string

        Returns
        -------
        str
            The plain text content
        """
        soup = BeautifulSoup(html_, features="lxml")
        # add newlines before and after block elements
        for element in soup.find_all(['br', 'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'div']):
            element.insert_after('\n')
            element.insert_before('\n')
        text = soup.get_text()
        text = re.sub(r'\n\s*\n', '\n', text)
        return text.strip()

    def extract_urls(self, str_: str) -> list[str]:
        """Extract all URLs from a string.

        Parameters
        ----------
        str_ : str
            The string containing URLs

        Returns
        -------
        list[str]
            List of found URLs
        """
        url_pattern = re.compile(r"https?://\S+|www\.\S+")
        return re.findall(url_pattern, str_)

    def is_word(self, _value: str) -> bool:
        """Check if a string is a word (not numeric).

        Parameters
        ----------
        _value : str
            The string to check

        Returns
        -------
        bool
            True if the string is non-numeric
        """
        if not _value:
            return False
        try:
            float(_value)
            return False
        except ValueError:
            return True

    def convert_case(
        self, 
        str_: str, 
        from_case: TypeCaseFrom, 
        to_case: TypeCaseTo
    ) -> str:
        """Convert string between different naming conventions.

        Supported cases:
        - camelCase ('camel')
        - PascalCase ('pascal')
        - kebab-case ('kebab')
        - UPPER_CONSTANT ('upper_constant')
        - lower_constant ('lower_constant')
        - snake_case ('snake')
        - UpperFirst ('upper_first')
        - Default (words separated by spaces, hyphens or underscores) ('default')

        Parameters
        ----------
        str_ : str
            The string to convert
        from_case : TypeCaseFrom
            Current case of the string
        to_case : TypeCaseTo
            Desired case of the string

        Returns
        -------
        str
            The converted string

        Raises
        ------
        ValueError
            If from_case or to_case are invalid
        """
        if not str_:
            return ""
        
        # from case
        if from_case == "camel":
            list_words = re.sub(r"([a-z])([A-Z])", r"\1_\2", str_)
            list_words = re.sub(r"([a-zA-Z])(\d)", r"\1_\2", list_words)
            list_words = re.sub(r"(\d)([a-zA-Z])", r"\1_\2", list_words)
            list_words = list_words.lower().split("_")
        elif from_case == "pascal":
            list_words = re.sub(r"([a-z])([A-Z])", r"\1_\2", str_).lower().split("_")
        elif from_case == "kebab":
            list_words = str_.lower().split("-")
        elif from_case in ("upper_constant", "lower_constant", "snake"):
            list_words = str_.lower().split("_")
        elif from_case == "upper_first":
            list_words = [str_[0].upper() + str_[1:].lower()]
        elif from_case == "default":
            str_ = str_.replace(" - ", " ")
            str_ = str_.replace("-", " ")
            str_ = str_.replace("_", " ")
            str_ = str_.replace("+", " ")
            str_ = str_.replace(" (", " ")
            str_ = str_.replace(") ", " ")
            str_ = str_.replace(r"\n", " ")
            list_words = str_.lower().split()
        else:
            raise ValueError(
                "Invalid from_case. Choose from ['camel', 'pascal', 'snake', 'kebab', "
                "'upper_constant', 'lower_constant', 'upper_first']"
            )
        if not list_words:
            return ""

        # converting to case
        if to_case == "camel":
            return list_words[0] + "".join(word.capitalize() for word in list_words[1:])
        elif to_case == "pascal":
            return "".join(word.capitalize() for word in list_words)
        elif to_case in ("snake", "lower_constant"):
            return "_".join(list_words).lower()
        elif to_case == "kebab":
            return "-".join(list_words).lower()
        elif to_case == "upper_constant":
            return "_".join(list_words).upper()
        elif to_case == "upper_first":
            return list_words[0].capitalize() + " " + " ".join(word for word in list_words[1:])
        else:
            raise ValueError(
                "Invalid to_case. Choose from ['camel', 'pascal', 'snake', 'kebab', "
                "'upper_constant', 'lower_constant', 'upper_first']"
            )

    def extract_info_between_braces(
        self, str_: str, str_pattern: str = r"\{\{(.*?)\}\}"
    ) -> list[str]:
        r"""Extract all text between double curly braces.

        Parameters
        ----------
        str_ : str
            The string to search
        str_pattern : str
            The regex pattern to use, by default r"\{\{(.*?)\}\}"

        Returns
        -------
        list[str]
            List of found matches
        """
        return re.findall(str_pattern, str_)

    def fill_placeholders(self, str_: str, dict_placeholders: dict) -> str:
        """Replace named placeholders in a string.

        Parameters
        ----------
        str_ : str
            The string with placeholders (in {{placeholder}} format)
        dict_placeholders : dict
            Mapping of placeholder names to values

        Returns
        -------
        str
            The string with placeholders filled
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
        """Format a number with leading zeros after a prefix.

        Parameters
        ----------
        str_prefix : str
            The prefix string
        int_num : int
            The number to format
        total_length : int
            Total desired length including prefix, by default 11

        Returns
        -------
        str
            The formatted string

        Raises
        ------
        ValueError
            If total_length is too small for the inputs
        """
        str_num = str(int_num)
        required_zeros = total_length - len(str_prefix) - len(str_num)
        if required_zeros < 0:
            raise ValueError("Total length is too small for the given inputs.")
        return f"{str_prefix}{'0' * required_zeros}{str_num}"

    def get_url_query(self, url: str, bool_include_fragment: bool = False) \
        -> dict[str, Union[str, list[str]]]:
        """Extract query parameters from a URL.

        Parameters
        ----------
        url : str
            The URL to parse
        bool_include_fragment : bool
            Whether to include fragment parameters, by default False

        Returns
        -------
        dict[str, Union[str, list[str]]]
            Dictionary of parameters (single values as strings, multiple as lists)
        """
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        if bool_include_fragment:
            fragment_params = parse_qs(parsed_url.fragment)
            query_params.update(fragment_params)
        return {
            key: value[0] if len(value) == 1 else value
            for key, value in query_params.items()
        }

    def has_no_letters(self, str_: str) -> bool:
        """Check if a string contains no letters.

        Parameters
        ----------
        str_ : str
            The string to check

        Returns
        -------
        bool
            True if the string contains no letters (A-Z, a-z)
        """
        return not any(char.isalpha() for char in str_)