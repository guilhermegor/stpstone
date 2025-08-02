"""Unit tests for StrHandler class.

Tests the string manipulation functionality with various input scenarios including:
- String extraction and searching
- Character encoding/decoding
- Case conversion and validation
- Pattern matching and substitution
- URL and HTML processing
- Unicode normalization and diacritic removal
- String formatting and placeholder handling
"""

import base64
from string import ascii_lowercase, ascii_uppercase
from typing import Any
import uuid

import pytest

from stpstone.utils.parsers.str import StrHandler


class TestStrHandler:
    """Test cases for StrHandler class."""

    @pytest.fixture
    def handler(self) -> Any: # noqa ANN01: typing.Any disallowed
        """Fixture providing StrHandler instance.

        Returns
        -------
        Any
            Instance of StrHandler class
        """
        return StrHandler()

    # --------------------------
    # Test multi_map_reference
    # --------------------------
    def test_multi_map_reference_returns_dict(
        self, 
        handler: Any # noqa ANN01: typing.Any disallowed
    ) -> None:
        """Test multi_map_reference returns a dictionary.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture

        Returns
        -------
        None

        Verifies
        --------
        That the method returns a dictionary with character mappings.
        """
        result = handler.multi_map_reference()
        assert isinstance(result, dict)
        assert result[ord("€")] == "<euro>"

    # --------------------------
    # Test get_between
    # --------------------------
    @pytest.mark.parametrize(
        "input_str,first,last,expected",
        [
            ("start[middle]end", "[", "]", "middle"),
            ("no delimiters", "[", "]", ""),
            ("multiple[first]middle[second]end", "[", "]", "first"),
        ],
    )
    def test_get_between(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        first: str, 
        last: str, 
        expected: str
    ) -> None:
        """Test extracting text between delimiters.

        Verifies
        --------
        That the method correctly extracts text between specified delimiters.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            Input string to test
        first : str
            Starting delimiter
        last : str
            Ending delimiter
        expected : str
            Expected result

        Returns
        -------
        None
        """
        assert handler.get_between(input_str, first, last) == expected

    # --------------------------
    # Test get_after
    # --------------------------
    @pytest.mark.parametrize(
        "input_str,delimiter,expected",
        [
            ("prefix:suffix", ":", "suffix"),
            ("no delimiter", ":", ""),
            ("multiple:colons:here", ":", "colons:here"),
        ],
    )
    def test_get_after(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        delimiter: str, 
        expected: str
    ) -> None:
        """Test extracting text after delimiter.

        Verifies
        --------
        That the method correctly extracts text after specified delimiter.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            Input string to test
        delimiter : str
            Delimiter to search for
        expected : str
            Expected result

        Returns
        -------
        None
        """
        assert handler.get_after(input_str, delimiter) == expected

    # --------------------------
    # Test find_substr_str
    # --------------------------
    @pytest.mark.parametrize(
        "string,substring,expected",
        [
            ("hello world", "world", True),
            ("hello world", "missing", False),
            ("", "empty", False),
        ],
    )
    def test_find_substr_str(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        string: str, 
        substring: str, 
        expected: bool
    ) -> None:
        """Test substring search functionality.

        Verifies
        --------
        That the method correctly identifies presence of substring.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        string : str
            String to search in
        substring : str
            Substring to search for
        expected : bool
            Expected result

        Returns
        -------
        None
        """
        assert handler.find_substr_str(string, substring) == expected

    # --------------------------
    # Test match_string_like
    # --------------------------
    @pytest.mark.parametrize(
        "string,pattern,expected",
        [
            ("file.txt", "*.txt", True),
            ("file.csv", "*.txt", False),
            ("data123", "data*", True),
        ],
    )
    def test_match_string_like(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        string: str, 
        pattern: str, 
        expected: bool
    ) -> None:
        """Test pattern matching with wildcards.

        Verifies
        --------
        That the method correctly matches strings against wildcard patterns.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        string : str
            String to match
        pattern : str
            Pattern with wildcards
        expected : bool
            Expected match result

        Returns
        -------
        None
        """
        assert handler.match_string_like(string, pattern) == expected

    # --------------------------
    # Test latin_characters
    # --------------------------
    def test_latin_characters_conversion(
        self, 
        handler: Any # noqa ANN01: typing.Any disallowed
    ) -> None:
        """Test latin1 to utf-8 conversion.

        Verifies
        --------
        That the method correctly converts latin1 encoded strings to utf-8.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture

        Returns
        -------
        None
        """
        test_str = "café"
        encoded = test_str.encode("latin1")
        decoded = handler.latin_characters(encoded.decode("latin1"))
        assert decoded == test_str

    # --------------------------
    # Test decode_special_characters_ftfy
    # --------------------------
    def test_decode_special_characters_ftfy(
        self, 
        handler: Any # noqa ANN01: typing.Any disallowed
    ) -> None:
        """Test fixing mojibake with ftfy.

        Verifies
        --------
        That the method correctly fixes encoding issues using ftfy.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture

        Returns
        -------
        None
        """
        broken = "â€“"
        fixed = handler.decode_special_characters_ftfy(broken)
        assert fixed == "–"

    # --------------------------
    # Test removing_accents
    # --------------------------
    @pytest.mark.parametrize(
        "input_str,expected",
        [
            ("café", "cafe"),
            ("naïve", "naive"),
            ("über", "uber"),
        ],
    )
    def test_removing_accents(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        expected: str
    ) -> None:
        """Test accent removal from Latin characters.

        Verifies
        --------
        That the method correctly removes accents from Latin characters.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            String with accents
        expected : str
            Expected result without accents

        Returns
        -------
        None
        """
        assert handler.removing_accents(input_str) == expected

    # --------------------------
    # Test remove_diacritics
    # --------------------------
    @pytest.mark.parametrize(
        "input_str,expected",
        [
            ("cliché", "cliche"),
            ("naïve", "naive"),
            ("žluťoučký", "zlutoucky"),
        ],
    )
    def test_remove_diacritics(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        expected: str
    ) -> None:
        """Test diacritic removal from all characters.

        Verifies
        --------
        That the method correctly removes diacritics from all characters.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            String with diacritics
        expected : str
            Expected result without diacritics

        Returns
        -------
        None
        """
        assert handler.remove_diacritics(input_str) == expected

    # --------------------------
    # Test remove_diacritics_nfkd
    # --------------------------
    @pytest.mark.parametrize(
        "input_str,lower_case,expected",
        [
            ("MÜNCHEN", True, "munchen"),
            ("MÜNCHEN", False, "MUNCHEN"),
            ("éclair", True, "eclair"),
        ],
    )
    def test_remove_diacritics_nfkd(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        lower_case: bool, 
        expected: str
    ) -> None:
        """Test NFKD diacritic removal with case option.

        Verifies
        --------
        That the method correctly removes diacritics using NFKD normalization.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            String with diacritics
        lower_case : bool
            Whether to convert to lowercase
        expected : str
            Expected result

        Returns
        -------
        None
        """
        assert handler.remove_diacritics_nfkd(input_str, lower_case) == expected

    # --------------------------
    # Test normalize_text
    # --------------------------
    def test_normalize_text_to_ascii(
        self, 
        handler: Any # noqa ANN01: typing.Any disallowed
    ) -> None:
        """Test text normalization to ASCII.

        Verifies
        --------
        That the method correctly normalizes text to ASCII, ignoring non-ASCII chars.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture

        Returns
        -------
        None
        """
        test_str = "café ñoño"
        normalized = handler.normalize_text(test_str)
        assert normalized == "cafe nono"

    # --------------------------
    # Test remove_sup_period_marks
    # --------------------------
    @pytest.mark.parametrize(
        "input_str,expected",
        [
            ("Hello! How are you?", "Hello How are you"),
            ("What... is this?", "What is this"),
            ("No punctuation", "No punctuation"),
        ],
    )
    def test_remove_sup_period_marks(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        expected: str
    ) -> None:
        """Test removal of sentence-ending punctuation.

        Verifies
        --------
        That the method correctly removes sentence-ending punctuation marks.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            String with punctuation
        expected : str
            Expected result without punctuation

        Returns
        -------
        None
        """
        assert handler.remove_sup_period_marks(input_str) == expected

    # --------------------------
    # Test dewinize and asciize
    # --------------------------
    def test_dewinize_replaces_win1252_symbols(
        self, 
        handler: Any # noqa ANN01: typing.Any disallowed
    ) -> None:
        """Test Win1252 symbol replacement.

        Verifies
        --------
        That the method correctly replaces Win1252 symbols with ASCII equivalents.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture

        Returns
        -------
        None
        """
        test_str = "™ € …"
        dewinized = handler.dewinize(test_str)
        assert "(TM)" in dewinized
        assert "<euro>" in dewinized
        assert "..." in dewinized

    def test_asciize_normalizes_to_ascii(
        self, 
        handler: Any # noqa ANN01: typing.Any disallowed
    ) -> None:
        """Test full ASCII normalization.

        Verifies
        --------
        That the method correctly normalizes text to ASCII with compatibility replacements.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture

        Returns
        -------
        None
        """
        test_str = "café ™ €"
        asciized = handler.asciize(test_str)
        assert "cafe" in asciized
        assert "(TM)" in asciized
        assert "<euro>" in asciized

    # --------------------------
    # Test remove_substr
    # --------------------------
    @pytest.mark.parametrize(
        "input_str,substr,expected",
        [
            ("hello world", "world", "hello "),
            ("repeat repeat", "peat", "re re"),
            ("no match", "xyz", "no match"),
        ],
    )
    def test_remove_substr(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        substr: str, 
        expected: str
    ) -> None:
        """Test substring removal.

        Verifies
        --------
        That the method correctly removes all occurrences of a substring.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            Original string
        substr : str
            Substring to remove
        expected : str
            Expected result

        Returns
        -------
        None
        """
        assert handler.remove_substr(input_str, substr) == expected

    # --------------------------
    # Test string splitting
    # --------------------------
    @pytest.mark.parametrize(
        "input_str,substring,expected",
        [
            ("prefix.suffix", ".", "prefix"),
            ("no delimiter", ".", "no delimiter"),
            ("multiple.dots.in.string", ".", "multiple"),
        ],
    )
    def test_get_string_until_substr(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        substring: str, 
        expected: str
    ) -> None:
        """Test getting string until substring.

        Verifies
        --------
        That the method correctly returns the portion before the first occurrence of substring.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            Original string
        substring : str
            Substring to split on
        expected : str
            Expected result

        Returns
        -------
        None
        """
        assert handler.get_string_until_substr(input_str, substring) == expected

    @pytest.mark.parametrize(
        "input_str,substring,expected",
        [
            ("prefix.suffix", ".", "suffix"),
            ("no delimiter", ".", "no delimiter"),
            ("multiple.dots.in.string", ".", "dots.in.string"),
        ],
    )
    def test_get_string_after_substr(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        substring: str, 
        expected: str
    ) -> None:
        """Test getting string after substring.

        Verifies
        --------
        That the method correctly returns the portion after the first occurrence of substring.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            Original string
        substring : str
            Substring to split on
        expected : str
            Expected result

        Returns
        -------
        None
        """
        assert handler.get_string_after_substr(input_str, substring) == expected

    # --------------------------
    # Test base64 encoding
    # --------------------------
    def test_base64_encode_credentials(
        self, 
        handler: Any # noqa ANN01: typing.Any disallowed
    ) -> None:
        """Test Base64 encoding of credentials.

        Verifies
        --------
        That the method correctly encodes username:password in Base64.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture

        Returns
        -------
        None
        """
        encoded = handler.base64_encode("user", "pass")
        assert encoded.startswith("Basic ")
        decoded = base64.b64decode(encoded[6:]).decode()
        assert decoded == "user:pass"

    def test_base64_str_encode(
        self, 
        handler: Any # noqa ANN01: typing.Any disallowed
    ) -> None:
        """Test Base64 string encoding.

        Verifies
        --------
        That the method correctly encodes a string in Base64.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture

        Returns
        -------
        None
        """
        test_str = "hello world"
        encoded = handler.base64_str_encode(test_str)
        decoded = base64.b64decode(encoded).decode()
        assert decoded == test_str

    def test_base64_str_encode_invalid_input(
        self, 
        handler: Any # noqa ANN01: typing.Any disallowed
    ) -> None:
        """Test Base64 encoding with invalid input.

        Verifies
        --------
        That the method raises TypeError when input is not a string.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be of type"):
            handler.base64_str_encode(123)  # type: ignore

    # --------------------------
    # Test UUID generation
    # --------------------------
    def test_universally_unique_identifier(
        self, 
        handler: Any # noqa ANN01: typing.Any disallowed
    ) -> None:
        """Test UUID generation.

        Verifies
        --------
        That the method returns a valid UUID in multiple formats.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture

        Returns
        -------
        None
        """
        result = handler.universally_unique_identifier()
        assert isinstance(result["uuid"], uuid.UUID)
        assert isinstance(result["uuid_hex_digits_str"], str)
        assert isinstance(result["uuid_32_character_hexadecimal_str"], str)
        assert len(result["uuid_32_character_hexadecimal_str"]) == 32

    # --------------------------
    # Test letters_to_numbers
    # --------------------------
    def test_letters_to_numbers_mapping(
        self, 
        handler: Any # noqa ANN01: typing.Any disallowed
    ) -> None:
        """Test letter to number mapping.

        Verifies
        --------
        That the method correctly maps letters to numbers with exclusions.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture

        Returns
        -------
        None
        """
        result = handler.letters_to_numbers(
            letters_in_alphabet=5,
            first_letter_alphabet="a",
            list_not_in_range=["b", "d"],
        )
        assert result == {"a": -4, "c": -3, "e": -2}

    # --------------------------
    # Test alphabetic_range
    # --------------------------
    @pytest.mark.parametrize(
        "case,expected",
        [
            ("upper", ascii_uppercase),
            ("lower", ascii_lowercase),
        ],
    )
    def test_alphabetic_range_valid_cases(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        case: str, 
        expected: list[str]
    ) -> None:
        """Test getting alphabet in specified case.

        Verifies
        --------
        That the method returns the correct alphabet for valid cases.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        case : str
            Case to test ('upper' or 'lower')
        expected : list[str]
            Expected alphabet list

        Returns
        -------
        None
        """
        assert handler.alphabetic_range(case) == list(expected)

    def test_alphabetic_range_invalid_case(
        self, 
        handler: Any # noqa ANN01: typing.Any disallowed
    ) -> None:
        """Test invalid case parameter.

        Verifies
        --------
        That the method raises ValueError for invalid case parameter.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="case must be either 'upper' or 'lower'"):
            handler.alphabetic_range("invalid")

    # --------------------------
    # Test regex matching
    # --------------------------
    @pytest.mark.parametrize(
        "input_str,pattern,expected",
        [
            ("abc123", "^[a-zA-Z0-9_]+$", True),
            ("abc!123", "^[a-zA-Z0-9_]+$", False),
            ("", "^[a-zA-Z0-9_]+$", False),
        ],
    )
    def test_regex_match_alphanumeric(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        pattern: str, 
        expected: bool
    ) -> None:
        """Test alphanumeric regex matching.

        Verifies
        --------
        That the method correctly matches strings against alphanumeric pattern.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            String to test
        pattern : str
            Regex pattern
        expected : bool
            Expected match result

        Returns
        -------
        None
        """
        assert handler.regex_match_alphanumeric(input_str, pattern) == expected

    # --------------------------
    # Test has_numbers
    # --------------------------
    @pytest.mark.parametrize(
        "input_str,expected",
        [
            ("abc123", True),
            ("no numbers", False),
            ("123", True),
            ("", False),
        ],
    )
    def test_has_numbers(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        expected: bool
    ) -> None:
        """Test digit detection in strings.

        Verifies
        --------
        That the method correctly detects digits in strings.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            String to test
        expected : bool
            Expected result

        Returns
        -------
        None
        """
        assert handler.has_numbers(input_str) == expected

    # --------------------------
    # Test string comparison
    # --------------------------
    @pytest.mark.parametrize(
        "str1,str2,expected",
        [
            ("café", "cafe\u0301", True),  # NFC normalization
            ("hello", "HELLO", False),
            ("", "", True),
        ],
    )
    def test_nfc_equal(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        str1: str, 
        str2: str, 
        expected: bool
    ) -> None:
        """Test NFC-normalized string comparison.

        Verifies
        --------
        That the method correctly compares strings using NFC normalization.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        str1 : str
            First string to compare
        str2 : str
            Second string to compare
        expected : bool
            Expected equality result

        Returns
        -------
        None
        """
        assert handler.nfc_equal(str1, str2) == expected

    @pytest.mark.parametrize(
        "str1,str2,expected",
        [
            ("HELLO", "hello", True),  # casefold comparison
            ("straße", "strasse", True),  # german sharp s
            ("hello", "world", False),
        ],
    )
    def test_casefold_equal(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        str1: str, 
        str2: str, 
        expected: bool
    ) -> None:
        """Test casefold-normalized string comparison.

        Verifies
        --------
        That the method correctly compares strings using casefold normalization.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        str1 : str
            First string to compare
        str2 : str
            Second string to compare
        expected : bool
            Expected equality result

        Returns
        -------
        None
        """
        assert handler.casefold_equal(str1, str2) == expected

    # --------------------------
    # Test character removal
    # --------------------------
    @pytest.mark.parametrize(
        "input_str,expected",
        [
            ("abc123", "abc"),
            ("!@#test123", "!@#test"),
            ("", ""),
        ],
    )
    def test_remove_numeric_chars(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        expected: str
    ) -> None:
        """Test numeric character removal.

        Verifies
        --------
        That the method correctly removes all numeric characters.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            String to process
        expected : str
            Expected result without digits

        Returns
        -------
        None
        """
        assert handler.remove_numeric_chars(input_str) == expected

    @pytest.mark.parametrize(
        "input_str,expected",
        [
            ("Hello_World!123", "HelloWorld123"),
            ("test-case", "testcase"),
            ("", ""),
        ],
    )
    def test_remove_non_alphanumeric_chars(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        expected: str
    ) -> None:
        """Test non-alphanumeric character removal.

        Verifies
        --------
        That the method correctly removes non-alphanumeric characters.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            String to process
        expected : str
            Expected result with only alphanumeric chars

        Returns
        -------
        None
        """
        assert handler.remove_non_alphanumeric_chars(input_str) == expected

    # --------------------------
    # Test capitalization checks
    # --------------------------
    @pytest.mark.parametrize(
        "input_str,simple,expected",
        [
            ("Hello", True, True),
            ("hello", True, False),
            ("HELLO", True, False),
            ("Hello", False, True),
            ("Hello World", False, False),  # second word capitalized
            ("", True, False),
        ],
    )
    def test_is_capitalized(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        simple: bool, 
        expected: bool
    ) -> None:
        """Test capitalization validation.

        Verifies
        --------
        That the method correctly validates string capitalization.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            String to check
        simple : bool
            Whether to use simple validation
        expected : bool
            Expected result

        Returns
        -------
        None
        """
        assert handler.is_capitalized(input_str, simple) == expected

    # --------------------------
    # Test string splitting
    # --------------------------
    @pytest.mark.parametrize(
        "input_str,pattern,expected",
        [
            ("a,b,c", r",", ["a", "b", "c"]),
            ("a b  c", r"\s+", ["a", "b", "c"]),
            ("", r",", [""]),
        ],
    )
    def test_split_re(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        pattern: str, 
        expected: list[str]
    ) -> None:
        """Test regex-based string splitting.

        Verifies
        --------
        That the method correctly splits strings using regex patterns.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            String to split
        pattern : str
            Regex pattern to split on
        expected : list[str]
            Expected split result

        Returns
        -------
        None
        """
        assert handler.split_re(input_str, pattern) == expected

    # --------------------------
    # Test string replacement
    # --------------------------
    @pytest.mark.parametrize(
        "input_str,old,new,expected",
        [
            ("Hello World", "World", "Python", "Hello Python"),
            ("case INSENSITIVE", "insensitive", "sensitive", "case sensitive"),
            ("no match", "xyz", "abc", "no match"),
        ],
    )
    def test_replace_case_insensitive(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        old: str, 
        new: str, 
        expected: str
    ) -> None:
        """Test case-insensitive string replacement.

        Verifies
        --------
        That the method correctly replaces substrings case-insensitively.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            Original string
        old : str
            Substring to replace
        new : str
            Replacement string
        expected : str
            Expected result

        Returns
        -------
        None
        """
        assert handler.replace_case_insensitive(input_str, old, new) == expected

    @pytest.mark.parametrize(
        "input_str,old,new,expected",
        [
            ("HELLO world", "hello", "hi", "HI world"),
            ("Hello world", "hello", "hi", "Hi world"),
            ("hello world", "hello", "hi", "hi world"),
        ],
    )
    def test_replace_respecting_case(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        old: str, 
        new: str, 
        expected: str
    ) -> None:
        """Test case-respecting string replacement.

        Verifies
        --------
        That the method correctly replaces substrings while respecting original case.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            Original string
        old : str
            Substring to replace
        new : str
            Replacement string
        expected : str
            Expected result

        Returns
        -------
        None
        """
        assert handler.replace_respecting_case(input_str, old, new) == expected

    def test_replace_all_multiple_replacements(
        self, 
        handler: Any # noqa ANN01: typing.Any disallowed
    ) -> None:
        """Test multiple string replacements.

        Verifies
        --------
        That the method correctly performs multiple string replacements.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture

        Returns
        -------
        None
        """
        input_str = "a b c"
        replacements = {"a": "1", "b": "2", "c": "3"}
        assert handler.replace_all(input_str, replacements) == "1 2 3"

    # --------------------------
    # Test HTML processing
    # --------------------------
    def test_html_to_txt_conversion(
        self, 
        handler: Any # noqa ANN01: typing.Any disallowed
    ) -> None:
        """Test HTML to plain text conversion.

        Verifies
        --------
        That the method correctly converts HTML to plain text.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture

        Returns
        -------
        None
        """
        html = "<html><body><h1>Title</h1><p>Paragraph</p></body></html>"
        text = handler.html_to_txt(html)
        assert "Title" in text
        assert "Paragraph" in text
        assert "<h1>" not in text

    # --------------------------
    # Test URL extraction
    # --------------------------
    @pytest.mark.parametrize(
        "input_str,expected",
        [
            ("Visit https://example.com", ["https://example.com"]),
            ("No URLs here", []),
            ("Multiple http://a.com and https://b.com", ["http://a.com", "https://b.com"]),
        ],
    )
    def test_extract_urls(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, expected: list[str]
    ) -> None:
        """Test URL extraction from text.

        Verifies
        --------
        That the method correctly extracts URLs from text.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            Text containing URLs
        expected : list[str]
            Expected list of URLs

        Returns
        -------
        None
        """
        assert handler.extract_urls(input_str) == expected

    # --------------------------
    # Test word detection
    # --------------------------
    @pytest.mark.parametrize(
        "input_str,expected",
        [
            ("word", True),
            ("123", False),
            ("word123", True),  # Mixed is considered a word
            ("", False),
        ],
    )
    def test_is_word(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        expected: bool
    ) -> None:
        """Test word vs numeric string detection.

        Verifies
        --------
        That the method correctly identifies word strings vs numeric strings.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            String to check
        expected : bool
            Expected result (True if word)

        Returns
        -------
        None
        """
        assert handler.is_word(input_str) == expected

    # --------------------------
    # Test case conversion
    # --------------------------
    @pytest.mark.parametrize(
        "input_str,from_case,to_case,expected",
        [
            ("helloWorld", "camel", "snake", "hello_world"),
            ("HelloWorld", "pascal", "snake", "hello_world"),
            ("hello_world", "snake", "camel", "helloWorld"),
            ("hello-world", "kebab", "snake", "hello_world"),
            ("HELLO_WORLD", "upper_constant", "lower_constant", "hello_world"),
            ("hello world", "default", "upper_first", "Hello world"),
        ],
    )
    def test_convert_case(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        from_case: str, 
        to_case: str, 
        expected: str
    ) -> None:
        """Test case conversion between naming conventions.

        Verifies
        --------
        That the method correctly converts between different naming conventions.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            String to convert
        from_case : str
            Source case format
        to_case : str
            Target case format
        expected : str
            Expected converted string

        Returns
        -------
        None
        """
        assert handler.convert_case(input_str, from_case, to_case) == expected

    def test_convert_case_invalid_cases(
        self, 
        handler: Any # noqa ANN01: typing.Any disallowed
    ) -> None:
        """Test case conversion with invalid cases.

        Verifies
        --------
        That the method raises ValueError for invalid case parameters.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture

        Returns
        -------
        None
        """
        with pytest.raises(ValueError):
            handler.convert_case("test", "invalid", "snake")

        with pytest.raises(ValueError):
            handler.convert_case("test", "snake", "invalid")

    # --------------------------
    # Test placeholder handling
    # --------------------------
    def test_extract_info_between_braces(
        self, 
        handler: Any # noqa ANN01: typing.Any disallowed
    ) -> None:
        """Test extraction of text between braces.

        Verifies
        --------
        That the method correctly extracts text between double curly braces.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture

        Returns
        -------
        None
        """
        input_str = "Hello {{name}}, welcome to {{place}}"
        result = handler.extract_info_between_braces(input_str)
        assert result == ["name", "place"]

    def test_fill_placeholders(
        self, 
        handler: Any # noqa ANN01: typing.Any disallowed
    ) -> None:
        """Test placeholder replacement.

        Verifies
        --------
        That the method correctly replaces named placeholders in a string.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture

        Returns
        -------
        None
        """
        input_str = "Hello {{name}}, welcome to {{place}}"
        replacements = {"name": "Alice", "place": "Wonderland"}
        result = handler.fill_placeholders(input_str, replacements)
        assert result == "Hello Alice, welcome to Wonderland"

    # --------------------------
    # Test zero padding
    # --------------------------
    @pytest.mark.parametrize(
        "prefix,num,length,expected",
        [
            ("ID", 42, 5, "ID042"),
            ("TEST", 1, 6, "TEST01"),
            ("A", 123, 5, "A0123"),
        ],
    )
    def test_fill_zeros(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        prefix: str, 
        num: int, 
        length: int, 
        expected: str
    ) -> None:
        """Test zero-padded string formatting.

        Verifies
        --------
        That the method correctly formats numbers with leading zeros after a prefix.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        prefix : str
            String prefix
        num : int
            Number to format
        length : int
            Total desired length
        expected : str
            Expected formatted string

        Returns
        -------
        None
        """
        assert handler.fill_zeros(prefix, num, length) == expected

    def test_fill_zeros_invalid_length(
        self, 
        handler: Any # noqa ANN01: typing.Any disallowed
    ) -> None:
        """Test zero padding with invalid length.

        Verifies
        --------
        That the method raises ValueError when total length is too small.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Total length is too small"):
            handler.fill_zeros("ID", 12345, 4)

    # --------------------------
    # Test URL query parsing
    # --------------------------
    @pytest.mark.parametrize(
        "url,include_fragment,expected",
        [
            (
                "http://example.com?param1=value1&param2=value2",
                False,
                {"param1": "value1", "param2": "value2"},
            ),
            (
                "http://example.com?param=1&param=2",
                False,
                {"param": ["1", "2"]},
            ),
            (
                "http://example.com#frag=ment", # codespell:ignore ment
                True,
                {"frag": "ment"}, # codespell:ignore ment
            ),
        ],
    )
    def test_get_url_query(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        url: str, 
        include_fragment: bool, 
        expected: dict
    ) -> None:
        """Test URL query parameter extraction.

        Verifies
        --------
        That the method correctly extracts query parameters from URLs.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        url : str
            URL to parse
        include_fragment : bool
            Whether to include fragment parameters
        expected : dict
            Expected query parameters

        Returns
        -------
        None
        """
        result = handler.get_url_query(url, include_fragment)
        assert result == expected

    # --------------------------
    # Test letter detection
    # --------------------------
    @pytest.mark.parametrize(
        "input_str,expected",
        [
            ("123", True),
            ("abc", False),
            ("123abc", False),
            ("", True),
        ],
    )
    def test_has_no_letters(
        self, 
        handler: Any, # noqa ANN01: typing.Any disallowed
        input_str: str, 
        expected: bool
    ) -> None:
        """Test letter absence detection.

        Verifies
        --------
        That the method correctly detects strings with no letters.

        Parameters
        ----------
        handler : Any
            StrHandler instance from fixture
        input_str : str
            String to check
        expected : bool
            Expected result (True if no letters)

        Returns
        -------
        None
        """
        assert handler.has_no_letters(input_str) == expected