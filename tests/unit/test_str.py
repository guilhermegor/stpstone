import unittest

from stpstone.utils.parsers.str import StrHandler


class TestStrHandler(unittest.TestCase):
    def setUp(self):
        self.handler = StrHandler()

    # Helper methods
    def assertTypeError(self, func: callable, *args: object) -> None:
        with self.assertRaises(ValueError):
            func(*args)

    # Test cases
    def test_get_between(self):
        # Normal cases
        self.assertEqual(self.handler.get_between("abc[def]ghi", "[", "]"), "def")
        self.assertEqual(self.handler.get_between("start[middle]end", "[", "]"), "middle")
        
        # Edge cases
        self.assertEqual(self.handler.get_between("no delimiters", "[", "]"), "")
        self.assertEqual(self.handler.get_between("", "[", "]"), "")
        self.assertEqual(self.handler.get_between("abc[def", "[", "]"), "")
        self.assertEqual(self.handler.get_between("abc]def[ghi", "]", "["), "def")
        
        # Type validation
        self.assertTypeError(self.handler.get_between, 123, "[", "]")
        self.assertTypeError(self.handler.get_between, "abc", 123, "]")
        self.assertTypeError(self.handler.get_between, "abc", "[", 123)

    def test_get_after(self):
        # Normal cases
        self.assertEqual(self.handler.get_after("prefix:suffix", "prefix:"), "suffix")
        self.assertEqual(self.handler.get_after("abc123", "abc"), "123")
        
        # Edge cases
        self.assertEqual(self.handler.get_after("no prefix", "abc"), "")
        self.assertEqual(self.handler.get_after("", "abc"), "")
        self.assertEqual(self.handler.get_after("abc", ""), "abc")
        
        # Type validation
        self.assertTypeError(self.handler.get_after, 123, "abc")
        self.assertTypeError(self.handler.get_after, "abc", 123)

    def test_find_substr_str(self):
        # Normal cases
        self.assertTrue(self.handler.find_substr_str("hello world", "world"))
        self.assertFalse(self.handler.find_substr_str("hello world", "earth"))
        
        # Edge cases
        self.assertTrue(self.handler.find_substr_str("", ""))
        self.assertFalse(self.handler.find_substr_str("", "abc"))
        self.assertTrue(self.handler.find_substr_str("abc", ""))
        
        # Type validation
        self.assertTypeError(self.handler.find_substr_str, 123, "abc")
        self.assertTypeError(self.handler.find_substr_str, "abc", 123)

    def test_match_string_like(self):
        # Normal cases
        self.assertTrue(self.handler.match_string_like("file.txt", "*.txt"))
        self.assertFalse(self.handler.match_string_like("file.txt", "*.csv"))
        
        # Edge cases
        self.assertTrue(self.handler.match_string_like("", "*"))
        self.assertTrue(self.handler.match_string_like("abc", "*"))
        self.assertTrue(self.handler.match_string_like("abc", "a?c"))
        
        # Type validation
        self.assertTypeError(self.handler.match_string_like, 123, "abc")
        self.assertTypeError(self.handler.match_string_like, "abc", 123)

    def test_removing_accents(self):
        # Normal cases
        self.assertEqual(self.handler.removing_accents("café"), "cafe")
        self.assertEqual(self.handler.removing_accents("naïve"), "naive")
        
        # Edge cases
        self.assertEqual(self.handler.removing_accents(""), "")
        self.assertEqual(self.handler.removing_accents("123"), "123")
        
        # Type validation
        self.assertTypeError(self.handler.removing_accents, 123)

    def test_remove_diacritics(self):
        # Normal cases
        self.assertEqual(self.handler.remove_diacritics("cliché"), "cliche")
        self.assertEqual(self.handler.remove_diacritics("naïve"), "naive")
        
        # Edge cases
        self.assertEqual(self.handler.remove_diacritics(""), "")
        self.assertEqual(self.handler.remove_diacritics("123"), "123")
        
        # Type validation
        self.assertTypeError(self.handler.remove_diacritics, 123)

    def test_remove_non_alphanumeric_chars(self):
        # Normal cases
        self.assertEqual(self.handler.remove_non_alphanumeric_chars("a!b@c#1"), "abc1")
        self.assertEqual(self.handler.remove_non_alphanumeric_chars("a b c"), "abc")
        
        # Edge cases
        self.assertEqual(self.handler.remove_non_alphanumeric_chars(""), "")
        self.assertEqual(self.handler.remove_non_alphanumeric_chars("!@#"), "")
        
        # Type validation
        self.assertTypeError(self.handler.remove_non_alphanumeric_chars, 123)

    def test_remove_numeric_chars(self):
        # Normal cases
        self.assertEqual(self.handler.remove_numeric_chars("a1b2c3"), "abc")
        self.assertEqual(self.handler.remove_numeric_chars("123"), "")
        
        # Edge cases
        self.assertEqual(self.handler.remove_numeric_chars(""), "")
        self.assertEqual(self.handler.remove_numeric_chars("abc"), "abc")
        
        # Type validation
        self.assertTypeError(self.handler.remove_numeric_chars, 123)

    def test_is_capitalized(self):
        # Normal cases
        self.assertTrue(self.handler.is_capitalized("Hello"))
        self.assertFalse(self.handler.is_capitalized("hello"))
        self.assertFalse(self.handler.is_capitalized("HELLO"))
        
        # Edge cases
        self.assertFalse(self.handler.is_capitalized(""))
        self.assertTrue(self.handler.is_capitalized("H"))
        self.assertFalse(self.handler.is_capitalized("h"))
        
        # Type validation
        self.assertTypeError(self.handler.is_capitalized, 123)

    def test_replace_case_insensitive(self):
        # Normal cases
        self.assertEqual(
            self.handler.replace_case_insensitive("Hello World", "world", "Earth"),
            "Hello Earth"
        )
        self.assertEqual(
            self.handler.replace_case_insensitive("FOO bar", "foo", "baz"),
            "baz bar"
        )
        
        # Edge cases
        self.assertEqual(
            self.handler.replace_case_insensitive("", "foo", "bar"),
            ""
        )
        self.assertEqual(
            self.handler.replace_case_insensitive("abc", "", "x"),
            "abc"
        )
        
        # Type validation
        self.assertTypeError(self.handler.replace_case_insensitive, 123, "a", "b")
        self.assertTypeError(self.handler.replace_case_insensitive, "abc", 123, "b")
        self.assertTypeError(self.handler.replace_case_insensitive, "abc", "a", 123)

    def test_replace_respecting_case(self):
        # Normal cases
        self.assertEqual(
            self.handler.replace_respecting_case("Hello World", "world", "earth"),
            "Hello Earth"
        )
        self.assertEqual(
            self.handler.replace_respecting_case("FOO bar", "foo", "baz"),
            "BAZ bar"
        )
        self.assertEqual(
            self.handler.replace_respecting_case("Foo Bar", "foo", "baz"),
            "Baz Bar"
        )
        
        # Edge cases
        self.assertEqual(
            self.handler.replace_respecting_case("", "foo", "bar"),
            ""
        )
        self.assertEqual(
            self.handler.replace_respecting_case("abc", "", "x"),
            "abc"
        )

    def test_html_to_txt(self):
        # Normal cases
        html = "<html><body><h1>Title</h1><p>Paragraph</p></body></html>"
        self.assertEqual(self.handler.html_to_txt(html), "Title\nParagraph")
        
        # Edge cases
        self.assertEqual(self.handler.html_to_txt(""), "")
        
        # Type validation
        self.assertTypeError(self.handler.html_to_txt, 123)

    def test_extract_urls(self):
        # Normal cases
        text = "Visit https://example.com and http://test.org"
        self.assertEqual(
            self.handler.extract_urls(text),
            ["https://example.com", "http://test.org"]
        )
        
        # Edge cases
        self.assertEqual(self.handler.extract_urls(""), [])
        self.assertEqual(
            self.handler.extract_urls("no urls here"),
            []
        )
        
        # Type validation
        self.assertTypeError(self.handler.extract_urls, 123)

    def test_convert_case(self):
        # Normal cases
        self.assertEqual(
            self.handler.convert_case("helloWorld", "camel", "snake"),
            "hello_world"
        )
        self.assertEqual(
            self.handler.convert_case("HelloWorld", "pascal", "kebab"),
            "hello-world"
        )
        self.assertEqual(
            self.handler.convert_case("hello_world", "default", "camel"),
            "helloWorld"
        )
        
        # Edge cases
        self.assertEqual(
            self.handler.convert_case("", "default", "camel"),
            ""
        )
        
        # Error conditions
        self.assertTypeError(self.handler.convert_case, "abc", "invalid", "camel")
        self.assertTypeError(self.handler.convert_case, "abc", "camel", "invalid")

    def test_extract_info_between_braces(self):
        # Normal cases
        self.assertEqual(
            self.handler.extract_info_between_braces("Hello {{name}}!"),
            ["name"]
        )
        self.assertEqual(
            self.handler.extract_info_between_braces("{{a}}{{b}}"),
            ["a", "b"]
        )
        
        # Edge cases
        self.assertEqual(
            self.handler.extract_info_between_braces(""),
            []
        )
        self.assertEqual(
            self.handler.extract_info_between_braces("no braces"),
            []
        )
        
        # Type validation
        self.assertTypeError(self.handler.extract_info_between_braces, 123)

    def test_fill_placeholders(self):
        # Normal cases
        self.assertEqual(
            self.handler.fill_placeholders(
                "Hello {{name}}!",
                {"name": "World"}
            ),
            "Hello World!"
        )
        self.assertEqual(
            self.handler.fill_placeholders(
                "{{a}} {{b}}",
                {"a": "1", "b": "2"}
            ),
            "1 2"
        )
        
        # Edge cases
        self.assertEqual(
            self.handler.fill_placeholders(
                "",
                {"name": "World"}
            ),
            ""
        )
        self.assertEqual(
            self.handler.fill_placeholders(
                "Hello {{name}}!",
                {}
            ),
            "Hello {{name}}!"
        )
        
        # Type validation
        self.assertTypeError(self.handler.fill_placeholders, 123, {})
        self.assertTypeError(self.handler.fill_placeholders, "abc", "not a dict")

    def test_fill_zeros(self):
        # Normal cases
        self.assertEqual(
            self.handler.fill_zeros("ID", 123, 7),
            "ID00123"
        )
        
        # Edge cases
        self.assertEqual(
            self.handler.fill_zeros("", 123, 5),
            "00123"
        )
        
        # Error conditions
        self.assertTypeError(self.handler.fill_zeros, "ID", 123, 2)
        
        # Type validation
        self.assertTypeError(self.handler.fill_zeros, 123, 123, 5)
        self.assertTypeError(self.handler.fill_zeros, 123, 123, 22)

    def test_get_url_query(self):
        # Normal cases
        self.assertEqual(
            self.handler.get_url_query("https://example.com?name=John&age=30"),
            {"name": "John", "age": "30"}
        )
        self.assertEqual(
            self.handler.get_url_query("https://example.com?colors=red&colors=blue"),
            {"colors": ["red", "blue"]}
        )
        
        # Edge cases
        self.assertEqual(
            self.handler.get_url_query("https://example.com"),
            {}
        )
        self.assertEqual(
            self.handler.get_url_query(""),
            {}
        )
        
        # Type validation
        self.assertTypeError(self.handler.get_url_query, 123)

    def test_has_no_letters(self):
        # Normal cases
        self.assertTrue(self.handler.has_no_letters("123!@#"))
        self.assertFalse(self.handler.has_no_letters("123abc"))
        
        # Edge cases
        self.assertTrue(self.handler.has_no_letters(""))
        self.assertTrue(self.handler.has_no_letters(" "))
        
        # Type validation
        self.assertTypeError(self.handler.has_no_letters, 123)

    def test_base64_str_encode(self):
        # Normal cases
        self.assertEqual(
            self.handler.base64_str_encode("hello"),
            "aGVsbG8="
        )
        
        # Edge cases
        self.assertEqual(
            self.handler.base64_str_encode(""),
            ""
        )
        
        # Type validation
        self.assertTypeError(self.handler.base64_str_encode, 123)
        self.assertTypeError(self.handler.base64_str_encode, "abc", 123)

    def test_universally_unique_identifier(self):
        # Just verify the structure of the return value
        result = self.handler.universally_unique_identifier()
        self.assertIn("uuid", result)
        self.assertIn("uuid_hex_digits_str", result)
        self.assertIn("uuid_32_character_hexadecimal_str", result)
        self.assertEqual(len(result["uuid_32_character_hexadecimal_str"]), 32)

    def test_letters_to_numbers(self):
        # Normal cases
        result = self.handler.letters_to_numbers()
        self.assertIsInstance(result, dict)
        self.assertTrue(all(isinstance(k, str) and isinstance(v, int) for k, v in result.items()))
        
        # Test with custom parameters
        custom_result = self.handler.letters_to_numbers(
            letters_in_alphabet=5,
            first_letter_alphabet="a",
            list_not_in_range=[]
        )
        self.assertEqual(len(custom_result), 5)
        
        # Type validation
        self.assertTypeError(self.handler.letters_to_numbers, "not an int")
        self.assertTypeError(self.handler.letters_to_numbers, 5, 123)  # first_letter not str
        self.assertTypeError(self.handler.letters_to_numbers, 5, "a", "not a list")

    def test_alphabetic_range(self):
        # Normal cases
        self.assertEqual(
            self.handler.alphabetic_range("upper"),
            list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        )
        self.assertEqual(
            self.handler.alphabetic_range("lower"),
            list("abcdefghijklmnopqrstuvwxyz")
        )
        
        # Error conditions
        self.assertTypeError(self.handler.alphabetic_range, "invalid")
        
        # Type validation
        self.assertTypeError(self.handler.alphabetic_range, 123)

    def test_regex_match_alphanumeric(self):
        # Normal cases
        self.assertTrue(self.handler.regex_match_alphanumeric("abc123"))
        self.assertFalse(self.handler.regex_match_alphanumeric("abc!123"))
        
        # Edge cases
        self.assertFalse(self.handler.regex_match_alphanumeric(""))
        self.assertFalse(self.handler.regex_match_alphanumeric(" "))
        
        # Type validation
        self.assertTypeError(self.handler.regex_match_alphanumeric, 123)
        self.assertTypeError(self.handler.regex_match_alphanumeric, "abc", 123)

    def test_bl_has_numbers(self):
        # Normal cases
        self.assertTrue(self.handler.bl_has_numbers("abc123"))
        self.assertFalse(self.handler.bl_has_numbers("abc"))
        
        # Edge cases
        self.assertFalse(self.handler.bl_has_numbers(""))
        
        # Type validation
        self.assertTypeError(self.handler.bl_has_numbers, 123)

    def test_nfc_equal(self):
        # Normal cases
        self.assertTrue(self.handler.nfc_equal("café", "cafe\u0301"))
        self.assertFalse(self.handler.nfc_equal("café", "cafe"))
        
        # Edge cases
        self.assertTrue(self.handler.nfc_equal("", ""))
        self.assertFalse(self.handler.nfc_equal("", "a"))
        
        # Type validation
        self.assertTypeError(self.handler.nfc_equal, 123, "abc")
        self.assertTypeError(self.handler.nfc_equal, "abc", 123)

    def test_casefold_equal(self):
        # Normal cases
        self.assertTrue(self.handler.casefold_equal("Strasse", "STRASSE"))
        self.assertTrue(self.handler.casefold_equal("Strasse", "strasse"))
        
        # Edge cases
        self.assertTrue(self.handler.casefold_equal("", ""))
        self.assertFalse(self.handler.casefold_equal("", "a"))
        
        # Type validation
        self.assertTypeError(self.handler.casefold_equal, 123, "abc")
        self.assertTypeError(self.handler.casefold_equal, "abc", 123)

    def test_remove_substr(self):
        # Normal cases
        self.assertEqual(self.handler.remove_substr("abc123abc", "abc"), "123")
        self.assertEqual(self.handler.remove_substr("abc123", "xyz"), "abc123")
        
        # Edge cases
        self.assertEqual(self.handler.remove_substr("", "abc"), "")
        self.assertEqual(self.handler.remove_substr("abc", ""), "abc")
        
        # Type validation
        self.assertTypeError(self.handler.remove_substr, 123, "abc")
        self.assertTypeError(self.handler.remove_substr, "abc", 123)

    def test_get_string_until_substr(self):
        # Normal cases
        self.assertEqual(self.handler.get_string_until_substr("abc123", "123"), "abc")
        self.assertEqual(self.handler.get_string_until_substr("abc123", "xyz"), "abc123")
        
        # Edge cases
        self.assertEqual(self.handler.get_string_until_substr("", "abc"), "")
        self.assertEqual(self.handler.get_string_until_substr("abc", ""), "")
        
        # Type validation
        self.assertTypeError(self.handler.get_string_until_substr, 123, "abc")
        self.assertTypeError(self.handler.get_string_until_substr, "abc", 123)

    def test_get_string_after_substr(self):
        # Normal cases
        self.assertEqual(self.handler.get_string_after_substr("abc123", "abc"), "123")
        self.assertEqual(self.handler.get_string_after_substr("abc123", "xyz"), "abc123")
        
        # Edge cases
        self.assertEqual(self.handler.get_string_after_substr("", "abc"), "")
        self.assertEqual(self.handler.get_string_after_substr("abc", ""), "abc")
        
        # Type validation
        self.assertTypeError(self.handler.get_string_after_substr, 123, "abc")
        self.assertTypeError(self.handler.get_string_after_substr, "abc", 123)

if __name__ == "__main__":
    unittest.main()