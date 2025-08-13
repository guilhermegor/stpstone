"""Unit tests for GPT class.

Tests the OpenAI GPT model interface functionality including:
- Initialization with valid and invalid parameters
- Prompt execution with various input types
- Error handling and validation
- Streaming and context capabilities
"""

from typing import Any
from unittest.mock import MagicMock, patch

from openai.types.chat import ChatCompletion
import pytest

from stpstone.utils.llms.gpt import GPT


class TestGPT:
    """Test cases for GPT class.

    This test class verifies the behavior of the GPT model interface
    with different input types and edge cases.
    """

    @pytest.fixture
    def valid_init_params(self) -> dict[str, Any]:
        """Fixture providing valid initialization parameters.

        Returns
        -------
        dict[str, Any]
            Dictionary containing valid initialization parameters
        """
        return {
            "api_key": "valid_api_key",
            "str_model": "gpt-4",
            "int_max_tokens": 100,
            "str_context": "Test context",
            "bool_stream": False
        }

    @pytest.fixture
    def mock_chat_completion(self) -> MagicMock:
        """Fixture providing mock ChatCompletion response.

        Returns
        -------
        MagicMock
            Mocked ChatCompletion object
        """
        mock = MagicMock(spec=ChatCompletion)
        return mock

    @pytest.fixture
    def mock_openai_client(self, mock_chat_completion: MagicMock) -> MagicMock:
        """Fixture providing mock OpenAI client.

        Parameters
        ----------
        mock_chat_completion : MagicMock
            Mocked ChatCompletion object

        Returns
        -------
        MagicMock
            Mocked OpenAI client
        """
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = mock_chat_completion
        return mock_client

    @pytest.fixture
    def sample_text_prompt(self) -> list[tuple]:
        """Fixture providing sample text prompt.

        Returns
        -------
        list[tuple]
            List of tuples with text prompt data
        """
        return [("text", "Hello world")]

    @pytest.fixture
    def sample_image_prompt(self) -> list[tuple]:
        """Fixture providing sample image prompt.

        Returns
        -------
        list[tuple]
            List of tuples with image URL prompt data
        """
        return [("image_url", "http://example.com/image.jpg")]

    @pytest.fixture
    def mixed_prompt(self) -> list[tuple]:
        """Fixture providing mixed text and image prompt.

        Returns
        -------
        list[tuple]
            List of tuples with mixed prompt data
        """
        return [
            ("text", "Describe this image"),
            ("image_url", "http://example.com/image.jpg")
        ]

    def test_init_with_valid_params(
        self,
        valid_init_params: dict[str, Any],
        mock_openai_client: MagicMock
    ) -> None:
        """Test initialization with valid parameters.

        Verifies
        --------
        - The GPT class can be initialized with valid parameters
        - The values are correctly stored in instance attributes
        - The OpenAI client is properly initialized

        Parameters
        ----------
        valid_init_params : dict[str, Any]
            Valid initialization parameters from fixture
        mock_openai_client : MagicMock
            Mocked OpenAI client from fixture
        """
        with patch("stpstone.utils.llms.gpt.OpenAI", return_value=mock_openai_client):
            gpt = GPT(**valid_init_params)
            
            assert gpt.api_key == valid_init_params["api_key"]
            assert gpt.str_model == valid_init_params["str_model"]
            assert gpt.int_max_tokens == valid_init_params["int_max_tokens"]
            assert gpt.str_context == valid_init_params["str_context"]
            assert gpt.bool_stream == valid_init_params["bool_stream"]
            assert gpt.client is mock_openai_client

    @pytest.mark.parametrize(
        "api_key,type_error", 
        [
            (None, TypeError), 
            ("", ValueError), 
            (123, TypeError)
        ]
    )
    def test_init_invalid_api_key(
        self,
        api_key: Any, # noqa ANN401: typing.Any is not allowed
        type_error: type,
        valid_init_params: dict[str, Any],
        mock_openai_client: MagicMock
    ) -> None:
        """Test initialization with invalid API keys.

        Verifies
        --------
        - ValueError is raised for invalid API keys
        - Error message matches expected pattern

        Parameters
        ----------
        api_key : Any
            Invalid API key to test
        type_error : type
            Expected exception type
        valid_init_params : dict[str, Any]
            Base valid parameters from fixture
        mock_openai_client : MagicMock
            Mocked OpenAI client from fixture
        """
        with patch("stpstone.utils.llms.gpt.OpenAI", return_value=mock_openai_client):
            valid_init_params["api_key"] = api_key
            with pytest.raises(type_error):
                GPT(**valid_init_params)

    @pytest.mark.parametrize(
        "model,type_error", 
        [
            (None, TypeError), 
            ("", ValueError), 
            (123, TypeError)
        ]
    )
    def test_init_invalid_model(
        self,
        model: Any, # noqa ANN401: typing.Any is not allowed
        type_error: type,
        valid_init_params: dict[str, Any],
        mock_openai_client: MagicMock
    ) -> None:
        """Test initialization with invalid model names.

        Verifies
        --------
        - ValueError is raised for invalid model names
        - Error message matches expected pattern

        Parameters
        ----------
        model : Any
            Invalid model name to test
        type_error : type
            Expected exception type
        valid_init_params : dict[str, Any]
            Base valid parameters from fixture
        mock_openai_client : MagicMock
            Mocked OpenAI client from fixture
        """
        with patch("stpstone.utils.llms.gpt.OpenAI", return_value=mock_openai_client):
            valid_init_params["str_model"] = model
            with pytest.raises(type_error):
                GPT(**valid_init_params)
                
    @pytest.mark.parametrize("max_tokens", [0, -1, -100])
    def test_init_invalid_max_tokens(
        self,
        max_tokens: int,
        valid_init_params: dict[str, Any],
        mock_openai_client: MagicMock
    ) -> None:
        """Test initialization with invalid max tokens.

        Verifies
        --------
        - ValueError is raised for non-positive max tokens
        - Error message matches expected pattern

        Parameters
        ----------
        max_tokens : int
            Invalid max tokens value to test
        valid_init_params : dict[str, Any]
            Base valid parameters from fixture
        mock_openai_client : MagicMock
            Mocked OpenAI client from fixture
        """
        with patch("stpstone.utils.llms.gpt.OpenAI", return_value=mock_openai_client):
            valid_init_params["int_max_tokens"] = max_tokens
            with pytest.raises(ValueError) as excinfo:
                GPT(**valid_init_params)
            
            assert "max_tokens must be a positive integer" in str(excinfo.value)

    def test_run_prompt_with_text(
        self,
        valid_init_params: dict[str, Any],
        mock_openai_client: MagicMock,
        sample_text_prompt: list[tuple],
        mock_chat_completion: MagicMock
    ) -> None:
        """Test run_prompt with text input.

        Verifies
        --------
        - Method executes successfully with text prompt
        - Correct ChatCompletion is returned
        - OpenAI client called with expected parameters

        Parameters
        ----------
        valid_init_params : dict[str, Any]
            Valid initialization parameters from fixture
        mock_openai_client : MagicMock
            Mocked OpenAI client from fixture
        sample_text_prompt : list[tuple]
            Sample text prompt from fixture
        mock_chat_completion : MagicMock
            Mocked ChatCompletion from fixture
        """
        with patch("stpstone.utils.llms.gpt.OpenAI", return_value=mock_openai_client):
            gpt = GPT(**valid_init_params)
            result = gpt.run_prompt(sample_text_prompt)
            
            assert result == mock_chat_completion
            mock_openai_client.chat.completions.create.assert_called_once()
            call_args = mock_openai_client.chat.completions.create.call_args[1]
            assert call_args["model"] == valid_init_params["str_model"]
            assert call_args["max_tokens"] == valid_init_params["int_max_tokens"]
            assert call_args["stream"] == valid_init_params["bool_stream"]

    def test_run_prompt_with_image(
        self,
        valid_init_params: dict[str, Any],
        mock_openai_client: MagicMock,
        sample_image_prompt: list[tuple],
        mock_chat_completion: MagicMock
    ) -> None:
        """Test run_prompt with image URL input.

        Verifies
        --------
        - Method executes successfully with image prompt
        - Correct ChatCompletion is returned
        - OpenAI client called with expected parameters

        Parameters
        ----------
        valid_init_params : dict[str, Any]
            Valid initialization parameters from fixture
        mock_openai_client : MagicMock
            Mocked OpenAI client from fixture
        sample_image_prompt : list[tuple]
            Sample image prompt from fixture
        mock_chat_completion : MagicMock
            Mocked ChatCompletion from fixture
        """
        with patch("stpstone.utils.llms.gpt.OpenAI", return_value=mock_openai_client):
            gpt = GPT(**valid_init_params)
            result = gpt.run_prompt(sample_image_prompt)
            
            assert result == mock_chat_completion
            mock_openai_client.chat.completions.create.assert_called_once()
            call_args = mock_openai_client.chat.completions.create.call_args[1]
            assert call_args["messages"][-1]["content"][0]["type"] == "image_url"
            assert call_args["messages"][-1]["content"][0]["image_url"]["url"] \
                == sample_image_prompt[0][1]

    def test_run_prompt_with_mixed_input(
        self,
        valid_init_params: dict[str, Any],
        mock_openai_client: MagicMock,
        mixed_prompt: list[tuple],
        mock_chat_completion: MagicMock
    ) -> None:
        """Test run_prompt with mixed text and image input.

        Verifies
        --------
        - Method executes successfully with mixed prompt
        - Correct ChatCompletion is returned
        - OpenAI client called with expected parameters

        Parameters
        ----------
        valid_init_params : dict[str, Any]
            Valid initialization parameters from fixture
        mock_openai_client : MagicMock
            Mocked OpenAI client from fixture
        mixed_prompt : list[tuple]
            Mixed prompt from fixture
        mock_chat_completion : MagicMock
            Mocked ChatCompletion from fixture
        """
        with patch("stpstone.utils.llms.gpt.OpenAI", return_value=mock_openai_client):
            gpt = GPT(**valid_init_params)
            result = gpt.run_prompt(mixed_prompt)
            
            assert result == mock_chat_completion
            mock_openai_client.chat.completions.create.assert_called_once()

    def test_run_prompt_with_context(
        self,
        valid_init_params: dict[str, Any],
        mock_openai_client: MagicMock,
        sample_text_prompt: list[tuple],
        mock_chat_completion: MagicMock
    ) -> None:
        """Test run_prompt with context provided.

        Verifies
        --------
        - Context is properly included in the prompt
        - Method executes successfully
        - Correct ChatCompletion is returned

        Parameters
        ----------
        valid_init_params : dict[str, Any]
            Valid initialization parameters from fixture
        mock_openai_client : MagicMock
            Mocked OpenAI client from fixture
        sample_text_prompt : list[tuple]
            Sample text prompt from fixture
        mock_chat_completion : MagicMock
            Mocked ChatCompletion from fixture
        """
        with patch("stpstone.utils.llms.gpt.OpenAI", return_value=mock_openai_client):
            gpt = GPT(**valid_init_params)
            result = gpt.run_prompt(sample_text_prompt)
            
            assert result == mock_chat_completion
            call_args = mock_openai_client.chat.completions.create.call_args[1]
            assert len(call_args["messages"]) == 2  # system + user messages
            assert call_args["messages"][0]["role"] == "system"
            assert call_args["messages"][0]["content"] == valid_init_params["str_context"]

    def test_run_prompt_without_context(
        self,
        valid_init_params: dict[str, Any],
        mock_openai_client: MagicMock,
        sample_text_prompt: list[tuple],
        mock_chat_completion: MagicMock
    ) -> None:
        """Test run_prompt without context.

        Verifies
        --------
        - Only user message is included when no context
        - Method executes successfully
        - Correct ChatCompletion is returned

        Parameters
        ----------
        valid_init_params : dict[str, Any]
            Valid initialization parameters from fixture
        mock_openai_client : MagicMock
            Mocked OpenAI client from fixture
        sample_text_prompt : list[tuple]
            Sample text prompt from fixture
        mock_chat_completion : MagicMock
            Mocked ChatCompletion from fixture
        """
        valid_init_params["str_context"] = None
        with patch("stpstone.utils.llms.gpt.OpenAI", return_value=mock_openai_client):
            gpt = GPT(**valid_init_params)
            result = gpt.run_prompt(sample_text_prompt)
            
            assert result == mock_chat_completion
            call_args = mock_openai_client.chat.completions.create.call_args[1]
            assert len(call_args["messages"]) == 1  # only user message
            assert call_args["messages"][0]["role"] == "user"

    def test_run_prompt_with_streaming(
        self,
        valid_init_params: dict[str, Any],
        mock_openai_client: MagicMock,
        sample_text_prompt: list[tuple],
        mock_chat_completion: MagicMock
    ) -> None:
        """Test run_prompt with streaming enabled.

        Verifies
        --------
        - Streaming parameter is properly passed to API
        - Method executes successfully
        - Correct ChatCompletion is returned

        Parameters
        ----------
        valid_init_params : dict[str, Any]
            Valid initialization parameters from fixture
        mock_openai_client : MagicMock
            Mocked OpenAI client from fixture
        sample_text_prompt : list[tuple]
            Sample text prompt from fixture
        mock_chat_completion : MagicMock
            Mocked ChatCompletion from fixture
        """
        valid_init_params["bool_stream"] = True
        with patch("stpstone.utils.llms.gpt.OpenAI", return_value=mock_openai_client):
            gpt = GPT(**valid_init_params)
            result = gpt.run_prompt(sample_text_prompt)
            
            assert result == mock_chat_completion
            call_args = mock_openai_client.chat.completions.create.call_args[1]
            assert call_args["stream"] is True

    @pytest.mark.parametrize(
        "invalid_prompt,type_error,excinfo", [
            ([], ValueError, "list_tuple cannot be empty"),
            ([("invalid_type", "content")], ValueError, "Invalid tuple type"),
            ([("text", "")], ValueError, "Tuple content cannot be empty"),
            ([("image_url", "")], ValueError, "Tuple content cannot be empty"),
            ([("text", "content", "ext")], ValueError, "Each tuple must have exactly 2 elements"),
            ([("text")], TypeError, "must be of type"),
            ([123], TypeError, "must be of type"),
            ("not_a_list", TypeError, "must be of type"),
        ]
    )
    def test_run_prompt_invalid_inputs(
        self,
        valid_init_params: dict[str, Any],
        mock_openai_client: MagicMock,
        invalid_prompt: Any, # noqa ANN401: typing.Any is not allowed
        type_error: type, 
        excinfo: str
    ) -> None:
        """Test run_prompt with various invalid inputs.

        Verifies
        --------
        - ValueError is raised for invalid prompts
        - Error message matches expected pattern

        Parameters
        ----------
        valid_init_params : dict[str, Any]
            Valid initialization parameters from fixture
        mock_openai_client : MagicMock
            Mocked OpenAI client from fixture
        invalid_prompt : Any
            Invalid prompt to test
        type_error : type
            Expected error type
        excinfo : str
            Expected error message

        Returns
        -------
        None
        """
        with patch("stpstone.utils.llms.gpt.OpenAI", return_value=mock_openai_client):
            gpt = GPT(**valid_init_params)
            
            with pytest.raises(type_error, match=excinfo):
                gpt.run_prompt(invalid_prompt)

    def test_run_prompt_api_error(
        self,
        valid_init_params: dict[str, Any],
        mock_openai_client: MagicMock,
        sample_text_prompt: list[tuple]
    ) -> None:
        """Test run_prompt when API call fails.

        Verifies
        --------
        - Exception is properly propagated
        - Error message is preserved

        Parameters
        ----------
        valid_init_params : dict[str, Any]
            Valid initialization parameters from fixture
        mock_openai_client : MagicMock
            Mocked OpenAI client from fixture
        sample_text_prompt : list[tuple]
            Sample text prompt from fixture
        """
        mock_openai_client.chat.completions.create.side_effect = Exception("API error")
        with patch("stpstone.utils.llms.gpt.OpenAI", return_value=mock_openai_client):
            gpt = GPT(**valid_init_params)
            
            with pytest.raises(Exception, match="API error"):
                gpt.run_prompt(sample_text_prompt)