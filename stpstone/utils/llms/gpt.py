"""OpenAI GPT model interface for text generation.

This module provides a class for interacting with OpenAI's GPT models through their API.
It supports both text and image URL inputs, with optional context and streaming capabilities.
"""

from typing import Optional, TypedDict

from openai import OpenAI
from openai.types.chat import ChatCompletion

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class ReturnRunPrompt(TypedDict):
    """Typed dictionary for run_prompt method return type.

    Parameters
    ----------
    role : str
        Role of the message sender (system/user/assistant)
    content : list[dict]
        list of message content dictionaries
    """

    role: str
    content: list[dict]


class GPT(metaclass=TypeChecker):
    """Interface for OpenAI's GPT models.

    This class provides methods to generate text completions using OpenAI's GPT models
    with support for both text and image URL inputs.

    References
    ----------
    .. [1] https://platform.openai.com/docs/guides/gpt
    .. [2] https://platform.openai.com/docs/models/gpt
    """

    def _validate_api_key(self, api_key: str) -> None:
        """Validate the OpenAI API key.

        Parameters
        ----------
        api_key : str
            API key to validate

        Raises
        ------
        ValueError
            If API key is empty or not a string
        """
        if not api_key:
            raise ValueError("API key cannot be empty")
        if not isinstance(api_key, str):
            raise ValueError("API key must be a string")

    def _validate_model(self, model: str) -> None:
        """Validate the model name.

        Parameters
        ----------
        model : str
            Model name to validate

        Raises
        ------
        ValueError
            If model name is empty or not a string
        """
        if not model:
            raise ValueError("Model name cannot be empty")
        if not isinstance(model, str):
            raise ValueError("Model name must be a string")

    def _validate_max_tokens(self, max_tokens: int) -> None:
        """Validate the max tokens parameter.

        Parameters
        ----------
        max_tokens : int
            Maximum tokens to validate

        Raises
        ------
        ValueError
            If max_tokens is not positive
        """
        if max_tokens <= 0:
            raise ValueError("max_tokens must be a positive integer")

    def __init__(
        self,
        api_key: str,
        str_model: str,
        int_max_tokens: int = 100,
        str_context: Optional[str] = None,
        bool_stream: bool = False
    ) -> None:
        """Initialize the GPT class.

        Parameters
        ----------
        api_key : str
            The API key for accessing the OpenAI service.
        str_model : str
            The model name to use for generating completions.
        int_max_tokens : int, optional
            Maximum number of tokens for the completion (default: 100)
        str_context : Optional[str], optional
            Optional context to provide to the model (default: None)
        bool_stream : bool, optional
            Whether to stream the completion (default: False)

        Notes
        -----
        When choosing a model, consider:
        - Model capabilities (some support longer contexts)
        - Cost (pricing varies by model)
        - Token limits
        """
        self._validate_api_key(api_key)
        self._validate_model(str_model)
        self._validate_max_tokens(int_max_tokens)

        self.api_key = api_key
        self.str_model = str_model
        self.int_max_tokens = int_max_tokens
        self.str_context = str_context
        self.bool_stream = bool_stream
        self.client = OpenAI(api_key=self.api_key)

    def run_prompt(self, list_tuple: list[tuple]) -> ChatCompletion:
        """Run the prompt on the model.

        Parameters
        ----------
        list_tuple : list[tuple]
            list of tuples with the information to build the prompt.
            Each tuple must have two elements: type and content.
            The type must be one of: 'text' or 'image_url'.
            The content must be a string with the content of the prompt.

        Returns
        -------
        ChatCompletion
            The response from the model.

        Raises
        ------
        ValueError
            If list_tuple is empty
            If any tuple has invalid type
            If any tuple content is empty
        """
        if not list_tuple:
            raise ValueError("list_tuple cannot be empty")

        list_content = []
        dict_content: ReturnRunPrompt = {"role": "user", "content": []}

        for tup_ in list_tuple:
            if len(tup_) != 2:
                raise ValueError("Each tuple must have exactly 2 elements (type, content)")
            
            content_type, content = tup_
            if not content:
                raise ValueError("Tuple content cannot be empty")

            if content_type == "text":
                list_content.append({
                    "type": "text",
                    "text": str(content)
                })
            elif content_type == "image_url":
                list_content.append({
                    "type": "image_url",
                    "image_url": {"url": str(content)}
                })
            else:
                raise ValueError(f"Invalid tuple type: {content_type}")

        dict_content["content"] = list_content

        list_prompt = []
        if self.str_context is not None:
            list_prompt.append({
                "role": "system",
                "content": self.str_context
            })
        list_prompt.append(dict_content)

        return self.client.chat.completions.create(
            model=self.str_model,
            messages=list_prompt,
            max_tokens=self.int_max_tokens,
            stream=self.bool_stream
        )