from typing import List, Optional
from openai import OpenAI
from openai.api_resources.chat_completion import ChatCompletion
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone._config.global_slots import YAML_LLMS


class GPT():

    def __init__(self, api_key:str, str_model:str, int_max_tokens:int=100,
                 str_context:Optional[str]=None, bl_stream:bool=False) -> None:
        """Initialize the GPT class.

        Parameters
        ----------
        api_key : str
            The API key for accessing the OpenAI service.
        str_model : str
            The model name to use for generating completions.
        int_max_tokens : int, optional
            Maximum number of tokens for the completion. Defaults to 100.
        str_context : Optional[str], optional
            Optional context to provide to the model. Defaults to None.
        bl_stream : bool, optional
            Whether to stream the completion. Defaults to False.

        Returns
        -------
        None

        Notes
        -----
        This class provides an interface to OpenAI's GPT models for text generation.

        Documentation: https://platform.openai.com/docs/guides/gpt
        Models available: https://platform.openai.com/docs/models/gpt

        When choosing a model, consider:
        - Model capabilities (some support longer contexts)
        - Cost (pricing varies by model)
        - Token limits
        """

        self.api_key = api_key
        self.str_model = str_model
        self.int_max_tokens = int_max_tokens
        self.str_context = str_context
        self.bl_strem=bl_stream,
        self.client = OpenAI(api_key=self.api_key)

    def run_prompt(self, list_tuple:List[tuple]) -> ChatCompletion:
        """Run the prompt on the model.

        Parameters
        ----------
        list_tuple : List[tuple]
            List of tuples with the information to build the prompt.

        Returns
        -------
        ChatCompletion
            The response from the model.

        Notes
        -----
        The list of tuples must have the following structure:
        - Each tuple must have two elements: type and content.
        - The type must be one of the following: 'text' or 'image_url'.
        - The content must be a string with the content of the prompt.

        The context, if provided, will be added to the prompt as a system message.
        """
        list_ = list()
        # user's information in order to build the prompt
        dict_content = {
            "role": "user"
        }
        # looping within types and messages, in order to create the content of the prompt
        for tup_ in list_tuple:
            if tup_[0] == "text":
                list_.append({
                    "type": str(tup_[0]).lower(),
                    str(tup_[0]).lower(): str(tup_[1])
                })
            elif tup_[0] == "image_url":
                list_.append({
                    "type": str(tup_[0]).lower(),
                    "image_url": {
                        "url": str(tup_[1]).lower()
                    }
                })
        # creating the message content
        dict_content = HandlingDicts().merge_n_dicts(dict_content, {"content": list_})
        # add context info, regarding the prompt
        if self.str_context is not None:
            list_prompt = [
                {
                    "role": "system",
                    "content": self.str_context
                },
                dict_content
            ]
        else:
            list_prompt = [dict_content]
        # request llm info
        return self.client.chat.completions.create(
            model=self.str_model,
            messages=list_prompt,
            max_tokens=self.int_max_tokens,
            strem=self.bl_strem
        )
