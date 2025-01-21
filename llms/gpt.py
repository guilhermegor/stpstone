### INTEGRATION WITH GPT LLM ###

from typing import List, Optional
from openai import OpenAI
from openai.api_resources.chat_completion import ChatCompletion
from stpstone.handling_data.dicts import HandlingDicts
from stpstone.settings._global_slots import YAML_LLMS


class GPT():

    def __init__(self, api_key:str, str_model:str, int_max_tokens:int=100, 
                 str_context:Optional[str]=None, bl_stream:bool=False) -> None:
        '''
        REFERENCES:
            - DOCUMENTATION: https://platform.openai.com/docs/guides/gpt
            - MODELS AVAILABLE: https://platform.openai.com/docs/models/gp
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        self.api_key = api_key
        self.str_model = str_model
        self.int_max_tokens = int_max_tokens
        self.str_context = str_context
        self.bl_strem=bl_stream,
        self.client = OpenAI(api_key=self.api_key)

    def run_prompt(self, list_tuple:List[tuple], list_:list=list()) -> ChatCompletion:
        '''
        DOCSTRING: RUN LLM WITH PROMPT
        INPUTS: 
            - LIST_TUPLE:LIST[TUPLE] IN ORDER TO CREATE A PROMPT TO REQUEST THE LLM
        OUTPUTS: STR
        '''
        # user's information in order to build the prompt
        dict_content = {
            YAML_LLMS['openai']['key_role']: YAML_LLMS['openai']['value_user']
        }
        # looping within types and messages, in order to create the content of the prompt
        for tup_ in list_tuple:
            if tup_[0] == 'text':
                list_.append({
                    'type': str(tup_[0]).lower(),
                    str(tup_[0]).lower(): str(tup_[1])
                })
            elif tup_[0] == 'image_url':
                list_.append({
                    'type': str(tup_[0]).lower(),
                    'image_url': {
                        'url': str(tup_[1]).lower()
                    }
                })
        # creating the message content
        dict_content = HandlingDicts().merge_n_dicts(dict_content, {'content': list_})
        # add context info, regarding the prompt
        if self.str_context is not None:
            list_prompt = [
                {
                    'role': 'system', 
                    'content': self.str_context
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