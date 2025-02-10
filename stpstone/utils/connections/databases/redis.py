import redis
from typing import Dict
from __future__ import annotations


class RedisClient:
    """
    DOCSTRING: SINGLETON REDIS CLIENT FOR MANAGING CONNECTIONS
    INPUTS: -
    OUTPUTS: -
    """
    _instance = None

    def __init__(self, str_host:str='localhost', int_port:int=6379, bl_decode_resp:bool=True) -> None:
        """
        DOCSTRING: STORE THE CONNECTION CONFIGURATION
        INPUTS: HOST:STR, PORT:INT, DECODE_RESP:BOOL
        OUTPUTS: -
        """
        self.str_host = str_host
        self.int_port = int_port
        self.bl_decode_resp = bl_decode_resp

    def __new__(cls, *args, **kwargs) -> RedisClient:
        """
        DOCSTRING: NEW CONNECTION
        INPUTS: CREATE OR RETRIEVE THE SINGLETON INSTANCE OF REDISCLIENT
        OUTPUTS: CLASS INSTANCE
        """
        if not cls._instance:
            #   create a new instance if it doesn't exist
            cls._instance = super(RedisClient, cls).__new__(cls)
            #   initialize the redis client connection
            cls._instance._redis_client = cls._connect(*args, **kwargs)
        return cls._instance

    @staticmethod
    def _load_config(str_host:str, int_port:int, bl_decode_resp:bool) -> Dict[str, object]:
        """
        DOCSTRING: LOAD REDIS CONFIGURATION PARAMETERS
        INPUTS:
            - str_host: REDIS HOST
            - int_port: REDIS PORT
            - bl_decode_resp: WHETHER TO DECODE RESPONSES
        OUTPUTS: CONFIGURATION DICTIONARY
        """
        return {
            'host': str_host,
            'port': int_port,
            'decode_responses': bl_decode_resp
        }

    @classmethod
    def _connect(cls, str_host:str, int_port:int, bl_decode_resp:bool) -> redis.StrictRedis:
        """
        DOCSTRING: ESTABLISH A CONNECTION TO REDIS
        INPUTS:
            - str_host: REDIS HOST
            - int_port: REDIS PORT
            - bl_decode_resp: WHETHER TO DECODE RESPONSES
        OUTPUTS: REDIS CLIENT INSTANCE
        """
        # load configuration parameters
        config = cls._load_config(str_host, int_port, bl_decode_resp)
        # create and return a redis client instance
        return redis.StrictRedis(**config)

    @classmethod
    def get(cls) -> redis.StrictRedis:
        """
        DOCSTRING: RETRIEVE THE REDIS CLIENT INSTANCE
        INPUTS: -
        OUTPUTS: REDIS CLIENT INSTANCE
        """
        # return the singleton redis client instance
        return cls()._redis_client
