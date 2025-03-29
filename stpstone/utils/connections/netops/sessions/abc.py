from requests import Session
from typing import List, Dict, Union, Tuple, Any
from abc import ABC, abstractmethod


class ABCSession(ABC):

    @property
    @abstractmethod
    def _available_proxies(self):
        pass

    def _dict_proxy(self, str_ip:str, int_port:int) -> Dict[str, str]:
        return {
            'http': 'http://{}:{}'.format(str_ip, str(int_port)),
            'https': 'http://{}:{}'.format(str_ip, str(int_port))
        }

    @property
    @abstractmethod
    def _filter_proxies(self) -> List[Dict[str, Union[str, int]]]:
        pass

    @abstractmethod
    def _test_proxy(self, str_ip: str, int_port: int, bl_return_availability: bool = True) -> bool:
        pass

    @abstractmethod
    def _configure_session(self, dict_proxy: Union[Dict[str, str], None] = None,
                          int_retries: int = 10, int_backoff_factor: int = 1) -> Session:
        pass

    @property
    @abstractmethod
    def get_proxy(self) -> Dict[str, str]:
        pass

    @property
    @abstractmethod
    def get_proxies(self) -> List[Dict[str, str]]:
        pass

    @abstractmethod
    def ip_infos(self, session: Session, bl_return_availability: bool = False,
                 tup_timeout: Tuple[int, int] = (5, 5)) -> Union[List[Dict[str, Any]], None]:
        pass

    @property
    @abstractmethod
    def session(self):
        pass
