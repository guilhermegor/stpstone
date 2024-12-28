### HANDLING API REQUESTS ###

from requests.adapters import HTTPAdapter
from requests import Session
from urllib3.util import Retry


class ReqSession:

    def __init__(self, url:str, int_retries:int=10, int_backoff_factor:int=1, 
                 list_status_forcelist:list=[429, 500, 502, 503, 504]) -> None:
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        self.url = url
        self.int_retries = int_retries
        self.int_backoff_factor = int_backoff_factor
        self.list_status_forcelist = list_status_forcelist
        self.session = self.configure_session

    @property
    def configure_session(self) -> Session:
        '''
        DOCSTRING: CONFIGURES AN HTTP SESSION WITH RETRY MECHANISM AND EXPONENTIAL BACKOFF
        INPUTS: NONE
        OUTPUTS: CONFIGURED HTTP SESSION OBJECT
        OBS:
            1. RETRY_STRATEGY OVERVIEW:
            - TOTAL: TOTAL NUMBER OF RETRIES
            - BACKOFF_FACTOR: EXPONENTIAL BACKOFF FACTOR
                . CALCULATED AS THE DEALAY BEFORE THE NEXT RETRY
                . DELAY = BACKOFF_FACTOR * (2 ** (RETRY_NUMBER - 1))
                . AFTER THE 1ST RETRY: DELAY = 1 * 2**0 = 1 SECOND
                . AFTER THE 2ND RETRY: DELAY = 1 * 2**1 = 4 SECONDS
                . IN THE AFORE EXAMPLE THE BACKOFF FACTOR IS 1
            - STATUS_FORCELIST: LIST OF STATUS CODES TO RETRY
            2. SESSION OBJECT OVERVIEW:
            - MOUNT: MOUNTS THE RETRY STRATEGY TO THE SESSION, WITH THE GIVEN ADAPTER
            - SESSION OBJECTS HAVE METHODS AS .GET() AND .POST()
        '''
        retry_strategy = Retry(
            total=self.int_retries,
            backoff_factor=self.int_backoff_factor,
            status_forcelist=self.list_status_forcelist
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = Session()
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session