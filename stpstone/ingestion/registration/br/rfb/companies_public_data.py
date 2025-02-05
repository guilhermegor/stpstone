### EINS (EMPLOYER IDENTIFICATION NUMBER - CNPJ) - BRAZILLIAN FEDERAL REVENUE OFFICE (RFB) ###

# pypi.org libs
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session
from logging import Logger
# project modules
from stpstone._config._global_slots import YAML_RFB
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.connections.netops.session import ReqSession
from stpstone.ingestion.abc.requests import ABCRequests


class CompaniesRFB(ABCRequests):

    def __init__(
        self,
        bl_new_proxy:bool=False,
        bl_verify:bool=False,
        dt_ref:datetime=DatesBR().curr_date,
        session:Optional[ReqSession]=None, 
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None
    ) -> None:
        """
        Ein (Employer Identification Numb - acronym for CNPJ, in Portugues-BR) registry from 
            Brazillian Federal Reserver Office (RFB)
        Args:
            - dt_ref(datetime): reference date, current day as standard
        Return:
            None
        Metadata:
            https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf
            https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/
        """
        super().__init__(
            dict_metadata=YAML_RFB,
            bl_new_proxy=bl_new_proxy,
            bl_verify=bl_verify,
            str_host = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{}-{}/'.format(
                DatesBR().year_number(dt_ref),
                DatesBR().month_number(dt_ref, bl_month_mm=True)
            ),
            dt_ref=dt_ref,
            dict_headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'pt-BR,pt;q=0.9',
                'Connection': 'keep-alive',
                'Referer': 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-01/',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
                'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"'
            },
            dict_payload=None,
            session=session,
            cls_db=cls_db,
            logger=logger
        )

