
### VALIDATE BRAZILIAN DOCUMENTS NUMBERS ###

from validate_docbr import CNPJ, CPF, CNH, CNS, PIS, TituloEleitoral, RENAVAM
from requests import request
from typing import List


class DocumentsNumbersBR:

    def __init__(self, list_docs:List[str]):
        self.list_docs = list_docs

    def validate(self, doc:str = 'CNPJ') -> List[bool]:
        # determine document of interest
        if doc.upper() == 'CNPJ':
            class_ = CNPJ()
        elif doc.upper() == 'CPF':
            class_ = CPF()
        elif doc.upper() == 'CNS':
            class_ = CNS()
        elif doc.upper() == 'PIS':
            class_ = PIS()
        elif doc.upper() == 'TITULO_ELEITORAL':
            class_ = TituloEleitoral()
        elif doc.upper() == 'RENAVAM':
            class_ = RENAVAM()
        elif doc.upper() == 'CNH':
            class_ = CNH()
        else:
            raise Exception('Type of document provided not identified, please revisit '
                            + 'the variable doc. Given value: {}'.format(doc))
        # return valide/invalid reponse for each given number, provided its nature
        return class_.validate_list(self.list_docs)

    def mask_numbers(self, doc:str = 'CNPJ') -> List[str]:
        # passing variables
        list_nums_to_mask = list()
        # determine document of interest
        if doc.upper() == 'CNPJ':
            class_ = CNPJ()
        elif doc.upper() == 'CPF':
            class_ = CPF()
        elif doc.upper() == 'CNS':
            class_ = CNS()
        elif doc.upper() == 'PIS':
            class_ = PIS()
        elif doc.upper() == 'TITULO_ELEITORAL':
            class_ = TituloEleitoral()
        elif doc.upper() == 'RENAVAM':
            class_ = RENAVAM()
        elif doc.upper() == 'CNH':
            class_ = CNH()
        else:
            raise Exception('Type of document provided not identified, please revisit '
                            + 'the variable doc. Given value: {}'.format(doc))
        # iterate through each instance and mask its format, given the nature of interest
        for num in self.list_docs:
            list_nums_to_mask.append(class_.mask(num))
        # return masked value of interest
        return list_nums_to_mask

    @property
    def unmask_docs(self) -> List[str]:
        return [d.replace('.', '').replace('/', '').replace('-', '') for d in self.list_docs]

    @property
    def get_public_info_cnpj(self, url: str = 'https://receitaws.com.br/v1/cnpj/{}',
                             method: str = 'GET') -> List[dict]:
        return [request(method, url.format(cnpj_number)).json() for cnpj_number in
                self.list_docs]
