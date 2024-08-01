
### VALIDATE BRAZILIAN DOCUMENT NUMBERS ###

from validate_docbr import CNPJ, CPF, CNH, CNS, PIS, TituloEleitoral, RENAVAM
from requests import request


class DocumentsNumbersBR:

    def __init__(self, array_numbers):
        self.array_numbers = array_numbers

    def validate(self, doc='CNPJ'):
        '''
        DOCSTRING: BOOLEAN TO VALDIATE WHETER OR NOT THE NUMBER CLASSIFIES A CORRECT ID FOR A GIVEN 
            DOCUMENT
        INPUTS: DOC NATURE
        OUTPUTS: BOOLEAN LIST
        '''
        # determine document of interest
        if doc.upper() == 'CNPJ':
            class_ = CNPJ
        elif doc.upper() == 'CPF':
            class_ = CPF
        elif doc.upper() == 'CNS':
            class_ = CNS
        elif doc.upper() == 'PIS':
            class_ = PIS
        elif doc.upper() == 'TITULO_ELEITORAL':
            class_ = TituloEleitoral
        elif doc.upper() == 'RENAVAM':
            class_ = RENAVAM
        elif doc.upper() == 'CNH':
            class_ = CNH
        else:
            raise Exception('Type of document provided not identified, please revisit '
                            + 'the variable doc. Given value: {}'.format(doc))
        # return valide/invalid reponse for each given number, provided its nature
        return class_.validate_list(self.array_numbers)

    def mask_number(self, doc='CNPJ'):
        '''
        DOCSTRING: MASK A LIST OF NUMBERS TO THE DOCUMENT FORMAT OF INTEREST
        INPUTS: DOC NATURE
        OUTPUTS: STRING LIST
        '''
        # passing variables
        list_nums_to_mask = list()
        # determine document of interest
        if doc.upper() == 'CNPJ':
            class_ = CNPJ
        elif doc.upper() == 'CPF':
            class_ = CPF
        elif doc.upper() == 'CNS':
            class_ = CNS
        elif doc.upper() == 'PIS':
            class_ = PIS
        elif doc.upper() == 'TITULO_ELEITORAL':
            class_ = TituloEleitoral
        elif doc.upper() == 'RENAVAM':
            class_ = RENAVAM
        elif doc.upper() == 'CNH':
            class_ = CNH
        else:
            raise Exception('Type of document provided not identified, please revisit '
                            + 'the variable doc. Given value: {}'.format(doc))
        # iterate through each instance and mask its format, given the nature of interest
        for num in self.array_numbers:
            list_nums_to_mask.append(class_.mask(num))
        # return masked value of interest
        return list_nums_to_mask

    @property
    def get_public_info_cnpj(self, url='https://receitaws.com.br/v1/cnpj/{}',
                             method='GET'):
        '''
        DOCSTRING: GET PUBLIC AVAILABLE BRAZILLIAN INTERNAL REVENUE SERVICE (IRS) REGARDING 
            A CNPJ (DOMESTIC DOCUMENT FOR COMPANIES)
        INPUTS: CNPJ NUMBER
        OUTPUTS: JSON
        '''
        return [request(method, url.format(cnpj_number)).json() for cnpj_number in
                self.array_numbers]
