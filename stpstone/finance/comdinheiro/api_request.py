### API COMDINHEIRO - MÉTODO POST PARA REALIZAÇÃO DE REQUISIÇÕES ###

import requests


class ComDinheiro:

    def requests_api_cd(self, payload):
        """
        DOCSTRING: CONSULTA COMDINHEIRO
        INPUTS: URL ENCODED EM FORMATO LEGÍVEL PELO PYTHON, USAR A API DA EMPRESA:
            https://www.comdinheiro.com.br/ManualImportacaoAPI001.php?usuario=xp.rv&ferramenta=FundScreener001&linguagem=python&encode=1&formato=json2&consulta_especifica=ComparaEmpresas001.php%253F%2526data_d%253D31129999%2526data_a%253D16%252F06%252F2020%2526trailing%253D12%2526conv%253DMIXED%2526c_c%253Dconsolidado%252520preferencialmente%2526moeda%253DMOEDA_ORIGINAL%2526m_m%253D1000000000%2526n_c%253D2%2526f_v%253D1%2526papeis%253DWEGE3%252BMDIA3%252BBBAS3%252BITSA4%2526indic%253DNEGOCIABILIDADE%2827~03~2020%252C17~06~2020%252C%252C%252C2%29%2526enviar_email%253D0%2526enviar_email_log%253D0%2526transpor%253D0%2526op01%253Dtabela%2526oculta_cabecalho_sup%253D0%2526relat_alias_automatico%253Dcmd_alias_01&gerar_codigo=1&
        """
        url = "https://www.comdinheiro.com.br/Clientes/API/EndPoint001.php?code=import_data"
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        response = requests.request(
            "POST", url, headers=headers, data=payload)
        return response
