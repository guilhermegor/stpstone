"""B3 Trading Hours for OTC (Balcao Organizado)."""

from datetime import date
from logging import Logger
from typing import Optional, Union

from lxml.html import HtmlElement
import pandas as pd
from requests import Session

from stpstone.ingestion.countries.br.exchange._b3_trading_hours_core import B3TradingHoursCore


class B3TradingHoursOTC(B3TradingHoursCore):
	"""B3 Trading Hours for OTC."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the ingestion class.

		Parameters
		----------
		date_ref : Optional[date]
		    The date of reference, by default None.
		logger : Optional[Logger]
		    The logger, by default None.
		cls_db : Optional[Session]
		    The database session, by default None.

		Returns
		-------
		None
		"""
		super().__init__(
			date_ref=date_ref,
			logger=logger,
			cls_db=cls_db,
			url="https://www.b3.com.br/pt_br/solucoes/plataformas/puma-trading-system/para-participantes-e-traders/horario-de-negociacao/balcao-organizado/",
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_trading_hours_exchange_otc",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
		    The timeout, by default (12.0, 21.0).
		bool_verify : bool
		    Whether to verify the SSL certificate, by default True.
		bool_insert_or_ignore : bool
		    Whether to insert or ignore the data, by default False.
		str_table_name : str
		    The name of the table, by default "br_b3_trading_hours_exchange_otc".

		Returns
		-------
		Optional[pd.DataFrame]
		    The transformed DataFrame.
		"""
		return super().run(
			dict_dtypes={
				"MERCADO": str,
				"CANCELAMENTO_DE_OFERTAS_INICIO": str,
				"CANCELAMENTO_DE_OFERTAS_FIM": str,
				"PRE_ABERTURA_INICIO": str,
				"PRE_ABERTURA_FIM": str,
				"NEGOCIACAO_INICIO": str,
				"NEGOCIACAO_FIM": str,
				"EXERCICIO_DE_OPCOES_ANTES_DO_VENCIMENTO_EXERCICIO_POSICAO_TITULAR_INICIO": str,
				"EXERCICIO_DE_OPCOES_ANTES_DO_VENCIMENTO_EXERCICIO_POSICAO_TITULAR_FIM": str,
				"EXERCICIO_DE_OPCOES_NO_VENCIMENTO_ENCERRAMENTO_POSICAO_INICIO": str,
				"EXERCICIO_DE_OPCOES_NO_VENCIMENTO_ENCERRAMENTO_POSICAO_FIM": str,
				"EXERCICIO_DE_OPCOES_NO_VENCIMENTO_EXERCICIO_DE_POSICAO_TITULAR_INICIO": str,
				"EXERCICIO_DE_OPCOES_NO_VENCIMENTO_EXERCICIO_DE_POSICAO_TITULAR_FIM": str,
				"CALL_DE_FECHAMENTO_INICIO": str,
				"CALL_DE_FECHAMENTO_FIM": str,
				"CALL_DE_FECHAMENTO_CANCELAMENTO_DE_OFERTAS_INICIO": str,
				"CALL_DE_FECHAMENTO_CANCELAMENTO_DE_OFERTAS_FIM": str,
				"AFTER_MARKET_NEGOCIACAO_INICIO": str,
				"AFTER_MARKET_NEGOCIACAO_FIM": str,
			},
			timeout=timeout,
			bool_verify=bool_verify,
			bool_insert_or_ignore=bool_insert_or_ignore,
			str_table_name=str_table_name,
		)

	def transform_data(
		self,
		html_root: HtmlElement,
	) -> pd.DataFrame:
		"""Transform a list of response objects into a DataFrame.

		Parameters
		----------
		html_root : HtmlElement
		    The root element of the HTML document.

		Returns
		-------
		pd.DataFrame
		    The transformed DataFrame.
		"""
		return super().transform_data(
			html_root=html_root,
			list_th=[
				"MERCADO",
				"CANCELAMENTO_DE_OFERTAS_INICIO",
				"CANCELAMENTO_DE_OFERTAS_FIM",
				"PRE_ABERTURA_INICIO",
				"PRE_ABERTURA_FIM",
				"NEGOCIACAO_INICIO",
				"NEGOCIACAO_FIM",
				"EXERCICIO_DE_OPCOES_ANTES_DO_VENCIMENTO_EXERCICIO_POSICAO_TITULAR_INICIO",
				"EXERCICIO_DE_OPCOES_ANTES_DO_VENCIMENTO_EXERCICIO_POSICAO_TITULAR_FIM",
				"EXERCICIO_DE_OPCOES_NO_VENCIMENTO_ENCERRAMENTO_POSICAO_INICIO",
				"EXERCICIO_DE_OPCOES_NO_VENCIMENTO_ENCERRAMENTO_POSICAO_FIM",
				"EXERCICIO_DE_OPCOES_NO_VENCIMENTO_EXERCICIO_DE_POSICAO_TITULAR_INICIO",
				"EXERCICIO_DE_OPCOES_NO_VENCIMENTO_EXERCICIO_DE_POSICAO_TITULAR_FIM",
				"CALL_DE_FECHAMENTO_INICIO",
				"CALL_DE_FECHAMENTO_FIM",
				"CALL_DE_FECHAMENTO_CANCELAMENTO_DE_OFERTAS_INICIO",
				"CALL_DE_FECHAMENTO_CANCELAMENTO_DE_OFERTAS_FIM",
				"AFTER_MARKET_NEGOCIACAO_INICIO",
				"AFTER_MARKET_NEGOCIACAO_FIM",
			],
			xpath_td='//*[@id="conteudo-principal"]/div[4]/div/div/table/tbody/tr/td',
			na_values="-",
		)
