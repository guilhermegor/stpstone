"""B3 Trading Hours for Options Exercise."""

from datetime import date
from logging import Logger
from typing import Optional, Union

from lxml.html import HtmlElement
import pandas as pd
from requests import Session

from stpstone.ingestion.countries.br.exchange._b3_trading_hours_core import B3TradingHoursCore


class B3TradingHoursOptionsExercise(B3TradingHoursCore):
	"""B3 Trading Hours for Options Exercise."""

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
			url="https://www.b3.com.br/pt_br/solucoes/plataformas/puma-trading-system/para-participantes-e-traders/horario-de-negociacao/acoes/",
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_b3_trading_hours_options_exercise",
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
		    The name of the table, by default "br_b3_trading_hours_options_exercise".

		Returns
		-------
		Optional[pd.DataFrame]
		    The transformed DataFrame.
		"""
		return super().run(
			dict_dtypes={
				"MERCADO": str,
				"EXERCICIO_MANUAL_DE_POSICAO_TITULAR_ANTES_DO_VENCIMENTO_INICIO": str,
				"EXERCICIO_MANUAL_DE_POSICAO_TITULAR_ANTES_DO_VENCIMENTO_FIM": str,
				"EXERCICIO_MANUAL_DE_POSICAO_TITULAR_NO_VENCIMENTO_INICIO": str,
				"EXERCICIO_MANUAL_DE_POSICAO_TITULAR_NO_VENCIMENTO_FIM": str,
				"ARQUIVO_DE_POSICOES_MAIS_IMBARQ_NO_VENCIMENTO": str,
				"CONTRARY_EXERCISE_NO_VENCIMENTO_INICIO": str,
				"CONTRARY_EXERCISE_NO_VENCIMENTO_FIM": str,
				"EXERCICIO_AUTOMATICO_DE_POSICAO_TITULAR_NO_VENCIMENTO_INICIO": str,
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
				"EXERCICIO_MANUAL_DE_POSICAO_TITULAR_ANTES_DO_VENCIMENTO_INICIO",
				"EXERCICIO_MANUAL_DE_POSICAO_TITULAR_ANTES_DO_VENCIMENTO_FIM",
				"EXERCICIO_MANUAL_DE_POSICAO_TITULAR_NO_VENCIMENTO_INICIO",
				"EXERCICIO_MANUAL_DE_POSICAO_TITULAR_NO_VENCIMENTO_FIM",
				"ARQUIVO_DE_POSICOES_MAIS_IMBARQ_NO_VENCIMENTO",
				"CONTRARY_EXERCISE_NO_VENCIMENTO_INICIO",
				"CONTRARY_EXERCISE_NO_VENCIMENTO_FIM",
				"EXERCICIO_AUTOMATICO_DE_POSICAO_TITULAR_NO_VENCIMENTO_INICIO",
			],
			xpath_td='//*[@id="conteudo-principal"]/div[4]/div/div/table[2]/'
			+ "tbody/tr[position() > 1]/td",
			na_values="-",
		)
