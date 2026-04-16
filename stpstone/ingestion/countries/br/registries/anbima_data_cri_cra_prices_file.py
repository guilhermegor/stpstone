"""Implementation of Anbima CRI/CRA prices file download ingestion instance."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Any, Optional

import pandas as pd
import requests
from requests import Response, Session

from stpstone.ingestion.abc.ingestion_abc import (
	ABCIngestionOperations,
	ContentParser,
	CoreIngestion,
)
from stpstone.transformations.validation.metaclass_type_checker import type_checker
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


class AnbimaDataCRICRAPricesFile(ABCIngestionOperations):
	"""Anbima CRI/CRA prices file download ingestion class."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the Anbima CRI/CRA prices file download ingestion class.

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

		Notes
		-----
		[1] Download URL: https://www.anbima.com.br/pt_br/anbima/TaxasCriCraExport/downloadExterno
		"""
		super().__init__(cls_db=cls_db)
		CoreIngestion.__init__(self)
		ContentParser.__init__(self)

		self.logger = logger
		self.cls_db = cls_db
		self.cls_dir_files_management = DirFilesManagement()
		self.cls_dates_current = DatesCurrent()
		self.cls_create_log = CreateLog()
		self.cls_dates_br = DatesBRAnbima()
		self.date_ref = date_ref or self.cls_dates_br.add_working_days(
			self.cls_dates_current.curr_date(), -1
		)
		self.download_url = (
			"https://www.anbima.com.br/pt_br/anbima/TaxasCriCraExport/downloadExterno"
		)

	def run(
		self,
		bool_insert_or_ignore: bool = False,
		str_table_name: str = "br_anbimadata_cri_cra_prices_file",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process.

		If the database session is provided, the data is inserted into the database.
		Otherwise, the transformed DataFrame is returned.

		Parameters
		----------
		bool_insert_or_ignore : bool
			Whether to insert or ignore the data, by default False
		str_table_name : str
			The name of the table, by default "br_anbimadata_cri_cra_prices_file"

		Returns
		-------
		Optional[pd.DataFrame]
			The transformed DataFrame.
		"""
		raw_data = self.get_response()
		df_ = self.transform_data(raw_data=raw_data)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes={
				"DATA_REFERENCIA": "date",
				"RISCO_CREDITO": str,
				"EMISSOR": str,
				"SERIE": str,  # codespell:ignore
				"EMISSAO": str,
				"CODIGO": str,
				"VENCIMENTO": "date",
				"INDICE_CORRECAO": str,
				"TAXA_COMPRA": str,
				"TAXA_VENDA": str,
				"TAXA_INDICATIVA": str,
				"DESVIO_PADRAO": str,
				"PU": str,
				"PCT_PU_PAR_PCT_VNE": str,
				"DURATION": str,
				"DATA_REFERENCIA_NTNB": "date",
				"PCT_REUNE": str,
			},
			str_fmt_dt="DD/MM/YYYY",
			url=self.download_url,
		)

		if self.cls_db:
			self.insert_table_db(
				cls_db=self.cls_db,
				str_table_name=str_table_name,
				df_=df_,
				bool_insert_or_ignore=bool_insert_or_ignore,
			)
		else:
			return df_

	def get_response(self) -> Response:
		"""Download CRI/CRA prices file from Anbima.

		Returns
		-------
		Response
			The response object containing the CSV file.
		"""
		self.cls_create_log.log_message(
			self.logger,
			f"🚀 Starting CRI/CRA prices file download from {self.download_url}...",
			"info",
		)

		try:
			headers = {
				"accept": "application/json, text/plain, */*",
				"accept-language": "en-US,en;q=0.9,pt;q=0.8,es;q=0.7",
				"origin": "https://data.anbima.com.br",
				"referer": "https://data.anbima.com.br/",
				"sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
				"sec-ch-ua-mobile": "?0",
				"sec-ch-ua-platform": '"Linux"',
				"sec-fetch-dest": "empty",
				"sec-fetch-mode": "cors",
				"sec-fetch-site": "same-site",
				"user-agent": (
					"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
					"(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
				),
			}

			response = requests.get(self.download_url, headers=headers, timeout=30)
			response.raise_for_status()

			self.cls_create_log.log_message(self.logger, "✅ File downloaded successfully", "info")

			return response

		except Exception as e:
			self.cls_create_log.log_message(
				self.logger, f"❌ Error downloading file: {str(e)}", "error"
			)
			raise

	def parse_raw_file(self, resp_req: Response) -> StringIO:
		"""Parse the raw CSV file content.

		Parameters
		----------
		resp_req : Response
			The response object containing the CSV file.

		Returns
		-------
		StringIO
			The parsed CSV content.
		"""
		try:
			content = resp_req.text

			self.cls_create_log.log_message(
				self.logger, f"📄 Parsing CSV content ({len(content)} characters)...", "info"
			)

			return StringIO(content)

		except Exception as e:
			self.cls_create_log.log_message(
				self.logger, f"❌ Error parsing file: {str(e)}", "error"
			)
			raise

	def transform_data(self, raw_data: Response) -> pd.DataFrame:
		"""Transform downloaded CSV data into a DataFrame.

		Parameters
		----------
		raw_data : Response
			The response object containing the CSV file.

		Returns
		-------
		pd.DataFrame
			The transformed DataFrame.
		"""
		try:
			csv_content = self.parse_raw_file(raw_data)

			df_raw = pd.read_csv(
				csv_content,
				sep=";",
				encoding="latin1",
				skiprows=0,
			)

			self.cls_create_log.log_message(
				self.logger,
				f"📊 Raw DataFrame: {len(df_raw)} rows, columns: {list(df_raw.columns)}",
				"info",
			)

			column_mapping = {}

			for col in df_raw.columns:
				col_clean = col.strip()

				if "Data de Refer" in col_clean and "NTN" not in col_clean:
					column_mapping[col] = "DATA_REFERENCIA"
				elif "Risco de Cr" in col_clean:
					column_mapping[col] = "RISCO_CREDITO"
				elif "Emissor" in col_clean:
					column_mapping[col] = "EMISSOR"
				elif "Série" in col_clean or "Serie" in col_clean:
					column_mapping[col] = "SERIE"
				elif "Emissão" in col_clean or "Emiss" in col_clean:
					column_mapping[col] = "EMISSAO"
				elif "Código" in col_clean or "Codigo" in col_clean:
					column_mapping[col] = "CODIGO"
				elif "Vencimento" in col_clean:
					column_mapping[col] = "VENCIMENTO"
				elif ("Índice" in col_clean or "Indice" in col_clean) and "Correção" in col_clean:
					column_mapping[col] = "INDICE_CORRECAO"
				elif "Taxa Compra" in col_clean:
					column_mapping[col] = "TAXA_COMPRA"
				elif "Taxa Venda" in col_clean:
					column_mapping[col] = "TAXA_VENDA"
				elif "Taxa Indicativa" in col_clean:
					column_mapping[col] = "TAXA_INDICATIVA"
				elif "Desvio" in col_clean and ("Padrão" in col_clean or "Padrao" in col_clean):
					column_mapping[col] = "DESVIO_PADRAO"
				elif col_clean.strip() in ["PU", " PU", "PU "]:
					column_mapping[col] = "PU"
				elif "% PU Par" in col_clean and "% VNE" in col_clean:
					column_mapping[col] = "PCT_PU_PAR_PCT_VNE"
				elif "Duration" in col_clean:
					column_mapping[col] = "DURATION"
				elif "NTN" in col_clean or ("Refer" in col_clean and "NTN" in col_clean):
					column_mapping[col] = "DATA_REFERENCIA_NTNB"
				elif "Reune" in col_clean or "% Reune" in col_clean:
					column_mapping[col] = "PCT_REUNE"

			self.cls_create_log.log_message(
				self.logger, f"📝 Column mapping: {column_mapping}", "info"
			)

			df_ = df_raw.rename(columns=column_mapping).copy()
			expected_columns = [
				"DATA_REFERENCIA",
				"RISCO_CREDITO",
				"EMISSOR",
				"SERIE",
				"EMISSAO",
				"CODIGO",
				"VENCIMENTO",
				"INDICE_CORRECAO",
				"TAXA_COMPRA",
				"TAXA_VENDA",
				"TAXA_INDICATIVA",
				"DESVIO_PADRAO",
				"PU",
				"PCT_PU_PAR_PCT_VNE",
				"DURATION",
				"DATA_REFERENCIA_NTNB",
				"PCT_REUNE",
			]
			missing_columns = [col for col in expected_columns if col not in df_.columns]

			if missing_columns:
				self.cls_create_log.log_message(
					self.logger, f"⚠️ Missing columns: {missing_columns}", "warning"
				)

				for col in missing_columns:
					df_[col] = None

			df_ = df_[expected_columns].copy()

			percentage_columns = [
				"TAXA_COMPRA",
				"TAXA_VENDA",
				"TAXA_INDICATIVA",
				"DESVIO_PADRAO",
				"PCT_REUNE",
			]

			for col in percentage_columns:
				if col in df_.columns:
					df_[col] = df_[col].astype(str).str.replace("%", "").str.strip()

			if "PCT_PU_PAR_PCT_VNE" in df_.columns:
				df_["PCT_PU_PAR_PCT_VNE"] = df_["PCT_PU_PAR_PCT_VNE"].astype(str).str.strip()

			date_columns = ["DATA_REFERENCIA", "VENCIMENTO", "DATA_REFERENCIA_NTNB"]
			for col in date_columns:
				if col in df_.columns:

					@type_checker
					def clean_date(val: Any) -> str:  # noqa ANN401: typing.Any not allowed
						"""Clean date values, replacing invalids with '01/01/2100'.

						Parameters
						----------
						val : Any
							The value to clean.

						Returns
						-------
						str
							The cleaned date value
						"""
						if pd.isna(val):
							return "01/01/2100"
						str_val = str(val).strip()
						if str_val in ["", "-", "nan", "None"]:
							return "01/01/2100"
						return str_val

					df_[col] = df_[col].apply(clean_date)

			self.cls_create_log.log_message(
				self.logger,
				f"✅ Data transformation complete: {len(df_)} records processed",
				"info",
			)

			return df_

		except Exception as e:
			self.cls_create_log.log_message(
				self.logger, f"❌ Error transforming data: {str(e)}", "error"
			)
			raise
