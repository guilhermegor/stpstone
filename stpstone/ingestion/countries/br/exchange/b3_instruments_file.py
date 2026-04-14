"""B3 Instruments File BVBG.028.02 ingestion with temporary caching."""

from datetime import date
from io import BytesIO, StringIO
from logging import Logger
from pathlib import Path
import shutil
import tempfile
from typing import Optional, Union

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import Page as PlaywrightPage
from requests import Response, Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.countries.br.exchange._b3_search_by_trading_session_base import (
	ABCB3SearchByTradingSession,
)


class B3InstrumentsFile(ABCB3SearchByTradingSession):
	"""B3 Instruments File BVBG.028.02 InstrumentsFile with temporary caching."""

	def __init__(
		self,
		date_ref: Optional[date] = None,
		logger: Optional[Logger] = None,
		cls_db: Optional[Session] = None,
	) -> None:
		"""Initialize the ingestion class.

		Parameters
		----------
		date_ref : Optional[date], optional
		    The date of reference, by default None.
		logger : Optional[Logger], optional
		    The logger, by default None.
		cls_db : Optional[Session], optional
		    The database session, by default None.

		Returns
		-------
		None
		"""
		super().__init__(
			date_ref=date_ref,
			logger=logger,
			cls_db=cls_db,
			url="https://www.b3.com.br/pesquisapregao/download?filelist=IN{}.zip",
		)

		self.temp_dir = Path(tempfile.mkdtemp(prefix="b3_instruments_"))
		self.cls_create_log.log_message(
			self.logger,
			f"Created temporary directory: {self.temp_dir}",
			"info",
		)

	def run(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
		bool_insert_or_ignore: bool = False,
		dict_dtypes: Optional[dict[str, Union[str, int, float]]] = None,
		str_fmt_dt: str = "YYYY-MM-DD",
		cols_from_case: str = "pascal",
		cols_to_case: str = "upper_constant",
		str_table_name: str = "<COUNTRY>_<SOURCE>_<TABLE_NAME>",
	) -> Optional[pd.DataFrame]:
		"""Run the ingestion process with caching.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout, by default (12.0, 21.0).
		bool_verify : bool, optional
		    Whether to verify the SSL certificate, by default True.
		bool_insert_or_ignore : bool, optional
		    Whether to insert or ignore the data, by default False.
		dict_dtypes : Optional[dict[str, Union[str, int, float]]], optional
		    Data types mapping, by default None.
		str_fmt_dt : str, optional
		    Date format string, by default "YYYY-MM-DD".
		cols_from_case : str, optional
		    Source column case format, by default "pascal".
		cols_to_case : str, optional
		    Target column case format, by default "upper_constant".
		str_table_name : str, optional
		    The name of the table, by default "<COUNTRY>_<SOURCE>_<TABLE_NAME>".

		Returns
		-------
		Optional[pd.DataFrame]
		    The transformed DataFrame.
		"""
		file, file_name = self.get_cached_or_fetch(timeout=timeout, bool_verify=bool_verify)
		df_ = self.transform_data(file=file, file_name=file_name)
		df_ = self.standardize_dataframe(
			df_=df_,
			date_ref=self.date_ref,
			dict_dtypes=dict_dtypes,
			str_fmt_dt=str_fmt_dt,
			url=self.url,
			cols_from_case=cols_from_case,
			cols_to_case=cols_to_case,
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

	def get_cached_or_fetch(
		self,
		timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (
			12.0,
			21.0,
		),
		bool_verify: bool = True,
	) -> tuple[StringIO, str]:
		"""Get XML content from cache or fetch from server.

		Parameters
		----------
		timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
		    The timeout, by default (12.0, 21.0).
		bool_verify : bool, optional
		    Verify the SSL certificate, by default True.

		Returns
		-------
		tuple[StringIO, str]
		    The XML content and file name.
		"""
		try:
			return self._load_from_cache()
		except ValueError:
			self.cls_create_log.log_message(
				self.logger,
				f"Cache miss, fetching from server: {self.url}",
				"warning",
			)
			resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
			return self.parse_raw_file(resp_req)

	def _load_from_cache(self) -> tuple[StringIO, str]:
		"""Load XML content from cache if available.

		Returns
		-------
		tuple[StringIO, str]
		    Cached XML content and file name.

		Raises
		------
		ValueError
		    If cache file is not found or fails to load.
		"""
		cache_path = self._get_cached_file_path()
		if not cache_path.exists():
			raise ValueError("Cache file not found.")
		self.cls_create_log.log_message(
			self.logger,
			f"Loading XML from cache: {cache_path}",
			"info",
		)
		try:
			with open(cache_path, encoding="utf-8") as f:
				content = f.read()
			return StringIO(content), cache_path.name
		except Exception as e:
			self.cls_create_log.log_message(
				self.logger,
				f"Failed to load from cache: {e}",
				"error",
			)
			raise ValueError(f"Failed to load from cache. Error: {e}") from e

	def parse_raw_file(
		self,
		resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
	) -> tuple[StringIO, str]:
		"""Parse the raw file content and cache it.

		Parameters
		----------
		resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
		    The response object.

		Returns
		-------
		tuple[StringIO, str]
		    The parsed content and file name.
		"""
		file_io, file_name = self.cls_dir_files_management.recursive_unzip_in_memory(
			BytesIO(resp_req.content)
		)[0]

		xml_content = file_io.getvalue()
		try:
			xml_content_str = xml_content.decode("utf-8")
		except UnicodeDecodeError:
			xml_content_str = xml_content.decode("latin-1")

		self._save_to_cache(xml_content_str)

		file_io.seek(0)
		string_io = StringIO(xml_content_str)
		return string_io, file_name

	def _save_to_cache(self, xml_content: str) -> None:
		"""Save XML content to cache.

		Parameters
		----------
		xml_content : str
		    XML content to save.

		Returns
		-------
		None
		"""
		cache_path = self._get_cached_file_path()
		try:
			cache_path.parent.mkdir(parents=True, exist_ok=True)
			with open(cache_path, "w", encoding="utf-8") as f:
				f.write(xml_content)
			self.cls_create_log.log_message(
				self.logger,
				f"Saved XML to cache: {cache_path}",
				"info",
			)
		except Exception as e:
			self.cls_create_log.log_message(
				self.logger,
				f"Failed to save to cache: {e}",
				"warning",
			)

	def _get_cached_file_path(self) -> Path:
		"""Get the cached file path for the current date.

		Returns
		-------
		Path
		    Path to the cached XML file.
		"""
		filename = f"instruments_{self.date_ref.strftime('%y%m%d')}.xml"
		return self.temp_dir / filename

	def transform_data(
		self,
		file: StringIO,
		file_name: str,
		tag_parent: str = "",
		list_tags_children: Optional[list[str]] = None,
		list_tups_attributes: Optional[list[tuple[str, str]]] = None,
	) -> pd.DataFrame:
		"""Transform file content into a DataFrame.

		Parameters
		----------
		file : StringIO
		    The file content.
		file_name : str
		    The file name.
		tag_parent : str
		    Parent tag name.
		list_tags_children : Optional[list[str]]
		    List of child tags.
		list_tups_attributes : Optional[list[tuple[str, str]]]
		    List of tuples containing tag name and attribute name.

		Returns
		-------
		pd.DataFrame
		    The transformed DataFrame.
		"""
		soup_xml = self.cls_xml_handler.memory_parser(file)
		list_ser = self._get_node_info(
			soup_xml=soup_xml,
			tag_parent=tag_parent,
			list_tags_children=list_tags_children or [],
			list_tups_attributes=list_tups_attributes,
		)
		df_ = pd.DataFrame(list_ser)
		df_["FILE_NAME"] = file_name
		return df_

	def _get_node_info(
		self,
		soup_xml: BeautifulSoup,
		tag_parent: str,
		list_tags_children: list[str],
		list_tups_attributes: Optional[list[tuple[str, str]]],
	) -> list[dict[str, Union[str, int, float]]]:
		"""Get node information from BeautifulSoup XML.

		Parameters
		----------
		soup_xml : BeautifulSoup
		    Parsed XML document.
		tag_parent : str
		    Parent tag name.
		list_tags_children : list[str]
		    List of child tags.
		list_tups_attributes : Optional[list[tuple[str, str]]]
		    List of tuples containing tag name and attribute name.

		Returns
		-------
		list[dict[str, Union[str, int, float]]]
		    List of dictionaries containing node information.
		"""
		soup_node = self.cls_xml_handler.find_all(soup_xml=soup_xml, tag=tag_parent)
		list_ser: list[dict[str, Union[str, int, float]]] = []

		for soup_parent in soup_node:
			dict_ = {}
			for tag in list_tags_children:
				soup_content_tag = soup_parent.find(tag)
				dict_[tag] = soup_content_tag.getText() if soup_content_tag else None
			if list_tups_attributes:
				for tag, attribute in list_tups_attributes:
					soup_content_tag = soup_parent.find(tag)
					dict_[tag + attribute] = (
						soup_content_tag.get(attribute) if soup_content_tag else None
					)
			list_ser.append(dict_)

		return list_ser

	def cleanup_cache(self) -> None:
		"""Clean up the temporary directory and all cached files.

		Returns
		-------
		None
		"""
		try:
			if self.temp_dir.exists():
				shutil.rmtree(self.temp_dir)
				self.cls_create_log.log_message(
					self.logger,
					f"Cleaned up temporary directory: {self.temp_dir}",
					"info",
				)
		except Exception as e:
			self.cls_create_log.log_message(
				self.logger,
				f"Failed to cleanup cache: {e}",
				"warning",
			)
