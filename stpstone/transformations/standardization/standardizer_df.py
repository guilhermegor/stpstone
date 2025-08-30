from contextlib import suppress
from logging import Logger
from typing import Any, Literal, Optional

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, StandardScaler

from stpstone._config.global_slots import YAML_GEN
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker, type_checker
from stpstone.utils.calendars.calendar_abc import DateFormatter
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.lists import ListHandler
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.pipelines.generic import generic_pipeline


pd.set_option('future.no_silent_downcasting', True)

class DFStandardization(metaclass=TypeChecker):
    """Class for standardizing DataFrames."""

    def __init__(
        self,
        dict_dtypes: dict[str, Any],
        cols_from_case: Optional[str] = None,
        cols_to_case: Optional[str] = None,
        list_cols_drop_dupl: list[str] = None,
        dict_fillna_strt: Optional[dict[str, str]] = None,
        str_fmt_dt: str = "YYYY-MM-DD",
        type_error_action: str = "raise",
        strategy_keep_when_dupl: str = "first",
        str_data_fillna: str = "-99999",
        str_dt_fillna: Optional[str] = None,
        str_tz: str = "UTC",
        encoding: str = "latin-1",
        bool_debug: bool = False,
        logger: Optional[Logger] = None,
    ) -> None:
        if not dict_dtypes:
            raise ValueError("dict_dtypes cannot be empty")
        self.dict_dtypes = dict_dtypes
        self.cols_from_case = cols_from_case
        self.cols_to_case = cols_to_case
        self.list_cols_drop_dupl = list_cols_drop_dupl or []
        self.dict_fillna_strt = dict_fillna_strt if dict_fillna_strt is not None else {}
        self.str_fmt_dt = str_fmt_dt
        self.type_error_action = type_error_action
        self.strategy_keep_when_dupl = strategy_keep_when_dupl
        self.str_data_fillna = str_data_fillna
        self.str_dt_fillna = str_dt_fillna if str_dt_fillna is not None else (
            '2099-12-31' if self.str_fmt_dt == 'YYYY-MM-DD' else
            '31/12/2099' if self.str_fmt_dt == 'DD/MM/YYYY' else
            '31/12/99' if self.str_fmt_dt == 'DD/MM/YY' else
            '31122099' if self.str_fmt_dt == 'DDMMYYYY' else
            '311299' if self.str_fmt_dt == 'DDMMYY' else
            '20991231' if self.str_fmt_dt == 'YYYYMMDD' else
            4102358400 if self.str_fmt_dt == 'unix_ts' else
            '31122099'
        )
        self.list_cols_dt = [key for key, value in dict_dtypes.items() if value == "date"]
        self.str_tz = str_tz
        self.encoding = encoding
        self.bool_debug = bool_debug
        self.logger = logger
        self.cls_dates = DateFormatter()

    def check_if_empty(self, df_: pd.DataFrame) -> pd.DataFrame:
        if df_.empty:
            raise ValueError("DataFrame is empty")
        return df_

    def clean_encoding_issues(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Clean encoding issues in the DataFrame."""
        @type_checker
        def clean_cell(x: Any) -> Any:
            if isinstance(x, str):
                x = StrHandler().remove_diacritics(x)
                x = x.encode('ascii', errors='ignore').decode('ascii')
            return x
        df_ = df_.copy()
        for col in df_.select_dtypes(include=["object"]).columns:
            df_[col] = df_[col].apply(clean_cell)
        return df_

    def coluns_names_case(self, df_: pd.DataFrame) -> pd.DataFrame:
        list_valid_cases = ["camel", "pascal", "snake", "kebab", 
                            "upper_constant", "lower_constant", "upper_first"]
        if self.cols_from_case and self.cols_to_case and \
           self.cols_from_case in list_valid_cases and self.cols_to_case in list_valid_cases:
            list_cols = [
                StrHandler().convert_case(
                    StrHandler().remove_diacritics(col_),
                    self.cols_from_case,
                    self.cols_to_case,
                )
                for col_ in list(df_.columns)
            ]
            df_.columns = list_cols
        return df_

    def limit_columns_to_dtypes(self, df_: pd.DataFrame) -> pd.DataFrame:
        if self.logger is not None:
            CreateLog().log_message(
                self.logger,
                f"list cols dataframe before filtering: {list(df_.columns)}",
                "info"
            )
            CreateLog().log_message(
                self.logger, 
                f"list cols to filter: {list(self.dict_dtypes.keys())}",
                "info"
            )
            CreateLog().log_message(
                self.logger,
                f"list of columns excluded: {[x for x in list(df_.columns) if x not in list(self.dict_dtypes.keys())]}",
                "info"
            )
        if self.bool_debug:
            print(f"list cols dataframe before filtering: {list(df_.columns)}")
            print(f"list cols to filter: {list(self.dict_dtypes.keys())}")
            print(f"list of columns excluded: {[x for x in list(df_.columns) if x not in list(self.dict_dtypes.keys())]}")
        list_cols = list(self.dict_dtypes.keys())
        df_ = df_[list_cols]
        return df_

    def delete_empty_rows(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Delete empty rows from the DataFrame."""
        list_mask = df_.apply(lambda row: row.isin([None, "", np.nan]).all(), axis=1)
        df_cleaned = df_[~list_mask]
        return df_cleaned

    def filler(self, df_: pd.DataFrame) -> pd.DataFrame:
        df_ = df_.replace("", np.nan)
        list_cols = [col_ for col_ in list(df_.columns) if col_ not in self.list_cols_dt]
        if self.dict_fillna_strt:
            for col_, strt_fill_na in self.dict_fillna_strt.items():
                if col_ not in list_cols:
                    continue
                elif strt_fill_na == "ffill":
                    df_[col_] = df_[col_].ffill()
                elif strt_fill_na == "bfill":
                    df_[col_] = df_[col_].bfill()
                else:
                    raise ValueError(f"Invalid fillna strategy: {strt_fill_na}")
        if self.list_cols_dt:
            df_[self.list_cols_dt] = df_[self.list_cols_dt].fillna(self.str_dt_fillna)
        df_[list_cols] = df_[list_cols].fillna(self.str_data_fillna)
        return df_

    def replace_num_delimiters(self, df_: pd.DataFrame) -> pd.DataFrame:
        list_cols_numerical = [
            k for k, v in self.dict_dtypes.items()
            if v in ["int64", "float64", "int32", "float32", int, float, "int", "float"]
        ]
        for col_ in list_cols_numerical:
            if col_ in df_.columns and df_[col_].dtype == "object":
                df_[col_] = df_[col_].str.replace(r"\.", "", regex=True)
                df_[col_] = df_[col_].str.replace(",", ".", regex=True)
                df_[col_] = pd.to_numeric(df_[col_], errors="coerce")
        return df_

    def change_dtypes(self, df_: pd.DataFrame) -> pd.DataFrame:
        df_ = df_.copy()
        dict_dtypes = {
            k: ("str" if v == "date" else v) for k, v in self.dict_dtypes.items()
        }
        df_ = df_.astype(dict_dtypes, errors=self.type_error_action)
        for col_ in self.list_cols_dt:
            if col_ != YAML_GEN["audit_log_cols"]["ref_date"]:
                str_fmt_dt = self.str_fmt_dt
            else:
                str_fmt_dt = YAML_GEN["audit_log_cols"]["str_fmt_dt"]
            df_[col_] = df_[col_].apply(
                lambda x: (
                    self.cls_dates.str_date_to_datetime(x, str_fmt_dt)
                    if pd.notnull(x) and x != self.str_dt_fillna
                    else pd.Timestamp(self.str_dt_fillna)
                )
            )
        list_cols_missing = [
            c for c in df_.columns
            if c not in ListHandler().extend_lists(list(dict_dtypes.keys()), self.list_cols_dt)
        ]
        if list_cols_missing:
            df_[list_cols_missing] = df_[list_cols_missing].astype(str)
        return df_

    def strip_hidden_characters(self, df_: pd.DataFrame) -> pd.DataFrame:
        for col_ in [
            c for c in df_.select_dtypes(include=["object", "category"]).columns
            if c not in self.list_cols_dt
        ]:
            df_[col_] = df_[col_].str.replace(r"[\u200B\u200C\u200D\uFEFF]", "", regex=True)
        return df_

    def strip_data(self, df_: pd.DataFrame) -> pd.DataFrame:
        list_cols = [
            col_ for col_ in df_.select_dtypes(["object"]).columns
            if col_ not in self.list_cols_dt
        ]
        for col_ in list_cols:
            with suppress(AttributeError):
                df_[col_] = df_[col_].str.strip()
        return df_

    def remove_duplicated_cols(self, df_: pd.DataFrame) -> pd.DataFrame:
        df_ = df_.loc[:, ~df_.columns.duplicated(keep=self.strategy_keep_when_dupl)]
        return df_

    def data_remove_dupl(self, df_: pd.DataFrame) -> pd.DataFrame:
        if self.list_cols_drop_dupl:
            df_ = df_.drop_duplicates(
                subset=self.list_cols_drop_dupl, keep=self.strategy_keep_when_dupl
            )
        return df_

    def pipeline(self, df_: pd.DataFrame) -> pd.DataFrame:
        steps = [
            self.check_if_empty,
            self.clean_encoding_issues,
            self.coluns_names_case,
            self.limit_columns_to_dtypes,
            self.delete_empty_rows,
            self.filler,
            self.replace_num_delimiters,
            self.change_dtypes,
            self.strip_hidden_characters,
            self.strip_data,
            self.remove_duplicated_cols,
            self.data_remove_dupl,
        ]
        df_ = generic_pipeline(df_, steps)
        return df_


class DFStandardizationML(DFStandardization):
    """Class for standardizing data for machine learning."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    def handle_outliers(self, df_: pd.DataFrame, method: Literal["iqr", "zscore"] = "iqr") -> pd.DataFrame:
        df_ = df_.copy()
        numeric_cols = df_.select_dtypes(include=["number"]).columns
        if not numeric_cols.empty:
            df_[numeric_cols] = df_[numeric_cols].fillna(df_[numeric_cols].mean())
            for col_ in numeric_cols:
                if method == "iqr":
                    q1, q3 = df_[col_].quantile([0.25, 0.75])
                    iqr = q3 - q1
                    lower_bound, upper_bound = q1 - 1.5 * iqr, q3 + 1.5 * iqr
                    df_[col_] = np.clip(df_[col_], lower_bound, upper_bound)
                elif method == "zscore":
                    mean, std = df_[col_].mean(), df_[col_].std(ddof=1)
                    lower_bound, upper_bound = mean - 3 * std, mean + 3 * std
                    df_[col_] = np.clip(df_[col_], lower_bound, upper_bound)
        return df_

    def scale_numeric_data(self, df_: pd.DataFrame, method: Literal["minmax", "standard"] = "minmax") -> pd.DataFrame:
        numeric_cols = df_.select_dtypes(include=["number"]).columns
        if len(numeric_cols) == 0:
            return df_
        df_ = df_.copy()
        df_[numeric_cols] = df_[numeric_cols].fillna(0)
        if method == "minmax":
            scaler = MinMaxScaler()
        elif method == "standard":
            scaler = StandardScaler()
        else:
            raise ValueError(f"Invalid scaling method: {method}")
        df_[numeric_cols] = scaler.fit_transform(df_[numeric_cols])
        return df_

    def pipeline_ml(
        self,
        df_: pd.DataFrame,
        method_handle_outliers: str = "iqr",
        method_scale_numeric_data: str = "minmax",
        cols_from_case: Optional[str] = None,
        cols_to_case: Optional[str] = None,
        list_cols_drop_dupl: list[str] = None,
        str_fmt_dt: str = "YYYY-MM-DD",
    ) -> pd.DataFrame:
        self.cols_from_case = cols_from_case
        self.cols_to_case = cols_to_case
        self.list_cols_drop_dupl = list_cols_drop_dupl or []
        self.str_fmt_dt = str_fmt_dt
        df_ = self.pipeline(df_)
        df_ = self.handle_outliers(df_, method_handle_outliers)
        df_ = self.scale_numeric_data(df_, method_scale_numeric_data)
        return df_