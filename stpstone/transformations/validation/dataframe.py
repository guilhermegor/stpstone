import pandas as pd
from logging import Logger, Tuple
from numbers import Number
from typing import Dict, List, Optional
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.pipelines.generic import generic_pipeline


class DataFrameValidator:

    def __init__(self, df_: pd.DataFrame, logger: Logger):
        self.df_ = df_.copy()
        self.logger = logger
        self.create_log = CreateLog()

    @property
    def check_missing_values(self):
        missing_summary = self.df_.isnull().sum()
        self.create_log.warning(
            self.logger, "Missing Values:\n", missing_summary[missing_summary > 0]
        )

    @property
    def check_duplicates(self):
        duplicate_count = self.df_.duplicated().sum()
        if duplicate_count:
            self.create_log.warning(
                self.logger, f"Found {duplicate_count} duplicate rows."
            )

    def validate_ranges(
        self, dict_rng_constraints: Dict[str, Tuple[Number, Number]]
    ) -> None:
        for col, (low, high) in dict_rng_constraints.items():
            out_of_bounds = self.df_[(self.df_[col] < low) | (self.df_[col] > high)]
            if not out_of_bounds.empty:
                self.create_log.warning(
                    self.logger,
                    f"{len(out_of_bounds)} values in '{col}' out of range ({low} - {high})",
                )

    def validate_dates(self, col_start: str, col_sup: str) -> None:
        list_inv_dates = self.df_[self.df_[col_start] > self.df_[col_sup]]
        if not list_inv_dates.empty:
            self.create_log.warning(
                self.logger,
                f"Found {len(list_inv_dates)} rows where {col_start} is after {col_sup}.",
            )

    def validate_categorical_values(
        self, col_: Optional[str] = None, list_allowed_values: Optional[str] = None
    ):
        list_inv_values = self.df_[~self.df_[col_].isin(list_allowed_values)]
        if not list_inv_values.empty:
            self.create_log.warning(
                self.logger,
                f"Found invalid values in '{col_}':",
                list_inv_values[col_].unique(),
            )

    def pipeline(
        self,
        dict_rng_constraints: Dict[str, Tuple[Number, Number]] = None,
        col_start: Optional[str] = None,
        col_sup: Optional[str] = None,
        list_tup_categorical_constraints: Optional[Tuple[str, List[str]]] = None,
    ):
        steps = [
            self.check_missing_values,
            self.check_duplicates,
        ]
        if dict_rng_constraints:
            steps.append(self.validate_ranges(dict_rng_constraints))
        if col_start and col_sup:
            steps.append(self.validate_dates(col_start, col_sup))
        if list_tup_categorical_constraints:
            col_, allowed_values = list_tup_categorical_constraints
            steps.append(self.validate_categorical_values(col_, allowed_values))
        return generic_pipeline(self.df_, steps)
