from unittest import TestCase, main
import pandas as pd
from stpstone.ingestion.countries.br.macroeconomics.yahii_rates import YahiiRatesBRMacro
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.cals.handling_dates import DatesBR


class TestYahiiRatesBRMacroPact(TestCase):
    """
    Tests for the YahiiRatesBRMacro class, focusing on data integrity
    and comparison with a control file.
    """

    FETCH_FRESH_DATA = True

    def setUp(self):
        """Load actual Yahii data and the control DataFrame for comparison."""
        if self.FETCH_FRESH_DATA:
            dt_ref = DatesBR().sub_working_days(DatesBR().curr_date, 1)
            self.yahii_instance = YahiiRatesBRMacro(session=None, dt_ref=dt_ref, cls_db=None)
            self.df_actual = self.yahii_instance.source("pmi_rf_rates", bl_fetch=True)
            self.df_actual.to_excel(
                f'data/yahii-pmi-rf-rates_{DatesBR().curr_date.strftime("%Y%m%d")}_{DatesBR().curr_time.strftime("%H%M%S")}.xlsx',
                index=False
            )
        else:
            self.df_actual = pd.read_excel(DirFilesManagement().choose_last_saved_file_w_rule(
                "data/", "yahii-pmi-rf-rates_*.xlsx"), decimal=",")
        self.df_actual["VALUE"] = self.df_actual["VALUE"].astype(float).round(2)
        self.df_actual_filtered = self.df_actual[
            self.df_actual["YEAR"].between(2020, 2024)
        ].reset_index(drop=True)
        self.df_control = pd.read_excel(DirFilesManagement().choose_last_saved_file_w_rule(
            "data", "yahii-pmi-rf-rates-control_*.xlsx"), decimal=",")
        self.list_expected_columns = [
            "INT_START", "INT_Y", "INT_M", "INT_COLS_TBL", "INT_STEP_TBL_BEGIN", 
            "INT_YEAR_MIN", "INT_START_ACC", "INT_NUM_TBL", "I_Y", "I_M", 
            "INT_START_TBL", "INDEX_TD", "YEAR", "MONTH", "VALUE", 
            "ECONOMIC_INDICATOR", "URL", "REF_DATE", "LOG_TIMESTAMP", "SLUG_URL"
        ]
        self.df_control["VALUE"] = self.df_control["VALUE"].astype(float).round(2)

    def test_column_presence(self):
        """Ensure all expected columns are present in the actual DataFrame."""
        self.assertTrue(all(col in self.df_actual.columns for col in self.list_expected_columns))

    def test_no_null_values(self):
        """Ensure there are no null values in the relevant columns of the actual DataFrame."""
        for col in self.list_expected_columns:
            self.assertFalse(self.df_actual[col].isnull().any(), 
                             f"Column '{col}' contains null values.")

    def test_year_range_filtering(self):
        """Ensure only years 2020-2024 are in the filtered DataFrame."""
        years = self.df_actual_filtered["YEAR"].unique()
        self.assertTrue(all(2020 <= year <= 2024 for year in years))

    def test_value_equality_control_vs_actual(self):
        """Check equality of filtered values (up to 2 decimal places) against the control DataFrame."""
        df_merged = pd.merge(
            self.df_control,
            self.df_actual_filtered,
            on=["YEAR", "MONTH", "ECONOMIC_INDICATOR"],
            suffixes=("_control", "_actual")
        )
        self.assertGreater(len(df_merged), 0, "No common entries found to compare")
        for _, row in df_merged.iterrows():
            with self.subTest(year=row["YEAR"], month=row["MONTH"], 
                              indicator=row["ECONOMIC_INDICATOR"]):
                self.assertAlmostEqual(row["VALUE_control"], row["VALUE_actual"], places=2)

if __name__ == "__main__":
    main()