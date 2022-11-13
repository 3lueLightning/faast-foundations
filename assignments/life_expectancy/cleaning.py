"""
Cleaning object that performs an entire ETL on the selected file.
"""
from typing import Optional
from collections.abc import Iterable
import pandas as pd

from life_expectancy.constants import StrDict
from life_expectancy import config


class DataCleaner:
    def __init__(self, input_fn: str):
        self.input_fn: str = input_fn
        self.raw_df: Optional[pd.DataFrame] = None
        self.df: Optional[pd.DataFrame] = None

    def extract(self) -> None:
        self.raw_df = pd.read_csv(self.input_fn, sep="\t")

    def _reshape(self, id_vars: Iterable) -> None:
        expanded_index = self.raw_df.iloc[:, 0].str.split(',', expand=True)
        index_cols = self.raw_df.columns[0].replace("\\", ",").split(",")[:-1]
        expanded_index.columns = index_cols
        year_values = self.raw_df.iloc[:, 1:]
        year_values.columns = year_values.columns.str.strip()
        years = year_values.columns
        expanded_df = pd.concat([expanded_index, year_values], axis=1)
        self.df = pd.melt(expanded_df, id_vars, years, var_name='year')

    def _rename(self, rename_cols: Optional[StrDict] = None):
        if rename_cols:
            self.df.rename(columns=rename_cols, inplace=True)

    def _filter(self) -> None:
        self.df = self.df[self.df.region == 'PT']

    def _reformat(self) -> None:
        value = self.df.value.str.extract('(\d+.\d)')
        self.df["value"] = value.astype(float)
        self.df = self.df[self.df.value.notnull()]

    def transform(self, id_vars: Iterable, rename_cols: Optional[StrDict] = None) -> None:
        self._reshape(id_vars)
        self._rename(rename_cols)
        self._filter()
        self._reformat()

    def load(self, output_fn: str) -> None:
        self.df.to_csv(output_fn, index=False)


def clean_data():
    data_cleaner = DataCleaner(config.EU_LIFE_EXPECTANCY_FN)
    data_cleaner.extract()
    data_cleaner.transform(['unit', 'sex', 'age', 'geo'], {'geo': 'region'})
    data_cleaner.load(config.PT_LIFE_EXPECTANCY_FN)


clean_data()

