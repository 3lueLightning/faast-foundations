"""
Cleaning object that performs an entire ETL on the selected file.
"""
import argparse
from typing import Optional
from collections.abc import Iterable
import pandas as pd

from life_expectancy.constants import StrDict
from life_expectancy import config


class DataCleaner:
    """
    Object performs an ETL on a file with the following characteristics:
        1) a single index column containing a concatenation of coman separated components
            ex: unit,sex,age,region
        2) numerical information for each year column
        3) have a region indication in the index
    The ETL reshapes the data to be in a format where the indexes are in different columns
    and the all years are in a single column along with the values.
    """
    def __init__(self, input_fn: str):
        self.input_fn: str = input_fn
        self.raw_df: Optional[pd.DataFrame] = None
        self.transformed_df: Optional[pd.DataFrame] = None

    # pylint: disable=C0116
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
        self.transformed_df = pd.melt(expanded_df, id_vars, years, var_name='year')

    def _rename(self, rename_cols: Optional[StrDict] = None):
        if rename_cols:
            self.transformed_df.rename(columns=rename_cols, inplace=True)

    def _filter(self, region: list[str]) -> None:
        self.transformed_df = self.transformed_df[self.transformed_df.region.isin(region)]

    def _reformat(self) -> None:
        value = self.transformed_df.value.str.extract(r'(\d+.\d)')
        self.transformed_df["value"] = value.astype(float)
        self.transformed_df = self.transformed_df[self.transformed_df.value.notnull()]

    # pylint: disable=C0116
    def transform(
            self,
            id_vars: Iterable,
            region: list[str],
            rename_cols: Optional[StrDict] = None) -> None:
        self._reshape(id_vars)
        self._rename(rename_cols)
        self._filter(region)
        self._reformat()

    # pylint: disable=C0116
    def load(self, output_fn: str) -> None:
        self.transformed_df.to_csv(output_fn, index=False)


def clean_data(region: list[str] = 'PT') -> None:
    """
    Perform an ETL on the EU life expectancy file
    """
    data_cleaner = DataCleaner(config.EU_LIFE_EXPECTANCY_FN)
    data_cleaner.extract()
    data_cleaner.transform(
        ['unit', 'sex', 'age', 'geo'],
        region,
        {'geo': 'region'})
    data_cleaner.load(config.PT_LIFE_EXPECTANCY_FN)


# pylint: disable=C0116
def parse_cli_args() -> dict:
    description = """
        Reads a file with lifetime expectancy per year, unit, sex, age, geo, unpivots it and saves to a new files.
        """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-b', action='store_const', const=42)
    parser.add_argument(
        "-r",
        "--region",
        type=str,
        default=['PT'],
        required=False,
        nargs='+',
        help="list of countries of the export in the final file"
    )
    return vars(parser.parse_args())


if __name__ == "__main__":  # pragma: no cover
    args = parse_cli_args()
    clean_data(args['region'])
