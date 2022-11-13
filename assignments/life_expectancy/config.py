"""
The configuration variables for the entire project
"""
from pathlib import Path

MAIN_DIR = Path('life_expectancy')
DATA_DIR = MAIN_DIR / 'data'
EU_LIFE_EXPECTANCY_FN = DATA_DIR / 'eu_life_expectancy_raw.tsv'
PT_LIFE_EXPECTANCY_FN = DATA_DIR / 'pt_life_expectancy.csv'
