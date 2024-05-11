#! /home/v7por9/.pyenv/shims/python

import polars as pl
from pathlib import Path

# Reading parquet dataset
PARENT_PATH = Path.cwd()

# Final working dataset
WORKING_FILE_NAME = "wrking_Superstore.parquet"
WORKING_DATASET = PARENT_PATH / WORKING_FILE_NAME

dataset = pl.read_parquet(WORKING_DATASET.resolve())

# Finacial data reports.
# # Total sales by country

COLUMNS = ['country', 'sales', 'discount', 'profit']
sales_dataset = dataset.select(pl.col(COLUMNS))

# Group by country
sales_by_country = sales_dataset.group_by(
                                 "country").agg(pl.col(
                                                      'sales',
                                                      'discount',
                                                      'profit')
                                                .sum())

# Sorting by the profitable country
sales_by_country = sales_by_country.sort(by=pl.col("profit"), descending=True)

with pl.Config(
     tbl_cell_numeric_alignment="RIGHT",
     thousands_separator=True,
     float_precision=2,
     tbl_formatting="ASCII_MARKDOWN",
     tbl_hide_column_data_types=True,
     tbl_hide_dataframe_shape=True,
     ):

    print(sales_by_country.head(9))

