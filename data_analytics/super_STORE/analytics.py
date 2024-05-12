#! /home/v7por9/.pyenv/shims/python

import polars as pl
from pathlib import Path

# Reading parquet dataset
PARENT_PATH = Path.cwd()

# Final working dataset
WORKING_FILE_NAME = "wrking_Superstore.parquet"
WORKING_DATASET = PARENT_PATH / WORKING_FILE_NAME

dataset = pl.read_parquet(WORKING_DATASET.resolve())


class Location_Data:
    # Finacial data reports.
    # # Total sales by country
    def __init__(self):
        self.columns = ['country', 'sales', 'discount',
                        'profit', 'shipping_cost']

        self.dataset = dataset.select(pl.col(self.columns))

    def totals(self):
        # Total Sales, discounts, profit and shipping cost
        totals = self.dataset.select(pl.col(
                                         'sales',
                                         'discount',
                                         'profit',
                                         'shipping_cost'
                                     ).sum())
        return totals

    def sales_by_country(self):
        # Group by country
        country_sales = self.dataset.group_by(
                                         "country").agg(pl.col(
                                                     'sales',
                                                     'discount',
                                                     'profit')
                                               .sum())

        # Sorting by the profitable country
        country_sales = country_sales.sort(by=pl.col("profit"),
                                           descending=True)
        return country_sales


# Country Data Reporting
location_data = Location_Data()


with pl.Config(
     tbl_cell_numeric_alignment="RIGHT",
     thousands_separator=True,
     float_precision=2,
     tbl_formatting="ASCII_MARKDOWN",
     tbl_hide_column_data_types=True,
     tbl_hide_dataframe_shape=True,
     ):

    print(location_data.sales_by_country().head(9))
