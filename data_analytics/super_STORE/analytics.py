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
                        'profit', 'shipping_cost', 'region']

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
                                                     'profit',
                                                     'shipping_cost')
                                               .sum())

        # Sorting by the profitable country
        country_sales = country_sales.sort(by=pl.col("profit"),
                                           descending=True)
        return country_sales

    def sales_expense(self):
        # Aggregating the discount and shipping costs
        sales_expense_by_country = self.dataset.group_by(
                                            "country").agg(pl.col(
                                                               'discount',
                                                               'shipping_cost'
                                                           ).sum())
        # Sorting the data based on shipping cost.
        sales_expense_by_country = sales_expense_by_country.sort(
                                                    by=pl.col("shipping_cost"),
                                                    descending=True)

        return sales_expense_by_country

    def sales_by_region(self):
        # Generating sales report based on the regions
        regional_sales = self.dataset.group_by(
                                "region").agg(pl.col(
                                                 'sales'
                                              ).sum())
        regional_sales = regional_sales.sort(by=pl.col('sales'),
                                             descending=True)
        return regional_sales
# Country Data Reporting
location_data = Location_Data()


class Product_Data:
    def __init__(self):
        self.columns = ['product_id', 'category', 'sub-category',
                        'product_name', 'sales', 'profit']

        self.dataset = dataset.select(pl.col(self.columns))
        return

    
with pl.Config(
     tbl_cell_numeric_alignment="RIGHT",
     thousands_separator=True,
     float_precision=2,
     tbl_formatting="ASCII_MARKDOWN",
     tbl_hide_column_data_types=True,
     tbl_hide_dataframe_shape=True,
     ):

    print(location_data.sales_by_country().head(9))
