#! /home/v7por9/.pyenv/shims/python

import polars as pl

# Local Import
from analytics import Location_Data
from analytics import Product_Data
from analytics import Market_Data

with pl.Config(
     tbl_cell_numeric_alignment="RIGHT",
     thousands_separator=True,
     float_precision=2,
     tbl_formatting="ASCII_MARKDOWN",
     tbl_hide_column_data_types=True,
     tbl_hide_dataframe_shape=True,
     ):

    location = Location_Data()
    product = Product_Data()
    market = Market_Data()

    print(location.totals())
    print(location.sales_by_country().head(9))
    print(location.sales_expense().head(9))
    print(location.sales_by_region())
    print(product.products_profitability().get('top_products'))
    print(product.products_profitability().get('least_products'))
    print(market.market_performance())
    print(market.segment())
    print(market.region().head(13))
    pass
