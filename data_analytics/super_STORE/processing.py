#! /home/v7por9/.pyenv/shims/python

import polars as pl
from pathlib import Path

# Reading parquet dataset
PARENT_PATH = Path.cwd()
FILE_NAME = "global_Superstore.parquet"
FULL_PATHNAME = PARENT_PATH / FILE_NAME

dataset = pl.read_parquet(FULL_PATHNAME.resolve())

# Renaming dataset column header
new_column_name = (str(dataset.columns).
                   lower().replace(" ", "_").
                   replace(",_", ", "))

dataset.columns = eval(new_column_name)

# Defining columns based on use cases
ORDER_DETAILS = ['order_id', 'order_date', 'ship_date', 'ship_mode',
                 'order_priority']

CUSTOMER_DETAILS = ['customer_name', 'segment', 'city', 'state',
                    'country', 'market', 'region']

PRODUCT_DETAILS = ['product_id', 'category', 'sub-category', 'product_name',
                   'sales', 'quantity', 'discount', 'profit', 'shipping_cost']
