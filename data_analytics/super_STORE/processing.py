#! /home/v7por9/.pyenv/shims/python

import polars as pl
from pathlib import Path
from datetime import datetime

# Reading parquet dataset
PARENT_PATH = Path.cwd()
FILE_NAME = "global_Superstore.parquet"
FULL_PATHNAME = PARENT_PATH / FILE_NAME

# Final working dataset
WORKING_FILE_NAME = "wrking_Superstore.parquet"
WORKING_PATH = PARENT_PATH / WORKING_FILE_NAME

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

# data cleaning for the whole dataset
# ## Setting similar date for the transaction dates
orders_dataset = dataset.select(pl.col(ORDER_DETAILS))
order_date = orders_dataset.select(pl.col('order_date'))
ship_date = orders_dataset.select(pl.col('ship_date'))


def date_convert(date_list):
    data_return = []
    for set_data in date_list:
        if str(set_data).__contains__("-"):
            data_return.append(datetime.strptime(set_data, "%d-%m-%y"))
        elif str(set_data).__contains__("/"):
            data_return.append(datetime.strptime(set_data, "%d/%m/%Y"))
    return data_return


order_list = (eval(str(list(order_date
                            .to_dict(as_series="")
                            .values()))
                   .replace("[[", "[")
                   .replace("]]", "]")))

ship_list = (eval(str(list(ship_date
                           .to_dict(as_series="")
                           .values()))
                  .replace("[[", "[")
                  .replace("]]", "]")))

order_and_ship_df = pl.DataFrame({"order_date": date_convert(order_list),
                                  "ship_date": date_convert(ship_list)})

order_dataset = order_and_ship_df.with_columns(orders_dataset
                                               .select(pl.col(['order_id',
                                                               'ship_mode',
                                                               'order_priority'
                                                               ])))

# ## Defining order_priority to UPPER_CASE for ease of reference and uniqueness
unique_priority = order_dataset.select(pl.col('order_priority'))
# print(unique_priority.unique())

order_dataset = order_dataset.with_columns(pl
                                           .col('order_priority')
                                           .str.to_uppercase())
# # Processing customer details
customer_dataset = dataset.select(pl.col(CUSTOMER_DETAILS))

# ## Getting unique sections for listed columns
# 1. segment
unique_segment = customer_dataset.select(pl.col("segment").unique())
# print(unique_segment)

# 2. country
unique_country = customer_dataset.select(pl.col("country").unique())
# print(unique_country)

# 3. market
unique_market = customer_dataset.select(pl.col("market").unique())
# print(unique_market)

# 4. region
unique_region = customer_dataset.select(pl.col("region").unique())
# print(unique_region)

# 5. customer_names
unique_customers = customer_dataset.select(pl.col("customer_name").unique())
# print(unique_customers)


# Processing products data
product_dataset = dataset.select(pl.col(PRODUCT_DETAILS))
# print(product_dataset.null_count)


# Defining the final dataframes
wrking_dataset = pl.concat([order_dataset,
                            customer_dataset,
                            product_dataset], how="horizontal")

# Exporting the final working dataset for data analytics
wrking_dataset.write_parquet(WORKING_PATH.resolve())
