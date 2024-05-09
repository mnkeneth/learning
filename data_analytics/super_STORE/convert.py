#! /home/v7por9/.pyenv/shims/python

import polars as pl
from pathlib import Path

# Reading the xlsx dataset and exporting to parquet
PARENT_PATH = Path.cwd()
FILE_NAME = "global_Superstore.xlsx"
FULL_PATHNAME = PARENT_PATH / FILE_NAME

# Reading data into polars
dataset = pl.read_excel(FULL_PATHNAME.resolve())

# Writing data into parquet
dataset.write_parquet("global_Superstore.parquet")
