#! /home/v7por9/.pyenv/shims/python

import polars as pl
from pathlib import Path

# Reading parquet dataset
PARENT_PATH = Path.cwd()
FILE_NAME = "global_Superstore.parquet"
FULL_PATHNAME = PARENT_PATH / FILE_NAME

dataset = pl.read_parquet(FULL_PATHNAME.resolve())

