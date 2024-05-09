#! /home/v7por9/.pyenv/shims/python

import polars as pl
from pathlib import Path

# Reading parquet dataset
PARENT_PATH = Path.cwd()

# Final working dataset
WORKING_FILE_NAME = "wrking_Superstore.parquet"
WORKING_DATASET = PARENT_PATH / WORKING_FILE_NAME

dataset = pl.read_parquet(WORKING_DATASET.resolve())
