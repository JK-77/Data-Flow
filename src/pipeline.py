from __future__ import annotations

import argparse
import os
from typing import Tuple

from utils import (
	Engine,
	ensure_dirs,
	load_csv,
	write_parquet,
	standardize_schema,
	drop_nulls,
	transform_sales,
	validate_sales,
	aggregate_sales,
	read_parquet,
)


def bronze(raw_csv: str, bronze_path: str) -> None:
	engine = Engine.detect()
	ensure_dirs()
	df = load_csv(engine, raw_csv)
	df = standardize_schema(engine, df)
	df = drop_nulls(engine, df)
	write_parquet(engine, df, bronze_path)


def silver(bronze_path: str, silver_path: str) -> None:
	engine = Engine.detect()
	df = read_parquet(engine, bronze_path)
	df = transform_sales(engine, df)
	ok, msg = validate_sales(engine, df)
	if not ok:
		raise ValueError(f"Data validation failed: {msg}")
	write_parquet(engine, df, silver_path)


def gold(silver_path: str, gold_path: str, group_cols=("region",)) -> None:
	engine = Engine.detect()
	df = read_parquet(engine, silver_path)
	agg = aggregate_sales(engine, df, group_cols=group_cols)
	write_parquet(engine, agg, gold_path)


def run_all(data_dir: str = "data") -> None:
	raw_csv = os.path.join(data_dir, "raw", "sales.csv")
	bronze_path = os.path.join(data_dir, "bronze", "cleaned_sales.parquet")
	silver_path = os.path.join(data_dir, "silver", "sales_transformed.parquet")
	gold_path = os.path.join(data_dir, "gold", "sales_summary.parquet")

	bronze(raw_csv, bronze_path)
	silver(bronze_path, silver_path)
	gold(silver_path, gold_path)


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Run dataflow bronze→silver→gold pipeline")
	parser.add_argument("--data-dir", default="data", help="Root data directory")
	args = parser.parse_args()
	run_all(args.data_dir)
