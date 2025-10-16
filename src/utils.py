from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple
import os
import sys
import datetime as dt

import pandas as pd

try:
	from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
	from pyspark.sql import functions as F
	from pyspark.sql import types as T
	_SPARK_AVAILABLE = True
except Exception:
	SparkSession = None  # type: ignore
	SparkDataFrame = None  # type: ignore
	F = None  # type: ignore
	T = None  # type: ignore
	_SPARK_AVAILABLE = False


@dataclass
class Engine:
	"""Simple execution engine wrapper to abstract Pandas vs Spark.

	If Spark is available, we use it; otherwise we fallback to Pandas.
	"""
	use_spark: bool
	spark: Optional[SparkSession] = None

	@staticmethod
	def detect() -> "Engine":
		use_spark = _SPARK_AVAILABLE and os.environ.get("FORCE_PANDAS", "0") != "1"
		spark = SparkSession.builder.appName("dataflow-databricks-sample").getOrCreate() if use_spark else None
		return Engine(use_spark=use_spark, spark=spark)


# --------------------
# Common helpers
# --------------------

def ensure_dirs() -> None:
	for d in [
		"data/raw",
		"data/bronze",
		"data/silver",
		"data/gold",
	]:
		os.makedirs(d, exist_ok=True)


def clean_column_names(name: str) -> str:
	return name.strip().lower().replace(" ", "_").replace("-", "_")


# --------------------
# IO helpers
# --------------------

def load_csv(engine: Engine, path: str):
	"""Load CSV into Spark or Pandas depending on engine."""
	if engine.use_spark and engine.spark is not None:
		return (
			engine.spark.read.option("header", True).option("inferSchema", True).csv(path)
		)
	return pd.read_csv(path)


def write_parquet(engine: Engine, df, path: str) -> None:
	if engine.use_spark:
		df.write.mode("overwrite").parquet(path)
	else:
		# Pandas -> Parquet requires pyarrow
		df.to_parquet(path, index=False)


# --------------------
# Quality and transforms
# --------------------

def standardize_schema(engine: Engine, df):
	"""Clean column names and parse dates."""
	if engine.use_spark:
		for c in df.columns:
			df = df.withColumnRenamed(c, clean_column_names(c))
		# parse date
		if "date" in df.columns:
			df = df.withColumn("date", F.to_date(F.col("date")))
		return df
	else:
		df.columns = [clean_column_names(c) for c in df.columns]
		if "date" in df.columns:
			df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
		return df


def drop_nulls(engine: Engine, df):
	if engine.use_spark:
		return df.dropna()
	else:
		return df.dropna()


def transform_sales(engine: Engine, df):
	"""Add derived columns like total_amount = quantity * unit_price."""
	if engine.use_spark:
		return df.withColumn(
			"total_amount", F.col("quantity") * F.col("unit_price")
		)
	else:
		df = df.copy()
		df["total_amount"] = df["quantity"] * df["unit_price"]
		return df


def validate_sales(engine: Engine, df) -> Tuple[bool, str]:
	"""Ensure no negative values and product not null."""
	if engine.use_spark:
		neg_count = df.filter((F.col("quantity") < 0) | (F.col("unit_price") < 0)).count()
		null_prod = df.filter(F.col("product").isNull()).count()
		return (neg_count == 0 and null_prod == 0, "validation passed" if neg_count == 0 and null_prod == 0 else "failed: negatives or null product")
	else:
		neg = ((df["quantity"] < 0) | (df["unit_price"] < 0)).any()
		null_prod = df["product"].isna().any()
		ok = (not neg) and (not null_prod)
		return (ok, "validation passed" if ok else "failed: negatives or null product")


def aggregate_sales(engine: Engine, df, group_cols=("region",)):
	"""Compute total revenue and avg order value grouped by region/category."""
	if engine.use_spark:
		grouped = df.groupBy(*[F.col(c) for c in group_cols]).agg(
			F.sum("total_amount").alias("total_revenue"),
			F.avg("total_amount").alias("avg_order_value"),
		)
		return grouped
	else:
		grouped = (
			df.groupby(list(group_cols))["total_amount"].agg(["sum", "mean"]).reset_index()
		)
		grouped = grouped.rename(columns={"sum": "total_revenue", "mean": "avg_order_value"})
		return grouped


def read_parquet(engine: Engine, path: str):
	if engine.use_spark and engine.spark is not None:
		return engine.spark.read.parquet(path)
	return pd.read_parquet(path)
