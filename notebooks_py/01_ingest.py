# Databricks notebook source
# 01 - Ingest (raw → bronze)

# COMMAND ----------
import sys, os
sys.path.append(os.path.abspath('../src'))
from pipeline import bronze

data_dir = '../data'
raw_csv = os.path.join(data_dir, 'raw', 'sales.csv')
bronze_path = os.path.join(data_dir, 'bronze', 'cleaned_sales.parquet')
bronze(raw_csv, bronze_path)
print('Bronze written to', bronze_path)
