# Databricks notebook source
# 02 - Transform (bronze → silver)

# COMMAND ----------
import sys, os
sys.path.append(os.path.abspath('../src'))
from pipeline import silver

data_dir = '../data'
bronze_path = os.path.join(data_dir, 'bronze', 'cleaned_sales.parquet')
silver_path = os.path.join(data_dir, 'silver', 'sales_transformed.parquet')
silver(bronze_path, silver_path)
print('Silver written to', silver_path)
