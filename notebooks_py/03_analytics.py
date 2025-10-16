# Databricks notebook source
# 03 - Analytics (silver → gold)

# COMMAND ----------
import sys, os
import matplotlib.pyplot as plt
sys.path.append(os.path.abspath('../src'))
from pipeline import gold
from utils import Engine, read_parquet

data_dir = '../data'
silver_path = os.path.join(data_dir, 'silver', 'sales_transformed.parquet')
gold_path = os.path.join(data_dir, 'gold', 'sales_summary.parquet')

gold(silver_path, gold_path)
print('Gold written to', gold_path)

engine = Engine.detect()
df = read_parquet(engine, gold_path)

if not engine.use_spark:
	ax = df.plot(kind='bar', x='region', y='total_revenue', title='Revenue by Region')
	plt.tight_layout()
	plt.show()
