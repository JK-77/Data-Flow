# dataflow-databricks-sample

[![Build](https://github.com/JK-77/dataflow-databricks-sample/actions/workflows/ci.yml/badge.svg)](https://github.com/JK-77/dataflow-databricks-sample/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

This repository demonstrates a mini dataflow pipeline using a Databricks-style bronze/silver/gold architecture. It’s a beginner-friendly portfolio project for data engineers. The pipeline ingests CSV data, cleans and transforms it, and produces a final analytics table (e.g., total revenue per region).

## Demo
If you want a quick visual, generate demo assets:
```bash
python scripts/generate_demo.py
```
This will create `assets/demo.png` and (if possible) `assets/demo.gif`.

## Project Structure
```
dataflow-databricks-sample/
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── notebooks/
│   ├── 01_ingest.ipynb
│   ├── 02_transform.ipynb
│   └── 03_analytics.ipynb
├── src/
│   ├── utils.py
│   └── pipeline.py
├── tests/
│   └── test_pipeline.py
├── scripts/
│   └── generate_demo.py
├── requirements.txt
├── README.md
├── .gitignore
└── LICENSE
```

## Setup
```bash
pip install -r requirements.txt
python src/pipeline.py
```

Run tests:
```bash
pytest -q
```

Notes:
- Tested locally with Pandas; automatically uses PySpark if available.
- Notebooks can run locally or on Databricks (import functions from `src/`).

## Bronze → Silver → Gold
- Bronze: cleaned base data from `data/raw/sales.csv` → `data/bronze/cleaned_sales.parquet`
- Silver: transformed and validated → `data/silver/sales_transformed.parquet`
- Gold: aggregated analytics → `data/gold/sales_summary.parquet`

## License
MIT. See `LICENSE`.
