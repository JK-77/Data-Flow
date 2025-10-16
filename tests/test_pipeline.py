import os

from src.pipeline import run_all
from src.utils import Engine, read_parquet


def test_pipeline_generates_gold(tmp_path):
	# Arrange: copy sample data dir structure under tmp
	data_dir = tmp_path / "data"
	(data_dir / "raw").mkdir(parents=True)
	(data_dir / "bronze").mkdir(parents=True)
	(data_dir / "silver").mkdir(parents=True)
	(data_dir / "gold").mkdir(parents=True)

	# Copy the provided sample CSV from repo into tmp
	repo_csv = os.path.join(os.getcwd(), "data", "raw", "sales.csv")
	assert os.path.exists(repo_csv), "sample CSV missing"
	with open(repo_csv, "r") as fsrc, open(data_dir / "raw" / "sales.csv", "w") as fdst:
		fdst.write(fsrc.read())

	# Act
	run_all(str(data_dir))

	# Assert
	gold_path = data_dir / "gold" / "sales_summary.parquet"
	assert gold_path.exists(), "gold parquet not created"

	engine = Engine.detect()
	df = read_parquet(engine, str(gold_path))
	cols = df.columns if hasattr(df, "columns") else df.columns
	for c in ["region", "total_revenue", "avg_order_value"]:
		assert c in cols
