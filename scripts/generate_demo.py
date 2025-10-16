from __future__ import annotations

import os
import sys
import math
import imageio.v2 as imageio
import matplotlib.pyplot as plt
import pandas as pd

# Ensure local src is importable
sys.path.append(os.path.abspath('src'))

from pipeline import run_all
from utils import Engine, read_parquet


def ensure_assets_dir() -> str:
	assets = os.path.join(os.getcwd(), "assets")
	os.makedirs(assets, exist_ok=True)
	return assets


def generate_png(df: pd.DataFrame, out_path: str) -> None:
	ax = df.plot(kind="bar", x="region", y="total_revenue", title="Revenue by Region")
	plt.tight_layout()
	plt.savefig(out_path)
	plt.close()


def generate_gif(df: pd.DataFrame, out_path: str) -> None:
	# Simple animated buildup of bars
	images = []
	regions = list(df["region"])  # type: ignore
	revenues = list(df["total_revenue"])  # type: ignore
	steps = 8
	for s in range(1, steps + 1):
		plt.figure()
		scaled = [rev * (s / steps) for rev in revenues]
		plt.bar(regions, scaled)
		plt.title("Revenue by Region (demo)")
		plt.xlabel("Region")
		plt.ylabel("Total Revenue")
		plt.tight_layout()
		frame_path = f"/tmp/frame_{s}.png"
		plt.savefig(frame_path)
		plt.close()
		images.append(imageio.imread(frame_path))
	imageio.mimsave(out_path, images, duration=0.25)


def main() -> None:
	assets = ensure_assets_dir()
	# Run pipeline to ensure gold exists
	run_all("data")
	engine = Engine.detect()
	gold_path = os.path.join("data", "gold", "sales_summary.parquet")
	df = read_parquet(engine, gold_path)
	if getattr(engine, 'use_spark', False):
		# convert to pandas for plotting/animation
		df = df.toPandas()
	png_out = os.path.join(assets, "demo.png")
	gif_out = os.path.join(assets, "demo.gif")
	generate_png(df, png_out)
	try:
		generate_gif(df, gif_out)
	except Exception:
		# Fallback: if GIF fails (e.g., no imageio codecs), skip silently
		pass
	print("Demo assets written to", assets)


if __name__ == "__main__":
	main()
