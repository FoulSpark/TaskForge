"""Automated verification for the Stage 2 public benchmark visualization suite."""

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RESULTS_PATH = REPO_ROOT / "benchmarks" / "public" / "benchmark_results.json"
GENERATOR_PATH = REPO_ROOT / "benchmarks" / "public" / "generate_charts.py"
CHARTS_DIR = REPO_ROOT / "benchmarks" / "public" / "charts"

CHART_FILES = [
    "throughput_comparison.svg",
    "latency_percentiles.svg",
    "latency_decomposition.svg",
]


def test_benchmark_chart_assets_exist_and_match_results():
    """Verify committed SVG/PNG benchmark charts exist and reflect exact values from benchmark_results.json."""
    assert GENERATOR_PATH.is_file()
    assert RESULTS_PATH.is_file()
    data = json.loads(RESULTS_PATH.read_text(encoding="utf-8"))
    run_id = data["provenance"]["run_id"]
    wr = data["workload_results"]

    for fname in CHART_FILES:
        svg_path = CHARTS_DIR / fname
        png_path = svg_path.with_suffix(".png")
        assert svg_path.is_file(), f"Missing SVG chart: {svg_path}"
        assert png_path.is_file(), f"Missing PNG chart: {png_path}"

        svg_text = svg_path.read_text(encoding="utf-8")
        assert svg_text.startswith("<svg ") and "</svg>" in svg_text
        assert run_id in svg_text

    tps_svg = (CHARTS_DIR / "throughput_comparison.svg").read_text(encoding="utf-8")
    for wid in ("W-01", "W-02", "W-03"):
        tps_str = f"{wr[wid]['throughput_tasks_per_sec']:.2f} tasks/s"
        assert tps_str in tps_svg

    decomp_svg = (CHARTS_DIR / "latency_decomposition.svg").read_text(encoding="utf-8")
    for wid in ("W-01", "W-02", "W-03"):
        e2e_mean = f"{wr[wid]['latencies_ms']['mean']:,.3f} ms"
        assert e2e_mean in decomp_svg


def test_benchmark_chart_generator_is_reproducible(tmp_path):
    """Verify generate_charts.py regenerates deterministic SVGs into an isolated output directory."""
    res = subprocess.run(
        [
            sys.executable,
            str(GENERATOR_PATH),
            "--input",
            str(RESULTS_PATH),
            "--output-dir",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert res.returncode == 0, f"Chart generator failed: {res.stderr}"

    for fname in CHART_FILES:
        generated = (tmp_path / fname).read_text(encoding="utf-8")
        committed = (CHARTS_DIR / fname).read_text(encoding="utf-8")
        assert generated == committed, f"Committed {fname} differs from generate_charts.py output"
