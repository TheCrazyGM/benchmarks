"""Command line interface package for engine-bench."""

from engine_bench.cli.bench_runner import main as run_benchmarks
from engine_bench.cli.generate_post import main as generate_post

__all__ = ["run_benchmarks", "generate_post"]
