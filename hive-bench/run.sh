#!/bin/bash

# Get the script directory
scriptdir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$scriptdir" || exit 1

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
  source .venv/bin/activate
fi

# Load environment variables from .env file if it exists
if [ -f ".env" ]; then
  export $(grep -v '^#' .env | xargs)
fi

# Run the benchmark
python -m src.hive_bench.cli.bench_runner -u -o hive_benchmark_results.json -a nectarflower

# Exit with the status code of the benchmark
exit $?
