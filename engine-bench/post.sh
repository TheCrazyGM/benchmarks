#!/usr/bin/env bash
set -euo pipefail

# Get the script directory
scriptdir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$scriptdir" || exit 1

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
  source .venv/bin/activate
fi

# Load environment variables from .env file if it exists
if [ -f ".env" ]; then
  # Load env vars from .env file
  set -a
  source .env
  set +a
fi

# Generate the post content and publish to Hive
python -m src.engine_bench.cli.generate_post -p -a nectarflower

# Exit with the status code of the post generation script
exit $?
