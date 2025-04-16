"""Hive node benchmarking package."""

import sys

__version__ = "0.2.0"  # Update to match pyproject.toml

# Check Python version
if sys.version_info < (3, 13):
    print("Error: hive-bench requires Python 3.13 or higher")
    sys.exit(1)
