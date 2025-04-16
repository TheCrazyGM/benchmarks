"""Nectar-bench package for benchmarking hive-engine nodes using nectarengine."""

import sys

__version__ = "0.1.0"

# Check Python version
if sys.version_info < (3, 13):
    print("Error: engine-bench requires Python 3.13 or higher")
    sys.exit(1)
