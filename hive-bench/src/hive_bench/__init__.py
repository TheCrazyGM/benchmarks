"""Hive node benchmarking package."""

import sys

__app_name__ = "hive-bench"
__version__ = "0.2.1"

# Check Python version
if sys.version_info < (3, 13):
    print("Error: hive-bench requires Python 3.13 or higher")
    sys.exit(1)
