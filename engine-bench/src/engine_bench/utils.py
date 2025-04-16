"""Utility functions for engine-bench project."""

import logging
import time
from pathlib import Path

# Initial nodes to use for benchmarking
# These are just placeholder nodes - you should replace with actual hive-engine nodes
INITIAL_NODES = [
    "https://api.hive-engine.com/rpc",
    "https://engine.rishipanthee.com",
    "https://api2.hive-engine.com/rpc",
    "https://herpc.dtools.dev",
]

# Global flag for thread interruption
quit_thread = False


def format_float(value, precision=2):
    """Format float value to the specified precision.

    Args:
        value (float): The float value to format
        precision (int, optional): Decimal precision. Defaults to 2.

    Returns:
        float: Formatted float value rounded to specified precision
    """
    try:
        if isinstance(value, (int, float)):
            return round(float(value), precision)
        return 0.0
    except (ValueError, TypeError):
        return 0.0


def get_project_root() -> Path:
    """Get the absolute path to the project root directory."""
    current_file = Path(__file__).resolve()
    # Go up two levels from src/engine_bench/utils.py to reach project root
    return current_file.parent.parent.parent


def benchmark_executor(benchmark_func, node, *args, **kwargs):
    """Execute benchmark function with proper error handling.

    This function wraps benchmark function calls with appropriate error handling,
    making sure that any exceptions are caught and logged. It adds the node URL
    to the result dict for easy identification.

    Args:
        benchmark_func (callable): The benchmark function to execute
        node (str): URL of the node to benchmark
        *args: Additional positional arguments for the benchmark function
        **kwargs: Additional keyword arguments for the benchmark function

    Returns:
        dict: The benchmark result with node URL added
    """
    global quit_thread

    if quit_thread:
        return {
            "successful": False,
            "node": node,
            "error": "Interrupted",
            "total_duration": 0.0,
        }

    try:
        start_time = time.time()
        result = benchmark_func(node, *args, **kwargs)
        # Add node URL to result for easy identification
        if isinstance(result, dict):
            result["node"] = node
            # If total_duration wasn't set by the benchmark function, set it now
            if "total_duration" not in result:
                result["total_duration"] = time.time() - start_time
        else:
            # If result isn't a dict, create a new dict with the result
            result = {
                "successful": True,
                "node": node,
                "result": result,
                "total_duration": time.time() - start_time,
            }
        return result
    except Exception as e:
        logging.error(f"Error executing benchmark on node {node}: {str(e)}")
        return {
            "successful": False,
            "node": node,
            "error": str(e),
            "total_duration": time.time() - start_time if "start_time" in locals() else 0.0,
        }
