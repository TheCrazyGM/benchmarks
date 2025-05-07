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


def calculate_weighted_node_score(node_data, all_nodes_data=None):
    """Calculate a weighted score for a node based on real-world performance importance.

    This function applies weights to different benchmark results to provide a more
    realistic assessment of node performance for actual usage scenarios.

    Args:
        node_data (dict): The node data dictionary containing benchmark results
        all_nodes_data (list, optional): List of all node data for normalized scoring

    Returns:
        float: A weighted score where higher is better
    """
    # Define weights for each test (should sum to 1.0)
    weights = {
        "token": 0.25,  # Token retrieval is critical for most operations
        "contract": 0.20,  # Contract info is important for smart contract operations
        "account_history": 0.20,  # Account history important for wallets and apps
        "latency": 0.25,  # Latency is crucial for real-time applications
        "config": 0.10,  # Config is less critical for everyday usage
    }

    # Base score starts at 0
    score = 0

    # Track if any critical services failed
    critical_service_failed = False

    # Find maximum values for normalization if all_nodes_data is provided
    max_values = {
        "token": {"count": 0},
        "contract": {"count": 0},
        "account_history": {"count": 0},
    }

    if all_nodes_data:
        for node in all_nodes_data:
            # Find max token count
            if node["token"].get("ok", False):
                max_values["token"]["count"] = max(
                    max_values["token"]["count"], node["token"].get("count", 0)
                )
            # Find max contract count
            if node["contract"].get("ok", False):
                max_values["contract"]["count"] = max(
                    max_values["contract"]["count"], node["contract"].get("count", 0)
                )
            # Find max account history count
            if node["account_history"].get("ok", False):
                max_values["account_history"]["count"] = max(
                    max_values["account_history"]["count"], node["account_history"].get("count", 0)
                )

    # Calculate normalized score for each test
    for test, weight in weights.items():
        test_data = node_data.get(test, {})

        # Skip tests that failed or don't have valid data
        if not test_data.get("ok", False):
            # Mark critical service failures
            if test in ["token", "contract", "latency"]:
                critical_service_failed = True
            continue

        # Calculate normalized score based on test type
        if test == "token" or test == "contract" or test == "account_history":
            # For these tests, higher count is better
            max_count = (
                max_values[test]["count"] if all_nodes_data and max_values[test]["count"] > 0 else 1
            )
            ratio = test_data.get("count", 0) / max_count
            test_score = ratio * 100  # Scale to 0-100
        elif test == "config":
            # For config, lower access time is better
            access_time = test_data.get("access_time", 30)
            test_score = 100 * (1 - min(access_time / 2, 1))  # Scale 0-2s to 100-0
        elif test == "latency":
            # For latency, lower avg_latency is better
            avg_latency = test_data.get("avg_latency", 10)
            test_score = 100 * (1 - min(avg_latency / 3, 1))  # Scale 0-3s to 100-0

        # Apply weight to test score
        score += test_score * weight

    # Apply version bonus (newer versions generally better)
    version = node_data.get("SSCnodeVersion", "unknown")
    if version != "unknown":
        try:
            # Try to extract version numbers (formats vary)
            if version.startswith("v"):
                version = version[1:]
            # Extract numbers from version
            import re

            version_nums = re.findall(r"\d+", version)
            if version_nums and len(version_nums) >= 1:
                # Small bonus for newer versions (up to 5%)
                version_bonus = min(5, sum(int(n) for n in version_nums[:3]) / 100)
                score += version_bonus
        except (ValueError, IndexError):
            pass

    # Heavily penalize nodes with critical service failures
    if critical_service_failed:
        score *= 0.5

    return score
