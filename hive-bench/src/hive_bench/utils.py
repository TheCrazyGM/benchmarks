"""Utility functions for benchmarking operations."""

import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initial nodes list for benchmarking
INITIAL_NODES = [
    "https://api.syncad.com",
    "https://hive-api.arcange.eu",
    "https://api.openhive.network",
    "https://techcoderx.com",
    "https://api.c0ff33a.uk",
    "https://hiveapi.actifit.io",
    "https://rpc.mahdiyari.info",
    "https://api.hive.blog",
    "https://anyx.io",
    "https://hive-api.dlux.io",
]

# Global flag to signal thread termination
quit_thread = False


def format_float(value):
    """Format a float value to 2 decimal places.

    Args:
        value (float): The float value to format

    Returns:
        float: The formatted float value with 2 decimal places
    """
    try:
        return float("{:.2f}".format(value))
    except (ValueError, TypeError):
        return 0.0


def benchmark_executor(func, node, *args, **kwargs):
    """Execute a benchmark function with comprehensive error handling and timing.

    This function serves as a wrapper for all benchmark functions, providing consistent
    error handling, timing, and result formatting. It catches common exceptions and
    ensures that all benchmark results have a consistent structure.

    Args:
        func (callable): The benchmark function to execute
        node (str): URL of the node to benchmark
        *args: Additional positional arguments to pass to the benchmark function
        **kwargs: Additional keyword arguments to pass to the benchmark function

    Returns:
        dict: A dictionary containing benchmark results with the following keys:
            - successful (bool): Whether the benchmark was successful
            - node (str): The node URL that was benchmarked
            - error (str or None): Error message if an exception occurred, None otherwise
            - total_duration (float): Total time taken to execute the benchmark in seconds
            - Additional keys returned by the benchmark function
    """
    from timeit import default_timer as timer

    from nectarapi.exceptions import NumRetriesReached

    start_total = timer()
    successful = True
    error_msg = None
    result = {}

    try:
        func_result = func(node, *args, **kwargs)

        # Ensure result is a dictionary
        if isinstance(func_result, dict):
            result = func_result
        else:
            logging.warning(
                f"Benchmark function {func.__name__} returned {type(func_result)}, expected dict"
            )
            result = {}

    except NumRetriesReached:
        error_msg = "NumRetriesReached"
        successful = False
    except KeyboardInterrupt:
        error_msg = "KeyboardInterrupt"
    except Exception as e:
        error_msg = str(e)
        successful = False

    total_duration = format_float(timer() - start_total)

    # Add common fields if not present
    result.update(
        {
            "successful": successful if "successful" not in result else result["successful"],
            "node": node,
            "error": error_msg,
            "total_duration": total_duration
            if "total_duration" not in result
            else result["total_duration"],
        }
    )

    return result


def make_sort_key(current_test):
    """Create a sort key function for ranking benchmark results.

    Args:
        current_test (str): The test type to create a sort key for

    Returns:
        callable: A function that can be used as a key for sorting
    """

    def sort_key(item):
        test_data = item[1][current_test]
        if not test_data["ok"]:
            return -1 if current_test in ["block", "history"] else float("inf")

        if current_test in ["block", "history"]:
            return test_data["count"]
        elif current_test in ["apicall", "config"]:
            return test_data["access_time"]
        elif current_test == "block_diff":
            return test_data["head_delay"]
        return 0

    return sort_key


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
        "block": 0.30,  # Block retrieval is critical for most operations
        "history": 0.25,  # History retrieval is important for many apps
        "apicall": 0.25,  # API call performance matters for user experience
        "config": 0.05,  # Config is least important for everyday usage
        "block_diff": 0.15,  # Block difference shows node synchronization status
    }

    # Base score starts at 0
    score = 0

    # Track if any critical services failed
    critical_service_failed = False

    # Find maximum values for normalization if all_nodes_data is provided
    max_values = {
        "block": {"count": 0},
        "history": {"count": 0},
    }

    if all_nodes_data:
        for node in all_nodes_data:
            # Find max block count
            if node["block"].get("ok", False):
                max_values["block"]["count"] = max(
                    max_values["block"]["count"], node["block"].get("count", 0)
                )
            # Find max history count
            if node["history"].get("ok", False):
                max_values["history"]["count"] = max(
                    max_values["history"]["count"], node["history"].get("count", 0)
                )

    # Calculate normalized score for each test
    for test, weight in weights.items():
        test_data = node_data.get(test, {})

        # Skip tests that failed or don't have valid data
        if not test_data.get("ok", False):
            # Mark critical service failures
            if test in ["block", "history", "apicall"]:
                critical_service_failed = True
            continue

        # Calculate normalized score based on test type
        if test == "block":
            # For block, higher count is better
            max_count = max_values["block"]["count"] if all_nodes_data else 1
            if max_count > 0:
                ratio = test_data.get("count", 0) / max_count
                test_score = ratio * 100  # Scale to 0-100
            else:
                test_score = 0
        elif test == "history":
            # For history, higher count is better
            max_count = max_values["history"]["count"] if all_nodes_data else 1
            if max_count > 0:
                ratio = test_data.get("count", 0) / max_count
                test_score = ratio * 100  # Scale to 0-100
            else:
                test_score = 0
        elif test == "apicall" or test == "config":
            # For API call and config, lower access time is better
            # Invert the access time so smaller times get higher scores
            access_time = test_data.get("access_time", 30)
            test_score = 100 * (1 - min(access_time / 2, 1))  # Scale 0-2s to 100-0
        elif test == "block_diff":
            # For block difference, lower head delay is better
            head_delay = test_data.get("head_delay", 10)
            test_score = 100 * (1 - min(head_delay / 3, 1))  # Scale 0-3s to 100-0

        # Apply weight to test score
        score += test_score * weight

    # Apply version bonus (newer versions generally better)
    version = node_data.get("version", "0.0.0")
    try:
        # Try to parse version (e.g. "1.27.11")
        version_parts = [int(p) for p in version.split(".")]
        # Small bonus for newer versions (up to 5%)
        version_bonus = min(
            5, (version_parts[0] * 100 + version_parts[1] * 10 + version_parts[2]) / 200
        )
        score += version_bonus  # Up to 1 point bonus
    except (ValueError, IndexError):
        pass

    # Heavily penalize nodes with critical service failures
    if critical_service_failed:
        score *= 0.5

    return score
