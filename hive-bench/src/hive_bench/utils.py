"""Utility functions for benchmarking operations."""

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Initial nodes list for benchmarking
INITIAL_NODES = [
    "https://api.syncad.com",
    "https://api.deathwing.me",
    "https://hive-api.arcange.eu",
    "https://api.openhive.network",
    "https://techcoderx.com",
    "https://api.c0ff33a.uk",
    "https://hive-api.3speak.tv",
    "https://hiveapi.actifit.io",
    "https://rpc.mahdiyari.info",
    "https://api.hive.blog",
    "https://anyx.io",
    "https://hive.roelandp.nl",
    "https://hived.emre.sh",
    "https://api.hive.blue",
    "https://rpc.ausbit.dev",
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
            "successful": successful
            if "successful" not in result
            else result["successful"],
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
