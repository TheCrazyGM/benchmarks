"""Benchmarking class for running multiple benchmark tests on Hive nodes."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from hive_bench.benchmark_functions import (
    benchmark_block_diff,
    benchmark_calls,
    benchmark_node_blocks,
    benchmark_node_history,
    get_config_node,
)
from hive_bench.utils import benchmark_executor


class Benchmarks:
    """A class for running various benchmark tests on Hive nodes.

    This class provides methods to benchmark different aspects of Hive nodes, including
    configuration retrieval, block retrieval, account history retrieval, API calls,
    and block synchronization status. It supports both threaded and sequential execution
    of benchmark tests.

    Attributes:
        num_retries (int): Number of connection retries for all benchmark tests
        num_retries_call (int): Number of API call retries for all benchmark tests
        timeout (int): Connection timeout in seconds for all benchmark tests
    """

    def __init__(self, num_retries=10, num_retries_call=10, timeout=60):
        """Initialize the Benchmarks class with connection parameters.

        Args:
            num_retries (int, optional): Number of connection retries. Defaults to 10.
            num_retries_call (int, optional): Number of API call retries. Defaults to 10.
            timeout (int, optional): Connection timeout in seconds. Defaults to 60.
        """
        self.num_retries = num_retries
        self.num_retries_call = num_retries_call
        self.timeout = timeout

    def _run_benchmark_threaded(self, nodes, benchmark_func, *args):
        """Run benchmark tests on multiple nodes concurrently using threads.

        This method executes the specified benchmark function on multiple nodes
        concurrently using a ThreadPoolExecutor. It handles keyboard interrupts
        gracefully by setting the global quit_thread flag.

        Args:
            nodes (list): List of node URLs to benchmark
            benchmark_func (callable): The benchmark function to execute
            *args: Additional arguments to pass to the benchmark function

        Returns:
            list: List of benchmark results, one for each node
        """
        results = []
        global quit_thread

        try:
            with ThreadPoolExecutor(max_workers=min(32, len(nodes))) as executor:
                # Create a dict mapping futures to their node URLs
                future_to_node = {}
                for node in nodes:
                    future = executor.submit(
                        benchmark_executor,
                        benchmark_func,
                        node,
                        *args,
                        num_retries=self.num_retries,
                        num_retries_call=self.num_retries_call,
                        timeout=self.timeout,
                    )
                    future_to_node[future] = node

                # Process results as they complete
                for future in as_completed(future_to_node):
                    node = future_to_node[future]
                    try:
                        result = future.result()
                        logging.info(
                            f"Benchmark completed for node {node}: {result['successful']}"
                        )
                        results.append(result)
                    except Exception as e:
                        logging.error(f"Error benchmarking node {node}: {str(e)}")
                        results.append(
                            {
                                "successful": False,
                                "node": node,
                                "error": str(e),
                                "total_duration": 0.0,
                            }
                        )

        except KeyboardInterrupt:
            logging.info("KeyboardInterrupt received, stopping threads...")
            quit_thread = True

        return results

    def _run_benchmark_sequential(self, nodes, benchmark_func, *args):
        """Run benchmark tests on multiple nodes sequentially.

        This method executes the specified benchmark function on multiple nodes
        one after another in a sequential manner.

        Args:
            nodes (list): List of node URLs to benchmark
            benchmark_func (callable): The benchmark function to execute
            *args: Additional arguments to pass to the benchmark function

        Returns:
            list: List of benchmark results, one for each node
        """
        results = []

        for node in nodes:
            try:
                result = benchmark_executor(
                    benchmark_func,
                    node,
                    *args,
                    num_retries=self.num_retries,
                    num_retries_call=self.num_retries_call,
                    timeout=self.timeout,
                )
                logging.info(
                    f"Benchmark completed for node {node}: {result['successful']}"
                )
                results.append(result)
            except Exception as e:
                logging.error(f"Error benchmarking node {node}: {str(e)}")
                results.append(
                    {
                        "successful": False,
                        "node": node,
                        "error": str(e),
                        "total_duration": 0.0,
                    }
                )

        return results

    def run_config_benchmark(self, nodes, how_many_seconds, threading=True):
        """Run configuration retrieval benchmark tests on multiple nodes.

        This method benchmarks how quickly each node can provide its configuration information.
        It retrieves the node's version, configuration, and determines if it's a Hive node.

        Args:
            nodes (list): List of node URLs to benchmark
            how_many_seconds (int): Time limit for benchmark in seconds
            threading (bool, optional): Whether to run benchmarks concurrently. Defaults to True.

        Returns:
            list: List of benchmark results, one for each node
        """
        logging.info(f"Running config benchmark on {len(nodes)} nodes...")

        # Create a wrapper function that passes connection parameters properly
        def config_wrapper(node, how_many_seconds, **kwargs):
            # Extract connection parameters
            num_retries = kwargs.get("num_retries", self.num_retries)
            num_retries_call = kwargs.get("num_retries_call", self.num_retries_call)
            timeout = kwargs.get("timeout", self.timeout)

            return get_config_node(
                node,
                num_retries=num_retries,
                num_retries_call=num_retries_call,
                timeout=timeout,
                how_many_seconds=how_many_seconds,
            )

        if threading:
            return self._run_benchmark_threaded(nodes, config_wrapper, how_many_seconds)
        else:
            return self._run_benchmark_sequential(
                nodes, config_wrapper, how_many_seconds
            )

    def run_block_benchmark(self, nodes, how_many_seconds, threading=True):
        """Run block retrieval benchmark tests on multiple nodes.

        This method benchmarks how many blocks each node can retrieve within a specified time period.
        It starts at 75% of the current block height and counts how many blocks can be retrieved
        sequentially within the time limit.

        Args:
            nodes (list): List of node URLs to benchmark
            how_many_seconds (int): Time limit for benchmark in seconds
            threading (bool, optional): Whether to run benchmarks concurrently. Defaults to True.

        Returns:
            list: List of benchmark results, one for each node
        """
        logging.info(f"Running block benchmark on {len(nodes)} nodes...")

        # Create a wrapper function that passes connection parameters properly
        def block_wrapper(node, how_many_seconds, **kwargs):
            # Extract connection parameters
            num_retries = kwargs.get("num_retries", self.num_retries)
            num_retries_call = kwargs.get("num_retries_call", self.num_retries_call)
            timeout = kwargs.get("timeout", self.timeout)

            return benchmark_node_blocks(
                node,
                how_many_seconds=how_many_seconds,
                num_retries=num_retries,
                num_retries_call=num_retries_call,
                timeout=timeout,
            )

        if threading:
            return self._run_benchmark_threaded(nodes, block_wrapper, how_many_seconds)
        else:
            return self._run_benchmark_sequential(
                nodes, block_wrapper, how_many_seconds
            )

    def run_hist_benchmark(
        self, nodes, how_many_seconds, threading=True, account_name="thecrazygm"
    ):
        """Run account history retrieval benchmark tests on multiple nodes.

        This method benchmarks how many account history operations each node can retrieve
        within a specified time period. It retrieves history operations in reverse order
        for the specified account and counts how many can be retrieved within the time limit.

        Args:
            nodes (list): List of node URLs to benchmark
            how_many_seconds (int): Time limit for benchmark in seconds
            threading (bool, optional): Whether to run benchmarks concurrently. Defaults to True.
            account_name (str, optional): Account name to retrieve history for. Defaults to "thecrazygm".

        Returns:
            list: List of benchmark results, one for each node
        """
        logging.info(f"Running history benchmark on {len(nodes)} nodes...")

        # Create a wrapper function that passes connection parameters properly
        def history_wrapper(node, how_many_seconds, account_name, **kwargs):
            # Extract connection parameters
            num_retries = kwargs.get("num_retries", self.num_retries)
            num_retries_call = kwargs.get("num_retries_call", self.num_retries_call)
            timeout = kwargs.get("timeout", self.timeout)

            return benchmark_node_history(
                node,
                how_many_seconds=how_many_seconds,
                account_name=account_name,
                num_retries=num_retries,
                num_retries_call=num_retries_call,
                timeout=timeout,
            )

        if threading:
            return self._run_benchmark_threaded(
                nodes, history_wrapper, how_many_seconds, account_name
            )
        else:
            return self._run_benchmark_sequential(
                nodes, history_wrapper, how_many_seconds, account_name
            )

    def run_call_benchmark(self, nodes, authorpermvoter, threading=True):
        """Run API call benchmark tests on multiple nodes for retrieving post data.

        This method benchmarks how quickly each node can retrieve a specific post or
        any recent post from a specified author. It measures the time taken to access
        the post data as a performance metric.

        Args:
            nodes (list): List of node URLs to benchmark
            authorpermvoter (str): String in format "author/permlink" identifying the post to retrieve
            threading (bool, optional): Whether to run benchmarks concurrently. Defaults to True.

        Returns:
            list: List of benchmark results, one for each node
        """
        logging.info(f"Running API call benchmark on {len(nodes)} nodes...")

        # Create a wrapper function that passes connection parameters properly
        def calls_wrapper(node, authorpermvoter, **kwargs):
            # Extract connection parameters
            num_retries = kwargs.get("num_retries", self.num_retries)
            num_retries_call = kwargs.get("num_retries_call", self.num_retries_call)
            timeout = kwargs.get("timeout", self.timeout)

            return benchmark_calls(
                node,
                authorpermvoter,
                num_retries=num_retries,
                num_retries_call=num_retries_call,
                timeout=timeout,
            )

        if threading:
            return self._run_benchmark_threaded(nodes, calls_wrapper, authorpermvoter)
        else:
            return self._run_benchmark_sequential(nodes, calls_wrapper, authorpermvoter)

    def run_block_diff_benchmark(self, nodes, threading=True):
        """Run block synchronization benchmark tests on multiple nodes.

        This method benchmarks two key metrics for each node:
        1. Head delay: The time difference between the current UTC time and the timestamp of the
           most recent block (head block). This indicates how up-to-date the node is.
        2. Difference between head and irreversible blocks: The number of blocks between the
           head block and the last irreversible block. This indicates how many recent blocks
           are still potentially reversible.

        Args:
            nodes (list): List of node URLs to benchmark
            threading (bool, optional): Whether to run benchmarks concurrently. Defaults to True.

        Returns:
            list: List of benchmark results, one for each node
        """
        logging.info(f"Running block diff benchmark on {len(nodes)} nodes...")

        # Create a wrapper function that passes connection parameters properly
        def block_diff_wrapper(node, **kwargs):
            # Extract connection parameters
            num_retries = kwargs.get("num_retries", self.num_retries)
            num_retries_call = kwargs.get("num_retries_call", self.num_retries_call)
            timeout = kwargs.get("timeout", self.timeout)

            return benchmark_block_diff(
                node,
                num_retries=num_retries,
                num_retries_call=num_retries_call,
                timeout=timeout,
            )

        if threading:
            return self._run_benchmark_threaded(nodes, block_diff_wrapper)
        else:
            return self._run_benchmark_sequential(nodes, block_diff_wrapper)
