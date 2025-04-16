"""Benchmarking class for running multiple benchmark tests on hive-engine nodes."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from engine_bench.benchmark_functions import (
    benchmark_account_history,
    benchmark_contract_retrieval,
    benchmark_latency,
    benchmark_token_retrieval,
    get_status_node,
)
from engine_bench.utils import benchmark_executor


class Benchmarks:
    """A class for running various benchmark tests on hive-engine nodes.

    This class provides methods to benchmark different aspects of hive-engine nodes, including
    configuration retrieval, token retrieval, contract retrieval, and account history retrieval.
    It supports both threaded and sequential execution of benchmark tests.

    Attributes:
        num_retries (int): Number of connection retries for all benchmark tests
        num_retries_call (int): Number of API call retries for all benchmark tests
        timeout (int): Connection timeout in seconds for all benchmark tests
    """

    def __init__(self, num_retries=3, num_retries_call=3, timeout=30):
        """Initialize the Benchmarks class with connection parameters.

        Args:
            num_retries (int, optional): Number of connection retries. Defaults to 3.
            num_retries_call (int, optional): Number of API call retries. Defaults to 3.
            timeout (int, optional): Connection timeout in seconds. Defaults to 30.
        """
        self.num_retries = num_retries
        self.num_retries_call = num_retries_call
        self.timeout = timeout

    def _run_benchmark_threaded(self, nodes, benchmark_func, *args):
        """Run benchmark tests on multiple nodes concurrently using threads.

        This method executes the specified benchmark function on multiple nodes
        concurrently using a ThreadPoolExecutor. It handles keyboard interrupts
        gracefully.

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
                        logging.info(f"Benchmark completed for node {node}: {result['successful']}")
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
                logging.info(f"Benchmark completed for node {node}: {result['successful']}")
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

            return get_status_node(
                node,
                num_retries=num_retries,
                num_retries_call=num_retries_call,
                timeout=timeout,
                how_many_seconds=how_many_seconds,
            )

        if threading:
            return self._run_benchmark_threaded(nodes, config_wrapper, how_many_seconds)
        else:
            return self._run_benchmark_sequential(nodes, config_wrapper, how_many_seconds)

    def run_token_benchmark(self, nodes, how_many_seconds, token="SWAP.HIVE", threading=True):
        """Run token retrieval benchmark tests on multiple nodes.

        This method benchmarks how many token queries each node can handle within a specified time period.

        Args:
            nodes (list): List of node URLs to benchmark
            how_many_seconds (int): Time limit for benchmark in seconds
            token (str, optional): Token symbol to query. Defaults to "SWAP.HIVE".
            threading (bool, optional): Whether to run benchmarks concurrently. Defaults to True.

        Returns:
            list: List of benchmark results, one for each node
        """
        logging.info(f"Running token benchmark on {len(nodes)} nodes...")

        # Create a wrapper function that passes connection parameters properly
        def token_wrapper(node, how_many_seconds, **kwargs):
            # Extract connection parameters
            num_retries = kwargs.get("num_retries", self.num_retries)
            num_retries_call = kwargs.get("num_retries_call", self.num_retries_call)
            timeout = kwargs.get("timeout", self.timeout)

            return benchmark_token_retrieval(
                node,
                num_retries=num_retries,
                num_retries_call=num_retries_call,
                timeout=timeout,
                how_many_seconds=how_many_seconds,
                token=token,
            )

        if threading:
            return self._run_benchmark_threaded(nodes, token_wrapper, how_many_seconds)
        else:
            return self._run_benchmark_sequential(nodes, token_wrapper, how_many_seconds)

    def run_contract_benchmark(self, nodes, how_many_seconds, contract="tokens", threading=True):
        """Run contract retrieval benchmark tests on multiple nodes.

        This method benchmarks how many contract queries each node can handle within a specified time period.

        Args:
            nodes (list): List of node URLs to benchmark
            how_many_seconds (int): Time limit for benchmark in seconds
            contract (str, optional): Contract name to query. Defaults to "tokens".
            threading (bool, optional): Whether to run benchmarks concurrently. Defaults to True.

        Returns:
            list: List of benchmark results, one for each node
        """
        logging.info(f"Running contract benchmark on {len(nodes)} nodes...")

        # Create a wrapper function that passes connection parameters properly
        def contract_wrapper(node, how_many_seconds, **kwargs):
            # Extract connection parameters
            num_retries = kwargs.get("num_retries", self.num_retries)
            num_retries_call = kwargs.get("num_retries_call", self.num_retries_call)
            timeout = kwargs.get("timeout", self.timeout)

            return benchmark_contract_retrieval(
                node,
                num_retries=num_retries,
                num_retries_call=num_retries_call,
                timeout=timeout,
                how_many_seconds=how_many_seconds,
                contract=contract,
            )

        if threading:
            return self._run_benchmark_threaded(nodes, contract_wrapper, how_many_seconds)
        else:
            return self._run_benchmark_sequential(nodes, contract_wrapper, how_many_seconds)

    def run_account_history_benchmark(
        self, nodes, how_many_seconds, account_name="thecrazygm", threading=True
    ):
        """Run account history retrieval benchmark tests on multiple nodes.

        This method benchmarks how many account history queries each node can handle within a specified time period.

        Args:
            nodes (list): List of node URLs to benchmark
            how_many_seconds (int): Time limit for benchmark in seconds
            account_name (str, optional): Account name to query history for. Defaults to "thecrazygm".
            threading (bool, optional): Whether to run benchmarks concurrently. Defaults to True.

        Returns:
            list: List of benchmark results, one for each node
        """
        logging.info(f"Running account history benchmark on {len(nodes)} nodes...")

        # Create a wrapper function that passes connection parameters properly
        def history_wrapper(node, how_many_seconds, **kwargs):
            # Extract connection parameters
            num_retries = kwargs.get("num_retries", self.num_retries)
            num_retries_call = kwargs.get("num_retries_call", self.num_retries_call)
            timeout = kwargs.get("timeout", self.timeout)

            return benchmark_account_history(
                node,
                num_retries=num_retries,
                num_retries_call=num_retries_call,
                timeout=timeout,
                account_name=account_name,
                how_many_seconds=how_many_seconds,
            )

        if threading:
            return self._run_benchmark_threaded(nodes, history_wrapper, how_many_seconds)
        else:
            return self._run_benchmark_sequential(nodes, history_wrapper, how_many_seconds)

    def run_latency_benchmark(self, nodes, threading=True):
        """Run latency benchmark tests on multiple nodes.

        This method benchmarks the round-trip latency to each node.

        Args:
            nodes (list): List of node URLs to benchmark
            threading (bool, optional): Whether to run benchmarks concurrently. Defaults to True.

        Returns:
            list: List of benchmark results, one for each node
        """
        logging.info(f"Running latency benchmark on {len(nodes)} nodes...")

        # Create a wrapper function that passes connection parameters properly
        def latency_wrapper(node, **kwargs):
            # Extract connection parameters
            num_retries = kwargs.get("num_retries", self.num_retries)
            num_retries_call = kwargs.get("num_retries_call", self.num_retries_call)
            timeout = kwargs.get("timeout", self.timeout)

            return benchmark_latency(
                node,
                num_retries=num_retries,
                num_retries_call=num_retries_call,
                timeout=timeout,
            )

        if threading:
            return self._run_benchmark_threaded(nodes, latency_wrapper)
        else:
            return self._run_benchmark_sequential(nodes, latency_wrapper)
