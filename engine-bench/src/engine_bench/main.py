#!/usr/bin/env python
"""Main entry point for the Hive-Engine node benchmarking application."""

import json
import logging
import os
from datetime import datetime, timezone

from engine_bench import __version__
from engine_bench.benchmarks import Benchmarks
from engine_bench.database import store_benchmark_data_in_db
from engine_bench.utils import INITIAL_NODES


def run_benchmarks(
    seconds=30,
    threading=True,
    account_name="thecrazygm",
    token="SWAP.HIVE",
    contract="tokens",
    num_retries=3,
    num_retries_call=3,
    timeout=30,
):
    """Run all benchmark tests on Hive-Engine nodes.

    This function runs a complete set of benchmark tests on Hive-Engine nodes, including
    configuration retrieval, token retrieval, contract retrieval, account history retrieval,
    and latency. It collects the results and returns a structured report that can be
    used for further analysis or storage.

    Args:
        seconds (int, optional): Time limit for each benchmark in seconds. Defaults to 30.
        threading (bool, optional): Whether to run benchmarks concurrently. Defaults to True.
        account_name (str, optional): Account name to retrieve history for. Defaults to "thecrazygm".
        token (str, optional): Token symbol to retrieve for token benchmark. Defaults to "SWAP.HIVE".
        contract (str, optional): Contract name to retrieve for contract benchmark. Defaults to "tokens".
        num_retries (int, optional): Number of connection retries. Defaults to 3.
        num_retries_call (int, optional): Number of API call retries. Defaults to 3.
        timeout (int, optional): Connection timeout in seconds. Defaults to 30.

    Returns:
        dict: A dictionary containing the benchmark report data with the following structure:
            - parameter (dict): Benchmark parameters including start_time, end_time, versions, etc.
            - report (list): List of dictionaries containing data for working nodes
            - failing_nodes (dict): Dictionary mapping node URLs to error messages
            - nodes (list): List of working node URLs
    """
    # Get node list from file or environment variable if available
    nodes = INITIAL_NODES
    nodes_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "h-e-nodes.txt"
    )
    if os.path.exists(nodes_file):
        with open(nodes_file, "r") as f:
            nodes = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    # Initialize the benchmark class
    benchmarks = Benchmarks(
        num_retries=num_retries, num_retries_call=num_retries_call, timeout=timeout
    )

    # Track results and failing nodes
    all_results = {}
    failing_nodes = {}

    # Record start time in UTC
    start_time = datetime.now(timezone.utc)
    logging.info(f"Starting benchmark run at {start_time.isoformat()}")

    # Run all benchmark tests
    logging.info("Running all benchmark tests...")

    # Run all benchmarks
    all_results["config"] = benchmarks.run_config_benchmark(nodes, seconds, threading=threading)
    all_results["token"] = benchmarks.run_token_benchmark(
        nodes, seconds, token=token, threading=threading
    )
    all_results["contract"] = benchmarks.run_contract_benchmark(
        nodes, seconds, contract=contract, threading=threading
    )
    all_results["account_history"] = benchmarks.run_account_history_benchmark(
        nodes, seconds, account_name=account_name, threading=threading
    )
    all_results["latency"] = benchmarks.run_latency_benchmark(nodes, threading=threading)

    # Record end time in UTC
    end_time = datetime.now(timezone.utc)
    logging.info(f"Finished benchmark run at {end_time.isoformat()}")
    logging.info(f"Total duration: {(end_time - start_time).total_seconds()} seconds")

    # Add timestamp and parameters to results
    all_results["timestamp"] = datetime.now().isoformat()
    all_results["parameters"] = {
        "num_retries": num_retries,
        "num_retries_call": num_retries_call,
        "timeout": timeout,
        "threading": threading,
        "seconds": seconds,
        "account_name": account_name,
        "token": token,
        "contract": contract,
    }

    # Import nectarengine version for report
    from nectarengine import __version__ as nectar_engine_version

    # Process results to create report structure
    node_data = {}
    all_nodes = set()

    # Extract node information and identify failing/working nodes
    for test_name, test_results in all_results.items():
        if test_name in ["timestamp", "parameters"]:
            continue

        if not isinstance(test_results, list):
            logging.warning(
                f"Unexpected data format for {test_name}: expected list, got {type(test_results)}"
            )
            continue

        for result in test_results:
            if not isinstance(result, dict):
                logging.warning(
                    f"Unexpected result format in {test_name}: expected dict, got {type(result)}"
                )
                continue

            if "node" not in result:
                logging.warning(f"Missing node information in result for {test_name}")
                continue

            node = result["node"]
            all_nodes.add(node)

            # Initialize node data structure if needed
            if node not in node_data:
                node_data[node] = {
                    "node": node,
                    "SSCnodeVersion": "unknown",
                    "engine": False,
                    "token": {"ok": False, "count": 0, "time": 0, "rank": -1},
                    "contract": {"ok": False, "count": 0, "time": 0, "rank": -1},
                    "account_history": {"ok": False, "count": 0, "time": 0, "rank": -1},
                    "config": {"ok": False, "time": 0, "access_time": 0, "rank": -1},
                    "latency": {
                        "ok": False,
                        "min_latency": 0.0,
                        "max_latency": 0.0,
                        "avg_latency": 0.0,
                        "time": 0,
                        "rank": -1,
                    },
                }

            # Add version and engine flag if available (from config test)
            if test_name == "config" and result["successful"]:
                # Patch: Use sscnodeversion from result, fallback to unknown
                sscnodeversion = result.get("sscnodeversion", "unknown")
                node_data[node]["SSCnodeVersion"] = sscnodeversion
                node_data[node]["engine"] = result.get("is_engine", False)

            # Update test data
            if result["successful"]:
                if test_name == "token":
                    node_data[node]["token"]["ok"] = True
                    node_data[node]["token"]["count"] = result["count"]
                    node_data[node]["token"]["time"] = result["total_duration"]
                elif test_name == "contract":
                    node_data[node]["contract"]["ok"] = True
                    node_data[node]["contract"]["count"] = result["count"]
                    node_data[node]["contract"]["time"] = result["total_duration"]
                elif test_name == "account_history":
                    node_data[node]["account_history"]["ok"] = True
                    node_data[node]["account_history"]["count"] = result["count"]
                    node_data[node]["account_history"]["time"] = result["total_duration"]
                elif test_name == "config":
                    node_data[node]["config"]["ok"] = True
                    node_data[node]["config"]["time"] = result["total_duration"]
                    node_data[node]["config"]["access_time"] = result["access_time"]
                elif test_name == "latency":
                    node_data[node]["latency"]["ok"] = True
                    node_data[node]["latency"]["min_latency"] = result.get("min_latency", 0.0)
                    node_data[node]["latency"]["max_latency"] = result.get("max_latency", 0.0)
                    node_data[node]["latency"]["avg_latency"] = result.get("avg_latency", 0.0)
                    node_data[node]["latency"]["time"] = result["total_duration"]
            else:
                # Store error information for failing nodes
                error_msg = result.get("error", "")
                if error_msg and node not in failing_nodes:
                    failing_nodes[node] = error_msg

    # Determine working nodes (those not in failing_nodes)
    working_nodes = [node for node in all_nodes if node not in failing_nodes]

    # Calculate rankings for each test type
    for test_type in ["token", "contract", "account_history", "config", "latency"]:
        # Get the ranking metric for each test type
        if test_type == "token" or test_type == "contract" or test_type == "account_history":
            # For count-based tests, higher is better
            nodes_ranked = sorted(
                [n for n in node_data.values() if n[test_type]["ok"]],
                key=lambda x: (-x[test_type]["count"], x[test_type]["time"]),
            )
        elif test_type == "config":
            # For config, faster is better
            nodes_ranked = sorted(
                [n for n in node_data.values() if n[test_type]["ok"]],
                key=lambda x: x[test_type]["time"],
            )
        elif test_type == "latency":
            # For latency, lower is better
            nodes_ranked = sorted(
                [n for n in node_data.values() if n[test_type]["ok"]],
                key=lambda x: x[test_type]["avg_latency"],
            )

        # Assign rankings
        for i, node in enumerate(nodes_ranked):
            node_data[node["node"]][test_type]["rank"] = i + 1

    # Calculate weighted scores for nodes based on real-world performance metrics
    # Higher score is better with this new weighted system
    from engine_bench.utils import calculate_weighted_node_score

    # Collect all node data for normalization
    all_node_data = list(node_data.values())

    # Calculate weighted scores and add to node data
    for data in all_node_data:
        weighted_score = calculate_weighted_node_score(data, all_node_data)
        # Add weighted score to node data for future reference
        data["weighted_score"] = round(weighted_score, 2)
        # Keep track of how many tests were completed
        data["tests_completed"] = sum(
            1
            for test_type in ["token", "contract", "account_history", "config", "latency"]
            if data[test_type]["ok"]
        )

    # Create a sorted list of nodes by weighted score (higher is better)
    sorted_node_data = sorted(
        all_node_data,
        key=lambda x: (x.get("weighted_score", 0), x.get("node", "")),
        reverse=True,  # Higher score is better
    )

    # Create report structure using the sorted nodes
    report = sorted_node_data

    # Get the list of nodes in the same order as the sorted report
    sorted_node_urls = [node["node"] for node in report if node["node"] in working_nodes]

    # Create benchmark report data structure
    report_data = {
        "nodes": sorted_node_urls,  # Use the sorted list of node URLs
        "failing_nodes": failing_nodes,
        "report": report,
        "parameter": {
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration": (end_time - start_time).total_seconds(),
            "timestamp": datetime.now().isoformat(),
            "nectar_engine_version": nectar_engine_version,
            "script_version": __version__,
            **all_results["parameters"],
            "benchmarks": {
                "token": {"data": ["count"]},
                "contract": {"data": ["count"]},
                "account_history": {"data": ["count"]},
                "config": {"data": ["access_time"]},
                "latency": {"data": ["avg_latency"]},
            },
        },
    }

    # Save report data to engine_benchmark_results.json
    with open(
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "engine_benchmark_results.json",
        ),
        "w",
    ) as f:
        json.dump(report_data, f, indent=2)

    return report_data


def main():
    """Main entry point for the benchmarking application.

    This function runs a complete benchmark test and stores the results in the database.
    It also prints summary information to the console.
    """
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Log application startup
    logging.info(f"Starting Hive-Engine benchmarking application v{__version__}")

    # Run benchmarks and get report data
    report_data = run_benchmarks()

    # Store benchmark data in database
    store_benchmark_data_in_db(report_data)

    # Print summary information
    print("\nBenchmark Summary:")
    print(f"  Start time: {report_data['parameter']['start_time']}")
    print(f"  End time: {report_data['parameter']['end_time']}")
    print(f"  Duration: {report_data['parameter']['duration']:.2f} seconds")
    print(f"  Working nodes: {len(report_data['nodes'])}")
    print(f"  Failing nodes: {len(report_data['failing_nodes'])}")

    # Display top 5 nodes by weighted score (real-world performance importance)
    print("\nTop 5 nodes by weighted score (higher is better):")
    # Sort nodes by weighted score (higher is better)
    for i, node in enumerate(report_data["nodes"][:5]):
        # Find the node data
        for node_data in report_data["report"]:
            if node_data["node"] == node:
                weighted_score = node_data.get("weighted_score", 0)
                tests_completed = node_data.get("tests_completed", 0)
                print(
                    f"  {i + 1}. {node} (weighted score: {weighted_score:.2f}, tests completed: {tests_completed}/5)"
                )
                break

    # Display top 5 nodes for each test type
    test_types = ["token", "contract", "account_history", "config", "latency"]
    for test_type in test_types:
        print(f"\nTop 5 nodes for {test_type} benchmark:")
        # Sort nodes by rank for this test type
        sorted_nodes = sorted(
            [n for n in report_data["report"] if n[test_type]["ok"]],
            key=lambda x: x[test_type]["rank"],
        )
        for i, node in enumerate(sorted_nodes[:5]):
            print(f"  {i + 1}. {node['node']}")

    logging.info("Benchmark run completed successfully")


if __name__ == "__main__":
    main()
