#!/usr/bin/env python
"""Main entry point for the Hive node benchmarking application."""

import logging
from datetime import datetime, timezone

from nectar.hive import Hive
from nectar.instance import set_shared_blockchain_instance
from nectar.nodelist import NodeList

from hive_bench import __version__
from hive_bench.benchmarks import Benchmarks
from hive_bench.database import store_benchmark_data_in_db
from hive_bench.utils import INITIAL_NODES


def run_benchmarks(
    seconds=30,
    threading=True,
    account_name="thecrazygm",
    authorpermvoter="thecrazygm/still-lazy|steembasicincome",
    num_retries=3,
    num_retries_call=3,
    timeout=30,
):
    """Run all benchmark tests on Hive nodes.

    This function runs a complete set of benchmark tests on Hive nodes, including
    configuration retrieval, block retrieval, account history retrieval, API calls,
    and block synchronization status. It collects the results and returns a structured
    report that can be used for further analysis or storage.

    Args:
        seconds (int, optional): Time limit for each benchmark in seconds. Defaults to 30.
        threading (bool, optional): Whether to run benchmarks concurrently. Defaults to True.
        account_name (str, optional): Account name to retrieve history for. Defaults to "thecrazygm".
        authorpermvoter (str, optional): Post identifier for API call benchmark. Defaults to "thecrazygm/still-lazy|steembasicincome".
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
    # Get a list of working nodes
    hv = Hive(node=INITIAL_NODES)
    set_shared_blockchain_instance(hv)
    n = NodeList()
    nodes = INITIAL_NODES
    n.update(nodes)

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
    all_results["block"] = benchmarks.run_block_benchmark(nodes, seconds, threading=threading)
    all_results["history"] = benchmarks.run_hist_benchmark(
        nodes, seconds, threading=threading, account_name=account_name
    )
    all_results["apicall"] = benchmarks.run_call_benchmark(
        nodes, authorpermvoter, threading=threading
    )
    all_results["block_diff"] = benchmarks.run_block_diff_benchmark(nodes, threading=threading)

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
        "authorpermvoter": authorpermvoter,
    }

    # Import nectar version for report
    from nectar import __version__ as hive_nectar_version

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
                    "version": "0.0.0",
                    "hive": False,
                    "block": {"ok": False, "count": 0, "time": 0, "rank": -1},
                    "history": {"ok": False, "count": 0, "time": 0, "rank": -1},
                    "apicall": {
                        "ok": False,
                        "time": 0,
                        "access_time": 30.0,
                        "rank": -1,
                    },
                    "config": {"ok": False, "time": 0, "access_time": 0, "rank": -1},
                    "block_diff": {
                        "ok": False,
                        "head_delay": 0.0,
                        "diff_head_irreversible": 0.0,
                        "time": 0,
                        "rank": -1,
                    },
                }

            # Add version and hive flag if available (from config test)
            if test_name == "config" and result["successful"]:
                node_data[node]["version"] = result.get("version", "0.0.0")
                node_data[node]["hive"] = result.get("is_hive", False)

            # Update test data
            if result["successful"]:
                if test_name == "block":
                    node_data[node]["block"]["ok"] = True
                    node_data[node]["block"]["count"] = result["count"]
                    node_data[node]["block"]["time"] = result["total_duration"]
                elif test_name == "history":
                    node_data[node]["history"]["ok"] = True
                    node_data[node]["history"]["count"] = result["count"]
                    node_data[node]["history"]["time"] = result["total_duration"]
                elif test_name == "apicall":
                    node_data[node]["apicall"]["ok"] = True
                    node_data[node]["apicall"]["time"] = result["total_duration"]
                    node_data[node]["apicall"]["access_time"] = result["access_time"]
                elif test_name == "config":
                    node_data[node]["config"]["ok"] = True
                    node_data[node]["config"]["time"] = result["total_duration"]
                    node_data[node]["config"]["access_time"] = result["access_time"]
                elif test_name == "block_diff":
                    node_data[node]["block_diff"]["ok"] = True
                    node_data[node]["block_diff"]["head_delay"] = result.get("head_delay", 0.0)
                    node_data[node]["block_diff"]["diff_head_irreversible"] = result.get(
                        "diff_head_irreversible", 0.0
                    )
                    node_data[node]["block_diff"]["time"] = result["total_duration"]
            else:
                # Store error information for failing nodes
                error_msg = result.get("error", "")
                if error_msg and node not in failing_nodes:
                    failing_nodes[node] = error_msg

    # Determine working nodes (those not in failing_nodes)
    working_nodes = [node for node in all_nodes if node not in failing_nodes]

    # Import make_sort_key function for rankings
    from hive_bench.utils import make_sort_key

    # Calculate rankings for each test
    for ranking_test in ["block", "history", "apicall", "config", "block_diff"]:
        # Set reverse order for count-based tests
        reverse = ranking_test in ["block", "history"]

        # Sort and assign ranks
        rank = 1
        for node, data in sorted(
            node_data.items(), key=make_sort_key(ranking_test), reverse=reverse
        ):
            if data[ranking_test]["ok"]:
                node_data[node][ranking_test]["rank"] = rank
                rank += 1

    # Sort nodes by their config rank to ensure consistent ordering in the report
    # Create a dictionary to map ranks to nodes for precise ordering
    nodes_by_rank = {}

    # First, collect all nodes with valid config ranks
    for node, data in node_data.items():
        if data["config"]["ok"] and data["config"]["rank"] > 0:
            rank = data["config"]["rank"]
            nodes_by_rank[rank] = data

    # Create a sorted list of nodes
    sorted_node_data = []

    # Add nodes in order of rank (1, 2, 3, etc.)
    for rank in sorted(nodes_by_rank.keys()):
        sorted_node_data.append(nodes_by_rank[rank])

    # Add nodes without valid config ranks at the end
    for node, data in node_data.items():
        if not data["config"]["ok"] or data["config"]["rank"] <= 0:
            sorted_node_data.append(data)

    # Get the list of nodes in the same order as the sorted report
    sorted_node_urls = [node["node"] for node in sorted_node_data if node["node"] in working_nodes]

    # Build the final report structure
    report = {
        "nodes": sorted_node_urls,  # Use the sorted list of node URLs
        "failing_nodes": failing_nodes,
        "report": sorted_node_data,  # Use the explicitly sorted list instead of dict.values()
        "parameter": {
            "num_retries": num_retries,
            "num_retries_call": num_retries_call,
            "timeout": timeout,
            "threading": threading,
            "hive_nectar_version": hive_nectar_version,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "script_version": __version__,
            "benchmarks": {
                "block": {"data": ["count"]},
                "history": {"data": ["count"]},
                "apicall": {"data": ["access_time"]},
                "config": {"data": ["access_time"]},
                "block_diff": {"data": ["diff_head_irreversible", "head_delay"]},
            },
        },
    }

    return report


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
    logging.info(f"Starting Hive benchmarking application v{__version__}")

    # Run all benchmarks
    report = run_benchmarks()

    # Calculate weighted scores for each node based on real-world importance
    from hive_bench.utils import calculate_weighted_node_score

    # First, collect all node data for normalization
    all_node_data = report["report"]

    # Calculate weighted scores and add to node data
    node_scores = {}
    for node_data in all_node_data:
        weighted_score = calculate_weighted_node_score(node_data, all_node_data)
        node_scores[node_data["node"]] = weighted_score
        # Add weighted score to node data for future reference
        node_data["weighted_score"] = round(weighted_score, 2)

    # Create a better sorted list of nodes based on weighted scores
    better_sorted_nodes = [
        node
        for node, _ in sorted(node_scores.items(), key=lambda x: x[1], reverse=True)
        if node in report["nodes"]
    ]

    # Update the report with the better sorted node list
    report["nodes"] = better_sorted_nodes

    # Re-sort the report data based on the new ordering
    sorted_report_data = []

    # First add nodes in the better order
    for node in better_sorted_nodes:
        for node_data in all_node_data:
            if node_data["node"] == node:
                sorted_report_data.append(node_data)
                break

    # Then add any remaining nodes that weren't in the working nodes list
    for node_data in all_node_data:
        if node_data["node"] not in better_sorted_nodes:
            sorted_report_data.append(node_data)

    # Update the report with the better sorted data
    report["report"] = sorted_report_data

    # Add the weighted scoring methodology to the parameters
    report["parameter"]["weighted_scoring"] = {
        "weights": {
            "block": 0.30,
            "history": 0.25,
            "apicall": 0.25,
            "config": 0.05,
            "block_diff": 0.15,
        },
        "description": "Nodes are ordered by a weighted scoring system prioritizing real-world performance factors",
    }

    # Store benchmark data in the database
    store_benchmark_data_in_db(report)

    # Save the report to a JSON file
    import json
    import os
    from pathlib import Path

    # Get the absolute path to the project root directory
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent.parent

    # Define the output file path
    output_file = os.path.join(project_root, "hive_benchmark_results.json")

    with open(output_file, "w") as f:
        json.dump(report, f, indent=2)

    logging.info(f"Results saved to {output_file}")

    # Print summary information
    print("\nBenchmark run completed.")
    print(
        f"Tested {len(report['nodes'])} working nodes and {len(report['failing_nodes'])} failing nodes."
    )
    print("Results have been stored in the database and saved to benchmark_results.json.")
    print("\nTop 3 nodes by performance using weighted real-world scoring:")

    # Calculate weighted scores for each node based on real-world importance
    from hive_bench.utils import calculate_weighted_node_score

    # First, collect all node data for normalization
    all_node_data = report["report"]

    # Calculate weighted scores
    node_scores = {}
    for node_data in all_node_data:
        weighted_score = calculate_weighted_node_score(node_data, all_node_data)
        node_scores[node_data["node"]] = weighted_score

    # Create a better sorted list of nodes based on weighted scores
    better_sorted_nodes = [
        node
        for node, _ in sorted(node_scores.items(), key=lambda x: x[1], reverse=True)
        if node in report["nodes"]
    ]

    # Update the report with the better sorted node list
    report["nodes"] = better_sorted_nodes

    # Re-sort the report data based on the new ordering
    sorted_report_data = []

    # First add nodes in the better order
    for node in better_sorted_nodes:
        for node_data in all_node_data:
            if node_data["node"] == node:
                sorted_report_data.append(node_data)
                break

    # Then add any remaining nodes that weren't in the working nodes list
    for node_data in all_node_data:
        if node_data["node"] not in better_sorted_nodes:
            sorted_report_data.append(node_data)

    # Update the report with the better sorted data
    report["report"] = sorted_report_data

    # Print top 3 nodes with new scoring system
    for i, (node, score) in enumerate(
        sorted(node_scores.items(), key=lambda x: x[1], reverse=True)[:3]
        if len(node_scores) >= 3
        else sorted(node_scores.items(), key=lambda x: x[1], reverse=True)
    ):
        print(f"{i + 1}. {node} (weighted score: {score:.2f})")


if __name__ == "__main__":
    main()
