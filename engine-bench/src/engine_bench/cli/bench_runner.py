#!/usr/bin/env python
"""Runner script for benchmarking Hive-Engine nodes."""

import argparse
import json
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

from engine_bench.blockchain import update_json_metadata
from engine_bench.database import store_benchmark_data_in_db
from engine_bench.main import run_benchmarks


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Benchmark Hive-Engine nodes")
    parser.add_argument(
        "--seconds",
        "-s",
        type=int,
        default=30,
        help="Time limit for each benchmark in seconds (default: 30)",
    )
    parser.add_argument(
        "--no-threading", action="store_true", help="Disable threading for benchmarks"
    )
    parser.add_argument(
        "--account",
        "-a",
        type=str,
        help="Account name to use for history benchmark",
    )
    parser.add_argument(
        "--retries",
        "-r",
        type=int,
        default=3,
        help="Number of connection retries (default: 3)",
    )
    parser.add_argument(
        "--call-retries",
        "-c",
        type=int,
        default=3,
        help="Number of API call retries (default: 3)",
    )
    parser.add_argument(
        "--timeout",
        "-t",
        type=int,
        default=30,
        help="Connection timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="engine_benchmark_results.json",
        help="Output file for benchmark results (JSON format)",
    )
    parser.add_argument(
        "--report-file",
        "-f",
        type=str,
        help="Existing report file to use instead of running benchmarks",
    )
    parser.add_argument("--no-db", action="store_true", help="Do not store results in database")
    parser.add_argument(
        "--update-metadata",
        "-u",
        action="store_true",
        help="Update account JSON metadata with benchmark results",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    return parser.parse_args()


def main():
    """Main entry point for benchmarking script."""
    # Load environment variables from .env file
    load_dotenv()

    args = parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")

    report = None
    output_path = None

    # If a report file is provided, load it instead of running benchmarks
    if args.report_file:
        try:
            report_path = Path(args.report_file)
            if not report_path.exists():
                logging.error(f"Report file not found: {report_path}")
                return 1

            logging.info(f"Loading report from {report_path}")
            with open(report_path, "r") as f:
                report = json.load(f)

            # If --output is specified, use it as the output path
            if args.output:
                output_path = Path(args.output)
                if report_path != output_path:  # Only save if it's a different file
                    # Apply sorting logic to ensure nodes are in correct order by total score
                    if "report" in report and isinstance(report["report"], list):
                        # Sort nodes by their total score (lower is better)
                        nodes_by_score = {}

                        # First, collect all nodes with valid scores
                        for node in report["report"]:
                            if "total_score" in node:
                                score = node["total_score"]
                                if score not in nodes_by_score:
                                    nodes_by_score[score] = []
                                nodes_by_score[score].append(node)

                        # Create a sorted list of nodes
                        sorted_nodes = []

                        # Add nodes in order of score (lower is better)
                        for score in sorted(nodes_by_score.keys()):
                            # If multiple nodes have the same score, sort them by node URL for consistency
                            for node in sorted(nodes_by_score[score], key=lambda x: x["node"]):
                                sorted_nodes.append(node)

                        # Add nodes without valid scores at the end
                        for node in report["report"]:
                            if "total_score" not in node:
                                sorted_nodes.append(node)

                        # Update the report with sorted nodes
                        report["report"] = sorted_nodes

                        # Also sort the nodes list to match the report order
                        sorted_node_urls = [
                            node["node"]
                            for node in sorted_nodes
                            if node["node"] in report.get("nodes", [])
                        ]
                        report["nodes"] = sorted_node_urls

                        logging.info(
                            "Applied sorting to ensure nodes are in benchmark order (best score first)"
                        )

                    with open(output_path, "w") as f:
                        json.dump(report, f, indent=2)
                    logging.info(f"Sorted report saved to {output_path}")
                else:
                    output_path = report_path
            else:
                output_path = report_path
        except Exception as e:
            logging.error(f"Failed to load report file: {e}")
            return 1

    # Run benchmarks if no report file was provided or if --update-metadata was not specified
    if report is None and (not args.update_metadata or not args.report_file):
        logging.info("Running benchmarks...")
        try:
            report = run_benchmarks(
                seconds=args.seconds,
                threading=not args.no_threading,
                num_retries=args.retries,
                num_retries_call=args.call_retries,
                timeout=args.timeout,
            )
        except Exception as err:
            logging.error(f"Benchmark execution failed: {err}")
            return 1

        # Store results in database if requested
        if not args.no_db:
            store_benchmark_data_in_db(report)
            logging.info("Results stored in database.")

        # Output results to file if requested
        if args.output:
            output_path = Path(args.output)
            with open(output_path, "w") as f:
                json.dump(report, f, indent=2)
            logging.info(f"Results written to {output_path}")
    elif report is None:
        logging.error("No report file provided and no benchmarks run.")
        return 1

    # Update the JSON metadata if requested
    if args.update_metadata:
        try:
            account = args.account
            logging.info(f"Updating JSON metadata{f' for account {account}' if account else ''}...")
            tx = update_json_metadata(report, account=account)
            logging.info(f"Updated JSON metadata: {tx}")
        except Exception as e:
            logging.error(f"Failed to update JSON metadata: {e}")
            return 1

    # Print summary to console
    print("\nBenchmark Summary:")
    print(
        f"Tested {len(report['nodes'])} working nodes and {len(report['failing_nodes'])} failing nodes"
    )

    if report["nodes"]:
        print("\nTop performing nodes:")
        # Create a dictionary of nodes with their scores and tests completed
        node_scores = {}
        node_tests_completed = {}
        # Process the report data
        for node_data in report["report"]:
            # Check if node_data is a dictionary
            if isinstance(node_data, dict) and "node" in node_data:
                node_url = node_data["node"]
                score = node_data.get(
                    "total_score", 999
                )  # Default to high score (worse) if not found
                tests_completed = node_data.get("tests_completed", 0)
                node_scores[node_url] = score
                node_tests_completed[node_url] = tests_completed
        # If no scores were found in the report data, try using the nodes list as a fallback
        if not node_scores and isinstance(report["nodes"], list):
            for node_url in report["nodes"]:
                if isinstance(node_url, str):
                    node_scores[node_url] = 999  # Default high score
                    node_tests_completed[node_url] = 0
                    logging.debug(f"Using fallback for node: {node_url}")
        # Print top 5 nodes or all if fewer than 5 (lower score is better)
        print("\nTop performing nodes (lower score is better):")
        # Sort by score (ascending - lower is better)
        sorted_nodes = sorted(node_scores.items(), key=lambda x: x[1])[:5]
        if sorted_nodes:
            for i, (node, score) in enumerate(sorted_nodes):
                tests = node_tests_completed.get(node, 0)
                print(f"{i + 1}. {node} (score: {score}, tests completed: {tests}/5)")
        else:
            print("No node performance data available.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
