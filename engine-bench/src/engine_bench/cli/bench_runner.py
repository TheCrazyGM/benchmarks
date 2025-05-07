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
                    # Apply weighted scoring logic to ensure nodes are sorted by real-world performance
                    if "report" in report and isinstance(report["report"], list):
                        from engine_bench.utils import calculate_weighted_node_score

                        all_node_data = report["report"]

                        # Calculate weighted scores for each node
                        for node_data in all_node_data:
                            weighted_score = calculate_weighted_node_score(node_data, all_node_data)
                            # Add weighted score to node data for future reference
                            node_data["weighted_score"] = round(weighted_score, 2)
                            # Keep track of how many tests were completed
                            node_data["tests_completed"] = sum(
                                1
                                for test_type in [
                                    "token",
                                    "contract",
                                    "account_history",
                                    "config",
                                    "latency",
                                ]
                                if node_data[test_type].get("ok", False)
                            )

                        # Create a sorted list of nodes by weighted score (higher is better)
                        sorted_nodes = sorted(
                            all_node_data,
                            key=lambda x: (x.get("weighted_score", 0), x.get("node", "")),
                            reverse=True,  # Higher score is better with weighted system
                        )

                        # Update the report with the better sorted nodes
                        report["report"] = sorted_nodes

                        # Also sort the nodes list to match the report order
                        sorted_node_urls = [
                            node["node"]
                            for node in sorted_nodes
                            if node["node"] in report.get("nodes", [])
                        ]
                        report["nodes"] = sorted_node_urls

                        # Add the weighted scoring methodology to the parameters if not already there
                        if "parameter" in report and "weighted_scoring" not in report["parameter"]:
                            report["parameter"]["weighted_scoring"] = {
                                "weights": {
                                    "token": 0.25,
                                    "contract": 0.20,
                                    "account_history": 0.20,
                                    "latency": 0.25,
                                    "config": 0.10,
                                },
                                "description": "Nodes are ordered by a weighted scoring system prioritizing real-world performance factors",
                            }

                        logging.info(
                            "Applied weighted scoring to ensure nodes are ordered by real-world performance importance"
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
        # Create dictionaries for weighted scores and test completion
        weighted_scores = {}
        tests_completed = {}

        # Process the report data to find weighted scores
        for node_data in report["report"]:
            if isinstance(node_data, dict) and "node" in node_data:
                node_url = node_data["node"]
                # Use weighted_score if available, otherwise calculate it
                if "weighted_score" in node_data:
                    weighted_scores[node_url] = node_data["weighted_score"]
                    tests_completed[node_url] = node_data.get("tests_completed", 0)
                else:
                    # If no weighted score in report, default to 0
                    weighted_scores[node_url] = 0
                    # Count completed tests
                    tests_completed[node_url] = sum(
                        1
                        for test_type in [
                            "token",
                            "contract",
                            "account_history",
                            "config",
                            "latency",
                        ]
                        if node_data.get(test_type, {}).get("ok", False)
                    )

        # If no weighted scores at all, try to get them from nodes list
        if not weighted_scores and isinstance(report["nodes"], list):
            for node_url in report["nodes"]:
                if isinstance(node_url, str):
                    weighted_scores[node_url] = 0  # Default score
                    tests_completed[node_url] = 0
                    logging.debug(f"Using fallback for node: {node_url}")

        # Print top 5 nodes by weighted score (higher is better)
        print("\nTop performing nodes by weighted score (higher is better):")
        # Sort by weighted score (descending - higher is better)
        sorted_nodes = sorted(weighted_scores.items(), key=lambda x: x[1], reverse=True)[:5]

        if sorted_nodes:
            for i, (node, score) in enumerate(sorted_nodes):
                tests = tests_completed.get(node, 0)
                print(f"{i + 1}. {node} (weighted score: {score:.2f}, tests completed: {tests}/5)")
        else:
            print("No node performance data available.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
