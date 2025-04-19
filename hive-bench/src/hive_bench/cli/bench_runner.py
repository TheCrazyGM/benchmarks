#!/usr/bin/env python
"""Runner script for benchmarking Hive nodes."""

import argparse
import json
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

from hive_bench.blockchain import update_json_metadata
from hive_bench.database import store_benchmark_data_in_db
from hive_bench.main import run_benchmarks


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Benchmark Hive nodes")
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
        default="thecrazygm",
        help="Account name to use for history benchmark (default: thecrazygm)",
    )
    parser.add_argument(
        "--post",
        "-p",
        type=str,
        default="thecrazygm/still-lazy|steembasicincome",
        help="Post identifier for API call benchmark (default: thecrazygm/still-lazy|steembasicincome)",
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
        default="hive_benchmark_results.json",
        help="Output file for benchmark results (JSON format) (default: hive_benchmark_results.json)",
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
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
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
                    with open(output_path, "w") as f:
                        json.dump(report, f, indent=2)
                    logging.info(f"Copied report to {output_path}")
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
                account_name=args.account,
                authorpermvoter=args.post,
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
        # Create a dictionary of nodes with their scores and tests completed
        node_scores = {}
        node_tests_completed = {}

        # Process the report data
        for node_data in report["report"]:
            if isinstance(node_data, dict) and "node" in node_data:
                node_url = node_data["node"]

                # Calculate score (lower is better, like in engine-bench)
                score = 0
                tests_completed = 0

                for test in ["block", "history", "apicall", "config", "block_diff"]:
                    test_data = node_data.get(test, {})
                    if test_data.get("ok", False) and test_data.get("rank", -1) > 0:
                        tests_completed += 1
                        # Convert score to be in line with engine-bench (lower is better)
                        # Normalize by max_rank and convert to 0-100 scale
                        max_rank = max(
                            [
                                d.get(test, {}).get("rank", 0)
                                for d in report["report"]
                                if isinstance(d, dict) and d.get(test, {}).get("ok", False)
                            ]
                            or [1]
                        )
                        score += (
                            test_data.get("rank", max_rank) / max_rank
                        ) * 20  # 5 tests * 20 = 100 max

                # Store the final score
                if tests_completed > 0:
                    node_scores[node_url] = round(score / tests_completed, 2)
                    node_tests_completed[node_url] = tests_completed
                else:
                    node_scores[node_url] = 999  # Default high score
                    node_tests_completed[node_url] = 0

        # Print top nodes with scores
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
