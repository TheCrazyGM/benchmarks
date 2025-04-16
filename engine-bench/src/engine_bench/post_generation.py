"""Post generation functionality for hive-engine benchmark results."""

import json
import logging
import os
import sqlite3
import statistics
from datetime import datetime, timedelta

# Reuse the format_float from utils
from engine_bench.utils import format_float


def get_historical_data(db_path, days=7):
    """Get historical data from SQLite database for report generation.

    This function retrieves node performance data from the SQLite database
    for generating historical trends and analysis.

    Args:
        db_path (str): Path to the SQLite database file.
        days (int, optional): Number of days of history to retrieve. Defaults to 7.

    Returns:
        dict: A dictionary containing historical data with the following structure:
            {"trends": {node_url: {test_type: {...trend data...}}},
             "consistency": {node_url: {test_type: value}},
             "uptime": {node_url: {"success": count, "total": count}}}
    """
    # Initialize return dict
    historical_data = {"trends": {}, "consistency": {}, "uptime": {}}

    # Connect to database
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Calculate timestamp threshold (days ago from now)
    threshold = (
        datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        if days <= 0
        else (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S")
    )

    # For a single day of data, include all data in the database to have more data points
    if days <= 1:
        # Count how many benchmark runs we have
        cursor.execute("SELECT COUNT(*) FROM benchmark_runs")
        count = cursor.fetchone()[0]

        if count <= 3:  # If we have very limited data, use all of it
            threshold = "0000-01-01T00:00:00"  # A date far in the past

    # Collect node uptime statistics
    node_uptime = {}
    cursor.execute(
        """
        SELECT n.url as url,
            COUNT(CASE WHEN ns.is_working = 1 THEN 1 END) as up_count,
            COUNT(*) as total_count
        FROM nodes n
        JOIN node_status ns ON n.node_id = ns.node_id
        JOIN benchmark_runs br ON ns.run_id = br.run_id
        WHERE br.timestamp > ?
        GROUP BY n.url
        """,
        (threshold,),
    )

    for row in cursor.fetchall():
        node_uptime[row["url"]] = {
            "uptime_percent": format_float((row["up_count"] / row["total_count"]) * 100)
            if row["total_count"] > 0
            else 0,
            "total_runs": row["total_count"],
        }

    # Get a list of all nodes (including those that never worked)
    cursor.execute("""SELECT url as url FROM nodes""")
    all_nodes = [row["url"] for row in cursor.fetchall()]

    # Add nodes that don't have any records in the uptime data
    # These are likely consistently failing nodes
    for node_url in all_nodes:
        if node_url not in node_uptime:
            node_uptime[node_url] = {
                "uptime_percent": 0,
                "total_runs": 0,  # Zero runs means it was never tested successfully
            }

    # Collect node performance trends
    node_trends = {}
    for test_type in ["token", "contract", "account_history", "config", "latency"]:
        cursor.execute(
            """
            SELECT n.url as url, tr.test_type as test_type, tr.rank,
                br.timestamp
            FROM test_results tr
            JOIN nodes n ON tr.node_id = n.node_id
            JOIN benchmark_runs br ON tr.run_id = br.run_id
            WHERE tr.test_type = ? AND br.timestamp > ?
            ORDER BY br.timestamp
            """,
            (test_type, threshold),
        )

        results = cursor.fetchall()

        for row in results:
            if row["url"] not in node_trends:
                node_trends[row["url"]] = {}

            if test_type not in node_trends[row["url"]]:
                node_trends[row["url"]][test_type] = []

            node_trends[row["url"]][test_type].append(
                {"rank": row["rank"], "timestamp": row["timestamp"]}
            )

    # Add failing nodes to the trends data
    cursor.execute(
        """
        SELECT DISTINCT n.url as url
        FROM nodes n
        JOIN node_status ns ON n.node_id = ns.node_id
        JOIN benchmark_runs br ON ns.run_id = br.run_id
        WHERE br.timestamp > ? AND ns.is_working = 0
        """,
        (threshold,),
    )

    failing_nodes = [row["url"] for row in cursor.fetchall()]

    # Calculate trend indicators
    for node, tests in node_trends.items():
        for test_type, results in tests.items():
            if len(results) < 2:
                continue

            # Skip if already processed
            if not isinstance(results, list):
                continue

            # Sort by timestamp
            sorted_results = sorted(results, key=lambda x: x["timestamp"])

            # Get first and last rank
            first_rank = sorted_results[0]["rank"]
            last_rank = sorted_results[-1]["rank"]

            # Calculate average rank
            ranks = [r["rank"] for r in sorted_results]
            avg_rank = format_float(sum(ranks) / len(ranks))

            # Determine trend direction
            if last_rank < first_rank:
                trend = "improving"  # Lower rank is better
            elif last_rank > first_rank:
                trend = "degrading"
            else:
                trend = "stable"

            # Add trend data
            node_trends[node][test_type] = {
                "first_rank": first_rank,
                "last_rank": last_rank,
                "avg_rank": avg_rank,
                "trend": trend,
                "change": first_rank - last_rank,  # Positive means improvement
            }

    # Handle failing nodes separately
    for node_url in failing_nodes:
        if node_url not in node_trends:
            node_trends[node_url] = {}

        # Mark failing nodes as 'failing' instead of 'stable'
        for test_type in ["token", "contract", "account_history", "config", "latency"]:
            if test_type not in node_trends[node_url]:
                node_trends[node_url][test_type] = {
                    "first_rank": 0,
                    "last_rank": 0,
                    "avg_rank": 0,
                    "trend": "failing",  # Changed from 'stable' to 'failing'
                    "change": 0,
                }

    # Get consistency metrics (standard deviation of ranks)
    node_consistency = {}
    for test_type in ["token", "contract", "account_history", "config", "latency"]:
        cursor.execute(
            """
            SELECT n.url as url, tr.test_type as test_type, tr.rank
            FROM test_results tr
            JOIN nodes n ON tr.node_id = n.node_id
            JOIN benchmark_runs br ON tr.run_id = br.run_id
            WHERE tr.test_type = ? AND br.timestamp > ?
            ORDER BY br.timestamp
            """,
            (test_type, threshold),
        )

        results = cursor.fetchall()

        # Group by node
        node_ranks = {}
        for row in results:
            if row["url"] not in node_ranks:
                node_ranks[row["url"]] = []
            node_ranks[row["url"]].append(row["rank"])

        # Calculate consistency (standard deviation) for each node
        for node, ranks in node_ranks.items():
            if len(ranks) >= 2:  # Need at least 2 values for stdev
                if node not in node_consistency:
                    node_consistency[node] = {}
                node_consistency[node][test_type] = format_float(
                    statistics.stdev(ranks) if len(ranks) > 1 else 0
                )

    # Combine all historical data
    historical_data["trends"] = node_trends
    historical_data["consistency"] = node_consistency
    historical_data["uptime"] = node_uptime

    conn.close()
    return historical_data


def get_latest_benchmark_data(db_path="engine_benchmark_history.db"):
    """Get the latest benchmark data from the SQLite database.

    This function queries the database for the latest benchmark run and formats
    the data to match the structure expected by the generate_markdown function.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        dict: A dictionary containing benchmark data with the same structure as
            that produced by run_benchmarks() and saved in engine_results.json
    """
    from engine_bench.database import get_latest_benchmark_data as get_data

    return get_data(db_path)


def generate_markdown(benchmark_data, output_file=None, historical_data=None, days=7):
    """Generate a markdown post from benchmark data.

    This function creates a formatted markdown document from benchmark results,
    including sections for failing nodes, working nodes, node statistics, and
    comparisons. It can incorporate historical data for trend analysis.

    Args:
        benchmark_data (dict): A dictionary containing benchmark results
        output_file (str, optional): Path to save the markdown output. Defaults to None.
        historical_data (dict, optional): Historical data from database. Defaults to None.
        days (int, optional): Number of days of historical data. Defaults to 7.

    Returns:
        tuple: A tuple containing:
            - markdown_content (str): The generated markdown content
            - metadata (dict): Metadata about the generated content
    """
    if not benchmark_data:
        logging.error("No benchmark data provided")
        return "No benchmark data available.", {}

    # Extract timestamp from data or use current time
    timestamp = benchmark_data.get("timestamp", datetime.now().isoformat())
    formatted_date = datetime.fromisoformat(timestamp).strftime("%d/%m/%Y")

    # Extract data from the benchmark_data dictionary
    params = benchmark_data.get("parameter", {})
    report = benchmark_data.get("report", [])
    failing_nodes = benchmark_data.get("failing_nodes", {})
    nodes = benchmark_data.get("nodes", [])

    # Prepare metadata for json_metadata
    metadata = {
        "benchmark": {
            "timestamp": params.get("timestamp", ""),
            "node_count": len(nodes),
            "failing_count": len(failing_nodes),
            "nectar_engine_SSCnodeVersion": params.get("nectar_engine_SSCnodeVersion", ""),
            "script_SSCnodeVersion": params.get("script_SSCnodeVersion", ""),
        }
    }

    # Add top nodes for each test type to metadata
    for test_type in ["token", "contract", "account_history", "config", "latency"]:
        # Sort nodes by rank for this test type
        sorted_nodes = sorted(
            [n for n in report if n[test_type]["ok"]],
            key=lambda x: x[test_type]["rank"],
        )

        # Add top 3 nodes to metadata
        metadata["benchmark"][f"top_{test_type}"] = [
            {"url": node["node"], "rank": node[test_type]["rank"]} for node in sorted_nodes[:3]
        ]

    # Create markdown content list
    markdown = []

    # Header
    markdown.append(f"# Full Hive-Engine API Node Update - ({formatted_date})\n")
    current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    markdown.append(f"{current_time} (UTC)")
    markdown.append(
        "@nectarflower provides daily updates about the state of all available full API node servers for Hive-Engine."
    )
    markdown.append(
        "More information about nectarflower can be found in the [github repository](https://github.com/thecrazygm/nectarflower-bench).\n"
    )

    # Failing nodes section
    markdown.append("## List of failing nodes\n")
    markdown.append(
        "This table includes a list of all nodes which were not able to answer to a `getStatus` API call within the specified timeout.\n"
    )

    markdown.append("|node | error |")
    markdown.append("| --- | --- |")

    for node, error in failing_nodes.items():
        # Truncate error message if too long
        error_msg = error if len(error) < 100 else error[:97] + "..."
        markdown.append(f"| <{node}> | {error_msg} |")

    markdown.append("\n")

    # Working nodes section - config time
    markdown.append("## List of working nodes (At least once)\n")
    markdown.append(
        "This table includes all nodes which were able to answer a `getStatus` call within the timeout. The achieved mean duration values are shown. The returned SSCnodeVersion is also shown.\n"
    )

    markdown.append("|node |  mean time [s] | SSCnodeVersion  |")
    markdown.append("| --- | --- | ---  |")

    # Sort nodes by config time (ascending)
    config_sorted_nodes = sorted(
        [node for node in report if node.get("config", {}).get("ok", False)],
        key=lambda x: x["config"]["access_time"],
    )

    for node_data in config_sorted_nodes:
        node = node_data["node"]
        SSCnodeVersion = node_data.get("SSCnodeVersion", "unknown")
        config_time = format_float(node_data["config"]["access_time"])
        markdown.append(f"| <{node}> | {config_time} | {SSCnodeVersion} |")

    markdown.append("\n")

    # Node Uptime Statistics (if historical data available)
    if historical_data and historical_data.get("uptime"):
        markdown.append("## Node Uptime Statistics (7-day period)\n")
        markdown.append("This table shows how reliable nodes have been over the past week.\n")

        markdown.append("| node | uptime % | total checks |")
        markdown.append("| --- | --- | --- |")

        # Sort by uptime percentage (descending)
        sorted_uptime = sorted(
            historical_data["uptime"].items(),
            key=lambda x: x[1]["uptime_percent"],
            reverse=True,
        )

        for node_url, uptime_data in sorted_uptime:
            markdown.append(
                f"| <{node_url}> | {uptime_data['uptime_percent']}% | {uptime_data['total_runs']} |"
            )

        markdown.append("\n")

    # Token retrieval section
    markdown.append("## Token retrieval\n")
    markdown.append(
        "This table shows how many token operations were processed by the node within the specified time. The nodes are ordered according to the number of operations processed.\n"
    )

    markdown.append("| node | operations processed | operations per second  |")
    markdown.append("| --- | --- | --- |")

    # Sort nodes by number of token operations (descending)
    token_sorted_nodes = sorted(
        [node for node in report if node.get("token", {}).get("ok", False)],
        key=lambda x: x["token"]["count"],
        reverse=True,
    )

    for node_data in token_sorted_nodes:
        node = node_data["node"]
        operations = node_data["token"]["count"]
        time = node_data["token"]["time"]
        ops_per_second = format_float(operations / time) if time > 0 else 0
        markdown.append(f"| <{node}> | {operations} | {ops_per_second} |")

    markdown.append("\n")

    # Contract operations section
    markdown.append("## Contract operations\n")
    markdown.append(
        "This table shows how many contract operations were processed by the node within the specified time. The nodes are ordered according to the number of operations processed.\n"
    )

    markdown.append("| node | operations processed | operations per second |")
    markdown.append("| --- | --- | --- |")

    # Sort nodes by number of contract operations (descending)
    contract_sorted_nodes = sorted(
        [node for node in report if node.get("contract", {}).get("ok", False)],
        key=lambda x: x["contract"]["count"],
        reverse=True,
    )

    for node_data in contract_sorted_nodes:
        node = node_data["node"]
        operations = node_data["contract"]["count"]
        time = node_data["contract"]["time"]
        ops_per_second = format_float(operations / time) if time > 0 else 0
        markdown.append(f"| <{node}> | {operations} | {ops_per_second} |")

    markdown.append("\n")

    # Account history section
    markdown.append("## Account history operations\n")
    markdown.append(
        "This table shows how many account history operations were processed by the node within the specified time. The nodes are ordered according to the number of operations processed.\n"
    )

    markdown.append("| node | operations processed | operations per second |")
    markdown.append("| --- | --- | --- |")

    # Sort nodes by number of account history operations (descending)
    history_sorted_nodes = sorted(
        [node for node in report if node.get("account_history", {}).get("ok", False)],
        key=lambda x: x["account_history"]["count"],
        reverse=True,
    )

    for node_data in history_sorted_nodes:
        node = node_data["node"]
        operations = node_data["account_history"]["count"]
        time = node_data["account_history"]["time"]
        ops_per_second = format_float(operations / time) if time > 0 else 0
        markdown.append(f"| <{node}> | {operations} | {ops_per_second} |")

    markdown.append("\n")

    # Latency section
    markdown.append("## Latency\n")
    markdown.append(
        "This table shows the latency measurements for each node. The nodes are ordered according to average latency (lowest first).\n"
    )

    markdown.append("| node | avg latency [s] | min latency [s] | max latency [s] |")
    markdown.append("| --- | --- | --- | --- |")

    # Sort nodes by average latency (ascending)
    latency_sorted_nodes = sorted(
        [node for node in report if node.get("latency", {}).get("ok", False)],
        key=lambda x: x["latency"]["avg_latency"],
    )

    for node_data in latency_sorted_nodes:
        node = node_data["node"]
        avg_latency = format_float(node_data["latency"]["avg_latency"])
        min_latency = format_float(node_data["latency"]["min_latency"])
        max_latency = format_float(node_data["latency"]["max_latency"])
        markdown.append(f"| <{node}> | {avg_latency} | {min_latency} | {max_latency} |")

    markdown.append("\n")

    # Add historical trends if available
    if historical_data and "trends" in historical_data and "uptime" in historical_data:
        markdown.append("## Node Trends and Reliability")
        markdown.append("")
        markdown.append(f"The following data shows trends over the past {days} days:")
        markdown.append("")

        # Show uptime data
        markdown.append("### Node Uptime")
        markdown.append("")
        markdown.append("| Node | Uptime % | Total Runs |")
        markdown.append("| --- | --- | --- |")

        # Sort nodes by uptime percentage (descending)
        sorted_nodes = sorted(
            historical_data["uptime"].items(),
            key=lambda x: x[1]["uptime_percent"],
            reverse=True,
        )

        for node_url, uptime_data in sorted_nodes[:15]:  # Show top 15
            markdown.append(
                f"| <{node_url}> | {uptime_data['uptime_percent']}% | {uptime_data['total_runs']} |"
            )

        markdown.append("")

        # Show trending nodes (improving or degrading)
        markdown.append("### Notable Trends")
        markdown.append("")

        trends = historical_data["trends"]

        # Find significant improvements or degradations
        significant_trends = []
        for node, test_types in trends.items():
            for test_type, trend_info in test_types.items():
                # Handle both dict and list of dicts
                if isinstance(trend_info, list):
                    for ti in trend_info:
                        if (
                            isinstance(ti, dict)
                            and ti.get("trend") in ("improving", "degrading")
                            and abs(ti.get("change", 0)) >= 1
                        ):
                            significant_trends.append(
                                {
                                    "node": node,
                                    "test_type": test_type,
                                    "trend": ti["trend"],
                                    "change": ti["change"],
                                }
                            )
                elif isinstance(trend_info, dict):
                    if (
                        trend_info.get("trend") in ("improving", "degrading")
                        and abs(trend_info.get("change", 0)) >= 1
                    ):
                        significant_trends.append(
                            {
                                "node": node,
                                "test_type": test_type,
                                "trend": trend_info["trend"],
                                "change": trend_info["change"],
                            }
                        )

        if significant_trends:
            # Sort by absolute change magnitude (descending)
            significant_trends.sort(key=lambda x: abs(x["change"]), reverse=True)

            markdown.append("| Node | Test Type | Trend | Change |")
            markdown.append("| --- | --- | --- | --- |")

            for trend in significant_trends[:10]:  # Show top 10 most significant trends
                test_display_name = {
                    "token": "Token",
                    "contract": "Contract",
                    "account_history": "Account History",
                    "config": "Config",
                    "latency": "Latency",
                }.get(trend["test_type"], trend["test_type"].capitalize())
                change_display = f"{abs(trend['change'])} ranks"
                markdown.append(
                    f"| <{trend['node']}> | {test_display_name} | {trend['trend'].capitalize()} | {change_display} |"
                )
            markdown.append("")
        else:
            markdown.append(f"No significant trends detected in the past {days} days.")
            markdown.append("")

    # Add consistency data if available
    if historical_data and "consistency" in historical_data:
        markdown.append("## Node Consistency")
        markdown.append("")
        markdown.append("This section shows how consistent each node's performance has been.")
        markdown.append("Lower standard deviation indicates more consistent performance.")
        markdown.append("")

        # Combine all test types for an overall consistency score
        node_consistency_scores = {}

        for node, test_data in historical_data["consistency"].items():
            # Calculate average standard deviation across all test types
            valid_scores = [
                score
                for test_type, score in test_data.items()
                if score is not None and not isinstance(score, str)
            ]
            if valid_scores:
                node_consistency_scores[node] = sum(valid_scores) / len(valid_scores)

        # Sort nodes by consistency score (ascending - lower is better)
        sorted_nodes = sorted(node_consistency_scores.items(), key=lambda x: x[1])

        markdown.append("| Node | Consistency Score (lower is better) |")
        markdown.append("| --- | --- |")

        for node, score in sorted_nodes[:15]:  # Show top 15 most consistent nodes
            markdown.append(f"| {node} | {format_float(score)} |")

        markdown.append("")

    # Conclusion
    markdown.append("## Conclusion")
    markdown.append("")
    markdown.append(
        "This report provides a snapshot of Hive-Engine node performance. Users can use this information to select reliable nodes for their applications."
    )
    markdown.append("")
    markdown.append(
        "The benchmark data is also available in JSON format in the account's `json_metadata` for automated processing."
    )
    markdown.append("")
    markdown.append("---")
    markdown.append("Generated by the engine-bench tool.")

    # Create the final markdown content
    markdown_content = "\n".join(markdown)

    # Save to file if specified
    if output_file:
        with open(output_file, "w") as f:
            f.write(markdown_content)
        logging.info(f"Markdown content saved to {output_file}")

    return markdown_content, metadata


def generate_post(output_file, db_path="engine_benchmark_history.db", days=7):
    """Generate a markdown post from benchmark data.

    This function orchestrates the entire post generation process. It retrieves the
    latest benchmark data from the database, collects historical data for trend analysis,
    and generates a formatted markdown post.

    Args:
        output_file (str): Path to save the markdown post.
        db_path (str): Path to the SQLite database file.
        days (int): Number of days of historical data to include. Default is 7.

    Returns:
        tuple: A tuple containing (content, metadata) of the generated post.
    """
    # Get absolute path for output file (if not absolute)
    if not os.path.isabs(output_file):
        output_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))), output_file
        )

    # Get latest benchmark data from database
    benchmark_data = get_latest_benchmark_data(db_path)

    if not benchmark_data:
        # Try to read from JSON file as fallback
        json_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "engine_results.json",
        )
        if os.path.exists(json_file):
            try:
                with open(json_file, "r") as f:
                    benchmark_data = json.load(f)
                logging.info(f"Loaded benchmark data from {json_file}")
            except Exception as e:
                logging.error(f"Error loading benchmark data from {json_file}: {e}")
                return "No benchmark data available.", {}
        else:
            logging.error("No benchmark data available in database or JSON file")
            return "No benchmark data available.", {}

    # Get historical data for trends
    historical_data = get_historical_data(db_path, days)

    # Generate markdown content and metadata
    content, metadata = generate_markdown(benchmark_data, output_file, historical_data, days)

    logging.info(f"Post generated and saved to {output_file}")
    return content, metadata
