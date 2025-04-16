"""Post generation functionality for benchmark results."""

import json
import logging
import os
import sqlite3
import statistics
from datetime import datetime, timedelta

# Reuse the format_float from utils
from hive_bench.utils import format_float


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
             "uptime": {node_url: {"success": count, "total": count}}
            }
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
    for test_type in ["block", "history", "apicall", "config", "block_diff"]:
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
        for test_type in ["block", "history", "apicall", "config", "block_diff"]:
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
    for test_type in ["block", "history", "apicall", "config", "block_diff"]:
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

        # Calculate consistency (lower std dev means more consistent)
        for node, ranks in node_ranks.items():
            if len(ranks) < 2:
                continue

            if node not in node_consistency:
                node_consistency[node] = {}

            try:
                std_dev = statistics.stdev(ranks)
                node_consistency[node][test_type] = format_float(std_dev)
            except statistics.StatisticsError:
                # Handle case where all values are identical
                node_consistency[node][test_type] = 0.0

    # Add failing nodes to consistency data with value 0
    for node_url in failing_nodes:
        if node_url not in node_consistency:
            node_consistency[node_url] = {}

        for test_type in ["block", "history", "apicall", "config", "block_diff"]:
            if test_type not in node_consistency[node_url]:
                node_consistency[node_url][test_type] = 0.0

    conn.close()

    historical_data["trends"] = node_trends
    historical_data["consistency"] = node_consistency
    historical_data["uptime"] = node_uptime

    return historical_data


def get_latest_benchmark_data(db_path="benchmark_history.db"):
    """Get the latest benchmark data from the SQLite database.

    This function queries the database for the latest benchmark run and formats
    the data to match the structure expected by the generate_markdown function.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        dict: A dictionary containing benchmark data with the same structure as
            that produced by run_benchmarks() and saved in benchmark_results.json
    """
    import sqlite3

    from nectar import __version__ as hive_nectar_version

    from hive_bench import __version__

    # Initialize the database if it doesn't exist
    if not os.path.exists(db_path):
        logging.error(f"Database {db_path} does not exist.")
        return None

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Return rows as dictionaries
        cursor = conn.cursor()

        # Get the latest benchmark run
        cursor.execute(
            """SELECT run_id, timestamp, test_parameters FROM benchmark_runs 
               ORDER BY timestamp DESC LIMIT 1"""
        )
        latest_run = cursor.fetchone()

        if not latest_run:
            logging.error("No benchmark runs found in the database.")
            return None

        run_id = latest_run["run_id"]
        timestamp = latest_run["timestamp"]
        test_parameters = (
            json.loads(latest_run["test_parameters"])
            if latest_run["test_parameters"]
            else {}
        )

        # Get all nodes for this run
        cursor.execute(
            """SELECT n.node_id, n.url, ns.is_working, ns.error_message, ns.is_hive 
               FROM node_status ns 
               JOIN nodes n ON ns.node_id = n.node_id 
               WHERE ns.run_id = ?""",
            (run_id,),
        )
        node_statuses = cursor.fetchall()

        # Prepare the result data structures
        working_nodes = []
        failing_nodes = {}
        node_data = {}

        for status in node_statuses:
            node_url = status["url"]

            if status["is_working"]:
                working_nodes.append(node_url)

                # Create node data structure
                node_data[node_url] = {
                    "node": node_url,
                    "version": "0.0.0",
                    "hive": status["is_hive"] == 1,
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
            else:
                failing_nodes[node_url] = status["error_message"]

        # Get test results for all working nodes
        cursor.execute(
            """SELECT tr.result_id, tr.node_id, tr.test_type, tr.is_ok, tr.rank, 
                      tr.time, tr.count, tr.access_time, tr.head_delay, tr.diff_head_irreversible,
                      n.url 
               FROM test_results tr 
               JOIN nodes n ON tr.node_id = n.node_id 
               WHERE tr.run_id = ?""",
            (run_id,),
        )
        test_results = cursor.fetchall()

        # Process test results
        for result in test_results:
            node_url = result["url"]
            test_type = result["test_type"]

            if node_url not in node_data:
                continue  # Skip if node is not among working nodes

            if test_type == "config":
                # Get version from the database
                cursor.execute(
                    """SELECT version FROM node_status 
                       WHERE node_id = ? AND run_id = ? LIMIT 1""",
                    (result["node_id"], run_id),
                )
                config_data = cursor.fetchone()
                if config_data:
                    node_data[node_url]["version"] = config_data["version"]

            # Update test data in node_data
            if result["is_ok"]:
                if test_type == "block":
                    node_data[node_url]["block"]["ok"] = True
                    node_data[node_url]["block"]["count"] = result["count"]
                    node_data[node_url]["block"]["time"] = result["time"]
                    node_data[node_url]["block"]["rank"] = result["rank"]
                elif test_type == "history":
                    node_data[node_url]["history"]["ok"] = True
                    node_data[node_url]["history"]["count"] = result["count"]
                    node_data[node_url]["history"]["time"] = result["time"]
                    node_data[node_url]["history"]["rank"] = result["rank"]
                elif test_type == "apicall":
                    node_data[node_url]["apicall"]["ok"] = True
                    node_data[node_url]["apicall"]["time"] = result["time"]
                    node_data[node_url]["apicall"]["access_time"] = result[
                        "access_time"
                    ]
                    node_data[node_url]["apicall"]["rank"] = result["rank"]
                elif test_type == "config":
                    node_data[node_url]["config"]["ok"] = True
                    node_data[node_url]["config"]["time"] = result["time"]
                    node_data[node_url]["config"]["access_time"] = result["access_time"]
                    node_data[node_url]["config"]["rank"] = result["rank"]
                elif test_type == "block_diff":
                    node_data[node_url]["block_diff"]["ok"] = True
                    node_data[node_url]["block_diff"]["head_delay"] = result[
                        "head_delay"
                    ]
                    node_data[node_url]["block_diff"]["diff_head_irreversible"] = (
                        result["diff_head_irreversible"]
                    )
                    node_data[node_url]["block_diff"]["time"] = result["time"]
                    node_data[node_url]["block_diff"]["rank"] = result["rank"]

        # Get start and end time
        start_time = test_parameters.get("start_time", timestamp)
        end_time = test_parameters.get("end_time", timestamp)

        # Build the final report structure
        report = {
            "nodes": working_nodes,
            "failing_nodes": failing_nodes,
            "report": list(node_data.values()),
            "parameter": {
                "num_retries": test_parameters.get("num_retries", 3),
                "num_retries_call": test_parameters.get("num_retries_call", 3),
                "timeout": test_parameters.get("timeout", 30),
                "threading": test_parameters.get("threading", True),
                "hive_nectar_version": hive_nectar_version,
                "start_time": start_time,
                "end_time": end_time,
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
    except sqlite3.Error as e:
        logging.error(f"Database error: {str(e)}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error retrieving benchmark data: {str(e)}")
        return None
    finally:
        if "conn" in locals() and conn:
            conn.close()


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
    # Extract timestamp from data or use current time
    timestamp = benchmark_data.get("timestamp", datetime.now().isoformat())
    formatted_date = datetime.fromisoformat(timestamp).strftime("%d/%m/%Y")

    # Create markdown content list
    markdown = []

    # Header
    markdown.append(f"# Full API Node Update - ({formatted_date})\n")
    current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    markdown.append(f"{current_time} - {current_time} (UTC)")
    markdown.append(
        "@nectarflower provides daily updates about the state of all available full API node server for HIVE."
    )
    markdown.append(
        "More information about nectarflower can be found in the [github repository](https://github.com/thecrazygm/bench).\n"
    )

    # Failing nodes section
    markdown.append("## List of failing nodes\n")
    markdown.append(
        "This table includes a list of all nodes which were not able to answer to a `get_config` API call within the specified timeout.\n"
    )

    markdown.append("|node | error |")
    markdown.append("| --- | --- |")

    failing_nodes = benchmark_data.get("failing_nodes", {})
    for node, error in failing_nodes.items():
        markdown.append(f"| <{node}> | {error} |")

    markdown.append("\n")

    # Working nodes section - config time
    markdown.append("## List of working nodes (At least once)\n")
    markdown.append(
        "This table includes all nodes which were able to answer a `get_config` call within the timeout. The achieved mean duration values are shown. The returned version is also shown.\n"
    )

    markdown.append("|node |  mean time [s] | version  |")
    markdown.append("| --- | --- | ---  |")

    # Process report data
    report = benchmark_data.get("report", [])

    # Sort nodes by config time (ascending)
    config_sorted_nodes = sorted(
        [node for node in report if node.get("config", {}).get("ok", False)],
        key=lambda x: x["config"]["access_time"],
    )

    for node_data in config_sorted_nodes:
        node = node_data["node"]
        version = node_data.get("version", "unknown")
        config_time = format_float(node_data["config"]["access_time"])
        markdown.append(f"| <{node}> | {config_time} | {version} |")

    markdown.append("\n")

    # Node Uptime Statistics (if historical data available)
    if historical_data and historical_data.get("uptime"):
        markdown.append("## Node Uptime Statistics (7-day period)\n")
        markdown.append(
            "This table shows how reliable nodes have been over the past week.\n"
        )

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

    # Streaming blocks section
    markdown.append("## Streaming blocks\n")
    markdown.append(
        "This table shows how many blocks were streamed by the node within the specified time. The RPCs were ordered according to the number of blocks streamed.\n"
    )

    markdown.append("| node | blocks streamed | blocks per second  |")
    markdown.append("| --- | --- | --- |")

    # Sort nodes by number of blocks streamed (descending)
    block_sorted_nodes = sorted(
        [node for node in report if node.get("block", {}).get("ok", False)],
        key=lambda x: x["block"]["count"],
        reverse=True,
    )

    for node_data in block_sorted_nodes:
        node = node_data["node"]
        blocks = node_data["block"]["count"]
        time = node_data["block"]["time"]
        blocks_per_second = format_float(blocks / time) if time > 0 else 0
        markdown.append(f"| <{node}> | {blocks} | {blocks_per_second} |")

    markdown.append("\n")

    # Streaming account history section
    markdown.append("## Streaming account history\n")
    markdown.append(
        "This table shows how many account history operations were streamed by the node within the specified time. The RPCs were ordered according to the number of operations streamed.\n"
    )

    markdown.append("| node | operations streamed | operations per second |")
    markdown.append("| --- | --- | --- |")

    # Sort nodes by number of account history operations streamed (descending)
    history_sorted_nodes = sorted(
        [node for node in report if node.get("history", {}).get("ok", False)],
        key=lambda x: x["history"]["count"],
        reverse=True,
    )

    for node_data in history_sorted_nodes:
        node = node_data["node"]
        operations = node_data["history"]["count"]
        time = node_data["history"]["time"]
        operations_per_second = format_float(operations / time) if time > 0 else 0
        markdown.append(f"| <{node}> | {operations} | {operations_per_second} |")

    markdown.append("\n")

    # API call time section
    markdown.append("## API call time\n")
    markdown.append(
        "This table shows how long it took to call an API. The RPCs were ordered according to the access time.\n"
    )

    markdown.append("| node |  response time [s] |")
    markdown.append("| --- | --- |")

    # Sort nodes by API call time (ascending)
    apicall_sorted_nodes = sorted(
        [node for node in report if node.get("apicall", {}).get("ok", False)],
        key=lambda x: x["apicall"]["access_time"],
    )

    for node_data in apicall_sorted_nodes:
        node = node_data["node"]
        access_time = format_float(node_data["apicall"]["access_time"])
        markdown.append(f"| <{node}> | {access_time} |")

    markdown.append("\n")

    # Block difference section
    markdown.append("## Block difference\n")
    markdown.append(
        "This table shows the head blocks reported by each node. By comparing with the highest one, we can tell if a node has issues with block processing.\n"
    )

    markdown.append("| node | blocks behind | head blocks behind irreversible |")
    markdown.append("| --- | --- | --- |")

    # Sort nodes by block difference (ascending)
    diff_sorted_nodes = sorted(
        [node for node in report if node.get("block_diff", {}).get("ok", False)],
        key=lambda x: x["block_diff"]["head_delay"],
    )

    for node_data in diff_sorted_nodes:
        node = node_data["node"]
        # Add null checks before converting to integers
        head_delay = node_data["block_diff"].get("head_delay", 0)
        diff_head_irreversible = node_data["block_diff"].get(
            "diff_head_irreversible", 0
        )
        blocks_behind = int(head_delay) if head_delay is not None else 0
        diff_head_irr = (
            int(diff_head_irreversible) if diff_head_irreversible is not None else 0
        )
        markdown.append(f"| <{node}> | {blocks_behind} | {diff_head_irr} |")

    markdown.append("\n")

    # Node Trends Section (if historical data available)
    if historical_data and historical_data.get("trends"):
        period_text = "today" if days <= 1 else f"{days}-day period"
        markdown.append(f"## Node Performance Trends ({period_text})\n")
        markdown.append(
            "This table shows how node performance has changed over the past week.\n"
        )

        markdown.append(
            "| node | block trend | history trend | API call trend | config trend | block diff trend |"
        )
        markdown.append("| --- | --- | --- | --- | --- | --- |")

        # Process all nodes in historical data
        sorted_nodes = sorted(historical_data["trends"].keys())
        for node_url in sorted_nodes:
            # Helper function to calculate trend from rank history
            def calculate_trend_from_data(node_url, test_type, historical_data):
                try:
                    # First, check if we have direct trend data calculated by get_historical_data
                    if (
                        historical_data
                        and "trends" in historical_data
                        and node_url in historical_data["trends"]
                        and test_type in historical_data["trends"][node_url]
                        and isinstance(
                            historical_data["trends"][node_url][test_type], dict
                        )
                        and "trend" in historical_data["trends"][node_url][test_type]
                    ):
                        return historical_data["trends"][node_url][test_type]["trend"]

                    # If we don't have calculated trends, check if we have raw data to calculate it
                    elif (
                        historical_data
                        and "trends" in historical_data
                        and node_url in historical_data["trends"]
                        and test_type in historical_data["trends"][node_url]
                        and isinstance(
                            historical_data["trends"][node_url][test_type], list
                        )
                    ):
                        ranks_data = historical_data["trends"][node_url][test_type]
                        if not ranks_data or len(ranks_data) < 2:
                            return "n/a"

                        # Safely extract ranks, handling both dictionaries and other data types
                        ranks = []
                        timestamps = []
                        for entry in ranks_data:
                            if isinstance(entry, dict):
                                rank = entry.get("rank")
                                timestamp = entry.get("timestamp")
                                if rank is not None:
                                    ranks.append(rank)
                                    if timestamp:
                                        timestamps.append(timestamp)
                            elif isinstance(entry, (int, float)):
                                ranks.append(entry)
                            else:
                                # Skip any entries that are neither dicts nor numbers
                                continue

                        # Need at least 2 valid ranks to calculate a trend
                        if len(ranks) < 2:
                            return "n/a"

                        # Sort by timestamp if available
                        if timestamps and len(timestamps) == len(ranks):
                            # Create pairs and sort by timestamp
                            pairs = sorted(zip(timestamps, ranks), key=lambda x: x[0])
                            ranks = [r for _, r in pairs]

                        # Simple trend detection - compare last rank with first rank
                        if ranks[-1] < ranks[0]:  # Lower rank is better
                            return "improving"
                        elif ranks[-1] > ranks[0]:
                            return "worsening"
                        else:
                            return "stable"

                    # Otherwise, we don't have trend data for this node/test_type
                    return "n/a"
                except Exception as e:
                    logging.warning(
                        f"Error calculating trend for {node_url}, {test_type}: {e}"
                    )
                    return "n/a"

            # Get trend indicators for each test type
            block_trend = calculate_trend_from_data(node_url, "block", historical_data)
            history_trend = calculate_trend_from_data(
                node_url, "history", historical_data
            )
            api_trend = calculate_trend_from_data(node_url, "apicall", historical_data)
            config_trend = calculate_trend_from_data(
                node_url, "config", historical_data
            )
            block_diff_trend = calculate_trend_from_data(
                node_url, "block_diff", historical_data
            )

            # Create emoji indicators
            def get_trend_emoji(trend):
                if trend == "improving":
                    return "&nearr;  improving"
                elif trend == "worsening":
                    return "&searr; worsening"
                elif trend == "failing":
                    return "&cross; failing"
                elif trend == "stable":
                    return "&check; stable"
                else:
                    return "n/a"

            block_emoji = get_trend_emoji(block_trend)
            history_emoji = get_trend_emoji(history_trend)
            api_emoji = get_trend_emoji(api_trend)
            config_emoji = get_trend_emoji(config_trend)
            block_diff_emoji = get_trend_emoji(block_diff_trend)

            markdown.append(
                f"| <{node_url}> | {block_emoji} | {history_emoji} | {api_emoji} | {config_emoji} | {block_diff_emoji} |"
            )

        markdown.append("\n")

    # Node Consistency Section (if historical data available)
    if historical_data and historical_data.get("consistency"):
        period_text = "today" if days <= 1 else f"{days}-day period"
        markdown.append(f"## Node Consistency ({period_text})\n")
        markdown.append(
            "This table shows how consistent node performance has been. Lower values indicate more consistent performance.\n"
        )

        markdown.append("| node | block | history | API call | config | block diff |")
        markdown.append("| --- | --- | --- | --- | --- | --- |")

        # Sort nodes by average consistency across all tests
        def avg_consistency(node_data):
            values = [
                value for value in node_data.values() if isinstance(value, (int, float))
            ]

            # If all values are 0, this is likely a failing node - place it at the end
            if all(v == 0 for v in values) and values:
                return float("inf")

            # Filter out zeros when calculating average, as 0 indicates a failure rather than perfect consistency
            non_zero_values = [v for v in values if v != 0]

            # If we have no valid values after filtering zeros, return infinity
            if not non_zero_values:
                return float("inf")

            # Return the average of non-zero values
            return sum(non_zero_values) / len(non_zero_values)

        sorted_nodes = sorted(
            historical_data["consistency"].items(), key=lambda x: avg_consistency(x[1])
        )

        for node_url, consistency in sorted_nodes:
            block_cons = consistency.get("block", "n/a")
            history_cons = consistency.get("history", "n/a")
            api_cons = consistency.get("apicall", "n/a")
            config_cons = consistency.get("config", "n/a")
            block_diff_cons = consistency.get("block_diff", "n/a")

            markdown.append(
                f"| <{node_url}> | {block_cons} | {history_cons} | {api_cons} | {config_cons} | {block_diff_cons} |"
            )

        markdown.append("\n")

    # Overall Ranking Section
    markdown.append("## Overall Node Ranking\n")
    markdown.append(
        "This table shows an overall ranking of nodes based on their performance across all tests.\n"
    )

    markdown.append(
        "| node | score | block rank | history rank | API call rank | config rank | block diff rank |"
    )
    markdown.append("| --- | --- | --- | --- | --- | --- | --- |")

    # Calculate overall score for each node
    node_scores = {}
    for node_data in report:
        node = node_data["node"]
        score = 0
        ranks = []

        # Collect ranks from each test
        block_rank = node_data.get("block", {}).get("rank", -1)
        if block_rank > 0:
            score += max(
                10 - block_rank, 0
            )  # Higher score for better rank (lower number)
            ranks.append(block_rank)

        history_rank = node_data.get("history", {}).get("rank", -1)
        if history_rank > 0:
            score += max(10 - history_rank, 0)
            ranks.append(history_rank)

        api_rank = node_data.get("apicall", {}).get("rank", -1)
        if api_rank > 0:
            score += max(10 - api_rank, 0)
            ranks.append(api_rank)

        config_rank = node_data.get("config", {}).get("rank", -1)
        if config_rank > 0:
            score += max(10 - config_rank, 0)
            ranks.append(config_rank)

        block_diff_rank = node_data.get("block_diff", {}).get("rank", -1)
        if block_diff_rank > 0:
            score += max(10 - block_diff_rank, 0)
            ranks.append(block_diff_rank)

        # Calculate additional score based on average rank
        if ranks:
            avg_rank = sum(ranks) / len(ranks)
            score += max(10 - avg_rank, 0)  # Bonus for consistent performance

        node_scores[node] = {
            "score": score,
            "block_rank": block_rank if block_rank > 0 else "n/a",
            "history_rank": history_rank if history_rank > 0 else "n/a",
            "api_rank": api_rank if api_rank > 0 else "n/a",
            "config_rank": config_rank if config_rank > 0 else "n/a",
            "block_diff_rank": block_diff_rank if block_diff_rank > 0 else "n/a",
        }

    # Sort nodes by score (descending)
    sorted_scores = sorted(
        node_scores.items(), key=lambda x: x[1]["score"], reverse=True
    )

    for node, score_data in sorted_scores:
        markdown.append(
            f"| <{node}> | {score_data['score']} | {score_data['block_rank']} | "
            f"{score_data['history_rank']} | {score_data['api_rank']} | "
            f"{score_data['config_rank']} | {score_data['block_diff_rank']} |"
        )

    # Create metadata for the post
    metadata = {
        "timestamp": datetime.now().isoformat(),
        "node_count": len(report),
        "failing_nodes": len(benchmark_data.get("failing_nodes", {})),
        "top_nodes": [node for node, _ in sorted_scores[:5]] if sorted_scores else [],
    }
    metadata["title"] = f"Full API Node Update - {formatted_date}"

    # Join all lines to create the markdown content
    markdown_content = "\n".join(markdown)

    # Save to file if output_file is provided
    if output_file:
        with open(output_file, "w") as f:
            f.write(markdown_content)
        logging.info(f"Markdown saved to {output_file}")

    return markdown_content, metadata


def generate_post(
    output_file="benchmark_post.md", db_path="benchmark_history.db", days=7
):
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
    try:
        # Get the latest benchmark data
        logging.info("Retrieving latest benchmark data from database")
        benchmark_data = get_latest_benchmark_data(db_path)

        if not benchmark_data:
            logging.error("Failed to retrieve benchmark data from database.")
            return None, None

        # Get historical data for trends
        logging.info(f"Retrieving historical data for the past {days} days")
        try:
            historical_data = get_historical_data(db_path, days=days)
        except Exception as e:
            logging.error(f"Database error while retrieving historical data: {e}")
            historical_data = None

        # Generate the formatted markdown post
        try:
            content, metadata = generate_markdown(
                benchmark_data, output_file, historical_data, days=days
            )
        except Exception as e:
            import traceback

            logging.error(f"Error generating markdown: {e}")
            logging.error(traceback.format_exc())
            return None, None

        return content, metadata
    except Exception as e:
        import traceback

        logging.error(f"Unexpected error in generate_post: {e}")
        logging.error(traceback.format_exc())
        return None, None
