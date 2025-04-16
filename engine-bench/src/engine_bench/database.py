"""Database operations for storing hive-engine benchmark results."""

import json
import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path


def get_project_root() -> Path:
    """Get the absolute path to the project root directory."""
    current_file = Path(__file__).resolve()
    # Go up two levels from src/engine_bench/database.py to reach project root
    return current_file.parent.parent.parent


def get_db_path(db_path="engine_benchmark_history.db") -> Path:
    """Get the absolute path to the database file.

    Args:
        db_path (str, optional): Path to the SQLite database file.
            If not absolute, it will be relative to project root.
            Defaults to "engine_benchmark_history.db".

    Returns:
        Path: Absolute path to the database file
    """
    if os.path.isabs(db_path):
        return Path(db_path)
    return get_project_root() / db_path


def initialize_database(db_path="engine_benchmark_history.db"):
    """Create the SQLite database and tables if they don't exist.

    This function initializes the SQLite database used to store benchmark results.
    It creates the necessary tables for storing benchmark runs, node information,
    and individual benchmark results if they don't already exist.

    The database schema includes:
    - benchmark_runs: Stores metadata about each benchmark run
    - nodes: Stores information about each node tested
    - node_status: Stores the status of each node for a given benchmark run
    - test_results: Stores the actual benchmark results

    Args:
        db_path (str, optional): Path to the SQLite database file.
            If not absolute, it will be relative to project root.
            Defaults to "engine_benchmark_history.db".

    Returns:
        None
    """
    try:
        # Get absolute path to database
        db_path = get_db_path(db_path)

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create benchmark runs table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS benchmark_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            nectar_engine_version TEXT,
            script_version TEXT,
            num_retries INTEGER,
            num_retries_call INTEGER,
            timeout INTEGER,
            threading INTEGER,
            test_parameters TEXT
        )
        """)

        # Create nodes table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS nodes (
            node_id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL,
            first_seen TEXT,
            last_seen TEXT
        )
        """)

        # Create node status table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS node_status (
            status_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER,
            node_id INTEGER,
            is_working INTEGER,
            error_message TEXT,
            SSCnodeVersion TEXT,
            is_engine INTEGER,
            FOREIGN KEY (run_id) REFERENCES benchmark_runs (run_id),
            FOREIGN KEY (node_id) REFERENCES nodes (node_id)
        )
        """)

        # Create test results table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS test_results (
            result_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER,
            node_id INTEGER,
            test_type TEXT,  -- token, contract, account_history, config, latency
            is_ok INTEGER,
            rank INTEGER,
            time REAL,
            count INTEGER,
            access_time REAL,
            min_latency REAL,
            max_latency REAL,
            avg_latency REAL,
            FOREIGN KEY (run_id) REFERENCES benchmark_runs (run_id),
            FOREIGN KEY (node_id) REFERENCES nodes (node_id)
        )
        """)

        conn.commit()
        conn.close()
        logging.info(f"Database initialized at {db_path}")
    except sqlite3.Error as e:
        logging.error(f"Database initialization error: {e}")
        raise


def get_or_create_node_id(cursor, node_url, timestamp):
    """Get node ID from the database or create it if it doesn't exist.

    This function checks if a node with the given URL already exists in the database.
    If it does, it updates the last_seen timestamp and returns the existing node_id.
    If not, it creates a new node record with the given URL and timestamp, and returns
    the newly created node_id.

    Args:
        cursor (sqlite3.Cursor): Database cursor for executing SQL queries
        node_url (str): URL of the node to get or create
        timestamp (str): Current timestamp in string format

    Returns:
        int: The node_id of the existing or newly created node record
    """
    # Check if node already exists
    cursor.execute("SELECT node_id FROM nodes WHERE url = ?", (node_url,))
    node_record = cursor.fetchone()

    if node_record:
        # Update last_seen timestamp for existing node
        node_id = node_record[0]
        cursor.execute("UPDATE nodes SET last_seen = ? WHERE node_id = ?", (timestamp, node_id))
        return node_id
    else:
        # Create new node record
        cursor.execute(
            "INSERT INTO nodes (url, first_seen, last_seen) VALUES (?, ?, ?)",
            (node_url, timestamp, timestamp),
        )
        return cursor.lastrowid


def store_benchmark_data_in_db(report_data, db_path="engine_benchmark_history.db"):
    """Store benchmark data in SQLite database.

    This function takes the benchmark report data and stores it in the SQLite database.
    It creates records for the benchmark run, node statuses, and test results. It handles
    both working nodes and failing nodes, storing appropriate information for each.

    The function performs the following operations:
    1. Initializes the database if it doesn't exist
    2. Stores benchmark run metadata (timestamp, versions, parameters)
    3. For each working node, stores node status and test results
    4. For each failing node, stores node status with error message

    Args:
        report_data (dict): A dictionary containing the benchmark report data with the following structure:
            - parameter (dict): Benchmark parameters including start_time, end_time, versions, etc.
            - report (list): List of dictionaries containing data for working nodes
            - failing_nodes (dict): Dictionary mapping node URLs to error messages
        db_path (str, optional): Path to the SQLite database file.
            If not absolute, it will be relative to project root.
            Defaults to "engine_benchmark_history.db".

    Returns:
        None

    Raises:
        sqlite3.Error: If there's an error with the database operations
        Exception: For any other unexpected errors during data storage
    """
    try:
        # Get absolute path to database
        db_path = get_db_path(db_path)

        # Initialize database if needed
        if not os.path.exists(db_path):
            initialize_database(db_path)

        # Set up database connection
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Extract data from report
        params = report_data.get("parameter", {})
        report = report_data.get("report", [])
        failing_nodes = report_data.get("failing_nodes", {})
        timestamp = params.get("timestamp", datetime.now().isoformat())

        # Insert benchmark run record
        cursor.execute(
            """
            INSERT INTO benchmark_runs (
                timestamp, start_time, end_time, nectar_engine_version, script_version,
                num_retries, num_retries_call, timeout, threading, test_parameters
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp,
                params.get("start_time", ""),
                params.get("end_time", ""),
                params.get("nectar_engine_version", ""),
                params.get("script_version", ""),
                params.get("num_retries", 0),
                params.get("num_retries_call", 0),
                params.get("timeout", 0),
                1 if params.get("threading", False) else 0,
                json.dumps(params),
            ),
        )

        # Get the run_id of the newly inserted benchmark run
        run_id = cursor.lastrowid

        # Process working nodes
        for node_data in report:
            node_url = node_data.get("node", "")
            if not node_url:
                continue

            # Get or create node record
            node_id = get_or_create_node_id(cursor, node_url, timestamp)

            # Insert node status record
            cursor.execute(
                """
                INSERT INTO node_status (
                    run_id, node_id, is_working, SSCnodeVersion, is_engine
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    node_id,
                    1,  # is_working = True for working nodes
                    node_data.get("SSCnodeVersion", "unknown"),
                    1 if node_data.get("engine", False) else 0,
                ),
            )

            # Insert test results for each test type
            for test_type in [
                "token",
                "contract",
                "account_history",
                "config",
                "latency",
            ]:
                if test_type in node_data and isinstance(node_data[test_type], dict):
                    test_data = node_data[test_type]
                    cursor.execute(
                        """
                        INSERT INTO test_results (
                            run_id, node_id, test_type, is_ok, rank, time, count,
                            access_time, min_latency, max_latency, avg_latency
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            run_id,
                            node_id,
                            test_type,
                            1 if test_data.get("ok", False) else 0,
                            test_data.get("rank", -1),
                            test_data.get("time", 0.0),
                            test_data.get("count", 0),
                            test_data.get("access_time", 0.0),
                            test_data.get("min_latency", 0.0),
                            test_data.get("max_latency", 0.0),
                            test_data.get("avg_latency", 0.0),
                        ),
                    )

        # Process failing nodes
        for node_url, error_message in failing_nodes.items():
            # Get or create node record
            node_id = get_or_create_node_id(cursor, node_url, timestamp)

            # Insert node status record
            cursor.execute(
                """
                INSERT INTO node_status (
                    run_id, node_id, is_working, error_message, SSCnodeVersion, is_engine
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    node_id,
                    0,  # is_working = False for failing nodes
                    error_message,
                    "unknown",  # Default SSCnodeVersion for failing nodes
                    0,  # Default is_engine = False for failing nodes
                ),
            )

        # Commit changes and close connection
        conn.commit()
        conn.close()
        logging.info(f"Benchmark data stored in database at {db_path}")
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error storing benchmark data: {e}")
        raise


def get_latest_benchmark_data(db_path="engine_benchmark_history.db"):
    """Get the latest benchmark data from the SQLite database.

    This function queries the database for the latest benchmark run and formats
    the data to match the structure expected by the generate_markdown function.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        dict: A dictionary containing benchmark data with the same structure as
            that produced by run_benchmarks() and saved in engine_benchmark_results.json
    """
    try:
        # Get absolute path to database
        db_path = get_db_path(db_path)

        # Check if database exists
        if not os.path.exists(db_path):
            logging.error(f"Database file not found at {db_path}")
            return None

        # Set up database connection
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # This enables column access by name
        cursor = conn.cursor()

        # Get the most recent benchmark run
        cursor.execute(
            """
            SELECT * FROM benchmark_runs
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        run = cursor.fetchone()

        if not run:
            logging.warning("No benchmark runs found in database")
            return None

        run_id = run["run_id"]
        params = json.loads(run["test_parameters"]) if run["test_parameters"] else {}

        # Add additional parameters from the benchmark_runs table
        params.update(
            {
                "timestamp": run["timestamp"],
                "start_time": run["start_time"],
                "end_time": run["end_time"],
                "nectar_engine_version": run["nectar_engine_version"],
                "script_version": run["script_version"],
                "num_retries": run["num_retries"],
                "num_retries_call": run["num_retries_call"],
                "timeout": run["timeout"],
                "threading": bool(run["threading"]),
            }
        )

        # Get all working nodes for this run
        cursor.execute(
            """
            SELECT n.url, ns.SSCnodeVersion, ns.is_engine
            FROM node_status ns
            JOIN nodes n ON ns.node_id = n.node_id
            WHERE ns.run_id = ? AND ns.is_working = 1
            """,
            (run_id,),
        )
        working_nodes = cursor.fetchall()

        # Get all failing nodes for this run
        cursor.execute(
            """
            SELECT n.url, ns.error_message
            FROM node_status ns
            JOIN nodes n ON ns.node_id = n.node_id
            WHERE ns.run_id = ? AND ns.is_working = 0
            """,
            (run_id,),
        )
        failing_nodes_data = cursor.fetchall()

        # Convert failing nodes data to dictionary format
        failing_nodes = {}
        for node in failing_nodes_data:
            failing_nodes[node["url"]] = node["error_message"]

        # Prepare report data for working nodes
        report = []
        for node in working_nodes:
            node_url = node["url"]
            node_id = get_or_create_node_id(cursor, node_url, run["timestamp"])

            # Get test results for this node
            cursor.execute(
                """
                SELECT test_type, is_ok, rank, time, count, access_time,
                       min_latency, max_latency, avg_latency
                FROM test_results
                WHERE run_id = ? AND node_id = ?
                """,
                (run_id, node_id),
            )
            test_results = cursor.fetchall()

            # Create node data structure
            node_data = {
                "node": node_url,
                "SSCnodeVersion": node["SSCnodeVersion"],
                "engine": bool(node["is_engine"]),
            }

            # Add test results to node data
            for result in test_results:
                test_type = result["test_type"]
                node_data[test_type] = {
                    "ok": bool(result["is_ok"]),
                    "rank": result["rank"],
                    "time": result["time"],
                    "count": result["count"],
                    "access_time": result["access_time"],
                }

                # Add latency metrics if available
                if test_type == "latency" and result["is_ok"]:
                    node_data[test_type]["min_latency"] = result["min_latency"]
                    node_data[test_type]["max_latency"] = result["max_latency"]
                    node_data[test_type]["avg_latency"] = result["avg_latency"]

            report.append(node_data)

        # Get the list of all node URLs that were tested
        nodes = [node["node"] for node in report]

        # Close connection
        conn.close()

        # Construct final benchmark data structure
        benchmark_data = {
            "parameter": params,
            "report": report,
            "failing_nodes": failing_nodes,
            "nodes": nodes,
        }

        return benchmark_data

    except sqlite3.Error as e:
        logging.error(f"Database error retrieving benchmark data: {e}")
        return None
    except Exception as e:
        logging.error(f"Error retrieving benchmark data: {e}")
        return None
