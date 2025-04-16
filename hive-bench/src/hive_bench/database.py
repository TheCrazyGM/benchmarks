"""Database operations for storing benchmark results."""

import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path


def get_project_root() -> Path:
    """Get the absolute path to the project root directory."""
    current_file = Path(__file__).resolve()
    # Go up two levels from src/bench/database.py to reach project root
    return current_file.parent.parent.parent


def get_db_path(db_path="hive_benchmark_history.db") -> Path:
    """Get the absolute path to the database file.

    Args:
        db_path (str, optional): Path to the SQLite database file.
            If not absolute, it will be relative to project root.
            Defaults to "hive_benchmark_history.db".

    Returns:
        Path: Absolute path to the database file
    """
    if os.path.isabs(db_path):
        return Path(db_path)
    return get_project_root() / db_path


def initialize_database(db_path="hive_benchmark_history.db"):
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
            Defaults to "hive_benchmark_history.db".

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
            hive_nectar_version TEXT,
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
            version TEXT,
            is_hive INTEGER,
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
            test_type TEXT,  -- block, history, apicall, config, block_diff
            is_ok INTEGER,
            rank INTEGER,
            time REAL,
            count INTEGER,
            access_time REAL,
            head_delay REAL,
            diff_head_irreversible INTEGER,
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
        cursor.execute(
            "UPDATE nodes SET last_seen = ? WHERE node_id = ?", (timestamp, node_id)
        )
        return node_id
    else:
        # Create new node record
        cursor.execute(
            "INSERT INTO nodes (url, first_seen, last_seen) VALUES (?, ?, ?)",
            (node_url, timestamp, timestamp),
        )
        return cursor.lastrowid


def store_benchmark_data_in_db(report_data, db_path="hive_benchmark_history.db"):
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
            Defaults to "hive_benchmark_history.db".

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

        # Current timestamp for this data insertion
        timestamp = datetime.now().isoformat()

        # Extract parameters
        params = report_data["parameter"]

        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Store benchmark run metadata
        cursor.execute(
            """
            INSERT INTO benchmark_runs 
            (timestamp, start_time, end_time, hive_nectar_version, script_version, 
             num_retries, num_retries_call, timeout, threading, test_parameters) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp,
                params["start_time"],
                params["end_time"],
                params["hive_nectar_version"],
                params["script_version"],
                params.get("num_retries", 3),
                params.get("num_retries_call", 3),
                params.get("timeout", 30),
                1 if params.get("threading", False) else 0,
                params.get("test_parameters", ""),
            ),
        )
        run_id = cursor.lastrowid

        # Process working nodes
        for node_data in report_data["report"]:
            node_url = node_data["node"]
            node_id = get_or_create_node_id(cursor, node_url, timestamp)

            # Store node status
            cursor.execute(
                """
                INSERT INTO node_status 
                (run_id, node_id, is_working, version, is_hive) 
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    node_id,
                    1,  # is_working = True for nodes in report
                    node_data.get("version", ""),
                    1 if node_data.get("hive", False) else 0,
                ),
            )

            # Store test results for each test type
            for test_name in ["block", "history", "apicall", "config", "block_diff"]:
                test_data = node_data.get(test_name, {})

                if not test_data.get("ok", False):
                    continue

                if test_name == "block" or test_name == "history":
                    cursor.execute(
                        """
                        INSERT INTO test_results 
                        (run_id, node_id, test_type, is_ok, rank, time, count) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            run_id,
                            node_id,
                            test_name,
                            1,  # is_ok = True for working nodes
                            test_data.get("rank", -1),
                            test_data.get("time", 0.0),
                            test_data.get("count", 0),
                        ),
                    )
                elif test_name == "apicall" or test_name == "config":
                    cursor.execute(
                        """
                        INSERT INTO test_results 
                        (run_id, node_id, test_type, is_ok, rank, time, access_time) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            run_id,
                            node_id,
                            test_name,
                            1,  # is_ok = True for working nodes
                            test_data.get("rank", -1),
                            test_data.get("time", 0.0),
                            test_data.get("access_time", 0.0),
                        ),
                    )
                elif test_name == "block_diff":
                    # Store head_delay
                    cursor.execute(
                        """
                        INSERT INTO test_results 
                        (run_id, node_id, test_type, is_ok, rank, time, head_delay) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            run_id,
                            node_id,
                            test_name,
                            1,  # is_ok = True for working nodes
                            test_data.get("rank", -1),
                            test_data.get("time", 0.0),
                            test_data.get("head_delay", 0.0),
                        ),
                    )
                    # Store diff_head_irreversible
                    cursor.execute(
                        """
                        INSERT INTO test_results 
                        (run_id, node_id, test_type, is_ok, rank, time, diff_head_irreversible) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            run_id,
                            node_id,
                            f"{test_name}_diff",  # Use a different test_type for diff measurement
                            1,  # is_ok = True for working nodes
                            -1,  # No rank for this metric
                            test_data.get("time", 0.0),
                            test_data.get("diff_head_irreversible", 0),
                        ),
                    )

        # Process failing nodes
        for node_url, error_msg in report_data.get("failing_nodes", {}).items():
            node_id = get_or_create_node_id(cursor, node_url, timestamp)

            # Store node status
            cursor.execute(
                """
                INSERT INTO node_status 
                (run_id, node_id, is_working, error_message, version, is_hive) 
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    node_id,
                    0,  # is_working = False for failing nodes
                    error_msg,
                    "",  # No version for failing nodes
                    0,  # is_hive = False for failing nodes
                ),
            )

        conn.commit()
        conn.close()
        logging.info(f"Benchmark results stored in database at {db_path}")
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error storing benchmark data: {e}")
        raise
