"""Benchmark functions for hive-engine nodes using nectarengine."""

import logging
import time

from nectarengine.api import Api


def get_status_node(node, num_retries=3, num_retries_call=3, timeout=30, how_many_seconds=30):
    """Benchmark status retrieval from a hive-engine node using getStatus().

    This function measures how long it takes to retrieve the node's status
    and extracts version and other relevant information from getStatus().

    Args:
        node (str): URL of the hive-engine node to benchmark
        num_retries (int, optional): Number of connection retries. Defaults to 3.
        num_retries_call (int, optional): Number of API call retries. Defaults to 3.
        timeout (int, optional): Connection timeout in seconds. Defaults to 30.
        how_many_seconds (int, optional): Time limit for benchmark in seconds. Defaults to 30.

    Returns:
        dict: Benchmark results including success status, sscnodeversion, and timing information
    """
    start_time = time.time()
    access_start_time = time.time()
    result = {
        "successful": False,
        "sscnodeversion": "unknown",
        "is_engine": False,
        "total_duration": 0.0,
        "access_time": timeout,
        "chainId": "",
        "lastBlockNumber": "",
        "lastBlockRef": "",
    }

    try:
        # Initialize nectarengine API
        api = Api(
            url=node,
            num_retries=num_retries,
            num_retries_call=num_retries_call,
            timeout=timeout,
        )

        # Record initial access time
        access_time = time.time() - access_start_time

        # Use get_status (snake_case)
        try:
            status = api.get_status()
            logging.debug(f"get_status() output for node {node}: {status}")
            if status:
                result["is_engine"] = True
                result["successful"] = True
                result["sscnodeversion"] = status.get("SSCnodeVersion", "unknown")
                result["chainId"] = status.get("chainId", "")
                result["lastBlockNumber"] = status.get("lastBlockNumber", "")
                result["lastBlockRef"] = status.get("lastBlockRef", "")
            else:
                result["is_engine"] = False
                result["successful"] = False
                result["sscnodeversion"] = "unknown"
        except Exception as e:
            logging.error(f"Failed to get_status from node {node}: {str(e)}")
            result["is_engine"] = False
            result["successful"] = False
            result["error"] = f"Not a valid hive-engine node: {str(e)}"
            result["total_duration"] = time.time() - start_time
            return result

        result["successful"] = True
        result["access_time"] = access_time

    except Exception as e:
        logging.error(f"Error getting status from node {node}: {str(e)}")
        result["error"] = str(e)
        result["total_duration"] = time.time() - start_time
        return result

    result["total_duration"] = time.time() - start_time
    return result


# For backward compatibility, you may keep the old name as an alias for now:
get_config_node = get_status_node


def benchmark_token_retrieval(
    node,
    num_retries=3,
    num_retries_call=3,
    timeout=30,
    how_many_seconds=30,
    token="SWAP.HIVE",
):
    """Benchmark token retrieval from a hive-engine node.

    This function measures how quickly a node can retrieve a specific token's information.

    Args:
        node (str): URL of the hive-engine node to benchmark
        num_retries (int, optional): Number of connection retries. Defaults to 3.
        num_retries_call (int, optional): Number of API call retries. Defaults to 3.
        timeout (int, optional): Connection timeout in seconds. Defaults to 30.
        how_many_seconds (int, optional): Time limit for benchmark in seconds. Defaults to 30.
        token (str, optional): Token symbol to retrieve. Defaults to "SWAP.HIVE".

    Returns:
        dict: Benchmark results including success status, count, and timing information
    """
    start_time = time.time()
    end_time = start_time + how_many_seconds
    count = 0

    try:
        # Initialize nectarengine API
        api = Api(
            url=node,
            num_retries=num_retries,
            num_retries_call=num_retries_call,
            timeout=timeout,
        )

        # Perform token queries until time limit is reached
        while time.time() < end_time:
            try:
                api.find("tokens", "tokens", {"symbol": token})
                count += 1
            except Exception as e:
                logging.error(f"Error retrieving token {token} from node {node}: {str(e)}")
                break

        total_duration = time.time() - start_time

        return {
            "successful": count > 0,
            "count": count,
            "token": token,
            "total_duration": total_duration,
        }
    except Exception as e:
        logging.error(f"Error benchmarking token retrieval on node {node}: {str(e)}")
        return {
            "successful": False,
            "count": 0,
            "token": token,
            "error": str(e),
            "total_duration": time.time() - start_time,
        }


def benchmark_contract_retrieval(
    node,
    num_retries=3,
    num_retries_call=3,
    timeout=30,
    how_many_seconds=30,
    contract="tokens",
):
    """Benchmark contract retrieval from a hive-engine node.

    This function measures how quickly a node can retrieve contract information.

    Args:
        node (str): URL of the hive-engine node to benchmark
        num_retries (int, optional): Number of connection retries. Defaults to 3.
        num_retries_call (int, optional): Number of API call retries. Defaults to 3.
        timeout (int, optional): Connection timeout in seconds. Defaults to 30.
        how_many_seconds (int, optional): Time limit for benchmark in seconds. Defaults to 30.
        contract (str, optional): Contract name to retrieve. Defaults to "tokens".

    Returns:
        dict: Benchmark results including success status, count, and timing information
    """
    start_time = time.time()
    end_time = start_time + how_many_seconds
    count = 0

    try:
        # Initialize nectarengine API
        api = Api(
            url=node,
            num_retries=num_retries,
            num_retries_call=num_retries_call,
            timeout=timeout,
        )

        # Perform contract queries until time limit is reached
        while time.time() < end_time:
            try:
                # For contracts, we just query the first few records
                api.find(contract, contract, {}, limit=5)
                count += 1
            except Exception as e:
                logging.error(f"Error retrieving contract {contract} from node {node}: {str(e)}")
                break

        total_duration = time.time() - start_time

        return {
            "successful": count > 0,
            "count": count,
            "contract": contract,
            "total_duration": total_duration,
        }
    except Exception as e:
        logging.error(f"Error benchmarking contract retrieval on node {node}: {str(e)}")
        return {
            "successful": False,
            "count": 0,
            "contract": contract,
            "error": str(e),
            "total_duration": time.time() - start_time,
        }


def benchmark_account_history(
    node,
    num_retries=3,
    num_retries_call=3,
    timeout=30,
    account_name="thecrazygm",
    how_many_seconds=30,
):
    """Benchmark account history retrieval from a hive-engine node.

    This function measures how quickly a node can retrieve account history information.

    Args:
        node (str): URL of the hive-engine node to benchmark
        num_retries (int, optional): Number of connection retries. Defaults to 3.
        num_retries_call (int, optional): Number of API call retries. Defaults to 3.
        timeout (int, optional): Connection timeout in seconds. Defaults to 30.
        account_name (str, optional): Account name to retrieve history for. Defaults to "thecrazygm".
        how_many_seconds (int, optional): Time limit for benchmark in seconds. Defaults to 30.

    Returns:
        dict: Benchmark results including success status, count, and timing information
    """
    start_time = time.time()
    end_time = start_time + how_many_seconds
    count = 0

    try:
        # Initialize nectarengine API
        api = Api(
            url=node,
            num_retries=num_retries,
            num_retries_call=num_retries_call,
            timeout=timeout,
        )

        # Perform account history queries until time limit is reached
        while time.time() < end_time:
            try:
                # Query account history for specified account
                # Use a default token symbol for history retrieval
                api.get_history(account_name, limit=5, symbol="SWAP.HIVE")
                count += 1
            except Exception as e:
                logging.error(
                    f"Error retrieving account history for {account_name} from node {node}: {str(e)}"
                )
                break

        total_duration = time.time() - start_time

        return {
            "successful": count > 0,
            "count": count,
            "account": account_name,
            "total_duration": total_duration,
        }
    except Exception as e:
        logging.error(f"Error benchmarking account history on node {node}: {str(e)}")
        return {
            "successful": False,
            "count": 0,
            "account": account_name,
            "error": str(e),
            "total_duration": time.time() - start_time,
        }


def benchmark_latency(node, num_retries=3, num_retries_call=3, timeout=30):
    """Benchmark latency to a hive-engine node.

    This function measures the round-trip latency to the node by making a simple API call.

    Args:
        node (str): URL of the hive-engine node to benchmark
        num_retries (int, optional): Number of connection retries. Defaults to 3.
        num_retries_call (int, optional): Number of API call retries. Defaults to 3.
        timeout (int, optional): Connection timeout in seconds. Defaults to 30.

    Returns:
        dict: Benchmark results including success status and latency information
    """
    latencies = []
    num_samples = 5  # Number of latency samples to take

    try:
        # Initialize nectarengine API
        api = Api(
            url=node,
            num_retries=num_retries,
            num_retries_call=num_retries_call,
            timeout=timeout,
        )

        # Take multiple latency measurements
        for _ in range(num_samples):
            try:
                start_time = time.time()
                # Make a simple query to measure latency - use a lightweight call
                api.find("tokens", "tokens", {"symbol": "SWAP.HIVE"}, limit=1)
                latency = time.time() - start_time
                latencies.append(latency)
                time.sleep(0.1)  # Brief pause between measurements
            except Exception as e:
                logging.error(f"Error measuring latency to node {node}: {str(e)}")
                break

        if not latencies:
            return {
                "successful": False,
                "error": "No successful latency measurements",
                "total_duration": 0.0,
            }

        # Calculate average latency
        avg_latency = sum(latencies) / len(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)

        return {
            "successful": True,
            "min_latency": min_latency,
            "max_latency": max_latency,
            "avg_latency": avg_latency,
            "samples": len(latencies),
            "total_duration": sum(latencies),
        }
    except Exception as e:
        logging.error(f"Error benchmarking latency to node {node}: {str(e)}")
        return {"successful": False, "error": str(e), "total_duration": 0.0}
