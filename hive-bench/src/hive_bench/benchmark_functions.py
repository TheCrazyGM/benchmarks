"""Benchmark functions for testing Hive nodes."""

from datetime import datetime, timezone
from timeit import default_timer as timer

from nectar.account import Account
from nectar.blockchain import Blockchain
from nectar.comment import Comment
from nectar.hive import Hive
from nectar.utils import resolve_authorpermvoter

from hive_bench.utils import format_float, quit_thread


def get_config_node(
    node, num_retries=10, num_retries_call=10, timeout=60, how_many_seconds=30
):
    """Retrieve configuration information from a Hive node.

    This function connects to a Hive node and retrieves its configuration, version,
    and determines if it's a Hive node. It measures the time taken to access the
    configuration as a performance metric.

    Args:
        node (str): URL of the node to connect to
        num_retries (int, optional): Number of connection retries. Defaults to 10.
        num_retries_call (int, optional): Number of API call retries. Defaults to 10.
        timeout (int, optional): Connection timeout in seconds. Defaults to 60.
        how_many_seconds (int, optional): Time limit for benchmark in seconds. Defaults to 30.
            Note: This parameter is not used in this function but kept for API consistency.

    Returns:
        dict: A dictionary containing the following keys:
            - successful (bool): Always True if no exception is raised
            - version (str): Blockchain version string
            - config (dict): Node configuration dictionary
            - is_hive (bool): Whether the node is a Hive node
            - access_time (float): Time taken to access the configuration in seconds
            - count (None): Always None, included for API consistency

    Raises:
        Exception: Any exception that occurs during node connection or data retrieval
    """
    blockchain_version = "0.0.0"
    is_hive = False
    access_time = timeout
    config = {}

    try:
        hv = Hive(
            node=node,
            num_retries=num_retries,
            num_retries_call=num_retries_call,
            timeout=timeout,
        )
        blockchain_version = hv.get_blockchain_version()
        is_hive = hv.is_hive

        start = timer()
        config = hv.get_config(use_stored_data=False)
        access_time = format_float(timer() - start)

        return {
            "successful": True,
            "version": blockchain_version,
            "config": config,
            "is_hive": is_hive,
            "access_time": access_time,
            "count": None,
        }
    except Exception:
        # Let benchmark_executor handle the exception
        raise


def benchmark_node_blocks(
    node, num_retries=10, num_retries_call=10, timeout=60, how_many_seconds=30
):
    """Benchmark a node's block retrieval performance.

    This function measures how many blocks a node can retrieve within a specified time period.
    It starts at 75% of the current block height and counts how many blocks can be retrieved
    sequentially within the time limit.

    Args:
        node (str): URL of the node to benchmark
        num_retries (int, optional): Number of connection retries. Defaults to 10.
        num_retries_call (int, optional): Number of API call retries. Defaults to 10.
        timeout (int, optional): Connection timeout in seconds. Defaults to 60.
        how_many_seconds (int, optional): Time limit for benchmark in seconds. Defaults to 30.

    Returns:
        dict: A dictionary containing the following keys:
            - successful (bool): Always True if no exception is raised
            - count (int): Number of blocks retrieved within the time limit
            - access_time (None): Always None, included for API consistency
            - total_duration (float): Actual time taken for the benchmark in seconds

    Raises:
        Exception: Any exception that occurs during node connection or block retrieval
    """
    block_count = 0
    start_time = timer()

    try:
        hv = Hive(
            node=node,
            num_retries=num_retries,
            num_retries_call=num_retries_call,
            timeout=timeout,
        )
        blockchain = Blockchain(blockchain_instance=hv)
        last_block_id = int(blockchain.get_current_block_num() * 0.75)

        for entry in blockchain.blocks(
            start=last_block_id,
        ):
            block_count += 1

            # Check if we need to break out of the loop
            if timer() - start_time > how_many_seconds or quit_thread:
                break

        return {
            "successful": True,
            "count": block_count,
            "access_time": None,
            "total_duration": format_float(timer() - start_time),
        }
    except Exception:
        # Let benchmark_executor handle the exception
        raise


def benchmark_node_history(
    node,
    num_retries=10,
    num_retries_call=10,
    timeout=60,
    how_many_seconds=60,
    account_name="thecrazygm",
):
    """Benchmark a node's account history retrieval performance.

    This function measures how many account history operations a node can retrieve
    within a specified time period. It retrieves history operations in reverse order
    for the specified account and counts how many can be retrieved within the time limit.

    Args:
        node (str): URL of the node to benchmark
        num_retries (int, optional): Number of connection retries. Defaults to 10.
        num_retries_call (int, optional): Number of API call retries. Defaults to 10.
        timeout (int, optional): Connection timeout in seconds. Defaults to 60.
        how_many_seconds (int, optional): Time limit for benchmark in seconds. Defaults to 60.
        account_name (str, optional): Account name to retrieve history for. Defaults to "thecrazygm".

    Returns:
        dict: A dictionary containing the following keys:
            - successful (bool): Always True if no exception is raised
            - count (int): Number of history operations retrieved within the time limit
            - access_time (None): Always None, included for API consistency
            - total_duration (float): Actual time taken for the benchmark in seconds

    Raises:
        Exception: Any exception that occurs during node connection or history retrieval
    """
    history_count = 0
    start_time = timer()

    try:
        hv = Hive(
            node=node,
            num_retries=num_retries,
            num_retries_call=num_retries_call,
            timeout=timeout,
        )
        account = Account(account_name, blockchain_instance=hv)

        for _ in account.history_reverse(batch_size=100):
            history_count += 1
            if timer() - start_time > how_many_seconds or quit_thread:
                break

        return {
            "successful": True,
            "count": history_count,
            "access_time": None,
            "total_duration": format_float(timer() - start_time),
        }
    except Exception:
        # Let benchmark_executor handle the exception
        raise


def benchmark_calls(
    node, authorpermvoter, num_retries=10, num_retries_call=10, timeout=60
):
    """Benchmark a node's API call performance for retrieving post data.

    This function measures the time it takes to retrieve a post from a node. It tries to
    retrieve the specified post first, and if that fails, it attempts to retrieve any recent
    post from the same author as a fallback.

    Args:
        node (str): URL of the node to benchmark
        authorpermvoter (str): String in format "author/permlink" identifying the post to retrieve
        num_retries (int, optional): Number of connection retries. Defaults to 10.
        num_retries_call (int, optional): Number of API call retries. Defaults to 10.
        timeout (int, optional): Connection timeout in seconds. Defaults to 60.

    Returns:
        dict: A dictionary containing the following keys:
            - successful (bool): Always True if no exception is raised
            - access_time (float): Time taken to access the post in seconds

    Raises:
        Exception: Any exception that occurs during node connection or post retrieval
        ValueError: If the authorpermvoter format is invalid and can't be parsed
    """
    access_time = timeout

    try:
        # Initialize Hive client
        hv = Hive(
            node=node,
            num_retries=num_retries,
            num_retries_call=num_retries_call,
            timeout=timeout,
        )

        # Parse parameters safely
        try:
            comment_author, comment_permlink, _ = resolve_authorpermvoter(
                authorpermvoter
            )
        except Exception:
            # If we can't resolve the authorpermvoter, use a fallback approach
            parts = authorpermvoter.split("|")[0].split("/")
            if len(parts) >= 2:
                comment_author, comment_permlink = parts[0], parts[1]
            else:
                raise ValueError(f"Could not parse authorpermvoter: {authorpermvoter}")

        # Try to get a post that's more likely to exist
        try:
            start = timer()
            # Try the specified post first
            post_identifier = f"@{comment_author}/{comment_permlink}"
            comment = Comment(post_identifier, blockchain_instance=hv)

            # Basic check to see if we got a valid post
            if not hasattr(comment, "id") or not comment.id:
                raise ValueError("Post not found, trying recent posts")

            access_time = format_float(timer() - start)
        except Exception:
            # If the specified post doesn't exist, try to get any recent post from the author
            start = timer()
            account = Account(comment_author, blockchain_instance=hv)

            # Get the most recent post from the author
            history = list(account.get_blog(limit=1))
            if history:
                # Use the most recent post
                recent_post = history[0]
                if isinstance(recent_post, dict) and "comment" in recent_post:
                    comment = Comment(recent_post["comment"], blockchain_instance=hv)
                else:
                    comment = recent_post
            else:
                # If no posts are found, use the blockchain's head block as a fallback timing metric
                blockchain = Blockchain(blockchain_instance=hv)
                blockchain.get_current_block()

            access_time = format_float(timer() - start)

        return {
            "successful": True,
            "access_time": access_time,
        }
    except Exception:
        # Let benchmark_executor handle the exception
        raise


def benchmark_block_diff(node, num_retries=10, num_retries_call=10, timeout=60):
    """Benchmark a node's block synchronization status.

    This function measures two key metrics for a node:
    1. Head delay: The time difference between the current UTC time and the timestamp of the
       most recent block (head block). This indicates how up-to-date the node is.
    2. Difference between head and irreversible blocks: The number of blocks between the
       head block and the last irreversible block. This indicates how many recent blocks
       are still potentially reversible.

    Args:
        node (str): URL of the node to benchmark
        num_retries (int, optional): Number of connection retries. Defaults to 10.
        num_retries_call (int, optional): Number of API call retries. Defaults to 10.
        timeout (int, optional): Connection timeout in seconds. Defaults to 60.

    Returns:
        dict: A dictionary containing the following keys:
            - successful (bool): Always True if no exception is raised
            - head_delay (float): Time difference between current UTC time and head block time in seconds
            - diff_head_irreversible (int): Number of blocks between head block and last irreversible block

    Raises:
        Exception: Any exception that occurs during node connection or data retrieval
    """
    try:
        # Initialize Hive client
        hv = Hive(
            node=node,
            num_retries=num_retries,
            num_retries_call=num_retries_call,
            timeout=timeout,
        )

        # Get dynamic global properties through the Hive object directly
        # rather than from the Blockchain object
        dgp = hv.rpc.get_dynamic_global_properties()

        # Get head block number and time
        head_block_num = dgp["head_block_number"]
        head_block_time = dgp["time"]

        # Convert head block time to datetime
        if isinstance(head_block_time, str):
            head_block_time = datetime.strptime(
                head_block_time, "%Y-%m-%dT%H:%M:%S"
            ).replace(tzinfo=timezone.utc)

        # Calculate head delay (difference between current time and head block time)
        current_time = datetime.now(timezone.utc)
        head_delay = (current_time - head_block_time).total_seconds()

        # Calculate difference between head block and last irreversible block
        last_irreversible_block_num = dgp["last_irreversible_block_num"]
        diff_head_irreversible = head_block_num - last_irreversible_block_num

        return {
            "successful": True,
            "head_delay": format_float(head_delay),
            "diff_head_irreversible": diff_head_irreversible,
        }
    except Exception:
        # Let benchmark_executor handle the exception
        raise
