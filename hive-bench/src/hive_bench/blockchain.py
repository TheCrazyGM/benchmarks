#!/usr/bin/env python
"""Blockchain interaction module for the Hive node benchmarking application.

This module provides functions for interacting with the Hive blockchain,
including updating account metadata with benchmark results.
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from dotenv import load_dotenv
from nectar.account import Account
from nectar.exceptions import (
    AccountDoesNotExistsException,
    MissingKeyError,
    RPCConnectionRequired,
)
from nectar.hive import Hive
from nectar.nodelist import NodeList

# Initialize logger
logger = logging.getLogger(__name__)

# Optional fallback nodes if the NodeList fails
FALLBACK_NODES = [
    "https://api.hive.blog",
]


def get_project_root() -> Path:
    """Get the absolute path to the project root directory."""
    current_file = Path(__file__).resolve()
    # Go up two levels from src/bench/blockchain.py to reach project root
    return current_file.parent.parent.parent


def load_env_file():
    """Load environment variables from .env file in project root."""
    env_path = get_project_root() / ".env"
    if not env_path.exists():
        logger.warning(f".env file not found at {env_path}")
    load_dotenv(dotenv_path=env_path)


def get_hive_connection(custom_nodes: Optional[List[str]] = None) -> Hive:
    """Get a connection to the Hive blockchain.

    Args:
        custom_nodes: Optional list of custom nodes to use instead of the NodeList.
            If not provided, nodes from NodeList will be used.

    Returns:
        A connected Hive instance.

    Raises:
        RPCConnectionRequired: If connection to the Hive blockchain fails.
    """
    # Load environment variables
    load_env_file()

    # Get configuration from environment variables with defaults
    POSTING_WIF = os.getenv("POSTING_WIF")
    ACTIVE_WIF = os.getenv("ACTIVE_WIF")
    DRY_RUN = os.getenv("DRY_RUN", "False").lower() in ("true", "1", "t")

    try:
        # Use provided nodes or get from NodeList
        nodes = custom_nodes if custom_nodes else NodeList().get_hive_nodes()

        # If no nodes are available, use fallback nodes
        if not nodes:
            logger.warning("No nodes available from NodeList, using fallback nodes")
            nodes = FALLBACK_NODES

        # Create Hive instance with posting key if available
        keys = [POSTING_WIF, ACTIVE_WIF] if POSTING_WIF and ACTIVE_WIF else []
        hive = Hive(node=nodes, keys=keys, no_broadcast=DRY_RUN)

        # Test connection by getting config
        _ = hive.get_config()

        return hive
    except Exception as e:
        logger.error(f"Failed to connect to Hive blockchain: {e}")
        raise RPCConnectionRequired(f"Failed to connect to Hive blockchain: {e}")


def update_json_metadata(
    data: Dict[str, Any], account: Optional[str] = None
) -> Union[Dict[str, Any], str]:
    """Update account JSON metadata with benchmark results.

    This function updates the account JSON metadata with the provided benchmark data.
    The account must be accessible with the ACTIVE_WIF.

    Args:
        data: Dictionary containing the benchmark data to store in account metadata.
            The data structure should match the output of the benchmark runner.
        account: Optional account name to update metadata for. If not provided,
            the HIVE_ACCOUNT environment variable will be used.

    Returns:
        The transaction details if successful.

    Raises:
        ValueError: If ACTIVE_WIF or account name is not set.
        MissingKeyError: If the required keys are not available.
        RPCConnectionRequired: If connection to the Hive blockchain fails.
    """
    # Load environment variables
    load_env_file()

    # Get configuration from environment variables with defaults
    ACTIVE_WIF = os.getenv("ACTIVE_WIF")
    HIVE_ACCOUNT = account or os.getenv("HIVE_ACCOUNT")
    DRY_RUN = os.getenv("DRY_RUN", "False").lower() in ("true", "1", "t")

    # Validate environment variables
    if not ACTIVE_WIF:
        raise ValueError("ACTIVE_WIF environment variable is not set")

    if not HIVE_ACCOUNT:
        raise ValueError("No account specified and HIVE_ACCOUNT environment variable is not set")

    # Validate input data
    if not isinstance(data, dict):
        raise ValueError(f"Data must be a dictionary, got {type(data).__name__}")

    try:
        # Get Hive connection
        hive = get_hive_connection()

        # Get account
        acc = Account(HIVE_ACCOUNT, blockchain_instance=hive)

        # Update account metadata
        logger.info(f"Updating account metadata for {HIVE_ACCOUNT}")
        if DRY_RUN:
            logger.warning("DRY_RUN mode enabled, no actual transaction will be broadcast")

        tx = acc.update_account_metadata(data, account=HIVE_ACCOUNT)
        logger.info(f"Successfully updated account metadata: {tx}")

        return tx
    except MissingKeyError as e:
        logger.error(f"Missing key error: {e}")
        raise
    except RPCConnectionRequired as e:
        logger.error(f"Connection error: {e}")
        raise
    except AccountDoesNotExistsException as e:
        logger.error(f"Account does not exist: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to update account metadata: {e}")
        raise


def post_to_hive(
    content: str,
    metadata: Dict[str, Any],
    permlink: Optional[str] = None,
    tags: Optional[List[str]] = None,
    community: Optional[str] = None,
    beneficiaries: Optional[List[Dict[str, Any]]] = None,
) -> Union[Dict[str, Any], str]:
    """Post benchmark results to Hive.

    This function creates a new post on the Hive blockchain with the provided content
    and metadata. The post will be created under the HIVE_ACCOUNT account.

    Args:
        content: The markdown content of the post.
        metadata: Dictionary containing metadata for the post, including the title.
        permlink: Optional custom permlink for the post. If not provided, it will be
            generated from the title.
        tags: Optional list of tags for the post. If not provided, default tags will be used.
        community: Optional community to post to. If not provided, no community will be specified.
        beneficiaries: Optional list of beneficiaries for the post rewards.
            Each beneficiary should be a dict with 'account' and 'weight' keys.

    Returns:
        The transaction details if successful.

    Raises:
        ValueError: If POSTING_WIF or HIVE_ACCOUNT environment variables are not set,
            or if required parameters are missing or invalid.
        MissingKeyError: If the required keys are not available.
        RPCConnectionRequired: If connection to the Hive blockchain fails.
    """
    # Load environment variables
    load_env_file()

    # Get configuration from environment variables with defaults
    POSTING_WIF = os.getenv("POSTING_WIF")
    HIVE_ACCOUNT = os.getenv("HIVE_ACCOUNT")
    DRY_RUN = os.getenv("DRY_RUN", "False").lower() in ("true", "1", "t")

    # Validate environment variables
    if not POSTING_WIF:
        raise ValueError("POSTING_WIF environment variable is not set")

    if not HIVE_ACCOUNT:
        raise ValueError("HIVE_ACCOUNT environment variable is not set")

    # Validate input data
    if not content:
        raise ValueError("Content cannot be empty")

    if not metadata or not isinstance(metadata, dict):
        raise ValueError("Metadata must be a non-empty dictionary")

    if "title" not in metadata:
        raise ValueError("Metadata must contain a 'title' key")

    # Set default tags if not provided
    if not tags:
        tags = ["hive", "benchmark", "nodes", "api", "performance"]

    try:
        # Get Hive connection
        hive = get_hive_connection()

        # Prepare JSON metadata
        json_metadata = {
            "tags": tags,
            "app": "nectar-node-benchmarks/1.0",
            "timestamp": metadata.get("timestamp", ""),
            "node_count": metadata.get("node_count", 0),
            "failing_nodes": metadata.get("failing_nodes", 0),
            "top_nodes": metadata.get("top_nodes", []),
        }

        # Prepare comment options with beneficiaries if provided
        comment_options = None
        if beneficiaries:
            comment_options = {
                "max_accepted_payout": "1000000.000 HBD",
                "percent_hbd": 10000,  # 100% HBD
                "allow_votes": True,
                "allow_curation_rewards": True,
                "extensions": [[0, {"beneficiaries": beneficiaries}]],
            }
        else:
            comment_options = {
                "max_accepted_payout": "1000000.000 HBD",
                "percent_hbd": 0,  # 100% Powerup
                "allow_votes": True,
                "allow_curation_rewards": True,
            }

        # Post to Hive
        logger.info(f"Creating post on Hive with title: {metadata['title']}")
        if DRY_RUN:
            logger.warning("DRY_RUN mode enabled, no actual transaction will be broadcast")
            return {"status": "dry_run", "title": metadata["title"]}

        tx = hive.post(
            title=metadata["title"],
            body=content,
            author=HIVE_ACCOUNT,
            permlink=permlink,
            json_metadata=json_metadata,
            comment_options=comment_options,
            community=community,
            tags=tags,
            self_vote=False,
        )

        post_url = f"https://peakd.com/@{HIVE_ACCOUNT}/{permlink or ''}"
        logger.info(f"Successfully posted to Hive: {post_url}")
        return tx

    except MissingKeyError as e:
        logger.error(f"Missing key error: {e}")
        raise
    except RPCConnectionRequired as e:
        logger.error(f"Connection error: {e}")
        raise
    except AccountDoesNotExistsException as e:
        logger.error(f"Account does not exist: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to post to Hive: {e}")
        raise
