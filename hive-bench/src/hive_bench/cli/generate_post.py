#!/usr/bin/env python

import argparse
import logging
import os
import re
import sys
from datetime import datetime

from dotenv import load_dotenv

from hive_bench.blockchain import post_to_hive
from hive_bench.post_generation import generate_post


def generate_permlink(title, date_str):
    """Generate a standardized permlink from title and date."""
    title_slug = title.lower().replace(" ", "-").replace("/", "-")
    title_slug = re.sub(r"[^a-z0-9-]", "", title_slug)
    return f"{date_str}-{title_slug}"


def main():
    # Load environment variables from .env file
    load_dotenv()
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Generate a markdown post from benchmark results")
    parser.add_argument(
        "-o",
        "--output",
        default="hive_benchmark_post.md",
        help="Path to save the markdown post (default: hive_benchmark_post.md)",
    )
    parser.add_argument(
        "-d",
        "--db",
        default="hive_benchmark_history.db",
        help="Path to the SQLite database file (default: hive_benchmark_history.db)",
    )
    parser.add_argument(
        "-j",
        "--json",
        default="hive_benchmark_metadata.json",
        help="Path to save the post metadata JSON (default: hive_benchmark_metadata.json)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days of historical data to include (default: 7)",
    )
    parser.add_argument("-p", "--publish", action="store_true", help="Publish the post to Hive")
    parser.add_argument(
        "-a", "--author", help="Hive account name to post from (or set HIVE_ACCOUNT env var)"
    )
    parser.add_argument(
        "-k", "--key", help="Hive posting key for the account (or set POSTING_WIF env var)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't actually post to Hive, just show what would be posted",
    )
    parser.add_argument(
        "--permlink", help="Custom permlink for the post (default: auto-generated from title)"
    )
    parser.add_argument("--community", help="Community to post to (optional)")
    parser.add_argument(
        "--tags",
        help="Comma-separated list of tags (default: hive,benchmark,nodes,api,performance)",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()
    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")
    # Warn if DB missing
    if not os.path.exists(args.db):
        logging.warning(f"Database '{args.db}' not found. Starting fresh without history.")
    # Execute generation and optional publish
    try:
        content, metadata = generate_post(output_file=args.output, db_path=args.db, days=args.days)
        if not content or metadata is None:
            logging.error("Post generation failed; no content or metadata.")
            return 1
        # Save metadata
        import json

        meta_path = (
            args.json
            if os.path.isabs(args.json)
            else os.path.join(os.path.dirname(__file__), args.json)
        )
        with open(meta_path, "w") as mf:
            json.dump(metadata, mf, indent=2)
        logging.info(f"Metadata saved to {meta_path}")
        # Print summary
        print(
            f"Generated post: {args.output}\nMetadata: {meta_path}\nDays: {args.days}\nNodes: {metadata.get('node_count')} (failures: {metadata.get('failing_nodes')})"
        )
        # Publish
        if args.publish:
            account = args.author or os.environ.get("HIVE_ACCOUNT")
            key = args.key or os.environ.get("POSTING_WIF")
            if not account or (not key and not args.dry_run):
                logging.error("Missing account or key for publish.")
                return 1
            tags = (
                [t.strip() for t in args.tags.split(",")]
                if args.tags
                else ["hive", "benchmark", "nodes", "api", "performance"]
            )
            if "title" not in metadata:
                logging.error("Metadata is missing the 'title' key. Metadata: %s", metadata)
                print("ERROR: Metadata is missing the 'title' key. Cannot generate permlink or post.")
                return 1
            permlink = args.permlink or generate_permlink(
                metadata["title"], datetime.now().strftime("%Y%m%d")
            )
            os.environ.update(
                {"HIVE_ACCOUNT": account, "POSTING_WIF": key or "", "DRY_RUN": str(args.dry_run)}
            )
            post_to_hive(
                content=content,
                metadata=metadata,
                permlink=permlink,
                tags=tags,
                community=args.community,
            )
            print(f"Published to Hive as @{account}/{permlink}")
        return 0
    except Exception as err:
        logging.error(f"Error in CLI generate_post: {err}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
