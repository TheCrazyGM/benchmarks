#!/usr/bin/env python

import argparse
import logging
import os
import sys
from datetime import datetime

from dotenv import load_dotenv

from hive_bench.blockchain import post_to_hive

# Import from modular structure
from hive_bench.post_generation import generate_post


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
    parser.add_argument(
        "-p",
        "--publish",
        action="store_true",
        help="Publish the post to Hive",
    )
    parser.add_argument(
        "-a",
        "--author",
        help="Hive account name to post from (or set HIVE_ACCOUNT env var)",
    )
    parser.add_argument(
        "-k",
        "--key",
        help="Hive posting key for the account (or set POSTING_WIF env var)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't actually post to Hive, just show what would be posted",
    )
    parser.add_argument(
        "--permlink",
        help="Custom permlink for the post (default: auto-generated from title)",
    )
    parser.add_argument(
        "--community",
        help="Community to post to (optional)",
    )
    parser.add_argument(
        "--tags",
        help="Comma-separated list of tags (default: hive,benchmark,nodes,api,performance)",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="hive-bench 1.0.0",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    # Parse arguments
    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")

    # Check if database exists
    if not os.path.exists(args.db):
        logging.warning(
            f"Database '{args.db}' not found. A new database will be created, but no historical data will be available."
        )

    try:
        # Generate the post using our modular code
        content, metadata = generate_post(output_file=args.output, db_path=args.db, days=args.days)
        logging.info(f"Generated post content and saved to {args.output}")

        if metadata is None or content is None:
            logging.error("Post generation failed; no metadata/content returned.")
            print("Failed to generate post content or metadata. See earlier errors for details.")
            return 1

        # Save metadata to JSON file
        import json
        from pathlib import Path

        json_path = args.json
        if not os.path.isabs(json_path):
            current_file = Path(__file__).resolve()
            project_root = current_file.parent.parent.parent.parent
            json_path = os.path.join(project_root, json_path)

        with open(json_path, "w") as f:
            json.dump(metadata, f, indent=2)
        logging.info(f"Metadata saved to {json_path}")

        # Print summary information
        print("\nPost Generation Summary:")
        print(f"Generated post saved to: {args.output}")
        print(f"Metadata saved to: {json_path}")
        print(f"Included {args.days} days of historical data")
        print(f"Working nodes: {metadata.get('node_count', 0)}")
        print(f"Failing nodes: {metadata.get('failing_nodes', 0)}")
        if metadata.get("top_nodes"):
            print("\nTop performing nodes:")
            for i, node in enumerate(metadata["top_nodes"][:5]):
                print(f"    {i + 1}. {node}")

        # Publish to Hive if requested
        if args.publish:
            # Get account and key from args or environment
            account = args.author or os.environ.get("HIVE_ACCOUNT")
            key = args.key or os.environ.get("POSTING_WIF")

            if not account:
                logging.error(
                    "No Hive account specified. Use --author or set HIVE_ACCOUNT environment variable."
                )
                print("No Hive account specified. Use --author or set HIVE_ACCOUNT environment variable.")
                return 1

            if not key and not args.dry_run:
                logging.error(
                    "No Hive posting key specified. Use --key or set POSTING_WIF environment variable."
                )
                print("No Hive posting key specified. Use --key or set POSTING_WIF environment variable.")
                return 1

            # Parse tags if provided
            tags = [tag.strip() for tag in args.tags.split(",")] if args.tags else ["hive", "benchmark", "nodes", "api", "performance"]

            # Generate a permlink if not provided
            permlink = args.permlink
            if not permlink and "title" in metadata:
                date_str = datetime.now().strftime("%Y%m%d")
                title_slug = metadata["title"].lower().replace(" ", "-").replace("/", "-")
                import re
                title_slug = re.sub(r"[^a-z0-9-]", "", title_slug)
                permlink = f"{date_str}-{title_slug}"

            # Set environment variables for downstream modules
            os.environ["HIVE_ACCOUNT"] = account
            if key:
                os.environ["POSTING_WIF"] = key
            if args.dry_run:
                os.environ["DRY_RUN"] = "True"

            # Post to Hive
            try:
                _ = post_to_hive(
                    content=content,
                    metadata=metadata,
                    permlink=permlink,
                    tags=tags,
                    community=args.community,
                    beneficiaries=None,
                )
                logging.info("Successfully posted to Hive")
                print("\nSuccessfully posted to Hive!")
                if permlink:
                    print(f"View your post at: https://peakd.com/@{account}/{permlink}")
            except Exception as e:
                logging.error(f"Failed to post to Hive: {e}")
                print(f"\nFailed to post to Hive: {e}")

        return 0
    except Exception as e:
        logging.error(f"Error generating post: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
