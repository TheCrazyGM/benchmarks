#!/usr/bin/env python
"""Generate a benchmark post from the latest benchmark data and optionally publish it to Hive."""

import argparse
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv  # Load environment variables

from engine_bench import __version__
from engine_bench.blockchain import post_to_hive
from engine_bench.post_generation import generate_post


def generate_permlink(title, date_str):
    """Generate a standardized permlink from title and date."""
    title_slug = title.lower().replace(" ", "-").replace("/", "-")
    title_slug = re.sub(r"[^a-z0-9-]", "", title_slug)
    return f"{date_str.replace('-', '')}-{title_slug}"


def get_project_root() -> Path:
    """Get the absolute path to the project root directory."""
    current_file = Path(__file__).resolve()
    # Go up three levels from src/engine_bench/cli/generate_post.py to reach project root
    return current_file.parent.parent.parent.parent


def load_post_content(markdown_path):
    """Load post content from markdown file.

    Args:
        markdown_path (str): Path to the markdown file containing post content

    Returns:
        str: Post content
    """
    if not os.path.isabs(markdown_path):
        markdown_path = os.path.join(get_project_root(), markdown_path)

    if not os.path.exists(markdown_path):
        logging.error(f"Post content file not found at {markdown_path}")
        return None

    try:
        with open(markdown_path, "r") as f:
            return f.read()
    except Exception as e:
        logging.error(f"Error loading post content from {markdown_path}: {e}")
        return None


def main():
    """Main entry point for generating benchmark posts and optionally publishing to Hive."""
    try:
        # Load environment variables
        load_dotenv()
        # Parse command line arguments
        parser = argparse.ArgumentParser(
            description="Generate a benchmark post from the latest data and optionally publish it to Hive"
        )
        parser.add_argument(
            "-o",
            "--output",
            default="engine_benchmark_post.md",
            help="Path to save the markdown post (default: engine_benchmark_post.md)",
        )
        parser.add_argument(
            "-d",
            "--db",
            default="engine_benchmark_history.db",
            help="Path to the SQLite database file (default: engine_benchmark_history.db)",
        )
        parser.add_argument(
            "-j",
            "--json",
            default="engine_benchmark_metadata.json",
            help="Path to save the post metadata JSON (default: engine_benchmark_metadata.json)",
        )
        parser.add_argument(
            "--days",
            type=int,
            default=7,
            help="Number of days of historical data to include (default: 7)",
        )
        parser.add_argument("-p", "--publish", action="store_true", help="Publish the post to Hive")
        parser.add_argument("-a", "--account", help="Hive account name to post from")
        parser.add_argument("-k", "--key", help="Hive posting key for the account")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Don't actually post to Hive, just show what would be posted",
        )
        parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
        parser.add_argument("--version", action="version", version=f"engine-bench {__version__}")
        args = parser.parse_args()

        # Configure logging
        log_level = logging.DEBUG if args.verbose else logging.INFO
        logging.basicConfig(
            level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # Generate post
        logging.info(f"Generating benchmark post from database {args.db}")
        content, metadata = generate_post(output_file=args.output, db_path=args.db, days=args.days)

        if metadata is None or content is None:
            logging.error("Post generation failed; no metadata/content returned.")
            print("Failed to generate post content or metadata. See earlier errors for details.")
            return 1

        # Save metadata to JSON file
        json_path = args.json
        if not os.path.isabs(json_path):
            json_path = os.path.join(get_project_root(), json_path)

        with open(json_path, "w") as f:
            json.dump(metadata, f, indent=2)
        logging.info(f"Metadata saved to {json_path}")

        # Print summary
        print("\nPost Generation Summary:")
        print(f"  Post saved to: {args.output}")
        print(f"  Metadata saved to: {json_path}")
        print(f"  Included {args.days} days of historical data")
        print(f"  Working nodes: {metadata.get('node_count', 'N/A')}")
        print(f"  Failing nodes: {metadata.get('failing_nodes', 'N/A')}")
        print("\nTop nodes from benchmark:")
        if "top_nodes" in metadata and metadata["top_nodes"]:
            for i, node in enumerate(metadata["top_nodes"], 1):
                print(f"    {i}. {node['url']} (rank: {node.get('rank', 'N/A')})")
        else:
            print("    No top nodes available.")

        # Publish to Hive if requested
        if args.publish:
            # Get account and key from environment if not provided as args
            account = args.account or os.environ.get("HIVE_ACCOUNT")
            key = args.key or os.environ.get("POSTING_WIF")

            if not account:
                logging.error(
                    "No Hive account specified. Use --account or set HIVE_ACCOUNT environment variable."
                )
                return

            if not key and not args.dry_run:
                logging.error(
                    "No Hive posting key specified. Use --key or set POSTING_WIF set  environment variable."
                )
                return

            # Load the post content if needed
            if not content:
                content = load_post_content(args.output)
                if not content:
                    logging.error(f"Failed to load post content from {args.output}")
                    return

            # Prepare post metadata
            date_str = datetime.now().strftime("%Y-%m-%d")
            title = f"Hive-Engine Benchmark Report - {date_str}"
            metadata["title"] = title

            # Standardized permlink generation
            permlink = generate_permlink(metadata["title"], date_str)

            # Get default tags
            tags = ["hive-engine", "benchmark", "nodes", "performance", "api", "sbi-skip"]

            # Post to Hive
            logging.info(f"Publishing post to Hive as @{account}...")
            try:
                # Set DRY_RUN environment variable for the blockchain module
                if args.dry_run:
                    os.environ["DRY_RUN"] = "True"

                # Set HIVE_ACCOUNT and POSTING_WIF environment variables
                os.environ["HIVE_ACCOUNT"] = account
                if key:
                    os.environ["POSTING_WIF"] = key

                # Post to Hive using the blockchain module
                post_to_hive(content=content, metadata=metadata, permlink=permlink, tags=tags)

                # Print success message
                print("\nSuccessfully posted to Hive!")
                print(f"View your post at: https://peakd.com/@{account}/{permlink}")

            except Exception as e:
                logging.error(f"Failed to post to Hive: {e}")
                print(f"\nFailed to post to Hive: {e}")
        return 0
    except Exception as e:
        logging.error(f"Unexpected error in generate_post: {e}")
        return 1


if __name__ == "__main__":
    main()
