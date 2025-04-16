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
        "--output",
        "-o",
        default="hive_benchmark_post.md",
        help="Output markdown file (default: hive_benchmark_post.md)",
    )
    parser.add_argument(
        "--db",
        "-d",
        default="hive_benchmark_history.db",
        help="SQLite database file with historical data (default: hive_benchmark_history.db)",
    )
    parser.add_argument(
        "--json",
        "-j",
        default="hive_benchmark_metadata.json",
        help="Path to save the post metadata JSON (default: hive_benchmark_metadata.json)",
    )
    parser.add_argument(
        "--days",
        "-t",
        type=int,
        default=7,
        help="Number of days of historical data to include (default: 7)",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument(
        "--post", "-p", action="store_true", help="Post the generated content to Hive"
    )
    parser.add_argument(
        "--permlink",
        help="Custom permlink for the post (default: auto-generated from title)",
    )
    parser.add_argument("--community", help="Community to post to (optional)")
    parser.add_argument(
        "--tags",
        help="Comma-separated list of tags (default: hive,benchmark,nodes,api,performance)",
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

    # Generate the post using our modular code
    try:
        content, metadata = generate_post(output_file=args.output, db_path=args.db, days=args.days)
        logging.info(f"Generated post content and saved to {args.output}")

        # Save metadata to JSON file
        import json
        import os
        from pathlib import Path

        # Get the absolute path to the project root directory if needed
        json_path = args.json
        if not os.path.isabs(json_path):
            current_file = Path(__file__).resolve()
            project_root = current_file.parent.parent.parent.parent
            json_path = os.path.join(project_root, json_path)

        with open(json_path, "w") as f:
            json.dump(metadata, f, indent=2)
        logging.info(f"Metadata saved to {json_path}")

        # Post to Hive if requested
        if args.post:
            # Parse tags if provided
            tags = None
            if args.tags:
                tags = [tag.strip() for tag in args.tags.split(",")]

            # Generate a permlink if not provided
            permlink = args.permlink
            if not permlink and "title" in metadata:
                # Create a permlink from the title
                date_str = datetime.now().strftime("%Y%m%d")
                title_slug = metadata["title"].lower().replace(" ", "-").replace("/", "-")
                # Remove excessive -
                title_slug = title_slug.replace("---", "-")
                # Remove special characters
                import re

                title_slug = re.sub(r"[^a-z0-9-]", "", title_slug)
                permlink = f"{date_str}-{title_slug}"

            # Post to Hive
            try:
                # Post to Hive
                _ = post_to_hive(
                    content=content,
                    metadata=metadata,
                    permlink=permlink,
                    tags=tags,
                    community=args.community,
                    beneficiaries=None,  # No beneficiaries for now
                )

                logging.info("Successfully posted to Hive")
                print("\nSuccessfully posted to Hive!")
                if permlink:
                    print(
                        f"View your post at: https://peakd.com/@{os.getenv('HIVE_ACCOUNT')}/{permlink}"
                    )
            except Exception as e:
                logging.error(f"Failed to post to Hive: {e}")
                print(f"\nFailed to post to Hive: {e}")

        # Print summary information
        print("\nPost Generation Summary:")
        print(f"Generated post saved to: {args.output}")
        print(f"Working nodes: {metadata.get('node_count', 0)}")
        print(f"Failing nodes: {metadata.get('failing_nodes', 0)}")

        if metadata.get("top_nodes"):
            print("\nTop performing nodes:")
            for i, node in enumerate(metadata["top_nodes"][:5]):
                print(f"{i + 1}. {node}")

        return 0
    except Exception as e:
        logging.error(f"Error generating post: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
