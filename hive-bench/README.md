# Hive-Bench

A benchmarking tool for Hive nodes using the hive-nectar library.

## Overview

Hive-Bench performs a series of benchmark tests on Hive nodes to measure their performance and reliability. It tests:

- Configuration retrieval speed
- Block retrieval performance
- Account history retrieval
- API call response time
- Block synchronization status (head block vs irreversible block)

The benchmark results are stored in a SQLite database and can be used to generate daily performance reports that are posted to the Hive blockchain.

## Installation

1. Clone this repository:

   ```bash
   git clone https://github.com/thecrazygm/hive-bench.git
   cd hive-bench
   ```

2. Create and activate a virtual environment:

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows, use: .venv\Scripts\activate
   ```

3. Install dependencies:

   ```bash
   pip install hive-nectar python-dotenv
   ```

4. Set up your Hive account credentials:

   ```bash
   cp .env.sample .env
   # Edit .env with your Hive account and posting key
   ```

## Usage

### Running Benchmarks

To run benchmarks on all Hive nodes:

```bash
./run.sh
```

Or manually:

```bash
python -m src.hive_bench.cli.bench_runner -u -o hive_benchmark_results.json
```

This will:

1. Connect to all Hive nodes
2. Run the benchmark tests
3. Store results in the SQLite database (`hive_benchmark_history.db`)
4. Save a JSON report (`hive_benchmark_results.json`)
5. Update the account's JSON metadata with the benchmark results (if `-u` flag is used, prints transaction details)

### Generating Post Reports

To generate a markdown report from the latest benchmark results and post it to Hive:

```bash
./post.sh
```

Or manually:

```bash
python -m src.hive_bench.cli.generate_post -p
```

This will create a markdown file (`hive_benchmark_post.md`) containing the benchmark results and a metadata file (`hive_benchmark_metadata.json`) for use in the Hive post, and post the results to Hive.

### Command-line Options

#### Benchmark Runner (`hive-bench`)

- `-s, --seconds`: Time limit for each benchmark in seconds (default: 30)
- `--no-threading`: Disable threading for benchmarks
- `-a, --account`: Account name to use for history benchmark and metadata updates (when using -u) (default: thecrazygm)
- `-p, --post`: Post identifier for API call benchmark
- `-r, --retries`: Number of connection retries (default: 3)
- `-c, --call-retries`: Number of API call retries (default: 3)
- `-t, --timeout`: Connection timeout in seconds (default: 30)
- `-o, --output`: Output file for benchmark results (default: hive_benchmark_results.json)
- `-f, --report-file`: Existing report file to use instead of running benchmarks
- `--no-db`: Do not store results in database
- `-u, --update-metadata`: Update account JSON metadata with benchmark results
- `-v, --verbose`: Enable verbose logging

#### Post Generator (`hive-bench-post`)

- `-o, --output`: Output markdown file (default: hive_benchmark_post.md)
- `-d, --db`: SQLite database file (default: hive_benchmark_history.db)
- `-j, --json`: Path to save post metadata JSON (default: hive_benchmark_metadata.json)
- `-t, --days`: Number of days of historical data to include (default: 7)
- `-p, --publish`: Publish the generated content to Hive
- `--permlink`: Custom permlink for the post
- `--community`: Community to post to
- `--tags`: Comma-separated list of tags
- `-v, --verbose`: Enable verbose logging

## Database Schema

The benchmark results are stored in an SQLite database with the following schema:

- `benchmark_runs`: Metadata about each benchmark run
- `nodes`: Information about each node tested
- `node_status`: Status of each node for a given benchmark run
- `test_results`: Results for specific benchmark tests

## JSON Metadata

When posting to Hive, the benchmark results are included in the post's `json_metadata` field, making the data available for automated processing by other applications.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
