# Nectar-Bench

A benchmarking tool for Hive-Engine nodes using the nectarengine library. This tool is inspired by and similar to the [bench](https://github.com/thecrazygm/bench) project that benchmarks Hive nodes.

## Overview

Nectar-Bench performs a series of benchmark tests on Hive-Engine nodes to measure their performance and reliability. It tests:

- Token retrieval performance
- Contract operation performance
- Account history retrieval
- Configuration retrieval speed
- Node latency

The benchmark results are stored in a SQLite database and can be used to generate daily performance reports that are posted to the Hive blockchain.

## Installation

1. Clone this repository:

   ```bash
   git clone https://github.com/thecrazygm/engine-bench.git
   cd engine-bench
   ```

2. Create and activate a virtual environment:

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows, use: .venv\Scripts\activate
   ```

3. Install dependencies:

   ```bash
   pip install nectar nectarengine
   ```

4. Set up your Hive account credentials:

   ```bash
   cp .env.sample .env
   # Edit .env with your Hive account and posting key
   ```

## Usage

### Running Benchmarks

To run benchmarks on all Hive-Engine nodes listed in `h-e-nodes.txt`:

```bash
./run.sh
```

Or manually:

```bash
python -m src.engine_bench.cli.bench_runner -u -o engine_benchmark_results.json
```

This will:

1. Connect to all listed Hive-Engine nodes
2. Run the benchmark tests
3. Store results in the SQLite database (`engine_benchmark_history.db`)
4. Save a JSON report (`engine_benchmark_results.json`)
5. Update the account's JSON metadata with the benchmark results (if `-u` flag is used, prints transaction details)

### Generating Post Reports

To generate a markdown report from the latest benchmark results and post it to Hive:

```bash
./post.sh
```

Or manually:

```bash
python -m src.engine_bench.cli.generate_post -p
```

This will create a markdown file (`engine_benchmark_post.md`) containing the benchmark results and a metadata file (`engine_benchmark_metadata.json`) for use in the Hive post, and post the results to Hive.

### Command-line Options

#### Benchmark Runner (`engine-bench`)

- `-s, --seconds`: Time limit for each benchmark in seconds (default: 30)
- `--no-threading`: Disable threading for benchmarks
- `-a, --account`: Account name to use for history benchmark and metadata updates (when using -u)
- `-r, --retries`: Number of connection retries (default: 3)
- `-c, --call-retries`: Number of API call retries (default: 3)
- `-t, --timeout`: Connection timeout in seconds (default: 30)
- `-o, --output`: Output file for benchmark results (default: engine_benchmark_results.json)
- `-f, --report-file`: Existing report file to use instead of running benchmarks
- `--no-db`: Do not store results in database
- `-u, --update-metadata`: Update account JSON metadata with benchmark results
- `-v, --verbose`: Enable verbose logging

#### Post Generator (`engine-bench-post`)

- `-o, --output`: Output markdown file (default: engine_benchmark_post.md)
- `-d, --db`: SQLite database file (default: engine_benchmark_history.db)
- `-j, --json`: Path to save post metadata JSON (default: engine_benchmark_metadata.json)
- `-t, --days`: Number of days of historical data to include (default: 7)
- `-p, --publish`: Publish the generated content to Hive
- `--permlink`: Custom permlink for the post
- `--community`: Community to post to
- `--tags`: Comma-separated list of tags
- `-v, --verbose`: Enable verbose logging

## Customization

### Adding/Removing Nodes

Edit the `h-e-nodes.txt` file to add or remove Hive-Engine nodes from the benchmark.

### Changing Benchmark Parameters

You can customize benchmark parameters by editing the `.env` file:

```bash
# Benchmark parameters
BENCHMARK_SECONDS=30       # Duration for each benchmark test
BENCHMARK_ACCOUNT=username  # Account for history benchmark
BENCHMARK_TOKEN=HIVE       # Token for token benchmark
BENCHMARK_CONTRACT=tokens   # Contract for contract benchmark
```

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
