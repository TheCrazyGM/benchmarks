# Hive Blockchain Benchmarking Tools

This repository contains benchmarking tools for Hive and Hive-Engine nodes. These tools help measure and track the performance of blockchain API nodes to identify the best performing nodes for various operations.

## Projects

### Hive-Bench

[Hive-Bench](./hive-bench) is a benchmarking tool for Hive nodes using the hive-nectar library. It measures node performance for operations like:

- Configuration retrieval
- Block retrieval
- Account history
- API call response times
- Block sync status

### Engine-Bench

[Engine-Bench](./engine-bench) is a benchmarking tool for Hive-Engine nodes using the nectarengine library. It measures node performance for operations like:

- Token retrieval
- Contract operation performance
- Account history
- Configuration retrieval
- Node latency

## Standardization

Both tools follow a standardized approach to:

1. Command-line interfaces and arguments
2. File naming and directory structure
3. Shell script behavior
4. Database schema
5. Post generation and blockchain posting

See the [standardization plan](./standardization-plan) folder for detailed information about the standardization efforts.

## Common Usage

Both tools support similar commands:

```bash
# Run benchmarks
cd hive-bench    # or engine-bench
./run.sh

# Generate and post benchmark report
./post.sh
```

Or use the CLI tools directly:

```bash
# For Hive benchmark
python -m src.hive_bench.cli.bench_runner -u -o hive_benchmark_results.json
python -m src.hive_bench.cli.generate_post -p

# For Hive-Engine benchmark
python -m src.engine_bench.cli.bench_runner -u -o engine_benchmark_results.json
python -m src.engine_bench.cli.generate_post -p
```

## Installation

Each project has its own installation instructions. See the respective README files for details:

- [Hive-Bench README](./hive-bench/README.md)
- [Engine-Bench README](./engine-bench/README.md)

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.