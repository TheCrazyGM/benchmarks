# Implementation Steps for Standardization

This document outlines the specific steps needed to standardize the `hive-bench` and `engine-bench` projects.

## Engine Bench Updates

1. Add missing `post_to_hive` function to `engine_bench/blockchain.py`

   - Copy the function from `hive-bench/bench/blockchain.py`
   - Update any project-specific constants

2. Standardize shell scripts

   - Update `run.sh` to include better environment detection and script directory handling
   - Update `post.sh` to use similar format and include `-p` flag for posting

3. Update CLI command structure

   - Add posting functionality to `generate_post.py`
   - Ensure consistent argument handling

4. Review database schema and ensure consistency

## Hive Bench Updates

1. Standardize shell scripts

   - Update `run.sh` to use more robust environment checks
   - Ensure `post.sh` follows the standardized pattern

2. Align file naming conventions

   - Update references to `results.json` to `hive_benchmark_results.json`
   - Update references to `benchmark_history.db` to `hive_benchmark_history.db`
   - Update references to `benchmark_post.md` to `hive_benchmark_post.md`

3. Update CLI scripts to match standardized pattern

   - Update argument handling in `bench_runner.py` and `generate_post.py`

4. Enhance error handling and logging

## General Updates for Both Projects

1. Standardize dependencies in `pyproject.toml`

   - Ensure both projects have similar development tools configuration
   - Align Python version requirements

2. Update README files for consistency

   - Ensure both have similar structure and level of detail
   - Include similar installation and usage instructions

3. Standardize logging configuration

   - Use same format and levels in both projects

4. Align code formatting
   - Apply same code style to both projects
   - Use ruff for linting and formatting

## Verification Steps

After making the changes, verify:

1. Both projects can run benchmarks successfully
2. Both projects can generate posts and update JSON metadata
3. Both projects can post to Hive
4. Error handling works correctly in both projects

## Special Considerations

1. Keep benchmark differences that are specific to each blockchain

   - Hive: block, history, apicall, block_diff
   - Hive-Engine: token, contract, account_history, latency

2. Maintain separation where needed

   - Keep separate package names
   - Keep specific blockchain interaction logic

3. Environmental compatibility
   - Ensure both projects work with the same Python version
   - Test both projects on the same platforms
