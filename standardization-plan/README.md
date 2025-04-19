# Benchmarks Standardization Plan

This document outlines the standardization plan for the `hive-bench` and `engine-bench` projects to ensure consistent structure, naming, and functionality.

## 1. File Structure and Naming

### Current Differences

- `hive-bench` uses a `bench` package
- `engine-bench` uses an `engine_bench` package
- Different database names (`benchmark_history.db` vs `engine_benchmark_history.db`)
- Different results file names (`results.json` vs `engine_results.json`)

### Standardization Plan

- Keep separate package names but ensure internal structure is identical
- Standardize database naming convention: `{project}_benchmark_history.db`
- Standardize results file naming: `{project}_benchmark_results.json`
- Standardize post output files: `{project}_benchmark_post.md`

## 2. Benchmark Tests

### Current Differences

- `hive-bench` tests: config, block, history, apicall, block_diff
- `engine-bench` tests: config, token, contract, account_history, latency

### Standardization Plan

- Keep benchmark differences as they are measuring different things specific to each blockchain
- Standardize benchmark function names and structure
- Ensure all benchmarks follow the same pattern for returning results

## 3. CLI and Runner Scripts

### Current Differences

- Different command-line script names in pyproject.toml
- Slightly different argument handling
- Post generation varies between projects

### Standardization Plan

- Standardize CLI scripts (naming and behavior)
  - `{project}-bench` for running benchmarks
  - `{project}-bench-post` for post generation
- Align argument handling between projects
- Standardize post generation approach

## 4. Shell Scripts

### Current Differences

- `hive-bench` uses hardcoded paths in scripts
- `engine-bench` has more robust environment checks

### Standardization Plan

- Copy `engine-bench` approach to `hive-bench`
- Ensure both projects use similar error handling and environment checks

## 5. Blockchain Interaction

### Current Differences

- `engine-bench` is missing the `post_to_hive` function that exists in `hive-bench`

### Standardization Plan

- Add the `post_to_hive` function to `engine-bench`
- Centralize application name and version in `src/<package>/__init__.py` and import them in `blockchain.py` instead of hard-coded constants
- Ensure blockchain.py files otherwise match, with only project-specific tags differing

## 6. Documentation

### Current Differences

- README files have different levels of detail
- Different instructions for installation and usage

### Standardization Plan

- Standardize README format
- Create consistent installation and usage instructions
- Ensure both projects have clear examples

## 7. Dependencies

### Current Differences

- Different dependency specifications in pyproject.toml
- Different development tools configuration

### Standardization Plan

- Align dependencies where possible
- Standardize development tools configuration
- Ensure consistent Python version requirements

## Implementation Priority

1. Standardize blockchain.py in engine-bench to include missing functions
2. Standardize shell scripts
3. Standardize CLI command structure and argument handling
4. Align database and results file naming
5. Update documentation for consistency
