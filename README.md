# SQL Snippets

This is my personal collection of SQL examples and proof-of-concept files, specifically focused on **TimescaleDB** features and capabilities. The repository features a diverse range of SQL files demonstrating time-series data handling, hypertables, continuous aggregates, and other TimescaleDB-specific functionality.

## Quick Start

1. **Start TimescaleDB**: Use the Docker setup to run TimescaleDB locally:
   ```bash
   ./run_timescale_docker.sh
   ```

2. **Load environment**: Source the environment variables:
   ```bash
   source .env
   ```

3. **Run any SQL snippet**: Use the helper script to execute snippets:
   ```bash
   ./psql_cmd.sh <filename.sql>
   ```
   
   Example:
   ```bash
   ./psql_cmd.sh sensors.sql
   ```

## Environment Setup

- **TimescaleDB Docker**: Uses `timescale/timescaledb-ha:pg17.4-ts2.21.3-all`
- **Connection**: `postgres://postgres:password@localhost:5432/postgres`
- **No pager**: All psql commands are configured to disable paging for better command-line experience

## Learning Categories

**TimescaleDB Core Features**: Files like `hypertable_model.sql`, `caggs.sql`, `compression.sql`, and `retention.sql` demonstrate the core TimescaleDB functionality including hypertables, continuous aggregates, compression policies, and data retention.

**Time-Series Analytics**: Files such as `bollinger_bands.sql`, `correlation_matrix.sql`, `ohlcv.sql`, and `frequency.sql` showcase advanced time-series analysis techniques using TimescaleDB's analytical functions.

**Background Jobs & Automation**: Examples like `job.sql`, `jobs.sql`, and various caggs files demonstrate TimescaleDB's background job system for automated data processing and maintenance.

**Performance & Scale**: Files like `massive_distributed_inserts.sql`, `chunk_skipping.sql`, and `skip_scan_example.sql` provide insights into TimescaleDB's performance optimization features.

## File Conventions

- All SQL files are **self-contained** and can be executed independently
- Each file includes necessary setup and demonstrates specific TimescaleDB features
- Use `#file:run_timescale_docker.sh` reference for database connection in any new snippets
- Focus on practical, real-world TimescaleDB use cases
