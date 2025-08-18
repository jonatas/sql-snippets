# GitHub Copilot Instructions for SQL Snippets

## Database Connection Rules

- **Always use**: `#file:run_timescale_docker.sh` when running any SQL snippet
- **Local environment**: Use `.env` file for local development configuration
- **Password handling**: Use `PGPASSWORD` environment variable for psql command line authentication
- **Pager settings**: Disable pager in psql with `--no-pager` or `\pset pager off`

## File Organization

- **All-in-one approach**: All SQL snippets should work as complete, standalone files
- **No extra files**: Do not create additional test files - use direct psql command line execution
- **Single file execution**: Each `.sql` file should be self-contained and executable

## Database Preferences

- **TimescaleDB features**: Prefer TimescaleDB-specific functionality:
  - Hypertables for time-series data
  - Background jobs for automated tasks
  - Continuous aggregates (caggs)
  - Compression and retention policies
  - Time-based partitioning

## Command Line Usage

- **psql execution pattern**:
  ```bash
  PGPASSWORD=password psql -h localhost -U postgres -d postgres --no-pager -f snippet.sql
  ```

## Development Workflow

1. Start TimescaleDB using `run_timescale_docker.sh`
2. Use environment variables from `.env` for configuration
3. Test snippets directly with psql command line
4. Ensure each snippet is self-contained and demonstrates TimescaleDB features
5. Focus on practical, real-world TimescaleDB use cases

## Code Style

- Prefer TimescaleDB functions and features over vanilla PostgreSQL when available
- Include comments explaining TimescaleDB-specific functionality
- Use meaningful table and column names that reflect time-series data patterns
- Include setup and cleanup within each snippet when necessary in the top to always allow to re-run it with no failures.
- Do not add instructions in the SQL in the bottom of the file or comments. This includes any usage examples or explanations.
- Avoid using `\i` or `\include` commands in snippets, as they are not compatible with the all-in-one execution model.
- Keep functions minimal and do not add helper functions like some utility functions to watch or monitor the status. This is for minimal POCs.
- Do never over promote words like "best practices" or "recommended", "production-ready", "comprehensible" in comments or documentation. Focus on practical descriptions and use cases instead.